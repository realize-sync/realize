use clap::Parser;
use console::style;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use realize::algo::{FileProgress as AlgoFileProgress, MoveFileError, Progress};
use realize::model::service::DirectoryId;
use realize::server::RealizeServer;
use realize::transport::security::{self, PeerVerifier};
use realize::transport::tcp;
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{PrivateKeyDer, SubjectPublicKeyInfoDer};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Realize command-line tool
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Source local path
    #[arg(long, required = false)]
    src_path: Option<PathBuf>,

    /// Destination local path
    #[arg(long, required = false)]
    dst_path: Option<PathBuf>,

    /// Source remote address (host:port)
    #[arg(long, required = false)]
    src_addr: Option<String>,

    /// Destination remote address (host:port)
    #[arg(long, required = false)]
    dst_addr: Option<String>,

    /// Path to the private key (required for remote)
    #[arg(long, required = false)]
    privkey: Option<PathBuf>,

    /// Path to the PEM file with one or more peer public keys (required for remote)
    #[arg(long, required = false)]
    peers: Option<PathBuf>,

    /// Directory id (optional, defaults to 'dir' for local/local)
    #[arg(long, required = false)]
    directory_id: Option<String>,

    /// Throttle upload and download (e.g. 1M)
    #[arg(long)]
    throttle: Option<String>,

    /// Throttle upload (e.g. 1M)
    #[arg(long, alias = "throttle-up")]
    throttle_up: Option<String>,

    /// Throttle download (e.g. 512k)
    #[arg(long, alias = "throttle-down")]
    throttle_down: Option<String>,

    /// Suppress all output except errors
    #[arg(long)]
    quiet: bool,
}

#[tokio::main]
async fn main() {
    // Only log if env variable is set
    env_logger::init();

    let cli = Cli::parse();

    // Determine mode
    let src_is_remote = cli.src_addr.is_some();
    let dst_is_remote = cli.dst_addr.is_some();
    let src_is_local = cli.src_path.is_some();
    let dst_is_local = cli.dst_path.is_some();

    // Validate combinations
    if (src_is_remote && dst_is_remote) && (src_is_local || dst_is_local) {
        print_error("Cannot mix local and remote for the same endpoint");
        process::exit(1);
    }
    if !(src_is_remote || src_is_local) || !(dst_is_remote || dst_is_local) {
        print_error("Must specify both source and destination (local or remote)");
        process::exit(1);
    }

    // Directory id
    let dir_id = DirectoryId::from(
        cli.directory_id
            .clone()
            .unwrap_or_else(|| "dir".to_string()),
    );

    // Setup crypto and peer verifier if needed
    let (privkey, verifier) = if src_is_remote || dst_is_remote {
        let crypto = Arc::new(security::default_provider());
        let privkey = match &cli.privkey {
            Some(path) => match PrivateKeyDer::from_pem_file(path) {
                Ok(key) => match crypto.key_provider.load_private_key(key) {
                    Ok(key) => key,
                    Err(e) => {
                        print_error_with_path(path, &format!("Invalid private key file: {e}",));
                        process::exit(1);
                    }
                },
                Err(e) => {
                    print_error_with_path(path, &format!("Invalid private key file: {e}",));
                    process::exit(1);
                }
            },
            None => {
                print_error("--privkey is required for remote endpoint");
                process::exit(1);
            }
        };
        let mut verifier = PeerVerifier::new(&crypto);
        let peers_path = cli
            .peers
            .as_ref()
            .expect("--peers required for remote endpoint");
        let file = peers_path;
        let pem_iter = SubjectPublicKeyInfoDer::pem_file_iter(file).unwrap_or_else(|e| {
            print_error_with_path(peers_path, &format!("Failed to open peers file: {e}",));
            process::exit(1);
        });
        for pem in pem_iter {
            match pem {
                Ok(spki) => verifier.add_peer(&spki),
                Err(e) => {
                    print_error_with_path(peers_path, &format!("Failed to parse peers file: {e}"));
                    process::exit(1);
                }
            }
        }
        (Some(privkey), Some(Arc::new(verifier)))
    } else {
        (None, None)
    };

    // Build src client
    let src_client = if src_is_local {
        let src_path = cli.src_path.as_ref().unwrap();
        let server = RealizeServer::for_dir(&dir_id, src_path);
        server.as_inprocess_client()
    } else {
        let addr = cli.src_addr.as_ref().unwrap();
        let domain = addr.split(':').next().unwrap();
        match tcp::connect_client(
            domain,
            addr,
            Arc::clone(verifier.as_ref().unwrap()),
            Arc::clone(privkey.as_ref().unwrap()),
        )
        .await
        {
            Ok(client) => client,
            Err(e) => {
                print_error(&format!("Failed to connect to src {addr}: {e}"));
                process::exit(1);
            }
        }
    };

    // Build dst client
    let dst_client = if dst_is_local {
        let dst_path = cli.dst_path.as_ref().unwrap();
        let server = RealizeServer::for_dir(&dir_id, dst_path);
        server.as_inprocess_client()
    } else {
        let addr = cli.dst_addr.as_ref().unwrap();
        let domain = addr.split(':').next().unwrap();
        match tcp::connect_client(
            domain,
            addr,
            Arc::clone(verifier.as_ref().unwrap()),
            Arc::clone(privkey.as_ref().unwrap()),
        )
        .await
        {
            Ok(client) => client,
            Err(e) => {
                print_error(&format!("Failed to connect to dst {addr}: {e}"));
                process::exit(1);
            }
        }
    };

    // TODO: Set throttle limits via RPC if implemented
    if cli.throttle.is_some() || cli.throttle_up.is_some() || cli.throttle_down.is_some() {
        eprintln!("Throttle options are not yet implemented");
    }

    // Move files
    let mut progress = CliProgress::new(&dir_id, cli.quiet);
    let result = realize::algo::move_files(&src_client, &dst_client, dir_id, &mut progress).await;
    progress.finish_and_clear();
    match result {
        Ok((success, error)) => {
            if error == 0 {
                if !cli.quiet {
                    println!(
                        "{} {} file(s) moved",
                        style("SUCCESS").for_stdout().green().bold(),
                        success
                    );
                }
                process::exit(0);
            } else {
                print_error(&format!(
                    "{} file(s) failed, and {} files(s) moved",
                    error, success
                ));
                process::exit(1);
            }
        }
        Err(err) => {
            print_error(&format!("{err}"));
            process::exit(1);
        }
    }
}

/// Print an error message to stderr, with standard format.
fn print_error(msg: &str) {
    eprintln!("{}: {}", style("ERROR").for_stderr().red().bold(), msg);
}

/// Print an error message to stderr, with standard format.
fn print_error_with_path(path: &Path, msg: &str) {
    eprintln!(
        "{} {}: {}",
        style("ERROR").for_stderr().red().bold(),
        path.display(),
        msg
    );
}

struct CliProgress {
    multi: MultiProgress,
    total_files: usize,
    total_bytes: u64,
    overall_pb: ProgressBar,
    next_file_index: Arc<AtomicUsize>,
    dirid: String,
    quiet: bool,
}

impl CliProgress {
    fn new(dirid: &DirectoryId, quiet: bool) -> Self {
        let multi = MultiProgress::with_draw_target(if quiet {
            ProgressDrawTarget::hidden()
        } else {
            ProgressDrawTarget::stdout()
        });
        let overall_pb = multi.add(ProgressBar::no_length());
        overall_pb.set_style(
            ProgressStyle::with_template(
                "{prefix:<9.cyan.bold} [{bar:40.cyan/blue}] {pos}/{len} files",
            )
            .unwrap()
            .progress_chars("=> "),
        );
        overall_pb.set_prefix("Listing");
        Self {
            multi,
            total_files: 0,
            total_bytes: 0,
            overall_pb,
            next_file_index: Arc::new(AtomicUsize::new(1)),
            dirid: dirid.to_string(),
            quiet,
        }
    }

    fn finish_and_clear(&self) {
        self.overall_pb.finish_and_clear();
        let _ = self.multi.clear();
    }
}

impl Progress for CliProgress {
    fn set_length(&mut self, total_files: usize, total_bytes: u64) {
        self.total_files = total_files;
        self.total_bytes = total_bytes;
        self.overall_pb.set_length(total_files as u64);
        self.overall_pb.set_position(0);
        self.overall_pb.set_prefix("Moving");
    }

    fn for_file(&self, path: &std::path::Path, bytes: u64) -> Box<dyn AlgoFileProgress> {
        Box::new(CliFileProgress {
            bar: None,
            next_file_index: self.next_file_index.clone(),
            total_files: self.total_files,
            bytes,
            dirid: self.dirid.clone(),
            path: path.display().to_string(),
            multi: self.multi.clone(),
            quiet: self.quiet,
        })
    }
}

struct CliFileProgress {
    bar: Option<(usize, ProgressBar)>,
    next_file_index: Arc<AtomicUsize>,
    total_files: usize,
    bytes: u64,
    dirid: String,
    path: String,
    multi: MultiProgress,
    quiet: bool,
}

impl CliFileProgress {
    fn get_or_create_bar(&mut self) -> &mut ProgressBar {
        let (_, pb) = self.bar.get_or_insert_with(|| {
            let file_index = self.next_file_index.fetch_add(1, Ordering::Relaxed);
            let pb = self.multi.insert_from_back(1, ProgressBar::new(self.bytes));
            let tag = format!("[{}/{}]", file_index, self.total_files);
            let path = self.path.clone();
            pb.set_style(
                ProgressStyle::with_template(
                    "{tag} {prefix} {path} [{bar:40.cyan/blue}] {pos}/{len} bytes",
                )
                .unwrap()
                .progress_chars("=> ")
                .with_key(
                    "tag",
                    move |_state: &indicatif::ProgressState, w: &mut dyn std::fmt::Write| {
                        let _ = w.write_str(&tag);
                    },
                )
                .with_key(
                    "path",
                    move |_state: &indicatif::ProgressState, w: &mut dyn std::fmt::Write| {
                        let _ = w.write_str(&path);
                    },
                ),
            );
            pb.set_prefix("Checking");

            (file_index, pb)
        });

        pb
    }
}

impl AlgoFileProgress for CliFileProgress {
    fn verifying(&mut self) {
        let pb = self.get_or_create_bar();
        pb.set_prefix("Verifying");
    }
    fn rsyncing(&mut self) {
        let pb = self.get_or_create_bar();
        pb.set_prefix("Rsyncing");
    }
    fn copying(&mut self) {
        let pb = self.get_or_create_bar();
        pb.set_prefix("Copying");
    }
    fn inc(&mut self, bytecount: u64) {
        self.overall_pb.inc(bytecount);
        if let Some((_, pb)) = &mut self.bar {
            pb.inc(bytecount);
        }
    }
    fn success(&mut self) {
        if let Some((index, pb)) = self.bar.take() {
            pb.finish_and_clear();
            if !self.quiet {
                self.multi.suspend(|| {
                    println!(
                        "[{}/{}] {:<9} {}/{}",
                        index,
                        self.total_files,
                        style("Moved").for_stdout().green().bold(),
                        self.dirid,
                        self.path
                    );
                });
            }
        }
    }

    fn error(&mut self, err: &MoveFileError) {
        // Write report even if no bar was ever created.
        let tag = if let Some((index, pb)) = self.bar.take() {
            pb.finish_and_clear();

            format!("[{}/{}] ", index, self.total_files)
        } else {
            "".to_string()
        };
        self.multi.suspend(|| {
            eprintln!(
                "{}{:<9} {}/{}: {}",
                tag,
                style("ERROR").for_stderr().red().bold(),
                self.dirid,
                self.path,
                err,
            );
        });
    }
}
