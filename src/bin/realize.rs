use anyhow::Context as _;
use clap::Parser;
use console::style;
use humantime;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use prometheus::{IntCounter, register_int_counter};
use realize::algo::{FileProgress as AlgoFileProgress, MoveFileError, Progress};
use realize::metrics;
use realize::model::service::DirectoryId;
use realize::server::RealizeServer;
use realize::transport::security::{self, PeerVerifier};
use realize::transport::tcp;
use realize::utils::async_utils::AbortOnDrop;
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{PrivateKeyDer, SubjectPublicKeyInfoDer};
use rustls::sign::SigningKey;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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

    /// Suppress all output except errors
    #[arg(long)]
    quiet: bool,

    /// Maximum total duration for the operation (e.g. "5m", "30s"). If exceeded, the process exits with code 11.
    #[arg(long)]
    max_duration: Option<humantime::Duration>,

    /// Address to export prometheus metrics (host:port, optional)
    #[arg(long)]
    metrics_addr: Option<String>,

    /// Address of prometheus pushgateway (optional)
    #[arg(long)]
    metrics_pushgateway: Option<String>,

    /// Job name for prometheus pushgateway (default: realize)
    #[arg(long, default_value = "realize")]
    metrics_job: String,

    /// Instance label for prometheus pushgateway (optional)
    #[arg(long)]
    metrics_instance: Option<String>,
}

lazy_static::lazy_static! {
    static ref METRIC_UP: IntCounter =
        register_int_counter!("realize_cmd_up", "Command is up").unwrap();
}

#[tokio::main]
async fn main() {
    let _ = ctrlc::set_handler(|| {
        print_error("Interrupted");
        process::exit(20);
    });
    env_logger::init();

    let cli = Cli::parse();
    let _timeout_guard = if let Some(duration) = cli.max_duration {
        let handle = tokio::spawn(async move {
            tokio::time::sleep(duration.into()).await;
            print_error(&format!(
                "Maximum duration ({duration}) exceeded. Giving up."
            ));
            process::exit(11);
        });
        Some(AbortOnDrop::new(handle))
    } else {
        None
    };

    let mut status = 0;
    if let Err(err) = execute(&cli).await {
        print_error(&format!("{err:#}"));
        status = 1;
    };
    if let Some(pushgw) = &cli.metrics_pushgateway {
        if let Err(err) =
            metrics::push_metrics(pushgw, &cli.metrics_job, cli.metrics_instance.as_deref()).await
        {
            print_warning(&format!("Failed to push metrics to {pushgw}: {err}"));
            if status == 0 {
                status = 12;
            }
        }
    }
    process::exit(status);
}

async fn execute(cli: &Cli) -> anyhow::Result<()> {
    metrics::export_metrics(cli.metrics_addr.as_deref());
    METRIC_UP.reset();

    // Directory id
    let dir_id = DirectoryId::from(
        cli.directory_id
            .clone()
            .unwrap_or_else(|| "dir".to_string()),
    );

    // Determine mode
    let src_is_remote = cli.src_addr.is_some();
    let dst_is_remote = cli.dst_addr.is_some();
    let src_is_local = cli.src_path.is_some();
    let dst_is_local = cli.dst_path.is_some();

    // Validate command-line arguments
    if (src_is_remote && dst_is_remote) && (src_is_local || dst_is_local) {
        return Err(anyhow::anyhow!(
            "Cannot mix local and remote for the same endpoint"
        ));
    }
    if !(src_is_remote || src_is_local) || !(dst_is_remote || dst_is_local) {
        return Err(anyhow::anyhow!(
            "Must specify both source and destination (local or remote)"
        ));
    }

    // Setup crypto and peer verifier if needed
    let (privkey, verifier) = if src_is_remote || dst_is_remote {
        let key_path = cli
            .privkey
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("--privkey is required for remote endpoint"))?;
        let privkey = load_private_key_file(key_path)
            .with_context(|| format!("{}: Invalid private key file", key_path.display()))?;

        let crypto = Arc::new(security::default_provider());
        let mut verifier = PeerVerifier::new(&crypto);
        let peers_path = cli
            .peers
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("--peers required for remote endpoint"))?;
        add_peers_from_file(peers_path, &mut verifier)
            .with_context(|| format!("{}: Invalid peer file", peers_path.display()))?;

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

        tcp::connect_client(
            domain,
            addr,
            Arc::clone(verifier.as_ref().unwrap()),
            Arc::clone(privkey.as_ref().unwrap()),
        )
        .await
        .with_context(|| format!("Connection to src {addr} failed"))?
    };

    // Build dst client
    let dst_client = if dst_is_local {
        let dst_path = cli.dst_path.as_ref().unwrap();
        let server = RealizeServer::for_dir(&dir_id, dst_path);
        server.as_inprocess_client()
    } else {
        let addr = cli.dst_addr.as_ref().unwrap();
        let domain = addr.split(':').next().unwrap();

        tcp::connect_client(
            domain,
            addr,
            Arc::clone(verifier.as_ref().unwrap()),
            Arc::clone(privkey.as_ref().unwrap()),
        )
        .await
        .with_context(|| format!("Connection to dst {addr} failed"))?
    };

    // Move files
    let mut progress = CliProgress::new(cli.quiet);
    METRIC_UP.inc();
    let result = realize::algo::move_files(&src_client, &dst_client, dir_id, &mut progress).await;
    progress.finish_and_clear();
    let (success, error) = result?;

    if error > 0 {
        return Err(anyhow::anyhow!(
            "{error} file(s) failed, and {success} files(s) moved"
        ));
    }
    if !cli.quiet {
        println!(
            "{} {} file(s) moved",
            style("SUCCESS").for_stdout().green().bold(),
            success
        );
    }

    Ok(())
}

/// Print a warning message to stderr, with standard format.
fn print_warning(msg: &str) {
    eprintln!("{}: {}", style("WARNING").for_stderr().red(), msg);
}

/// Print an error message to stderr, with standard format.
fn print_error(msg: &str) {
    eprintln!("{}: {}", style("ERROR").for_stderr().red().bold(), msg);
}

struct CliProgress {
    multi: MultiProgress,
    total_files: usize,
    total_bytes: u64,
    overall_pb: ProgressBar,
    next_file_index: Arc<AtomicUsize>,
    quiet: bool,
}

impl CliProgress {
    fn new(quiet: bool) -> Self {
        let multi = MultiProgress::with_draw_target(if quiet {
            ProgressDrawTarget::hidden()
        } else {
            ProgressDrawTarget::stdout()
        });
        let overall_pb = multi.add(ProgressBar::no_length());
        overall_pb.set_style(
            ProgressStyle::with_template(
                "{prefix:<9.cyan.bold} [{wide_bar:.cyan/blue}] {bytes_per_sec} ({bytes}/{total_bytes}) {percent}%",
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
        self.overall_pb.set_length(total_bytes);
        self.overall_pb.set_position(0);
        self.overall_pb.set_prefix("Moving");
    }

    fn for_file(&self, path: &std::path::Path, bytes: u64) -> Box<dyn AlgoFileProgress> {
        Box::new(CliFileProgress {
            bar: None,
            next_file_index: self.next_file_index.clone(),
            total_files: self.total_files,
            bytes,
            path: path.display().to_string(),
            multi: self.multi.clone(),
            overall_pb: self.overall_pb.clone(),
            quiet: self.quiet,
        })
    }
}

struct CliFileProgress {
    bar: Option<(usize, ProgressBar)>,
    next_file_index: Arc<AtomicUsize>,
    total_files: usize,
    bytes: u64,
    path: String,
    multi: MultiProgress,
    overall_pb: ProgressBar,
    quiet: bool,
}

impl CliFileProgress {
    fn get_or_create_bar(&mut self) -> &mut ProgressBar {
        let (_, pb) = self.bar.get_or_insert_with(|| {
            let file_index = self.next_file_index.fetch_add(1, Ordering::Relaxed);
            let pb = self.multi.insert_from_back(1, ProgressBar::new(self.bytes));
            let tag = format!("[{}/{}]", file_index, self.total_files);
            pb.set_message(self.path.clone());
            pb.set_style(
                ProgressStyle::with_template(
                    "{prefix:<9.cyan.bold} {tag} {wide_msg} ({bytes}/{total_bytes}) {percent}%",
                )
                .unwrap()
                .progress_chars("=> ")
                .with_key(
                    "tag",
                    move |_state: &indicatif::ProgressState, w: &mut dyn std::fmt::Write| {
                        let _ = w.write_str(&tag);
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
                        "{:<9} [{}/{}] {}",
                        style("Moved").for_stdout().green().bold(),
                        index,
                        self.total_files,
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
                "{:<9} {}{}: {}",
                style("ERROR").for_stderr().red().bold(),
                tag,
                self.path,
                err,
            );
        });
    }
}

fn load_private_key_file(path: &Path) -> anyhow::Result<Arc<dyn SigningKey>> {
    let key = PrivateKeyDer::from_pem_file(path)?;

    Ok(security::default_provider()
        .key_provider
        .load_private_key(key)?)
}

fn add_peers_from_file(path: &Path, verifier: &mut PeerVerifier) -> anyhow::Result<()> {
    let mut got_peer = false;

    for spki in SubjectPublicKeyInfoDer::pem_file_iter(path)? {
        verifier.add_peer(spki?);
        got_peer = true;
    }

    if !got_peer {
        anyhow::bail!("No PEM-encoded public key found");
    }

    Ok(())
}
