use anyhow::Context as _;
use async_speed_limit::Limiter;
use async_speed_limit::clock::StandardClock;
use clap::Parser;
use console::style;
use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use prometheus::{IntCounter, register_int_counter};
use realize::algo::{FileProgress as AlgoFileProgress, MoveFileError, Progress};
use realize::metrics;
use realize::model::service::DirectoryId;
use realize::transport::security::{self, PeerVerifier};
use realize::transport::tcp::{self, TcpRealizeServiceClient};
use realize::utils::async_utils::AbortOnDrop;
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{PrivateKeyDer, SubjectPublicKeyInfoDer};
use rustls::sign::SigningKey;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

lazy_static::lazy_static! {
    static ref METRIC_UP: IntCounter =
        register_int_counter!("realize_cmd_up", "Command is up").unwrap();
}

/// Realize command-line tool
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Source remote address (host:port)
    #[arg(long, required = false)]
    src_addr: String,

    /// Destination remote address (host:port)
    #[arg(long, required = false)]
    dst_addr: String,

    /// Path to the private key
    #[arg(long, required = true)]
    privkey: PathBuf,

    /// Path to the PEM file with one or more peer public keys
    #[arg(long, required = true)]
    peers: PathBuf,

    /// IDs of the directories to process.
    ///
    /// Source and dest must support these directories.
    #[arg(value_name = "ID", required = true, num_args = 1..)]
    directory_ids: Vec<String>,

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
    #[cfg(feature = "push")]
    #[arg(long)]
    metrics_pushgateway: Option<String>,

    /// Job name for prometheus pushgateway (default: realize)
    #[arg(long, default_value = "realize")]
    metrics_job: String,

    /// Instance label for prometheus pushgateway (optional)
    #[arg(long)]
    metrics_instance: Option<String>,

    /// Throttle download (reading from src) in bytes/sec. Only applies to remote services.
    #[arg(long, required = false, value_parser = |s: &str| parse_bytes(s))]
    throttle_down: Option<u64>,

    /// Throttle uploads (writing to dst) in bytes/sec.  Only applies to remote services.
    #[arg(long, required = false, value_parser = |s: &str| parse_bytes(s))]
    throttle_up: Option<u64>,
}

/// Parse byte arguments
fn parse_bytes(str: &str) -> Result<u64, parse_size::Error> {
    parse_size::Config::new().with_binary().parse_size(str)
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
    #[cfg(feature = "push")]
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
    METRIC_UP.reset(); // Set it to 0, so it's available
    if let Some(addr) = &cli.metrics_addr {
        metrics::export_metrics(addr)
            .await
            .with_context(|| format!("Failed to export metrics on {addr}"))?;
    }

    let privkey = load_private_key_file(&cli.privkey)
        .with_context(|| format!("{}: Invalid private key file", cli.privkey.display()))?;

    let verifier = build_peer_verifier(&cli.peers)
        .with_context(|| format!("{}: Invalid peer file", cli.peers.display()))?;

    // Build src client
    let src_client = tcp::connect_client(
        &cli.src_addr,
        Arc::clone(&verifier),
        Arc::clone(&privkey),
        tcp::ClientOptions::default(),
    )
    .await
    .with_context(|| format!("Connection to src {} failed", cli.src_addr))?;

    // Apply throttle limits if set
    if let Some(limit) = cli.throttle_down {
        if let Some(val) = configure_limit(&src_client, limit)
            .await
            .with_context(|| format!("Failed to apply --throttle-down={limit}"))?
        {
            log::info!("Throttling downloads: {}/s", HumanBytes(val));
        }
    }

    // Build dst client
    let dst_client = {
        let addr = &cli.dst_addr;
        let mut options = tcp::ClientOptions::default();
        if let Some(limit) = cli.throttle_up {
            log::info!("Throttling uploads: {}/s", HumanBytes(limit));
            options.limiter = Some(Limiter::<StandardClock>::new(limit as f64));
        }
        tcp::connect_client(addr, Arc::clone(&verifier), Arc::clone(&privkey), options)
            .await
            .with_context(|| format!("Connection to dst {addr} failed"))?
    };

    METRIC_UP.inc();

    // Move files for each directory id
    let mut total_success = 0;
    let mut total_error = 0;
    let mut progress = CliProgress::new(cli.quiet);
    let should_show_dir = cli.directory_ids.len() >= 2;
    for dir_id in &cli.directory_ids {
        if should_show_dir {
            progress.set_path_prefix(format!("{}/", dir_id));
        }
        let (success, error) = realize::algo::move_files(
            &src_client,
            &dst_client,
            DirectoryId::from(dir_id.to_string()),
            &mut progress,
        )
        .await?;
        total_success += success;
        total_error += error;
    }
    progress.finish_and_clear();

    if total_error > 0 {
        return Err(anyhow::anyhow!(
            "{total_error} file(s) failed, and {total_success} files(s) moved"
        ));
    }
    if !cli.quiet {
        println!(
            "{} {} file(s) moved",
            style("SUCCESS").for_stdout().green().bold(),
            total_success
        );
    }

    Ok(())
}

/// Set server-site write rate limit on client, return it.
///
/// Not all servers support setting rate limit; It's not an error if
/// this function returns None.
async fn configure_limit(
    client: &TcpRealizeServiceClient,
    limit: u64,
) -> anyhow::Result<Option<u64>> {
    let config = client
        .configure(
            tarpc::context::current(),
            realize::model::service::Config {
                write_limit: Some(limit),
            },
        )
        .await??;

    Ok(config.write_limit)
}

/// Print a warning message to stderr, with standard format.
#[cfg(feature = "push")]
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
    path_prefix: String,
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
            path_prefix: "".to_string(),
        }
    }

    fn set_path_prefix(&mut self, prefix: String) {
        self.path_prefix = prefix;
    }

    fn finish_and_clear(&self) {
        self.overall_pb.finish_and_clear();
        let _ = self.multi.clear();
    }
}

impl Progress for CliProgress {
    fn set_length(&mut self, total_files: usize, total_bytes: u64) {
        self.total_files += total_files;
        self.total_bytes += total_bytes;
        self.overall_pb.set_length(self.total_bytes);
        self.overall_pb.set_position(0);
        self.overall_pb.set_prefix("Moving");
    }

    fn for_file(&self, path: &std::path::Path, bytes: u64) -> Box<dyn AlgoFileProgress> {
        Box::new(CliFileProgress {
            bar: None,
            next_file_index: self.next_file_index.clone(),
            total_files: self.total_files,
            bytes,
            path: format!("{}{}", self.path_prefix, path.display()),
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

fn build_peer_verifier(path: &Path) -> anyhow::Result<Arc<PeerVerifier>> {
    let crypto = Arc::new(security::default_provider());
    let mut verifier = PeerVerifier::new(&crypto);
    add_peers_from_file(path, &mut verifier)?;

    Ok(Arc::new(verifier))
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
