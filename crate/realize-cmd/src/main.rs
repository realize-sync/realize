use anyhow::Context as _;
use async_speed_limit::Limiter;
use async_speed_limit::clock::StandardClock;
use clap::Parser;
use console::style;
use indicatif::HumanBytes;
use progress::CliProgress;
use prometheus::{IntCounter, register_int_counter};
use realize_lib::algo::MoveFileError;
use realize_lib::metrics;
use realize_lib::model::service::DirectoryId;
use realize_lib::transport::security::{self, PeerVerifier};
use realize_lib::transport::tcp::{self, HostPort, TcpRealizeServiceClient};
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{PrivateKeyDer, SubjectPublicKeyInfoDer};
use rustls::sign::SigningKey;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;
use std::time::Instant;
use tarpc::client::RpcError;
use tarpc::context;
use tokio::sync::mpsc;

mod progress;
mod push;

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

    let mut status = 0;
    if let Err(err) = execute(&cli).await {
        print_error(&format!("{err:#}"));
        status = 1;
    };
    if let Some(pushgw) = &cli.metrics_pushgateway {
        if let Err(err) =
            push::push_metrics(pushgw, &cli.metrics_job, cli.metrics_instance.as_deref()).await
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

    let mut ctx = context::current();
    ctx.deadline = std::time::Instant::now()
        + cli
            .max_duration
            .map(|d| d.into())
            .unwrap_or_else(|| std::time::Duration::from_secs(24 * 60 * 60));

    let privkey = load_private_key_file(&cli.privkey)
        .with_context(|| format!("{}: Invalid private key file", cli.privkey.display()))?;

    let verifier = build_peer_verifier(&cli.peers)
        .with_context(|| format!("{}: Invalid peer file", cli.peers.display()))?;

    let (src_client, dst_client) = tokio::join!(
        connect(
            "--src-addr",
            &cli.src_addr,
            None,
            privkey.clone(),
            verifier.clone(),
        ),
        connect(
            "--dst-addr",
            &cli.dst_addr,
            cli.throttle_up,
            privkey,
            verifier,
        )
    );

    let src_client = src_client?;
    let dst_client = dst_client?;

    if let Some(limit) = cli.throttle_down {
        if let Some(val) = configure_limit(&src_client, limit)
            .await
            .with_context(|| format!("Failed to apply --throttle-down={limit}"))?
        {
            log::info!("Throttling downloads: {}/s", HumanBytes(val));
        }
    }

    METRIC_UP.inc();

    let mut cli_progress = CliProgress::new(cli.quiet, cli.directory_ids.len());
    let (progress_tx, progress_rx) = mpsc::channel(32);

    let (res, _) = tokio::join!(
        async move {
            let mut total_success = 0;
            let mut total_error = 0;
            let mut total_interrupted = 0;
            let mut interrupted = false;
            for dir_id in &cli.directory_ids {
                let result = realize_lib::algo::move_files(
                    ctx,
                    &src_client,
                    &dst_client,
                    DirectoryId::from(dir_id.to_string()),
                    Some(progress_tx.clone()),
                )
                .await;
                match result {
                    Ok((success, error, interrupted_count)) => {
                        total_success += success;
                        total_error += error;
                        total_interrupted += interrupted_count;
                    }
                    Err(MoveFileError::Rpc(RpcError::DeadlineExceeded)) => {
                        interrupted = true;
                        break;
                    }
                    Err(err) => {
                        return Err(anyhow::Error::from(err));
                    }
                }
            }
            Ok((total_success, total_error, total_interrupted, interrupted))
        },
        cli_progress.update(progress_rx),
    );
    cli_progress.finish_and_clear();

    let (total_success, total_error, total_interrupted, interrupted) = res?;
    if total_error > 0 {
        return Err(anyhow::anyhow!(
            "{total_error} file(s) failed, {total_success} file(s) moved, {total_interrupted} interrupted"
        ));
    }
    if interrupted || ctx.deadline <= Instant::now() {
        eprintln!(
            "{} {} file(s) moved, {} interrupted",
            style("INTERRUPTED").for_stderr().red(),
            total_success,
            total_interrupted
        );
        process::exit(11);
    }
    if !cli.quiet {
        println!(
            "{} {} file(s) moved{}",
            style("SUCCESS").for_stdout().green().bold(),
            total_success,
            if total_interrupted > 0 {
                format!(", {} interrupted", total_interrupted)
            } else {
                String::new()
            }
        );
    }

    Ok(())
}

async fn connect(
    argument: &str,
    addr: &str,
    limit: Option<u64>,
    privkey: Arc<dyn SigningKey>,
    verifier: Arc<PeerVerifier>,
) -> anyhow::Result<realize_lib::model::service::RealizeServiceClient<tcp::TcpStub>, anyhow::Error>
{
    let addr = HostPort::parse(addr)
        .await
        .with_context(|| format!("Failed to resolve {} {}", argument, addr))?;
    let mut options = tcp::ClientOptions::default();
    if let Some(limit) = limit {
        log::info!("Throttling uploads: {}/s", HumanBytes(limit));
        options.limiter = Some(Limiter::<StandardClock>::new(limit as f64));
    }

    Ok(
        tcp::connect_client(&addr, Arc::clone(&verifier), Arc::clone(&privkey), options)
            .await
            .with_context(|| format!("Connection to {argument} {addr} failed"))?,
    )
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
            context::current(),
            realize_lib::model::service::Config {
                write_limit: Some(limit),
            },
        )
        .await??;

    Ok(config.write_limit)
}

/// Print a warning message to stderr, with standard format.
fn print_warning(msg: &str) {
    eprintln!("{}: {}", style("WARNING").for_stderr().red(), msg);
}

/// Print an error message to stderr, with standard format.
fn print_error(msg: &str) {
    eprintln!("{}: {}", style("ERROR").for_stderr().red().bold(), msg);
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
