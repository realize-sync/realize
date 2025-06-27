use anyhow::Context as _;
use async_speed_limit::Limiter;
use async_speed_limit::clock::StandardClock;
use clap::{Parser, ValueEnum};
use console::style;
use futures_util::stream::StreamExt as _;
use indicatif::HumanBytes;
use progress::CliProgress;
use prometheus::{IntCounter, register_int_counter};
use realize_lib::logic::consensus::movedirs::MoveFileError;
use realize_lib::model::{Arena, Peer};
use realize_lib::network::Networking;
use realize_lib::network::config::PeerConfig;
use realize_lib::network::rpc::realstore;
use realize_lib::network::rpc::realstore::client::{ClientConnectionState, ClientOptions};
use realize_lib::network::rpc::realstore::metrics;
use realize_lib::utils::logging;
use signal_hook_tokio::Signals;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;
use std::{fs, process};
use tarpc::client::RpcError;
use tarpc::context;
use tokio::sync::mpsc;

mod progress;
mod push;

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum OutputMode {
    /// Print only error messages and warnings to stderr.
    Quiet,

    /// Print progress bar and summary  to stdout, and error messages and warnings to stderr.
    Progress,

    /// Disable progress and printing of errors, just log.
    ///
    /// Set RUST_LOG to configure what gets included.
    Log,
}

lazy_static::lazy_static! {
    static ref METRIC_UP: IntCounter =
        register_int_counter!("realize_cmd_up", "Command is up").unwrap();
}

/// Move files between Realize RPC server instances.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about=None)]
struct Cli {
    /// Name of the source peer.
    ///
    /// The peer name and address must be listed in the file passed to
    /// --config.
    #[arg(long, required = false)]
    src: String,

    /// Name of the destination peer.
    ///
    /// The peer name and address must be listed in the file passed to
    /// --config.
    #[arg(long, required = false)]
    dst: String,

    /// Path to the private key used to identify this command to the
    /// RPC servers.
    ///
    /// The server needs to be configured with the equivalent public
    /// key for the connection to succeed.
    #[arg(long, required = true)]
    privkey: PathBuf,

    /// Path to a TOML config file defining the peers.
    ///
    /// The command only connects to known servers, whose public key
    /// is listed in this file.
    #[arg(long, required = true)]
    config: PathBuf,

    /// IDs of the directories to process.
    ///
    /// The directory ID must be configured on both the source and
    /// destination RPC servers.
    #[arg(value_name = "ID", required = true, num_args = 1..)]
    directory_ids: Vec<String>,

    /// Output mode.
    ///
    /// Logging can be further configured by setting the env var
    /// RUST_LOG. For a systemd-friendly output format, set the env
    /// var RUST_LOG_FORMAT=SYSTEMD
    #[arg(long, value_enum, default_value = "progress", verbatim_doc_comment)]
    output: OutputMode,

    /// Maximum total duration for the operation (e.g. "5m", "30s").
    ///
    /// Once exceeded, file transfers time out and the process exits
    /// with status code 11.
    #[arg(long, default_value = "24h")]
    max_duration: humantime::Duration,

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

    /// Throttle download (reading from src) in bytes/sec.
    #[arg(long, required = false, value_parser = |s: &str| parse_bytes(s))]
    throttle_down: Option<u64>,

    /// Throttle uploads (writing to dst) in bytes/sec.
    #[arg(long, required = false, value_parser = |s: &str| parse_bytes(s))]
    throttle_up: Option<u64>,
}

/// Config file structure
#[derive(Debug, serde::Deserialize)]
struct Config {
    peers: HashMap<Peer, PeerConfig>,
}

/// Parse byte arguments
fn parse_bytes(str: &str) -> Result<u64, parse_size::Error> {
    parse_size::Config::new().with_binary().parse_size(str)
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let output_mode = cli.output;

    if output_mode == OutputMode::Log {
        logging::init_with_info_modules(vec!["realize_cmd", "realize_cmd::progress"]);
    } else {
        logging::init(log::LevelFilter::Off);
    }

    let mut status = match execute(&cli).await {
        Ok(code) => code,
        Err(err) => {
            print_error(output_mode, &format!("{err:#}"));

            1
        }
    };
    if let Some(pushgw) = &cli.metrics_pushgateway {
        if let Err(err) =
            push::push_metrics(pushgw, &cli.metrics_job, cli.metrics_instance.as_deref()).await
        {
            print_warning(
                output_mode,
                &format!("Failed to push metrics to {pushgw}: {err}"),
            );
            if status == 0 {
                status = 12;
            }
        }
    }
    process::exit(status);
}

async fn execute(cli: &Cli) -> anyhow::Result<i32> {
    METRIC_UP.reset(); // Set it to 0, so it's available
    if let Some(addr) = &cli.metrics_addr {
        metrics::export_metrics(addr)
            .await
            .with_context(|| format!("Failed to export metrics on {addr}"))?;
    }

    let mut ctx = context::current();
    ctx.deadline = std::time::Instant::now() + cli.max_duration.into();

    let config = parse_config(&cli.config)
        .with_context(|| format!("{}: failed to read TOML config file", cli.config.display()))?;
    let networking = Networking::from_config(&config.peers, &cli.privkey)?;

    let mut signals = Signals::new([
        signal_hook::consts::SIGHUP,
        signal_hook::consts::SIGTERM,
        signal_hook::consts::SIGINT,
        signal_hook::consts::SIGQUIT,
    ])?;

    let mut cli_progress =
        CliProgress::new(cli.output != OutputMode::Progress, cli.directory_ids.len());
    let output_mode = cli.output;

    tokio::select!(
        ret = run_with_progress(cli, ctx, networking, &mut cli_progress) => {
        ret.map(|_| 0)
    }
    _ = signals.next() => {
        cli_progress.finish();
        signals.handle().close();
        print_warning(output_mode, "Interrupted");

        Ok(20)
    })
}

async fn run_with_progress(
    cli: &Cli,
    ctx: context::Context,
    networking: Networking,
    cli_progress: &mut CliProgress,
) -> anyhow::Result<()> {
    let (progress_tx, progress_rx) = mpsc::channel(32);
    let (src_watch_tx, src_watch_rx) =
        tokio::sync::watch::channel(ClientConnectionState::NotConnected);
    let (dst_watch_tx, dst_watch_rx) =
        tokio::sync::watch::channel(ClientConnectionState::NotConnected);
    let (_, res) = tokio::join!(
        cli_progress.update(progress_rx, src_watch_rx, dst_watch_rx),
        async move {
            let (src_client, dst_client) = tokio::join!(
                connect(&networking, &cli.src, None, src_watch_tx),
                connect(&networking, &cli.dst, cli.throttle_up, dst_watch_tx)
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

            let mut total_success = 0;
            let mut total_error = 0;
            let mut total_interrupted = 0;
            let mut interrupted = false;
            let result = realize_lib::logic::consensus::movedirs::move_dirs(
                ctx,
                &src_client,
                &dst_client,
                cli.directory_ids.iter().map(|s| Arena::from(s.to_string())),
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
                }
                Err(err) => {
                    return Err(anyhow::Error::from(err));
                }
            }
            Ok((total_success, total_error, total_interrupted, interrupted))
        },
    );
    cli_progress.finish_and_clear();
    let (total_success, total_error, total_interrupted, interrupted) = res?;
    if total_error > 0 {
        log::error!(
            "{total_error} file(s) failed, {total_success} file(s) moved, {total_interrupted} interrupted"
        );
        match cli.output {
            OutputMode::Log => {}
            OutputMode::Quiet | OutputMode::Progress => {
                eprintln!(
                    "{} {} file(s) failed, {} file(s) moved, {} interrupted",
                    style("ERROR").for_stderr().red().bold(),
                    total_error,
                    total_success,
                    total_interrupted
                );
            }
        }
        return Err(anyhow::anyhow!(
            "{total_error} file(s) failed, {total_success} file(s) moved, {total_interrupted} interrupted"
        ));
    }
    if interrupted || ctx.deadline <= Instant::now() {
        log::warn!(
            "Deadline exceeded ({}): {} file(s) moved, {} interrupted",
            cli.max_duration,
            total_success,
            total_interrupted
        );
        match cli.output {
            OutputMode::Log => {}
            OutputMode::Quiet | OutputMode::Progress => {
                eprintln!(
                    "{} {} file(s) moved, {} interrupted",
                    style("INTERRUPTED").for_stderr().red(),
                    total_success,
                    total_interrupted
                );
            }
        }
        process::exit(11);
    }
    log::info!(
        "SUCCESS {} file(s) moved{}",
        total_success,
        if total_interrupted > 0 {
            format!(", {total_interrupted} interrupted")
        } else {
            String::new()
        }
    );
    if cli.output == OutputMode::Progress {
        println!(
            "{} {} file(s) moved{}",
            style("SUCCESS").for_stdout().green().bold(),
            total_success,
            if total_interrupted > 0 {
                format!(", {total_interrupted} interrupted")
            } else {
                String::new()
            }
        );
    }

    Ok(())
}

async fn connect(
    networking: &Networking,
    peer: &str,
    limit: Option<u64>,
    conn_status: tokio::sync::watch::Sender<ClientConnectionState>,
) -> anyhow::Result<realstore::client::RealStoreClient, anyhow::Error> {
    let mut options = ClientOptions::default();
    if let Some(limit) = limit {
        log::info!("Throttling uploads: {}/s", HumanBytes(limit));
        options.limiter = Some(Limiter::<StandardClock>::new(limit as f64));
    }
    options.connection_events = Some(conn_status);

    realstore::client::connect(networking, &Peer::from(peer), options).await
}

/// Set server-site write rate limit on client, return it.
///
/// Not all servers support setting rate limit; It's not an error if
/// this function returns None.
async fn configure_limit(
    client: &realstore::client::RealStoreClient,
    limit: u64,
) -> anyhow::Result<Option<u64>> {
    let config = client
        .configure(
            context::current(),
            realize_lib::network::rpc::realstore::Config {
                write_limit: Some(limit),
            },
        )
        .await??;

    Ok(config.write_limit)
}

/// Print a warning message to stderr, with standard format.
fn print_warning(mode: OutputMode, msg: &str) {
    log::warn!("{msg}");
    match mode {
        OutputMode::Log => {}
        OutputMode::Quiet | OutputMode::Progress => {
            eprintln!("{}: {}", style("WARNING").for_stderr().red(), msg);
        }
    }
}

/// Print an error message to stderr, with standard format.
fn print_error(mode: OutputMode, msg: &str) {
    log::error!("{msg}");
    match mode {
        OutputMode::Log => {}
        OutputMode::Quiet | OutputMode::Progress => {
            eprintln!("{}: {}", style("ERROR").for_stderr().red().bold(), msg);
        }
    }
}

fn parse_config(path: &Path) -> anyhow::Result<Config> {
    let content = fs::read_to_string(path)?;

    Ok(toml::from_str(&content)?)
}
