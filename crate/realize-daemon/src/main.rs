//! Daemon binary for the realized server

use anyhow::Context as _;
use clap::Parser;
use futures_util::stream::StreamExt as _;
use prometheus::{IntCounter, register_int_counter};
use realize_lib::fs::nfs;
use realize_lib::logic::config::Config;
use realize_lib::logic::setup::Setup;
use realize_lib::network::hostport::HostPort;
use realize_lib::network::rpc::realstore::metrics;
use realize_lib::utils::logging;
use signal_hook_tokio::Signals;
use std::path::{Path, PathBuf};
use std::{fs, process};

/// Run the realize daemon in the foreground.
///
/// Exposes the realize RPC service at the given address. The RPC
/// service gives access to the directories configured in the TOML
/// configuration file to known peers. Stop it with SIGTERM.
///
/// By default, outputs errors and warnings to stderr. To configure
/// the output, set the env variable RUST_LOG. Set the env variable
/// RUST_LOG_FORMAT=SYSTEMD to a systemd-friendly log output.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about, verbatim_doc_comment)]
struct Cli {
    /// TCP address to listen on (default: localhost:9771)
    #[arg(long, default_value = "localhost:9771")]
    address: String,

    /// TCP address to export NFS on.
    #[arg(long)]
    nfs: Option<String>,

    /// Path to the PEM-encoded ED25519 private key file
    #[arg(long)]
    privkey: PathBuf,

    /// Path to the TOML configuration file
    #[arg(long)]
    config: PathBuf,

    /// Address to export prometheus metrics (host:port, optional)
    #[arg(long)]
    metrics_addr: Option<String>,
}

lazy_static::lazy_static! {
    static ref METRIC_UP: IntCounter =
        register_int_counter!("realize_daemon_up", "Server is up").unwrap();
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    logging::init_with_info_modules(vec!["realize_daemon"]);

    if let Err(err) = execute(cli).await {
        eprintln!("ERROR: {err:#}");
        process::exit(1);
    };
}

async fn execute(cli: Cli) -> anyhow::Result<()> {
    let config = parse_config(&cli.config)
        .with_context(|| format!("{}: failed to read TOML config file", cli.config.display()))?;

    let mut setup = Setup::new(config, &cli.privkey)?;

    if let Some(addr) = &cli.metrics_addr {
        metrics::export_metrics(addr)
            .await
            .with_context(|| format!("Failed to export metrics on {addr}"))?;
        log::info!("Metrics available on http://{addr}/metrics");
    }

    if let Some(addr) = &cli.nfs {
        let (cache, downloader) = setup.setup_cache()?;
        nfs::export(
            cache,
            downloader,
            HostPort::parse(addr)
                .await
                .with_context(|| format!("Failed to parse --nfs {addr}"))?
                .addr(),
        )
        .await?;
    }

    let hostport = HostPort::parse(&cli.address)
        .await
        .with_context(|| format!("Failed to parse --address {}", cli.address))?;
    log::debug!("Starting server on {}/{:?}...", hostport, hostport.addr());
    let server = setup.setup_server()?;

    let addr = server
        .listen(&hostport)
        .await
        .with_context(|| format!("Failed to start server on {hostport}"))?;

    let mut signals = Signals::new([
        signal_hook::consts::SIGHUP,
        signal_hook::consts::SIGTERM,
        signal_hook::consts::SIGINT,
        signal_hook::consts::SIGQUIT,
    ])?;

    METRIC_UP.inc();
    log::info!("Listening on {addr}");
    println!("Listening on {addr}");

    let _ = signals.next().await;

    log::info!("Interrupted. Shutting down..");
    signals.handle().close(); // A 2nd signal kills the process
    server.shutdown().await?;

    Ok(())
}

fn parse_config(path: &Path) -> anyhow::Result<Config> {
    let content = fs::read_to_string(path)?;

    Ok(toml::from_str(&content)?)
}
