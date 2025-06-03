//! Daemon binary for the realized server

use anyhow::Context as _;
use clap::Parser;
use futures_util::stream::StreamExt as _;
use prometheus::{register_int_counter, IntCounter};
use realize_lib::model::{Arena, LocalArena, Peer};
use realize_lib::network::config::PeerConfig;
use realize_lib::network::rpc::realize::metrics;
use realize_lib::network::security::{self, PeerVerifier};
use realize_lib::network::tcp::{self, HostPort};
use realize_lib::storage::config::ArenaConfig;
use realize_lib::storage::real::LocalStorage;
use realize_lib::utils::logging;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::PrivateKeyDer;
use rustls::sign::SigningKey;
use serde::Deserialize;
use signal_hook_tokio::Signals;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
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

/// Config file structure
#[derive(Debug, Deserialize)]
struct Config {
    arenas: HashMap<Arena, ArenaConfig>,
    peers: HashMap<Peer, PeerConfig>,
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

    // LocalArena access checks (see spec/future.md #daemonaccess)
    check_directory_access(&config.arenas)?;

    // Build directory list
    let mut dirs = Vec::new();
    for (arena, config) in &config.arenas {
        dirs.push(LocalArena::new(arena, &config.path));
    }
    let dirs = LocalStorage::new(dirs);

    let verifier = PeerVerifier::from_config(&config.peers)?;
    let privkey = load_private_key_file(&cli.privkey)
        .with_context(|| format!("{}: Failed to parse private key", cli.privkey.display()))?;

    if let Some(addr) = &cli.metrics_addr {
        metrics::export_metrics(addr)
            .await
            .with_context(|| format!("Failed to export metrics on {addr}"))?;
        log::info!("Metrics available on http://{addr}/metrics");
    }

    let hostport = HostPort::parse(&cli.address)
        .await
        .with_context(|| format!("Failed to parse --address {}", cli.address))?;
    log::debug!("Starting server on {}/{:?}...", hostport, hostport.addr());
    let (addr, shutdown) = tcp::start_server(&hostport, dirs, verifier, privkey)
        .await
        .with_context(|| format!("Failed to start server on {}", hostport))?;

    let mut signals = Signals::new(&[
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
    shutdown.send(())?;

    shutdown.closed().await;

    Ok(())
}

/// Checks that all directories in the config file are accessible.
fn check_directory_access(arenas: &HashMap<Arena, ArenaConfig>) -> anyhow::Result<()> {
    for (arena, config) in arenas {
        let path = &config.path;
        log::debug!("Checking directory {}: {}", arena, config.path.display());
        if !path.exists() {
            anyhow::bail!(
                "LocalArena '{}' (id: {}) does not exist",
                path.display(),
                arena
            );
        }
        if !fs::metadata(path)
            .and_then(|m| Ok(m.is_dir()))
            .unwrap_or(false)
        {
            anyhow::bail!(
                "Path '{}' (id: {}) is not a directory",
                path.display(),
                arena
            );
        }

        // Check read access
        if fs::read_dir(path).is_err() {
            anyhow::bail!(
                "No read access to directory '{}' (id: {})",
                path.display(),
                arena
            );
        }

        // Check write access (warn only)
        let testfile = path.join(".realize_write_test");
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&testfile)
        {
            Ok(_) => {
                let _ = fs::remove_file(&testfile);
            }
            Err(_) => {
                log::warn!(
                    "No write access to directory '{}' (id: {})",
                    path.display(),
                    arena
                );
            }
        }
    }

    Ok(())
}

fn parse_config(path: &Path) -> anyhow::Result<Config> {
    let content = fs::read_to_string(path)?;

    Ok(toml::from_str(&content)?)
}

fn load_private_key_file(path: &Path) -> anyhow::Result<Arc<dyn SigningKey>> {
    let key = PrivateKeyDer::from_pem_file(path)?;

    Ok(security::default_provider()
        .key_provider
        .load_private_key(key)?)
}
