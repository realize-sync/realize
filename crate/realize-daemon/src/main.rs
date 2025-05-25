//! Daemon binary for the realized server

use anyhow::Context as _;
use clap::Parser;
use prometheus::{IntCounter, register_int_counter};
use realize_lib::metrics;
use realize_lib::server::{Directory, DirectoryMap};
use realize_lib::transport::security::{self, PeerVerifier};
use realize_lib::transport::tcp::{self, HostPort};
use realize_lib::utils::logging;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{PrivateKeyDer, SubjectPublicKeyInfoDer};
use rustls::sign::SigningKey;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, process};

/// Run the realize daemon in the foreground.
///
/// Exposes the realize RPC service at the given address. The RPC
/// service gives access to the directories configured in the YAML
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

    /// Path to the YAML configuration file
    #[arg(long)]
    config: PathBuf,

    /// Address to export prometheus metrics (host:port, optional)
    #[arg(long)]
    metrics_addr: Option<String>,
}

/// Config file structure (stub, to be expanded)
#[derive(Debug, Deserialize)]
struct Config {
    dirs: Vec<DirEntry>,
    peers: Vec<PeerEntry>,
}

#[derive(Debug, Deserialize)]
struct DirEntry {
    #[serde(flatten)]
    map: std::collections::HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct PeerEntry {
    #[serde(flatten)]
    map: std::collections::HashMap<String, String>,
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
    // Use Result<!> as return type once it's available in stable.
    unreachable!("execute should never execute successfully");
}

async fn execute(cli: Cli) -> anyhow::Result<()> {
    let config = parse_config(&cli.config)
        .with_context(|| format!("{}: failed to read YAML config file", cli.config.display()))?;

    // Directory access checks (see spec/future.md #daemonaccess)
    check_directory_access(&config.dirs)?;

    // Build directory list
    let mut dirs = Vec::new();
    for dir in &config.dirs {
        for (id, path) in &dir.map {
            dirs.push(Directory::new(&id.as_str().into(), path.as_ref()));
        }
    }
    let dirs = DirectoryMap::new(dirs);

    // Build PeerVerifier
    let crypto = Arc::new(security::default_provider());
    let mut verifier = PeerVerifier::new(&crypto);
    for peer in &config.peers {
        for (peer_id, pubkey_pem) in &peer.map {
            let spki = SubjectPublicKeyInfoDer::from_pem_slice(pubkey_pem.as_bytes())
                .with_context(|| "Failed to parse public key for peer {peer_id}")?;
            verifier.add_peer_with_id(spki, peer_id);
        }
    }
    let verifier = Arc::new(verifier);

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
    let (addr, handle) = tcp::start_server(&hostport, dirs, verifier, privkey)
        .await
        .with_context(|| format!("Failed to start server on {}", hostport))?;

    METRIC_UP.inc();
    println!("Listening on {addr}");
    log::info!("Listening on {addr}");

    handle.join().await?;

    Err(anyhow::anyhow!("Server shut down"))
}

/// Checks that all directories in the config file are accessible.
fn check_directory_access(dirs: &Vec<DirEntry>) -> anyhow::Result<()> {
    for dir in dirs {
        for (id, path) in &dir.map {
            log::debug!("Checking directory {}: {}", id, path);
            let path = std::path::Path::new(path);
            if !path.exists() {
                anyhow::bail!("Directory '{}' (id: {}) does not exist", path.display(), id);
            }
            if !fs::metadata(path)
                .and_then(|m| Ok(m.is_dir()))
                .unwrap_or(false)
            {
                anyhow::bail!("Path '{}' (id: {}) is not a directory", path.display(), id);
            }

            // Check read access
            if fs::read_dir(path).is_err() {
                anyhow::bail!(
                    "No read access to directory '{}' (id: {})",
                    path.display(),
                    id
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
                        id
                    );
                }
            }
        }
    }

    Ok(())
}

fn parse_config(path: &Path) -> anyhow::Result<Config> {
    let content = fs::read_to_string(path)?;
    let config = serde_yaml::from_str(&content)?;

    Ok(config)
}

fn load_private_key_file(path: &Path) -> anyhow::Result<Arc<dyn SigningKey>> {
    let key = PrivateKeyDer::from_pem_file(path)?;

    Ok(security::default_provider()
        .key_provider
        .load_private_key(key)?)
}
