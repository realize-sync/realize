//! Daemon binary for the realized server
//! Implements the server as described in spec/design.md and spec/future.md

use clap::Parser;
use prometheus::{Encoder, IntCounter, TextEncoder, register_int_counter};
use realize::server::{Directory, RealizeServer};
use realize::transport::security::{self, PeerVerifier};
use realize::transport::tcp;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{PrivateKeyDer, SubjectPublicKeyInfoDer};
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

/// Command-line arguments for the realized daemon
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
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
    // Init env_logger (log nothing unless env variable is set)
    env_logger::init();

    // Parse command-line arguments
    let args = Args::parse();

    // Parse YAML config file
    let config_content = match fs::read_to_string(&args.config) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to read config file: {e}");
            std::process::exit(1);
        }
    };
    let config: Config = match serde_yaml::from_str(&config_content) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Failed to parse config file: {e}");
            std::process::exit(1);
        }
    };

    // Build directory list
    let mut dirs = Vec::new();
    for dir in &config.dirs {
        for (id, path) in &dir.map {
            dirs.push(Directory::new(&id.as_str().into(), path.as_ref()));
        }
    }
    let server = RealizeServer::new(dirs);

    // Build PeerVerifier
    let crypto = Arc::new(security::default_provider());
    let mut verifier = PeerVerifier::new(&crypto);
    for peer in &config.peers {
        for (peer_id, pubkey_pem) in &peer.map {
            match SubjectPublicKeyInfoDer::from_pem_slice(pubkey_pem.as_bytes()) {
                Ok(spki) => verifier.add_peer_with_id(spki, peer_id),
                Err(e) => {
                    eprintln!("Failed to parse peer public key: {e}");
                    std::process::exit(1);
                }
            }
        }
    }
    let verifier = Arc::new(verifier);

    // Load private key
    let privkey = match PrivateKeyDer::from_pem_file(&args.privkey) {
        Ok(key) => key,
        Err(e) => {
            eprintln!("Failed to parse private key: {e}");
            std::process::exit(1);
        }
    };
    let privkey = match crypto.key_provider.load_private_key(privkey) {
        Ok(key) => key,
        Err(e) => {
            eprintln!("Failed to load private key: {e}");
            std::process::exit(1);
        }
    };

    export_metrics(args.metrics_addr);

    // Start the server (tokio runtime)
    match tcp::start_server(&args.address, server, verifier, privkey).await {
        Err(err) => {
            eprintln!("Failed to start server: {err}");
            std::process::exit(1);
        }
        Ok((addr, handle)) => {
            println!("Listening on {addr}");
            METRIC_UP.inc();

            if let Err(err) = handle.join().await {
                eprintln!("Server stopped: {err}");
                std::process::exit(1);
            }
            std::process::exit(0);
        }
    }
}

fn export_metrics(metrics_addr: Option<String>) {
    if let Some(metrics_addr) = &metrics_addr {
        let metrics_addr = metrics_addr.to_string();
        thread::spawn(move || {
            log::info!("[metrics] server listening on {}", metrics_addr);
            rouille::start_server(metrics_addr, move |request| {
                if request.url() == "/metrics" {
                    handle_metrics_request().unwrap_or_else(|_| {
                        rouille::Response::text("Internal error").with_status_code(500)
                    })
                } else {
                    rouille::Response::empty_404()
                }
            });
        });
    }
}

fn handle_metrics_request() -> anyhow::Result<rouille::Response> {
    let metrics = prometheus::gather();
    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    let content_type = encoder.format_type().to_string();
    encoder.encode(&metrics, &mut buffer)?;

    Ok(rouille::Response::from_data(content_type, buffer))
}
