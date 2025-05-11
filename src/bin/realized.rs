//! Daemon binary for the realized server
//! Implements the server as described in spec/design.md and spec/future.md

use clap::Parser;
use realize::server::{Directory, RealizeServer};
use realize::transport::security::{self, PeerVerifier};
use realize::transport::tcp::RunningServer;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{PrivateKeyDer, SubjectPublicKeyInfoDer};
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

/// Command-line arguments for the realized daemon
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// TCP port to listen on (default: 9771)
    #[arg(long, default_value_t = 9771)]
    port: u16,

    /// Path to the PEM-encoded ED25519 private key file
    #[arg(long)]
    privkey: PathBuf,

    /// Path to the YAML configuration file
    #[arg(long)]
    config: PathBuf,
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
        for (_id, pubkey_pem) in &peer.map {
            match SubjectPublicKeyInfoDer::from_pem_slice(pubkey_pem.as_bytes()) {
                Ok(spki) => verifier.add_peer(&spki),
                Err(e) => {
                    eprintln!("Failed to parse peer public key: {e}");
                    std::process::exit(1);
                }
            }
        }
    }
    let verifier = Arc::new(verifier);

    // Load private key
    let privkey = match fs::read(&args.privkey) {
        Ok(bytes) => bytes,
        Err(e) => {
            eprintln!("Failed to read private key: {e}");
            std::process::exit(1);
        }
    };
    let privkey = match PrivateKeyDer::from_pem_slice(&privkey) {
        Ok(key) => key,
        Err(e) => {
            eprintln!("Failed to parse private key: {e}");
            std::process::exit(1);
        }
    };
    let privkey = match crypto.key_provider.load_private_key(privkey) {
        Ok(k) => Arc::from(k),
        Err(e) => {
            eprintln!("Failed to load private key: {e}");
            std::process::exit(1);
        }
    };

    // Start the server (tokio runtime)
    match RunningServer::bind(format!("0.0.0.0:{}", args.port), server, verifier, privkey).await {
        Err(err) => {
            eprintln!("Failed to start server: {err}");
            std::process::exit(1);
        }
        Ok(running) => {
            let addr = running.local_addr().unwrap();
            println!("Listening on {addr}");
            if let Err(err) = running.spawn().await {
                eprintln!("Server stopped: {err}");
                std::process::exit(1);
            }
            std::process::exit(0);
        }
    }
}
