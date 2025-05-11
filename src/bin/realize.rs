use clap::Parser;
use realize::model::service::DirectoryId;
use realize::server::RealizeServer;
use realize::transport::security::{self, PeerVerifier};
use realize::transport::tcp;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{PrivateKeyDer, SubjectPublicKeyInfoDer};
use std::process;
use std::sync::Arc;

/// Realize command-line tool
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Address of the RealizeService to connect to
    #[arg(long)]
    address: String,

    /// Path to the private key
    #[arg(long)]
    private_key: String,

    /// Server public key
    #[arg(long)]
    server_public_key: String,

    /// Directory to operate on
    #[arg(long)]
    directory: String,

    /// Directory id
    #[arg(long)]
    directory_id: String,

    /// Throttle upload and download (e.g. 1M)
    #[arg(long)]
    throttle: Option<String>,

    /// Throttle upload (e.g. 1M)
    #[arg(long, alias = "throttle-up")]
    throttle_up: Option<String>,

    /// Throttle download (e.g. 512k)
    #[arg(long, alias = "throttle-down")]
    throttle_down: Option<String>,
}

#[tokio::main]
async fn main() {
    // Only log if env variable is set
    let _ = env_logger::init();

    let cli = Cli::parse();

    // Parse private key
    let privkey_bytes = match std::fs::read(&cli.private_key) {
        Ok(bytes) => bytes,
        Err(e) => {
            eprintln!("Failed to read private key: {e}");
            process::exit(1);
        }
    };
    let privkey = match PrivateKeyDer::from_pem_slice(&privkey_bytes) {
        Ok(key) => key,
        Err(e) => {
            eprintln!("Failed to parse private key: {e}");
            process::exit(1);
        }
    };
    let crypto = Arc::new(security::default_provider());
    let privkey = match crypto.key_provider.load_private_key(privkey) {
        Ok(k) => Arc::from(k),
        Err(e) => {
            eprintln!("Failed to load private key: {e}");
            process::exit(1);
        }
    };

    // Parse server public key
    let server_pubkey_pem = match std::fs::read_to_string(&cli.server_public_key) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to read server public key file: {e}");
            process::exit(1);
        }
    };
    let server_pubkey = match SubjectPublicKeyInfoDer::from_pem_slice(server_pubkey_pem.as_bytes())
    {
        Ok(spki) => spki,
        Err(e) => {
            eprintln!("Failed to parse server public key: {e:?}");
            process::exit(1);
        }
    };
    let mut verifier = PeerVerifier::new(&crypto);
    verifier.add_peer(&server_pubkey);
    let verifier = Arc::new(verifier);

    let domain = cli
        .address
        .split(':')
        .next()
        .expect("Invalid address format");

    // Connect to remote RealizeService
    let remote_client = match tcp::connect_client(
        domain,
        &cli.address,
        Arc::clone(&verifier),
        Arc::clone(&privkey),
    )
    .await
    {
        Ok(client) => client,
        Err(e) => {
            eprintln!("Failed to connect to remote RealizeService: {e}");
            process::exit(1);
        }
    };

    // Start in-process RealizeService for local directory
    let dir_id = DirectoryId::from(cli.directory_id.clone());
    let local_server = RealizeServer::for_dir(&dir_id, cli.directory.as_ref());
    let local_client = local_server.as_inprocess_client();

    // TODO: Set throttle limits via RPC if implemented
    if cli.throttle.is_some() || cli.throttle_up.is_some() || cli.throttle_down.is_some() {
        eprintln!("Throttle options are not yet implemented");
    }

    // Move files
    match realize::algo::move_files(
        tarpc::context::current(),
        &local_client,
        &remote_client,
        dir_id,
    )
    .await
    {
        Ok(_) => process::exit(0),
        Err(e) => {
            eprintln!("Error: {e}");
            process::exit(1);
        }
    }
}
