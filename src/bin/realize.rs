use clap::Parser;
use realize::model::service::DirectoryId;
use realize::server::RealizeServer;
use realize::transport::security::{self, PeerVerifier};
use realize::transport::tcp;
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{PrivateKeyDer, SubjectPublicKeyInfoDer};
use std::path::PathBuf;
use std::process;
use std::sync::Arc;

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
    env_logger::init();

    let cli = Cli::parse();

    // Determine mode
    let src_is_remote = cli.src_addr.is_some();
    let dst_is_remote = cli.dst_addr.is_some();
    let src_is_local = cli.src_path.is_some();
    let dst_is_local = cli.dst_path.is_some();

    // Validate combinations
    if (src_is_remote && dst_is_remote) && (src_is_local || dst_is_local) {
        eprintln!("Cannot mix local and remote for the same endpoint");
        process::exit(1);
    }
    if !(src_is_remote || src_is_local) || !(dst_is_remote || dst_is_local) {
        eprintln!("Must specify both source and destination (local or remote)");
        process::exit(1);
    }

    // Directory id
    let dir_id = DirectoryId::from(
        cli.directory_id
            .clone()
            .unwrap_or_else(|| "dir".to_string()),
    );

    // Setup crypto and peer verifier if needed
    let (privkey, verifier) = if src_is_remote || dst_is_remote {
        let crypto = Arc::new(security::default_provider());
        let privkey = match &cli.privkey {
            Some(path) => match PrivateKeyDer::from_pem_file(path) {
                Ok(key) => match crypto.key_provider.load_private_key(key) {
                    Ok(k) => Arc::from(k),
                    Err(e) => {
                        eprintln!("Failed to load private key: {e}");
                        process::exit(1);
                    }
                },
                Err(e) => {
                    eprintln!("Failed to parse private key: {e}");
                    process::exit(1);
                }
            },
            None => {
                eprintln!("--privkey is required for remote endpoint");
                process::exit(1);
            }
        };
        let mut verifier = PeerVerifier::new(&crypto);
        let peers_path = cli
            .peers
            .as_ref()
            .expect("--peers required for remote endpoint");
        let file = peers_path;
        let pem_iter = SubjectPublicKeyInfoDer::pem_file_iter(file).unwrap_or_else(|e| {
            eprintln!("{:?}: Failed to open peers file: {}", peers_path, e);
            process::exit(1);
        });
        for pem in pem_iter {
            match pem {
                Ok(spki) => verifier.add_peer(&spki),
                Err(e) => {
                    eprintln!(
                        "{:?}: Failed to parse public key in peer file: {}",
                        peers_path, e
                    );
                    process::exit(1);
                }
            }
        }
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
        match tcp::connect_client(
            domain,
            addr,
            Arc::clone(verifier.as_ref().unwrap()),
            Arc::clone(privkey.as_ref().unwrap()),
        )
        .await
        {
            Ok(client) => client,
            Err(e) => {
                eprintln!("Failed to connect to src remote: {e}");
                process::exit(1);
            }
        }
    };

    // Build dst client
    let dst_client = if dst_is_local {
        let dst_path = cli.dst_path.as_ref().unwrap();
        let server = RealizeServer::for_dir(&dir_id, dst_path);
        server.as_inprocess_client()
    } else {
        let addr = cli.dst_addr.as_ref().unwrap();
        let domain = addr.split(':').next().unwrap();
        match tcp::connect_client(
            domain,
            addr,
            Arc::clone(verifier.as_ref().unwrap()),
            Arc::clone(privkey.as_ref().unwrap()),
        )
        .await
        {
            Ok(client) => client,
            Err(e) => {
                eprintln!("Failed to connect to dst remote: {e}");
                process::exit(1);
            }
        }
    };

    // TODO: Set throttle limits via RPC if implemented
    if cli.throttle.is_some() || cli.throttle_up.is_some() || cli.throttle_down.is_some() {
        eprintln!("Throttle options are not yet implemented");
    }

    // Move files
    match realize::algo::move_files(&src_client, &dst_client, dir_id).await {
        Ok(_) => process::exit(0),
        Err(e) => {
            eprintln!("Error: {e}");
            process::exit(1);
        }
    }
}
