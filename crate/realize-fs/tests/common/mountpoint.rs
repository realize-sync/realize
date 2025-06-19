use std::{fs::File, path::PathBuf, process::Stdio, sync::Arc, time::Duration};

use assert_fs::TempDir;
use nix::fcntl::{Flock, FlockArg};
use realize_lib::{
    model::{Arena, Peer},
    network::{
        hostport::HostPort,
        rpc::realstore,
        security::{PeerVerifier, RawPublicKeyResolver},
        Networking, Server,
    },
    storage::{
        real::LocalStorage,
        unreal::{Downloader, UnrealCacheAsync, UnrealCacheBlocking},
    },
    utils::async_utils::AbortOnDrop,
};
use rustls::pki_types::pem::PemObject as _;
use tokio::process::Command;

use crate::common::config::Config;

pub struct Fixture {
    pub arena: Arena,
    pub cache: UnrealCacheAsync,
    pub mountpoint: PathBuf,
    pub tempdir: TempDir,
    _export: Option<AbortOnDrop<std::io::Result<()>>>,
    _lock: Flock<File>,
    _server: Arc<Server>,
}
impl Fixture {
    pub async fn setup() -> anyhow::Result<Self> {
        let _ = env_logger::try_init();

        let tempdir = TempDir::new()?;
        let arena = Arena::from("test");
        let store = LocalStorage::new(vec![(arena.clone(), tempdir.path().to_path_buf())]);
        let keys = test_keys();
        let verifier = setup_verifier(&keys);
        let client = Peer::from("server");
        let mut server = Server::new(Networking::new(
            vec![],
            RawPublicKeyResolver::from_private_key_file(&keys.privkey_a_path)?,
            Arc::clone(&verifier),
        ));
        realstore::server::register(&mut server, store);
        let server = Arc::new(server);
        let addr = server.listen(&HostPort::localhost(0)).await?;

        let mut cache = in_memory_cache()?;
        cache.add_arena(&arena)?;
        let cache = cache.into_async();

        let downloader = Downloader::new(
            Networking::new(
                vec![(&client, addr.to_string().as_ref())],
                RawPublicKeyResolver::from_private_key_file(&keys.privkey_b_path)?,
                Arc::clone(&verifier),
            ),
            cache.clone(),
        );

        let config = Config::read()?;
        let nfs_config = config.nfs.ok_or_else(|| {
            anyhow::anyhow!("NFS should be configure in {}", Config::path().display())
        })?;

        log::debug!("Obtaining lock {}...", nfs_config.flock.display());
        if let Some(p) = nfs_config.flock.parent() {
            if !p.exists() {
                std::fs::create_dir_all(p)?;
            }
        }
        let lock = Flock::lock(
            std::fs::File::create(&nfs_config.flock)?,
            FlockArg::LockExclusive,
        )
        .map_err(|(_, errno)| {
            anyhow::anyhow!(
                "flock failed on {}: errno {}",
                nfs_config.flock.display(),
                errno
            )
        })?;
        log::debug!("Obtained lock {}", nfs_config.flock.display());

        log::debug!("Exporting UnrealFs on localhost:{}", nfs_config.port);
        let export = export_retry(cache.clone(), downloader, nfs_config.port).await?;

        if nfs_config.mountpoint.exists() {
            log::debug!("Unmounting {} (cleanup)", nfs_config.mountpoint.display());
            let _ = Command::new("umount")
                .stdin(Stdio::null())
                .arg(&nfs_config.mountpoint)
                .stderr(Stdio::null())
                .status()
                .await?;
        } else {
            log::debug!("Create mountpoint {}", nfs_config.mountpoint.display());
            std::fs::create_dir_all(&nfs_config.mountpoint)?;
        }
        log::debug!("Mounting on {}", nfs_config.mountpoint.display());
        let status = Command::new("mount")
            .stdin(Stdio::null())
            .arg(&nfs_config.mountpoint)
            .status()
            .await?;
        if !status.success() {
            anyhow::bail!("mount failed with status code {status}");
        }

        log::debug!("UnrealFs mounted on {}", nfs_config.mountpoint.display());

        Ok(Fixture {
            _lock: lock,
            arena,
            cache,
            mountpoint: nfs_config.mountpoint,
            _export: Some(export),
            _server: server,
            tempdir,
        })
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let mountpoint = &self.mountpoint;
        log::debug!("Unmounting {} (on drop)", mountpoint.display());

        let _ = std::process::Command::new("umount")
            .stdin(Stdio::null())
            .arg(mountpoint)
            .status();
    }
}

/// It might take some time for the address to be freed between runs.
/// Retry exporting for 1s.
async fn export_retry(
    cache: UnrealCacheAsync,
    downloader: Downloader,
    port: u16,
) -> std::io::Result<AbortOnDrop<std::io::Result<()>>> {
    let addr = HostPort::localhost(port).addr();
    for _ in 0..10 {
        match realize_fs::nfs::export(cache.clone(), downloader.clone(), addr).await {
            Ok(handle) => {
                return Ok(AbortOnDrop::new(handle));
            }
            Err(err) => {
                if err.kind() == std::io::ErrorKind::AddrInUse {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                return Err(err);
            }
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AddrInUse,
        "Address already in use (retried)",
    ))
}

fn in_memory_cache() -> anyhow::Result<UnrealCacheBlocking> {
    let cache = realize_lib::storage::unreal::UnrealCacheBlocking::new(
        redb::Builder::new().create_with_backend(redb::backends::InMemoryBackend::new())?,
    )?;

    Ok(cache)
}

pub struct TestKeys {
    pub privkey_a_path: Arc<PathBuf>,
    pub privkey_b_path: Arc<PathBuf>,
    pub pubkey_a_path: PathBuf,
    pub pubkey_b_path: PathBuf,
}

pub fn test_keys() -> TestKeys {
    let resources =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("../../resources/test");
    TestKeys {
        privkey_a_path: Arc::new(resources.join("a.key")),
        privkey_b_path: Arc::new(resources.join("b.key")),
        pubkey_a_path: resources.join("a-spki.pem"),
        pubkey_b_path: resources.join("b-spki.pem"),
    }
}

pub fn setup_verifier(keys: &TestKeys) -> Arc<PeerVerifier> {
    let mut verifier = PeerVerifier::new();
    verifier.add_peer(
        &Peer::from("a"),
        rustls::pki_types::SubjectPublicKeyInfoDer::from_pem_file(&keys.pubkey_a_path).unwrap(),
    );
    verifier.add_peer(
        &Peer::from("b"),
        rustls::pki_types::SubjectPublicKeyInfoDer::from_pem_file(&keys.pubkey_b_path).unwrap(),
    );

    Arc::new(verifier)
}
