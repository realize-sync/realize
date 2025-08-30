use super::config::Config;
use crate::consensus::churten::Churten;
use crate::fs::downloader::Downloader;
use crate::fs::fuse::{self, FuseHandle};
use crate::fs::nfs;
use crate::rpc::Household;
use crate::rpc::control::server::ControlServer;
use anyhow::Context;
use realize_network::{Networking, Server, unixsocket};
use realize_storage::Storage;

use realize_types::Arena;
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::task::LocalSet;
use tokio_util::sync::CancellationToken;

pub struct SetupHelper {
    pub networking: Networking,
    pub storage: Arc<Storage>,
    pub household: Arc<Household>,
}

impl SetupHelper {
    pub async fn setup(
        config: Config,
        privkey: &std::path::Path,
        local: &LocalSet,
    ) -> anyhow::Result<Self> {
        check_dirs(&config.storage.arena)?;

        let networking = Networking::from_config(&config.network.peers, privkey)?;
        let storage = Storage::from_config(&config.storage).await?;
        log::info!(
            "Cached arenas: {:?}",
            storage
                .cache()
                .arenas()
                .map(|a| a.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        log::info!(
            "Indexed arenas: {:?}",
            storage
                .indexed_arenas()
                .map(|a| a.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        let household = Household::spawn(local, networking.clone(), storage.clone())?;

        Ok(Self {
            networking,
            storage,
            household,
        })
    }

    /// Export NFS at the given address.
    ///
    /// A local cache must be configured.
    pub async fn export_nfs(&self, addr: SocketAddr) -> anyhow::Result<()> {
        let cache = self.storage.cache();

        let downloader = Downloader::new(self.household.clone(), cache.clone());

        nfs::export(Arc::clone(cache), downloader, addr).await?;

        log::info!("NFS filesystem available on {addr:?}");
        Ok(())
    }

    /// Mount a FUSE filesystem at the given mountpoint.
    ///
    /// The returned object must be kept to keep the filesytem mounted. Call join() on it to
    pub async fn export_fuse(&self, mountpoint: &std::path::Path) -> anyhow::Result<FuseHandle> {
        let cache = self.storage.cache();
        let downloader = Downloader::new(self.household.clone(), cache.clone());
        let handle = fuse::export(Arc::clone(self.storage.cache()), downloader, mountpoint)?;
        log::info!("FUSE filesystem mounted on {mountpoint:?}");

        Ok(handle)
    }

    /// Bind to a UNIX socket that allows the owning user to control the server
    pub async fn bind_control_socket(
        &self,
        local: &LocalSet,
        path: Option<&std::path::Path>,
        umask: u32,
    ) -> anyhow::Result<CancellationToken> {
        let path = match path {
            Some(p) => p.to_path_buf(),
            None => {
                if let Some(path) = default_control_socket_path().await {
                    path
                } else {
                    anyhow::bail!(
                        "No appropriate default location for a unix socket. Please specify a path."
                    );
                }
            }
        };

        let token = CancellationToken::new();
        let churten = Churten::new(Arc::clone(&self.storage), self.household.clone());
        let control_server =
            ControlServer::new(Arc::clone(&self.storage), churten, self.household.clone());
        unixsocket::bind(
            local,
            path.as_ref(),
            umask,
            move || control_server.clone().into_client().client,
            token.clone(),
        )
        .await
        .with_context(|| format!("binding socket at {path:?}"))?;

        log::info!("Control socket created at {path:?}");

        Ok(token)
    }

    /// Setup server as specified in the configuration.
    ///
    /// The returned server is configured, but not started.
    pub async fn setup_server(self) -> anyhow::Result<Arc<Server>> {
        let SetupHelper {
            networking,
            household,
            ..
        } = self;

        let mut server = Server::new(networking.clone());
        household.keep_all_connected()?;
        household.register(&mut server);

        Ok(Arc::new(server))
    }
}

/// Look for a reasonable default path for the control socket.
pub async fn default_control_socket_path() -> Option<PathBuf> {
    for pathstr in ["/run", "/var/run", "/tmp"] {
        let path = PathBuf::from(pathstr);
        if let Ok(m) = fs::metadata(&path).await
            && m.is_dir()
        {
            let dir = path.join("realize");
            if !make_private_dir(&dir).await.is_ok() {
                continue;
            }
            return Some(dir.join("control.socket"));
        }
    }

    None
}

async fn make_private_dir(dir: &std::path::Path) -> std::io::Result<()> {
    if !fs::metadata(dir).await.is_ok() {
        fs::create_dir(dir).await?;
        let mut permissions = fs::metadata(dir).await?.permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(dir, permissions).await?;
    }

    Ok(())
}

/// Checks that all directories in the config file are accessible.
fn check_dirs(arenas: &[realize_storage::config::ArenaConfig]) -> anyhow::Result<()> {
    for config in arenas {
        let arena = Arena::from(config.arena.as_str());
        // Check root directory if specified
        if let Some(root) = &config.root {
            if !root.exists() {
                anyhow::bail!("[{arena}] Arena root not found: {root:?}");
            }

            check_is_accessible_dir(root, &arena, "arena root")?;
            if !is_writable_dir(root) {
                log::warn!("[{arena}] Arena root not writable: {root:?}");
            }
        }

        // Check blob_dir (required)
        let blob_dir = &config.blob_dir;
        if !blob_dir.exists() {
            log::debug!("[{arena}] Blob dir missing; will create: {blob_dir:?}");
            if let Err(e) = std::fs::create_dir_all(blob_dir) {
                anyhow::bail!("[{arena}] Failed to create blob dir {blob_dir:?}: {e:?}",);
            }
        }
        check_is_accessible_dir(blob_dir, &arena, "blob dir")?;
        if !is_writable_dir(blob_dir) {
            anyhow::bail!("[{arena}/{}]: blob dir not writable", blob_dir.display());
        }
    }

    Ok(())
}

/// Check if a directory path exists, is a directory, and is
/// accessible. If the directory doesn't exist, try to create it with
/// create_dir_all.
fn check_is_accessible_dir(
    path: &std::path::Path,
    arena: &Arena,
    path_type: &str,
) -> anyhow::Result<()> {
    if !std::fs::metadata(path).map(|m| m.is_dir()).unwrap_or(false) {
        anyhow::bail!(
            "[{arena}] {}: {path_type} is not a directory",
            path.display()
        );
    }

    if std::fs::read_dir(path).is_err() {
        anyhow::bail!("[{arena}] {}: {path_type} not accessible", path.display());
    }

    Ok(())
}

fn is_writable_dir(path: &std::path::Path) -> bool {
    let testfile = path.join(".realize_write_test");
    if let Ok(_) = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&testfile)
    {
        let _ = std::fs::remove_file(&testfile);

        return true;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use realize_storage::config::ArenaConfig;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn check_creates_blob_dir() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;
        let arena = Arena::from("test-arena");

        let root_path = tempdir.child("root");
        root_path.create_dir_all()?;
        let blob_dir_path = tempdir.child("blobs");

        let arenas = vec![ArenaConfig::new(
            arena,
            root_path.to_path_buf(),
            tempdir.child("cache.db").to_path_buf(),
            blob_dir_path.to_path_buf(),
        )];

        // Should succeed - directories will be created
        check_dirs(&arenas)?;

        // Verify directory was created
        assert!(blob_dir_path.exists());
        assert!(blob_dir_path.is_dir());

        Ok(())
    }

    #[test]
    fn check_rejects_missing_root_dir() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;
        let arena = Arena::from("test-arena");

        let root_path = tempdir.child("root");
        let blob_dir_path = tempdir.child("blobs");
        blob_dir_path.create_dir_all()?;

        let arenas = vec![ArenaConfig::new(
            arena,
            root_path.to_path_buf(),
            tempdir.child("cache.db").to_path_buf(),
            blob_dir_path.to_path_buf(),
        )];

        let result = check_dirs(&arenas);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn check_accepts_root_and_blob_dir() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;
        let arena = Arena::from("test-arena");

        let root_path = tempdir.child("root");
        let blob_dir_path = tempdir.child("blobs");

        // Create directories beforehand
        root_path.create_dir_all()?;
        blob_dir_path.create_dir_all()?;

        let arenas = vec![ArenaConfig::new(
            arena,
            root_path.to_path_buf(),
            tempdir.child("cache.db").to_path_buf(),
            blob_dir_path.to_path_buf(),
        )];

        // Should succeed with existing directories
        check_dirs(&arenas)?;

        Ok(())
    }

    #[test]
    fn check_accepts_rootless_arena() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;
        let arena = Arena::from("test-arena");

        let blob_dir_path = tempdir.child("blobs");

        let arenas = vec![ArenaConfig::rootless(
            arena,
            tempdir.child("cache.db").to_path_buf(),
            blob_dir_path.to_path_buf(),
        )];

        // Should succeed - only blob_dir is required
        check_dirs(&arenas)?;

        // Verify blob_dir was created
        assert!(blob_dir_path.exists());
        assert!(blob_dir_path.is_dir());

        Ok(())
    }

    #[test]
    fn check_rejects_nondirectory_root() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;
        let arena = Arena::from("test-arena");

        let root_path = tempdir.child("root");
        let blob_dir_path = tempdir.child("blobs");

        // Create a file instead of directory
        root_path.write_str("not a directory")?;
        blob_dir_path.create_dir_all()?;

        let arenas = vec![ArenaConfig::new(
            arena,
            root_path.to_path_buf(),
            tempdir.child("cache.db").to_path_buf(),
            blob_dir_path.to_path_buf(),
        )];

        // Should fail because root path is a file, not directory
        let result = check_dirs(&arenas);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("is not a directory")
        );

        Ok(())
    }

    #[test]
    fn check_rejects_root_not_accessible() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;
        let arena = Arena::from("test-arena");

        let root_path = tempdir.child("root");
        let blob_dir_path = tempdir.child("blobs");

        root_path.create_dir_all()?;
        blob_dir_path.create_dir_all()?;

        // Remove read permissions from blob_dir
        let mut perms = fs::metadata(root_path.path())?.permissions();
        perms.set_mode(0o000); // No permissions
        fs::set_permissions(root_path.path(), perms)?;

        let arenas = vec![ArenaConfig::new(
            arena,
            root_path.to_path_buf(),
            tempdir.child("cache.db").to_path_buf(),
            blob_dir_path.to_path_buf(),
        )];

        // Should fail because root has no read access
        let result = check_dirs(&arenas);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not accessible"));

        Ok(())
    }

    #[test]
    fn check_accepts_root_not_writable() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;
        let arena = Arena::from("test-arena");

        let root_path = tempdir.child("root");
        let blob_dir_path = tempdir.child("blobs");

        root_path.create_dir_all()?;
        blob_dir_path.create_dir_all()?;

        // Remove write permissions from root
        let mut perms = fs::metadata(root_path.path())?.permissions();
        perms.set_mode(0o444); // Read-only
        fs::set_permissions(root_path.path(), perms)?;

        let arenas = vec![ArenaConfig::new(
            arena,
            root_path.to_path_buf(),
            tempdir.child("cache.db").to_path_buf(),
            blob_dir_path.to_path_buf(),
        )];

        // Should succeed but log a warning about write access
        check_dirs(&arenas)?;

        Ok(())
    }

    #[test]
    fn check_rejects_blob_dir_not_writable() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;
        let arena = Arena::from("test-arena");

        let root_path = tempdir.child("root");
        let blob_dir_path = tempdir.child("blobs");

        root_path.create_dir_all()?;
        blob_dir_path.create_dir_all()?;

        // Remove write permissions from blob_dir
        let mut perms = fs::metadata(blob_dir_path.path())?.permissions();
        perms.set_mode(0o444); // Read-only
        fs::set_permissions(blob_dir_path.path(), perms)?;

        let arenas = vec![ArenaConfig::new(
            arena,
            root_path.to_path_buf(),
            tempdir.child("cache.db").to_path_buf(),
            blob_dir_path.to_path_buf(),
        )];

        let result = check_dirs(&arenas);
        assert!(result.is_err());

        Ok(())
    }
}
