use super::config::Config;
use crate::consensus::churten::Churten;
use crate::fs::downloader::Downloader;
use crate::fs::fuse::{self, FuseHandle};
use crate::rpc::Household;
use crate::rpc::control::server::ControlServer;
use anyhow::Context;
use realize_network::{Networking, Server, unixsocket};
use realize_storage::Storage;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
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
        check_dirs(&config.storage.arenas)?;

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

    /// Mount a FUSE filesystem at the given mountpoint.
    ///
    /// The returned object must be kept to keep the filesytem mounted. Call join() on it to
    pub async fn export_fuse(
        &self,
        mountpoint: &std::path::Path,
        umask: u16,
    ) -> anyhow::Result<FuseHandle> {
        let cache = self.storage.cache();
        let downloader = Downloader::new(self.household.clone(), cache.clone());
        let handle = fuse::export(
            Arc::clone(self.storage.cache()),
            downloader,
            mountpoint,
            umask,
        )?;
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
        let arena = config.arena;
        let workdir = &config.workdir;
        let workdir_m = match config.workdir.metadata() {
            Ok(m) => m,
            Err(_) => {
                log::debug!("[{arena}] Arena workdir missing; will create: {workdir:?}");
                if let Err(e) = std::fs::create_dir_all(workdir) {
                    anyhow::bail!("[{arena}] Failed to create arena workdir {workdir:?}: {e:?}",);
                }
                config.workdir.metadata()?
            }
        };
        if !is_writable_dir(workdir) {
            anyhow::bail!("[{arena}]: Arena workdir must be a writable directory: {workdir:?}",);
        }

        if let Some(root) = &config.datadir {
            let root_m = root
                .metadata()
                .with_context(|| format!("[{arena}] Arena datadir not found: {root:?}"))?;
            if !is_readable_dir(root) {
                anyhow::bail!("[{arena}] Arena datadir must be a readable directory :{workdir:?}");
            }
            if !is_writable_dir(root) {
                log::warn!("[{arena}] Arena datadir should be a writable directory: {root:?}");
            }

            if workdir_m.dev() != root_m.dev() {
                anyhow::bail!(
                    "[{arena}] Workdir and datadir of an arena must be on the same device.\n\
                     workdir: {workdir:?}\n\
                     datadir: {root:?}"
                );
            }

            if workdir.canonicalize()?.starts_with(root.canonicalize()?) {
                anyhow::bail!(
                    "[{arena}] Workdir of an arena cannot be a child of its datadir \
                     (the reverse is possible)\n\
                     workdir: {workdir:?}\n\
                     datadir: {root:?}"
                );
            }
        }
    }

    Ok(())
}

fn is_readable_dir(root: &PathBuf) -> bool {
    std::fs::read_dir(&root).is_ok()
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
    use realize_types::Arena;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn check_creates_metadata_dir() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;
        let arena = Arena::from("test-arena");

        let root_path = tempdir.child("root");
        root_path.create_dir_all()?;
        let metadata = tempdir.child("metadata");

        let arenas = vec![ArenaConfig::new(
            arena,
            root_path.to_path_buf(),
            metadata.to_path_buf(),
        )];

        check_dirs(&arenas)?;

        assert!(metadata.exists());
        assert!(metadata.is_dir());

        Ok(())
    }

    #[test]
    fn check_rejects_missing_root_dir() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;
        let arena = Arena::from("test-arena");

        let root_path = tempdir.child("root");
        let metadata_path = tempdir.child("metadata");

        let arenas = vec![ArenaConfig::new(
            arena,
            root_path.to_path_buf(),
            metadata_path.to_path_buf(),
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
        let metadata_path = tempdir.child("metadata");

        // Create directories beforehand
        root_path.create_dir_all()?;
        metadata_path.create_dir_all()?;

        let arenas = vec![ArenaConfig::new(
            arena,
            root_path.to_path_buf(),
            metadata_path.to_path_buf(),
        )];

        // Should succeed with existing directories
        check_dirs(&arenas)?;

        Ok(())
    }

    #[test]
    fn check_accepts_rootless_arena() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;
        let arena = Arena::from("test-arena");

        let metadata = tempdir.child("metadata");

        let arenas = vec![ArenaConfig::rootless(arena, metadata.to_path_buf())];

        check_dirs(&arenas)?;

        assert!(metadata.exists());
        assert!(metadata.is_dir());

        Ok(())
    }

    #[test]
    fn check_rejects_nondirectory_root() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;
        let arena = Arena::from("test-arena");

        let root_path = tempdir.child("root");
        let metadata_path = tempdir.child("metadata");

        // Create a file instead of directory
        root_path.write_str("not a directory")?;
        metadata_path.create_dir_all()?;

        let arenas = vec![ArenaConfig::new(
            arena,
            root_path.to_path_buf(),
            metadata_path.to_path_buf(),
        )];

        // Should fail because root path is a file, not directory
        let result = check_dirs(&arenas);
        let errmsg = result.unwrap_err().to_string();
        assert!(errmsg.contains("readable dir"), "Message: {errmsg}");

        Ok(())
    }

    #[test]
    fn check_rejects_root_not_accessible() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;
        let arena = Arena::from("test-arena");

        let root_path = tempdir.child("root");
        let metadata_path = tempdir.child("metadata");

        root_path.create_dir_all()?;
        metadata_path.create_dir_all()?;

        // Remove read permissions from root
        let mut perms = fs::metadata(root_path.path())?.permissions();
        perms.set_mode(0o000); // No permissions
        fs::set_permissions(root_path.path(), perms)?;

        let arenas = vec![ArenaConfig::new(
            arena,
            root_path.to_path_buf(),
            metadata_path.to_path_buf(),
        )];

        // Should fail because root has no read access
        let result = check_dirs(&arenas);
        let errmsg = result.unwrap_err().to_string();
        assert!(errmsg.contains("readable dir"), "Message: {errmsg}");

        Ok(())
    }

    #[test]
    fn check_accepts_root_not_writable() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;
        let arena = Arena::from("test-arena");

        let root_path = tempdir.child("root");
        let metadata_path = tempdir.child("metadata");

        root_path.create_dir_all()?;
        metadata_path.create_dir_all()?;

        // Remove write permissions from root
        let mut perms = fs::metadata(root_path.path())?.permissions();
        perms.set_mode(0o444); // Read-only
        fs::set_permissions(root_path.path(), perms)?;

        let arenas = vec![ArenaConfig::new(
            arena,
            root_path.to_path_buf(),
            metadata_path.to_path_buf(),
        )];

        // Should succeed but log a warning about write access
        check_dirs(&arenas)?;

        Ok(())
    }

    #[test]
    fn check_rejects_metadata_child_of_root() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;
        let arena = Arena::from("test-arena");

        let root = tempdir.child("root");
        let metadata = tempdir.child("root/metadata");

        metadata.create_dir_all()?;
        root.create_dir_all()?;

        let arenas = vec![ArenaConfig::new(
            arena,
            root.to_path_buf(),
            metadata.to_path_buf(),
        )];

        // Should fail because metadat is a child of root
        let result = check_dirs(&arenas);
        let errmsg = result.unwrap_err().to_string();
        assert!(errmsg.contains("child of"), "Message: {errmsg}");

        Ok(())
    }

    #[test]
    fn check_rejects_metadata_not_writable() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;
        let arena = Arena::from("test-arena");

        let metadata = tempdir.child("metadata");
        metadata.create_dir_all()?;

        // Remove write permissions from metadata dir
        let mut perms = fs::metadata(metadata.path())?.permissions();
        perms.set_mode(0o444); // Read-only
        fs::set_permissions(metadata.path(), perms)?;

        let arenas = vec![ArenaConfig::rootless(arena, metadata.to_path_buf())];

        // Should fail because metadata dir is not writable
        let result = check_dirs(&arenas);
        let errmsg = result.unwrap_err().to_string();
        assert!(errmsg.contains("writable dir"), "Message: {errmsg}");

        Ok(())
    }
}
