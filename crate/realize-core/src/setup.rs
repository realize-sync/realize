use super::config::Config;
use crate::consensus::churten::Churten;
use crate::fs::downloader::Downloader;
use crate::fs::nfs;
use crate::rpc::Household;
use crate::rpc::control::server::ControlServer;
use anyhow::Context;
use realize_network::{Networking, Server, unixsocket};
use realize_storage::Storage;
use realize_storage::config::ArenaConfig;
use realize_types::Arena;
use std::collections::HashMap;
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
    pub household: Household,
}

impl SetupHelper {
    pub async fn setup(
        config: Config,
        privkey: &std::path::Path,
        local: &LocalSet,
    ) -> anyhow::Result<Self> {
        check_directory_access(&config.storage.arenas)?;

        let networking = Networking::from_config(&config.network.peers, privkey)?;
        let storage = Storage::from_config(&config.storage).await?;
        log::debug!(
            "Storage: Cached {:?}",
            storage.cache().arenas().collect::<Vec<_>>()
        );
        log::debug!(
            "Storage: Indexed {:?}",
            storage.indexed_arenas().collect::<Vec<_>>()
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

        nfs::export(cache.clone(), downloader, addr).await?;

        Ok(())
    }

    /// Bind to a UNIX socket that allows the owning user to control the server
    pub async fn bind_control_socket(
        &self,
        local: &LocalSet,
        path: Option<&std::path::Path>,
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
        let control_server = ControlServer::new(Arc::clone(&self.storage), churten);
        unixsocket::bind(
            local,
            path.as_ref(),
            move || control_server.clone().into_client().client,
            token.clone(),
        )
        .await
        .with_context(|| format!("binding socket at {path:?}"))?;

        log::debug!("Control socket available at {path:?}");

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
fn check_directory_access(arenas: &HashMap<Arena, ArenaConfig>) -> anyhow::Result<()> {
    for (arena, config) in arenas {
        // Only check arenas that have a local path specified
        if let Some(root) = &config.root {
            log::debug!("Checking directory {}: {}", arena, root.display());
            if !root.exists() {
                anyhow::bail!(
                    "LocalArena '{}' (id: {}) does not exist",
                    root.display(),
                    arena
                );
            }
            if !std::fs::metadata(root).map(|m| m.is_dir()).unwrap_or(false) {
                anyhow::bail!(
                    "Path '{}' (id: {}) is not a directory",
                    root.display(),
                    arena
                );
            }

            // Check read access
            if std::fs::read_dir(root).is_err() {
                anyhow::bail!(
                    "No read access to directory '{}' (id: {})",
                    root.display(),
                    arena
                );
            }

            // Check write access (warn only)
            let testfile = root.join(".realize_write_test");
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&testfile)
            {
                Ok(_) => {
                    let _ = std::fs::remove_file(&testfile);
                }
                Err(_) => {
                    log::warn!(
                        "No write access to directory '{}' (id: {})",
                        root.display(),
                        arena
                    );
                }
            }
        }
    }

    Ok(())
}
