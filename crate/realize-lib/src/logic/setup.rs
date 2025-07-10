use tokio::task::LocalSet;

use super::config::Config;
use crate::fs::downloader::Downloader;
use crate::fs::nfs;
use crate::model::Arena;
use crate::network::{Networking, Server};
use crate::rpc::{Household, realstore};
use crate::storage::Storage;
use crate::storage::config::ArenaConfig;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

pub struct SetupHelper {
    networking: Networking,
    storage: Arc<Storage>,
    household: Household,
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
    pub async fn export_nfs(&mut self, addr: SocketAddr) -> anyhow::Result<()> {
        let cache = self
            .storage
            .cache()
            .ok_or_else(|| anyhow::anyhow!("cache.db must be set in the configuration"))?;

        let downloader = Downloader::new(self.household.clone(), cache.clone());

        nfs::export(cache.clone(), downloader, addr).await?;

        Ok(())
    }

    /// Setup server as specified in the configuration.
    ///
    /// The returned server is configured, but not started.
    pub async fn setup_server(self) -> anyhow::Result<Arc<Server>> {
        let SetupHelper {
            networking,
            storage,
            household,
        } = self;

        let mut server = Server::new(networking.clone());
        realstore::server::register(&mut server, storage.store().clone());

        let has_cache = storage.cache().is_some();
        if has_cache {
            household.keep_connected()?;
        }
        household.register(&mut server);

        Ok(Arc::new(server))
    }
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
        if !std::fs::metadata(path).map(|m| m.is_dir()).unwrap_or(false) {
            anyhow::bail!(
                "Path '{}' (id: {}) is not a directory",
                path.display(),
                arena
            );
        }

        // Check read access
        if std::fs::read_dir(path).is_err() {
            anyhow::bail!(
                "No read access to directory '{}' (id: {})",
                path.display(),
                arena
            );
        }

        // Check write access (warn only)
        let testfile = path.join(".realize_write_test");
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
                    path.display(),
                    arena
                );
            }
        }
    }

    Ok(())
}
