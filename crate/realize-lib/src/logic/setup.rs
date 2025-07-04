use super::config::Config;
use crate::model::{Arena, Peer};
use crate::network::rpc::{Household, realstore};
use crate::network::{Networking, Server};
use crate::storage::config::ArenaConfig;
use crate::storage::unreal::Downloader;
use crate::storage::{Notification, Storage, UnrealCacheAsync};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct Setup {
    networking: Networking,
    config: Config,
    notification_tx: Option<mpsc::Sender<(Peer, Notification)>>,
}

impl Setup {
    pub fn new(config: Config, privkey: &std::path::Path) -> anyhow::Result<Self> {
        let networking = Networking::from_config(&config.network.peers, privkey)?;

        Ok(Self {
            networking,
            config,
            notification_tx: None,
        })
    }

    /// Setup cache as specified in the configuration.
    ///
    /// The content of the returned cache is kept up-to-date as much as
    /// possible by connecting to other peers to track changes.
    pub fn setup_cache(&mut self) -> anyhow::Result<(UnrealCacheAsync, Downloader)> {
        let cache = UnrealCacheAsync::from_config(&self.config.storage)?
            .ok_or_else(|| anyhow::anyhow!("cache.db missing from configuration"))?;
        let (tx, _rx) = mpsc::channel(100);
        //tokio::spawn(keep_cache_updated(cache.clone(), rx)); TODO
        self.notification_tx = Some(tx);

        let downloader = Downloader::new(self.networking.clone(), cache.clone());

        Ok((cache, downloader))
    }

    /// Setup server as specified in the configuration.
    ///
    /// The returned server is configured, but not started.
    pub async fn setup_server(self) -> anyhow::Result<Arc<Server>> {
        check_directory_access(&self.config.storage.arenas)?;
        let Setup {
            networking,
            config,
            notification_tx,
        } = self;

        let storage = Storage::from_config(&config.storage).await?;

        let mut server = Server::new(networking.clone());
        realstore::server::register(&mut server, storage.store());

        let has_notifications = notification_tx.is_some();
        let (household, _) = Household::spawn(networking, storage, notification_tx)?;
        if has_notifications {
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
