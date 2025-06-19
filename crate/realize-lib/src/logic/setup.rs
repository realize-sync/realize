use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::sync::mpsc;
use tokio_retry::strategy::ExponentialBackoff;

use crate::{
    model::Arena,
    network::{
        config::NetworkConfig,
        rpc::{
            history::{self, server::forward_peer_history},
            realstore,
        },
        Networking, Server,
    },
    storage::{
        config::{ArenaConfig, StorageConfig},
        real::LocalStorage,
        unreal::{keep_cache_updated, UnrealCacheAsync},
    },
};

/// Setup server as specified in the configuration.
///
/// The returned server is configured, but not started.
pub fn setup_server(
    networking: &Networking,
    storage_config: &StorageConfig,
) -> anyhow::Result<Arc<Server>> {
    check_directory_access(&storage_config.arenas)?;
    let store = LocalStorage::from_config(&storage_config.arenas);

    let mut server = Server::new(networking.clone());
    realstore::server::register(&mut server, store.clone());

    #[cfg(target_os = "linux")]
    history::client::register(&mut server, store.clone());

    Ok(Arc::new(server))
}

/// Setup cache as specified in the configuration.
///
/// The content of the returned cache is kept up-to-date as much as
/// possible by connecting to other peers to track changes.
pub fn setup_cache(
    networking: &Networking,
    storage_config: &StorageConfig,
    network_config: &NetworkConfig,
) -> anyhow::Result<UnrealCacheAsync> {
    let cache = UnrealCacheAsync::from_config(storage_config)?;
    let retry_strategy =
        ExponentialBackoff::from_millis(500).max_delay(Duration::from_secs(5 * 60));

    let arenas = storage_config
        .arenas
        .keys()
        .map(|a| a.clone())
        .collect::<Vec<Arena>>();
    let (tx, rx) = mpsc::channel(100);
    tokio::spawn(keep_cache_updated(cache.clone(), rx));

    for (peer, peer_config) in &network_config.peers {
        if !peer_config.address.is_some() {
            continue;
        }
        tokio::spawn(forward_peer_history(
            networking.clone(),
            peer.clone(),
            arenas.clone(),
            retry_strategy.clone(),
            tx.clone(),
        ));
    }

    Ok(cache)
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
        if !std::fs::metadata(path)
            .and_then(|m| Ok(m.is_dir()))
            .unwrap_or(false)
        {
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
