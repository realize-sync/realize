use std::{collections::HashMap, sync::Arc};

use config::StorageConfig;
use real::index::RealIndexAsync;
use real::watcher::RealWatcher;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::model;
use crate::model::Arena;

pub mod config;
mod real;
#[cfg(test)]
pub mod testing;
mod unreal;

pub use real::notifier::Notification;
pub use real::notifier::Progress;
pub use real::store::{Options as RealStoreOptions, RealStore, RealStoreError, SyncedFile};
pub use unreal::error::UnrealError;
pub use unreal::future::UnrealCacheAsync;
pub use unreal::sync::{FileAvailability, FileVersion};
pub use unreal::{FileMetadata, InodeAssignment};

/// Local storage, including the real store and an unreal cache.
pub struct Storage {
    cache: Option<UnrealCacheAsync>,
    arenas: HashMap<Arena, ArenaStorage>,
    store: RealStore,
}

struct ArenaStorage {
    /// The arena's index, kept up-to-date by the watcher.
    index: RealIndexAsync,

    /// Keep a handle on the spawned watcher, which runs only
    /// as long as this instance exists.
    _watcher: RealWatcher,
}

impl Storage {
    /// Create and initialize storage from its configuration.
    pub async fn from_config(config: &StorageConfig) -> anyhow::Result<Arc<Self>> {
        let store = RealStore::from_config(&config.arenas);
        let cache = UnrealCacheAsync::from_config(&config)?;
        let mut arenas = HashMap::new();
        for (arena, arena_config) in &config.arenas {
            let root = &arena_config.path;
            if let Some(index_config) = &arena_config.index {
                let index_path = &index_config.db;
                let index = RealIndexAsync::open(arena.clone(), &index_path).await?;
                let mut exclude = vec![];
                if let Ok(path) = model::Path::from_real_path_in(&index_path, root) {
                    exclude.push(path);
                }
                if let Some(cache_config) = &config.cache {
                    if let Ok(path) = model::Path::from_real_path_in(&cache_config.db, root) {
                        exclude.push(path);
                    }
                }
                let watcher = RealWatcher::spawn(root, exclude, index.clone()).await?;
                arenas.insert(
                    arena.clone(),
                    ArenaStorage {
                        index,
                        _watcher: watcher,
                    },
                );
            }
        }

        Ok(Arc::new(Self {
            cache,
            arenas,
            store,
        }))
    }

    /// Return an iterator over arenas that have an index, and so can
    /// be subscribed to.
    pub fn indexed_arenas(&self) -> impl Iterator<Item = &Arena> {
        self.arenas.keys()
    }

    /// Subscribe to files in the given arena.
    ///
    /// The arena must have an index; check with [Storage::indexed_arenas] first.
    pub async fn subscribe(
        &self,
        arena: &Arena,
        tx: mpsc::Sender<Notification>,
        progress: Option<Progress>,
    ) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
        let storage = self
            .arenas
            .get(arena)
            .ok_or_else(|| anyhow::anyhow!("No index available for arena {arena}"))?;

        real::notifier::subscribe(storage.index.clone(), tx, progress).await
    }

    /// Return a handle on the unreal cache.
    pub fn cache(&self) -> Option<&UnrealCacheAsync> {
        self.cache.as_ref()
    }

    /// Return a handle on the real store.
    pub fn store(&self) -> RealStore {
        self.store.clone()
    }
}
