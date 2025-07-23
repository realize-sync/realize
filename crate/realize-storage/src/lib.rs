use std::path::PathBuf;
use std::{collections::HashMap, sync::Arc};

use arena::db::ArenaDatabase;
use arena::engine::DirtyPaths;
use arena::index::RealIndexAsync;
use arena::watcher::RealWatcher;
use config::StorageConfig;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use realize_types;
use realize_types::Arena;

mod arena;
pub mod config;
mod error;
mod global;
#[cfg(any(test, feature = "testing"))]
pub mod testing;
mod types;
pub mod utils;

pub use arena::blob::{Blob, BlobIncomplete};
pub use arena::notifier::Notification;
pub use arena::notifier::Progress;
pub use arena::reader::Reader;
pub use arena::store::{Options as RealStoreOptions, RealStore, RealStoreError, SyncedFile};
pub use arena::types::Mark;
pub use error::StorageError;
pub use global::cache::UnrealCacheAsync;
pub use global::types::{FileAvailability, FileMetadata, InodeAssignment};
pub use types::Inode;
use utils::redb_utils;

/// Local storage, including the real store and an unreal cache.
pub struct Storage {
    cache: UnrealCacheAsync,
    indexed_arenas: HashMap<Arena, ArenaStorage>,
    store: RealStore,
}

struct ArenaStorage {
    /// The arena's index, kept up-to-date by the watcher.
    index: RealIndexAsync,

    /// Arena root on the filesystem.
    root: PathBuf,

    /// Keep a handle on the spawned watcher, which runs only
    /// as long as this instance exists.
    _watcher: RealWatcher,
}

impl Storage {
    /// Create and initialize storage from its configuration.
    pub async fn from_config(config: &StorageConfig) -> anyhow::Result<Arc<Self>> {
        let store = RealStore::from_config(&config.arenas);
        let mut indexed_arenas = HashMap::new();
        let exclude = build_exclude(&config);

        // Create databases in advance, as the same database may be
        // passed to multiple different subsystems.
        let mut arena_dbs = HashMap::new();
        for (arena, arena_config) in &config.arenas {
            let db = ArenaDatabase::new(redb_utils::open(&arena_config.db).await?)?;
            let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;
            arena_dbs.insert(*arena, (arena_config, db, dirty_paths));
        }

        let cache = UnrealCacheAsync::with_db(
            redb_utils::open(&config.cache.db).await?,
            arena_dbs
                .iter()
                .map(|(arena, (config, db, dirty_paths))| {
                    (
                        *arena,
                        Arc::clone(db),
                        config.blob_dir.to_path_buf(),
                        Arc::clone(dirty_paths),
                    )
                })
                .collect::<Vec<_>>(),
        )
        .await?;

        for (arena, (arena_config, db, dirty_paths)) in arena_dbs {
            let root = match arena_config.root.as_ref() {
                Some(p) => p,
                None => {
                    continue;
                }
            };
            let index = RealIndexAsync::with_db(arena, db.clone(), dirty_paths).await?;
            let exclude = exclude
                .iter()
                .map(|p| realize_types::Path::from_real_path_in(p, root))
                .flatten()
                .collect::<Vec<_>>();

            log::debug!("Watch {root:?}, excluding {exclude:?}");
            let watcher = RealWatcher::spawn(root, exclude, index.clone()).await?;
            indexed_arenas.insert(
                arena,
                ArenaStorage {
                    index,
                    root: root.to_path_buf(),
                    _watcher: watcher,
                },
            );
        }

        Ok(Arc::new(Self {
            cache,
            indexed_arenas,
            store,
        }))
    }

    /// Return an iterator over arenas that have an index, and so can
    /// be subscribed to.
    pub fn indexed_arenas(&self) -> impl Iterator<Item = Arena> {
        self.indexed_arenas.keys().map(|a| *a)
    }

    /// Subscribe to files in the given arena.
    ///
    /// The arena must have an index; check with [Storage::indexed_arenas] first.
    pub async fn subscribe(
        &self,
        arena: Arena,
        tx: mpsc::Sender<Notification>,
        progress: Option<Progress>,
    ) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
        let arena_storage = self.arena_storage(arena)?;

        arena::notifier::subscribe(arena_storage.index.clone(), tx, progress).await
    }

    /// Return a handle on the unreal cache.
    pub fn cache(&self) -> Option<&UnrealCacheAsync> {
        Some(&self.cache)
    }

    /// Get a reader on the given file, if possible.
    pub async fn reader(
        &self,
        arena: Arena,
        path: &realize_types::Path,
    ) -> Result<Reader, StorageError> {
        let s = self.arena_storage(arena)?;

        Reader::open(&s.index, s.root.as_ref(), path).await
    }

    /// Return a handle on the real store.
    pub fn store(&self) -> RealStore {
        self.store.clone()
    }

    /// Return the index for the given arena, if one exists.
    fn arena_storage(&self, arena: Arena) -> Result<&ArenaStorage, StorageError> {
        self.indexed_arenas
            .get(&arena)
            .ok_or_else(|| StorageError::UnknownArena(arena))
    }
}

/// Build a vector of all databases listed in `config`, to be excluded
/// from syncing.
fn build_exclude(config: &StorageConfig) -> Vec<&std::path::Path> {
    let mut exclude = vec![];
    // Cache is now required
    exclude.push(config.cache.db.as_ref());
    for (_, arena_config) in &config.arenas {
        // Arena cache (db + blob_dir) is now required for all arenas
        exclude.push(arena_config.db.as_ref());
        exclude.push(arena_config.blob_dir.as_ref());
    }

    exclude
}
