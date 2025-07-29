use crate::InodeAllocator;
use crate::StorageError;
use crate::config;
use crate::utils::redb_utils;
use arena_cache::ArenaCache;
use db::ArenaDatabase;
use engine::{DirtyPaths, Engine};
use index::RealIndexAsync;
use mark::PathMarks;
use realize_types::{Arena, Hash};
use std::time::Duration;
use std::{path::PathBuf, sync::Arc};
use tokio::task;
use watcher::RealWatcher;

pub mod arena_cache;
pub mod blob;
pub mod db;
pub mod engine;
pub mod hasher;
pub mod index;
pub mod indexed_store;
pub mod mark;
pub mod notifier;
pub mod store;
pub mod types;
pub mod watcher;

/// Gives access to arena-specific stores and functions.
pub(crate) struct ArenaStorage {
    pub(crate) arena: Arena,
    pub(crate) db: Arc<ArenaDatabase>,
    pub(crate) cache: Arc<ArenaCache>,
    pub(crate) pathmarks: PathMarks,
    pub(crate) engine: Arc<Engine>,
    pub(crate) indexed: Option<IndexedArenaStorage>,
}

/// Indexed (FS-based) local storage.
pub(crate) struct IndexedArenaStorage {
    pub(crate) root: PathBuf,
    pub(crate) index: RealIndexAsync,
    _watcher: RealWatcher,
}

impl ArenaStorage {
    pub(crate) async fn from_config(
        arena: Arena,
        arena_config: &config::ArenaConfig,
        exclude: &Vec<&std::path::Path>,
        allocator: &Arc<InodeAllocator>,
    ) -> anyhow::Result<Self> {
        let db = ArenaDatabase::new(redb_utils::open(&arena_config.db).await?)?;
        let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;
        let arena_cache = ArenaCache::new(
            arena,
            Arc::clone(allocator),
            Arc::clone(&db),
            &arena_config.blob_dir,
            Arc::clone(&dirty_paths),
        )?;
        let indexed = match arena_config.root.as_ref() {
            None => None,
            Some(root) => {
                let index =
                    RealIndexAsync::with_db(arena, Arc::clone(&db), Arc::clone(&dirty_paths))
                        .await?;
                let exclude = exclude
                    .iter()
                    .filter_map(|p| realize_types::Path::from_real_path_in(p, root).ok())
                    .collect::<Vec<_>>();
                log::debug!("Watch {root:?}, excluding {exclude:?}");
                let watcher = RealWatcher::builder(root, index.clone())
                    .exclude_all(exclude.iter())
                    .spawn()
                    .await?;

                Some(IndexedArenaStorage {
                    root: root.to_path_buf(),
                    index,
                    _watcher: watcher,
                })
            }
        };
        let arena_root = arena_cache.arena_root();
        let engine = Engine::new(
            arena,
            Arc::clone(&db),
            Arc::clone(&dirty_paths),
            arena_root,
            job_retry_strategy,
        );
        let pathmarks = PathMarks::new(Arc::clone(&db), arena_root, Arc::clone(&dirty_paths))?;

        Ok(ArenaStorage {
            arena,
            db,
            cache: Arc::clone(&arena_cache),
            engine,
            pathmarks,
            indexed,
        })
    }

    /// Move a file from the cache to the filesystem.
    ///
    /// The file must have been fully downloaded and verified or the
    /// move will fail.
    ///
    /// Give up and return false if the current versions in the cache
    /// don't match `cache_hash` and `index_hash`.
    ///
    /// A `index_hash` value of `None` means that the file must not
    /// exit. If it exists, realize gives up and returns false.
    pub async fn realize(
        &self,
        path: &realize_types::Path,
        cache_hash: &Hash,
        index_hash: Option<&Hash>,
    ) -> Result<bool, StorageError> {
        let indexed = match &self.indexed {
            None => return Err(StorageError::NoLocalStorage(self.arena)),
            Some(indexed) => indexed,
        };

        let cache = self.cache.clone();
        let root = indexed.root.clone();
        let path = path.clone();
        let cache_hash = cache_hash.clone();
        let index_hash = index_hash.cloned();
        let db = Arc::clone(&self.db);
        let done = task::spawn_blocking(move || {
            let txn = db.begin_write()?;
            if let Some(realpath) =
                index::get_indexed_file(&txn, &root, &path, index_hash.as_ref())?
            {
                if cache.move_blob(&txn, &path, &cache_hash, &realpath)? {
                    txn.commit()?;
                    return Ok(true);
                }
            }

            Ok::<bool, StorageError>(false)
        })
        .await??;

        Ok(done)
    }
}

/// Minimum wait time after a failed job.
const JOB_RETRY_TIME_BASE: Duration = Duration::from_secs(60);

/// Max is less than one day, so we retry at different time of day.
const MAX_JOB_RETRY_DURATION: Duration = Duration::from_secs(18 * 23600);

/// Exponential backoff, starting with [JOB_RETRY_TIME_BASE] with a
/// max of [MAX_JOB_RETRY_DURATION].
///
/// TODO: make that configurable in ArenaConfig.
fn job_retry_strategy(attempt: u32) -> Option<Duration> {
    let duration = 2u32.pow(attempt) * JOB_RETRY_TIME_BASE;

    Some(duration.max(MAX_JOB_RETRY_DURATION))
}
