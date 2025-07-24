use arena::db::ArenaDatabase;
use arena::engine::{DirtyPaths, Engine};
use arena::index::RealIndexAsync;
use arena::watcher::RealWatcher;
use config::StorageConfig;
use futures::Stream;
use realize_types;
use realize_types::Arena;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::StreamMap;
use utils::redb_utils;

mod arena;
pub mod config;
mod error;
mod global;
#[cfg(any(test, feature = "testing"))]
pub mod testing;
mod types;
pub mod utils;

pub use arena::blob::{Blob, BlobIncomplete};
pub use arena::engine::{Job, JobStatus, JobUpdate};
pub use arena::notifier::Notification;
pub use arena::notifier::Progress;
pub use arena::reader::Reader;
pub use arena::store::{Options as RealStoreOptions, RealStore, RealStoreError, SyncedFile};
pub use arena::types::Mark;
pub use error::StorageError;
pub use global::cache::UnrealCacheAsync;
pub use global::types::{FileAvailability, FileMetadata, InodeAssignment};
pub use types::Inode;

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

    /// The arena's engine.
    engine: Arc<Engine>,

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
            let index =
                RealIndexAsync::with_db(arena, Arc::clone(&db), Arc::clone(&dirty_paths)).await?;
            let exclude = exclude
                .iter()
                .map(|p| realize_types::Path::from_real_path_in(p, root))
                .flatten()
                .collect::<Vec<_>>();

            log::debug!("Watch {root:?}, excluding {exclude:?}");
            let watcher = RealWatcher::spawn(root, exclude, index.clone()).await?;
            let engine = Engine::new(
                arena,
                Arc::clone(&db),
                Arc::clone(&dirty_paths),
                cache.arena_root(arena)?,
                job_retry_strategy,
            );
            indexed_arenas.insert(
                arena,
                ArenaStorage {
                    index,
                    root: root.to_path_buf(),
                    engine,
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
    pub fn cache(&self) -> &UnrealCacheAsync {
        &self.cache
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

    /// Return an infinite stream of jobs.
    ///
    /// Return a stream that looks at the dirty paths on the database
    /// and report jobs that need to be run.
    ///
    /// Uninteresting entries in the dirty path tables are deleted, but entries that
    /// correspond to jobs are left, so that if the process dies, the jobs will be
    /// returned again.
    ///
    /// This stream will wait for as long as necessary for changes on
    /// the database.
    ///
    /// Multiple streams will return the same results, even in the
    /// same process, as long as no job is marked done or failed.
    pub fn job_stream(&self) -> impl Stream<Item = (Arena, Job)> {
        self.indexed_arenas
            .iter()
            .map(|(arena, storage)| (*arena, Box::pin(storage.engine.job_stream())))
            .collect::<StreamMap<Arena, _>>()
    }

    /// Report the result of processing a job returned by the job stream.
    ///
    /// A job that is reported failed may be returned again for retry, after
    /// some backoff period.
    pub fn job_finished(
        &self,
        arena: Arena,
        job: &Job,
        status: anyhow::Result<JobStatus>,
    ) -> Result<(), StorageError> {
        self.engine(arena)?.job_finished(job, status)
    }

    /// Check whether the given job is still relevant.
    ///
    /// - If situation didn't change and the job is still relevant,
    ///   returns [JobUpdate::Same]
    ///
    /// - If the job is still necessary, even though the path was
    ///   updated, returns the new job counter within a
    ///   [JobUpdate::Updated].
    ///
    /// - If the job is no longer necessary, either because it is done
    ///   or because the cache or index state changed, returns
    ///   [JobUpdate::Outdated].
    pub async fn check_job(&self, arena: Arena, job: &Job) -> Result<JobUpdate, StorageError> {
        self.engine(arena)?.check_job(job).await
    }

    /// Return the engine for an arena.
    ///
    /// Only indexed arenas have engines.
    fn engine(&self, arena: Arena) -> Result<&Arc<Engine>, StorageError> {
        Ok(&self.arena_storage(arena)?.engine)
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
