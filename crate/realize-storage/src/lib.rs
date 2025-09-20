use anyhow::Context;
use arena::engine::Engine;
use arena::fs::ArenaFilesystem;
use arena::{ArenaStorage, indexed_store};
use config::StorageConfig;
use futures::Stream;
use global::db::GlobalDatabase;
use global::pathid_allocator::PathIdAllocator;
use realize_types::{self, Arena, ByteRange, Delta, Path, Peer, Signature};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::{self, JoinHandle};
use tokio_stream::{StreamExt, StreamMap};
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
pub use arena::engine::{Job, JobStatus};
pub use arena::indexed_store::Reader;
pub use arena::notifier::Notification;
pub use arena::notifier::Progress;
pub use arena::types::{
    CacheStatus, DirMetadata, FileMetadata, FileRealm, Mark, Metadata, RemoteAvailability,
};
pub use error::StorageError;
pub use global::fs::{Filesystem, FsLoc};
pub use types::{Inode, JobId, PathId};

/// Local storage, including the real store and an unreal cache.
pub struct Storage {
    cache: Arc<Filesystem>,
    arena_storage: HashMap<Arena, ArenaStorage>,
}

impl Storage {
    /// Create and initialize storage from its configuration.
    pub async fn from_config(config: &StorageConfig) -> anyhow::Result<Arc<Self>> {
        let mut arena_storage = HashMap::new();
        let exclude = build_exclude(&config);

        let globaldb = create_globaldb(&config.cache.db)
            .await
            .with_context(|| format!("global database {:?}", config.cache.db))?;
        let allocator = PathIdAllocator::new(
            Arc::clone(&globaldb),
            config.arenas.iter().map(|a| a.arena).collect::<Vec<_>>(),
        )?;
        for arena_config in &config.arenas {
            let arena = Arena::from(arena_config.arena.as_str());
            arena_storage.insert(
                arena,
                ArenaStorage::from_config(
                    arena,
                    arena_config,
                    &exclude.iter().map(|p| p.as_path()).collect::<Vec<_>>(),
                    &allocator,
                )
                .await
                .with_context(|| format!("in arena {arena}"))?,
            );
        }

        let cache = Filesystem::with_db(
            globaldb,
            allocator,
            arena_storage
                .values()
                .map(|s| Arc::clone(&s.fs))
                .collect::<Vec<_>>(),
        )
        .await
        .context("global cache")?;

        Ok(Arc::new(Self {
            cache,
            arena_storage,
        }))
    }

    /// Return a handle on the unreal cache.
    pub fn cache(&self) -> &Arc<Filesystem> {
        &self.cache
    }

    /// Return an iterator over registered arenas.
    pub fn arenas(&self) -> impl Iterator<Item = Arena> {
        self.arena_storage.iter().map(|(a, _)| *a)
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
        arena::notifier::subscribe(Arc::clone(&self.arena_storage(arena)?.db), tx, progress).await
    }

    /// Take into account notification from a remote peer.
    pub async fn update(&self, peer: Peer, notification: Notification) -> Result<(), StorageError> {
        let arena_storage = self.arena_storage(notification.arena())?;
        let fs: Arc<ArenaFilesystem> = Arc::clone(&arena_storage.fs);
        task::spawn_blocking(move || fs.update(peer, notification)).await??;

        Ok(())
    }

    /// Set the default mark for the files in the given arena.
    pub async fn set_arena_mark(
        self: &Arc<Self>,
        arena: Arena,
        mark: Mark,
    ) -> Result<(), StorageError> {
        let db = self.arena_storage(arena)?.db.clone();
        task::spawn_blocking(move || arena::mark::set_arena_mark(&db, mark)).await?
    }

    /// Set the default mark for the files in the given arena.
    pub async fn set_mark(
        self: &Arc<Self>,
        arena: Arena,
        path: &Path,
        mark: Mark,
    ) -> Result<(), StorageError> {
        let db = self.arena_storage(arena)?.db.clone();
        let path = path.clone();
        task::spawn_blocking(move || arena::mark::set(&db, &path, mark)).await?
    }

    /// Get the mark for a specific path in the given arena.
    pub async fn get_mark(
        self: &Arc<Self>,
        arena: Arena,
        path: &Path,
    ) -> Result<Mark, StorageError> {
        let db = self.arena_storage(arena)?.db.clone();
        let path = path.clone();
        task::spawn_blocking(move || arena::mark::get(&db, &path)).await?
    }

    /// Get the default mark for the files in the given arena.
    pub async fn get_arena_mark(self: &Arc<Self>, arena: Arena) -> Result<Mark, StorageError> {
        let db = self.arena_storage(arena)?.db.clone();
        task::spawn_blocking(move || arena::mark::get_arena_mark(&db)).await?
    }

    /// Get a reader on the given file, if possible.
    pub async fn reader(
        &self,
        arena: Arena,
        path: &realize_types::Path,
    ) -> Result<Reader, StorageError> {
        let arena_storage = self.arena_storage(arena)?;

        Reader::open(&arena_storage.db, path).await
    }

    pub async fn rsync(
        &self,
        arena: Arena,
        path: &realize_types::Path,
        range: &ByteRange,
        sig: Signature,
    ) -> anyhow::Result<Delta, StorageError> {
        let arena_storage = self.arena_storage(arena)?;

        indexed_store::rsync(&arena_storage.db, path, range, sig).await
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
    pub fn job_stream(&self) -> impl Stream<Item = (Arena, JobId, Job)> {
        self.arena_storage
            .iter()
            .map(|(arena, storage)| {
                (
                    *arena,
                    Box::pin(
                        storage
                            .engine
                            .job_stream()
                            .filter_map(|(job_id, job)| job.into_external().map(|j| (job_id, j))),
                    ),
                )
            })
            .collect::<StreamMap<Arena, _>>()
            .map(|(arena, (job_id, job))| (arena, job_id, job))
    }

    /// Tell the engine to retry job missing peers.
    ///
    /// This should be called after a new peer has become available.
    pub fn retry_jobs_missing_peers(&self) {
        for storage in self.arena_storage.values() {
            storage.engine.retry_jobs_missing_peers();
        }
    }

    /// Report the result of processing a job returned by the job stream.
    ///
    /// A job that is reported failed may be returned again for retry, after
    /// some backoff period.
    pub async fn job_finished(
        &self,
        arena: Arena,
        job_id: JobId,
        status: anyhow::Result<JobStatus>,
    ) -> Result<(), StorageError> {
        let engine = Arc::clone(self.engine(arena)?);
        task::spawn(async move { engine.job_finished(job_id, status) }).await?
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
    pub async fn job_for_path(
        &self,
        arena: Arena,
        path: &Path,
    ) -> Result<Option<(JobId, Job)>, StorageError> {
        match self.engine(arena)?.job_for_loc(path).await? {
            None => Ok(None),
            Some((id, job)) => Ok(job.into_external().map(|j| (id, j))),
        }
    }

    /// Return the engine for an arena.
    ///
    /// Only indexed arenas have engines.
    fn engine(&self, arena: Arena) -> Result<&Arc<Engine>, StorageError> {
        Ok(&self.arena_storage(arena)?.engine)
    }

    /// Return the index for the given arena, if one exists.
    fn arena_storage(&self, arena: Arena) -> Result<&ArenaStorage, StorageError> {
        self.arena_storage
            .get(&arena)
            .ok_or_else(|| StorageError::UnknownArena(arena))
    }
}

async fn create_globaldb(path: &std::path::Path) -> anyhow::Result<Arc<GlobalDatabase>> {
    Ok(GlobalDatabase::new(redb_utils::open(path).await?)?)
}

/// Build a vector of all databases listed in `config`, to be excluded
/// from syncing.
fn build_exclude(config: &StorageConfig) -> Vec<std::path::PathBuf> {
    let mut exclude = vec![];
    // Cache is now required
    exclude.push(config.cache.db.clone());
    for arena_config in &config.arenas {
        exclude.push(arena_config.workdir.clone())
    }

    exclude
}
