use anyhow::Context;
use arena::engine::Engine;
use arena::{ArenaStorage, indexed_store};
use config::StorageConfig;
use futures::Stream;
use global::db::GlobalDatabase;
use realize_types::{self, Arena, ByteRange, Delta, Path, Peer, Signature};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
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
    CacheStatus, DirMetadata, FileMetadata, FileRealm, Mark, Metadata, RemoteAvailability, Version,
};
pub use error::{SanityCheck, StorageError};
pub use global::fs::{FileContent, Filesystem, FsLoc};
pub use types::{Inode, JobId};

use crate::arena::db::ArenaDatabase;
use crate::config::{NamedArenaConfig, WatcherConfig};

/// Local storage, including the real store and an unreal cache.
pub struct Storage {
    cache: Arc<Filesystem>,
    arena_storage: Arc<RwLock<HashMap<Arena, ArenaStorage>>>,
    watcher_config: WatcherConfig,
}

impl Storage {
    /// Create and initialize storage from its configuration.
    pub async fn from_config(config: &StorageConfig) -> anyhow::Result<Arc<Self>> {
        let mut arena_storage = HashMap::new();
        let globaldb = create_globaldb(&config.cache.db)
            .await
            .with_context(|| format!("global database {:?}", config.cache.db))?;
        let cache = Filesystem::with_db(globaldb).await?;

        for NamedArenaConfig {
            arena,
            config: arena_config,
        } in &config.arenas
        {
            let arena = *arena;
            let arena_db = if let Some(db) = cache.arena_db(arena) {
                db
            } else {
                cache.add_arena(arena, &arena_config.datadir).await?
            };
            log::debug!(
                "[{}] Arena setup with datadir {:?}",
                arena_db.tag(),
                arena_db.cache().datadir()
            );
            arena_storage.insert(
                arena,
                ArenaStorage::with_db(arena_db, &config.watcher)
                    .await
                    .with_context(|| format!("in arena {arena}"))?,
            );
        }

        Ok(Arc::new(Self {
            cache,
            watcher_config: config.watcher.clone(),
            arena_storage: Arc::new(RwLock::new(arena_storage)),
        }))
    }

    pub async fn create_arena(
        &self,
        arena: Arena,
        datadir: &std::path::Path,
    ) -> Result<(), StorageError> {
        log::debug!("[{}] Create arena with directory {:?}", arena, datadir);
        let db = self.cache.add_arena(arena, datadir).await?;
        // TODO: deal with the situation where db is created but
        // ArenaStorage::with_db fails.
        let storage = ArenaStorage::with_db(db, &self.watcher_config).await?;
        self.arena_storage.write().unwrap().insert(arena, storage);

        Ok(())
    }

    /// Return a handle on the unreal cache.
    pub fn cache(&self) -> &Arc<Filesystem> {
        &self.cache
    }

    /// Return an iterator over registered arenas.
    pub fn arenas(&self) -> impl Iterator<Item = Arena> {
        self.arena_storage
            .read()
            .unwrap()
            .iter()
            .map(|(a, _)| *a)
            .collect::<Vec<_>>()
            .into_iter()
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
        arena::notifier::subscribe(self.arena_db(arena)?, tx, progress).await
    }

    /// Take into account notification from a remote peer.
    pub async fn update(&self, peer: Peer, notification: Notification) -> Result<(), StorageError> {
        self.cache().update(peer, notification).await
    }

    /// Set the default mark for the files in the given arena.
    pub async fn set_arena_mark(
        self: &Arc<Self>,
        arena: Arena,
        mark: Mark,
    ) -> Result<(), StorageError> {
        let db = self.arena_db(arena)?;
        task::spawn_blocking(move || arena::mark::set_arena_mark(&db, mark)).await?
    }

    /// Set the default mark for the files in the given arena.
    pub async fn set_mark(
        self: &Arc<Self>,
        arena: Arena,
        path: &Path,
        mark: Mark,
    ) -> Result<(), StorageError> {
        let db = self.arena_db(arena)?;
        let path = path.clone();
        task::spawn_blocking(move || arena::mark::set(&db, &path, mark)).await?
    }

    /// Get the mark for a specific path in the given arena.
    pub async fn get_mark(
        self: &Arc<Self>,
        arena: Arena,
        path: &Path,
    ) -> Result<Mark, StorageError> {
        let db = self.arena_db(arena)?;
        let path = path.clone();
        task::spawn_blocking(move || arena::mark::get(&db, &path)).await?
    }

    /// Get the default mark for the files in the given arena.
    pub async fn get_arena_mark(self: &Arc<Self>, arena: Arena) -> Result<Mark, StorageError> {
        let db = self.arena_db(arena)?;
        task::spawn_blocking(move || arena::mark::get_arena_mark(&db)).await?
    }

    /// Get a reader on the given file, if possible.
    pub async fn reader(
        &self,
        arena: Arena,
        path: &realize_types::Path,
    ) -> Result<Reader, StorageError> {
        Reader::open(&self.arena_db(arena)?, path).await
    }

    pub async fn rsync(
        &self,
        arena: Arena,
        path: &realize_types::Path,
        range: &ByteRange,
        sig: Signature,
    ) -> anyhow::Result<Delta, StorageError> {
        let db = self.arena_db(arena)?;

        indexed_store::rsync(&db, path, range, sig).await
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
        self.engines()
            .into_iter()
            .map(|(arena, engine)| {
                (
                    arena,
                    Box::pin(
                        engine
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
        for (_, engine) in self.engines() {
            engine.retry_jobs_missing_peers();
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
        let engine = self.engine(arena)?;
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
    fn engine(&self, arena: Arena) -> Result<Arc<Engine>, StorageError> {
        Ok(self
            .arena_storage
            .read()
            .unwrap()
            .get(&arena)
            .ok_or_else(|| StorageError::UnknownArena(arena))?
            .engine
            .clone())
    }

    /// Return the [ArenaDatabase] for te given arena.
    fn arena_db(&self, arena: Arena) -> Result<Arc<ArenaDatabase>, StorageError> {
        Ok(self
            .arena_storage
            .read()
            .unwrap()
            .get(&arena)
            .ok_or_else(|| StorageError::UnknownArena(arena))?
            .db
            .clone())
    }

    /// Return all engines
    fn engines(&self) -> Vec<(Arena, Arc<Engine>)> {
        self.arena_storage
            .read()
            .unwrap()
            .iter()
            .map(|(arena, storage)| (*arena, Arc::clone(&storage.engine)))
            .collect::<Vec<_>>()
    }
}

async fn create_globaldb(path: &std::path::Path) -> anyhow::Result<Arc<GlobalDatabase>> {
    Ok(GlobalDatabase::new(redb_utils::open(path).await?)?)
}
