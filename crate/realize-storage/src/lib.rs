use crate::arena::db::ArenaDatabase;
use crate::config::WatcherConfig;
use anyhow::Context;
use arena::engine::Engine;
use arena::{ArenaStorage, indexed_store};
use config::StorageConfig;
use futures::Stream;
use global::db::GlobalDatabase;
use realize_types::{self, Arena, ByteRange, Delta, Path, Peer, Signature};
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock};
use tokio::sync::{mpsc, watch};
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

/// Local storage, including the real store and an unreal cache.
pub struct Storage {
    cache: Arc<Filesystem>,
    arena_storage: Arc<RwLock<HashMap<Arena, ArenaStorage>>>,
    watcher_config: WatcherConfig,
    arena_set_watch_tx: watch::Sender<BTreeSet<Arena>>,
    _arena_set_watch_rx: watch::Receiver<BTreeSet<Arena>>,
}

impl Storage {
    /// Create and initialize storage from its configuration.
    pub async fn from_config(config: &StorageConfig) -> anyhow::Result<Arc<Self>> {
        let mut arena_storage = HashMap::new();
        let globaldb = create_globaldb(&config.cache.db)
            .await
            .with_context(|| format!("global database {:?}", config.cache.db))?;
        let cache = Filesystem::with_db(globaldb).await?;
        let (arena_set_watch_tx, arena_set_watch_rx) = watch::channel(cache.arenas().collect());
        for arena in cache.arenas() {
            if let Some(db) = cache.arena_db(arena) {
                log::debug!(
                    "[{}] Arena setup with datadir {:?}",
                    db.tag(),
                    db.cache().datadir()
                );
                arena_storage.insert(
                    arena,
                    ArenaStorage::spawn(&&db, &config.watcher)
                        .await
                        .with_context(|| format!("in arena {arena}"))?,
                );
            }
        }

        Ok(Arc::new(Self {
            cache,
            watcher_config: config.watcher.clone(),
            arena_storage: Arc::new(RwLock::new(arena_storage)),
            arena_set_watch_tx,
            _arena_set_watch_rx: arena_set_watch_rx,
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
        let storage = ArenaStorage::spawn(&&db, &self.watcher_config).await?;
        let mut lock = self.arena_storage.write().unwrap();
        lock.insert(arena, storage);
        let _ = self
            .arena_set_watch_tx
            .send(lock.keys().map(|a| *a).collect());

        Ok(())
    }

    /// Return a handle on the unreal cache.
    pub fn cache(&self) -> &Arc<Filesystem> {
        &self.cache
    }

    /// Return the set of registered arenas.
    ///
    /// This is a snapshot. Call [Storage::watch_arenas] to be kept
    /// up-to-date.
    pub fn arenas(&self) -> BTreeSet<Arena> {
        self.arena_storage
            .read()
            .unwrap()
            .iter()
            .map(|(a, _)| *a)
            .collect()
    }

    /// Watch for changes in set of arenas.
    pub fn watch_arenas(&self) -> watch::Receiver<BTreeSet<Arena>> {
        self.arena_set_watch_tx.subscribe()
    }

    /// Subscribe to shared file notifications in the given arena.
    pub async fn subscribe(
        &self,
        arena: Arena,
        tx: mpsc::Sender<Notification>,
        progress: Option<Progress>,
    ) -> Result<JoinHandle<anyhow::Result<()>>, StorageError> {
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

#[cfg(test)]
mod tests {
    use assert_fs::{
        TempDir,
        prelude::{PathChild, PathCreateDir},
    };

    use super::*;

    struct Fixture {
        tempdir: TempDir,
        arena: Arena,
        storage: Arc<Storage>,
    }

    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();
            let tempdir = TempDir::new()?;
            let arena = Arena::from("myarena");
            let datadir = tempdir.child(arena.as_str());
            datadir.create_dir_all()?;
            let storage = testing::storage(datadir.path(), [arena]).await?;

            Ok(Self {
                tempdir,
                arena,
                storage,
            })
        }
    }

    #[tokio::test]
    async fn watch_arena_set() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        assert_eq!(BTreeSet::from([fixture.arena]), fixture.storage.arenas());

        let mut watch = fixture.storage.watch_arenas();
        assert_eq!(BTreeSet::from([fixture.arena]), *watch.borrow_and_update());

        let new = Arena::from("new");
        let datadir = fixture.tempdir.child(new.as_str());
        datadir.create_dir_all()?;
        fixture.storage.create_arena(new, &datadir).await?;

        watch.changed().await.unwrap();
        assert_eq!(BTreeSet::from([fixture.arena, new]), *watch.borrow());
        assert_eq!(
            BTreeSet::from([fixture.arena, new]),
            fixture.storage.arenas()
        );
        Ok(())
    }
}
