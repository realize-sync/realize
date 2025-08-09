use arena::engine::Engine;
use arena::mark::PathMarks;
use arena::{ArenaStorage, indexed_store};
use config::StorageConfig;
use futures::Stream;
use global::db::GlobalDatabase;
use global::inode_allocator::InodeAllocator;
use realize_types::{self, Arena, ByteRange, Delta, Hash, Path, Peer, Signature};
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
pub use arena::types::{FileAvailability, FileMetadata, LocalAvailability, Mark};
pub use error::StorageError;
pub use global::cache::UnrealCacheAsync;
pub use global::types::InodeAssignment;
pub use types::{Inode, JobId};

/// Local storage, including the real store and an unreal cache.
pub struct Storage {
    cache: UnrealCacheAsync,
    arena_storage: HashMap<Arena, ArenaStorage>,
}

impl Storage {
    /// Create and initialize storage from its configuration.
    pub async fn from_config(config: &StorageConfig) -> anyhow::Result<Arc<Self>> {
        let mut arena_storage = HashMap::new();
        let exclude = build_exclude(&config);

        let globaldb = GlobalDatabase::new(redb_utils::open(&config.cache.db).await?)?;
        let allocator = InodeAllocator::new(
            Arc::clone(&globaldb),
            config.arenas.keys().map(|a| *a).collect::<Vec<_>>(),
        )?;
        for (arena, arena_config) in &config.arenas {
            arena_storage.insert(
                *arena,
                ArenaStorage::from_config(*arena, arena_config, &exclude, &allocator).await?,
            );
        }

        let cache = UnrealCacheAsync::with_db(
            globaldb,
            allocator,
            arena_storage
                .values()
                .map(|s| Arc::clone(&s.cache))
                .collect::<Vec<_>>(),
        )
        .await?;

        Ok(Arc::new(Self {
            cache,
            arena_storage,
        }))
    }

    /// Return a handle on the unreal cache.
    pub fn cache(&self) -> &UnrealCacheAsync {
        &self.cache
    }

    /// Return an iterator over arenas that have an index, and so can
    /// be subscribed to.
    pub fn indexed_arenas(&self) -> impl Iterator<Item = Arena> {
        self.arena_storage
            .iter()
            .filter(|(_, s)| s.indexed.is_some())
            .map(|(a, _)| *a)
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
        let index = match &self.arena_storage(arena)?.indexed {
            None => return Err(StorageError::NoLocalStorage(arena).into()),
            Some(indexed) => indexed.index.clone(),
        };
        arena::notifier::subscribe(index, tx, progress).await
    }

    /// Take into account notification from a remote peer.
    pub async fn update(&self, peer: Peer, notification: Notification) -> Result<(), StorageError> {
        // TODO: change both in the same transaction
        let arena_storage = self.arena_storage(notification.arena())?;
        if let Some(indexed) = &arena_storage.indexed {
            if let Err(err) = indexed
                .index
                .update(notification.clone(), &indexed.root)
                .await
            {
                log::warn!("Failed to update local store for {notification:?}: {err:?}",);
            }
        }
        let cache = Arc::clone(&arena_storage.cache);
        let index_root = arena_storage.indexed.as_ref().map(|i| i.root.to_path_buf());
        task::spawn_blocking(move || cache.update(peer, notification, index_root.as_deref()))
            .await??;

        Ok(())
    }

    /// Set the default mark for the files in the given arena.
    pub async fn set_arena_mark(
        self: &Arc<Self>,
        arena: Arena,
        mark: Mark,
    ) -> Result<(), StorageError> {
        let marks = self.arena_storage(arena)?.cache.clone() as Arc<dyn PathMarks>;
        task::spawn_blocking(move || marks.set_arena_mark(mark)).await?
    }

    /// Set the default mark for the files in the given arena.
    pub async fn set_mark(
        self: &Arc<Self>,
        arena: Arena,
        path: &Path,
        mark: Mark,
    ) -> Result<(), StorageError> {
        let marks = self.arena_storage(arena)?.cache.clone() as Arc<dyn PathMarks>;
        let path = path.clone();
        task::spawn_blocking(move || marks.set_mark(&path, mark)).await?
    }

    /// Get the mark for a specific path in the given arena.
    pub async fn get_mark(
        self: &Arc<Self>,
        arena: Arena,
        path: &Path,
    ) -> Result<Mark, StorageError> {
        let marks = self.arena_storage(arena)?.cache.clone() as Arc<dyn PathMarks>;
        let path = path.clone();
        task::spawn_blocking(move || marks.get_mark(&path)).await?
    }

    /// Get a reader on the given file, if possible.
    pub async fn reader(
        &self,
        arena: Arena,
        path: &realize_types::Path,
    ) -> Result<Reader, StorageError> {
        let indexed = match &self.arena_storage(arena)?.indexed {
            None => return Err(StorageError::NoLocalStorage(arena)),
            Some(indexed) => indexed,
        };

        Reader::open(&indexed.index, indexed.root.as_ref(), path).await
    }

    pub async fn rsync(
        &self,
        arena: Arena,
        path: &realize_types::Path,
        range: &ByteRange,
        sig: Signature,
    ) -> anyhow::Result<Delta, StorageError> {
        let indexed = match &self.arena_storage(arena)?.indexed {
            None => return Err(StorageError::NoLocalStorage(arena)),
            Some(indexed) => indexed,
        };

        indexed_store::rsync(&indexed.index, &indexed.root, path, range, sig).await
    }

    /// Move a file from the cache to the filesystem.
    ///
    /// The file must have been fully downloaded and verified or the
    /// move will fail.
    ///
    /// Gives up and returns false if the current versions in the cache
    /// don't match `cache_hash` and `index_hash`.
    ///
    /// A `index_hash` value of `None` means that the file must not
    /// exit. If it exists, realize gives up and returns false.
    pub async fn realize(
        &self,
        arena: Arena,
        path: &realize_types::Path,
        cache_hash: &Hash,
        index_hash: Option<&Hash>,
    ) -> Result<bool, StorageError> {
        self.arena_storage(arena)?
            .realize(path, cache_hash, index_hash)
            .await
    }

    pub async fn unrealize(
        &self,
        arena: Arena,
        path: &realize_types::Path,
        hash: &Hash,
    ) -> Result<bool, StorageError> {
        self.arena_storage(arena)?.unrealize(path, hash).await
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
            .map(|(arena, storage)| (*arena, Box::pin(storage.engine.job_stream())))
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
        self.engine(arena)?.job_for_path(path).await
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
