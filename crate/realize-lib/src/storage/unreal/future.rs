use super::{
    FileAvailability, FileMetadata, InodeAssignment, ReadDirEntry, UnrealCacheBlocking, UnrealError,
};
use crate::model::{Arena, Path, Peer, UnixTime};
use crate::storage::config::StorageConfig;
use crate::storage::real::notifier::{Notification, Progress};
use std::path;
use std::sync::Arc;
use tokio::task;

#[derive(Clone)]
pub struct UnrealCacheAsync {
    inner: Arc<UnrealCacheBlocking>,
}

impl UnrealCacheAsync {
    /// Create and configure a cache from configuration.
    pub fn from_config(config: &StorageConfig) -> anyhow::Result<Self> {
        let cache_config = config
            .cache
            .as_ref()
            .ok_or(anyhow::anyhow!("cache section missing from config file"))?;
        let mut cache = UnrealCacheBlocking::open(&cache_config.db)?;
        for arena in config.arenas.keys() {
            cache.add_arena(arena)?;
        }
        Ok(cache.into_async())
    }

    /// Create a new cache from a blocking one.
    pub fn new(inner: UnrealCacheBlocking) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Create a new cache with the database at the given path.
    pub async fn open(path: &path::Path) -> Result<Self, UnrealError> {
        let path = path.to_path_buf();
        Ok(Self::new(
            task::spawn_blocking(move || UnrealCacheBlocking::open(&path)).await??,
        ))
    }

    /// Return a reference on the blocking cache.
    pub fn blocking(&self) -> Arc<UnrealCacheBlocking> {
        Arc::clone(&self.inner)
    }

    pub fn arenas(&self) -> impl Iterator<Item = &Arena> {
        self.inner.arenas()
    }

    pub fn arena_root(&self, arena: &Arena) -> Result<u64, UnrealError> {
        self.inner.arena_root(arena)
    }

    pub async fn lookup(&self, parent_inode: u64, name: &str) -> Result<ReadDirEntry, UnrealError> {
        let name = name.to_string();
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || inner.lookup(parent_inode, &name)).await?
    }

    pub async fn lookup_path(
        &self,
        parent_inode: u64,
        path: &Path,
    ) -> Result<(u64, InodeAssignment), UnrealError> {
        let path = path.clone();
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || inner.lookup_path(parent_inode, &path)).await?
    }

    pub async fn file_metadata(&self, inode: u64) -> Result<FileMetadata, UnrealError> {
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || inner.file_metadata(inode)).await?
    }

    pub async fn file_availability(&self, inode: u64) -> Result<FileAvailability, UnrealError> {
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || inner.file_availability(inode)).await?
    }

    pub async fn dir_mtime(&self, inode: u64) -> Result<UnixTime, UnrealError> {
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || inner.dir_mtime(inode)).await?
    }

    pub async fn readdir(&self, inode: u64) -> Result<Vec<(String, ReadDirEntry)>, UnrealError> {
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || inner.readdir(inode)).await?
    }

    /// Return a [Progress] instance that represents how up-to-date
    /// the information in the cache is for that peer and arena.
    ///
    /// This should be passed to the peer when subscribing.
    pub async fn peer_progress(
        &self,
        peer: &Peer,
        arena: &Arena,
    ) -> Result<Option<Progress>, UnrealError> {
        let peer = peer.clone();
        let arena = arena.clone();
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || inner.peer_progress(&peer, &arena)).await?
    }

    /// Update the cache by applying a notification coming from the given peer.
    pub async fn update(&self, peer: &Peer, notification: Notification) -> Result<(), UnrealError> {
        let peer = peer.clone();
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || inner.update(&peer, notification)).await?
    }
}
