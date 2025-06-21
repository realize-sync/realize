use std::{path, sync::Arc, time::SystemTime};

use tokio::task;

use crate::{
    model::{Arena, Path, Peer},
    storage::config::StorageConfig,
};

use super::{
    FileEntry, FileMetadata, InodeAssignment, ReadDirEntry, UnrealCacheBlocking, UnrealError,
};

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
        for (arena, _) in &config.arenas {
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

    /// Async version of [BlockingUnrealCache::link]
    pub async fn link(
        &self,
        peer: &Peer,
        arena: &Arena,
        path: &Path,
        size: u64,
        mtime: SystemTime,
    ) -> Result<(), UnrealError> {
        let peer = peer.clone();
        let arena = arena.clone();
        let path = path.clone();
        let inner = Arc::clone(&self.inner);

        Ok(task::spawn_blocking(move || inner.link(&peer, &arena, &path, size, mtime)).await??)
    }

    /// Async version of [BlockingUnrealCache::unlink]
    pub async fn unlink(
        &self,
        peer: &Peer,
        arena: &Arena,
        path: &Path,
        mtime: SystemTime,
    ) -> Result<(), UnrealError> {
        let peer = peer.clone();
        let arena = arena.clone();
        let path = path.clone();
        let inner = Arc::clone(&self.inner);

        Ok(task::spawn_blocking(move || inner.unlink(&peer, &arena, &path, mtime)).await??)
    }

    /// Async version of [BlockingUnrealCache::catchup]
    pub async fn catchup(
        &self,
        peer: &Peer,
        arena: &Arena,
        path: &Path,
        size: u64,
        mtime: SystemTime,
    ) -> Result<(), UnrealError> {
        let peer = peer.clone();
        let arena = arena.clone();
        let path = path.clone();
        let inner = Arc::clone(&self.inner);

        Ok(
            task::spawn_blocking(move || inner.catchup(&peer, &arena, &path, size, mtime))
                .await??,
        )
    }

    pub async fn lookup(&self, parent_inode: u64, name: &str) -> Result<ReadDirEntry, UnrealError> {
        let name = name.to_string();
        let inner = Arc::clone(&self.inner);

        Ok(task::spawn_blocking(move || inner.lookup(parent_inode, &name)).await??)
    }

    pub async fn lookup_path(
        &self,
        parent_inode: u64,
        path: &Path,
    ) -> Result<(u64, InodeAssignment), UnrealError> {
        let path = path.clone();
        let inner = Arc::clone(&self.inner);

        Ok(task::spawn_blocking(move || inner.lookup_path(parent_inode, &path)).await??)
    }

    pub async fn file_metadata(&self, inode: u64) -> Result<FileMetadata, UnrealError> {
        let inner = Arc::clone(&self.inner);

        Ok(task::spawn_blocking(move || inner.file_metadata(inode)).await??)
    }

    pub async fn file_availability(
        &self,
        inode: u64,
    ) -> Result<Vec<(Peer, FileEntry)>, UnrealError> {
        let inner = Arc::clone(&self.inner);

        Ok(task::spawn_blocking(move || inner.file_availability(inode)).await??)
    }

    pub async fn dir_mtime(&self, inode: u64) -> Result<SystemTime, UnrealError> {
        let inner = Arc::clone(&self.inner);

        Ok(task::spawn_blocking(move || inner.dir_mtime(inode)).await??)
    }

    pub async fn readdir(&self, inode: u64) -> Result<Vec<(String, ReadDirEntry)>, UnrealError> {
        let inner = Arc::clone(&self.inner);

        Ok(task::spawn_blocking(move || inner.readdir(inode)).await??)
    }

    pub async fn mark_peer_files(&self, peer: &Peer, arena: &Arena) -> Result<(), UnrealError> {
        let peer = peer.clone();
        let arena = arena.clone();
        let inner = Arc::clone(&self.inner);

        Ok(task::spawn_blocking(move || inner.mark_peer_files(&peer, &arena)).await??)
    }

    pub async fn delete_marked_files(&self, peer: &Peer, arena: &Arena) -> Result<(), UnrealError> {
        let peer = peer.clone();
        let arena = arena.clone();
        let inner = Arc::clone(&self.inner);

        Ok(task::spawn_blocking(move || inner.delete_marked_files(&peer, &arena)).await??)
    }
}
