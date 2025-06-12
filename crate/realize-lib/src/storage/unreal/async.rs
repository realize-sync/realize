use std::{path, sync::Arc, time::SystemTime};

use tokio::task;

use crate::model::{Arena, Path, Peer};

use super::{UnrealCacheBlocking, UnrealCacheError};

pub struct UnrealCacheAsync {
    inner: Arc<UnrealCacheBlocking>,
}

impl UnrealCacheAsync {
    /// Create a new cache from a blocking one.
    pub fn new(inner: UnrealCacheBlocking) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Create a new cache with the database at the given path.
    pub async fn open(path: &path::Path) -> Result<Self, UnrealCacheError> {
        let path = path.to_path_buf();
        Ok(Self::new(
            task::spawn_blocking(move || UnrealCacheBlocking::open(&path)).await??,
        ))
    }

    /// Return a reference on the blocking cache.
    pub fn blocking(&self) -> Arc<UnrealCacheBlocking> {
        Arc::clone(&self.inner)
    }

    /// Async version of [BlockingUnrealCache::link]
    pub async fn link(
        &self,
        peer: &Peer,
        arena: &Arena,
        path: &Path,
        size: u64,
        mtime: SystemTime,
    ) -> Result<(), UnrealCacheError> {
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
    ) -> Result<(), UnrealCacheError> {
        let peer = peer.clone();
        let arena = arena.clone();
        let path = path.clone();
        let inner = Arc::clone(&self.inner);

        Ok(task::spawn_blocking(move || inner.unlink(&peer, &arena, &path, mtime)).await??)
    }
}
