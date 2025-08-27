#![allow(dead_code)] // in progress
use crate::fs::downloader::Downloader;
use fuser::{Filesystem, MountOption};
use realize_storage::GlobalCache;
use std::sync::Arc;

struct NullFS;

impl Filesystem for NullFS {}

/// Mount the cache as FUSE filesystem at the given mountpoint.
pub fn export(
    _cache: Arc<GlobalCache>,
    _downloader: Downloader,
    mountpoint: &std::path::Path,
) -> anyhow::Result<FuseHandle> {
    let bgsession = fuser::spawn_mount2(NullFS, mountpoint, &[MountOption::AutoUnmount])?;
    Ok(FuseHandle { inner: bgsession })
}

/// Handle that must be kept as long as the filesystem must
/// remain mounted.
///
/// To unmount the filesystem, call join() on the handle.
pub struct FuseHandle {
    inner: fuser::BackgroundSession,
}

impl FuseHandle {
    /// Unmount the filesystem and wait for the fuse run loop to stop.
    pub async fn join(self) -> Result<(), tokio::task::JoinError> {
        let Self { inner } = self;
        tokio::task::spawn_blocking(move || inner.join()).await
    }
}
