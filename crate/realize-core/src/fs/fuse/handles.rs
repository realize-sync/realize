//! FUSE handles layer - file handle management and lifecycle
//!
//! This module manages open file handles, their lifecycle, and access modes.
//! It tracks file handles by both file handle ID and inode for efficient lookup.

use crate::fs::downloader::Download;
use nix::errno::Errno;
use nix::libc;
use realize_storage::Inode;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::error::FuseError;

pub(crate) enum FileHandle {
    /// Handle to a remote file, which may be partially or fully cached locally.
    ///
    /// If FHMode allows writing, this handle will be turned into a
    /// local one before the first write operation.
    Remote(Download, FHMode),

    /// Handle to a local file.
    Local(tokio::fs::File, FHMode),

    /// Directory handle
    Dir(Vec<(String, Inode, realize_storage::Metadata)>),
}

/// Keeps track of open file handles.
pub(crate) struct FHRegistry {
    state: Arc<Mutex<FHRegistryState>>,
}

struct FHRegistryState {
    by_fh: BTreeMap<u64, (Inode, Arc<Mutex<FileHandle>>)>,
}

impl FHRegistry {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(FHRegistryState {
                by_fh: BTreeMap::new(),
            })),
        }
    }

    pub(crate) async fn add(&self, ino: Inode, handle: FileHandle) -> u64 {
        let mut this = self.state.lock().await;
        let fh = this
            .by_fh
            .last_key_value()
            .map(|(k, _)| *k + 1)
            .unwrap_or(1);
        this.by_fh.insert(fh, (ino, Arc::new(Mutex::new(handle))));

        fh
    }

    /// Gets a specific file handle.
    pub(crate) async fn get(&self, fh: u64) -> Option<(Inode, Arc<Mutex<FileHandle>>)> {
        self.state
            .lock()
            .await
            .by_fh
            .get(&fh)
            .map(|(ino, h)| (*ino, Arc::clone(h)))
    }

    pub(crate) async fn get_or_err(
        &self,
        fh: u64,
        expected_inode: Inode,
    ) -> Result<Arc<Mutex<FileHandle>>, FuseError> {
        let (ino, handle) = self.get(fh).await.ok_or(FuseError::from(Errno::EBADF))?;
        if ino != expected_inode {
            return Err(FuseError::from(Errno::EBADF));
        }
        return Ok(handle);
    }

    /// Removes a file handle from the registry.
    pub(crate) async fn remove(&self, fh: u64) -> Option<Arc<Mutex<FileHandle>>> {
        let mut state = self.state.lock().await;
        state.by_fh.remove(&fh).map(|(_, h)| h)
    }
}

// File mode, stored in the FileHandle.
//
// Sometimes, the underlying handle would allow operations that
// weren't asked in FUSE open(). [FHMode] helps make sure that these
// aren't allowed.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum FHMode {
    Invalid,
    ReadOnly,
    ReadWrite,
    WriteOnly,
}

impl FHMode {
    pub(crate) fn from_flags(flags: i32) -> Self {
        let mode = flags & (libc::O_RDONLY | libc::O_WRONLY | libc::O_RDWR);

        let res = match mode {
            libc::O_RDONLY => FHMode::ReadOnly,
            libc::O_WRONLY => FHMode::WriteOnly,
            libc::O_RDWR => FHMode::ReadWrite,
            _ => {
                log::debug!("Invalid open flags: mode={mode:o} (all flags= 0x{flags:x})");

                FHMode::Invalid
            }
        };

        res
    }

    pub(crate) fn allow_read(&self) -> bool {
        match self {
            FHMode::ReadOnly => true,
            FHMode::ReadWrite => true,
            FHMode::WriteOnly => false,
            FHMode::Invalid => false,
        }
    }

    pub(crate) fn allow_write(&self) -> bool {
        match self {
            FHMode::ReadOnly => false,
            FHMode::ReadWrite => true,
            FHMode::WriteOnly => true,
            FHMode::Invalid => false,
        }
    }

    pub(crate) fn check_allow_read(&self) -> Result<(), FuseError> {
        if !self.allow_read() {
            return Err(Errno::EPERM.into());
        }

        Ok(())
    }

    pub(crate) fn check_allow_write(&self) -> Result<(), FuseError> {
        if !self.allow_write() {
            return Err(Errno::EPERM.into());
        }

        Ok(())
    }
}
