//! FUSE handles layer - file handle management and lifecycle
//!
//! This module manages open file handles, their lifecycle, and access modes.
//! It tracks file handles by both file handle ID and inode for efficient lookup.

use crate::fs::downloader::Download;
use multimap::MultiMap;
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
    by_inode: MultiMap<Inode, u64>,
}

impl FHRegistry {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(FHRegistryState {
                by_fh: BTreeMap::new(),
                by_inode: MultiMap::new(),
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
        this.by_inode.insert(ino, fh);

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
        let (ino, handle) = self.get(fh).await.ok_or(FuseError::Errno(libc::EBADF))?;
        if ino != expected_inode {
            return Err(FuseError::Errno(libc::EBADF));
        }
        return Ok(handle);
    }

    /// Gets all file handles for the given inode.
    pub(crate) async fn iter_by_inode(&self, ino: Inode) -> Vec<Arc<Mutex<FileHandle>>> {
        let this = self.state.lock().await;

        this.by_inode
            .get_vec(&ino)
            .cloned()
            .unwrap_or_else(|| vec![])
            .into_iter()
            .flat_map(|fh| this.by_fh.get(&fh).map(|(_, h)| Arc::clone(h)))
            .collect()
    }

    /// Removes a file handle from the registry.
    pub(crate) async fn remove(&self, fh: u64) -> Option<Arc<Mutex<FileHandle>>> {
        let mut this = self.state.lock().await;
        if let Some((ino, handle)) = this.by_fh.remove(&fh) {
            if let Some(vec) = this.by_inode.get_vec_mut(&ino) {
                vec.retain(|e| *e != fh);
            }

            return Some(handle);
        }

        None
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
            return Err(FuseError::Errno(libc::EPERM));
        }

        Ok(())
    }

    pub(crate) fn check_allow_write(&self) -> Result<(), FuseError> {
        if !self.allow_write() {
            return Err(FuseError::Errno(libc::EPERM));
        }

        Ok(())
    }
}
