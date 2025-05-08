//! Service definition for Realize - Symmetric File Syncer
//!
//! This module defines the RealizeService trait for use with tarpc 0.36.

use crate::model::error::Result;
use std::path::PathBuf;

pub type DirectoryId = String;
pub type ByteRange = (u64, u64);

#[derive(Debug, Clone)]
pub struct SyncedFile {
    pub path: PathBuf,
    pub size: u64,
    pub state: SyncedFileState,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SyncedFileState {
    Final,
    Partial,
}

/// The service trait for file synchronization.
pub trait RealizeService {
    // List files in a directory
    fn list(&self, dir_id: DirectoryId) -> Result<Vec<SyncedFile>>;

    // Send a byte range of a file
    fn send(
        &self,
        dir_id: DirectoryId,
        relative_path: PathBuf,
        range: ByteRange,
        data: Vec<u8>,
    ) -> Result<()>;

    // Mark a partial file as complete
    fn finish(&self, dir_id: DirectoryId, relative_path: PathBuf) -> Result<()>;
}
