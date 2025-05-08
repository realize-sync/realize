//! Service definition for Realize - Symmetric File Syncer
//!
//! This module defines the RealizeService trait for use with tarpc 0.36.
//! It exposes the following methods:
//! - List
//! - Send
//! - Finish
//!
//! See spec/design.md and spec/future.md for details.

use std::path::PathBuf;

pub type DirectoryId = String;
pub type ByteRange = (u64, u64);
pub type Result<T> = std::result::Result<T, ()>;

#[derive(Debug, Clone)]
pub struct SyncedFile {
    pub path: PathBuf,
    pub size: usize,
    pub is_partial: bool,
}

/// The service trait for file synchronization.
///
/// Methods:
/// - List(DirectoryId) -> Result<Vec<SyncedFile>>
/// - Send(DirectoryId, Path, ByteRange, Data) -> Result<()> 
/// - Finish(DirectoryId, Path) -> Result<()> 
pub trait RealizeService {
    // List files in a directory
    fn list(&self, dir_id: DirectoryId) -> Result<Vec<SyncedFile>>;

    // Send a byte range of a file
    fn send(&self, dir_id: DirectoryId, path: PathBuf, range: ByteRange, data: Vec<u8>) -> Result<()>;

    // Mark a partial file as complete
    fn finish(&self, dir_id: DirectoryId, path: PathBuf) -> Result<()>;
} 