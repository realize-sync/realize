//! Service definition for Realize - Symmetric File Syncer
//!
//! This module defines the RealizeService trait for use with tarpc 0.36.

use std::path::PathBuf;

pub type Result<T> = std::result::Result<T, RealizeError>;

#[derive(Debug, Clone, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct DirectoryId(String);
impl From<String> for DirectoryId {
    fn from(value: String) -> Self {
        Self(value)
    }
}
impl From<&str> for DirectoryId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}
impl std::fmt::Display for DirectoryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl DirectoryId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn into_string(self) -> String {
        self.0
    }
}

pub type ByteRange = (u64, u64);

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SyncedFile {
    pub path: PathBuf,
    pub size: u64,
    pub state: SyncedFileState,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum SyncedFileState {
    Final,
    Partial,
}

/// The service trait for file synchronization.
#[tarpc::service]
pub trait RealizeService {
    /// List files in a directory
    async fn list(dir_id: DirectoryId) -> Result<Vec<SyncedFile>>;

    /// Send a byte range of a file.
    ///
    /// TODO: add "at end" boolean, so send knows to truncate the file
    /// if it is bigger than range end.
    async fn send(
        dir_id: DirectoryId,
        relative_path: PathBuf,
        range: ByteRange,
        data: Vec<u8>,
    ) -> Result<()>;

    /// Read a byte range from a file
    async fn read(dir_id: DirectoryId, relative_path: PathBuf, range: ByteRange)
        -> Result<Vec<u8>>;

    /// Mark a partial file as complete
    async fn finish(dir_id: DirectoryId, relative_path: PathBuf) -> Result<()>;
}

/// Error type used by [RealizeService].
///
/// This is limited, to remain usable through a RPC.
#[derive(thiserror::Error, Debug, serde::Serialize, serde::Deserialize)]
pub enum RealizeError {
    /// Returned by the RealizeService when given an invalid request.
    #[error("Bad request: {0}")]
    BadRequest(String),

    #[error("I/O error: {0}")]
    Io(String),

    #[error("Unexpected: {0}")]
    Other(String),
}

impl From<std::io::Error> for RealizeError {
    fn from(value: std::io::Error) -> Self {
        RealizeError::Io(value.to_string())
    }
}

impl From<anyhow::Error> for RealizeError {
    fn from(value: anyhow::Error) -> Self {
        RealizeError::Other(value.to_string())
    }
}
