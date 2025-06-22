use crate::model::{self};
use crate::utils::holder::{ByteConversionError, ByteConvertible, NamedType};
use std::time::SystemTime;

mod downloader;
mod error;
mod future;
mod sync;
#[cfg(test)]
pub mod testing;
mod updater;
mod unreal_capnp {
    include!(concat!(env!("OUT_DIR"), "/unreal_capnp.rs"));
}

pub use downloader::Download;
pub use downloader::Downloader;
pub use error::UnrealError;
pub use future::UnrealCacheAsync;
pub use sync::UnrealCacheBlocking;
pub use updater::keep_cache_updated;

/// Inode of the root dir.
pub const ROOT_DIR: u64 = 1;

/// An entry in a directory listing.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ReadDirEntry {
    /// The inode of the entry.
    pub inode: u64,
    /// The type of the entry.
    pub assignment: InodeAssignment,
}

/// The type of an inode.
#[derive(Debug, Copy, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum InodeAssignment {
    /// The inode of a file, look it up in the file table.
    File,
    /// The inode of a directory, look it up in the directory table.
    ///
    /// Note that an empty directory won't have any entries in
    /// the directory table.
    Directory,
}

/// An entry in the file table.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FileEntry {
    /// The arena to use to fetch file content in the peer.
    ///
    /// This is stored here as a key to fetch file content,
    /// to be replaced by a blob id.
    pub arena: model::Arena,

    /// The path to use to fetch file content in the peer.
    ///
    /// Note that it shouldn't matter whether the path
    /// here matches the path which led to this file. This
    /// is to be treated as a key for downloading and nothing else..
    ///
    /// This is stored here as a key to fetch file content,
    /// to be replaced by a blob id.
    pub path: model::Path,

    /// The metadata of the file.
    pub metadata: FileMetadata,

    /// Inode of the containing directory
    pub(crate) parent_inode: u64,
}

impl NamedType for FileEntry {
    fn typename() -> &'static str {
        "FileEntry"
    }
}

impl ByteConvertible<FileEntry> for FileEntry {
    fn from_bytes(data: &[u8]) -> Result<FileEntry, ByteConversionError> {
        Ok(bincode::deserialize::<FileEntry>(data)?)
    }

    fn to_bytes(self) -> Result<Vec<u8>, ByteConversionError> {
        Ok(bincode::serialize(&self)?)
    }
}

/// The metadata of a file.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FileMetadata {
    /// The size of the file in bytes.
    pub size: u64,
    /// The modification time of the file.
    pub mtime: SystemTime,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
enum DirTableEntry {
    Regular(ReadDirEntry),
    Dot(SystemTime),
}
impl DirTableEntry {
    fn as_readdir_entry(self, inode: u64) -> ReadDirEntry {
        match self {
            DirTableEntry::Regular(e) => e,
            DirTableEntry::Dot(_) => ReadDirEntry {
                inode,
                assignment: InodeAssignment::Directory,
            },
        }
    }
}

impl NamedType for DirTableEntry {
    fn typename() -> &'static str {
        "DirTableEntry"
    }
}

impl ByteConvertible<DirTableEntry> for DirTableEntry {
    fn from_bytes(data: &[u8]) -> Result<DirTableEntry, ByteConversionError> {
        Ok(bincode::deserialize::<DirTableEntry>(data)?)
    }

    fn to_bytes(self) -> Result<Vec<u8>, ByteConversionError> {
        Ok(bincode::serialize(&self)?)
    }
}
