use crate::model::{self};
use redb::Value;
use std::marker::PhantomData;
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
    fn from_bytes(data: &[u8]) -> Result<FileEntry, UnrealError> {
        Ok(bincode::deserialize::<FileEntry>(data)?)
    }

    fn to_bytes(self) -> Result<Vec<u8>, UnrealError> {
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
    fn from_bytes(data: &[u8]) -> Result<DirTableEntry, UnrealError> {
        Ok(bincode::deserialize::<DirTableEntry>(data)?)
    }

    fn to_bytes(self) -> Result<Vec<u8>, UnrealError> {
        Ok(bincode::serialize(&self)?)
    }
}

/// A type that can be converted to and from bytes.
///
/// Either conversion are allowed to fail without causing a panic..
trait ByteConvertible<T> {
    fn from_bytes(data: &[u8]) -> Result<T, UnrealError>;
    fn to_bytes(self) -> Result<Vec<u8>, UnrealError>;
}

/// Give a name to the type; used by redb.
trait NamedType {
    fn typename() -> &'static str;
}

/// A convenient type to use as value in redb tables.
///
/// All this type does is make parsing and generating [redb::Value]s
/// convenient and typesafe.
///
/// One big difference between this type and implementing
/// [redb::Value] directly is that serialization and deserialization
/// can fail without causing a panic.
///
/// To use a type in a holder, make it implement both [NamedType] and
/// [ByteConvertible].
#[derive(Clone, Debug)]
enum Holder<'a, T> {
    Borrowed(&'a [u8], PhantomData<T>),
    Owned(Vec<u8>, PhantomData<T>),
}

impl<'a, T> Holder<'a, T> {
    /// Return the data in the holder as a slice.
    fn as_bytes(&self) -> &'_ [u8] {
        match self {
            Holder::Owned(vec, _) => vec.as_slice(),
            Holder::Borrowed(arr, _) => *arr,
        }
    }
}

impl<'a, T> Holder<'a, T>
where
    T: ByteConvertible<T>,
{
    /// Create a Holder containing the byte representation of the
    /// given instance.
    fn new(obj: T) -> Result<Self, UnrealError> {
        Ok(Holder::Owned(obj.to_bytes()?, PhantomData))
    }

    /// Converts the Holder into an instance of the expected type.
    fn parse(self) -> Result<T, UnrealError> {
        match self {
            Holder::Owned(vec, _) => T::from_bytes(vec.as_slice()),
            Holder::Borrowed(arr, _) => T::from_bytes(arr),
        }
    }
}

impl<'a, T> From<&'a [u8]> for Holder<'a, T> {
    fn from(arr: &'a [u8]) -> Self {
        Holder::Borrowed::<T>(arr, PhantomData)
    }
}

impl<T> Value for Holder<'_, T>
where
    T: NamedType + std::fmt::Debug,
{
    type SelfType<'a>
        = Holder<'a, T>
    where
        Self: 'a;

    type AsBytes<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        data.into()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.as_bytes()
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new(T::typename())
    }
}
