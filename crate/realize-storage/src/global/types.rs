use crate::types::{BlobId, Inode};
use crate::utils::holder::{ByteConversionError, ByteConvertible, NamedType};
use capnp::message::ReaderOptions;
use capnp::serialize_packed;
use realize_types::{self, Arena, Hash, Path, Peer, UnixTime};
use uuid::Uuid;

#[allow(dead_code)]
#[allow(unknown_lints)]
#[allow(clippy::uninlined_format_args)]
#[allow(clippy::extra_unused_type_parameters)]
mod cache_capnp {
    include!(concat!(env!("OUT_DIR"), "/global/cache_capnp.rs"));
}

/// A file and all versions known to the cache.
#[derive(Clone, Debug, PartialEq)]
pub struct FileAvailability {
    pub arena: Arena,
    pub path: Path,
    pub metadata: FileMetadata,
    pub hash: Hash,
    pub peers: Vec<Peer>,
}

/// An entry in a directory listing.
#[derive(Debug, Clone, PartialEq)]
pub struct ReadDirEntry {
    /// The inode of the entry.
    pub inode: Inode,
    /// The type of the entry.
    pub assignment: InodeAssignment,
}

/// The type of an inode.
#[derive(Debug, Copy, Clone, PartialEq)]
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
#[derive(Debug, Clone, PartialEq)]
pub struct FileTableEntry {
    /// The metadata of the file.
    pub metadata: FileMetadata,
    /// How the file can be fetched from the peer.
    pub content: FileContent,

    /// Inode of the containing directory
    pub parent_inode: Inode,

    /// Track the history of old value of hash in this entry.
    ///
    /// This is only set in the default entry.
    ///
    /// If None, tracking is disabled.
    pub outdated_versions: Option<Vec<Hash>>,
}

impl FileTableEntry {
    pub fn new(path: Path, size: u64, mtime: UnixTime, hash: Hash, parent_inode: Inode) -> Self {
        Self {
            metadata: FileMetadata { size, mtime: mtime },
            content: FileContent {
                path,
                hash,
                blob: None,
            },
            parent_inode,
            outdated_versions: None,
        }
    }
}

/// Information needed to fetch a file from a remote peer.
#[derive(Clone, PartialEq)]
pub struct FileContent {
    /// The path to use to fetch file content in the peer.
    ///
    /// Note that it shouldn't matter whether the path
    /// here matches the path which led to this file. This
    /// is to be treated as a key for downloading and nothing else..
    ///
    /// This is stored here as a key to fetch file content,
    /// to be replaced by a blob id.
    pub path: realize_types::Path,

    /// Hash of the specific version of the content the peer has.
    pub hash: realize_types::Hash,

    /// ID of a locally-available blob containing this version.
    pub blob: Option<BlobId>,
}

impl std::fmt::Debug for FileContent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.path, self.hash)
    }
}

impl NamedType for FileTableEntry {
    fn typename() -> &'static str {
        "FileTableEntry"
    }
}

impl ByteConvertible<FileTableEntry> for FileTableEntry {
    fn from_bytes(data: &[u8]) -> Result<FileTableEntry, ByteConversionError> {
        let message_reader = serialize_packed::read_message(&mut &data[..], ReaderOptions::new())?;
        let msg: cache_capnp::file_table_entry::Reader =
            message_reader.get_root::<cache_capnp::file_table_entry::Reader>()?;

        let content = msg.get_content()?;
        let metadata = msg.get_metadata()?;
        let mtime = metadata.get_mtime()?;
        let blob: Option<BlobId> = BlobId::as_optional(content.get_blob());

        let outdated_versions = if msg.get_has_outdated_versions() {
            let mut vec = vec![];
            for v in msg.get_outdated_versions()?.iter() {
                vec.push(parse_hash(v?)?)
            }
            Some(vec)
        } else {
            None
        };
        Ok(FileTableEntry {
            metadata: FileMetadata {
                size: metadata.get_size(),
                mtime: UnixTime::new(mtime.get_secs(), mtime.get_nsecs()),
            },
            content: FileContent {
                path: Path::parse(content.get_path()?.to_str()?)?,
                hash: parse_hash(content.get_hash()?)?,
                blob,
            },
            parent_inode: Inode(msg.get_parent()),
            outdated_versions,
        })
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let mut builder: cache_capnp::file_table_entry::Builder =
            message.init_root::<cache_capnp::file_table_entry::Builder>();

        builder.set_parent(self.parent_inode.into());

        let mut content = builder.reborrow().init_content();
        content.set_path(self.content.path.as_str());
        content.set_hash(&self.content.hash.0);
        if let Some(blob) = self.content.blob {
            content.set_blob(blob.into());
        }

        let mut metadata = builder.reborrow().init_metadata();
        metadata.set_size(self.metadata.size);
        let mut mtime = metadata.init_mtime();
        mtime.set_secs(self.metadata.mtime.as_secs());
        mtime.set_nsecs(self.metadata.mtime.subsec_nanos());

        if let Some(versions) = &self.outdated_versions {
            builder.set_has_outdated_versions(true);
            let mut list = builder.init_outdated_versions(versions.len() as u32);
            for (i, hash) in versions.iter().enumerate() {
                list.set(i as u32, &hash.0);
            }
        }

        let mut buffer: Vec<u8> = Vec::new();
        serialize_packed::write_message(&mut buffer, &message)?;

        Ok(buffer)
    }
}

/// The metadata of a file.
#[derive(Debug, Clone, PartialEq)]
pub struct FileMetadata {
    /// The size of the file in bytes.
    pub size: u64,
    /// The modification time of the file.
    ///
    /// This is the duration since the start of the UNIX epoch.
    pub mtime: UnixTime,
}

#[derive(PartialEq, Debug, Clone)]
pub enum DirTableEntry {
    Regular(ReadDirEntry),
    Dot(UnixTime),
}
impl DirTableEntry {
    pub fn into_readdir_entry(self, inode: Inode) -> ReadDirEntry {
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
        let message_reader = serialize_packed::read_message(&mut &data[..], ReaderOptions::new())?;
        let msg: cache_capnp::dir_table_entry::Reader =
            message_reader.get_root::<cache_capnp::dir_table_entry::Reader>()?;

        match msg.which()? {
            cache_capnp::dir_table_entry::Regular(entry) => {
                let entry = entry?;
                Ok(DirTableEntry::Regular(ReadDirEntry {
                    inode: Inode(entry.get_inode()),
                    assignment: match entry.get_assignment()? {
                        cache_capnp::InodeAssignment::File => InodeAssignment::File,
                        cache_capnp::InodeAssignment::Directory => InodeAssignment::Directory,
                    },
                }))
            }
            cache_capnp::dir_table_entry::Dot(group) => {
                let mtime = group.get_mtime()?;

                Ok(DirTableEntry::Dot(UnixTime::new(
                    mtime.get_secs(),
                    mtime.get_nsecs(),
                )))
            }
        }
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let builder: cache_capnp::dir_table_entry::Builder =
            message.init_root::<cache_capnp::dir_table_entry::Builder>();

        match self {
            DirTableEntry::Regular(entry) => {
                let mut builder = builder.init_regular();
                builder.set_inode(entry.inode.into());
                builder.set_assignment(match entry.assignment {
                    InodeAssignment::Directory => cache_capnp::InodeAssignment::Directory,
                    InodeAssignment::File => cache_capnp::InodeAssignment::File,
                })
            }
            DirTableEntry::Dot(mtime) => {
                let mut builder = builder.init_dot().init_mtime();
                builder.set_secs(mtime.as_secs());
                builder.set_nsecs(mtime.subsec_nanos())
            }
        }

        let mut buffer: Vec<u8> = Vec::new();
        serialize_packed::write_message(&mut buffer, &message)?;

        Ok(buffer)
    }
}

/// An entry in the peer table.
#[derive(Debug, Clone, PartialEq)]
pub struct PeerTableEntry {
    pub uuid: Uuid,
}

impl NamedType for PeerTableEntry {
    fn typename() -> &'static str {
        "PeerEntry"
    }
}

impl ByteConvertible<PeerTableEntry> for PeerTableEntry {
    fn from_bytes(data: &[u8]) -> Result<PeerTableEntry, ByteConversionError> {
        let message_reader = serialize_packed::read_message(&mut &data[..], ReaderOptions::new())?;
        let msg: cache_capnp::peer_table_entry::Reader =
            message_reader.get_root::<cache_capnp::peer_table_entry::Reader>()?;

        Ok(PeerTableEntry {
            uuid: Uuid::from_u64_pair(msg.get_uuid_hi(), msg.get_uuid_lo()),
        })
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let mut builder: cache_capnp::peer_table_entry::Builder =
            message.init_root::<cache_capnp::peer_table_entry::Builder>();

        let (hi, lo) = self.uuid.as_u64_pair();
        builder.set_uuid_hi(hi);
        builder.set_uuid_lo(lo);

        let mut buffer: Vec<u8> = Vec::new();
        serialize_packed::write_message(&mut buffer, &message)?;

        Ok(buffer)
    }
}

fn parse_hash(hash: &[u8]) -> Result<Hash, ByteConversionError> {
    let hash: [u8; 32] = hash
        .try_into()
        .map_err(|_| ByteConversionError::Invalid("hash"))?;

    Ok(Hash(hash))
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn convert_file_table_entry() -> anyhow::Result<()> {
        let entry = FileTableEntry {
            content: FileContent {
                path: Path::parse("foo/bar.txt")?,
                hash: Hash([0xa1u8; 32]),
                blob: None,
            },
            metadata: FileMetadata {
                size: 200,
                mtime: UnixTime::from_secs(1234567890),
            },
            parent_inode: Inode(1234),
            outdated_versions: None,
        };

        assert_eq!(
            entry,
            FileTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }

    #[test]
    fn convert_file_table_entry_with_blob() -> anyhow::Result<()> {
        let entry = FileTableEntry {
            content: FileContent {
                path: Path::parse("foo/bar.txt")?,
                hash: Hash([0xa1u8; 32]),
                blob: Some(BlobId(5541)),
            },
            metadata: FileMetadata {
                size: 200,
                mtime: UnixTime::from_secs(1234567890),
            },
            parent_inode: Inode(1234),
            outdated_versions: None,
        };

        assert_eq!(
            entry,
            FileTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }

    #[test]
    fn convert_file_table_entry_with_empty_outdated_versions() -> anyhow::Result<()> {
        let entry = FileTableEntry {
            content: FileContent {
                path: Path::parse("foo/bar.txt")?,
                hash: Hash([0xa1u8; 32]),
                blob: None,
            },
            metadata: FileMetadata {
                size: 200,
                mtime: UnixTime::from_secs(1234567890),
            },
            parent_inode: Inode(1234),
            outdated_versions: Some(vec![]),
        };

        assert_eq!(
            entry,
            FileTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }

    #[test]
    fn convert_file_table_entry_with_outdated_versions() -> anyhow::Result<()> {
        let entry = FileTableEntry {
            content: FileContent {
                path: Path::parse("foo/bar.txt")?,
                hash: Hash([0xa1u8; 32]),
                blob: None,
            },
            metadata: FileMetadata {
                size: 200,
                mtime: UnixTime::from_secs(1234567890),
            },
            parent_inode: Inode(1234),
            outdated_versions: Some(vec![Hash([3u8; 32]), Hash([4u8; 32])]),
        };

        assert_eq!(
            entry,
            FileTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }

    #[test]
    fn convert_dir_table_entry() -> anyhow::Result<()> {
        let dot = DirTableEntry::Dot(UnixTime::from_secs(1234567890));
        assert_eq!(
            dot,
            DirTableEntry::from_bytes(dot.clone().to_bytes()?.as_slice())?
        );

        let regular_dir = DirTableEntry::Regular(ReadDirEntry {
            inode: Inode(1234),
            assignment: InodeAssignment::Directory,
        });
        assert_eq!(
            regular_dir,
            DirTableEntry::from_bytes(regular_dir.clone().to_bytes()?.as_slice())?
        );

        let regular_file = DirTableEntry::Regular(ReadDirEntry {
            inode: Inode(1234),
            assignment: InodeAssignment::Directory,
        });
        assert_eq!(
            regular_file,
            DirTableEntry::from_bytes(regular_file.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }
}
