use super::unreal_capnp;
use crate::model::{self, Arena, Hash, Path, Peer, UnixTime};
use crate::utils::holder::{ByteConversionError, ByteConvertible, NamedType};
use capnp::message::ReaderOptions;
use capnp::serialize_packed;
use uuid::Uuid;

/// A file and all versions known to the cache.
#[derive(Clone, Debug, PartialEq)]
pub struct FileAvailability {
    pub arena: Arena,
    pub path: Path,
    pub versions: Vec<FileVersion>,
}

/// Specific version of a file.
#[derive(Clone, Debug, PartialEq)]
pub struct FileVersion {
    pub peer: Peer,
    pub metadata: FileMetadata,
    pub hash: Hash,
}

/// An entry in a directory listing.
#[derive(Debug, Clone, PartialEq)]
pub struct ReadDirEntry {
    /// The inode of the entry.
    pub inode: u64,
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
    pub parent_inode: u64,
}

/// Information needed to fetch a file from a remote peer.
#[derive(Clone, PartialEq)]
pub struct FileContent {
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

    /// Hash of the specific version of the content the peer has.
    pub hash: model::Hash,
}

impl std::fmt::Debug for FileContent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]/{} {}", self.arena, self.path, self.hash)
    }
}

impl NamedType for FileTableEntry {
    fn typename() -> &'static str {
        "FileEntry"
    }
}

impl ByteConvertible<FileTableEntry> for FileTableEntry {
    fn from_bytes(data: &[u8]) -> Result<FileTableEntry, ByteConversionError> {
        let message_reader = serialize_packed::read_message(&mut &data[..], ReaderOptions::new())?;
        let msg: unreal_capnp::file_table_entry::Reader =
            message_reader.get_root::<unreal_capnp::file_table_entry::Reader>()?;

        let content = msg.get_content()?;
        let metadata = msg.get_metadata()?;
        let mtime = metadata.get_mtime()?;
        Ok(FileTableEntry {
            metadata: FileMetadata {
                size: metadata.get_size(),
                mtime: UnixTime::new(mtime.get_secs(), mtime.get_nsecs()),
            },
            content: FileContent {
                arena: Arena::from(content.get_arena()?.to_str()?),
                path: Path::parse(content.get_path()?.to_str()?)?,
                hash: parse_hash(content.get_hash()?)?,
            },
            parent_inode: msg.get_parent(),
        })
    }

    fn to_bytes(self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let mut builder: unreal_capnp::file_table_entry::Builder =
            message.init_root::<unreal_capnp::file_table_entry::Builder>();

        builder.set_parent(self.parent_inode);

        let mut content = builder.reborrow().init_content();
        content.set_arena(self.content.arena.as_str());
        content.set_path(self.content.path.as_str());
        content.set_hash(&self.content.hash.0);

        let mut metadata = builder.init_metadata();
        metadata.set_size(self.metadata.size);
        let mut mtime = metadata.init_mtime();
        mtime.set_secs(self.metadata.mtime.as_secs());
        mtime.set_nsecs(self.metadata.mtime.subsec_nanos());

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
    pub fn into_readdir_entry(self, inode: u64) -> ReadDirEntry {
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
        let msg: unreal_capnp::dir_table_entry::Reader =
            message_reader.get_root::<unreal_capnp::dir_table_entry::Reader>()?;

        match msg.which()? {
            unreal_capnp::dir_table_entry::Regular(entry) => {
                let entry = entry?;
                Ok(DirTableEntry::Regular(ReadDirEntry {
                    inode: entry.get_inode(),
                    assignment: match entry.get_assignment()? {
                        unreal_capnp::InodeAssignment::File => InodeAssignment::File,
                        unreal_capnp::InodeAssignment::Directory => InodeAssignment::Directory,
                    },
                }))
            }
            unreal_capnp::dir_table_entry::Dot(group) => {
                let mtime = group.get_mtime()?;

                Ok(DirTableEntry::Dot(UnixTime::new(
                    mtime.get_secs(),
                    mtime.get_nsecs(),
                )))
            }
        }
    }

    fn to_bytes(self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let builder: unreal_capnp::dir_table_entry::Builder =
            message.init_root::<unreal_capnp::dir_table_entry::Builder>();

        match self {
            DirTableEntry::Regular(entry) => {
                let mut builder = builder.init_regular();
                builder.set_inode(entry.inode);
                builder.set_assignment(match entry.assignment {
                    InodeAssignment::Directory => unreal_capnp::InodeAssignment::Directory,
                    InodeAssignment::File => unreal_capnp::InodeAssignment::File,
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
        let msg: unreal_capnp::peer_table_entry::Reader =
            message_reader.get_root::<unreal_capnp::peer_table_entry::Reader>()?;

        Ok(PeerTableEntry {
            uuid: Uuid::from_u64_pair(msg.get_uuid_hi(), msg.get_uuid_lo()),
        })
    }

    fn to_bytes(self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let mut builder: unreal_capnp::peer_table_entry::Builder =
            message.init_root::<unreal_capnp::peer_table_entry::Builder>();

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
                arena: Arena::from("test"),
                path: Path::parse("foo/bar.txt")?,
                hash: Hash([0xa1u8; 32]),
            },
            metadata: FileMetadata {
                size: 200,
                mtime: UnixTime::from_secs(1234567890),
            },
            parent_inode: 1234,
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
            inode: 1234,
            assignment: InodeAssignment::Directory,
        });
        assert_eq!(
            regular_dir,
            DirTableEntry::from_bytes(regular_dir.clone().to_bytes()?.as_slice())?
        );

        let regular_file = DirTableEntry::Regular(ReadDirEntry {
            inode: 1234,
            assignment: InodeAssignment::Directory,
        });
        assert_eq!(
            regular_file,
            DirTableEntry::from_bytes(regular_file.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }
}
