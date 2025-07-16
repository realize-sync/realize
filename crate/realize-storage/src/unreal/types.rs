use super::unreal_capnp;
use crate::{
    Inode,
    utils::holder::{ByteConversionError, ByteConvertible, NamedType},
};
use capnp::message::ReaderOptions;
use capnp::serialize_packed;
use realize_types::{self, Arena, ByteRanges, Hash, Path, Peer, UnixTime};
use redb::{Key, TypeName, Value};
use uuid::Uuid;

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
        let msg: unreal_capnp::file_table_entry::Reader =
            message_reader.get_root::<unreal_capnp::file_table_entry::Reader>()?;

        let content = msg.get_content()?;
        let metadata = msg.get_metadata()?;
        let mtime = metadata.get_mtime()?;
        let blob: Option<BlobId> = BlobId::as_optional(content.get_blob());
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
        })
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let mut builder: unreal_capnp::file_table_entry::Builder =
            message.init_root::<unreal_capnp::file_table_entry::Builder>();

        builder.set_parent(self.parent_inode.into());

        let mut content = builder.reborrow().init_content();
        content.set_path(self.content.path.as_str());
        content.set_hash(&self.content.hash.0);
        if let Some(blob) = self.content.blob {
            content.set_blob(blob.into());
        }

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
        let msg: unreal_capnp::dir_table_entry::Reader =
            message_reader.get_root::<unreal_capnp::dir_table_entry::Reader>()?;

        match msg.which()? {
            unreal_capnp::dir_table_entry::Regular(entry) => {
                let entry = entry?;
                Ok(DirTableEntry::Regular(ReadDirEntry {
                    inode: Inode(entry.get_inode()),
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

    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let builder: unreal_capnp::dir_table_entry::Builder =
            message.init_root::<unreal_capnp::dir_table_entry::Builder>();

        match self {
            DirTableEntry::Regular(entry) => {
                let mut builder = builder.init_regular();
                builder.set_inode(entry.inode.into());
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

    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError> {
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

#[derive(Debug, Clone, PartialEq)]
pub struct BlobTableEntry {
    pub written_areas: realize_types::ByteRanges,
}

impl NamedType for BlobTableEntry {
    fn typename() -> &'static str {
        "BlobTableEntry"
    }
}

impl ByteConvertible<BlobTableEntry> for BlobTableEntry {
    fn from_bytes(data: &[u8]) -> Result<BlobTableEntry, ByteConversionError> {
        let message_reader = serialize_packed::read_message(&mut &data[..], ReaderOptions::new())?;
        let reader: unreal_capnp::blob_table_entry::Reader =
            message_reader.get_root::<unreal_capnp::blob_table_entry::Reader>()?;

        Ok(BlobTableEntry {
            written_areas: parse_byte_ranges(reader.get_written_areas()?)?,
        })
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let builder: unreal_capnp::blob_table_entry::Builder =
            message.init_root::<unreal_capnp::blob_table_entry::Builder>();

        fill_byte_ranges(&self.written_areas, builder.init_written_areas());

        let mut buffer: Vec<u8> = Vec::new();
        serialize_packed::write_message(&mut buffer, &message)?;

        Ok(buffer)
    }
}

fn parse_byte_ranges(
    msg: unreal_capnp::byte_ranges::Reader<'_>,
) -> Result<ByteRanges, capnp::Error> {
    let ranges_reader = msg.get_ranges()?;
    let mut ranges = Vec::new();
    for i in 0..ranges_reader.len() {
        let range = ranges_reader.get(i);
        ranges.push(realize_types::ByteRange::new(
            range.get_start(),
            range.get_end(),
        ));
    }

    Ok(realize_types::ByteRanges::from_ranges(ranges))
}

fn fill_byte_ranges(ranges: &ByteRanges, builder: unreal_capnp::byte_ranges::Builder) {
    let ranges: Vec<_> = ranges.iter().collect();
    let mut ranges_builder = builder.init_ranges(ranges.len() as u32);
    for (i, range) in ranges.iter().enumerate() {
        let mut range_builder = ranges_builder.reborrow().get(i as u32);
        range_builder.set_start(range.start);
        range_builder.set_end(range.end);
    }
}

fn parse_hash(hash: &[u8]) -> Result<Hash, ByteConversionError> {
    let hash: [u8; 32] = hash
        .try_into()
        .map_err(|_| ByteConversionError::Invalid("hash"))?;

    Ok(Hash(hash))
}

/// A wrapper around u64 representing an blobId.
///
/// This type can be used as a key or value in redb database schemas.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobId(pub u64);

impl BlobId {
    pub const ZERO: BlobId = BlobId(0);
    pub const MAX: BlobId = BlobId(u64::MAX);

    /// Create a new BlobId from a u64 value.
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// Get the underlying u64 value.
    pub fn value(&self) -> u64 {
        self.0
    }

    pub fn plus(&self, val: u64) -> BlobId {
        BlobId(self.0 + val)
    }

    pub fn minus(&self, val: u64) -> BlobId {
        BlobId(self.0 - val)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn is_invalid(&self) -> bool {
        self.0 == 0
    }

    pub fn as_optional(blob_id: u64) -> Option<BlobId> {
        if blob_id == 0 {
            None
        } else {
            Some(BlobId(blob_id))
        }
    }

    pub fn from_optional(blob_id: Option<BlobId>) -> u64 {
        blob_id.map(|b| b.0).unwrap_or(0)
    }
}

impl From<u64> for BlobId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<BlobId> for u64 {
    fn from(value: BlobId) -> Self {
        value.0
    }
}

impl AsRef<u64> for BlobId {
    fn as_ref(&self) -> &u64 {
        &self.0
    }
}

impl AsMut<u64> for BlobId {
    fn as_mut(&mut self) -> &mut u64 {
        &mut self.0
    }
}

impl std::fmt::Display for BlobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl std::fmt::Debug for BlobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BlobId")
            .field(&format!("{:016x}", &self.0))
            .finish()
    }
}

impl Key for BlobId {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        let value1 = u64::from_le_bytes(data1.try_into().unwrap_or([0; 8]));
        let value2 = u64::from_le_bytes(data2.try_into().unwrap_or([0; 8]));
        value1.cmp(&value2)
    }
}

impl Value for BlobId {
    type SelfType<'a> = BlobId;
    type AsBytes<'a>
        = [u8; 8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(8)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> BlobId
    where
        Self: 'a,
    {
        BlobId(<u64>::from_le_bytes(data.try_into().unwrap_or([0; 8])))
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> [u8; 8]
    where
        Self: 'a,
        Self: 'b,
    {
        value.0.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::new("BlobId")
    }
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

    #[test]
    fn convert_blob_table_entry() -> anyhow::Result<()> {
        let entry = BlobTableEntry {
            written_areas: realize_types::ByteRanges::from_ranges(vec![
                realize_types::ByteRange::new(0, 1024),
                realize_types::ByteRange::new(2048, 4096),
            ]),
        };

        assert_eq!(
            entry,
            BlobTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }

    #[test]
    fn test_blobid_display() {
        let blobid = BlobId::new(0xd903);
        assert_eq!(blobid.to_string(), "000000000000d903");
    }
    #[test]
    fn test_blobid_redb_key() {
        let blobid1 = BlobId::new(100);
        let blobid2 = BlobId::new(200);
        let blobid3 = BlobId::new(100);

        let data1 = BlobId::as_bytes(&blobid1);
        let data2 = BlobId::as_bytes(&blobid2);
        let data3 = BlobId::as_bytes(&blobid3);

        assert_eq!(BlobId::compare(&data1, &data2), std::cmp::Ordering::Less);
        assert_eq!(BlobId::compare(&data2, &data1), std::cmp::Ordering::Greater);
        assert_eq!(BlobId::compare(&data1, &data3), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_blobid_redb_value() {
        let original = BlobId::new(12345);
        let bytes = BlobId::as_bytes(&original);
        let restored = BlobId::from_bytes(&bytes);

        assert_eq!(original, restored);
    }

    #[test]
    fn test_blobid_redb_value_edge_cases() {
        // Test zero
        let zero = BlobId::new(0);
        let zero_bytes = BlobId::as_bytes(&zero);
        let zero_restored = BlobId::from_bytes(&zero_bytes);
        assert_eq!(zero, zero_restored);

        // Test maximum u64 value
        let max = BlobId::new(u64::MAX);
        let max_bytes = BlobId::as_bytes(&max);
        let max_restored = BlobId::from_bytes(&max_bytes);
        assert_eq!(max, max_restored);
    }

    #[test]
    fn test_blobid_redb_value_invalid_data() {
        // Test with insufficient data (should handle gracefully)
        let short_data = &[1, 2, 3]; // Less than 8 bytes
        let restored = BlobId::from_bytes(short_data);
        // Should default to 0 or handle gracefully
        assert_eq!(restored.value(), 0);

        // Test with exactly 8 bytes
        let valid_data = &[1, 0, 0, 0, 0, 0, 0, 0]; // Little endian 1
        let restored = BlobId::from_bytes(valid_data);
        assert_eq!(restored.value(), 1);
    }
}
