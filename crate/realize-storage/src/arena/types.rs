use crate::types::BlobId;
use crate::utils::holder::{ByteConversionError, ByteConvertible, NamedType};
use crate::{Inode, InodeAssignment};
use capnp::message::ReaderOptions;
use capnp::serialize_packed;
use realize_types::{self, Arena, ByteRanges, Hash, Path, Peer, UnixTime};

#[allow(dead_code)]
#[allow(unknown_lints)]
#[allow(clippy::uninlined_format_args)]
#[allow(clippy::extra_unused_type_parameters)]
mod index_capnp {
    include!(concat!(env!("OUT_DIR"), "/arena/index_capnp.rs"));
}
#[allow(dead_code)]
#[allow(unknown_lints)]
#[allow(clippy::uninlined_format_args)]
#[allow(clippy::extra_unused_type_parameters)]
mod blob_capnp {
    include!(concat!(env!("OUT_DIR"), "/arena/blob_capnp.rs"));
}
#[allow(dead_code)]
#[allow(unknown_lints)]
#[allow(clippy::uninlined_format_args)]
#[allow(clippy::extra_unused_type_parameters)]
mod cache_capnp {
    include!(concat!(env!("OUT_DIR"), "/arena/cache_capnp.rs"));
}
#[allow(dead_code)]
#[allow(unknown_lints)]
#[allow(clippy::uninlined_format_args)]
#[allow(clippy::extra_unused_type_parameters)]
mod mark_capnp {
    include!(concat!(env!("OUT_DIR"), "/arena/mark_capnp.rs"));
}
#[allow(dead_code)]
#[allow(unknown_lints)]
#[allow(clippy::uninlined_format_args)]
#[allow(clippy::extra_unused_type_parameters)]
mod engine_capnp {
    include!(concat!(env!("OUT_DIR"), "/arena/engine_capnp.rs"));
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum LocalAvailability {
    /// File is not available locally
    Missing,

    /// File is only partially available locally, and here is its size
    /// and available range.
    Partial(u64, ByteRanges),

    /// File is available locally, but its content hasn't been verified.
    Complete,

    /// File is available locally and its content has been verified
    Verified,
}

/// LRU Queue ID enum
pub use blob_capnp::LruQueueId;
use redb::{Key, Value};
use uuid::Uuid;

/// An entry in the queue table.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct QueueTableEntry {
    /// First node in the queue (BlobId)
    pub head: Option<BlobId>,
    /// Last node in the queue (BlobId)
    pub tail: Option<BlobId>,
    /// Total disk usage in bytes
    pub disk_usage: u64,
}

impl NamedType for QueueTableEntry {
    fn typename() -> &'static str {
        "QueueTableEntry"
    }
}

impl ByteConvertible<QueueTableEntry> for QueueTableEntry {
    fn from_bytes(data: &[u8]) -> Result<QueueTableEntry, ByteConversionError> {
        let message_reader = serialize_packed::read_message(&mut &data[..], ReaderOptions::new())?;
        let reader: blob_capnp::queue_table_entry::Reader =
            message_reader.get_root::<blob_capnp::queue_table_entry::Reader>()?;

        let head_value = reader.get_head();
        let head = if head_value != 0 {
            Some(BlobId(head_value))
        } else {
            None
        };

        let tail_value = reader.get_tail();
        let tail = if tail_value != 0 {
            Some(BlobId(tail_value))
        } else {
            None
        };

        Ok(QueueTableEntry {
            head: head,
            tail: tail,
            disk_usage: reader.get_disk_usage(),
        })
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let mut builder: blob_capnp::queue_table_entry::Builder =
            message.init_root::<blob_capnp::queue_table_entry::Builder>();

        builder.set_head(self.head.map(|b| b.0).unwrap_or(0));
        builder.set_tail(self.tail.map(|b| b.0).unwrap_or(0));
        builder.set_disk_usage(self.disk_usage);

        let mut buffer: Vec<u8> = Vec::new();
        serialize_packed::write_message(&mut buffer, &message)?;

        Ok(buffer)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BlobTableEntry {
    pub written_areas: realize_types::ByteRanges,

    /// Hash of the content; this may be missing or different from the file hash.
    pub content_hash: Option<Hash>,

    /// Queue ID enum
    pub queue: LruQueueId,

    /// Next blob in the queue (BlobId)
    pub next: Option<BlobId>,

    /// Previous blob in the queue (BlobId)
    pub prev: Option<BlobId>,

    /// Disk usage in bytes
    pub disk_usage: u64,
}

impl NamedType for BlobTableEntry {
    fn typename() -> &'static str {
        "BlobTableEntry"
    }
}

impl ByteConvertible<BlobTableEntry> for BlobTableEntry {
    fn from_bytes(data: &[u8]) -> Result<BlobTableEntry, ByteConversionError> {
        let message_reader = serialize_packed::read_message(&mut &data[..], ReaderOptions::new())?;
        let reader: blob_capnp::blob_table_entry::Reader =
            message_reader.get_root::<blob_capnp::blob_table_entry::Reader>()?;

        let content_hash = if reader.has_content_hash() {
            Some(parse_hash(reader.get_content_hash()?)?)
        } else {
            None
        };

        let next_value = reader.get_next();
        let next = if next_value != 0 {
            Some(BlobId(next_value))
        } else {
            None
        };

        let prev_value = reader.get_prev();
        let prev = if prev_value != 0 {
            Some(BlobId(prev_value))
        } else {
            None
        };

        let queue = reader.get_queue()?;

        Ok(BlobTableEntry {
            written_areas: parse_byte_ranges(reader.get_written_areas()?)?,
            content_hash,
            queue,
            next,
            prev,
            disk_usage: reader.get_disk_usage(),
        })
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let mut builder: blob_capnp::blob_table_entry::Builder =
            message.init_root::<blob_capnp::blob_table_entry::Builder>();

        if let Some(h) = &self.content_hash {
            builder.set_content_hash(&h.0);
        }
        builder.set_queue(self.queue);
        builder.set_next(self.next.map(|b| b.0).unwrap_or(0));
        builder.set_prev(self.prev.map(|b| b.0).unwrap_or(0));
        builder.set_disk_usage(self.disk_usage);
        fill_byte_ranges(&self.written_areas, builder.init_written_areas());

        let mut buffer: Vec<u8> = Vec::new();
        serialize_packed::write_message(&mut buffer, &message)?;

        Ok(buffer)
    }
}

fn parse_byte_ranges(msg: blob_capnp::byte_ranges::Reader<'_>) -> Result<ByteRanges, capnp::Error> {
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

fn fill_byte_ranges(ranges: &ByteRanges, builder: blob_capnp::byte_ranges::Builder) {
    let ranges: Vec<_> = ranges.iter().collect();
    let mut ranges_builder = builder.init_ranges(ranges.len() as u32);
    for (i, range) in ranges.iter().enumerate() {
        let mut range_builder = ranges_builder.reborrow().get(i as u32);
        range_builder.set_start(range.start);
        range_builder.set_end(range.end);
    }
}

/// An entry in the file table.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexedFileTableEntry {
    pub hash: Hash,
    pub mtime: UnixTime,
    pub size: u64,

    // If set, a version is known to exist that replaces the version
    // in this entry.
    pub outdated_by: Option<Hash>,
}

impl From<FileTableEntry> for IndexedFileTableEntry {
    fn from(value: FileTableEntry) -> Self {
        IndexedFileTableEntry {
            hash: value.content.hash,
            mtime: value.metadata.mtime,
            size: value.metadata.size,
            outdated_by: value.outdated_by,
        }
    }
}
impl From<&FileTableEntry> for IndexedFileTableEntry {
    fn from(value: &FileTableEntry) -> Self {
        IndexedFileTableEntry {
            hash: value.content.hash.clone(),
            mtime: value.metadata.mtime,
            size: value.metadata.size,
            outdated_by: value.outdated_by.clone(),
        }
    }
}
impl NamedType for IndexedFileTableEntry {
    fn typename() -> &'static str {
        "index.file"
    }
}

impl ByteConvertible<IndexedFileTableEntry> for IndexedFileTableEntry {
    fn from_bytes(data: &[u8]) -> Result<IndexedFileTableEntry, ByteConversionError> {
        let message_reader = serialize_packed::read_message(&mut &data[..], ReaderOptions::new())?;
        let msg: index_capnp::indexed_file_table_entry::Reader =
            message_reader.get_root::<index_capnp::indexed_file_table_entry::Reader>()?;

        let mtime = msg.get_mtime()?;
        let hash: &[u8] = msg.get_hash()?;
        let hash = parse_hash(hash)?;
        let outdated_by: &[u8] = msg.get_outdated_by()?;
        let outdated_by = if outdated_by.is_empty() {
            None
        } else {
            Some(parse_hash(outdated_by)?)
        };
        Ok(IndexedFileTableEntry {
            hash,
            mtime: UnixTime::new(mtime.get_secs(), mtime.get_nsecs()),
            size: msg.get_size(),
            outdated_by,
        })
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let mut builder: index_capnp::indexed_file_table_entry::Builder =
            message.init_root::<index_capnp::indexed_file_table_entry::Builder>();

        builder.set_size(self.size);
        builder.set_hash(&self.hash.0);

        if let Some(hash) = &self.outdated_by {
            builder.set_outdated_by(&hash.0)
        }

        let mut mtime = builder.init_mtime();
        mtime.set_secs(self.mtime.as_secs());
        mtime.set_nsecs(self.mtime.subsec_nanos());

        let mut buffer: Vec<u8> = Vec::new();
        serialize_packed::write_message(&mut buffer, &message)?;

        Ok(buffer)
    }
}

/// An entry in the file table.
#[derive(Debug, Clone, PartialEq)]
pub enum HistoryTableEntry {
    /// The file was modified by the user. Modification should be forwarded to other copies.
    Add(realize_types::Path),

    /// The file was modified by the user. Modification should be forwarded to other copies.
    ///
    /// The hash is the removed hash version.
    Replace(realize_types::Path, Hash),

    /// The file was removed by the user. Removal should be forwarded to other copies.
    ///
    /// The hash is the removed hash version.
    Remove(realize_types::Path, Hash),

    /// This version of the file was removed from local store, but it should remain available in the cache.
    ///
    /// The hash is the dropped hash version.
    Drop(realize_types::Path, Hash),
}

impl NamedType for HistoryTableEntry {
    fn typename() -> &'static str {
        "index.file"
    }
}

impl ByteConvertible<HistoryTableEntry> for HistoryTableEntry {
    fn from_bytes(data: &[u8]) -> Result<HistoryTableEntry, ByteConversionError> {
        let message_reader = serialize_packed::read_message(&mut &data[..], ReaderOptions::new())?;
        let msg: index_capnp::history_table_entry::Reader =
            message_reader.get_root::<index_capnp::history_table_entry::Reader>()?;
        match msg.get_kind()? {
            index_capnp::history_table_entry::Kind::Add => {
                Ok(HistoryTableEntry::Add(parse_path(msg.get_path()?)?))
            }
            index_capnp::history_table_entry::Kind::Replace => Ok(HistoryTableEntry::Replace(
                parse_path(msg.get_path()?)?,
                parse_hash(msg.get_old_hash()?)?,
            )),
            index_capnp::history_table_entry::Kind::Remove => Ok(HistoryTableEntry::Remove(
                parse_path(msg.get_path()?)?,
                parse_hash(msg.get_old_hash()?)?,
            )),
            index_capnp::history_table_entry::Kind::Drop => Ok(HistoryTableEntry::Drop(
                parse_path(msg.get_path()?)?,
                parse_hash(msg.get_old_hash()?)?,
            )),
        }
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let mut builder: index_capnp::history_table_entry::Builder =
            message.init_root::<index_capnp::history_table_entry::Builder>();

        match self {
            HistoryTableEntry::Add(path) => {
                builder.set_kind(index_capnp::history_table_entry::Kind::Add);
                builder.set_path(path.as_str());
            }
            HistoryTableEntry::Replace(path, old_hash) => {
                builder.set_kind(index_capnp::history_table_entry::Kind::Replace);
                builder.set_path(path.as_str());
                builder.set_old_hash(&old_hash.0);
            }
            HistoryTableEntry::Remove(path, old_hash) => {
                builder.set_kind(index_capnp::history_table_entry::Kind::Remove);
                builder.set_path(path.as_str());
                builder.set_old_hash(&old_hash.0);
            }
            HistoryTableEntry::Drop(path, old_hash) => {
                builder.set_kind(index_capnp::history_table_entry::Kind::Drop);
                builder.set_path(path.as_str());
                builder.set_old_hash(&old_hash.0);
            }
        }

        let mut buffer: Vec<u8> = Vec::new();
        serialize_packed::write_message(&mut buffer, &message)?;

        Ok(buffer)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FailedJobTableEntry {
    pub failure_count: u32,
    pub retry: RetryJob,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RetryJob {
    WhenPeerConnects,
    After(UnixTime),
}

impl NamedType for FailedJobTableEntry {
    fn typename() -> &'static str {
        "engine.job"
    }
}

impl ByteConvertible<FailedJobTableEntry> for FailedJobTableEntry {
    fn from_bytes(data: &[u8]) -> Result<FailedJobTableEntry, ByteConversionError> {
        let message_reader = serialize_packed::read_message(&mut &data[..], ReaderOptions::new())?;
        let msg: engine_capnp::failed_job_table_entry::Reader =
            message_reader.get_root::<engine_capnp::failed_job_table_entry::Reader>()?;

        let retry = match msg.get_retry().which()? {
            engine_capnp::failed_job_table_entry::retry::After(after_time) => {
                RetryJob::After(UnixTime::from_secs(after_time))
            }
            engine_capnp::failed_job_table_entry::retry::WhenPeerConnects(()) => {
                RetryJob::WhenPeerConnects
            }
        };

        Ok(FailedJobTableEntry {
            failure_count: msg.get_failure_count(),
            retry,
        })
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let mut builder: engine_capnp::failed_job_table_entry::Builder =
            message.init_root::<engine_capnp::failed_job_table_entry::Builder>();

        builder.set_failure_count(self.failure_count);

        match &self.retry {
            RetryJob::After(time) => {
                builder.get_retry().set_after(time.as_secs());
            }
            RetryJob::WhenPeerConnects => {
                builder.get_retry().set_when_peer_connects(());
            }
        }

        let mut buffer: Vec<u8> = Vec::new();
        serialize_packed::write_message(&mut buffer, &message)?;

        Ok(buffer)
    }
}

fn parse_hash(hash: &[u8]) -> Result<Hash, ByteConversionError> {
    let hash: [u8; 32] = hash
        .try_into()
        .map_err(|_| ByteConversionError::Invalid("hash"))?;
    let hash = Hash(hash);
    Ok(hash)
}

fn parse_path(path: capnp::text::Reader<'_>) -> Result<realize_types::Path, ByteConversionError> {
    realize_types::Path::parse(path.to_str()?).map_err(|_| ByteConversionError::Invalid("path"))
}

/// A mark that can be applied to files and directories in an arena.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize, Default)]
pub enum Mark {
    /// Files marked as "watch" belong in the unreal. They should be left in the cache and are subject to normal LRU rules.
    #[default]
    Watch,
    /// Files marked as "keep" belong in the unreal. They should be left in the cache and are unconditionally kept.
    Keep,
    /// Files marked as "own" belong in the real. They should be moved into the arena root as regular files.
    Own,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MarkTableEntry {
    pub mark: Mark,
}

impl NamedType for MarkTableEntry {
    fn typename() -> &'static str {
        "mark"
    }
}

impl ByteConvertible<MarkTableEntry> for MarkTableEntry {
    fn from_bytes(data: &[u8]) -> Result<MarkTableEntry, ByteConversionError> {
        let message_reader = serialize_packed::read_message(&mut &data[..], ReaderOptions::new())?;
        let msg: mark_capnp::mark_table_entry::Reader =
            message_reader.get_root::<mark_capnp::mark_table_entry::Reader>()?;

        let mark = match msg.get_mark()? {
            mark_capnp::Mark::Own => Mark::Own,
            mark_capnp::Mark::Watch => Mark::Watch,
            mark_capnp::Mark::Keep => Mark::Keep,
        };

        Ok(MarkTableEntry { mark })
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let mut builder: mark_capnp::mark_table_entry::Builder =
            message.init_root::<mark_capnp::mark_table_entry::Builder>();

        let mark = match self.mark {
            Mark::Own => mark_capnp::Mark::Own,
            Mark::Watch => mark_capnp::Mark::Watch,
            Mark::Keep => mark_capnp::Mark::Keep,
        };
        builder.set_mark(mark);

        let mut buffer: Vec<u8> = Vec::new();
        serialize_packed::write_message(&mut buffer, &message)?;

        Ok(buffer)
    }
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

/// Key for the file table of the [ArenaDatabase].
#[derive(Debug, Clone)]
pub enum FileTableKey {
    /// The default entry, containing the selected version and its
    /// metadata for the cache.
    Default(Inode),
    /// An entry that represents the local copy.
    LocalCopy(Inode),
    /// An entry that represents another peer's copy.
    PeerCopy(Inode, Peer),

    /// A key that could not be parsed
    Invalid,
}

impl FileTableKey {
    /// A range that covers all keys with the given inode.
    pub fn range(inode: Inode) -> std::ops::Range<FileTableKey> {
        FileTableKey::Default(inode)..FileTableKey::Default(inode.plus(1))
    }

    /// The key's inode
    pub fn inode(&self) -> Inode {
        match self {
            FileTableKey::Invalid => Inode::ZERO,
            FileTableKey::Default(inode) => *inode,
            FileTableKey::LocalCopy(inode) => *inode,
            FileTableKey::PeerCopy(inode, _) => *inode,
        }
    }

    fn variant_order(&self) -> u8 {
        match self {
            FileTableKey::Invalid => 0,
            FileTableKey::Default(_) => 1,
            FileTableKey::LocalCopy(_) => 2,
            FileTableKey::PeerCopy(_, _) => 3,
        }
    }

    fn peer(&self) -> Option<&Peer> {
        match self {
            FileTableKey::PeerCopy(_, peer) => Some(peer),
            _ => None,
        }
    }
}

impl PartialEq for FileTableKey {
    fn eq(&self, other: &Self) -> bool {
        self.inode() == other.inode()
            && self.variant_order() == other.variant_order()
            && self.peer() == other.peer()
    }
}

impl Eq for FileTableKey {}

impl PartialOrd for FileTableKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FileTableKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // First compare by inode
        let inode_cmp = self.inode().cmp(&other.inode());
        if inode_cmp != std::cmp::Ordering::Equal {
            return inode_cmp;
        }

        // Then compare by variant type
        let variant_cmp = self.variant_order().cmp(&other.variant_order());
        if variant_cmp != std::cmp::Ordering::Equal {
            return variant_cmp;
        }

        // Finally compare by peer (only relevant for PeerCopy variants)
        match (self.peer(), other.peer()) {
            (Some(peer1), Some(peer2)) => peer1.cmp(peer2),
            (None, None) => std::cmp::Ordering::Equal,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (None, Some(_)) => std::cmp::Ordering::Less,
        }
    }
}

impl Value for FileTableKey {
    type SelfType<'a> = FileTableKey;

    type AsBytes<'a> = Vec<u8>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let inode = Inode(<u64>::from_be_bytes(
            data[0..8].try_into().unwrap_or([0; 8]),
        ));
        if inode == Inode::ZERO {
            return FileTableKey::Invalid;
        }
        match data.get(8) {
            None => FileTableKey::Default(inode),
            Some(0) => FileTableKey::LocalCopy(inode),
            Some(1) => {
                FileTableKey::PeerCopy(inode, Peer::from(str::from_utf8(&data[9..]).unwrap()))
            }
            Some(_) => FileTableKey::Invalid,
        }
    }

    fn as_bytes<'a, 'b: 'a>(value: &FileTableKey) -> Vec<u8>
    where
        Self: 'a,
        Self: 'b,
    {
        let mut ret = vec![];
        ret.extend_from_slice(&value.inode().as_u64().to_be_bytes());
        match value {
            FileTableKey::Default(_) | FileTableKey::Invalid => {}
            FileTableKey::LocalCopy(_) => {
                ret.push(0);
            }
            FileTableKey::PeerCopy(_, peer) => {
                ret.push(1);
                ret.extend_from_slice(peer.as_str().as_bytes());
            }
        };

        ret
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("FileTableKey")
    }
}

impl Key for FileTableKey {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        // The byte representation is designed so that byte comparison
        // matches object comparison, except for Invalid. No need to
        // parse.
        data1.cmp(data2)
    }
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

    // If set, a version is known to exist that replaces the version
    // in this entry.
    pub outdated_by: Option<Hash>,
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
            outdated_by: None,
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
        let outdated_by: &[u8] = msg.get_outdated_by()?;
        let outdated_by = if outdated_by.is_empty() {
            None
        } else {
            Some(parse_hash(outdated_by)?)
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
            outdated_by,
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

        if let Some(hash) = &self.outdated_by {
            builder.set_outdated_by(&hash.0)
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
    pub fn into_readdir_entry(self, inode: Inode) -> (Inode, InodeAssignment) {
        match self {
            DirTableEntry::Regular(e) => (e.inode, e.assignment),
            DirTableEntry::Dot(_) => (inode, InodeAssignment::Directory),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn convert_blob_table_entry() -> anyhow::Result<()> {
        let entry = BlobTableEntry {
            written_areas: realize_types::ByteRanges::from_ranges(vec![
                realize_types::ByteRange::new(0, 1024),
                realize_types::ByteRange::new(2048, 4096),
            ]),
            content_hash: None,
            queue: LruQueueId::WorkingArea,
            next: Some(BlobId(0x0101010101010101)),
            prev: Some(BlobId(0x0202020202020202)),
            disk_usage: 1024,
        };

        assert_eq!(
            entry,
            BlobTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        let empty_entry = BlobTableEntry {
            written_areas: realize_types::ByteRanges::from_ranges(vec![]),
            content_hash: Some(Hash([0x03; 32])),
            queue: LruQueueId::ProtectedArea,
            next: None,
            prev: None,
            disk_usage: 0,
        };

        assert_eq!(
            empty_entry,
            BlobTableEntry::from_bytes(empty_entry.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }

    #[test]
    fn convert_queue_table_entry() -> anyhow::Result<()> {
        let entry = QueueTableEntry {
            head: Some(BlobId(0x0101010101010101)),
            tail: Some(BlobId(0x0202020202020202)),
            disk_usage: 1024,
        };

        assert_eq!(
            entry,
            QueueTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        let empty_entry = QueueTableEntry {
            head: None,
            tail: None,
            disk_usage: 0,
        };

        assert_eq!(
            empty_entry,
            QueueTableEntry::from_bytes(empty_entry.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }

    #[tokio::test]
    async fn convert_indexed_file_table_entry() -> anyhow::Result<()> {
        let entry = IndexedFileTableEntry {
            size: 200,
            mtime: UnixTime::new(1234567890, 111),
            hash: Hash([0xf0; 32]),
            outdated_by: None,
        };

        assert_eq!(
            entry,
            IndexedFileTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }

    #[tokio::test]
    async fn convert_indexed_file_table_entry_outdated() -> anyhow::Result<()> {
        let entry = IndexedFileTableEntry {
            size: 200,
            mtime: UnixTime::new(1234567890, 111),
            hash: Hash([0xf0; 32]),
            outdated_by: Some(Hash([2; 32])),
        };

        assert_eq!(
            entry,
            IndexedFileTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }

    #[tokio::test]
    async fn convert_history_table_entry() -> anyhow::Result<()> {
        let add = HistoryTableEntry::Add(realize_types::Path::parse("foo/bar.txt")?);
        assert_eq!(
            add,
            HistoryTableEntry::from_bytes(add.clone().to_bytes()?.as_slice())?
        );

        let remove =
            HistoryTableEntry::Remove(realize_types::Path::parse("foo/bar.txt")?, Hash([0xfa; 32]));
        assert_eq!(
            remove,
            HistoryTableEntry::from_bytes(remove.clone().to_bytes()?.as_slice())?
        );

        let replace = HistoryTableEntry::Replace(
            realize_types::Path::parse("foo/bar.txt")?,
            Hash([0x1a; 32]),
        );
        assert_eq!(
            replace,
            HistoryTableEntry::from_bytes(replace.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }
    #[tokio::test]
    async fn convert_failed_job_table_entry_retry_after() -> anyhow::Result<()> {
        let entry = FailedJobTableEntry {
            failure_count: 3,
            retry: RetryJob::After(UnixTime::from_secs(1234567890)),
        };

        assert_eq!(
            entry,
            FailedJobTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }

    #[tokio::test]
    async fn convert_failed_job_table_entry_retry_when_peer_connects() -> anyhow::Result<()> {
        let entry = FailedJobTableEntry {
            failure_count: 3,
            retry: RetryJob::WhenPeerConnects,
        };

        assert_eq!(
            entry,
            FailedJobTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }

    #[tokio::test]
    async fn convert_mark_table_entry() -> anyhow::Result<()> {
        let entry = MarkTableEntry { mark: Mark::Own };

        assert_eq!(
            entry,
            MarkTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        let entry = MarkTableEntry { mark: Mark::Keep };

        assert_eq!(
            entry,
            MarkTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }

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
            outdated_by: Some(Hash([3u8; 32])),
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
            outdated_by: None,
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
    fn convert_file_table_key_default() -> anyhow::Result<()> {
        let key = FileTableKey::Default(Inode(12345));

        // Test round-trip conversion
        let bytes = FileTableKey::as_bytes(&key);
        let converted = FileTableKey::from_bytes(&bytes);
        assert_eq!(key, converted);

        Ok(())
    }

    #[test]
    fn convert_file_table_key_local_copy() -> anyhow::Result<()> {
        let key = FileTableKey::LocalCopy(Inode(67890));

        // Test round-trip conversion
        let bytes = FileTableKey::as_bytes(&key);
        let converted = FileTableKey::from_bytes(&bytes);
        assert_eq!(key, converted);

        Ok(())
    }

    #[test]
    fn convert_file_table_key_peer_copy() -> anyhow::Result<()> {
        let key = FileTableKey::PeerCopy(Inode(11111), Peer::from("peer1"));

        // Test round-trip conversion
        let bytes = FileTableKey::as_bytes(&key);
        let converted = FileTableKey::from_bytes(&bytes);
        assert_eq!(key, converted);

        Ok(())
    }

    #[test]
    fn convert_file_table_key_invalid() -> anyhow::Result<()> {
        let key = FileTableKey::Invalid;

        // Test round-trip conversion
        let bytes = FileTableKey::as_bytes(&key);
        let converted = FileTableKey::from_bytes(&bytes);
        assert_eq!(key, converted);

        Ok(())
    }

    #[test]
    fn convert_file_table_key_all_variants() -> anyhow::Result<()> {
        let test_cases = vec![
            FileTableKey::Default(Inode(1)),
            FileTableKey::Default(Inode(0xFFFFFFFFFFFFFFFF)),
            FileTableKey::LocalCopy(Inode(2)),
            FileTableKey::LocalCopy(Inode(0xFFFFFFFFFFFFFFFF)),
            FileTableKey::PeerCopy(Inode(3), Peer::from("short")),
            FileTableKey::PeerCopy(
                Inode(4),
                Peer::from("very_long_peer_name_that_might_cause_issues"),
            ),
            FileTableKey::Invalid,
        ];

        for key in test_cases {
            let bytes = FileTableKey::as_bytes(&key);
            let converted = FileTableKey::from_bytes(&bytes);
            assert_eq!(key, converted, "Failed for key: {:?}", key);
        }

        Ok(())
    }

    #[test]
    fn file_table_key_byte_comparison_behavior() {
        // Test that byte comparison behavior is consistent and documented

        let test_cases = vec![
            FileTableKey::Default(Inode(1)),
            FileTableKey::Default(Inode(2)),
            FileTableKey::LocalCopy(Inode(1)),
            FileTableKey::LocalCopy(Inode(2)),
            FileTableKey::PeerCopy(Inode(1), Peer::from("a")),
            FileTableKey::PeerCopy(Inode(1), Peer::from("b")),
            FileTableKey::PeerCopy(Inode(2), Peer::from("a")),
        ];

        // Test that byte comparison is consistent (same inputs always give same result)
        for i in 0..test_cases.len() {
            for j in 0..test_cases.len() {
                let key1 = &test_cases[i];
                let key2 = &test_cases[j];

                let bytes1 = FileTableKey::as_bytes(key1);
                let bytes2 = FileTableKey::as_bytes(key2);
                let byte_cmp = FileTableKey::compare(&bytes1, &bytes2);

                // Test consistency: reverse comparison should be opposite
                let byte_cmp_reverse = FileTableKey::compare(&bytes2, &bytes1);
                assert_eq!(
                    byte_cmp,
                    byte_cmp_reverse.reverse(),
                    "Byte comparison not consistent for {:?} vs {:?}",
                    key1,
                    key2
                );

                // Test reflexivity: same key should compare equal
                if i == j {
                    assert_eq!(
                        byte_cmp,
                        std::cmp::Ordering::Equal,
                        "Byte comparison not reflexive for {:?}",
                        key1
                    );
                }
            }
        }
    }

    #[test]
    fn file_table_key_byte_comparison_matches_object_comparison() {
        // Test that byte comparison matches object comparison for all valid keys
        let test_cases = vec![
            FileTableKey::Default(Inode(1)),
            FileTableKey::Default(Inode(2)),
            FileTableKey::Default(Inode(0x110000)),
            FileTableKey::LocalCopy(Inode(1)),
            FileTableKey::LocalCopy(Inode(2)),
            FileTableKey::Default(Inode(0x110000)),
            FileTableKey::PeerCopy(Inode(1), Peer::from("a")),
            FileTableKey::PeerCopy(Inode(1), Peer::from("b")),
            FileTableKey::PeerCopy(Inode(2), Peer::from("a")),
            FileTableKey::PeerCopy(Inode(0x110000), Peer::from("a")),
        ];

        for i in 0..test_cases.len() {
            for j in 0..test_cases.len() {
                let key1 = &test_cases[i];
                let key2 = &test_cases[j];

                let object_cmp = key1.cmp(key2);
                let bytes1 = FileTableKey::as_bytes(key1);
                let bytes2 = FileTableKey::as_bytes(key2);
                let byte_cmp = FileTableKey::compare(&bytes1, &bytes2);

                assert_eq!(
                    object_cmp, byte_cmp,
                    "Comparison mismatch for {:?} vs {:?}",
                    key1, key2
                );
            }
        }
    }

    #[test]
    fn file_table_key_edge_cases() -> anyhow::Result<()> {
        // Test edge cases
        let edge_cases = vec![
            FileTableKey::Default(Inode(1)),                   // Non-zero inode
            FileTableKey::LocalCopy(Inode(1)),                 // Non-zero inode
            FileTableKey::PeerCopy(Inode(1), Peer::from("")),  // Empty peer
            FileTableKey::PeerCopy(Inode(1), Peer::from("a")), // Single char peer
        ];

        for key in edge_cases {
            let bytes = FileTableKey::as_bytes(&key);
            let converted = FileTableKey::from_bytes(&bytes);
            assert_eq!(key, converted, "Failed for edge case: {:?}", key);
        }

        // Test that Inode(0) gets converted to Invalid (this is the intended behavior)
        let zero_inode_cases = vec![
            FileTableKey::Default(Inode(0)),
            FileTableKey::LocalCopy(Inode(0)),
            FileTableKey::PeerCopy(Inode(0), Peer::from("")),
        ];

        for key in zero_inode_cases {
            let bytes = FileTableKey::as_bytes(&key);
            let converted = FileTableKey::from_bytes(&bytes);
            assert_eq!(
                converted,
                FileTableKey::Invalid,
                "Expected Invalid for zero inode case: {:?}",
                key
            );
        }

        Ok(())
    }

    #[test]
    fn file_table_key_serialization_format() {
        // Test that the serialization format is correct
        let key = FileTableKey::Default(Inode(0x1021a3));
        let bytes = FileTableKey::as_bytes(&key);

        // Should be exactly 8 bytes for Default (just the inode),
        // with big endian encoding.
        assert_eq!(bytes.len(), 8);
        assert_eq!(bytes, [0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x21, 0xa3]);

        let key = FileTableKey::LocalCopy(Inode(67890));
        let bytes = FileTableKey::as_bytes(&key);

        // Should be 9 bytes for LocalCopy (inode + 0 byte)
        assert_eq!(bytes.len(), 9);
        assert_eq!(&bytes[0..8], Inode(67890).as_u64().to_be_bytes());
        assert_eq!(bytes[8], 0);

        let key = FileTableKey::PeerCopy(Inode(11111), Peer::from("test"));
        let bytes = FileTableKey::as_bytes(&key);

        // Should be 13 bytes for PeerCopy (inode + 1 byte + peer string)
        assert_eq!(bytes.len(), 13);
        assert_eq!(&bytes[0..8], Inode(11111).as_u64().to_be_bytes());
        assert_eq!(bytes[8], 1);
        assert_eq!(&bytes[9..], b"test");
    }
}
