use crate::StorageError;
use crate::types::PathId;
use crate::utils::holder::{ByteConversionError, ByteConvertible, NamedType};
use capnp::message::ReaderOptions;
use capnp::serialize_packed;
use realize_types::{self, Arena, ByteRanges, Hash, Path, Peer, UnixTime};

#[allow(dead_code)]
#[allow(unknown_lints)]
#[allow(clippy::uninlined_format_args)]
#[allow(clippy::extra_unused_type_parameters)]
mod history_capnp {
    include!(concat!(env!("OUT_DIR"), "/arena/history_capnp.rs"));
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
    /// First node in the queue (PathId)
    pub head: Option<PathId>,
    /// Last node in the queue (PathId)
    pub tail: Option<PathId>,
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
            Some(PathId(head_value))
        } else {
            None
        };

        let tail_value = reader.get_tail();
        let tail = if tail_value != 0 {
            Some(PathId(tail_value))
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

    /// Hash of the content.
    pub content_hash: Hash,

    /// Size of the content.
    pub content_size: u64,

    /// If true, content of the file was verified against the hash.
    pub verified: bool,

    /// Queue ID enum
    pub queue: LruQueueId,

    /// Next blob in the queue (PathId)
    pub next: Option<PathId>,

    /// Previous blob in the queue (PathId)
    pub prev: Option<PathId>,

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

        let content_hash = parse_hash(reader.get_content_hash()?)?;
        let next_value = reader.get_next();
        let next = if next_value != 0 {
            Some(PathId(next_value))
        } else {
            None
        };

        let prev_value = reader.get_prev();
        let prev = if prev_value != 0 {
            Some(PathId(prev_value))
        } else {
            None
        };

        let queue = reader.get_queue()?;

        Ok(BlobTableEntry {
            written_areas: parse_byte_ranges(reader.get_written_areas()?)?,
            content_hash,
            content_size: reader.get_content_size(),
            verified: reader.get_verified(),
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

        builder.set_content_hash(&self.content_hash.0);
        builder.set_content_size(self.content_size);
        builder.set_verified(self.verified);
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

    /// The file was branched from another file.
    Branch(realize_types::Path, realize_types::Path, Hash, Option<Hash>),

    /// The file was moved from another file.
    Rename(realize_types::Path, realize_types::Path, Hash, Option<Hash>),
}

impl NamedType for HistoryTableEntry {
    fn typename() -> &'static str {
        "index.file"
    }
}

impl ByteConvertible<HistoryTableEntry> for HistoryTableEntry {
    fn from_bytes(data: &[u8]) -> Result<HistoryTableEntry, ByteConversionError> {
        let message_reader = serialize_packed::read_message(&mut &data[..], ReaderOptions::new())?;
        let msg: history_capnp::history_table_entry::Reader =
            message_reader.get_root::<history_capnp::history_table_entry::Reader>()?;
        match msg.which()? {
            history_capnp::history_table_entry::Which::Add(add_reader) => {
                let add_reader = add_reader?;
                Ok(HistoryTableEntry::Add(parse_path(add_reader.get_path()?)?))
            }
            history_capnp::history_table_entry::Which::Replace(replace_reader) => {
                let replace_reader = replace_reader?;
                Ok(HistoryTableEntry::Replace(
                    parse_path(replace_reader.get_path()?)?,
                    parse_hash(replace_reader.get_old_hash()?)?,
                ))
            }
            history_capnp::history_table_entry::Which::Remove(remove_reader) => {
                let remove_reader = remove_reader?;
                Ok(HistoryTableEntry::Remove(
                    parse_path(remove_reader.get_path()?)?,
                    parse_hash(remove_reader.get_old_hash()?)?,
                ))
            }
            history_capnp::history_table_entry::Which::Drop(drop_reader) => {
                let drop_reader = drop_reader?;
                Ok(HistoryTableEntry::Drop(
                    parse_path(drop_reader.get_path()?)?,
                    parse_hash(drop_reader.get_old_hash()?)?,
                ))
            }
            history_capnp::history_table_entry::Which::Branch(branch_reader) => {
                let branch_reader = branch_reader?;
                Ok(HistoryTableEntry::Branch(
                    parse_path(branch_reader.get_path()?)?,
                    parse_path(branch_reader.get_dest_path()?)?,
                    parse_hash(branch_reader.get_hash()?)?,
                    if branch_reader.get_old_hash()?.is_empty() {
                        None
                    } else {
                        Some(parse_hash(branch_reader.get_old_hash()?)?)
                    },
                ))
            }
            history_capnp::history_table_entry::Which::Rename(rename_reader) => {
                let rename_reader = rename_reader?;
                Ok(HistoryTableEntry::Rename(
                    parse_path(rename_reader.get_path()?)?,
                    parse_path(rename_reader.get_dest_path()?)?,
                    parse_hash(rename_reader.get_hash()?)?,
                    if rename_reader.get_old_hash()?.is_empty() {
                        None
                    } else {
                        Some(parse_hash(rename_reader.get_old_hash()?)?)
                    },
                ))
            }
        }
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let builder: history_capnp::history_table_entry::Builder =
            message.init_root::<history_capnp::history_table_entry::Builder>();

        match self {
            HistoryTableEntry::Add(path) => {
                let mut add_builder = builder.init_add();
                add_builder.set_path(path.as_str());
            }
            HistoryTableEntry::Replace(path, old_hash) => {
                let mut replace_builder = builder.init_replace();
                replace_builder.set_path(path.as_str());
                replace_builder.set_old_hash(&old_hash.0);
            }
            HistoryTableEntry::Remove(path, old_hash) => {
                let mut remove_builder = builder.init_remove();
                remove_builder.set_path(path.as_str());
                remove_builder.set_old_hash(&old_hash.0);
            }
            HistoryTableEntry::Drop(path, old_hash) => {
                let mut drop_builder = builder.init_drop();
                drop_builder.set_path(path.as_str());
                drop_builder.set_old_hash(&old_hash.0);
            }
            HistoryTableEntry::Branch(path, dest_path, hash, old_hash) => {
                let mut branch_builder = builder.init_branch();
                branch_builder.set_path(path.as_str());
                branch_builder.set_dest_path(dest_path.as_str());
                branch_builder.set_hash(&hash.0);
                if let Some(old_hash) = old_hash {
                    branch_builder.set_old_hash(&old_hash.0);
                }
            }
            HistoryTableEntry::Rename(path, dest_path, hash, old_hash) => {
                let mut rename_builder = builder.init_rename();
                rename_builder.set_path(path.as_str());
                rename_builder.set_dest_path(dest_path.as_str());
                rename_builder.set_hash(&hash.0);
                if let Some(old_hash) = old_hash {
                    rename_builder.set_old_hash(&old_hash.0);
                }
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
    pub size: u64,
    pub hash: Hash,
    pub peers: Vec<Peer>,
}

/// Key for the file table of the [ArenaDatabase].
#[derive(Debug, Clone)]
pub enum CacheTableKey {
    /// The default entry, containing the selected version and its
    /// metadata for the cache.
    Default(PathId),
    /// An entry that represents another peer's copy.
    PeerCopy(PathId, Peer),

    /// A key that could not be parsed
    Invalid,
}

impl CacheTableKey {
    /// A range that covers all keys with the given pathid.
    pub fn range(pathid: PathId) -> std::ops::Range<CacheTableKey> {
        CacheTableKey::Default(pathid)..CacheTableKey::Default(pathid.plus(1))
    }

    /// The key's pathid
    pub fn pathid(&self) -> PathId {
        match self {
            CacheTableKey::Invalid => PathId::ZERO,
            CacheTableKey::Default(pathid) => *pathid,
            CacheTableKey::PeerCopy(pathid, _) => *pathid,
        }
    }

    fn variant_order(&self) -> u8 {
        match self {
            CacheTableKey::Invalid => 0,
            CacheTableKey::Default(_) => 1,
            CacheTableKey::PeerCopy(_, _) => 2,
        }
    }

    fn peer(&self) -> Option<&Peer> {
        match self {
            CacheTableKey::PeerCopy(_, peer) => Some(peer),
            _ => None,
        }
    }
}

impl PartialEq for CacheTableKey {
    fn eq(&self, other: &Self) -> bool {
        self.pathid() == other.pathid()
            && self.variant_order() == other.variant_order()
            && self.peer() == other.peer()
    }
}

impl Eq for CacheTableKey {}

impl PartialOrd for CacheTableKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CacheTableKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // First compare by pathid
        let pathid_cmp = self.pathid().cmp(&other.pathid());
        if pathid_cmp != std::cmp::Ordering::Equal {
            return pathid_cmp;
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

impl Value for CacheTableKey {
    type SelfType<'a> = CacheTableKey;

    type AsBytes<'a> = Vec<u8>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let pathid = PathId(<u64>::from_be_bytes(
            data[0..8].try_into().unwrap_or([0; 8]),
        ));
        if pathid == PathId::ZERO {
            return CacheTableKey::Invalid;
        }
        match data.get(8) {
            None => CacheTableKey::Default(pathid),
            Some(1) => {
                CacheTableKey::PeerCopy(pathid, Peer::from(str::from_utf8(&data[9..]).unwrap()))
            }
            Some(_) => CacheTableKey::Invalid,
        }
    }

    fn as_bytes<'a, 'b: 'a>(value: &CacheTableKey) -> Vec<u8>
    where
        Self: 'a,
        Self: 'b,
    {
        let mut ret = vec![];
        ret.extend_from_slice(&value.pathid().as_u64().to_be_bytes());
        match value {
            CacheTableKey::Default(_) | CacheTableKey::Invalid => {}
            CacheTableKey::PeerCopy(_, peer) => {
                ret.push(1);
                ret.extend_from_slice(peer.as_str().as_bytes());
            }
        };

        ret
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("CacheTableKey")
    }
}

impl Key for CacheTableKey {
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
    /// The size of the file in bytes.
    pub size: u64,
    /// The modification time of the file.
    pub mtime: UnixTime,
    /// The path to use to fetch file content in the peer.
    pub path: Path,
    /// Hash of the specific version of the content the peer has.
    pub hash: Hash,
    // If set, a version is known to exist that replaces the version
    // in this entry.
    pub outdated_by: Option<Hash>,
    /// PathId this specific version was branched from
    pub branched_from: Option<PathId>,
}

impl FileTableEntry {
    pub fn new(path: Path, size: u64, mtime: UnixTime, hash: Hash) -> Self {
        Self {
            size,
            mtime,
            path,
            hash,
            outdated_by: None,
            branched_from: None,
        }
    }
}

/// A simplified directory entry that only contains modification time.
#[derive(Debug, Clone, PartialEq)]
pub struct DirtableEntry {
    /// The modification time of the directory.
    pub mtime: UnixTime,
}

/// An entry that can be either a file or directory.
#[derive(Debug, Clone, PartialEq)]
pub enum CacheTableEntry {
    /// A file entry
    File(FileTableEntry),
    /// A directory entry
    Dir(DirtableEntry),
}

impl NamedType for FileTableEntry {
    fn typename() -> &'static str {
        "FileTableEntry"
    }
}

impl NamedType for CacheTableEntry {
    fn typename() -> &'static str {
        "CacheTableEntry"
    }
}

impl ByteConvertible<FileTableEntry> for FileTableEntry {
    fn from_bytes(data: &[u8]) -> Result<FileTableEntry, ByteConversionError> {
        let message_reader = serialize_packed::read_message(&mut &data[..], ReaderOptions::new())?;
        let msg: cache_capnp::file_table_entry::Reader =
            message_reader.get_root::<cache_capnp::file_table_entry::Reader>()?;

        parse_file_table_entry(msg)
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let builder: cache_capnp::file_table_entry::Builder =
            message.init_root::<cache_capnp::file_table_entry::Builder>();

        fill_file_table_entry(builder, self);

        let mut buffer: Vec<u8> = Vec::new();
        serialize_packed::write_message(&mut buffer, &message)?;

        Ok(buffer)
    }
}

fn fill_file_table_entry(
    mut builder: cache_capnp::file_table_entry::Builder<'_>,
    entry: &FileTableEntry,
) {
    builder.set_size(entry.size);
    let mut mtime = builder.reborrow().init_mtime();
    mtime.set_secs(entry.mtime.as_secs());
    mtime.set_nsecs(entry.mtime.subsec_nanos());
    builder.set_path(entry.path.as_str());
    builder.set_hash(&entry.hash.0);
    builder.set_branched_from(PathId::from_optional(entry.branched_from));

    if let Some(hash) = &entry.outdated_by {
        builder.set_outdated_by(&hash.0)
    }
}

fn parse_file_table_entry(
    msg: cache_capnp::file_table_entry::Reader<'_>,
) -> Result<FileTableEntry, ByteConversionError> {
    let mtime = msg.get_mtime()?;
    let outdated_by: &[u8] = msg.get_outdated_by()?;
    let outdated_by = if outdated_by.is_empty() {
        None
    } else {
        Some(parse_hash(outdated_by)?)
    };
    Ok(FileTableEntry {
        size: msg.get_size(),
        mtime: UnixTime::new(mtime.get_secs(), mtime.get_nsecs()),
        path: Path::parse(msg.get_path()?.to_str()?)?,
        hash: parse_hash(msg.get_hash()?)?,
        outdated_by,
        branched_from: PathId::as_optional(msg.get_branched_from()),
    })
}

impl ByteConvertible<CacheTableEntry> for CacheTableEntry {
    fn from_bytes(data: &[u8]) -> Result<CacheTableEntry, ByteConversionError> {
        let message_reader = serialize_packed::read_message(&mut &data[..], ReaderOptions::new())?;
        let msg: cache_capnp::cache_table_entry::Reader =
            message_reader.get_root::<cache_capnp::cache_table_entry::Reader>()?;

        match msg.which()? {
            cache_capnp::cache_table_entry::File(file_entry) => {
                let file_entry = file_entry?;
                Ok(CacheTableEntry::File(parse_file_table_entry(file_entry)?))
            }
            cache_capnp::cache_table_entry::Dir(dir_entry) => {
                let dir_entry = dir_entry?;
                let mtime = dir_entry.get_mtime()?;

                let dir_table_entry = DirtableEntry {
                    mtime: UnixTime::new(mtime.get_secs(), mtime.get_nsecs()),
                };

                Ok(CacheTableEntry::Dir(dir_table_entry))
            }
        }
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let builder: cache_capnp::cache_table_entry::Builder =
            message.init_root::<cache_capnp::cache_table_entry::Builder>();

        match self {
            CacheTableEntry::File(file_entry) => {
                fill_file_table_entry(builder.init_file(), file_entry);
            }
            CacheTableEntry::Dir(dir_entry) => {
                let dir_builder = builder.init_dir();
                let mut mtime = dir_builder.init_mtime();
                mtime.set_secs(dir_entry.mtime.as_secs());
                mtime.set_nsecs(dir_entry.mtime.subsec_nanos());
            }
        }

        let mut buffer: Vec<u8> = Vec::new();
        serialize_packed::write_message(&mut buffer, &message)?;

        Ok(buffer)
    }
}

impl CacheTableEntry {
    /// Extract the file entry, returning an error if this is a directory entry.
    pub fn expect_file(self) -> Result<FileTableEntry, StorageError> {
        match self {
            CacheTableEntry::File(file_entry) => Ok(file_entry),
            CacheTableEntry::Dir(_) => Err(StorageError::IsADirectory),
        }
    }

    /// Extract the file entry or return None
    pub fn file(self) -> Option<FileTableEntry> {
        match self {
            CacheTableEntry::File(file_entry) => Some(file_entry),
            CacheTableEntry::Dir(_) => None,
        }
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
    /// File version.
    pub hash: Hash,
}

/// The metadata of a directory.
#[derive(Debug, Clone, PartialEq)]
pub struct DirMetadata {
    pub read_only: bool,

    /// The modification time of the directory.
    pub mtime: UnixTime,
}

/// Unified metadata that can represent either a file or directory.
#[derive(Debug, Clone, PartialEq)]
pub enum Metadata {
    /// File metadata
    File(FileMetadata),
    /// Directory metadata
    Dir(DirMetadata),
}

impl Metadata {
    /// Extract the file metadata, returning an error if this is a directory.
    pub fn expect_file(self) -> Result<FileMetadata, StorageError> {
        match self {
            Metadata::File(file_metadata) => Ok(file_metadata),
            Metadata::Dir(_) => Err(StorageError::IsADirectory),
        }
    }

    /// Extract the directory metadata, returning an error if this is a file.
    pub fn expect_dir(self) -> Result<DirMetadata, StorageError> {
        match self {
            Metadata::Dir(dir_metadata) => Ok(dir_metadata),
            Metadata::File(_) => Err(StorageError::NotADirectory),
        }
    }

    /// Get the modification time regardless of whether this is a file or directory.
    pub fn mtime(&self) -> UnixTime {
        match self {
            Metadata::File(file_metadata) => file_metadata.mtime,
            Metadata::Dir(dir_metadata) => dir_metadata.mtime,
        }
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
            content_hash: Hash([2u8; 32]),
            content_size: 100,
            verified: false,
            queue: LruQueueId::WorkingArea,
            next: Some(PathId(0x0101010101010101)),
            prev: Some(PathId(0x0202020202020202)),
            disk_usage: 1024,
        };

        assert_eq!(
            entry,
            BlobTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        let empty_entry = BlobTableEntry {
            written_areas: realize_types::ByteRanges::from_ranges(vec![]),
            content_hash: Hash([0x03; 32]),
            content_size: 0,
            verified: false,
            queue: LruQueueId::WorkingArea,
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
            head: Some(PathId(0x0101010101010101)),
            tail: Some(PathId(0x0202020202020202)),
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
            size: 200,
            mtime: UnixTime::from_secs(1234567890),
            path: Path::parse("foo/bar.txt")?,
            hash: Hash([0xa1u8; 32]),
            outdated_by: Some(Hash([3u8; 32])),
            branched_from: Some(PathId(123)),
        };

        assert_eq!(
            entry,
            FileTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }

    #[test]
    fn convert_file_table_key_default() -> anyhow::Result<()> {
        let key = CacheTableKey::Default(PathId(12345));

        // Test round-trip conversion
        let bytes = CacheTableKey::as_bytes(&key);
        let converted = CacheTableKey::from_bytes(&bytes);
        assert_eq!(key, converted);

        Ok(())
    }

    #[test]
    fn convert_file_table_key_peer_copy() -> anyhow::Result<()> {
        let key = CacheTableKey::PeerCopy(PathId(11111), Peer::from("peer1"));

        // Test round-trip conversion
        let bytes = CacheTableKey::as_bytes(&key);
        let converted = CacheTableKey::from_bytes(&bytes);
        assert_eq!(key, converted);

        Ok(())
    }

    #[test]
    fn convert_file_table_key_invalid() -> anyhow::Result<()> {
        let key = CacheTableKey::Invalid;

        // Test round-trip conversion
        let bytes = CacheTableKey::as_bytes(&key);
        let converted = CacheTableKey::from_bytes(&bytes);
        assert_eq!(key, converted);

        Ok(())
    }

    #[test]
    fn convert_file_table_key_all_variants() -> anyhow::Result<()> {
        let test_cases = vec![
            CacheTableKey::Default(PathId(1)),
            CacheTableKey::Default(PathId(0xFFFFFFFFFFFFFFFF)),
            CacheTableKey::PeerCopy(PathId(3), Peer::from("short")),
            CacheTableKey::PeerCopy(
                PathId(4),
                Peer::from("very_long_peer_name_that_might_cause_issues"),
            ),
            CacheTableKey::Invalid,
        ];

        for key in test_cases {
            let bytes = CacheTableKey::as_bytes(&key);
            let converted = CacheTableKey::from_bytes(&bytes);
            assert_eq!(key, converted, "Failed for key: {:?}", key);
        }

        Ok(())
    }

    #[test]
    fn file_table_key_byte_comparison_behavior() {
        // Test that byte comparison behavior is consistent and documented

        let test_cases = vec![
            CacheTableKey::Default(PathId(1)),
            CacheTableKey::Default(PathId(2)),
            CacheTableKey::PeerCopy(PathId(1), Peer::from("a")),
            CacheTableKey::PeerCopy(PathId(1), Peer::from("b")),
            CacheTableKey::PeerCopy(PathId(2), Peer::from("a")),
        ];

        // Test that byte comparison is consistent (same inputs always give same result)
        for i in 0..test_cases.len() {
            for j in 0..test_cases.len() {
                let key1 = &test_cases[i];
                let key2 = &test_cases[j];

                let bytes1 = CacheTableKey::as_bytes(key1);
                let bytes2 = CacheTableKey::as_bytes(key2);
                let byte_cmp = CacheTableKey::compare(&bytes1, &bytes2);

                // Test consistency: reverse comparison should be opposite
                let byte_cmp_reverse = CacheTableKey::compare(&bytes2, &bytes1);
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
            CacheTableKey::Default(PathId(1)),
            CacheTableKey::Default(PathId(2)),
            CacheTableKey::Default(PathId(0x110000)),
            CacheTableKey::PeerCopy(PathId(1), Peer::from("a")),
            CacheTableKey::PeerCopy(PathId(1), Peer::from("b")),
            CacheTableKey::PeerCopy(PathId(2), Peer::from("a")),
            CacheTableKey::PeerCopy(PathId(0x110000), Peer::from("a")),
        ];

        for i in 0..test_cases.len() {
            for j in 0..test_cases.len() {
                let key1 = &test_cases[i];
                let key2 = &test_cases[j];

                let object_cmp = key1.cmp(key2);
                let bytes1 = CacheTableKey::as_bytes(key1);
                let bytes2 = CacheTableKey::as_bytes(key2);
                let byte_cmp = CacheTableKey::compare(&bytes1, &bytes2);

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
            CacheTableKey::Default(PathId(1)), // Non-zero pathid
            CacheTableKey::PeerCopy(PathId(1), Peer::from("")), // Empty peer
            CacheTableKey::PeerCopy(PathId(1), Peer::from("a")), // Single char peer
        ];

        for key in edge_cases {
            let bytes = CacheTableKey::as_bytes(&key);
            let converted = CacheTableKey::from_bytes(&bytes);
            assert_eq!(key, converted, "Failed for edge case: {:?}", key);
        }

        // Test that PathId(0) gets converted to Invalid (this is the intended behavior)
        let zero_pathid_cases = vec![
            CacheTableKey::Default(PathId(0)),
            CacheTableKey::PeerCopy(PathId(0), Peer::from("")),
        ];

        for key in zero_pathid_cases {
            let bytes = CacheTableKey::as_bytes(&key);
            let converted = CacheTableKey::from_bytes(&bytes);
            assert_eq!(
                converted,
                CacheTableKey::Invalid,
                "Expected Invalid for zero pathid case: {:?}",
                key
            );
        }

        Ok(())
    }

    #[test]
    fn file_table_key_serialization_format() {
        // Test that the serialization format is correct
        let key = CacheTableKey::Default(PathId(0x1021a3));
        let bytes = CacheTableKey::as_bytes(&key);

        // Should be exactly 8 bytes for Default (just the pathid),
        // with big endian encoding.
        assert_eq!(bytes.len(), 8);
        assert_eq!(bytes, [0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x21, 0xa3]);

        let key = CacheTableKey::PeerCopy(PathId(11111), Peer::from("test"));
        let bytes = CacheTableKey::as_bytes(&key);

        // Should be 13 bytes for PeerCopy (pathid + 1 byte + peer string)
        assert_eq!(bytes.len(), 13);
        assert_eq!(&bytes[0..8], PathId(11111).as_u64().to_be_bytes());
        assert_eq!(bytes[8], 1);
        assert_eq!(&bytes[9..], b"test");
    }

    #[test]
    fn convert_cache_table_entry_file() -> anyhow::Result<()> {
        let file_entry = FileTableEntry {
            size: 200,
            mtime: UnixTime::from_secs(1234567890),
            path: Path::parse("foo/bar.txt")?,
            hash: Hash([0xa1u8; 32]),
            outdated_by: Some(Hash([3u8; 32])),
            branched_from: None,
        };

        let entry = CacheTableEntry::File(file_entry);

        assert_eq!(
            entry,
            CacheTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }

    #[test]
    fn convert_cache_table_entry_dir() -> anyhow::Result<()> {
        let dir_entry = DirtableEntry {
            mtime: UnixTime::from_secs(987654321),
        };

        let entry = CacheTableEntry::Dir(dir_entry);

        assert_eq!(
            entry,
            CacheTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }

    #[test]
    fn convert_cache_table_entry_both_variants() -> anyhow::Result<()> {
        // Test both variants in the same test
        let file_entry = FileTableEntry {
            size: 100,
            mtime: UnixTime::from_secs(1234567890),
            path: Path::parse("test/file.txt")?,
            hash: Hash([0x42u8; 32]),
            outdated_by: None,
            branched_from: None,
        };

        let dir_entry = DirtableEntry {
            mtime: UnixTime::from_secs(987654321),
        };

        let file_variant = CacheTableEntry::File(file_entry);
        let dir_variant = CacheTableEntry::Dir(dir_entry);

        // Test file variant round-trip
        assert_eq!(
            file_variant,
            CacheTableEntry::from_bytes(file_variant.clone().to_bytes()?.as_slice())?
        );

        // Test dir variant round-trip
        assert_eq!(
            dir_variant,
            CacheTableEntry::from_bytes(dir_variant.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }
}
