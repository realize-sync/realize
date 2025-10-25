use crate::StorageError;
use crate::types::PathId;
use crate::utils::holder::{ByteConversionError, ByteConvertible, NamedType};
use capnp::message::ReaderOptions;
use capnp::serialize_packed;
use realize_types::{self, Arena, ByteRanges, Hash, Path, Peer, UnixTime};
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::time::SystemTime;

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

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum FileRealm {
    /// File is available as a regular file on the local filesystem
    Local(PathBuf),
    /// File is remote. Its content may have been cached.
    Remote(CacheStatus),
}

impl FileRealm {
    pub fn is_local(&self) -> bool {
        matches!(self, FileRealm::Local(_))
    }

    pub fn is_remote(&self) -> bool {
        !self.is_local()
    }

    /// Return true if the file can be accessed without
    /// connecting to another host.
    pub fn is_available_offline(&self) -> bool {
        match self {
            FileRealm::Local(_) => true,
            FileRealm::Remote(CacheStatus::Complete) => true,
            FileRealm::Remote(CacheStatus::Verified) => true,
            _ => false,
        }
    }

    pub fn path(&self) -> Option<&std::path::Path> {
        match self {
            FileRealm::Local(path) => Some(path.as_path()),
            FileRealm::Remote(_) => None,
        }
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum CacheStatus {
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

/// Layer enum for cache table keys.
///
/// Used in combination with PathId to form cache table keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Layer {
    /// The default entry, containing an entry for a local file or a cached entry from one of the remote layers.
    Default,
    /// An entry that represents another peer's copy.
    Remote(Peer),
}

impl Layer {
    pub fn peer(&self) -> Option<Peer> {
        match self {
            Layer::Default => None,
            Layer::Remote(peer) => Some(*peer),
        }
    }
}

impl Value for Layer {
    type SelfType<'a> = Layer;

    type AsBytes<'a> = Vec<u8>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        if data.is_empty() {
            return Layer::Default;
        }
        match data.get(0) {
            Some(1) => Layer::Default,
            Some(2) => Layer::Remote(Peer::from(str::from_utf8(&data[1..]).unwrap())),
            _ => Layer::Default,
        }
    }

    fn as_bytes<'a, 'b: 'a>(value: &Layer) -> Vec<u8>
    where
        Self: 'a,
        Self: 'b,
    {
        let mut ret = vec![];
        match value {
            Layer::Default => ret.push(1),
            Layer::Remote(peer) => {
                ret.push(2);
                ret.extend_from_slice(peer.as_str().as_bytes());
            }
        };
        ret
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("Layer")
    }
}

impl Key for Layer {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        // The byte representation is designed so that byte comparison
        // matches object comparison.
        data1.cmp(data2)
    }
}

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
    Branch(realize_types::Path, realize_types::Path, Hash),

    /// The file was moved from another file.
    Rename(realize_types::Path, realize_types::Path, Hash),
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
                ))
            }
            history_capnp::history_table_entry::Which::Rename(rename_reader) => {
                let rename_reader = rename_reader?;
                Ok(HistoryTableEntry::Rename(
                    parse_path(rename_reader.get_path()?)?,
                    parse_path(rename_reader.get_dest_path()?)?,
                    parse_hash(rename_reader.get_hash()?)?,
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
            HistoryTableEntry::Branch(path, dest_path, hash) => {
                let mut branch_builder = builder.init_branch();
                branch_builder.set_path(path.as_str());
                branch_builder.set_dest_path(dest_path.as_str());
                branch_builder.set_hash(&hash.0);
            }
            HistoryTableEntry::Rename(path, dest_path, hash) => {
                let mut rename_builder = builder.init_rename();
                rename_builder.set_path(path.as_str());
                rename_builder.set_dest_path(dest_path.as_str());
                rename_builder.set_hash(&hash.0);
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

impl std::fmt::Display for Mark {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Mark::Watch => "watch",
            Mark::Keep => "keep",
            Mark::Own => "own",
        })
    }
}

impl Mark {
    /// Parse a mark value from a string representation
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "watch" => Some(Mark::Watch),
            "keep" => Some(Mark::Keep),
            "own" => Some(Mark::Own),
            _ => None,
        }
    }
}

#[cfg(test)]
mod mark_tests {
    use super::*;

    #[test]
    fn test_mark_parse_valid_values() {
        assert_eq!(Mark::parse("watch"), Some(Mark::Watch));
        assert_eq!(Mark::parse("keep"), Some(Mark::Keep));
        assert_eq!(Mark::parse("own"), Some(Mark::Own));
    }

    #[test]
    fn test_mark_parse_invalid_values() {
        assert_eq!(Mark::parse(""), None);
        assert_eq!(Mark::parse("invalid"), None);
        assert_eq!(Mark::parse("Watch"), None); // Case sensitive
        assert_eq!(Mark::parse("KEEP"), None);
        assert_eq!(Mark::parse(" watch "), None); // No trimming
    }

    #[test]
    fn test_mark_display() {
        assert_eq!(Mark::Watch.to_string(), "watch");
        assert_eq!(Mark::Keep.to_string(), "keep");
        assert_eq!(Mark::Own.to_string(), "own");
    }

    #[test]
    fn test_mark_parse_round_trip() {
        let marks = [Mark::Watch, Mark::Keep, Mark::Own];
        for mark in marks {
            let str_repr = mark.to_string();
            let parsed = Mark::parse(&str_repr);
            assert_eq!(parsed, Some(mark));
        }
    }
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
pub struct RemoteAvailability {
    pub arena: Arena,
    pub path: Path,
    pub size: u64,
    pub hash: Hash,
    pub peers: Vec<Peer>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum FileAlternative {
    /// Reports a local file version, either indexed or a file with local modifications.
    Local(Version),
    /// Reports a file moved or linked from a remote file
    Branched(Path, Hash),
    /// Reports a file version available from a remote peer.
    Remote(Peer, Hash, u64, UnixTime),
}

/// Specifies the type of file entry (local, cached, or branched from another path).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileEntryKind {
    /// This is a regular file on the local disk, in datadir. Such
    /// files are shared remotely.
    LocalFile,
    /// This is a special file on the local disk, in datadir. Such
    /// files are not shared remotely and may hide remote files.
    SpecialFile,
    /// This is a file from another peer. There might be a blob
    /// associated to it which contains a local copy of the data.
    RemoteFile,
    /// This is a file from another peer branched locally from
    /// another. The file may or may not have been branched on the
    /// local peers.
    Branched(PathId),
}

impl FileEntryKind {
    pub fn is_local(&self) -> bool {
        match self {
            FileEntryKind::LocalFile => true,
            FileEntryKind::SpecialFile => true,
            FileEntryKind::RemoteFile => false,
            FileEntryKind::Branched(_) => false,
        }
    }

    pub fn is_remote(&self) -> bool {
        match self {
            FileEntryKind::LocalFile => false,
            FileEntryKind::SpecialFile => false,
            FileEntryKind::RemoteFile => true,
            FileEntryKind::Branched(_) => true,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Version {
    /// The file has unknown modifications. It hasn't been indexed and
    /// might still be in the process of being modified.
    ///
    /// The modified file is known to be based on a previously indexed
    /// file with the given hash. This can serve as `old_hash` to
    /// report the file through notifications, once it is indexed.
    ///
    /// If no hash is available, the modified file is new.
    Modified(Option<Hash>),

    /// File content is known to match the given hash.
    Indexed(Hash),
}

impl Default for Version {
    fn default() -> Self {
        Version::Modified(None)
    }
}

impl Version {
    /// Turn the given version into a [Version::Modified]. This can be
    /// called after noticing a local modification to a previously
    /// indexed file.
    pub fn modification_of(base: Option<Self>) -> Version {
        match base {
            None => Version::Modified(None),
            Some(Version::Modified(hash)) => Version::Modified(hash),
            Some(Version::Indexed(hash)) => Version::Modified(Some(hash)),
        }
    }

    /// Checks whether the given hash matches the current version.
    ///
    /// Only [Version::Indexed] will ever match.
    pub fn matches_hash(&self, other: &Hash) -> bool {
        if let Version::Indexed(hash) = self {
            return *hash == *other;
        }

        false
    }

    /// Return the hash this version is based on. This is the hash of
    /// last known indexed version. Any number of modifications might
    /// have been applied since that hash was computed.
    pub fn base_hash(&self) -> Option<&Hash> {
        match self {
            Version::Modified(Some(hash)) => Some(hash),
            Version::Modified(None) => None,
            Version::Indexed(hash) => Some(hash),
        }
    }

    /// Return the hash that the file was indexed with.
    pub fn indexed_hash(&self) -> Option<&Hash> {
        if let Version::Indexed(hash) = self {
            Some(hash)
        } else {
            None
        }
    }

    /// Return the hash that the file was indexed with or fail with [StorageError::NotIndexed].
    pub fn expect_indexed(&self) -> Result<&Hash, StorageError> {
        if let Version::Indexed(hash) = self {
            Ok(hash)
        } else {
            Err(StorageError::NotIndexed)
        }
    }
}

/// An entry in the file table.
#[derive(Debug, Clone, PartialEq)]
pub struct FileTableEntry {
    /// The size of the file in bytes.
    pub size: u64,
    /// The modification time of the file.
    pub mtime: UnixTime,
    /// Specific version of this content. Always [Version::Indexed]
    /// for cached file.
    pub version: Version,
    /// The type of file entry (local, cached, or branched from another path)
    pub kind: FileEntryKind,
}

impl FileTableEntry {
    pub fn new(size: u64, mtime: UnixTime, version: Version, kind: FileEntryKind) -> Self {
        Self {
            size,
            mtime,
            version,
            kind,
        }
    }

    /// Returns true if this is a local file on disk
    pub fn is_local(&self) -> bool {
        self.kind.is_local()
    }

    /// Returns true if this is a remote file (a cached or branched file)
    pub fn is_remote(&self) -> bool {
        self.kind.is_remote()
    }

    /// Check whether `file_path` matches this entry
    pub(crate) fn matches_file<P: AsRef<std::path::Path>>(&self, file_path: P) -> bool {
        match self.kind {
            FileEntryKind::LocalFile => {
                if let Ok(m) = file_path.as_ref().symlink_metadata()
                    && m.is_file()
                    && self.size == m.len()
                    && self.mtime == UnixTime::mtime(&m)
                {
                    return true;
                }
            }
            FileEntryKind::SpecialFile => {
                if let Ok(m) = file_path.as_ref().symlink_metadata() {
                    return !m.is_dir() && !m.is_file();
                }
            }
            _ => {}
        }

        false
    }
}

/// A simplified directory entry that only contains modification time.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DirTableEntry {
    /// True for a directory that should have a local equivalent in
    /// the datadir.
    pub local: bool,

    /// The modification time of the directory, for remote file
    /// changes.
    pub mtime: Option<UnixTime>,
}

impl DirTableEntry {
    /// True if this is a local dir.
    ///
    /// Note that a dir can be both local and remote.
    pub fn is_local(&self) -> bool {
        self.local
    }

    /// True if this is a remote dir.
    ///
    /// Note that a dir can be both local and remote.
    pub fn is_remote(&self) -> bool {
        self.mtime.is_some()
    }

    /// Apply an update to the current entry from `other`.
    ///
    /// `other.mtime` is ignored if it is earlier than `self.mtime`.
    ///
    /// Returns true if a change was made to the entry.
    pub fn update(&mut self, other: &DirTableEntry) -> bool {
        let mut modified = false;
        if other.local && !self.local {
            self.local = true;
            modified = true;
        }
        if let Some(mtime) = other.mtime
            && self.mtime.is_none_or(|t| t <= mtime)
        {
            self.mtime = Some(mtime);
            modified = true;
        }

        modified
    }
}
/// An entry that can be either a file or directory.
#[derive(Debug, Clone, PartialEq)]
pub enum CacheTableEntry {
    /// A file entry
    File(FileTableEntry),
    /// A directory entry
    Dir(DirTableEntry),
}

impl CacheTableEntry {
    pub fn is_local(&self) -> bool {
        match self {
            CacheTableEntry::File(e) => e.is_local(),
            CacheTableEntry::Dir(e) => e.is_local(),
        }
    }
    pub fn is_remote(&self) -> bool {
        match self {
            CacheTableEntry::File(e) => e.is_remote(),
            CacheTableEntry::Dir(e) => e.is_remote(),
        }
    }
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
    match &entry.version {
        Version::Modified(base) => {
            builder.set_modified(true);
            if let Some(hash) = base {
                builder.set_hash(&hash.0);
            }
        }
        Version::Indexed(hash) => {
            builder.set_hash(&hash.0);
        }
    }

    // Convert the new enum back to the old capnp fields for compatibility
    match entry.kind {
        FileEntryKind::LocalFile => {
            builder.set_local(true);
        }
        FileEntryKind::SpecialFile => {
            builder.set_local(true);
            builder.set_special(true);
        }
        FileEntryKind::RemoteFile => {}
        FileEntryKind::Branched(pathid) => {
            builder.set_branched_from(PathId::from_optional(Some(pathid)));
        }
    }
}

fn parse_file_table_entry(
    msg: cache_capnp::file_table_entry::Reader<'_>,
) -> Result<FileTableEntry, ByteConversionError> {
    let mtime = msg.get_mtime()?;

    let kind = if msg.get_special() {
        FileEntryKind::SpecialFile
    } else if msg.get_local() {
        FileEntryKind::LocalFile
    } else if let Some(pathid) = PathId::as_optional(msg.get_branched_from()) {
        FileEntryKind::Branched(pathid)
    } else {
        FileEntryKind::RemoteFile
    };

    let version = if msg.get_modified() {
        Version::Modified(if msg.has_hash() {
            Some(parse_hash(msg.get_hash()?)?)
        } else {
            None
        })
    } else {
        Version::Indexed(parse_hash(msg.get_hash()?)?)
    };

    Ok(FileTableEntry {
        size: msg.get_size(),
        mtime: UnixTime::new(mtime.get_secs(), mtime.get_nsecs()),
        version,
        kind,
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
                let dir_table_entry = DirTableEntry {
                    local: dir_entry.get_local(),
                    mtime: if mtime.get_secs() == 0 && mtime.get_nsecs() == 0 {
                        None
                    } else {
                        Some(UnixTime::new(mtime.get_secs(), mtime.get_nsecs()))
                    },
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
                let mut dir_builder = builder.init_dir();
                dir_builder.set_local(dir_entry.local);
                if let Some(mtime) = dir_entry.mtime {
                    let mut mtime_builder = dir_builder.init_mtime();
                    mtime_builder.set_secs(mtime.as_secs());
                    mtime_builder.set_nsecs(mtime.subsec_nanos());
                }
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

    /// Extract the dir entry, returning an error if this is a file entry.
    pub fn expect_dir(self) -> Result<DirTableEntry, StorageError> {
        match self {
            CacheTableEntry::Dir(dir_entry) => Ok(dir_entry),
            CacheTableEntry::File(_) => Err(StorageError::NotADirectory),
        }
    }

    /// Extract the dir entry or return None
    pub fn dir(self) -> Option<DirTableEntry> {
        match self {
            CacheTableEntry::Dir(dir_entry) => Some(dir_entry),
            CacheTableEntry::File(_) => None,
        }
    }
}

/// The metadata of a file.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FileMetadata {
    /// The size of the file in bytes.
    pub size: u64,

    /// The modification time of the file.
    ///
    /// This is the duration since the start of the UNIX epoch.
    pub mtime: UnixTime,

    /// File version in the database.
    pub version: Version,

    /// For a local file, ctime reported by the filesystem.
    pub ctime: Option<UnixTime>,

    /// UNIX file mode.
    ///
    /// This includes permissions, file types (for special files) - or any UNIX
    /// flag that is supported locally.
    pub mode: u32,

    /// For a local file, UNIX user ID
    pub uid: Option<u32>,

    /// For a local file, UNIX group ID
    pub gid: Option<u32>,

    /// Number of blocks (512b) used by the file
    pub blocks: u64,
}

impl FileMetadata {
    pub(crate) fn merged(e: &FileTableEntry, m: &std::fs::Metadata) -> Self {
        let mut file_metadata = FileMetadata::from(m);
        if e.is_local() {
            file_metadata.version = e.version.clone();
        } else {
            // A new local modification of a cached file
            file_metadata.version = Version::modification_of(Some(e.version.clone()));
        }

        file_metadata
    }
}

impl From<&FileTableEntry> for FileMetadata {
    fn from(e: &FileTableEntry) -> Self {
        FileMetadata {
            size: e.size,
            mtime: e.mtime,
            version: e.version.clone(),
            ctime: None,
            mode: 0o666,
            uid: None,
            gid: None,
            blocks: (e.size / 512) + if (e.size % 512) == 0 { 0 } else { 1 },
        }
    }
}

impl From<FileTableEntry> for FileMetadata {
    fn from(value: FileTableEntry) -> Self {
        FileMetadata::from(&value)
    }
}

impl From<&std::fs::Metadata> for FileMetadata {
    fn from(m: &std::fs::Metadata) -> Self {
        FileMetadata {
            size: m.len(),
            mtime: UnixTime::mtime(&m),
            version: Version::Modified(None),
            ctime: Some(
                UnixTime::from_system_time(m.created().unwrap_or(SystemTime::UNIX_EPOCH))
                    .unwrap_or(UnixTime::ZERO),
            ),
            mode: m.mode(),
            uid: Some(m.uid()),
            gid: Some(m.gid()),
            blocks: (m.len() / 512) + if (m.len() % 512) == 0 { 0 } else { 1 },
        }
    }
}
impl From<std::fs::Metadata> for FileMetadata {
    fn from(m: std::fs::Metadata) -> Self {
        FileMetadata::from(&m)
    }
}

/// The metadata of a directory.
#[derive(Debug, Clone, PartialEq)]
pub struct DirMetadata {
    /// The modification time of the directory.
    pub mtime: UnixTime,

    /// UNIX file mode may include flags not supported by the cache
    /// for a local dir.
    pub mode: u32,

    /// For a local dir, UNIX user ID
    pub uid: Option<u32>,

    /// For a local dir, UNIX group ID
    pub gid: Option<u32>,
}

impl DirMetadata {
    pub(crate) fn readonly(mtime: UnixTime) -> Self {
        DirMetadata {
            mtime: mtime,
            mode: 0o555,
            uid: None,
            gid: None,
        }
    }
    pub(crate) fn modifiable(mtime: UnixTime) -> Self {
        DirMetadata {
            mtime: mtime,
            mode: 0o777,
            uid: None,
            gid: None,
        }
    }

    pub(crate) fn merged(e: &DirTableEntry, m: &std::fs::Metadata) -> Self {
        let mut dir_metadata = DirMetadata::from(m);
        if let Some(mtime) = e.mtime
            && mtime > dir_metadata.mtime
        {
            dir_metadata.mtime = mtime;
        }

        dir_metadata
    }
}

impl From<&DirTableEntry> for DirMetadata {
    fn from(e: &DirTableEntry) -> Self {
        DirMetadata::modifiable(e.mtime.unwrap_or_else(|| UnixTime::ZERO))
    }
}
impl From<DirTableEntry> for DirMetadata {
    fn from(e: DirTableEntry) -> Self {
        DirMetadata::from(&e)
    }
}
impl From<&std::fs::Metadata> for DirMetadata {
    fn from(m: &std::fs::Metadata) -> Self {
        DirMetadata {
            mtime: UnixTime::mtime(m),
            mode: m.mode(),
            uid: Some(m.uid()),
            gid: Some(m.gid()),
        }
    }
}
impl From<std::fs::Metadata> for DirMetadata {
    fn from(m: std::fs::Metadata) -> Self {
        DirMetadata::from(&m)
    }
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

    pub fn is_file(&self) -> bool {
        match self {
            Metadata::File(_) => true,
            _ => false,
        }
    }

    pub fn is_dir(&self) -> bool {
        match self {
            Metadata::Dir(_) => true,
            _ => false,
        }
    }

    pub fn merged(e: &CacheTableEntry, m: &std::fs::Metadata) -> Self {
        match e {
            CacheTableEntry::Dir(e) => {
                if m.is_dir() {
                    return Metadata::Dir(DirMetadata::merged(e, m));
                }
            }
            CacheTableEntry::File(e) => {
                if !m.is_dir() {
                    return Metadata::File(FileMetadata::merged(e, m));
                }
            }
        }

        m.into()
    }
}

impl From<&std::fs::Metadata> for Metadata {
    fn from(m: &std::fs::Metadata) -> Self {
        if m.is_dir() {
            Metadata::Dir(m.into())
        } else {
            Metadata::File(m.into())
        }
    }
}

impl From<std::fs::Metadata> for Metadata {
    fn from(m: std::fs::Metadata) -> Self {
        Metadata::from(&m)
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

/// A file entry for the index layer.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IndexedFile {
    pub hash: Hash,
    pub mtime: UnixTime,
    pub size: u64,
}

impl IndexedFile {
    /// Check whether `file_path` is a regular file with size and mtime match this entry's.
    pub(crate) fn matches_file<P: AsRef<std::path::Path>>(&self, file_path: P) -> bool {
        if let Ok(m) = file_path.as_ref().symlink_metadata() {
            return m.is_file() && self.matches(m.len(), UnixTime::mtime(&m));
        }

        false
    }

    /// Check whether `file_path` size and mtime match this entry's.
    pub(crate) fn matches(&self, size: u64, mtime: UnixTime) -> bool {
        size == self.size && mtime == self.mtime
    }

    pub(crate) fn into_file(self) -> FileTableEntry {
        FileTableEntry {
            size: self.size,
            mtime: self.mtime,
            version: Version::Indexed(self.hash),
            kind: FileEntryKind::LocalFile,
        }
    }
}

impl From<FileTableEntry> for Option<IndexedFile> {
    fn from(value: FileTableEntry) -> Self {
        Option::<IndexedFile>::from(&value)
    }
}

impl From<&FileTableEntry> for Option<IndexedFile> {
    fn from(value: &FileTableEntry) -> Self {
        if let Version::Indexed(hash) = &value.version
            && value.kind == FileEntryKind::LocalFile
        {
            Some(IndexedFile {
                hash: hash.clone(),
                mtime: value.mtime,
                size: value.size,
            })
        } else {
            None
        }
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
    async fn convert_file_table_entry_capnp() -> anyhow::Result<()> {
        // Test Local variant
        let local_entry = FileTableEntry {
            size: 100,
            mtime: UnixTime::from_secs(1234567890),
            version: Version::Indexed(Hash([1u8; 32])),
            kind: FileEntryKind::LocalFile,
        };

        let bytes = local_entry.to_bytes()?;
        let recovered = FileTableEntry::from_bytes(&bytes)?;
        assert_eq!(local_entry, recovered);

        // Test Special variant
        let special_entry = FileTableEntry {
            size: 100,
            mtime: UnixTime::from_secs(1234567890),
            version: Version::Indexed(Hash([1u8; 32])),
            kind: FileEntryKind::SpecialFile,
        };

        let bytes = special_entry.to_bytes()?;
        let recovered = FileTableEntry::from_bytes(&bytes)?;
        assert_eq!(special_entry, recovered);

        // Test Cache variant
        let cache_entry = FileTableEntry {
            size: 200,
            mtime: UnixTime::from_secs(1234567891),
            version: Version::Indexed(Hash([2u8; 32])),
            kind: FileEntryKind::RemoteFile,
        };

        let bytes = cache_entry.to_bytes()?;
        let recovered = FileTableEntry::from_bytes(&bytes)?;
        assert_eq!(cache_entry, recovered);

        // Test Branched variant
        let branched_entry = FileTableEntry {
            size: 300,
            mtime: UnixTime::from_secs(1234567892),
            version: Version::Indexed(Hash([3u8; 32])),
            kind: FileEntryKind::Branched(PathId(42)),
        };

        let bytes = branched_entry.to_bytes()?;
        let recovered = FileTableEntry::from_bytes(&bytes)?;
        assert_eq!(branched_entry, recovered);

        Ok(())
    }

    #[test]
    fn test_file_entry_kind_methods() {
        // Test Local
        let local_entry = FileTableEntry {
            size: 100,
            mtime: UnixTime::from_secs(1234567890),
            version: Version::Indexed(Hash([1u8; 32])),
            kind: FileEntryKind::LocalFile,
        };
        assert!(local_entry.is_local());
        assert!(!local_entry.is_remote());

        // Test Cache
        let cache_entry = FileTableEntry {
            size: 200,
            mtime: UnixTime::from_secs(1234567891),
            version: Version::Indexed(Hash([2u8; 32])),
            kind: FileEntryKind::RemoteFile,
        };
        assert!(!cache_entry.is_local());
        assert!(cache_entry.is_remote());

        // Test Branched
        let branched_entry = FileTableEntry {
            size: 300,
            mtime: UnixTime::from_secs(1234567892),
            version: Version::Indexed(Hash([3u8; 32])),
            kind: FileEntryKind::Branched(PathId(42)),
        };
        assert!(!branched_entry.is_local());
        assert!(branched_entry.is_remote());
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
            version: Version::Indexed(Hash([0xa1u8; 32])),
            kind: FileEntryKind::Branched(PathId(123)),
        };

        assert_eq!(
            entry,
            FileTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }

    #[test]
    fn convert_cache_table_entry_file() -> anyhow::Result<()> {
        let file_entry = FileTableEntry {
            size: 200,
            mtime: UnixTime::from_secs(1234567890),
            version: Version::Indexed(Hash([0xa1u8; 32])),
            kind: FileEntryKind::RemoteFile,
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
        let dir_entry = DirTableEntry {
            local: true,
            mtime: Some(UnixTime::from_secs(987654321)),
        };

        let entry = CacheTableEntry::Dir(dir_entry);

        assert_eq!(
            entry,
            CacheTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }

    #[test]
    fn convert_cache_table_entry_dir_default() -> anyhow::Result<()> {
        let dir_entry = DirTableEntry::default();
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
            version: Version::Indexed(Hash([0x42u8; 32])),
            kind: FileEntryKind::RemoteFile,
        };

        let dir_entry = DirTableEntry {
            mtime: Some(UnixTime::from_secs(987654321)),
            local: true,
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

    #[test]
    fn convert_layer_default() -> anyhow::Result<()> {
        let layer = Layer::Default;

        // Test round-trip conversion
        let bytes = Layer::as_bytes(&layer);
        let converted = Layer::from_bytes(&bytes);
        assert_eq!(layer, converted);

        Ok(())
    }

    #[test]
    fn convert_layer_remote() -> anyhow::Result<()> {
        let layer = Layer::Remote(Peer::from("peer1"));

        // Test round-trip conversion
        let bytes = Layer::as_bytes(&layer);
        let converted = Layer::from_bytes(&bytes);
        assert_eq!(layer, converted);

        Ok(())
    }

    #[test]
    fn convert_layer_all_variants() -> anyhow::Result<()> {
        let test_cases = vec![
            Layer::Default,
            Layer::Remote(Peer::from("short")),
            Layer::Remote(Peer::from("very_long_peer_name_that_might_cause_issues")),
            Layer::Remote(Peer::from("")), // Empty peer name edge case
        ];

        for layer in test_cases {
            let bytes = Layer::as_bytes(&layer);
            let converted = Layer::from_bytes(&bytes);
            assert_eq!(layer, converted, "Failed for layer: {:?}", layer);
        }

        Ok(())
    }

    #[test]
    fn layer_byte_comparison_behavior() {
        // Test that byte comparison behavior is consistent and documented

        let test_cases = vec![
            Layer::Default,
            Layer::Remote(Peer::from("a")),
            Layer::Remote(Peer::from("b")),
            Layer::Remote(Peer::from("z")),
        ];

        // Test that byte comparison is consistent (same inputs always give same result)
        for i in 0..test_cases.len() {
            for j in 0..test_cases.len() {
                let layer1 = &test_cases[i];
                let layer2 = &test_cases[j];

                let bytes1 = Layer::as_bytes(layer1);
                let bytes2 = Layer::as_bytes(layer2);
                let byte_cmp = Layer::compare(&bytes1, &bytes2);

                // Test consistency: reverse comparison should be opposite
                let byte_cmp_reverse = Layer::compare(&bytes2, &bytes1);
                assert_eq!(
                    byte_cmp,
                    byte_cmp_reverse.reverse(),
                    "Byte comparison not consistent for {:?} vs {:?}",
                    layer1,
                    layer2
                );

                // Test reflexivity: same layer should compare equal
                if i == j {
                    assert_eq!(
                        byte_cmp,
                        std::cmp::Ordering::Equal,
                        "Byte comparison not reflexive for {:?}",
                        layer1
                    );
                }
            }
        }
    }

    #[test]
    fn layer_byte_comparison_matches_object_comparison() {
        // Test that byte comparison matches object comparison for all valid layers
        let test_cases = vec![
            Layer::Default,
            Layer::Remote(Peer::from("a")),
            Layer::Remote(Peer::from("b")),
            Layer::Remote(Peer::from("z")),
        ];

        for i in 0..test_cases.len() {
            for j in 0..test_cases.len() {
                let layer1 = &test_cases[i];
                let layer2 = &test_cases[j];

                let object_cmp = layer1.cmp(layer2);
                let bytes1 = Layer::as_bytes(layer1);
                let bytes2 = Layer::as_bytes(layer2);
                let byte_cmp = Layer::compare(&bytes1, &bytes2);

                assert_eq!(
                    object_cmp, byte_cmp,
                    "Comparison mismatch for {:?} vs {:?}",
                    layer1, layer2
                );
            }
        }
    }

    #[test]
    fn layer_serialization_format() {
        // Test that the serialization format is correct
        let layer = Layer::Default;
        let bytes = Layer::as_bytes(&layer);

        // Should be exactly 1 byte for Default
        assert_eq!(bytes.len(), 1);
        assert_eq!(bytes, [1]);

        let layer = Layer::Remote(Peer::from("test"));
        let bytes = Layer::as_bytes(&layer);

        // Should be 5 bytes for Remote (1 byte + "test")
        assert_eq!(bytes.len(), 5);
        assert_eq!(bytes[0], 2);
        assert_eq!(&bytes[1..], b"test");
    }

    #[test]
    fn layer_ordering_semantics() {
        // Test that Layer ordering matches the spec
        let default = Layer::Default;
        let remote_a = Layer::Remote(Peer::from("a"));
        let remote_b = Layer::Remote(Peer::from("b"));
        let remote_z = Layer::Remote(Peer::from("z"));

        // Default should come before Remote and index
        assert!(default < remote_a);
        assert!(default < remote_b);
        assert!(default < remote_z);

        // Remote peers should be ordered by peer name
        assert!(remote_a < remote_b);
        assert!(remote_b < remote_z);
        assert!(remote_a < remote_z);
    }
}
