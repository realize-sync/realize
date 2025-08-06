use crate::types::BlobId;
use crate::utils::holder::{ByteConversionError, ByteConvertible, NamedType};
use capnp::message::ReaderOptions;
use capnp::serialize_packed;
use realize_types::{self, ByteRanges, Hash, UnixTime};

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

/// An entry in the queue table.
#[derive(Debug, Clone, PartialEq)]
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
}
