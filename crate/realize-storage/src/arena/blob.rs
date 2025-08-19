use super::db::ArenaDatabase;
use super::dirty::WritableOpenDirty;
use super::hasher::hash_file;
use super::mark::MarkReadOperations;
use super::tree::{TreeExt, TreeLoc, TreeReadOperations, WritableOpenTree};
use super::types::{BlobTableEntry, LocalAvailability, LruQueueId, Mark, QueueTableEntry};
use crate::StorageError;
use crate::types::Inode;
use crate::utils::holder::Holder;
use realize_types::{ByteRange, ByteRanges, Hash};
use redb::{ReadableTable, Table};
use std::cmp::min;
use std::collections::HashMap;
use std::io::{SeekFrom, Write};
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt as _, ReadBuf};

// Arbitrary disk space considered to be occupied by an inode.
//
// This value is added to the disk usage estimates. It is meant to
// account for the disk space used by the inode itself, so it doesn't
// look like empty files are free. This is arbitrary and might not
// correspond to the actual value, as it depends on the filesystem.
const INODE_DISK_USAGE: u64 = 512;

pub(crate) struct Blobs {
    blob_dir: PathBuf,
    /// Reference counter for open blobs. Maps Inode to the number of open handles.
    open_blobs: OpenBlobRefCounter,
}
impl Blobs {
    pub(crate) fn new(blob_dir: &std::path::Path) -> Blobs {
        Self {
            blob_dir: blob_dir.to_path_buf(),
            open_blobs: OpenBlobRefCounter::new(),
        }
    }
}

pub(crate) struct ReadableOpenBlob<T>
where
    T: ReadableTable<Inode, Holder<'static, BlobTableEntry>>,
{
    blob_table: T,
}

impl<T> ReadableOpenBlob<T>
where
    T: ReadableTable<Inode, Holder<'static, BlobTableEntry>>,
{
    pub(crate) fn new(blob_table: T) -> Self {
        Self { blob_table }
    }
}

pub(crate) struct WritableOpenBlob<'a> {
    blob_table: Table<'a, Inode, Holder<'static, BlobTableEntry>>,
    blob_lru_queue_table: Table<'a, u16, Holder<'static, QueueTableEntry>>,
    subsystem: &'a Blobs,
}

impl<'a> WritableOpenBlob<'a> {
    pub(crate) fn new(
        blob_table: Table<'a, Inode, Holder<'static, BlobTableEntry>>,
        blob_lru_queue_table: Table<'a, u16, Holder<'static, QueueTableEntry>>,
        subsystem: &'a Blobs,
    ) -> Self {
        Self {
            blob_table,
            blob_lru_queue_table,
            subsystem: subsystem,
        }
    }
}

/// Public information about the blob.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlobInfo {
    pub(crate) inode: Inode,
    pub(crate) size: u64,
    pub(crate) hash: Hash,

    /// Byte ranges for which data is available locally.
    /// May be empty or may be the entire range [0, size).]
    pub(crate) available_ranges: ByteRanges,

    /// If true, local data will not be deleted by
    /// [WritableOpenBlob::cleanup].
    ///
    /// Protected blobs belong to the protected LRU queue.
    pub(crate) protected: bool,

    /// If true, content of the file was verified against the hash.
    pub(crate) verified: bool,
}

impl BlobInfo {
    fn new(inode: Inode, entry: BlobTableEntry) -> Self {
        Self {
            inode,
            size: entry.content_size,
            hash: entry.content_hash,
            available_ranges: entry.written_areas,
            protected: entry.queue == LruQueueId::Protected,
            verified: entry.verified,
        }
    }
}

/// Read operations for blobs. See also [BlobExt].
pub(crate) trait BlobReadOperations {
    /// Return some info about the blob.
    ///
    /// This call returns [StorageError::NotFound] if the blob doesn't
    /// exist on the database. Use [WritableOpenBlob::create] to
    /// create the blob entry.
    fn get_with_inode(&self, inode: Inode) -> Result<Option<BlobInfo>, StorageError>;
}

impl<'a, T> BlobReadOperations for ReadableOpenBlob<T>
where
    T: ReadableTable<Inode, Holder<'static, BlobTableEntry>>,
{
    fn get_with_inode(&self, inode: Inode) -> Result<Option<BlobInfo>, StorageError> {
        get_read_op(&self.blob_table, inode)
    }
}

impl<'a> BlobReadOperations for WritableOpenBlob<'a> {
    fn get_with_inode(&self, inode: Inode) -> Result<Option<BlobInfo>, StorageError> {
        get_read_op(&self.blob_table, inode)
    }
}

/// Extend [BlobReadOperations] with convenience functions.
#[allow(dead_code)] // for later
pub(crate) trait BlobExt {
    /// Open the blob and return a handle on it.
    ///
    /// This call returns None if the blob doesn't exist on the
    /// database. Use [WritableOpenBlob::create] to create the
    /// blob entry.
    fn get<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<Option<BlobInfo>, StorageError>;

    /// Returns local availability for the blob.
    ///
    /// Note that non-existing as well as empty blobs have
    /// [LocalAvailability::Missing].
    fn local_availability<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<LocalAvailability, StorageError>;
}

impl<T: BlobReadOperations> BlobExt for T {
    fn get<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<Option<BlobInfo>, StorageError> {
        if let Some(inode) = tree.resolve(loc)? {
            self.get_with_inode(inode)
        } else {
            Ok(None)
        }
    }

    fn local_availability<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<LocalAvailability, StorageError> {
        if let Some(blob_info) = self.get(tree, loc)? {
            let file_range = ByteRanges::single(0, blob_info.size);
            if file_range == file_range.intersection(&blob_info.available_ranges) {
                // Check if the blob is verified
                if blob_info.verified {
                    return Ok(LocalAvailability::Verified);
                } else {
                    return Ok(LocalAvailability::Complete);
                }
            }
            if blob_info.available_ranges.is_empty() {
                return Ok(LocalAvailability::Missing);
            }

            Ok(LocalAvailability::Partial(
                blob_info.size,
                blob_info.available_ranges,
            ))
        } else {
            Ok(LocalAvailability::Missing)
        }
    }
}

impl<'a> WritableOpenBlob<'a> {
    /// Create a new blob and prepare its associated file.
    ///
    /// The blob is put into the queue appropriate for the current
    /// mark, reported by `mark` (working LRU queue for watch and
    /// protected queue for keep and own).
    ///
    /// If a blob already exists with the same inode and hash, it is
    /// reused and this call then works just like
    /// [BlobReadOperations::get].
    ///
    /// If a blob already exists with the same inode, but another
    /// hash, it is overwritten and its data cleared.
    pub(crate) fn create<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        marks: &impl MarkReadOperations,
        loc: L,
        hash: &Hash,
        size: u64,
    ) -> Result<BlobInfo, StorageError> {
        let (inode, _, entry) = self.create_entry(tree, marks, loc, hash, size)?;

        Ok(BlobInfo::new(inode, entry))
    }

    fn create_entry<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree<'_>,
        marks: &impl MarkReadOperations,
        loc: L,
        hash: &Hash,
        size: u64,
    ) -> Result<(Inode, PathBuf, BlobTableEntry), StorageError> {
        let inode = tree.setup(loc)?;
        let existing_entry = if let Some(e) = self.blob_table.get(inode)? {
            Some(e.value().parse()?)
        } else {
            None
        };
        if let Some(e) = existing_entry {
            if e.content_hash == *hash {
                return Ok((inode, self.subsystem.blob_dir.join(inode.hex()), e));
            }

            // Existing entry cannot be reuse; remove.
            remove_from_queue(&mut self.blob_lru_queue_table, &mut self.blob_table, &e)?;
        }
        let blob_path = self.prepare_blob_file(inode, size)?;
        let queue = choose_queue(tree, marks, inode)?;
        let mut entry = BlobTableEntry {
            written_areas: ByteRanges::new(),
            content_hash: hash.clone(),
            content_size: size,
            verified: false,
            queue,
            next: None,
            prev: None,
            disk_usage: 0,
        };
        add_to_queue_front(
            &mut self.blob_lru_queue_table,
            &mut self.blob_table,
            queue,
            inode,
            &mut entry,
        )?;
        update_disk_usage(
            &mut self.blob_lru_queue_table,
            &mut entry,
            &blob_path.metadata()?,
        )?;

        log::debug!("Created blob {inode} {hash} in {queue:?} at {blob_path:?} -> {entry:?}");
        tree.insert_and_incref(inode, &mut self.blob_table, inode, Holder::new(&entry)?)?;
        Ok((inode, blob_path, entry))
    }

    fn prepare_blob_file(&mut self, inode: Inode, size: u64) -> Result<PathBuf, StorageError> {
        let blob_dir = self.subsystem.blob_dir.as_path();
        let blob_path = blob_dir.join(inode.hex());
        if !blob_dir.exists() {
            std::fs::create_dir_all(blob_dir)?;
        }
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(&blob_path)?;
        if size != 0 {
            file.set_len(size)?;
        }
        file.flush()?;

        Ok(blob_path)
    }

    /// Delete the blob and its associated data on filesystem, if it exists.
    pub(crate) fn delete<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        loc: L,
    ) -> Result<(), StorageError> {
        let inode = match tree.resolve(loc)? {
            Some(inode) => inode,
            None => return Ok(()), // Nothing to delete
        };

        remove_blob_entry(
            &mut self.blob_table,
            &mut self.blob_lru_queue_table,
            tree,
            inode,
        )?;
        let blob_path = self.subsystem.blob_dir.join(inode.hex());
        if blob_path.exists() {
            std::fs::remove_file(&blob_path)?;
        }

        Ok(())
    }

    /// Move the blob into or out of the protected queue.
    ///
    /// Does nothing if the blob doesn't exist.
    #[allow(dead_code)] // for later
    fn set_protected<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        loc: L,
        protected: bool,
    ) -> Result<(), StorageError> {
        let inode = match tree.resolve(loc)? {
            Some(inode) => inode,
            None => return Ok(()), // Nothing to modify
        };

        let mut blob_entry = match self.blob_table.get(inode)? {
            None => {
                return Ok(());
            }
            Some(v) => v.value().parse()?,
        };
        let new_queue = if protected {
            LruQueueId::Protected
        } else {
            LruQueueId::WorkingArea
        };

        // If the queue is already correct, nothing to do
        if blob_entry.queue == new_queue {
            return Ok(());
        }

        // Remove from current queue
        remove_from_queue(
            &mut self.blob_lru_queue_table,
            &mut self.blob_table,
            &blob_entry,
        )?;

        // Add to new queue
        add_to_queue_front(
            &mut self.blob_lru_queue_table,
            &mut self.blob_table,
            new_queue,
            inode,
            &mut blob_entry,
        )?;

        // Update the entry in the table
        self.blob_table
            .insert(inode, Holder::with_content(blob_entry)?)?;

        Ok(())
    }

    /// Move the blob to `dest` and delete the blob entry.
    ///
    /// Does nothing and return false unless the blob content hash is
    /// `hash` and has been downloaded and verified.
    pub(crate) fn export<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        loc: L,
        hash: &Hash,
        dest: &std::path::Path,
    ) -> Result<bool, StorageError> {
        let inode = match tree.resolve(loc)? {
            Some(inode) => inode,
            None => return Ok(false), // Nothing to export
        };

        let blob_entry = match self.blob_table.get(inode)? {
            None => {
                return Ok(false);
            }
            Some(v) => v.value().parse()?,
        };
        if blob_entry.content_hash != *hash || !blob_entry.verified {
            return Ok(false);
        }

        remove_blob_entry(
            &mut self.blob_table,
            &mut self.blob_lru_queue_table,
            tree,
            inode,
        )?;
        let blob_path = self.subsystem.blob_dir.join(inode.hex());
        std::fs::rename(blob_path, dest)?;

        Ok(true)
    }

    /// Setup the database to move some existing file into and return the
    /// path to write to.
    ///
    /// This assumes that the file will be written completely and so
    /// sets up local availability to complete already (but not verified).
    pub(crate) fn import<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        marks: &impl MarkReadOperations,
        loc: L,
        hash: &Hash,
        metadata: &std::fs::Metadata,
    ) -> Result<PathBuf, StorageError> {
        let size = metadata.len();
        let (inode, blob_path, mut entry) = self.create_entry(tree, marks, loc, hash, size)?;

        // Set written areas to complete (but not verified) and update
        // trusting that metadata is going to be the blob file
        // metadata.
        entry.written_areas = ByteRanges::single(0, size);
        entry.verified = false;
        update_disk_usage(&mut self.blob_lru_queue_table, &mut entry, metadata)?;
        self.blob_table
            .insert(inode, Holder::with_content(entry)?)?;

        // Return the path where the file should be moved
        Ok(blob_path)
    }

    /// Mark the blob as accessed.
    ///
    /// If the blob is in a LRU queue other than protected, it is put
    /// at the front of its queue.
    ///
    /// Does nothing if the blob doesn't exist.
    #[allow(dead_code)] // for later
    fn mark_accessed<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<(), StorageError> {
        let inode = match tree.resolve(loc)? {
            Some(inode) => inode,
            None => return Ok(()), // Nothing to mark
        };

        let mut blob_entry = match self.blob_table.get(inode)? {
            None => {
                return Ok(());
            }
            Some(v) => v.value().parse()?,
        };

        // If already at the head of its queue, nothing to do
        if blob_entry.prev.is_none() {
            return Ok(());
        }

        // Move to the front of the queue
        move_to_front(
            &mut self.blob_lru_queue_table,
            &mut self.blob_table,
            inode,
            &mut blob_entry,
        )?;

        // Update the entry in the table
        self.blob_table
            .insert(inode, Holder::with_content(blob_entry)?)?;

        Ok(())
    }

    /// Mark the blob as complete and verified for the given hash.
    ///
    /// If the blob exists and has the same hash as was given, update
    /// the blob and return true, otherwise do nothing and return
    /// false.
    fn mark_verified<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &impl TreeReadOperations,
        dirty: &mut WritableOpenDirty,
        loc: L,
        hash: &Hash,
    ) -> Result<bool, StorageError> {
        let inode = match tree.resolve(loc)? {
            Some(inode) => inode,
            None => return Ok(false), // Nothing to mark
        };

        let mut blob_entry = match self.blob_table.get(inode)? {
            None => {
                return Ok(false);
            }
            Some(v) => v.value().parse()?,
        };

        // Only mark as verified if the hash matches
        if blob_entry.content_hash == *hash {
            blob_entry.verified = true;
            log::debug!("{inode} content verified to be {hash}");

            self.blob_table
                .insert(inode, Holder::with_content(blob_entry)?)?;
            dirty.mark_dirty(inode, "verified")?;
            return Ok(true);
        }

        Ok(false)
    }

    /// Extend local availability of the blob with the given range.
    ///
    /// If the blob exists and has the same hash as was given, update
    /// the blob and return true, otherwise do nothing and return
    /// false.
    pub(crate) fn extend_local_availability<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &impl TreeReadOperations,
        loc: L,
        hash: &Hash,
        new_range: &ByteRanges,
    ) -> Result<bool, StorageError> {
        let inode = match tree.resolve(loc)? {
            Some(inode) => inode,
            None => return Ok(false), // Nothing to extend
        };

        let mut blob_entry = match get_blob_entry(&self.blob_table, inode)? {
            Some(e) => e,
            None => return Ok(false),
        };
        if blob_entry.content_hash != *hash {
            return Ok(false);
        }
        blob_entry.written_areas = blob_entry.written_areas.union(new_range);
        log::debug!(
            "{inode} extended by {new_range}; available: {}",
            blob_entry.written_areas
        );

        // Update disk usage if the blob file exists
        let blob_path = self.subsystem.blob_dir.join(inode.hex());
        if let Ok(metadata) = blob_path.metadata() {
            update_disk_usage(&mut self.blob_lru_queue_table, &mut blob_entry, &metadata)?;
        }

        self.blob_table
            .insert(inode, Holder::with_content(blob_entry)?)?;

        Ok(true)
    }

    /// Delete blobs as necessary to reach the target total size, in bytes.
    pub(crate) fn cleanup(
        &mut self,
        tree: &mut WritableOpenTree,
        target: u64,
    ) -> Result<(), StorageError> {
        // Get the WorkingArea queue
        let mut queue =
            match get_queue_if_available(&self.blob_lru_queue_table, LruQueueId::WorkingArea)? {
                Some(q) => q,
                None => {
                    // No queue exists, nothing to clean up
                    return Ok(());
                }
            };

        log::debug!(
            "Starting cleanup of WorkingArea with disk usage: {}, target: {}",
            queue.disk_usage,
            target
        );

        let mut prev = queue.tail;
        let mut removed_count = 0;
        while let Some(current_id) = prev
            && queue.disk_usage > target
        {
            let current = follow_queue_link(&self.blob_table, current_id)?;
            prev = current.prev;

            if self.subsystem.open_blobs.is_open(current_id) {
                continue;
            }
            log::debug!(
                "Evicted blob from WorkingArea: {current_id} ({} bytes)",
                current.disk_usage
            );
            remove_from_queue_update_entry(&mut self.blob_table, &current, &mut queue)?;
            removed_count += 1;
            tree.remove_and_decref(current_id, &mut self.blob_table, current_id)?;

            // Remove the blob file
            let blob_path = self.subsystem.blob_dir.join(current_id.hex());
            if blob_path.exists() {
                std::fs::remove_file(&blob_path)?;
            }
        }

        log::debug!(
            "Evicted {removed_count} entries from WorkingArea disk usage now {} (target: {target})",
            queue.disk_usage
        );

        if removed_count > 0 {
            self.blob_lru_queue_table
                .insert(LruQueueId::WorkingArea as u16, Holder::with_content(queue)?)?;
        }

        Ok(())
    }
}

fn choose_queue(
    tree: &mut WritableOpenTree<'_>,
    marks: &impl MarkReadOperations,
    inode: Inode,
) -> Result<LruQueueId, StorageError> {
    let queue = match marks.get_at_inode(tree, inode)? {
        Mark::Watch => LruQueueId::WorkingArea,
        Mark::Keep | Mark::Own => LruQueueId::Protected,
    };
    Ok(queue)
}

/// Move the blob to the front of its queue
fn move_to_front(
    blob_lru_queue_table: &mut redb::Table<'_, u16, Holder<'static, QueueTableEntry>>,
    blob_table: &mut redb::Table<'_, Inode, Holder<'static, BlobTableEntry>>,
    inode: Inode,
    blob_entry: &mut BlobTableEntry,
) -> Result<(), StorageError> {
    if blob_entry.prev.is_none() {
        // Already at the head of its queue
        return Ok(());
    }

    log::debug!("{inode} moved to the front of {:?}", blob_entry.queue);
    let queue_id = blob_entry.queue;
    remove_from_queue(blob_lru_queue_table, blob_table, blob_entry)?;
    add_to_queue_front(
        blob_lru_queue_table,
        blob_table,
        queue_id,
        inode,
        blob_entry,
    )?;
    Ok(())
}

/// Calculate disk usage for a blob file using Unix metadata.
fn calculate_disk_usage(metadata: &std::fs::Metadata) -> u64 {
    // block() takes into account actual usage, not the whole file
    // size, which might be sparse. Include the inode size into the
    // computation.
    metadata.blocks() * 512 + INODE_DISK_USAGE
}

/// Update disk usage in the given blob entry and the total in its
/// containing table.
fn update_disk_usage(
    blob_lru_queue_table: &mut redb::Table<'_, u16, Holder<'static, QueueTableEntry>>,
    entry: &mut BlobTableEntry,
    metadata: &std::fs::Metadata,
) -> Result<(), StorageError> {
    let disk_usage = calculate_disk_usage(metadata);
    let disk_usage_diff = disk_usage as i64 - (entry.disk_usage as i64);
    if disk_usage_diff == 0 {
        return Ok(());
    }
    entry.disk_usage = disk_usage;
    update_total_disk_usage(blob_lru_queue_table, entry.queue, disk_usage_diff)?;

    Ok(())
}

fn update_total_disk_usage(
    blob_lru_queue_table: &mut redb::Table<'_, u16, Holder<'static, QueueTableEntry>>,
    queue_id: LruQueueId,
    diff: i64,
) -> Result<(), StorageError> {
    let mut entry = get_queue_must_exist(blob_lru_queue_table, queue_id)?;
    entry.disk_usage = entry.disk_usage.saturating_add_signed(diff);
    blob_lru_queue_table.insert(queue_id as u16, Holder::with_content(entry)?)?;
    Ok(())
}

fn remove_blob_entry(
    blob_table: &mut redb::Table<'_, Inode, Holder<'static, BlobTableEntry>>,
    blob_lru_queue_table: &mut redb::Table<'_, u16, Holder<'static, QueueTableEntry>>,
    tree: &mut WritableOpenTree<'_>,
    inode: Inode,
) -> Result<(), StorageError> {
    if let Some(entry) = get_blob_entry(blob_table, inode)? {
        remove_from_queue(blob_lru_queue_table, blob_table, &entry)?;
        tree.remove_and_decref(inode, blob_table, inode)?;
    }

    Ok(())
}

fn get_queue_if_available(
    blob_lru_queue_table: &impl redb::ReadableTable<u16, Holder<'static, QueueTableEntry>>,
    queue_id: LruQueueId,
) -> Result<Option<QueueTableEntry>, StorageError> {
    let e = blob_lru_queue_table.get(queue_id as u16)?;
    if let Some(e) = e {
        Ok(Some(e.value().parse()?))
    } else {
        Ok(None)
    }
}

fn get_queue_must_exist(
    blob_lru_queue_table: &impl redb::ReadableTable<u16, Holder<'static, QueueTableEntry>>,
    queue_id: LruQueueId,
) -> Result<QueueTableEntry, StorageError> {
    Ok(blob_lru_queue_table
        .get(queue_id as u16)?
        .ok_or_else(|| StorageError::InconsistentDatabase(format!("{queue_id:?} missing")))?
        .value()
        .parse()?)
}

fn get_blob_entry(
    blob_table: &impl redb::ReadableTable<Inode, Holder<'static, BlobTableEntry>>,
    inode: Inode,
) -> Result<Option<BlobTableEntry>, StorageError> {
    if let Some(entry) = blob_table.get(inode)? {
        Ok(Some(entry.value().parse()?))
    } else {
        Ok(None)
    }
}

fn follow_queue_link(
    blob_table: &impl redb::ReadableTable<Inode, Holder<'static, BlobTableEntry>>,
    inode: Inode,
) -> Result<BlobTableEntry, StorageError> {
    get_blob_entry(blob_table, inode)?
        .ok_or_else(|| StorageError::InconsistentDatabase(format!("invalid queue link {inode}")))
}

/// Add a blob to the front of the WorkingArea queue.
fn add_to_queue_front(
    blob_lru_queue_table: &mut redb::Table<'_, u16, Holder<'static, QueueTableEntry>>,
    blob_table: &mut redb::Table<'_, Inode, Holder<'static, BlobTableEntry>>,
    queue_id: LruQueueId,
    inode: Inode,
    blob_entry: &mut BlobTableEntry,
) -> Result<(), StorageError> {
    blob_entry.queue = queue_id;

    let mut queue = get_queue_if_available(blob_lru_queue_table, queue_id)?.unwrap_or_default();
    if let Some(head_id) = queue.head {
        let mut head = follow_queue_link(blob_table, head_id)?;
        head.prev = Some(inode);

        blob_table.insert(head_id, Holder::with_content(head)?)?;
    } else {
        // This is the first node in the queue, so both the head
        // and the tail
        queue.tail = Some(inode);
    }
    blob_entry.next = queue.head;
    queue.head = Some(inode);
    blob_entry.prev = None;

    // Update queue's total disk usage
    queue.disk_usage += blob_entry.disk_usage;

    blob_lru_queue_table.insert(queue_id as u16, Holder::with_content(queue)?)?;

    Ok(())
}

/// Remove a blob from its queue.
fn remove_from_queue(
    blob_lru_queue_table: &mut redb::Table<'_, u16, Holder<'static, QueueTableEntry>>,
    blob_table: &mut redb::Table<'_, Inode, Holder<'static, BlobTableEntry>>,
    blob_entry: &BlobTableEntry,
) -> Result<(), StorageError> {
    let queue_id = blob_entry.queue;

    // Get the queue entry
    let mut queue = get_queue_must_exist(blob_lru_queue_table, queue_id)?;
    remove_from_queue_update_entry(blob_table, blob_entry, &mut queue)?;
    blob_lru_queue_table.insert(queue_id as u16, Holder::with_content(queue)?)?;

    Ok(())
}

fn remove_from_queue_update_entry(
    blob_table: &mut redb::Table<'_, Inode, Holder<'static, BlobTableEntry>>,
    blob_entry: &BlobTableEntry,
    queue: &mut QueueTableEntry,
) -> Result<(), StorageError> {
    if let Some(prev_id) = blob_entry.prev {
        let mut prev = follow_queue_link(blob_table, prev_id)?;
        prev.next = blob_entry.next;
        blob_table.insert(prev_id, Holder::with_content(prev)?)?;
    } else {
        // This was the first node
        queue.head = blob_entry.next;
    }
    if let Some(next_id) = blob_entry.next {
        let mut next = follow_queue_link(blob_table, next_id)?;
        next.prev = blob_entry.prev;
        blob_table.insert(next_id, Holder::with_content(next)?)?;
    } else {
        // This was the last node
        queue.tail = blob_entry.prev;
    }
    queue.disk_usage = queue.disk_usage.saturating_sub(blob_entry.disk_usage);
    Ok(())
}

/// Error returned by Blob when reading outside the available range.
///
/// This error is embedded into a [std::io::Error] of kind
/// [std::io::ErrorKind::InvalidData]. You can use
/// [BlobIncomplete::matches] to check an I/O error.
#[derive(thiserror::Error, Debug)]
#[error("local blob data is incomplete")]
pub struct BlobIncomplete;

impl BlobIncomplete {
    /// Returns true if the given I/O error is actually a BlobIncomplete.
    #[allow(dead_code)]
    pub fn matches(ioerr: &std::io::Error) -> bool {
        ioerr.kind() == std::io::ErrorKind::InvalidData
            && ioerr
                .get_ref()
                .map(|e| e.is::<BlobIncomplete>())
                .unwrap_or(false)
    }
}

/// A blob that provides async read and seek access to file data.
///
/// Attempts to read outside of available range will result in an I/O
/// error of kind [std::io::ErrorKind::InvalidData] with a
/// [BlobIncomplete] error.
pub struct Blob {
    inode: Inode,
    file: tokio::fs::File,
    size: u64,
    hash: Hash,
    db: Arc<ArenaDatabase>,

    /// Complete available range
    available_ranges: ByteRanges,
    /// Updates to the available range already integrated into
    /// available_range but not yet reported to update_tx.
    pending_ranges: ByteRanges,

    /// The read/write/seek position.
    offset: u64,
}

impl Blob {
    #[allow(dead_code)] // for later
    pub(crate) fn open<'a, L: Into<TreeLoc<'a>>>(
        db: &Arc<ArenaDatabase>,
        loc: L,
    ) -> Result<Blob, StorageError> {
        let info = {
            let txn = db.begin_read()?;
            let tree = txn.read_tree()?;
            let blobs = txn.read_blobs()?;

            blobs.get(&tree, loc)?.ok_or(StorageError::NotFound)?
        };

        Self::open_with_info(db, info)
    }

    pub(crate) fn open_with_info(
        db: &Arc<ArenaDatabase>,
        info: BlobInfo,
    ) -> Result<Self, StorageError> {
        let blob_dir = db.blobs().blob_dir.as_path();
        let path = blob_dir.join(info.inode.hex());
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(path)?;
        let file_meta = file.metadata()?;
        if info.size != file_meta.len() {
            file.set_len(info.size)?;
            file.flush()?;
        }

        db.blobs().open_blobs.increment(info.inode);

        Ok(Self {
            inode: info.inode,
            file: tokio::fs::File::from_std(file),
            available_ranges: info.available_ranges,
            db: Arc::clone(db),
            pending_ranges: ByteRanges::new(),
            size: info.size,
            hash: info.hash,
            offset: 0,
        })
    }

    /// Get the size of the corresponding file.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Current read/write position within the file.
    ///
    /// This is the position relative to the start of the file.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Get the parts of the file that are available locally.
    ///
    /// This might differ from local availability as reported by the
    /// cache, since this:
    /// - includes ranges that have been written to the file, but
    ///   haven't been flushed yet.
    /// - does not include ranges written through other file handles
    pub fn local_availability(&self) -> &ByteRanges {
        &self.available_ranges
    }

    /// Get the hash of the corresponding file.
    pub fn hash(&self) -> &Hash {
        &self.hash
    }

    /// Compute hash from the current local content.
    ///
    /// This hashes the entire content, no matter the current offset.
    /// When this ends without errors, offset is at the end of the
    /// file.
    pub async fn compute_hash(&mut self) -> Result<Hash, std::io::Error> {
        if self.offset != 0 {
            self.seek(SeekFrom::Start(0)).await?;
        }

        hash_file(self).await
    }

    /// Flush and mark file content as verified on the database.
    ///
    /// Return true if the database was updated
    pub async fn mark_verified(&mut self) -> Result<bool, StorageError> {
        self.update_db().await?;

        let inode = self.inode;
        let db = self.db.clone();
        let hash = self.hash.clone();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_write()?;
            {
                let mut blobs = txn.write_blobs()?;
                if !blobs.mark_verified(
                    &mut txn.read_tree()?,
                    &mut txn.write_dirty()?,
                    inode,
                    &hash,
                )? {
                    return Ok(false);
                }
            };
            txn.commit()?;
            Ok::<bool, StorageError>(true)
        })
        .await?
    }

    /// Make sure any updated content is stored on disk before
    /// continuing.
    pub async fn flush_and_sync(&mut self) -> std::io::Result<()> {
        self.file.flush().await?;
        self.file.sync_all().await?;

        Ok(())
    }

    /// Flush data and report any ranges written to to the database.
    pub async fn update_db(&mut self) -> Result<bool, StorageError> {
        if self.pending_ranges.is_empty() {
            return Ok(false);
        }
        self.flush_and_sync().await?;

        let inode = self.inode;
        let ranges = self.pending_ranges.clone();
        let db = Arc::clone(&self.db);
        let hash = self.hash.clone();

        let (res, ranges) = tokio::task::spawn_blocking(move || {
            fn extend(
                db: Arc<ArenaDatabase>,
                inode: Inode,
                hash: &Hash,
                ranges: &ByteRanges,
            ) -> Result<bool, StorageError> {
                let txn = db.begin_write()?;
                {
                    let mut blobs = txn.write_blobs()?;
                    if !blobs.extend_local_availability(
                        &mut txn.read_tree()?,
                        inode,
                        &hash,
                        &ranges,
                    )? {
                        // The blob has changed in the database
                        return Ok(false);
                    }
                }
                txn.commit()?;
                Ok(true)
            }

            (extend(db, inode, &hash, &ranges), ranges)
        })
        .await?;
        if matches!(res, Ok(true)) {
            // Keep the range as pending again, since updating failed.
            self.pending_ranges = self.pending_ranges.subtraction(&ranges);
        }

        res
    }

    /// Returns how much data, out of `requested_len` can be read at `offset`.
    ///
    /// - `Some(0)` means that reading would succeed, but would return nothing.
    /// - `None` means that reading would fail with the error [BlobIncomplete].
    pub fn readable_length(&self, offset: u64, requested_len: usize) -> Option<usize> {
        // It's always possible to read nothing, and nothing is what you'll get.
        // A file can always be read past its end, but yields no data.
        if requested_len == 0 || offset >= self.size() {
            return Some(0);
        }
        // Read what's left in a range containing offset
        self.available_ranges
            .containing_range(offset)
            .map(|r| min(requested_len, (r.end - offset) as usize))
    }
}

impl Drop for Blob {
    fn drop(&mut self) {
        // Decrement the reference count when the blob is dropped
        self.db.blobs().open_blobs.decrement(self.inode);
    }
}

impl AsyncRead for Blob {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.readable_length(self.offset, buf.remaining()) {
            None => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                BlobIncomplete,
            ))),
            Some(0) => Poll::Ready(Ok(())),
            Some(adjusted_len) => {
                let mut shortbuf = buf.take(adjusted_len);
                match Pin::new(&mut self.file).poll_read(cx, &mut shortbuf) {
                    Poll::Ready(Ok(())) => {
                        let filled = shortbuf.filled().len();
                        let initialized = shortbuf.initialized().len();
                        self.offset += filled as u64;
                        drop(shortbuf);

                        // We know at least initialized bytes of the
                        // previously unfilled portion of the buffer
                        // have been initialized through shortbuf.
                        unsafe {
                            buf.assume_init(initialized);
                        }
                        buf.set_filled(buf.filled().len() + filled);

                        Poll::Ready(Ok(()))
                    }
                    Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }
}

impl AsyncSeek for Blob {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        Pin::new(&mut self.file).start_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        match Pin::new(&mut self.file).poll_complete(cx) {
            Poll::Ready(Ok(pos)) => {
                self.offset = pos;
                Poll::Ready(Ok(pos))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for Blob {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let res = Pin::new(&mut self.file).poll_write(cx, buf);

        if let Poll::Ready(Ok(len)) = &res {
            if *len > 0 {
                let start = self.offset;
                let end = start + (*len as u64);
                self.offset = end;
                let range = ByteRange::new(start, end);
                self.available_ranges.add(&range);
                self.pending_ranges.add(&range);
            }
        }

        res
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.file).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.file).poll_shutdown(cx)
    }
}

struct OpenBlobRefCounter {
    counts: Mutex<HashMap<Inode, u32>>,
}
impl OpenBlobRefCounter {
    fn new() -> Self {
        Self {
            counts: Mutex::new(HashMap::new()),
        }
    }

    fn increment(&self, id: Inode) {
        let mut guard = self.counts.lock().unwrap();
        *guard.entry(id).or_insert(0) += 1;
        log::debug!("Blob {} opened, ref count: {}", id, guard[&id]);
    }
    fn decrement(&self, id: Inode) {
        let mut guard = self.counts.lock().unwrap();
        if let Some(count) = guard.get_mut(&id) {
            *count = count.saturating_sub(1);
            log::debug!("Blob {} closed, ref count: {}", id, *count);
            if *count == 0 {
                guard.remove(&id);
                log::debug!("Blob {} removed from open blobs", id);
            }
        }
    }
    fn is_open(&self, id: Inode) -> bool {
        let guard = self.counts.lock().unwrap();
        guard.contains_key(&id)
    }
}

fn get_read_op(
    blob_table: &impl ReadableTable<Inode, Holder<'static, BlobTableEntry>>,
    inode: Inode,
) -> Result<Option<BlobInfo>, StorageError> {
    if let Some(e) = blob_table.get(inode)? {
        Ok(Some(BlobInfo::new(inode, e.value().parse()?)))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arena::arena_cache::ArenaCache;
    use crate::arena::db::{ArenaReadTransaction, ArenaWriteTransaction};
    use crate::arena::mark;
    use crate::utils::hash;
    use crate::{Inode, Mark, Notification};
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use realize_types::{Arena, Path, Peer, UnixTime};
    use std::io::SeekFrom;
    use std::os::unix::fs::MetadataExt;
    use std::sync::Arc;
    use tokio::fs;
    use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

    fn test_peer() -> Peer {
        Peer::from("test_peer")
    }

    fn test_arena() -> Arena {
        Arena::from("test_arena")
    }

    fn test_hash() -> Hash {
        Hash([1u8; 32])
    }

    fn test_time() -> UnixTime {
        UnixTime::from_secs(1234567890)
    }

    fn later_time() -> UnixTime {
        UnixTime::from_secs(1234567891)
    }

    struct Fixture {
        arena: Arena,
        acache: Arc<ArenaCache>,
        db: Arc<ArenaDatabase>,
        tempdir: TempDir,
    }
    impl Fixture {
        fn setup() -> anyhow::Result<Fixture> {
            let _ = env_logger::try_init();
            let arena = test_arena();
            let tempdir = TempDir::new()?;
            let child = tempdir.child(format!("{arena}-cache.db"));
            let blob_dir = tempdir.child(format!("{arena}/blobs"));
            if let Some(p) = child.parent() {
                std::fs::create_dir_all(p)?;
            }
            let db = ArenaDatabase::for_testing_single_arena(arena, blob_dir.path())?;
            let acache = ArenaCache::new(arena, Arc::clone(&db), blob_dir.path())?;

            Ok(Self {
                arena,
                db,
                acache,
                tempdir,
            })
        }

        fn add_file(&self, path: &str, size: u64) -> anyhow::Result<Inode> {
            let path = Path::parse(path)?;

            self.acache.update(
                test_peer(),
                Notification::Add {
                    arena: self.arena,
                    index: 1,
                    path: path.clone(),
                    mtime: test_time(),
                    size,
                    hash: test_hash(),
                },
                None,
            )?;

            let inode = self.acache.expect(&path)?;

            Ok(inode)
        }

        fn add_file_with_mtime(
            &self,
            path: &Path,
            size: u64,
            mtime: UnixTime,
        ) -> anyhow::Result<()> {
            self.acache.update(
                test_peer(),
                Notification::Add {
                    arena: self.arena,
                    index: 1,
                    path: path.clone(),
                    mtime: mtime.clone(),
                    size,
                    hash: test_hash(),
                },
                None,
            )?;

            Ok(())
        }

        fn remove_file(&self, path: &Path) -> anyhow::Result<()> {
            self.acache.update(
                test_peer(),
                Notification::Remove {
                    arena: self.arena,
                    index: 1,
                    path: path.clone(),
                    old_hash: test_hash(),
                },
                None,
            )?;

            Ok(())
        }

        /// Check if a blob file exists for the given blob ID in the test arena.
        fn blob_file_exists(&self, inode: Inode) -> bool {
            self.blob_path(inode).exists()
        }

        /// Return the path to a blob file for test use.
        fn blob_path(&self, inode: Inode) -> std::path::PathBuf {
            self.tempdir
                .child(format!("{}/blobs/{}", self.arena, inode.hex()))
                .to_path_buf()
        }

        fn begin_read(&self) -> anyhow::Result<ArenaReadTransaction> {
            Ok(self.db.begin_read()?)
        }

        fn begin_write(&self) -> anyhow::Result<ArenaWriteTransaction> {
            Ok(self.db.begin_write()?)
        }

        fn get_blob_entry(&self, inode: Inode) -> anyhow::Result<BlobTableEntry> {
            let txn = self.begin_read()?;
            let blob_table = txn.blob_table()?;
            Ok(get_blob_entry(&blob_table, inode)?.ok_or(StorageError::NotFound)?)
        }

        /// Check if a blob entry exists in the database.
        fn blob_entry_exists(&self, inode: Inode) -> anyhow::Result<bool> {
            let txn = self.begin_read()?;
            let blob_table = txn.blob_table()?;
            match blob_table.get(inode)? {
                Some(_) => Ok(true),
                None => Ok(false),
            }
        }

        fn list_queue_content(&self, queue_id: LruQueueId) -> anyhow::Result<Vec<Inode>> {
            let mut content = vec![];
            let txn = self.begin_read()?;
            let blob_lru_queue_table = txn.blob_lru_queue_table()?;
            let blob_table = txn.blob_table()?;
            let queue = blob_lru_queue_table
                .get(queue_id as u16)?
                .unwrap()
                .value()
                .parse()?;
            let mut current = queue.head;
            while let Some(id) = current {
                content.push(id);
                current = follow_queue_link(&blob_table, id)?.next;
            }

            Ok(content)
        }

        fn list_queue_content_backward(&self, queue_id: LruQueueId) -> anyhow::Result<Vec<Inode>> {
            let mut content = vec![];
            let txn = self.begin_read()?;
            let blob_lru_queue_table = txn.blob_lru_queue_table()?;
            let blob_table = txn.blob_table()?;
            let queue = blob_lru_queue_table
                .get(queue_id as u16)?
                .unwrap()
                .value()
                .parse()?;
            let mut current = queue.tail;
            while let Some(id) = current {
                content.push(id);
                current = follow_queue_link(&blob_table, id)?.prev;
            }

            Ok(content)
        }

        fn queue_disk_usage(&self, queue_id: LruQueueId) -> anyhow::Result<u64> {
            let txn = self.begin_read()?;
            let queue_table = txn.blob_lru_queue_table()?;
            Ok(get_queue_if_available(&queue_table, queue_id)?
                .map(|q| q.disk_usage)
                .unwrap_or(0))
        }

        fn create_blob_with_partial_data<'b, L: Into<TreeLoc<'b>>>(
            &self,
            loc: L,
            test_data: &'static str,
            partial: usize,
        ) -> anyhow::Result<BlobInfo> {
            let hash = hash::digest(test_data);

            let txn = self.begin_write()?;
            let info: BlobInfo;
            {
                let marks = txn.read_marks()?;
                let mut blobs = txn.write_blobs()?;
                let mut tree = txn.write_tree()?;

                info = blobs.create(&mut tree, &marks, loc, &hash, test_data.len() as u64)?;

                if partial > 0 {
                    let blob_path = self.blob_path(info.inode);
                    std::fs::write(&blob_path, &test_data[0..partial])?;
                    blobs.extend_local_availability(
                        &tree,
                        info.inode,
                        &hash,
                        &ByteRanges::single(0, partial as u64),
                    )?;
                }
            }
            txn.commit()?;

            Ok(info)
        }

        fn blob_info<'b, L: Into<TreeLoc<'b>>>(&self, loc: L) -> anyhow::Result<Option<BlobInfo>> {
            let txn = self.begin_read()?;
            let tree = txn.read_tree()?;
            let blobs = txn.read_blobs()?;
            Ok(blobs.get(&tree, loc)?)
        }
    }

    #[test]
    fn create_blob() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.begin_write()?;
        let mut blobs = txn.write_blobs()?;
        let marks = txn.write_marks()?;
        let mut tree = txn.write_tree()?;
        let path = Path::parse("blob/test.txt")?;

        let info = blobs.create(&mut tree, &marks, &path, &hash::digest("test"), 4)?;
        assert_eq!(hash::digest("test"), info.hash);
        assert_eq!(4, info.size);
        assert_eq!(ByteRanges::new(), info.available_ranges);

        let inode = info.inode;
        assert_eq!(tree.resolve(path)?, Some(inode));
        assert_eq!(Some(info), blobs.get_with_inode(inode)?);

        let file_path = fixture.blob_path(inode);
        assert!(file_path.exists());
        let m = file_path.metadata()?;
        assert_eq!(4, m.len());
        assert_eq!(0, m.blocks()); // sparse file

        Ok(())
    }

    #[tokio::test]
    async fn recreate_blob() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.begin_write()?;
        let mut blobs = txn.write_blobs()?;
        let marks = txn.write_marks()?;
        let mut tree = txn.write_tree()?;
        let path = Path::parse("blob/test1.txt")?;

        let old_info = blobs.create(&mut tree, &marks, &path, &hash::digest("old"), 3)?;
        {
            let mut blob = Blob::open_with_info(&fixture.db, old_info)?;
            blob.write_all(b"old").await?;
            blob.flush().await?;
        }

        let new_info = blobs.create(&mut tree, &marks, &path, &hash::digest("new!"), 4)?;

        let actual_info = blobs.get_with_inode(new_info.inode)?.unwrap();
        assert_eq!(actual_info, new_info);

        let m = fixture.blob_path(actual_info.inode).metadata()?;
        assert_eq!(4, m.len());
        assert_eq!(0, m.blocks()); // sparse file

        Ok(())
    }

    #[tokio::test]
    async fn create_blob_noop_if_same_hash() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("blob/test1.txt")?;

        let txn = fixture.begin_write()?;
        {
            let mut blobs = txn.write_blobs()?;
            let marks = txn.write_marks()?;
            let mut tree = txn.write_tree()?;
            blobs.create(&mut tree, &marks, &path, &hash::digest("old"), 3)?;
        }
        txn.commit()?;

        {
            let mut blob = Blob::open(&fixture.db, &path)?;
            blob.write_all(b"old").await?;
            blob.flush().await?;
        }

        let txn = fixture.begin_write()?;
        let mut blobs = txn.write_blobs()?;
        let marks = txn.write_marks()?;
        let mut tree = txn.write_tree()?;

        // create does nothing given the same hash
        let actual_info = blobs.get(&mut tree, &path)?.unwrap();
        let new_info = blobs.create(&mut tree, &marks, &path, &hash::digest("old"), 3)?;

        // neither the info nor the file data was modified.
        assert_eq!(actual_info, new_info);
        assert_eq!(
            "old",
            std::fs::read_to_string(fixture.blob_path(actual_info.inode))?
        );

        Ok(())
    }

    #[test]
    fn create_blob_with_mark() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.begin_write()?;
        let mut marks = txn.write_marks()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut tree = txn.write_tree()?;

        let watch_path = Path::parse("watch")?;
        marks.set(&mut tree, &mut dirty, &watch_path, Mark::Watch)?;

        let keep_path = Path::parse("keep")?;
        marks.set(&mut tree, &mut dirty, &keep_path, Mark::Keep)?;

        let own_path = Path::parse("own")?;
        marks.set(&mut tree, &mut dirty, &own_path, Mark::Own)?;

        let hash = test_hash();
        assert_eq!(
            false,
            blobs
                .create(&mut tree, &marks, &watch_path, &hash, 4)?
                .protected
        );
        assert_eq!(
            true,
            blobs
                .create(&mut tree, &marks, &keep_path, &hash, 4)?
                .protected
        );
        assert_eq!(
            true,
            blobs
                .create(&mut tree, &marks, &own_path, &hash, 4)?
                .protected
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_blob_within_available_ranges() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("baa/baa")?;
        fixture.create_blob_with_partial_data(
            &path,
            "Baa, baa, black sheep, have you any wool? Yes, sir! Yes, sir! Three bags full",
            41,
        )?;

        let mut blob = Blob::open(&fixture.db, &path)?;

        let mut buf = [0; 5];
        let n = blob.read(&mut buf).await?;
        assert_eq!(b"Baa, ", &buf[0..n]);

        let mut buf = [0; 100];
        let n = blob.read(&mut buf).await?;
        assert_eq!(b"baa, black sheep, have you any wool?", &buf[0..n]);

        Ok(())
    }

    #[tokio::test]
    async fn read_blob_after_seek_within_available_range() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("baa/baa")?;
        fixture.create_blob_with_partial_data(
            &path,
            "Baa, baa, black sheep, have you any wool? Yes, sir! Yes, sir! Three bags full",
            41,
        )?;

        let mut blob = Blob::open(&fixture.db, &path)?;
        blob.seek(SeekFrom::Start(b"Baa, baa, black sheep, ".len() as u64))
            .await?;
        let mut buf = [0; 100];
        let n = blob.read(&mut buf).await?;
        assert_eq!(b"have you any wool?", &buf[0..n]);

        Ok(())
    }

    #[tokio::test]
    async fn read_blob_outside_available_ranges() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("baa/baa")?;
        fixture.create_blob_with_partial_data(&path, "Baa, baa, black sheep", 3)?;

        let mut blob = Blob::open(&fixture.db, &path)?;
        blob.seek(SeekFrom::Start(10)).await?;
        let mut buf = [0; 100];
        let res = blob.read(&mut buf).await;
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(BlobIncomplete::matches(&err));

        Ok(())
    }

    #[tokio::test]
    async fn writing_to_blob_updates_local_availability() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("baa/baa")?;
        fixture.create_blob_with_partial_data(&path, "Baa, baa, black sheep", 0)?;

        let mut blob = Blob::open(&fixture.db, &path)?;

        blob.write(b"Baa, baa").await?;
        assert_eq!(ByteRanges::single(0, 8), *blob.local_availability());

        // At this point, local availability is only in the blob, not
        // yet stored on the database.
        assert_eq!(
            ByteRanges::new(),
            fixture.blob_info(&path).unwrap().unwrap().available_ranges
        );

        blob.write(b", black sheep").await?;
        assert_eq!(ByteRanges::single(0, 21), *blob.local_availability());

        blob.update_db().await?;
        drop(blob);

        // After update_db, local availability changes on the database.
        assert_eq!(
            ByteRanges::single(0, 21),
            fixture.blob_info(&path)?.unwrap().available_ranges
        );

        // The file can now be read fully
        let mut blob = Blob::open(&fixture.db, &path)?;
        let mut buf = String::new();
        blob.read_to_string(&mut buf).await?;
        assert_eq!("Baa, baa, black sheep", buf.as_str());

        Ok(())
    }

    #[tokio::test]
    async fn writing_to_blob_out_of_order() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("baa/baa")?;
        fixture.create_blob_with_partial_data(&path, "Baa, baa, black sheep", 0)?;

        let mut blob = Blob::open(&fixture.db, &path)?;

        blob.seek(SeekFrom::Start(8)).await?;
        blob.write(b", black sheep").await?;

        blob.seek(SeekFrom::Start(0)).await?;
        blob.write(b"Baa, baa").await?;

        blob.update_db().await?;

        let mut blob = Blob::open(&fixture.db, &path)?;
        let mut buf = String::new();
        blob.read_to_string(&mut buf).await?;
        assert_eq!("Baa, baa, black sheep", buf.as_str());

        Ok(())
    }

    #[tokio::test]
    async fn writing_to_blob_multiple_update_db() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("baa/baa")?;
        fixture.create_blob_with_partial_data(&path, "Baa, baa, black sheep", 0)?;

        let mut blob = Blob::open(&fixture.db, &path)?;

        blob.seek(SeekFrom::Start(8)).await?;
        blob.write(b", black sheep").await?;
        blob.update_db().await?;
        assert_eq!(
            ByteRanges::single(8, 21),
            fixture.blob_info(&path)?.unwrap().available_ranges
        );

        blob.seek(SeekFrom::Start(0)).await?;
        blob.write(b"Baa, baa").await?;

        blob.update_db().await?;

        let mut blob = Blob::open(&fixture.db, &path)?;
        let mut buf = String::new();
        blob.read_to_string(&mut buf).await?;
        assert_eq!("Baa, baa, black sheep", buf.as_str());

        Ok(())
    }

    #[tokio::test]
    async fn writing_to_blob_new_hash_multiple_update_db() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("baa/baa")?;
        fixture.create_blob_with_partial_data(&path, "Baa, baa, black sheep", 0)?;

        let mut blob = Blob::open(&fixture.db, &path)?;

        blob.write(b"Baa, baa").await?;
        assert_eq!(true, blob.update_db().await?);
        assert_eq!(
            ByteRanges::single(0, 8),
            fixture.blob_info(&path)?.unwrap().available_ranges
        );

        let txn = fixture.begin_write()?;
        {
            let marks = txn.read_marks()?;
            let mut blobs = txn.write_blobs()?;
            let mut tree = txn.write_tree()?;

            blobs.create(&mut tree, &marks, &path, &hash::digest("other"), 100)?;
        }
        txn.commit()?;

        blob.write(b", black sheep").await?;
        assert_eq!(false, blob.update_db().await?);
        assert_eq!(ByteRanges::single(0, 21), *blob.local_availability());
        assert_eq!(
            ByteRanges::new(),
            fixture.blob_info(&path)?.unwrap().available_ranges
        );

        assert_eq!(false, blob.mark_verified().await?);
        assert_eq!(ByteRanges::single(0, 21), *blob.local_availability());
        assert_eq!(
            ByteRanges::new(),
            fixture.blob_info(&path)?.unwrap().available_ranges
        );

        Ok(())
    }

    // TODO:move to arena_cache test
    #[tokio::test]
    async fn blob_deleted_on_file_overwrite() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;

        // Create a file
        fixture.add_file_with_mtime(&file_path, 100, test_time())?;

        // Open the file to create a blob
        let inode = acache.expect(&file_path)?;
        acache.open_file(inode)?;

        // Verify the blob file was created
        assert!(fixture.blob_file_exists(inode));
        // Verify the blob entry exists in the database
        assert!(fixture.blob_entry_exists(inode)?);

        // Overwrite the file with a new version
        acache.update(
            test_peer(),
            Notification::Replace {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: later_time(),
                size: 200,
                hash: Hash([2u8; 32]),
                old_hash: test_hash(),
            },
            None,
        )?;

        // Verify the blob file has been deleted
        assert!(!fixture.blob_file_exists(inode));
        // Verify the blob entry has been deleted from the database
        assert!(!fixture.blob_entry_exists(inode)?);

        Ok(())
    }

    // TODO:move to arena_cache test
    #[tokio::test]
    async fn blob_deleted_on_file_removal() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;
        let file_path = Path::parse("file.txt")?;

        // Create a file
        fixture.add_file_with_mtime(&file_path, 100, test_time())?;

        // Open the file to create a blob
        let inode = acache.expect(&file_path)?;
        acache.open_file(inode)?;

        // Verify the blob file was created
        assert!(fixture.blob_file_exists(inode));
        // Verify the blob entry exists in the database
        assert!(fixture.blob_entry_exists(inode)?);

        // Remove the file
        fixture.remove_file(&file_path)?;

        // Verify the blob file has been deleted
        assert!(!fixture.blob_file_exists(inode));
        // Verify the blob entry has been deleted from the database
        assert!(!fixture.blob_entry_exists(inode)?);

        Ok(())
    }

    // TODO:move to arena_cache test
    #[tokio::test]
    async fn blob_deleted_on_catchup_removal() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;

        // Create a file
        fixture.add_file_with_mtime(&file_path, 100, test_time())?;

        // Open the file to create a blob
        let inode = acache.expect(&file_path)?;
        acache.open_file(inode)?;

        // Verify the blob file was created
        assert!(fixture.blob_file_exists(inode));
        // Verify the blob entry exists in the database
        assert!(fixture.blob_entry_exists(inode)?);

        // Do a catchup that doesn't include this file (simulating file removal)
        acache.update(test_peer(), Notification::CatchupStart(arena), None)?;
        // Note: No Catchup notification for the file, so it will be deleted
        acache.update(
            test_peer(),
            Notification::CatchupComplete {
                arena: arena,
                index: 0,
            },
            None,
        )?;

        // Verify the blob file has been deleted
        assert!(!fixture.blob_file_exists(inode));
        // Verify the blob entry has been deleted from the database
        assert!(!fixture.blob_entry_exists(inode)?);

        Ok(())
    }

    #[tokio::test]
    async fn mark_verified() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("baa/baa")?;
        fixture.create_blob_with_partial_data(&path, "Baa, baa, black sheep", 21)?;

        assert_eq!(false, fixture.blob_info(&path)?.unwrap().verified);
        let mut blob = Blob::open(&fixture.db, &path)?;
        assert_eq!(ByteRanges::single(0, 21), *blob.local_availability());
        assert_eq!(true, blob.mark_verified().await?);
        assert_eq!(true, fixture.blob_info(&path)?.unwrap().verified);
        drop(blob);

        Ok(())
    }

    #[test]
    fn mark_verified_wrong_hash() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("baa/baa")?;
        fixture.create_blob_with_partial_data(&path, "Baa, baa, black sheep", 21)?;

        let txn = fixture.begin_write()?;
        let mut blobs = txn.write_blobs()?;
        let tree = txn.read_tree()?;
        let mut dirty = txn.write_dirty()?;
        assert_eq!(
            false,
            blobs.mark_verified(&tree, &mut dirty, &path, &hash::digest("wrong"))?
        );
        assert_eq!(false, blobs.get(&tree, &path)?.unwrap().verified);

        Ok(())
    }

    #[tokio::test]
    async fn export_success() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("baa/baa")?;
        fixture.create_blob_with_partial_data(&path, "Baa, baa, black sheep", 21)?;
        let BlobInfo { inode, hash, .. } = fixture.blob_info(&path)?.unwrap();

        assert_eq!(true, Blob::open(&fixture.db, &path)?.mark_verified().await?);

        // export to dest
        let dest = fixture.tempdir.path().join("moved_blob");
        let txn = fixture.begin_write()?;
        {
            let mut blobs = txn.write_blobs()?;
            let mut tree = txn.write_tree()?;
            assert_eq!(true, blobs.export(&mut tree, &path, &hash, &dest)?);
        }
        txn.commit()?;

        assert_eq!("Baa, baa, black sheep", std::fs::read_to_string(&dest)?);
        assert!(fixture.blob_info(inode)?.is_none());

        // The LRU queue must have been updated properly.
        assert!(
            fixture
                .list_queue_content(LruQueueId::WorkingArea)?
                .is_empty()
        );

        Ok(())
    }

    #[tokio::test]
    async fn export_wrong_hash() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("baa/baa")?;
        fixture.create_blob_with_partial_data(&path, "Baa, baa, black sheep", 21)?;

        assert_eq!(true, Blob::open(&fixture.db, &path)?.mark_verified().await?);

        // export to dest
        let dest = fixture.tempdir.path().join("moved_blob");
        let txn = fixture.begin_write()?;
        {
            let mut blobs = txn.write_blobs()?;
            let mut tree = txn.write_tree()?;
            assert_eq!(
                false,
                blobs.export(&mut tree, &path, &hash::digest("wrong!"), &dest)?
            );
        }
        txn.commit()?;

        assert!(!dest.exists());
        assert!(fixture.blob_info(&path)?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn export_not_verified() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("baa/baa")?;
        fixture.create_blob_with_partial_data(&path, "Baa, baa, black sheep", 21)?;
        let hash = fixture.blob_info(&path)?.unwrap().hash;

        // do not mark verified

        // export to dest
        let dest = fixture.tempdir.path().join("moved_blob");
        let txn = fixture.begin_write()?;
        {
            let mut blobs = txn.write_blobs()?;
            let mut tree = txn.write_tree()?;
            assert_eq!(false, blobs.export(&mut tree, &path, &hash, &dest)?);
        }

        Ok(())
    }

    #[tokio::test]
    async fn export_no_blob() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("baa/baa")?;

        let dest = fixture.tempdir.path().join("moved_blob");
        let txn = fixture.begin_write()?;
        {
            let mut blobs = txn.write_blobs()?;
            let mut tree = txn.write_tree()?;
            assert_eq!(
                false,
                blobs.export(&mut tree, &path, &hash::digest("test"), &dest)?
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn move_into_blob_if_matches_creates_new_blob() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;
        let file_path = Path::parse("test.txt")?;

        // Create a file and add it to the cache
        let inode = fixture.add_file(file_path.as_str(), 48)?;

        let tmpfile = fixture.tempdir.child("tmp");
        tmpfile.write_str("When you light a candle, you also cast a shadow.")?;

        let txn = fixture.begin_write()?;
        let path_in_cache = acache
            .move_into_blob_if_matches(&txn, &file_path, &test_hash(), &tmpfile.path().metadata()?)?
            .unwrap();
        txn.commit()?;

        std::fs::rename(tmpfile.path(), path_in_cache)?;

        // Now open the file through the normal open_file mechanism
        let mut blob = acache.open_file(inode)?;
        let mut buf = String::new();
        blob.read_to_string(&mut buf).await?;
        assert_eq!(
            "When you light a candle, you also cast a shadow.".to_string(),
            buf
        );
        assert_eq!(ByteRanges::single(0, 48), *blob.local_availability());

        // Disk usage must be set.
        let blob_entry = fixture.get_blob_entry(inode)?;
        assert!(blob_entry.disk_usage > INODE_DISK_USAGE);

        Ok(())
    }

    #[tokio::test]
    async fn move_into_blob_if_matches_creates_new_protected_blob() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;
        let file_path = Path::parse("test.txt")?;

        let inode = fixture.add_file(file_path.as_str(), 3)?;
        mark::set(&fixture.db, &file_path, Mark::Keep)?;

        let tmpfile = fixture.tempdir.child("tmp");
        tmpfile.write_str("foo")?;

        let txn = fixture.begin_write()?;
        acache
            .move_into_blob_if_matches(&txn, &file_path, &test_hash(), &tmpfile.path().metadata()?)?
            .unwrap();
        txn.commit()?;

        // Since the file is marked keep, the blob that was created
        // must be in the protected queue.
        acache.open_file(inode)?;
        assert_eq!(
            LruQueueId::Protected,
            fixture.get_blob_entry(inode).unwrap().queue
        );

        Ok(())
    }

    #[tokio::test]
    async fn move_into_blob_if_matches_overwrites_existing_blob() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;
        let file_path = Path::parse("test.txt")?;

        // Create a file and add it to the cache
        let inode = fixture.add_file(file_path.as_str(), 59)?;

        // First, create a blob through normal open_file
        acache.open_file(inode)?;

        // Now use move_into_blob_if_matches with the existing blob
        let tmpfile = fixture.tempdir.child("tmp");
        tmpfile.write_str("What sane person could live in this world and not be crazy?")?;

        let txn = fixture.begin_write()?;
        let dest_path = acache
            .move_into_blob_if_matches(&txn, &file_path, &test_hash(), &tmpfile.path().metadata()?)?
            .unwrap();
        txn.commit()?;
        std::fs::rename(tmpfile.path(), dest_path)?;

        // Open the file again and verify we can read the content within the ranges
        let mut blob = acache.open_file(inode)?;
        let mut buf = String::new();
        blob.read_to_string(&mut buf).await?;
        assert_eq!(
            "What sane person could live in this world and not be crazy?".to_string(),
            buf
        );
        assert_eq!(ByteRanges::single(0, 59), *blob.local_availability());

        // Disk usage must be set.
        let blob_entry = fixture.get_blob_entry(inode)?;
        assert!(blob_entry.disk_usage > INODE_DISK_USAGE);

        Ok(())
    }

    #[tokio::test]
    async fn move_into_blob_if_matches_reject_wrong_file_size() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;
        let file_path = Path::parse("test.txt")?;

        // Create a file and add it to the cache
        fixture.add_file(file_path.as_str(), 48)?;

        let tmpfile = fixture.tempdir.child("tmp");
        tmpfile.write_str("not 48 bytes")?;

        let txn = fixture.begin_write()?;
        assert_eq!(
            None,
            acache.move_into_blob_if_matches(
                &txn,
                &file_path,
                &test_hash(),
                &tmpfile.path().metadata()?
            )?
        );

        Ok(())
    }

    #[tokio::test]
    async fn move_into_blob_if_matches_rejects_wrong_hash() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;
        let file_path = Path::parse("test.txt")?;

        // Create a file and add it to the cache
        fixture.add_file(file_path.as_str(), 5)?;

        let txn = fixture.begin_write()?;

        let tmpfile = fixture.tempdir.child("tmp");
        tmpfile.write_str("hello")?;

        // hash is test_hash(), which is not [99; 32]
        assert_eq!(
            None,
            acache.move_into_blob_if_matches(
                &txn,
                &file_path,
                &Hash([99; 32]),
                &tmpfile.metadata()?
            )?
        );

        Ok(())
    }

    #[tokio::test]
    async fn add_to_working_area_on_blob_creation() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;

        let one = fixture.add_file("one", 100)?;
        acache.open_file(one)?;
        assert_eq!(
            (vec![one], vec![one]),
            (
                fixture.list_queue_content(LruQueueId::WorkingArea)?,
                fixture.list_queue_content_backward(LruQueueId::WorkingArea)?
            )
        );

        let two = fixture.add_file("two", 100)?;
        acache.open_file(two)?;
        assert_eq!(
            (vec![two, one], vec![one, two]),
            (
                fixture.list_queue_content(LruQueueId::WorkingArea)?,
                fixture.list_queue_content_backward(LruQueueId::WorkingArea)?
            )
        );

        let three = fixture.add_file("three", 100)?;
        acache.open_file(three)?;

        assert_eq!(
            (vec![three, two, one], vec![one, two, three]),
            (
                fixture.list_queue_content(LruQueueId::WorkingArea)?,
                fixture.list_queue_content_backward(LruQueueId::WorkingArea)?
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn remove_from_queue_on_blob_deletion() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;

        let one = fixture.add_file("one", 100)?;
        acache.open_file(one)?;
        let two = fixture.add_file("two", 100)?;
        acache.open_file(two)?;
        let three = fixture.add_file("three", 100)?;
        acache.open_file(three)?;
        let four = fixture.add_file("four", 100)?;
        acache.open_file(four)?;

        // Delete the file to delete the blob.
        fixture.remove_file(&Path::parse("two")?)?;

        assert_eq!(
            (vec![four, three, one], vec![one, three, four]),
            (
                fixture.list_queue_content(LruQueueId::WorkingArea)?,
                fixture.list_queue_content_backward(LruQueueId::WorkingArea)?
            )
        );

        fixture.remove_file(&Path::parse("one")?)?;

        assert_eq!(
            (vec![four, three], vec![three, four]),
            (
                fixture.list_queue_content(LruQueueId::WorkingArea)?,
                fixture.list_queue_content_backward(LruQueueId::WorkingArea)?
            )
        );

        fixture.remove_file(&Path::parse("four")?)?;
        assert_eq!(
            (vec![three], vec![three]),
            (
                fixture.list_queue_content(LruQueueId::WorkingArea)?,
                fixture.list_queue_content_backward(LruQueueId::WorkingArea)?
            )
        );

        fixture.remove_file(&Path::parse("three")?)?;
        assert_eq!(
            (vec![], vec![]),
            (
                fixture.list_queue_content(LruQueueId::WorkingArea)?,
                fixture.list_queue_content_backward(LruQueueId::WorkingArea)?
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn mark_accessed() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;

        let one = fixture.add_file("one", 100)?;
        acache.open_file(one)?;
        let two = fixture.add_file("two", 100)?;
        acache.open_file(two)?;
        let three = fixture.add_file("three", 100)?;
        acache.open_file(three)?;
        let four = fixture.add_file("four", 100)?;
        acache.open_file(four)?;

        // four is now at the head, being the most recent. Let's move
        // two there, then one..
        let txn = fixture.db.begin_write()?;
        {
            let mut blobs = txn.write_blobs()?;
            let tree = txn.read_tree()?;
            blobs.mark_accessed(&tree, two)?;
        }
        txn.commit()?;
        assert_eq!(
            (vec![two, four, three, one], vec![one, three, four, two]),
            (
                fixture.list_queue_content(LruQueueId::WorkingArea)?,
                fixture.list_queue_content_backward(LruQueueId::WorkingArea)?
            )
        );

        let txn = fixture.db.begin_write()?;
        {
            let mut blobs = txn.write_blobs()?;
            let tree = txn.read_tree()?;
            blobs.mark_accessed(&tree, one)?;
        }
        txn.commit()?;
        assert_eq!(
            (vec![one, two, four, three], vec![three, four, two, one]),
            (
                fixture.list_queue_content(LruQueueId::WorkingArea)?,
                fixture.list_queue_content_backward(LruQueueId::WorkingArea)?
            )
        );

        let txn = fixture.db.begin_write()?;
        {
            let mut blobs = txn.write_blobs()?;
            let tree = txn.read_tree()?;
            blobs.mark_accessed(&tree, one)?;
        }
        txn.commit()?;
        assert_eq!(
            (vec![one, two, four, three], vec![three, four, two, one]),
            (
                fixture.list_queue_content(LruQueueId::WorkingArea)?,
                fixture.list_queue_content_backward(LruQueueId::WorkingArea)?
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn disk_usage_tracking() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;

        // Create a blob and verify initial disk usage is 0
        let inode = fixture.add_file("test.txt", 1000)?;
        acache.open_file(inode)?;

        let blob_entry = fixture.get_blob_entry(inode)?;
        // 512 bytes for an empty file, to account for inode usage.
        assert_eq!(INODE_DISK_USAGE, blob_entry.disk_usage);

        // Write some data to the blob
        let mut blob = acache.open_file(inode)?;
        blob.write(b"test data").await?;
        blob.update_db().await?;

        // disk usage is now 1 block
        let blksize = fs::metadata(fixture.blob_path(inode)).await?.blksize();
        let blob_entry = fixture.get_blob_entry(inode)?;
        assert_eq!(blob_entry.disk_usage, blksize + INODE_DISK_USAGE);

        assert_eq!(
            blob_entry.disk_usage,
            fixture.queue_disk_usage(LruQueueId::WorkingArea)?
        );

        Ok(())
    }

    #[tokio::test]
    async fn disk_usage_updates_on_blob_deletion() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;

        // Create a blob and write some data
        let inode = fixture.add_file("test.txt", 1000)?;
        acache.open_file(inode)?;

        let mut blob = acache.open_file(inode)?;
        blob.write(b"test data").await?;
        blob.update_db().await?;

        // Get initial disk usage
        let blob_entry = fixture.get_blob_entry(inode)?;
        assert!(blob_entry.disk_usage > 0);

        let initial_disk_usage = fixture.queue_disk_usage(LruQueueId::WorkingArea)?;
        assert_eq!(initial_disk_usage, blob_entry.disk_usage);

        // Delete the blob
        fixture.remove_file(&Path::parse("test.txt")?)?;

        // Verify blob entry is gone
        assert!(!fixture.blob_entry_exists(inode)?);

        assert_eq!(0, fixture.queue_disk_usage(LruQueueId::WorkingArea)?);

        Ok(())
    }

    #[tokio::test]
    async fn disk_usage_tracking_multiple_blobs() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;

        // Create multiple blobs and write data
        let inode1 = fixture.add_file("test1.txt", 1000)?;
        let inode2 = fixture.add_file("test2.txt", 2000)?;

        acache.open_file(inode1)?;
        acache.open_file(inode2)?;

        let blksize = fs::metadata(fixture.blob_path(inode1)).await?.blksize();

        // Write data to first blob
        let mut blob1 = acache.open_file(inode1)?;
        blob1.write(b"data for blob 1").await?;
        blob1.update_db().await?;

        // Write data to second blob
        let mut blob2 = acache.open_file(inode2)?;
        blob2.write(b"data for blob 2 which is longer").await?;
        blob2.update_db().await?;

        // Get individual disk usages
        let blob_entry1 = fixture.get_blob_entry(inode1)?;
        let blob_entry2 = fixture.get_blob_entry(inode2)?;

        // Check queue total disk usage
        assert_eq!(
            2 * blksize + 2 * INODE_DISK_USAGE,
            fixture.queue_disk_usage(LruQueueId::WorkingArea)?
        );
        assert_eq!(
            2 * blksize + 2 * INODE_DISK_USAGE,
            blob_entry1.disk_usage + blob_entry2.disk_usage
        );

        Ok(())
    }

    #[tokio::test]
    async fn cleanup_cache_removes_blobs_until_target_size() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;

        // Create multiple blobs with data
        let inode1 = fixture.add_file("test1.txt", 1000)?;
        let inode2 = fixture.add_file("test2.txt", 2000)?;
        let inode3 = fixture.add_file("test3.txt", 3000)?;
        let inode4 = fixture.add_file("test4.txt", 4000)?;

        acache.open_file(inode1)?;
        acache.open_file(inode2)?;
        acache.open_file(inode3)?;
        acache.open_file(inode4)?;

        // Write data to all blobs to create disk usage
        let mut blob1 = acache.open_file(inode1)?;
        blob1.write(b"data for blob 1").await?;
        blob1.update_db().await?;
        drop(blob1);

        let mut blob2 = acache.open_file(inode2)?;
        blob2.write(b"data for blob 2 which is longer").await?;
        blob2.update_db().await?;
        drop(blob2);

        let mut blob3 = acache.open_file(inode3)?;
        blob3.write(b"data for blob 3 which is even longer").await?;
        blob3.update_db().await?;
        drop(blob3);

        let mut blob4 = acache.open_file(inode4)?;
        blob4
            .write(b"data for blob 4 which is the longest of all")
            .await?;
        blob4.update_db().await?;
        drop(blob4);

        let initial_disk_usage = fixture.queue_disk_usage(LruQueueId::WorkingArea)?;
        assert!(initial_disk_usage > 0);

        // Verify all blobs exist
        assert!(fixture.blob_entry_exists(inode1)?);
        assert!(fixture.blob_entry_exists(inode2)?);
        assert!(fixture.blob_entry_exists(inode3)?);
        assert!(fixture.blob_entry_exists(inode4)?);
        assert!(fixture.blob_file_exists(inode1));
        assert!(fixture.blob_file_exists(inode2));
        assert!(fixture.blob_file_exists(inode3));
        assert!(fixture.blob_file_exists(inode4));

        // Clean up cache to target size (should remove some blobs)
        let target_size = initial_disk_usage / 2; // Remove half the blobs
        let txn = fixture.db.begin_write()?;
        {
            let mut blobs = txn.write_blobs()?;
            blobs.cleanup(&mut txn.write_tree()?, target_size)?;
        }
        txn.commit()?;

        // Verify that some blobs were removed (the least recently used ones)
        // The queue order should be [4, 3, 2, 1] so 1 and 2 should be removed first
        assert!(!fixture.blob_entry_exists(inode1)?);
        assert!(!fixture.blob_entry_exists(inode2)?);
        assert!(fixture.blob_entry_exists(inode3)?);
        assert!(fixture.blob_entry_exists(inode4)?);
        assert!(!fixture.blob_file_exists(inode1));
        assert!(!fixture.blob_file_exists(inode2));
        assert!(fixture.blob_file_exists(inode3));
        assert!(fixture.blob_file_exists(inode4));

        // Verify the queue now only contains the remaining blobs
        assert_eq!(
            vec![inode4, inode3],
            fixture.list_queue_content(LruQueueId::WorkingArea)?
        );

        // Verify the disk usage is now at or below target
        assert!(fixture.queue_disk_usage(LruQueueId::WorkingArea)? <= target_size);

        Ok(())
    }

    #[tokio::test]
    async fn cleanup_cache_removes_all_blobs_when_target_is_zero() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;

        // Create a few blobs with data
        let inode1 = fixture.add_file("test1.txt", 1000)?;
        let inode2 = fixture.add_file("test2.txt", 2000)?;

        acache.open_file(inode1)?;
        acache.open_file(inode2)?;

        // Write data so there is something to cleanup
        let mut blob1 = acache.open_file(inode1)?;
        blob1.write(b"data for blob 1").await?;
        blob1.update_db().await?;
        drop(blob1);

        // Verify blobs exist
        assert!(fixture.blob_entry_exists(inode1)?);
        assert!(fixture.blob_entry_exists(inode2)?);
        assert!(fixture.blob_file_exists(inode1));
        assert!(fixture.blob_file_exists(inode2));

        // Clean up cache with target size 0 (should remove all blobs)
        let txn = fixture.db.begin_write()?;
        {
            let mut blobs = txn.write_blobs()?;
            blobs.cleanup(&mut txn.write_tree()?, 0)?;
        }
        txn.commit()?;

        // Verify all blobs were removed
        assert!(!fixture.blob_entry_exists(inode1)?);
        assert!(!fixture.blob_entry_exists(inode2)?);
        assert!(!fixture.blob_file_exists(inode1));
        assert!(!fixture.blob_file_exists(inode2));

        // Verify the queue is now empty
        assert_eq!(
            vec![] as Vec<Inode>,
            fixture.list_queue_content(LruQueueId::WorkingArea)?
        );

        assert_eq!(0, fixture.queue_disk_usage(LruQueueId::WorkingArea)?);

        Ok(())
    }

    #[tokio::test]
    async fn cleanup_cache_does_nothing_when_already_under_target() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;

        // Create a blob with data
        let inode = fixture.add_file("test.txt", 1000)?;
        acache.open_file(inode)?;

        let mut blob = acache.open_file(inode)?;
        blob.write(b"data for blob").await?;
        blob.update_db().await?;

        let current_disk_usage = fixture.queue_disk_usage(LruQueueId::WorkingArea)?;

        // Verify blob exists
        assert!(fixture.blob_entry_exists(inode)?);
        assert!(fixture.blob_file_exists(inode));

        // Clean up cache with target size higher than current usage
        let target_size = current_disk_usage * 2;
        let txn = fixture.db.begin_write()?;
        {
            let mut blobs = txn.write_blobs()?;
            blobs.cleanup(&mut txn.write_tree()?, target_size)?;
        }
        txn.commit()?;

        // Verify blob still exists (nothing should have been removed)
        assert!(fixture.blob_entry_exists(inode)?);
        assert!(fixture.blob_file_exists(inode));

        // Verify the queue still contains the blob
        assert_eq!(
            vec![inode],
            fixture.list_queue_content(LruQueueId::WorkingArea)?
        );

        // Verify the disk usage hasn't changed
        assert_eq!(
            current_disk_usage,
            fixture.queue_disk_usage(LruQueueId::WorkingArea)?
        );

        Ok(())
    }

    #[tokio::test]
    async fn cleanup_cache_handles_empty_queue() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        // Clean up cache - should not error
        let txn = fixture.db.begin_write()?;
        {
            let mut blobs = txn.write_blobs()?;
            blobs.cleanup(&mut txn.write_tree()?, 1000)?;
        }
        txn.commit()?;

        // Verify the queue is still empty by checking that no queue entry exists
        let txn = fixture.begin_read()?;
        let queue_table = txn.blob_lru_queue_table()?;
        let queue_entry = queue_table.get(LruQueueId::WorkingArea as u16)?;
        assert!(queue_entry.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn cleanup_cache_removes_blobs_in_lru_order() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;

        // Create blobs in order
        let inode1 = fixture.add_file("test1.txt", 1000)?;
        let inode2 = fixture.add_file("test2.txt", 1000)?;
        let inode3 = fixture.add_file("test3.txt", 1000)?;

        acache.open_file(inode1)?;
        acache.open_file(inode2)?;
        acache.open_file(inode3)?;

        // Write data to all blobs
        let mut blob1 = acache.open_file(inode1)?;
        blob1.write(b"data1").await?;
        blob1.update_db().await?;
        drop(blob1);

        let mut blob2 = acache.open_file(inode2)?;
        blob2.write(b"data2").await?;
        blob2.update_db().await?;
        drop(blob2);

        let mut blob3 = acache.open_file(inode3)?;
        blob3.write(b"data3").await?;
        blob3.update_db().await?;
        drop(blob3);

        // Access blob1 to move it to the front (make it most recently used)
        let txn = fixture.db.begin_write()?;
        {
            let mut blobs = txn.write_blobs()?;
            let tree = txn.read_tree()?;
            blobs.mark_accessed(&tree, inode1)?;
        }
        txn.commit()?;

        // Verify the order is now [1, 3, 2] (1 is most recent, 2 is least recent)
        assert_eq!(
            vec![inode1, inode3, inode2],
            fixture.list_queue_content(LruQueueId::WorkingArea)?
        );

        // Get current disk usage
        let current_disk_usage = fixture.queue_disk_usage(LruQueueId::WorkingArea)?;

        // Clean up cache to remove one blob (should remove inode2 as it's least recently used)
        let target_size = current_disk_usage - 1; // Just under current usage
        let txn = fixture.db.begin_write()?;
        {
            let mut blobs = txn.write_blobs()?;
            blobs.cleanup(&mut txn.write_tree()?, target_size)?;
        }
        txn.commit()?;

        // Verify inode2 was removed (least recently used)
        assert!(!fixture.blob_entry_exists(inode2)?);
        assert!(!fixture.blob_file_exists(inode2));

        // Verify inode1 and inode3 still exist
        assert!(fixture.blob_entry_exists(inode1)?);
        assert!(fixture.blob_entry_exists(inode3)?);
        assert!(fixture.blob_file_exists(inode1));
        assert!(fixture.blob_file_exists(inode3));

        // Verify the queue now contains only the remaining blobs in correct order
        assert_eq!(
            vec![inode1, inode3],
            fixture.list_queue_content(LruQueueId::WorkingArea)?
        );

        Ok(())
    }

    #[tokio::test]
    async fn cleanup_cache_skips_open_blobs() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;

        // Create a single blob with data
        let inode = fixture.add_file("test.txt", 1000)?;
        acache.open_file(inode)?;

        // Write data to the blob
        let mut blob = acache.open_file(inode)?;
        blob.write(b"data for blob").await?;
        blob.update_db().await?;

        // Keep the blob open
        let _open_blob = acache.open_file(inode)?;

        // Try to clean up cache - should skip the open blob
        let target_size = 0;
        acache.cleanup_cache(target_size)?;

        // Verify the blob still exists (it was open)
        assert!(fixture.blob_entry_exists(inode)?);
        assert!(fixture.blob_file_exists(inode));

        // Drop all blob handles
        drop(blob);
        drop(_open_blob);

        // Now clean up cache again - should delete the blob
        acache.cleanup_cache(target_size)?;

        // Verify the blob was deleted
        assert!(!fixture.blob_entry_exists(inode)?);
        assert!(!fixture.blob_file_exists(inode));

        Ok(())
    }

    #[tokio::test]
    async fn cleanup_cache_deletes_blob_after_it_is_closed() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;

        // Create a blob with data
        let inode = fixture.add_file("test.txt", 1000)?;
        acache.open_file(inode)?;

        let mut blob = acache.open_file(inode)?;
        blob.write(b"data for blob").await?;
        blob.update_db().await?;

        // Keep the blob open
        let _open_blob = acache.open_file(inode)?;

        // Try to clean up cache - should skip the open blob
        let target_size = 0;
        acache.cleanup_cache(target_size)?;

        // Verify the blob still exists (it was open)
        assert!(fixture.blob_entry_exists(inode)?);
        assert!(fixture.blob_file_exists(inode));

        // Drop all blob handles
        drop(blob);
        drop(_open_blob);

        // Now clean up cache again - should delete the blob
        acache.cleanup_cache(target_size)?;

        // Verify the blob was deleted
        assert!(!fixture.blob_entry_exists(inode)?);
        assert!(!fixture.blob_file_exists(inode));

        Ok(())
    }

    #[tokio::test]
    async fn cleanup_cache_leaves_protected_blobs_alone() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;
        let inode = fixture.add_file("test.txt", 6)?;
        mark::set(&fixture.db, &Path::parse("test.txt")?, Mark::Keep)?;

        {
            let mut blob = acache.open_file(inode)?;
            blob.write(b"foobar").await?;
            blob.update_db().await?;
        }

        acache.cleanup_cache(0)?;

        let mut blob = acache.open_file(inode)?;
        let mut buf = [0; 100];
        let n = blob.read(&mut buf).await?;
        assert_eq!(b"foobar", &buf[0..n]);

        Ok(())
    }

    #[tokio::test]
    async fn multiple_open_handles_work_correctly() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;

        // Create a blob with data
        let inode = fixture.add_file("test.txt", 1000)?;
        acache.open_file(inode)?;

        let mut blob = acache.open_file(inode)?;
        blob.write(b"data for blob").await?;
        blob.update_db().await?;

        // Open multiple handles to the same blob
        let open_blob1 = acache.open_file(inode)?;
        let open_blob2 = acache.open_file(inode)?;
        let open_blob3 = acache.open_file(inode)?;

        // Try to clean up cache - should skip the open blob
        let target_size = 0;
        acache.cleanup_cache(target_size)?;

        // Verify the blob still exists (it has multiple open handles)
        assert!(fixture.blob_entry_exists(inode)?);
        assert!(fixture.blob_file_exists(inode));

        // Drop one handle
        drop(open_blob1);

        // Try to clean up again - should still skip (2 handles remain)
        acache.cleanup_cache(target_size)?;

        // Verify the blob still exists
        assert!(fixture.blob_entry_exists(inode)?);
        assert!(fixture.blob_file_exists(inode));

        // Drop another handle
        drop(open_blob2);

        // Try to clean up again - should still skip (1 handle remains)
        acache.cleanup_cache(target_size)?;

        // Verify the blob still exists
        assert!(fixture.blob_entry_exists(inode)?);
        assert!(fixture.blob_file_exists(inode));

        // Drop the last handle
        drop(open_blob3);

        // Drop the initial blob handle as well
        drop(blob);

        // Now clean up cache - should delete the blob
        acache.cleanup_cache(target_size)?;

        // Verify the blob was deleted
        assert!(!fixture.blob_entry_exists(inode)?);
        assert!(!fixture.blob_file_exists(inode));

        Ok(())
    }

    #[tokio::test]
    async fn reference_counting_with_mixed_open_and_closed_blobs() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;

        // Create multiple blobs
        let inode1 = fixture.add_file("test1.txt", 1000)?;
        let inode2 = fixture.add_file("test2.txt", 2000)?;
        let inode3 = fixture.add_file("test3.txt", 3000)?;

        acache.open_file(inode1)?;
        acache.open_file(inode2)?;
        acache.open_file(inode3)?;

        // Write data to all blobs
        let mut blob1 = acache.open_file(inode1)?;
        blob1.write(b"data for blob 1").await?;
        blob1.update_db().await?;

        let mut blob2 = acache.open_file(inode2)?;
        blob2.write(b"data for blob 2").await?;
        blob2.update_db().await?;

        let mut blob3 = acache.open_file(inode3)?;
        blob3.write(b"data for blob 3").await?;
        blob3.update_db().await?;

        // Keep inode1 and inode3 open, but close inode2
        let _open_blob1 = acache.open_file(inode1)?;
        let _open_blob3 = acache.open_file(inode3)?;
        drop(blob2); // Close inode2

        // Clean up cache with target size 0 - should remove inode2 but keep inode1 and inode3
        acache.cleanup_cache(0)?;

        // Verify that inode1 and inode3 still exist (they were open)
        assert!(fixture.blob_entry_exists(inode1)?);
        assert!(fixture.blob_entry_exists(inode3)?);
        assert!(fixture.blob_file_exists(inode1));
        assert!(fixture.blob_file_exists(inode3));

        // Verify that inode2 was removed (it was closed)
        assert!(!fixture.blob_entry_exists(inode2)?);
        assert!(!fixture.blob_file_exists(inode2));

        // Drop all remaining blob handles
        drop(blob1);
        drop(blob3);
        drop(_open_blob1);
        drop(_open_blob3);

        Ok(())
    }

    #[tokio::test]
    async fn inodes_never_reused_after_deletion() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;

        let inode1 = fixture.add_file("test.txt", 100)?;
        acache.open_file(inode1)?;

        // Verify the blob exists
        assert!(fixture.blob_entry_exists(inode1)?);
        assert!(fixture.blob_file_exists(inode1));

        // Delete the file (which deletes the blob)
        fixture.remove_file(&Path::parse("test.txt")?)?;

        // Verify the blob is gone
        assert!(!fixture.blob_entry_exists(inode1)?);
        assert!(!fixture.blob_file_exists(inode1));

        // Create a new file
        let inode2 = fixture.add_file("test2.txt", 200)?;
        acache.open_file(inode2)?;

        // Verify the new blob ID is different from the deleted one
        assert_ne!(inode1, inode2);

        // Verify the new blob exists
        assert!(fixture.blob_entry_exists(inode2)?);
        assert!(fixture.blob_file_exists(inode2));

        Ok(())
    }

    #[tokio::test]
    async fn open_file_handles_blob_deleted_by_cleanup_cache() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let acache = &fixture.acache;

        // 1. Add a file
        let inode = fixture.add_file("test.txt", 100)?;

        // 2. Call open_file() and drop the returned blob
        {
            let mut blob = acache.open_file(inode)?;
            assert!(fixture.blob_entry_exists(inode)?);
            assert!(fixture.blob_file_exists(inode));

            // Write some data to create disk usage
            let test_data = b"Hello, world!";
            blob.write(test_data).await?;
            blob.update_db().await?;

            // Explicitly drop the blob to ensure it's closed
            drop(blob);
        }

        // 3. Call cleanup_cache(0) to delete everything
        acache.cleanup_cache(0)?;

        // Verify the blob was deleted
        assert!(!fixture.blob_entry_exists(inode)?);
        assert!(!fixture.blob_file_exists(inode));

        // 4. Call open_file() and make sure the returned blob can be written to normally
        let mut new_blob = acache.open_file(inode)?;

        // Verify the new blob exists and it has been added to the LRU queue.
        assert!(fixture.blob_entry_exists(inode)?);
        assert!(fixture.blob_file_exists(inode));
        assert_eq!(
            vec![inode],
            fixture.list_queue_content(LruQueueId::WorkingArea)?
        );

        // Test that we can write to the new blob normally
        let test_data = b"Hello, world!";
        let bytes_written = new_blob.write(test_data).await?;
        assert_eq!(test_data.len(), bytes_written);

        Ok(())
    }

    #[test]
    fn read_blobs() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.begin_read()?;
        let blobs = txn.read_blobs()?;

        // Just test that the read transaction works correctly.
        assert!(blobs.get_with_inode(Inode(999))?.is_none());

        Ok(())
    }

    #[test]
    fn write_blobs() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.begin_write()?;
        let mut blobs = txn.write_blobs()?;
        let mut tree = txn.write_tree()?;
        let mut mark = txn.write_marks()?;

        // Just test that the write transaction works correctly.
        let blob_info = blobs.create(&mut tree, &mut mark, Inode(10), &test_hash(), 100)?;
        assert_eq!(blob_info.hash, test_hash());
        assert_eq!(blob_info.size, 100);

        Ok(())
    }

    #[test]
    fn local_availability_missing_when_no_blob() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.begin_write()?;
        let blobs = txn.write_blobs()?;
        let tree = txn.read_tree()?;

        // Test with a path that doesn't exist in the tree
        let path = Path::parse("nonexistent.txt")?;
        let availability = blobs.local_availability(&tree, &path)?;
        assert_eq!(availability, LocalAvailability::Missing);

        // Test with an inode that doesn't exist
        let availability = blobs.local_availability(&tree, Inode(99999))?;
        assert_eq!(availability, LocalAvailability::Missing);

        Ok(())
    }

    #[test]
    fn local_availability_missing_when_blob_empty() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.begin_write()?;
        let mut blobs = txn.write_blobs()?;
        let mut tree = txn.write_tree()?;
        let marks = txn.read_marks()?;

        // Create a blob with empty written areas
        let path = Path::parse("empty.txt")?;
        let hash = test_hash();
        let blob_info = blobs.create(&mut tree, &marks, &path, &hash, 100)?;

        // Verify the blob was created with empty written areas
        assert!(blob_info.available_ranges.is_empty());

        // Test local availability - should be Missing for empty blob
        let availability = blobs.local_availability(&tree, &path)?;
        assert_eq!(availability, LocalAvailability::Missing);

        Ok(())
    }

    #[test]
    fn local_availability_partial_when_blob_incomplete() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.begin_write()?;
        let mut blobs = txn.write_blobs()?;
        let mut tree = txn.write_tree()?;
        let marks = txn.read_marks()?;

        // Create a blob
        let path = Path::parse("partial.txt")?;
        let hash = test_hash();
        blobs.create(&mut tree, &marks, &path, &hash, 1000)?;

        // Extend with partial ranges
        let partial_ranges =
            ByteRanges::from_ranges(vec![ByteRange::new(0, 100), ByteRange::new(200, 300)]);
        blobs.extend_local_availability(&tree, &path, &hash, &partial_ranges)?;

        // Test local availability - should be Partial
        let availability = blobs.local_availability(&tree, &path)?;
        match availability {
            LocalAvailability::Partial(size, ranges) => {
                assert_eq!(size, 1000);
                assert_eq!(ranges, partial_ranges);
            }
            _ => panic!("Expected Partial availability, got {:?}", availability),
        }

        Ok(())
    }

    #[test]
    fn local_availability_complete_when_blob_full_but_unverified() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.begin_write()?;
        let mut blobs = txn.write_blobs()?;
        let mut tree = txn.write_tree()?;
        let marks = txn.read_marks()?;

        // Create a blob
        let path = Path::parse("complete.txt")?;
        let hash = test_hash();
        blobs.create(&mut tree, &marks, &path, &hash, 500)?;

        // Extend with complete range (0 to size)
        let complete_range = ByteRanges::single(0, 500);
        blobs.extend_local_availability(&tree, &path, &hash, &complete_range)?;

        // Test local availability - should be Complete (not verified)
        let availability = blobs.local_availability(&tree, &path)?;
        assert_eq!(availability, LocalAvailability::Complete);

        Ok(())
    }

    #[test]
    fn local_availability_verified_when_blob_full_and_verified() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.begin_write()?;
        let mut blobs = txn.write_blobs()?;
        let mut tree = txn.write_tree()?;
        let marks = txn.read_marks()?;
        let mut dirty = txn.write_dirty()?;

        // Create a blob
        let path = Path::parse("verified.txt")?;
        let hash = test_hash();
        blobs.create(&mut tree, &marks, &path, &hash, 500)?;

        // Extend with complete range
        let complete_range = ByteRanges::single(0, 500);
        blobs.extend_local_availability(&tree, &path, &hash, &complete_range)?;

        // Mark as verified
        blobs.mark_verified(&tree, &mut dirty, &path, &hash)?;

        let availability = blobs.local_availability(&tree, &path)?;
        assert_eq!(availability, LocalAvailability::Verified);

        Ok(())
    }

    #[test]
    fn local_availability_handles_zero_size_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.begin_write()?;
        let mut blobs = txn.write_blobs()?;
        let mut tree = txn.write_tree()?;
        let marks = txn.read_marks()?;

        let path = Path::parse("zero.txt")?;
        let hash = test_hash();
        blobs.create(&mut tree, &marks, &path, &hash, 0)?;

        let availability = blobs.local_availability(&tree, &path)?;
        assert_eq!(availability, LocalAvailability::Complete);

        Ok(())
    }

    #[test]
    fn local_availability_handles_nonexistent_inode() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.begin_write()?;
        let blobs = txn.write_blobs()?;
        let tree = txn.read_tree()?;

        // Test with a non-existent inode
        let availability = blobs.local_availability(&tree, Inode(99999))?;
        assert_eq!(availability, LocalAvailability::Missing);

        Ok(())
    }
}
