use super::db::{ArenaDatabase, BeforeCommit};
use super::dirty::WritableOpenDirty;
use super::hasher::hash_file;
use super::mark::MarkReadOperations;
use super::tree::{TreeExt, TreeLoc, TreeReadOperations, WritableOpenTree};
use super::types::{BlobTableEntry, LocalAvailability, LruQueueId, Mark, QueueTableEntry};
use crate::StorageError;
use crate::types::Inode;
use crate::utils::holder::Holder;
use priority_queue::PriorityQueue;
use realize_types::Arena;
use realize_types::{ByteRange, ByteRanges, Hash};
use redb::{ReadableTable, Table};
use std::cmp::{Reverse, min};
use std::collections::HashMap;
use std::io::{SeekFrom, Write};
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt as _, ReadBuf};
use tokio::sync::{broadcast, watch};
use tokio_util::sync::CancellationToken;

/// Background job that marks blobs that are accessed after a cooldown delay.
///
/// Marking blobs accessed is best effort. It may fail or skip blobs. The process
/// shutting down might leave accessed blobs unmarked.
///
/// This loop should run as long as blobs can be used, to be able to track
/// access and keep the LRU queue in the right order. It terminates when `shutdown` is cancelled.
pub(crate) async fn mark_accessed_loop(
    db: Arc<ArenaDatabase>,
    cooldown: Duration,
    shutdown: CancellationToken,
) {
    let mut queue = PriorityQueue::new();
    let mut rx = db.blobs().subscribe_accessed();
    loop {
        tokio::select!(
            _ = shutdown.cancelled() => {
                return;
            }
            ret = rx.recv() => {
                let inode = match ret {
                    Ok(inode) => inode,
                    Err(broadcast::error::RecvError::Lagged(_))=> continue,
                    Err(broadcast::error::RecvError::Closed) => return
                };
                queue.push(inode, Reverse(Instant::now()));
            }
            _ = tokio::time::sleep(cooldown), if !queue.is_empty() => {
                let db = Arc::clone(&db);
                let mut queue = std::mem::take(&mut queue);
                // This is all best effort.
                let _ = tokio::task::spawn_blocking(move || {
                    let txn = db.begin_write()?;
                    {
                        let mut blobs = txn.write_blobs()?;
                        while let Some((inode, _)) = queue.pop() {
                            // If marking one inode fails, don't make
                            // other inodes fail.
                            let _ = blobs.mark_accessed(inode);
                        }
                    }
                    txn.commit()?;

                    Ok::<(), StorageError>(())
                }).await;
            }
        );
    }
}

pub(crate) struct Blobs {
    blob_dir: PathBuf,
    /// Reference counter for open blobs. Maps Inode to the number of open handles.
    open_blobs: OpenBlobRefCounter,

    disk_usage_tx: watch::Sender<DiskUsage>,

    /// Kept to not lose history when there are no receivers.
    _disk_usage_tx: watch::Receiver<DiskUsage>,

    /// Report blob accesses
    accessed_tx: broadcast::Sender<Inode>,
}
impl Blobs {
    pub(crate) fn new(
        blob_dir: &std::path::Path,
        blob_lru_queue_table: &impl redb::ReadableTable<u16, Holder<'static, QueueTableEntry>>,
    ) -> Result<Blobs, StorageError> {
        let (tx, rx) = watch::channel(disk_usage_op(blob_lru_queue_table)?);
        let (accessed_tx, _) = broadcast::channel(16);
        Ok(Self {
            blob_dir: blob_dir.to_path_buf(),
            open_blobs: OpenBlobRefCounter::new(),
            disk_usage_tx: tx,
            _disk_usage_tx: rx,
            accessed_tx,
        })
    }

    /// Get a watch channel that reports changes to disk usage.
    ///
    /// The current value is also available as
    /// [BlobReadOperations::disk_usage].
    #[allow(dead_code)] // will be used soon
    pub(crate) fn watch_disk_usage(&self) -> watch::Receiver<DiskUsage> {
        self.disk_usage_tx.subscribe()
    }

    /// Return a receiver that can receive report about blob accesses.
    fn subscribe_accessed(&self) -> broadcast::Receiver<Inode> {
        self.accessed_tx.subscribe()
    }

    /// Return total and available disk space on FS for the blob directory
    pub(crate) fn disk_space(&self) -> Result<(u64, u64), StorageError> {
        let stat = nix::sys::statvfs::statvfs(&self.blob_dir)?;
        let unit = stat.fragment_size() as u64;

        Ok((
            stat.blocks() as u64 * unit,
            stat.blocks_free() as u64 * unit,
        ))
    }
}

pub(crate) struct ReadableOpenBlob<T, TQ>
where
    T: ReadableTable<Inode, Holder<'static, BlobTableEntry>>,
    TQ: ReadableTable<u16, Holder<'static, QueueTableEntry>>,
{
    blob_table: T,
    #[allow(dead_code)]
    blob_lru_queue_table: TQ,
}

impl<T, TQ> ReadableOpenBlob<T, TQ>
where
    T: ReadableTable<Inode, Holder<'static, BlobTableEntry>>,
    TQ: ReadableTable<u16, Holder<'static, QueueTableEntry>>,
{
    pub(crate) fn new(blob_table: T, blob_lru_queue_table: TQ) -> Self {
        Self {
            blob_table,
            blob_lru_queue_table,
        }
    }
}

pub(crate) struct WritableOpenBlob<'a> {
    before_commit: &'a BeforeCommit,

    blob_table: Table<'a, Inode, Holder<'static, BlobTableEntry>>,
    blob_lru_queue_table: Table<'a, u16, Holder<'static, QueueTableEntry>>,
    subsystem: &'a Blobs,
    arena: Arena,

    /// If true, an after-commit has been registered to report disk usage at
    /// the end of the transaction.
    will_report_disk_usage: bool,
}

impl<'a> WritableOpenBlob<'a> {
    pub(crate) fn new(
        before_commit: &'a BeforeCommit,
        blob_table: Table<'a, Inode, Holder<'static, BlobTableEntry>>,
        blob_lru_queue_table: Table<'a, u16, Holder<'static, QueueTableEntry>>,
        subsystem: &'a Blobs,
        arena: Arena,
    ) -> Self {
        Self {
            before_commit,
            blob_table,
            blob_lru_queue_table,
            subsystem,
            arena,
            will_report_disk_usage: false,
        }
    }

    /// Report disk usage changed to the watch channel after commit.
    fn report_disk_usage_changed(&mut self) {
        if self.will_report_disk_usage {
            return;
        }
        let tx = self.subsystem.disk_usage_tx.clone();
        self.before_commit.add(move |txn| {
            if let Ok(disk_usage) = txn.read_blobs()?.disk_usage() {
                txn.after_commit(move || {
                    let _ = tx.send(disk_usage);
                });
            }

            Ok::<(), StorageError>(())
        });
        self.will_report_disk_usage = true;
    }
}

/// Information about current disk usage
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DiskUsage {
    /// Total disk used to store local blobs, in bytes.
    pub(crate) total: u64,

    /// Subset of the total that can be evicted, in bytes.
    pub(crate) evictable: u64,
}

impl DiskUsage {
    #[allow(dead_code)]
    pub(crate) const ZERO: DiskUsage = DiskUsage {
        total: 0,
        evictable: 0,
    };

    // Arbitrary disk space considered to be occupied by an inode.
    //
    // This value is added to the disk usage estimates. It is meant to
    // account for the disk space used by the inode itself, so it doesn't
    // look like empty files are free. This is arbitrary and might not
    // correspond to the actual value, as it depends on the filesystem.
    pub(crate) const INODE: u64 = 512;

    // How many bytes cannot be cleaned up by evicting blobs.
    pub(crate) fn non_evictable(&self) -> u64 {
        self.total.saturating_sub(self.evictable)
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

    pub(crate) fn local_availability(&self) -> LocalAvailability {
        let file_range = ByteRanges::single(0, self.size);
        if file_range == file_range.intersection(&self.available_ranges) {
            if self.verified {
                return LocalAvailability::Verified;
            }
            return LocalAvailability::Complete;
        }
        if self.available_ranges.is_empty() {
            return LocalAvailability::Missing;
        }

        LocalAvailability::Partial(self.size, self.available_ranges.clone())
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

    /// Returns a double-ended iterator over the given queue, starting at the head.
    #[allow(dead_code)] // TODO: make it test-only
    fn head(&self, queue: LruQueueId) -> impl Iterator<Item = Result<Inode, StorageError>>;

    /// Returns a double-ended iterator over the given queue, starting at the tail.
    #[allow(dead_code)] // TODO: make it test-only
    fn tail(&self, queue: LruQueueId) -> impl Iterator<Item = Result<Inode, StorageError>>;

    /// Disk space used for storing local copies.
    ///
    /// Returns (total, evictable) with total the total disk usage, in
    /// bytes and evictable the portion of total that
    /// [WritableOpenBlob::cleanup] can delete.
    #[allow(dead_code)]
    fn disk_usage(&self) -> Result<DiskUsage, StorageError>;
}

impl<T, TQ> BlobReadOperations for ReadableOpenBlob<T, TQ>
where
    T: ReadableTable<Inode, Holder<'static, BlobTableEntry>>,
    TQ: ReadableTable<u16, Holder<'static, QueueTableEntry>>,
{
    fn get_with_inode(&self, inode: Inode) -> Result<Option<BlobInfo>, StorageError> {
        get_read_op(&self.blob_table, inode)
    }

    fn head(&self, queue: LruQueueId) -> impl Iterator<Item = Result<Inode, StorageError>> {
        QueueIterator::head(&self.blob_table, &self.blob_lru_queue_table, queue)
    }

    fn tail(&self, queue: LruQueueId) -> impl Iterator<Item = Result<Inode, StorageError>> {
        QueueIterator::tail(&self.blob_table, &self.blob_lru_queue_table, queue)
    }

    fn disk_usage(&self) -> Result<DiskUsage, StorageError> {
        disk_usage_op(&self.blob_lru_queue_table)
    }
}

impl<'a> BlobReadOperations for WritableOpenBlob<'a> {
    fn get_with_inode(&self, inode: Inode) -> Result<Option<BlobInfo>, StorageError> {
        get_read_op(&self.blob_table, inode)
    }

    fn head(&self, queue: LruQueueId) -> impl Iterator<Item = Result<Inode, StorageError>> {
        QueueIterator::head(&self.blob_table, &self.blob_lru_queue_table, queue)
    }

    fn tail(&self, queue: LruQueueId) -> impl Iterator<Item = Result<Inode, StorageError>> {
        QueueIterator::tail(&self.blob_table, &self.blob_lru_queue_table, queue)
    }

    fn disk_usage(&self) -> Result<DiskUsage, StorageError> {
        disk_usage_op(&self.blob_lru_queue_table)
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
            Ok(blob_info.local_availability())
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
        self.report_disk_usage_changed();

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
            self.remove_from_queue(&e)?;
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
            disk_usage: calculate_disk_usage(&blob_path.metadata()?),
        };
        self.add_to_queue_front(queue, inode, &mut entry)?;

        log::debug!(
            "[{arena}] Created blob {inode} {hash} in {queue:?} at {blob_path:?} -> {entry:?}",
            arena = self.arena
        );
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

        if !self.remove_blob_entry(tree, inode)? {
            return Ok(());
        }
        let blob_path = self.subsystem.blob_dir.join(inode.hex());
        if blob_path.exists() {
            std::fs::remove_file(&blob_path)?;
        }
        self.report_disk_usage_changed();

        Ok(())
    }

    /// Move the blob into or out of the protected queue.
    ///
    /// Does nothing if the blob doesn't exist.
    pub(crate) fn set_protected<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &impl TreeReadOperations,
        dirty: &mut WritableOpenDirty,
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
        self.remove_from_queue(&blob_entry)?;

        // Add to new queue
        self.add_to_queue_front(new_queue, inode, &mut blob_entry)?;

        // Update the entry in the table
        self.blob_table
            .insert(inode, Holder::with_content(blob_entry)?)?;
        dirty.mark_dirty(inode, "set_protected")?;
        self.report_disk_usage_changed();

        Ok(())
    }

    /// Prepare the move the blob file out of the blob store.
    ///
    /// Does nothing and return None unless the blob content hash is
    /// `hash` and has been downloaded and verified.
    ///
    /// Upon success, return the path of the blob file. That file
    /// should be moved; The blob entry has been deleted already.
    pub(crate) fn prepare_export<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        loc: L,
        hash: &Hash,
    ) -> Result<Option<std::path::PathBuf>, StorageError> {
        let inode = match tree.resolve(loc)? {
            Some(inode) => inode,
            None => return Ok(None), // Nothing to export
        };

        let blob_entry = match self.blob_table.get(inode)? {
            None => {
                return Ok(None);
            }
            Some(v) => v.value().parse()?,
        };
        if blob_entry.content_hash != *hash || !blob_entry.verified {
            return Ok(None);
        }

        self.remove_blob_entry(tree, inode)?;
        self.report_disk_usage_changed();
        Ok(Some(self.subsystem.blob_dir.join(inode.hex())))
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
        self.update_disk_usage(&mut entry, metadata)?;
        self.blob_table
            .insert(inode, Holder::with_content(entry)?)?;
        self.report_disk_usage_changed();

        // Return the path where the file should be moved
        Ok(blob_path)
    }

    /// Mark the blob as accessed.
    ///
    /// If the blob is in a LRU queue other than protected, it is put
    /// at the front of its queue.
    ///
    /// Does nothing if the blob doesn't exist.
    fn mark_accessed(&mut self, inode: Inode) -> Result<(), StorageError> {
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
        self.move_to_front(inode, &mut blob_entry)?;

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
            log::debug!(
                "[{arena}] {inode} content verified to be {hash}",
                arena = self.arena
            );

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

        // Update disk usage if the blob file exists
        let blob_path = self.subsystem.blob_dir.join(inode.hex());
        if let Ok(metadata) = blob_path.metadata() {
            self.update_disk_usage(&mut blob_entry, &metadata)?;
            self.report_disk_usage_changed();
        }

        self.blob_table
            .insert(inode, Holder::with_content(blob_entry)?)?;

        Ok(true)
    }

    /// Delete blobs as necessary to reach the target total size, in bytes.
    ///
    /// Returns the final disk usage.
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
            "[{arena}] Cleaning up of WorkingArea with disk usage: {}, target: {}",
            queue.disk_usage,
            target,
            arena = self.arena
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
                "[{arena}] Evicted blob from WorkingArea: {current_id} ({} bytes)",
                current.disk_usage,
                arena = self.arena
            );
            self.remove_from_queue_update_entry(&current, &mut queue)?;
            removed_count += 1;
            tree.remove_and_decref(current_id, &mut self.blob_table, current_id)?;

            // Remove the blob file
            let blob_path = self.subsystem.blob_dir.join(current_id.hex());
            if blob_path.exists() {
                std::fs::remove_file(&blob_path)?;
            }
        }

        log::debug!(
            "[{arena}] Evicted {removed_count} entries from WorkingArea disk usage now {} (target: {target})",
            queue.disk_usage,
            arena = self.arena
        );

        if removed_count > 0 {
            self.blob_lru_queue_table
                .insert(LruQueueId::WorkingArea as u16, Holder::new(&queue)?)?;
            self.report_disk_usage_changed();
        }

        Ok(())
    }

    /// Add a blob to the front of the WorkingArea queue.
    fn add_to_queue_front(
        &mut self,
        queue_id: LruQueueId,
        inode: Inode,
        blob_entry: &mut BlobTableEntry,
    ) -> Result<(), StorageError> {
        let mut queue =
            get_queue_if_available(&self.blob_lru_queue_table, queue_id)?.unwrap_or_default();
        self.add_to_queue_front_update_entry(queue_id, inode, blob_entry, &mut queue)?;
        self.blob_lru_queue_table
            .insert(queue_id as u16, Holder::with_content(queue)?)?;

        Ok(())
    }

    fn add_to_queue_front_update_entry(
        &mut self,
        queue_id: LruQueueId,
        inode: Inode,
        blob_entry: &mut BlobTableEntry,
        queue: &mut QueueTableEntry,
    ) -> Result<(), StorageError> {
        blob_entry.queue = queue_id;
        if let Some(head_id) = queue.head {
            let mut head = follow_queue_link(&self.blob_table, head_id)?;
            head.prev = Some(inode);

            self.blob_table
                .insert(head_id, Holder::with_content(head)?)?;
        } else {
            // This is the first node in the queue, so both the head
            // and the tail
            queue.tail = Some(inode);
        }
        blob_entry.next = queue.head;
        queue.head = Some(inode);
        blob_entry.prev = None;
        queue.disk_usage += blob_entry.disk_usage;

        Ok(())
    }

    /// Remove a blob from its queue.
    fn remove_from_queue(&mut self, blob_entry: &BlobTableEntry) -> Result<(), StorageError> {
        let queue_id = blob_entry.queue;

        // Get the queue entry
        let mut queue = get_queue_must_exist(&self.blob_lru_queue_table, queue_id)?;
        self.remove_from_queue_update_entry(blob_entry, &mut queue)?;
        self.blob_lru_queue_table
            .insert(queue_id as u16, Holder::with_content(queue)?)?;

        Ok(())
    }

    fn remove_from_queue_update_entry(
        &mut self,
        blob_entry: &BlobTableEntry,
        queue: &mut QueueTableEntry,
    ) -> Result<(), StorageError> {
        if let Some(prev_id) = blob_entry.prev {
            let mut prev = follow_queue_link(&self.blob_table, prev_id)?;
            prev.next = blob_entry.next;
            self.blob_table
                .insert(prev_id, Holder::with_content(prev)?)?;
        } else {
            // This was the first node
            queue.head = blob_entry.next;
        }
        if let Some(next_id) = blob_entry.next {
            let mut next = follow_queue_link(&self.blob_table, next_id)?;
            next.prev = blob_entry.prev;
            self.blob_table
                .insert(next_id, Holder::with_content(next)?)?;
        } else {
            // This was the last node
            queue.tail = blob_entry.prev;
        }
        queue.disk_usage = queue.disk_usage.saturating_sub(blob_entry.disk_usage);
        Ok(())
    }
    /// Update disk usage in the given blob entry and the total in its
    /// containing table.
    fn update_disk_usage(
        &mut self,
        entry: &mut BlobTableEntry,
        metadata: &std::fs::Metadata,
    ) -> Result<(), StorageError> {
        let disk_usage = calculate_disk_usage(metadata);
        let disk_usage_diff = disk_usage as i64 - (entry.disk_usage as i64);
        if disk_usage_diff == 0 {
            return Ok(());
        }
        entry.disk_usage = disk_usage;
        self.update_total_disk_usage(entry.queue, disk_usage_diff)?;

        Ok(())
    }

    fn update_total_disk_usage(
        &mut self,
        queue_id: LruQueueId,
        diff: i64,
    ) -> Result<(), StorageError> {
        let mut entry = get_queue_must_exist(&self.blob_lru_queue_table, queue_id)?;
        entry.disk_usage = entry.disk_usage.saturating_add_signed(diff);
        self.blob_lru_queue_table
            .insert(queue_id as u16, Holder::with_content(entry)?)?;
        Ok(())
    }

    fn remove_blob_entry(
        &mut self,
        tree: &mut WritableOpenTree<'_>,
        inode: Inode,
    ) -> Result<bool, StorageError> {
        if let Some(entry) = get_blob_entry(&self.blob_table, inode)? {
            self.remove_from_queue(&entry)?;
            tree.remove_and_decref(inode, &mut self.blob_table, inode)?;
            return Ok(true);
        }

        Ok(false)
    }

    /// Move the blob to the front of its queue
    fn move_to_front(
        &mut self,
        inode: Inode,
        blob_entry: &mut BlobTableEntry,
    ) -> Result<(), StorageError> {
        if blob_entry.prev.is_none() {
            // Already at the head of its queue
            return Ok(());
        }
        let queue_id = blob_entry.queue;
        let mut queue = get_queue_must_exist(&self.blob_lru_queue_table, queue_id)?;
        self.remove_from_queue_update_entry(blob_entry, &mut queue)?;
        self.add_to_queue_front_update_entry(queue_id, inode, blob_entry, &mut queue)?;
        self.blob_lru_queue_table
            .insert(queue_id as u16, Holder::with_content(queue)?)?;
        Ok(())
    }
}

#[allow(dead_code)]
pub(crate) struct QueueIterator<'a, T>
where
    T: redb::ReadableTable<Inode, Holder<'static, BlobTableEntry>>,
{
    blob_table: &'a T,
    err: Option<StorageError>,
    next: Option<Inode>,
    next_fn: fn(BlobTableEntry) -> Option<Inode>,
}

#[allow(dead_code)]
impl<'a, T> QueueIterator<'a, T>
where
    T: redb::ReadableTable<Inode, Holder<'static, BlobTableEntry>>,
{
    fn head(
        blob_table: &'a T,
        blob_lru_queue_table: &'_ impl redb::ReadableTable<u16, Holder<'static, QueueTableEntry>>,
        queue: LruQueueId,
    ) -> Self {
        let (err, next) = match get_queue_if_available(blob_lru_queue_table, queue) {
            Err(e) => (Some(e), None),
            Ok(None) => (None, None),
            Ok(Some(e)) => (None, e.head),
        };

        Self {
            blob_table,
            err,
            next,
            next_fn: |e: BlobTableEntry| e.next,
        }
    }
    fn tail(
        blob_table: &'a T,
        blob_lru_queue_table: &'_ impl redb::ReadableTable<u16, Holder<'static, QueueTableEntry>>,
        queue: LruQueueId,
    ) -> Self {
        let (err, next) = match get_queue_if_available(blob_lru_queue_table, queue) {
            Err(e) => (Some(e), None),
            Ok(None) => (None, None),
            Ok(Some(e)) => (None, e.tail),
        };
        Self {
            blob_table,
            err,
            next,
            next_fn: |e: BlobTableEntry| e.prev,
        }
    }
}

impl<'a, T> Iterator for QueueIterator<'a, T>
where
    T: redb::ReadableTable<Inode, Holder<'static, BlobTableEntry>>,
{
    type Item = Result<Inode, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(err) = self.err.take() {
            return Some(Err(err));
        }
        match self.next.take() {
            None => None,
            Some(inode) => {
                {
                    // return current and move to next
                    match follow_queue_link(self.blob_table, inode) {
                        Err(err) => {
                            self.err = Some(err);
                            self.next = None;
                        }
                        Ok(e) => {
                            self.next = (self.next_fn)(e);
                        }
                    };
                };
                Some(Ok(inode))
            }
        }
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

/// Calculate disk usage for a blob file using Unix metadata.
fn calculate_disk_usage(metadata: &std::fs::Metadata) -> u64 {
    // block() takes into account actual usage, not the whole file
    // size, which might be sparse. Include the inode size into the
    // computation.
    metadata.blocks() * 512 + DiskUsage::INODE
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
    info: BlobInfo,
    file: tokio::fs::File,
    db: Arc<ArenaDatabase>,

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

        let blobs = db.blobs();
        blobs.open_blobs.increment(info.inode);
        let _ = blobs.accessed_tx.send(info.inode);

        Ok(Self {
            info,
            file: tokio::fs::File::from_std(file),
            db: Arc::clone(db),
            pending_ranges: ByteRanges::new(),
            offset: 0,
        })
    }

    /// Get the size of the corresponding file.
    pub fn size(&self) -> u64 {
        self.info.size
    }

    /// Return the blob inode.
    pub fn inode(&self) -> Inode {
        self.info.inode
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
    pub fn available_range(&self) -> &ByteRanges {
        &self.info.available_ranges
    }

    /// Return local availability for the blob.
    ///
    /// This is based on the [Blob::available_range] so might return a
    /// different result than if you checked the database.
    pub fn local_availability(&self) -> LocalAvailability {
        self.info.local_availability()
    }

    /// Get the hash of the corresponding file.
    pub fn hash(&self) -> &Hash {
        &self.info.hash
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

        self.info.verified = true;
        let inode = self.info.inode;
        let db = self.db.clone();
        let hash = self.info.hash.clone();
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

        let inode = self.info.inode;
        let ranges = self.pending_ranges.clone();
        let db = Arc::clone(&self.db);
        let hash = self.info.hash.clone();

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
        self.info
            .available_ranges
            .containing_range(offset)
            .map(|r| min(requested_len, (r.end - offset) as usize))
    }
}

impl Drop for Blob {
    fn drop(&mut self) {
        let blobs = self.db.blobs();
        if blobs.open_blobs.decrement(self.info.inode) {
            // poke disk usage without modifying it so the cleaner
            // attempts to evict files it couldn't previously evict
            // because they were open.
            blobs.disk_usage_tx.send_modify(|_| {});
        }
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
                self.info.available_ranges.add(&range);
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

    fn increment(&self, inode: Inode) {
        let mut guard = self.counts.lock().unwrap();
        *guard.entry(inode).or_insert(0) += 1;
        log::trace!("Blob {} opened, ref count: {}", inode, guard[&inode]);
    }

    /// Decrement refcount for the given inode.
    ///
    /// Returns true once all counters have reached 0.
    fn decrement(&self, inode: Inode) -> bool {
        let mut guard = self.counts.lock().unwrap();
        if let Some(count) = guard.get_mut(&inode) {
            *count = count.saturating_sub(1);
            log::trace!("Blob {} closed, ref count: {}", inode, *count);
            if *count == 0 {
                guard.remove(&inode);
                log::trace!("Blob {} removed from open blobs", inode);
                return guard.is_empty();
            }
        }

        false
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

#[allow(dead_code)]
fn disk_usage_op(
    blob_lru_queue_table: &impl redb::ReadableTable<u16, Holder<'static, QueueTableEntry>>,
) -> Result<DiskUsage, StorageError> {
    let mut total: u64 = 0;
    let mut evictable: u64 = 0;
    for e in blob_lru_queue_table.iter()? {
        let (k, v) = e?;
        let disk_usage = v.value().parse()?.disk_usage;
        total += disk_usage;
        if k.value() == LruQueueId::WorkingArea as u16 {
            evictable += disk_usage;
        }
    }

    Ok(DiskUsage { total, evictable })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arena::db::{ArenaReadTransaction, ArenaWriteTransaction};
    use crate::arena::dirty::DirtyReadOperations;
    use crate::utils::hash;
    use crate::{Inode, Mark};
    use assert_fs::TempDir;
    use assert_fs::fixture::ChildPath;
    use assert_fs::prelude::*;
    use realize_types::{Arena, Path};
    use std::io::SeekFrom;
    use std::os::unix::fs::MetadataExt;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

    fn test_arena() -> Arena {
        Arena::from("test_arena")
    }

    fn test_hash() -> Hash {
        Hash([1u8; 32])
    }

    struct Fixture {
        arena: Arena,
        db: Arc<ArenaDatabase>,
        blob_dir: ChildPath,
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

            Ok(Self {
                arena,
                db,
                blob_dir,
                tempdir,
            })
        }

        /// Return the path to a blob file for test use.
        fn blob_path(&self, inode: Inode) -> std::path::PathBuf {
            self.tempdir
                .child(format!("{}/blobs/{}", self.arena, inode.hex()))
                .to_path_buf()
        }

        fn begin_read(&self) -> anyhow::Result<ArenaReadTransaction<'_>> {
            Ok(self.db.begin_read()?)
        }

        fn begin_write(&self) -> anyhow::Result<ArenaWriteTransaction<'_>> {
            Ok(self.db.begin_write()?)
        }

        fn get_blob_entry(&self, inode: Inode) -> anyhow::Result<BlobTableEntry> {
            let txn = self.begin_write()?;
            let blobs = txn.write_blobs()?;
            Ok(get_blob_entry(&blobs.blob_table, inode)?.ok_or(StorageError::NotFound)?)
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
        {
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
        }
        let watch = fixture.db.blobs().watch_disk_usage();
        txn.commit()?;
        assert!(watch.has_changed()?);

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

    #[test]
    fn delete_blob() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("blob/test.txt")?;
        let txn = fixture.begin_write()?;
        {
            let mut blobs = txn.write_blobs()?;
            let marks = txn.write_marks()?;
            let mut tree = txn.write_tree()?;

            blobs
                .create(&mut tree, &marks, &path, &hash::digest("test"), 4)?
                .inode;
        }
        txn.commit()?;

        let txn = fixture.begin_write()?;
        {
            let mut blobs = txn.write_blobs()?;
            let mut tree = txn.write_tree()?;

            let inode = tree.expect(&path)?;
            blobs.delete(&mut tree, &path)?;

            assert_eq!(None, blobs.get_with_inode(inode)?);

            let file_path = fixture.blob_path(inode);
            assert!(!file_path.exists());
        }
        let watch = fixture.db.blobs().watch_disk_usage();
        txn.commit()?;
        assert!(watch.has_changed()?);

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
        assert_eq!(ByteRanges::single(0, 8), *blob.available_range());

        // At this point, local availability is only in the blob, not
        // yet stored on the database.
        assert_eq!(
            ByteRanges::new(),
            fixture.blob_info(&path).unwrap().unwrap().available_ranges
        );

        blob.write(b", black sheep").await?;
        assert_eq!(ByteRanges::single(0, 21), *blob.available_range());

        let watch = fixture.db.blobs().watch_disk_usage();
        blob.update_db().await?;
        drop(blob);
        assert!(watch.has_changed()?);

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
        assert_eq!(ByteRanges::single(0, 21), *blob.available_range());
        assert_eq!(
            ByteRanges::new(),
            fixture.blob_info(&path)?.unwrap().available_ranges
        );

        assert_eq!(false, blob.mark_verified().await?);
        assert_eq!(ByteRanges::single(0, 21), *blob.available_range());
        assert_eq!(
            ByteRanges::new(),
            fixture.blob_info(&path)?.unwrap().available_ranges
        );

        Ok(())
    }

    #[tokio::test]
    async fn mark_verified() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("baa/baa")?;
        fixture.create_blob_with_partial_data(&path, "Baa, baa, black sheep", 21)?;

        assert_eq!(false, fixture.blob_info(&path)?.unwrap().verified);
        let mut blob = Blob::open(&fixture.db, &path)?;
        assert_eq!(ByteRanges::single(0, 21), *blob.available_range());
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
            let source = blobs.prepare_export(&mut tree, &path, &hash)?.unwrap();
            std::fs::rename(source, &dest)?;
        }
        let watch = fixture.db.blobs().watch_disk_usage();
        txn.commit()?;
        assert!(watch.has_changed()?);

        assert_eq!("Baa, baa, black sheep", std::fs::read_to_string(&dest)?);

        let txn = fixture.begin_read()?;
        let blobs = txn.read_blobs()?;
        assert!(blobs.get_with_inode(inode)?.is_none());

        // The LRU queue must have been updated properly.
        assert!(blobs.head(LruQueueId::WorkingArea).next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn export_wrong_hash() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("baa/baa")?;
        fixture.create_blob_with_partial_data(&path, "Baa, baa, black sheep", 21)?;

        assert_eq!(true, Blob::open(&fixture.db, &path)?.mark_verified().await?);

        // export to dest
        let txn = fixture.begin_write()?;
        {
            let mut blobs = txn.write_blobs()?;
            let mut tree = txn.write_tree()?;
            assert_eq!(
                None,
                blobs.prepare_export(&mut tree, &path, &hash::digest("wrong!"))?
            );
        }
        txn.commit()?;

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

        let txn = fixture.begin_write()?;
        {
            let mut blobs = txn.write_blobs()?;
            let mut tree = txn.write_tree()?;
            assert_eq!(None, blobs.prepare_export(&mut tree, &path, &hash)?);
        }

        Ok(())
    }

    #[tokio::test]
    async fn export_no_blob() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("baa/baa")?;

        let txn = fixture.begin_write()?;
        {
            let mut blobs = txn.write_blobs()?;
            let mut tree = txn.write_tree()?;
            assert_eq!(
                None,
                blobs.prepare_export(&mut tree, &path, &hash::digest("test"))?
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn import_new() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("test.txt")?;

        let data = "When you light a candle, you also cast a shadow.";
        let tmpfile = fixture.tempdir.child("tmp");
        tmpfile.write_str(data)?;

        let txn = fixture.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let mut blobs = txn.write_blobs()?;
            let marks = txn.read_marks()?;
            let path_in_cache = blobs.import(
                &mut tree,
                &marks,
                &path,
                &hash::digest(data),
                &tmpfile.metadata()?,
            )?;
            std::fs::rename(tmpfile.path(), path_in_cache)?;
        }
        let watch = fixture.db.blobs().watch_disk_usage();
        txn.commit()?;
        assert!(watch.has_changed()?);

        let mut blob = Blob::open(&fixture.db, &path)?;
        assert_eq!(hash::digest(data), *blob.hash());
        assert_eq!(data.len() as u64, blob.size());
        assert_eq!(
            ByteRanges::single(0, data.len() as u64),
            *blob.available_range()
        );
        let mut buf = String::new();
        blob.read_to_string(&mut buf).await?;
        assert_eq!(
            "When you light a candle, you also cast a shadow.".to_string(),
            buf
        );

        let BlobInfo {
            inode, verified, ..
        } = fixture.blob_info(&path)?.unwrap();
        assert_eq!(false, verified);

        // Disk usage must be set.
        let blob_entry = fixture.get_blob_entry(inode)?;
        assert!(blob_entry.disk_usage > DiskUsage::INODE);

        Ok(())
    }

    #[tokio::test]
    async fn import_replaces_partial() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("test.txt")?;

        let data = "When you light a candle, you also cast a shadow.";
        fixture.create_blob_with_partial_data(&path, data, 10)?;

        let tmpfile = fixture.tempdir.child("tmp");
        tmpfile.write_str(data)?;

        let txn = fixture.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let mut blobs = txn.write_blobs()?;
            let marks = txn.read_marks()?;
            let path_in_cache = blobs.import(
                &mut tree,
                &marks,
                &path,
                &hash::digest(data),
                &tmpfile.metadata()?,
            )?;
            std::fs::rename(tmpfile.path(), path_in_cache)?;
        }
        txn.commit()?;

        let mut blob = Blob::open(&fixture.db, &path)?;
        assert_eq!(hash::digest(data), *blob.hash());
        assert_eq!(data.len() as u64, blob.size());
        assert_eq!(
            ByteRanges::single(0, data.len() as u64),
            *blob.available_range()
        );
        let mut buf = String::new();
        blob.read_to_string(&mut buf).await?;
        assert_eq!(
            "When you light a candle, you also cast a shadow.".to_string(),
            buf
        );

        let BlobInfo {
            inode, verified, ..
        } = fixture.blob_info(&path)?.unwrap();
        assert_eq!(false, verified);

        // Disk usage must be set.
        let blob_entry = fixture.get_blob_entry(inode)?;
        assert!(blob_entry.disk_usage > DiskUsage::INODE);

        Ok(())
    }

    #[tokio::test]
    async fn import_protected() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("test.txt")?;

        let tmpfile = fixture.tempdir.child("tmp");
        tmpfile.write_str("data")?;

        let txn = fixture.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut marks = txn.write_marks()?;
        let mut dirty = txn.write_dirty()?;
        marks.set(&mut tree, &mut dirty, &path, Mark::Keep)?;
        let path_in_cache = blobs.import(
            &mut tree,
            &marks,
            &path,
            &hash::digest("data"),
            &tmpfile.metadata()?,
        )?;

        let info = blobs.get(&tree, &path)?.unwrap();
        assert_eq!(true, info.protected);
        std::fs::rename(tmpfile.path(), path_in_cache)?;

        Ok(())
    }

    #[tokio::test]
    async fn create_adds_to_queue() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.begin_write()?;
        let marks = txn.read_marks()?;
        let mut blobs = txn.write_blobs()?;
        let mut tree = txn.write_tree()?;

        assert!(blobs.head(LruQueueId::WorkingArea).next().is_none());
        assert!(blobs.tail(LruQueueId::WorkingArea).next().is_none());

        let one = blobs
            .create(
                &mut tree,
                &marks,
                Path::parse("one")?,
                &hash::digest("one"),
                3,
            )?
            .inode;

        assert_eq!(
            vec![one],
            blobs
                .head(LruQueueId::WorkingArea)
                .collect::<Result<Vec<_>, StorageError>>()?
        );
        assert_eq!(
            vec![one],
            blobs
                .tail(LruQueueId::WorkingArea)
                .collect::<Result<Vec<_>, StorageError>>()?
        );

        let two = blobs
            .create(
                &mut tree,
                &marks,
                Path::parse("two")?,
                &hash::digest("two"),
                3,
            )?
            .inode;

        assert_eq!(
            vec![two, one],
            blobs
                .head(LruQueueId::WorkingArea)
                .collect::<Result<Vec<_>, StorageError>>()?
        );
        assert_eq!(
            vec![one, two],
            blobs
                .tail(LruQueueId::WorkingArea)
                .collect::<Result<Vec<_>, StorageError>>()?
        );

        Ok(())
    }

    #[tokio::test]
    async fn delete_removes_from_queue() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.begin_write()?;
        let marks = txn.read_marks()?;
        let mut blobs = txn.write_blobs()?;
        let mut tree = txn.write_tree()?;

        let one = blobs
            .create(
                &mut tree,
                &marks,
                Path::parse("one")?,
                &hash::digest("one"),
                3,
            )?
            .inode;

        let two = blobs
            .create(
                &mut tree,
                &marks,
                Path::parse("two")?,
                &hash::digest("two"),
                3,
            )?
            .inode;
        let three = blobs
            .create(
                &mut tree,
                &marks,
                Path::parse("three")?,
                &hash::digest("three"),
                3,
            )?
            .inode;

        assert_eq!(
            vec![three, two, one],
            blobs
                .head(LruQueueId::WorkingArea)
                .collect::<Result<Vec<_>, StorageError>>()?
        );

        blobs.delete(&mut tree, two)?;

        assert_eq!(
            vec![three, one],
            blobs
                .head(LruQueueId::WorkingArea)
                .collect::<Result<Vec<_>, StorageError>>()?
        );

        blobs.delete(&mut tree, one)?;
        blobs.delete(&mut tree, three)?;

        assert!(blobs.head(LruQueueId::WorkingArea).next().is_none());
        assert!(blobs.tail(LruQueueId::WorkingArea).next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn mark_accessed() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let txn = fixture.begin_write()?;
        {
            let marks = txn.read_marks()?;
            let mut blobs = txn.write_blobs()?;
            let mut tree = txn.write_tree()?;

            let one = blobs
                .create(
                    &mut tree,
                    &marks,
                    Path::parse("one")?,
                    &hash::digest("one"),
                    3,
                )?
                .inode;

            let two = blobs
                .create(
                    &mut tree,
                    &marks,
                    Path::parse("two")?,
                    &hash::digest("two"),
                    3,
                )?
                .inode;
            let three = blobs
                .create(
                    &mut tree,
                    &marks,
                    Path::parse("three")?,
                    &hash::digest("three"),
                    3,
                )?
                .inode;

            assert_eq!(
                vec![three, two, one],
                blobs
                    .head(LruQueueId::WorkingArea)
                    .collect::<Result<Vec<_>, StorageError>>()?
            );
        }
        txn.commit()?;

        let txn = fixture.begin_write()?;
        {
            let mut blobs = txn.write_blobs()?;
            let tree = txn.read_tree()?;

            let two_inode = tree.resolve(Path::parse("two")?)?.unwrap();
            let three_inode = tree.resolve(Path::parse("three")?)?.unwrap();
            let one_inode = tree.resolve(Path::parse("one")?)?.unwrap();

            blobs.mark_accessed(two_inode)?;

            assert_eq!(
                vec![two_inode, three_inode, one_inode],
                blobs
                    .head(LruQueueId::WorkingArea)
                    .collect::<Result<Vec<_>, StorageError>>()?
            );

            blobs.mark_accessed(one_inode)?;

            assert_eq!(
                vec![one_inode, two_inode, three_inode],
                blobs
                    .head(LruQueueId::WorkingArea)
                    .collect::<Result<Vec<_>, StorageError>>()?
            );

            // make sure the list is correct both ways
            assert_eq!(
                vec![three_inode, two_inode, one_inode],
                blobs
                    .tail(LruQueueId::WorkingArea)
                    .collect::<Result<Vec<_>, StorageError>>()?
            );
        }
        let watch = fixture.db.blobs().watch_disk_usage();
        txn.commit()?;
        // mark_accessed should not change disk usage
        assert!(!watch.has_changed()?);

        Ok(())
    }

    #[tokio::test]
    async fn disk_usage_tracking() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let protected1 = Path::parse("protected/1")?;
        let protected2 = Path::parse("protected/2")?;
        let evictable1 = Path::parse("evictable/1")?;
        let evictable2 = Path::parse("evictable/2")?;
        fixture.blob_dir.create_dir_all()?;
        let blksize = fixture.blob_dir.metadata()?.blksize() as u64;
        // Using 64M files on macos apparently won't
        // remain sparse after data is written to them
        // below a certain size.
        let filesize = 64 * 1024 * 1024;

        let txn = fixture.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let mut blobs = txn.write_blobs()?;
            let mut marks = txn.write_marks()?;
            let mut dirty = txn.write_dirty()?;
            marks.set(&mut tree, &mut dirty, Path::parse("protected")?, Mark::Keep)?;
            assert_eq!(DiskUsage::ZERO, blobs.disk_usage()?);

            for path in [&protected1, &protected2, &evictable1, &evictable2] {
                blobs.create(&mut tree, &marks, path, &test_hash(), filesize)?;
            }
            assert_eq!(
                DiskUsage {
                    total: 4 * DiskUsage::INODE,
                    evictable: 2 * DiskUsage::INODE
                },
                blobs.disk_usage()?
            );
        }
        txn.commit()?;

        let mut blob = Blob::open(&fixture.db, protected1)?;
        blob.write_all(&vec![1u8; 1 * blksize as usize]).await?;
        blob.update_db().await?;

        let mut blob = Blob::open(&fixture.db, protected2)?;
        blob.write_all(&vec![1u8; 2 * blksize as usize]).await?;
        blob.update_db().await?;

        let mut blob = Blob::open(&fixture.db, evictable1)?;
        blob.write_all(&vec![1u8; 3 * blksize as usize]).await?;
        blob.update_db().await?;

        let mut blob = Blob::open(&fixture.db, evictable2)?;
        blob.write_all(&vec![1u8; 4 * blksize as usize]).await?;
        blob.update_db().await?;

        let txn = fixture.begin_read()?;
        let blobs = txn.read_blobs()?;

        let disk_usage = blobs.disk_usage()?;
        assert!(disk_usage.total > disk_usage.evictable);

        // This test also tests sparse file support on the FS/OS.
        // While actual disk usage is unpredictable, with sparse files
        // on different OS, disk usage should be less than the whole
        // file size.
        assert!(disk_usage.total < 4 * filesize);
        assert!(disk_usage.evictable < 2 * filesize);

        Ok(())
    }

    #[tokio::test]
    async fn disk_usage_updates_on_blob_deletion() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("foobar")?;

        let txn = fixture.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let mut blobs = txn.write_blobs()?;
            let marks = txn.read_marks()?;
            blobs.create(&mut tree, &marks, &path, &test_hash(), 1024 * 1024)?;
        }
        txn.commit()?;

        let mut blob = Blob::open(&fixture.db, &path)?;
        blob.write_all(&vec![1u8; 4096]).await?;
        blob.update_db().await?;

        let txn = fixture.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let mut blobs = txn.write_blobs()?;

            assert!(blobs.disk_usage()?.total > 0);

            blobs.delete(&mut tree, &path)?;

            assert_eq!(0, blobs.disk_usage()?.total);
        }
        txn.commit()?;

        Ok(())
    }

    #[tokio::test]
    async fn cleanup() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let mut inodes = vec![Inode::ZERO; 4];
        let txn = fixture.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let mut blobs = txn.write_blobs()?;
            let marks = txn.read_marks()?;
            for i in 0..4 {
                inodes[i] = blobs
                    .create(
                        &mut tree,
                        &marks,
                        Path::parse(format!("{i}").as_str())?,
                        &test_hash(),
                        4096,
                    )?
                    .inode;
            }

            // set LRU order, from least recently used (2) to most
            // recently used (0)
            blobs.mark_accessed(inodes[2])?;
            blobs.mark_accessed(inodes[1])?;
            blobs.mark_accessed(inodes[3])?;
            blobs.mark_accessed(inodes[0])?;
        }
        txn.commit()?;

        for i in 0..4 {
            let mut blob = Blob::open(&fixture.db, inodes[i])?;
            blob.write_all(&vec![1u8; 4096]).await?;
            blob.update_db().await?;
        }

        let txn = fixture.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let mut blobs = txn.write_blobs()?;
            assert_eq!(
                DiskUsage {
                    total: DiskUsage::INODE * 4 + 4 * 4096,
                    evictable: DiskUsage::INODE * 4 + 4 * 4096,
                },
                blobs.disk_usage()?
            );

            blobs.cleanup(&mut tree, 8 * 4096)?; // does nothing
            assert_eq!(
                DiskUsage::INODE * 4 + 4 * 4096,
                blobs.disk_usage()?.evictable
            );

            blobs.cleanup(&mut tree, DiskUsage::INODE * 2 + 2 * 4096)?; // remove 2/4
            assert_eq!(
                DiskUsage::INODE * 2 + 2 * 4096,
                blobs.disk_usage()?.evictable
            );

            // the 2 least recently accessed are the ones that were deleted
            assert!(blobs.get(&tree, inodes[2])?.is_none());
            assert!(blobs.get(&tree, inodes[1])?.is_none());

            blobs.cleanup(&mut tree, 0)?; // remove all
            assert_eq!(0, blobs.disk_usage()?.evictable);

            for i in 0..4 {
                assert!(blobs.get(&tree, inodes[i])?.is_none(), "blob {i}");
            }
        }
        let watch = fixture.db.blobs().watch_disk_usage();
        txn.commit()?;
        assert!(watch.has_changed()?);

        Ok(())
    }

    #[tokio::test]
    async fn cleanup_empty_queue() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let txn = fixture.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;

        // This does nothing; it just shouldn't fail
        blobs.cleanup(&mut tree, 0)?;
        blobs.cleanup(&mut tree, 8 * 4096)?;

        Ok(())
    }

    #[tokio::test]
    async fn cleanup_skips_open_blobs() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let mut inodes = vec![Inode::ZERO; 4];
        let txn = fixture.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let mut blobs = txn.write_blobs()?;
            let marks = txn.read_marks()?;
            for i in 0..4 {
                inodes[i] = blobs
                    .create(
                        &mut tree,
                        &marks,
                        Path::parse(format!("{i}").as_str())?,
                        &test_hash(),
                        4096,
                    )?
                    .inode;
            }
        }
        txn.commit()?;

        for i in 0..4 {
            let mut blob = Blob::open(&fixture.db, inodes[i])?;
            blob.write_all(&vec![1u8; 4096]).await?;
            blob.update_db().await?;
        }

        let open1 = Blob::open(&fixture.db, inodes[1])?;
        let open1_again = Blob::open(&fixture.db, inodes[1])?;
        let open2 = Blob::open(&fixture.db, inodes[2])?;

        let txn = fixture.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        blobs.cleanup(&mut tree, 0)?;

        // blobs 1 and 2 could not be cleaned up, because they were open
        assert!(blobs.get(&tree, inodes[0])?.is_none());
        assert!(blobs.get(&tree, inodes[1])?.is_some());
        assert!(blobs.get(&tree, inodes[2])?.is_some());
        assert!(blobs.get(&tree, inodes[3])?.is_none());

        assert_eq!(
            2 * DiskUsage::INODE + 2 * 4096,
            blobs.disk_usage()?.evictable
        );

        // dropping open2 allows it to be deleted, but dropping open1 isn't enough
        drop(open1);
        drop(open2);

        blobs.cleanup(&mut tree, 0)?;

        assert!(blobs.get(&tree, inodes[1])?.is_some());
        assert!(blobs.get(&tree, inodes[2])?.is_none());

        // dropping open1_again allows 1 to be deleted
        drop(open1_again);
        blobs.cleanup(&mut tree, 0)?;

        assert!(blobs.get(&tree, inodes[1])?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn cleanup_cache_leaves_protected_blobs_alone() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let protected1 = Path::parse("protected/1")?;
        let protected2 = Path::parse("protected/2")?;
        let evictable1 = Path::parse("evictable/1")?;
        let evictable2 = Path::parse("evictable/2")?;

        let txn = fixture.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let mut blobs = txn.write_blobs()?;
            let mut marks = txn.write_marks()?;
            let mut dirty = txn.write_dirty()?;
            marks.set(&mut tree, &mut dirty, Path::parse("protected")?, Mark::Keep)?;

            for path in [&protected1, &protected2, &evictable1, &evictable2] {
                blobs.create(
                    &mut tree,
                    &marks,
                    path,
                    &test_hash(),
                    1024 * 1024, // 1M
                )?;
            }
        }
        txn.commit()?;

        for path in [&protected1, &protected2, &evictable1, &evictable2] {
            let mut blob = Blob::open(&fixture.db, path)?;
            blob.write_all(&vec![1u8; 4096]).await?;
            blob.update_db().await?;
        }

        let txn = fixture.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        blobs.cleanup(&mut tree, 0)?;

        // the protected blobs are still there, while the evictable
        // ones are all gone.
        assert!(blobs.get(&tree, protected1)?.is_some());
        assert!(blobs.get(&tree, protected2)?.is_some());
        assert!(blobs.get(&tree, evictable1)?.is_none());
        assert!(blobs.get(&tree, evictable2)?.is_none());

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

    #[test]
    fn set_protected() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.begin_write()?;
        {
            let marks = txn.read_marks()?;
            let mut blobs = txn.write_blobs()?;
            let mut tree = txn.write_tree()?;
            let mut dirty = txn.write_dirty()?;

            let path = Path::parse("test.txt")?;
            let blob_info = blobs.create(&mut tree, &marks, &path, &test_hash(), 100)?;
            assert_eq!(false, blob_info.protected);

            dirty.delete_range(0, 999)?; // clear all
            blobs.set_protected(&tree, &mut dirty, &path, true)?;

            let updated_info = blobs.get(&tree, &path)?.unwrap();
            assert_eq!(true, updated_info.protected);

            assert_eq!(
                vec![blob_info.inode],
                blobs
                    .head(LruQueueId::Protected)
                    .collect::<Result<Vec<_>, StorageError>>()?
            );
            assert_eq!(
                vec![blob_info.inode],
                blobs
                    .tail(LruQueueId::Protected)
                    .collect::<Result<Vec<_>, StorageError>>()?
            );

            assert_eq!(
                Some(blob_info.inode),
                dirty.next_dirty(0)?.map(|(inode, _)| inode)
            );
        }
        let watch = fixture.db.blobs().watch_disk_usage();
        txn.commit()?;
        assert!(watch.has_changed()?);

        Ok(())
    }

    #[test]
    fn set_protected_protected_to_working() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.begin_write()?;
        let mut marks = txn.write_marks()?;
        let mut blobs = txn.write_blobs()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;

        let path = Path::parse("test.txt")?;
        marks.set(&mut tree, &mut dirty, &path, Mark::Keep)?;
        let blob_info = blobs.create(&mut tree, &marks, &path, &test_hash(), 100)?;
        assert_eq!(true, blob_info.protected);

        dirty.delete_range(0, 999)?; // clear all
        blobs.set_protected(&tree, &mut dirty, &path, false)?;

        let updated_info = blobs.get(&tree, &path)?.unwrap();
        assert_eq!(false, updated_info.protected);

        assert_eq!(
            vec![blob_info.inode],
            blobs
                .head(LruQueueId::WorkingArea)
                .collect::<Result<Vec<_>, StorageError>>()?
        );
        assert_eq!(
            vec![blob_info.inode],
            blobs
                .tail(LruQueueId::WorkingArea)
                .collect::<Result<Vec<_>, StorageError>>()?
        );

        assert!(blobs.head(LruQueueId::Protected).next().is_none());

        assert_eq!(
            Some(blob_info.inode),
            dirty.next_dirty(0)?.map(|(inode, _)| inode)
        );
        Ok(())
    }

    #[test]
    fn set_protected_no_change_when_already_protected() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.begin_write()?;
        let mut marks = txn.write_marks()?;
        let mut blobs = txn.write_blobs()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;

        let path = Path::parse("test.txt")?;
        marks.set(&mut tree, &mut dirty, &path, Mark::Keep)?;
        let blob_info = blobs.create(&mut tree, &marks, &path, &test_hash(), 100)?;
        assert_eq!(true, blob_info.protected);

        dirty.delete_range(0, 999)?; // clear all
        blobs.set_protected(&tree, &mut dirty, &path, true)?;

        // nothing changed, and the dirty bit wasn't set
        let updated_info = blobs.get(&tree, &path)?.unwrap();
        assert_eq!(true, updated_info.protected);
        assert_eq!(blob_info.inode, updated_info.inode);
        assert!(dirty.next_dirty(0)?.is_none());

        Ok(())
    }

    #[test]
    fn set_protected_nonexistent_blob() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.begin_write()?;
        let mut blobs = txn.write_blobs()?;
        let tree = txn.read_tree()?;
        let mut dirty = txn.write_dirty()?;

        let path = Path::parse("nonexistent.txt")?;

        // should not fail, just do noting
        blobs.set_protected(&tree, &mut dirty, &path, true)?;

        assert!(blobs.get(&tree, &path)?.is_none());

        Ok(())
    }

    #[test]
    fn set_protected_updates_disk_usage() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.begin_write()?;
        let marks = txn.read_marks()?;
        let mut blobs = txn.write_blobs()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;

        let path = Path::parse("test.txt")?;
        let _blob_info = blobs.create(&mut tree, &marks, &path, &test_hash(), 100)?;

        assert_eq!(
            DiskUsage {
                total: DiskUsage::INODE,
                evictable: DiskUsage::INODE
            },
            blobs.disk_usage()?
        );

        blobs.set_protected(&tree, &mut dirty, &path, true)?;

        assert_eq!(
            DiskUsage {
                total: DiskUsage::INODE,
                evictable: 0
            },
            blobs.disk_usage()?
        );

        blobs.set_protected(&tree, &mut dirty, &path, false)?;

        assert_eq!(
            DiskUsage {
                total: DiskUsage::INODE,
                evictable: DiskUsage::INODE
            },
            blobs.disk_usage()?
        );

        Ok(())
    }

    #[tokio::test]
    async fn watch_disk_usage() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        assert_eq!(
            DiskUsage::ZERO,
            *fixture.db.blobs().watch_disk_usage().borrow()
        );

        let path = Path::parse("test.txt")?;
        let txn = fixture.begin_write()?;
        {
            let marks = txn.read_marks()?;
            let mut blobs = txn.write_blobs()?;
            let mut tree = txn.write_tree()?;

            blobs.create(&mut tree, &marks, &path, &test_hash(), 100)?;

            // Before commit, disk usage should still be the same
            assert_eq!(
                DiskUsage::ZERO,
                *fixture.db.blobs().watch_disk_usage().borrow()
            );
        }
        txn.commit()?;

        // After commit, disk usage should include the new inode
        assert_eq!(
            DiskUsage {
                total: DiskUsage::INODE,
                evictable: DiskUsage::INODE
            },
            *fixture.db.blobs().watch_disk_usage().borrow()
        );

        let mut blob = Blob::open(&fixture.db, &path)?;
        blob.write_all(b"hello").await?;
        blob.update_db().await?;

        let blksize = fixture.blob_dir.metadata()?.blksize();
        assert_eq!(
            DiskUsage {
                total: DiskUsage::INODE + blksize,
                evictable: DiskUsage::INODE + blksize
            },
            *fixture.db.blobs().watch_disk_usage().borrow()
        );

        Ok(())
    }

    #[tokio::test]
    async fn mark_accessed_loop_updates_blob_queue() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let shutdown = CancellationToken::new();

        let txn = fixture.begin_write()?;
        let mut inodes = vec![];
        {
            let marks = txn.read_marks()?;
            let mut blobs = txn.write_blobs()?;
            let mut tree = txn.write_tree()?;

            for i in 0..3 {
                let path = Path::parse(format!("blob{i}.txt"))?;
                let info = blobs.create(&mut tree, &marks, &path, &test_hash(), 100)?;
                inodes.push(info.inode);
            }
        }
        txn.commit()?;

        let handle = tokio::spawn({
            let db = Arc::clone(&fixture.db);
            let shutdown = shutdown.clone();
            async move {
                mark_accessed_loop(db, Duration::from_millis(10), shutdown).await;
            }
        });

        // Wait a bit for the loop to start
        let deadline = Instant::now() + Duration::from_secs(5);
        while fixture.db.blobs().accessed_tx.receiver_count() == 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(1, fixture.db.blobs().accessed_tx.receiver_count());

        for _ in 0..20 {
            drop(Blob::open(&fixture.db, inodes[0])?);
            let _ = fixture.db.blobs().accessed_tx.send(Inode(999)); // invalid; should be ignored
            drop(Blob::open(&fixture.db, inodes[2])?);
            drop(Blob::open(&fixture.db, inodes[1])?);
        }

        // Cooldown period plus some buffer
        tokio::time::sleep(Duration::from_millis(100)).await;

        let txn = fixture.begin_read()?;
        let blobs = txn.read_blobs()?;
        let queue_order: Vec<Inode> = blobs
            .head(LruQueueId::WorkingArea)
            .collect::<Result<Vec<_>, StorageError>>()?;
        assert_eq!(vec![inodes[1], inodes[2], inodes[0]], queue_order);

        shutdown.cancel();
        handle.await?;

        Ok(())
    }
}
