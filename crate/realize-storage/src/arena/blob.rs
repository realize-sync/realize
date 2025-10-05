use super::db::{ArenaDatabase, BeforeCommit};
use super::dirty::WritableOpenDirty;
use super::mark::{MarkExt, MarkReadOperations};
use super::tree::{TreeExt, TreeLoc, TreeReadOperations, WritableOpenTree};
use super::types::{BlobTableEntry, CacheStatus, LruQueueId, Mark, QueueTableEntry};
use crate::arena::cache::CacheReadOperations;
use crate::arena::db::Tag;
use crate::types::PathId;
use crate::utils::hash;
use crate::utils::holder::Holder;
use crate::{RemoteAvailability, StorageError};
use priority_queue::PriorityQueue;
use realize_types::{ByteRanges, Hash};
use redb::{ReadableTable, Table};
use std::cmp::{Reverse, min};
use std::fs;
use std::io::{SeekFrom, Write};
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncSeek, AsyncSeekExt, ReadBuf};
use tokio::sync::{broadcast, watch};
use tokio_util::sync::CancellationToken;

mod file;

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
                let pathid = match ret {
                    Ok(pathid) => pathid,
                    Err(broadcast::error::RecvError::Lagged(_))=> continue,
                    Err(broadcast::error::RecvError::Closed) => return
                };
                queue.push(pathid, Reverse(Instant::now()));
            }
            _ = tokio::time::sleep(cooldown), if !queue.is_empty() => {
                let db = Arc::clone(&db);
                let mut queue = std::mem::take(&mut queue);
                // This is all best effort.
                let _ = tokio::task::spawn_blocking(move || {
                    let txn = db.begin_write()?;
                    {
                        let mut blobs = txn.write_blobs()?;
                        while let Some((pathid, _)) = queue.pop() {
                            // If marking one pathid fails, don't make
                            // other pathids fail.
                            let _ = blobs.mark_accessed(pathid);
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

    disk_usage_tx: watch::Sender<DiskUsage>,

    /// Kept to not lose history when there are no receivers.
    _disk_usage_tx: watch::Receiver<DiskUsage>,

    /// Report blob accesses
    accessed_tx: broadcast::Sender<PathId>,

    registry: Arc<file::BlobFileRegistry>,
}
impl Blobs {
    pub(crate) fn setup(
        blob_dir: &std::path::Path,
        blob_lru_queue_table: &impl redb::ReadableTable<u16, Holder<'static, QueueTableEntry>>,
    ) -> Result<Blobs, StorageError> {
        if !blob_dir.exists() {
            std::fs::create_dir_all(blob_dir)?;
        }

        let (tx, rx) = watch::channel(disk_usage_op(blob_lru_queue_table)?);
        let (accessed_tx, _) = broadcast::channel(16);
        Ok(Self {
            blob_dir: blob_dir.to_path_buf(),
            disk_usage_tx: tx,
            _disk_usage_tx: rx,
            accessed_tx,
            registry: file::BlobFileRegistry::new(),
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
    fn subscribe_accessed(&self) -> broadcast::Receiver<PathId> {
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
    T: ReadableTable<PathId, Holder<'static, BlobTableEntry>>,
    TQ: ReadableTable<u16, Holder<'static, QueueTableEntry>>,
{
    blob_table: T,
    #[allow(dead_code)]
    blob_lru_queue_table: TQ,
}

impl<T, TQ> ReadableOpenBlob<T, TQ>
where
    T: ReadableTable<PathId, Holder<'static, BlobTableEntry>>,
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
    tag: Tag,
    before_commit: &'a BeforeCommit,

    blob_table: Table<'a, PathId, Holder<'static, BlobTableEntry>>,
    blob_lru_queue_table: Table<'a, u16, Holder<'static, QueueTableEntry>>,
    subsystem: &'a Blobs,

    /// If true, an after-commit has been registered to report disk usage at
    /// the end of the transaction.
    will_report_disk_usage: bool,

    registry: Arc<file::BlobFileRegistry>,
}

impl<'a> WritableOpenBlob<'a> {
    pub(crate) fn new(
        tag: Tag,
        blobs: &Blobs,
        before_commit: &'a BeforeCommit,
        blob_table: Table<'a, PathId, Holder<'static, BlobTableEntry>>,
        blob_lru_queue_table: Table<'a, u16, Holder<'static, QueueTableEntry>>,
        subsystem: &'a Blobs,
    ) -> Self {
        Self {
            tag,
            before_commit,
            blob_table,
            blob_lru_queue_table,
            subsystem,
            will_report_disk_usage: false,
            registry: Arc::clone(&blobs.registry),
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

    // Arbitrary disk space considered to be occupied by an pathid.
    //
    // This value is added to the disk usage estimates. It is meant to
    // account for the disk space used by the pathid itself, so it doesn't
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
    pub(crate) pathid: PathId,
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
    fn new(pathid: PathId, entry: BlobTableEntry) -> Self {
        Self {
            pathid,
            size: entry.content_size,
            hash: entry.content_hash,
            available_ranges: entry.written_areas,
            protected: entry.queue == LruQueueId::Protected,
            verified: entry.verified,
        }
    }

    pub(crate) fn cache_status(&self) -> CacheStatus {
        let file_range = ByteRanges::single(0, self.size);
        if file_range == file_range.intersection(&self.available_ranges) {
            if self.verified {
                return CacheStatus::Verified;
            }
            return CacheStatus::Complete;
        }
        if self.available_ranges.is_empty() {
            return CacheStatus::Missing;
        }

        CacheStatus::Partial(self.size, self.available_ranges.clone())
    }
}

/// Read operations for blobs. See also [BlobExt].
pub(crate) trait BlobReadOperations {
    /// Return some info about the blob.
    ///
    /// This call returns [StorageError::NotFound] if the blob doesn't
    /// exist on the database. Use [WritableOpenBlob::create] to
    /// create the blob entry.
    fn get_with_pathid(&self, pathid: PathId) -> Result<Option<BlobInfo>, StorageError>;

    /// Returns a double-ended iterator over the given queue, starting at the head.
    #[allow(dead_code)] // TODO: make it test-only
    fn head(&self, queue: LruQueueId) -> impl Iterator<Item = Result<PathId, StorageError>>;

    /// Returns a double-ended iterator over the given queue, starting at the tail.
    #[allow(dead_code)] // TODO: make it test-only
    fn tail(&self, queue: LruQueueId) -> impl Iterator<Item = Result<PathId, StorageError>>;

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
    T: ReadableTable<PathId, Holder<'static, BlobTableEntry>>,
    TQ: ReadableTable<u16, Holder<'static, QueueTableEntry>>,
{
    fn get_with_pathid(&self, pathid: PathId) -> Result<Option<BlobInfo>, StorageError> {
        get_read_op(&self.blob_table, pathid)
    }

    fn head(&self, queue: LruQueueId) -> impl Iterator<Item = Result<PathId, StorageError>> {
        QueueIterator::head(&self.blob_table, &self.blob_lru_queue_table, queue)
    }

    fn tail(&self, queue: LruQueueId) -> impl Iterator<Item = Result<PathId, StorageError>> {
        QueueIterator::tail(&self.blob_table, &self.blob_lru_queue_table, queue)
    }

    fn disk_usage(&self) -> Result<DiskUsage, StorageError> {
        disk_usage_op(&self.blob_lru_queue_table)
    }
}

impl<'a> BlobReadOperations for WritableOpenBlob<'a> {
    fn get_with_pathid(&self, pathid: PathId) -> Result<Option<BlobInfo>, StorageError> {
        get_read_op(&self.blob_table, pathid)
    }

    fn head(&self, queue: LruQueueId) -> impl Iterator<Item = Result<PathId, StorageError>> {
        QueueIterator::head(&self.blob_table, &self.blob_lru_queue_table, queue)
    }

    fn tail(&self, queue: LruQueueId) -> impl Iterator<Item = Result<PathId, StorageError>> {
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
    fn cache_status<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<CacheStatus, StorageError>;
}

impl<T: BlobReadOperations> BlobExt for T {
    fn get<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<Option<BlobInfo>, StorageError> {
        if let Some(pathid) = tree.resolve(loc)? {
            self.get_with_pathid(pathid)
        } else {
            Ok(None)
        }
    }

    fn cache_status<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<CacheStatus, StorageError> {
        if let Some(blob_info) = self.get(tree, loc)? {
            Ok(blob_info.cache_status())
        } else {
            Ok(CacheStatus::Missing)
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
    /// If a blob already exists with the same pathid and hash, it is
    /// reused and this call then works just like
    /// [BlobReadOperations::get].
    ///
    /// If a blob already exists with the same pathid, but another
    /// hash, it is overwritten and its data cleared.
    pub(crate) fn create<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        marks: &impl MarkReadOperations,
        loc: L,
        hash: &Hash,
        size: u64,
    ) -> Result<BlobInfo, StorageError> {
        let (pathid, _, entry) = self.create_entry(tree, marks, loc, hash, size)?;
        self.report_disk_usage_changed();

        Ok(BlobInfo::new(pathid, entry))
    }

    fn create_entry<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree<'_>,
        marks: &impl MarkReadOperations,
        loc: L,
        hash: &Hash,
        size: u64,
    ) -> Result<(PathId, PathBuf, BlobTableEntry), StorageError> {
        let pathid = tree.setup(loc)?;
        let existing_entry = if let Some(e) = self.blob_table.get(pathid)? {
            Some(e.value().parse()?)
        } else {
            None
        };
        if let Some(e) = existing_entry {
            if e.content_hash == *hash {
                return Ok((pathid, self.subsystem.blob_dir.join(pathid.hex()), e));
            }

            // Existing entry cannot be reuse; remove.
            self.remove_from_queue(&e)?;
            let _ = fs::remove_file(self.subsystem.blob_dir.join(pathid.hex()));
        }
        let blob_path = self.prepare_blob_file(pathid, size)?;
        let queue = choose_queue(tree, marks, pathid)?;
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
        self.add_to_queue_front(queue, pathid, &mut entry)?;

        log::debug!(
            "[{}] Created blob {pathid} {hash} in {queue:?} at {blob_path:?} -> {entry:?}",
            self.tag
        );
        tree.insert_and_incref(pathid, &mut self.blob_table, pathid, Holder::new(&entry)?)?;
        Ok((pathid, blob_path, entry))
    }

    fn prepare_blob_file(&mut self, pathid: PathId, size: u64) -> Result<PathBuf, StorageError> {
        let blob_dir = self.subsystem.blob_dir.as_path();
        let blob_path = blob_dir.join(pathid.hex());
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
        let pathid = match tree.resolve(loc)? {
            Some(pathid) => pathid,
            None => return Ok(()), // Nothing to delete
        };

        if !self.remove_blob_entry(tree, pathid)? {
            return Ok(());
        }
        let blob_path = self.subsystem.blob_dir.join(pathid.hex());
        if let Ok(m) = blob_path.metadata() {
            self.registry.realize_blocking(&m);
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
        let pathid = match tree.resolve(loc)? {
            Some(pathid) => pathid,
            None => return Ok(()), // Nothing to modify
        };

        let mut blob_entry = match self.blob_table.get(pathid)? {
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
        self.add_to_queue_front(new_queue, pathid, &mut blob_entry)?;

        // Update the entry in the table
        self.blob_table
            .insert(pathid, Holder::with_content(blob_entry)?)?;
        dirty.mark_dirty(pathid, "set_protected")?;
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
    pub(crate) fn realize<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        loc: L,
    ) -> Result<Option<std::path::PathBuf>, StorageError> {
        let pathid = match tree.resolve(loc)? {
            Some(pathid) => pathid,
            None => return Ok(None), // Nothing to export
        };
        let realpath = self.subsystem.blob_dir.join(pathid.hex());
        let m = match realpath.metadata() {
            Ok(m) => m,
            Err(_) => return Ok(None),
        };

        self.remove_blob_entry(tree, pathid)?;
        self.report_disk_usage_changed();
        self.registry.realize_blocking(&m);

        Ok(Some(realpath))
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
        let (pathid, blob_path, mut entry) = self.create_entry(tree, marks, loc, hash, size)?;

        // Set written areas to complete (but not verified) and update
        // trusting that metadata is going to be the blob file
        // metadata.
        entry.written_areas = ByteRanges::single(0, size);
        entry.verified = false;
        self.update_disk_usage(&mut entry, metadata)?;
        self.blob_table
            .insert(pathid, Holder::with_content(entry)?)?;
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
    fn mark_accessed(&mut self, pathid: PathId) -> Result<(), StorageError> {
        let mut blob_entry = match self.blob_table.get(pathid)? {
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
        self.move_to_front(pathid, &mut blob_entry)?;

        // Update the entry in the table
        self.blob_table
            .insert(pathid, Holder::with_content(blob_entry)?)?;

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
        let pathid = match tree.resolve(loc)? {
            Some(pathid) => pathid,
            None => return Ok(false), // Nothing to mark
        };

        let mut blob_entry = match self.blob_table.get(pathid)? {
            None => {
                return Ok(false);
            }
            Some(v) => v.value().parse()?,
        };

        // Only mark as verified if the hash matches
        if blob_entry.content_hash == *hash {
            blob_entry.verified = true;
            log::debug!("[{}] {pathid} content verified to be {hash}", self.tag);

            self.blob_table
                .insert(pathid, Holder::with_content(blob_entry)?)?;
            dirty.mark_dirty(pathid, "verified")?;
            return Ok(true);
        }

        Ok(false)
    }

    /// Extend local availability of the blob with the given range.
    ///
    /// If the blob exists and has the same hash as was given, update
    /// the blob and return true, otherwise do nothing and return
    /// false.
    pub(crate) fn extend_cache_status<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &impl TreeReadOperations,
        loc: L,
        hash: &Hash,
        new_range: &ByteRanges,
    ) -> Result<bool, StorageError> {
        let pathid = match tree.resolve(loc)? {
            Some(pathid) => pathid,
            None => return Ok(false), // Nothing to extend
        };

        let mut blob_entry = match get_blob_entry(&self.blob_table, pathid)? {
            Some(e) => e,
            None => return Ok(false),
        };
        if blob_entry.content_hash != *hash {
            return Ok(false);
        }
        blob_entry.written_areas = blob_entry.written_areas.union(new_range);

        // Update disk usage if the blob file exists
        let blob_path = self.subsystem.blob_dir.join(pathid.hex());
        if let Ok(metadata) = blob_path.metadata() {
            self.update_disk_usage(&mut blob_entry, &metadata)?;
            self.report_disk_usage_changed();
        }

        self.blob_table
            .insert(pathid, Holder::with_content(blob_entry)?)?;

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
            "[{}] Cleaning up of WorkingArea with disk usage: {}, target: {}",
            queue.disk_usage,
            target,
            self.tag
        );

        let mut prev = queue.tail;
        let mut removed_count = 0;
        while let Some(current_id) = prev
            && queue.disk_usage > target
        {
            let current = follow_queue_link(&self.blob_table, current_id)?;
            prev = current.prev;

            log::debug!(
                "[{}] Evicted blob from WorkingArea: {current_id} ({} bytes)",
                current.disk_usage,
                self.tag
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
            "[{}] Evicted {removed_count} entries from WorkingArea disk usage now {} (target: {target})",
            queue.disk_usage,
            self.tag
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
        pathid: PathId,
        blob_entry: &mut BlobTableEntry,
    ) -> Result<(), StorageError> {
        let mut queue =
            get_queue_if_available(&self.blob_lru_queue_table, queue_id)?.unwrap_or_default();
        self.add_to_queue_front_update_entry(queue_id, pathid, blob_entry, &mut queue)?;
        self.blob_lru_queue_table
            .insert(queue_id as u16, Holder::with_content(queue)?)?;

        Ok(())
    }

    fn add_to_queue_front_update_entry(
        &mut self,
        queue_id: LruQueueId,
        pathid: PathId,
        blob_entry: &mut BlobTableEntry,
        queue: &mut QueueTableEntry,
    ) -> Result<(), StorageError> {
        blob_entry.queue = queue_id;
        if let Some(head_id) = queue.head {
            let mut head = follow_queue_link(&self.blob_table, head_id)?;
            head.prev = Some(pathid);

            self.blob_table
                .insert(head_id, Holder::with_content(head)?)?;
        } else {
            // This is the first node in the queue, so both the head
            // and the tail
            queue.tail = Some(pathid);
        }
        blob_entry.next = queue.head;
        queue.head = Some(pathid);
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
        pathid: PathId,
    ) -> Result<bool, StorageError> {
        if let Some(entry) = get_blob_entry(&self.blob_table, pathid)? {
            self.remove_from_queue(&entry)?;
            tree.remove_and_decref(pathid, &mut self.blob_table, pathid)?;
            return Ok(true);
        }

        Ok(false)
    }

    /// Move the blob to the front of its queue
    fn move_to_front(
        &mut self,
        pathid: PathId,
        blob_entry: &mut BlobTableEntry,
    ) -> Result<(), StorageError> {
        if blob_entry.prev.is_none() {
            // Already at the head of its queue
            return Ok(());
        }
        let queue_id = blob_entry.queue;
        let mut queue = get_queue_must_exist(&self.blob_lru_queue_table, queue_id)?;
        self.remove_from_queue_update_entry(blob_entry, &mut queue)?;
        self.add_to_queue_front_update_entry(queue_id, pathid, blob_entry, &mut queue)?;
        self.blob_lru_queue_table
            .insert(queue_id as u16, Holder::with_content(queue)?)?;
        Ok(())
    }
}

#[allow(dead_code)]
pub(crate) struct QueueIterator<'a, T>
where
    T: redb::ReadableTable<PathId, Holder<'static, BlobTableEntry>>,
{
    blob_table: &'a T,
    err: Option<StorageError>,
    next: Option<PathId>,
    next_fn: fn(BlobTableEntry) -> Option<PathId>,
}

#[allow(dead_code)]
impl<'a, T> QueueIterator<'a, T>
where
    T: redb::ReadableTable<PathId, Holder<'static, BlobTableEntry>>,
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
    T: redb::ReadableTable<PathId, Holder<'static, BlobTableEntry>>,
{
    type Item = Result<PathId, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(err) = self.err.take() {
            return Some(Err(err));
        }
        match self.next.take() {
            None => None,
            Some(pathid) => {
                {
                    // return current and move to next
                    match follow_queue_link(self.blob_table, pathid) {
                        Err(err) => {
                            self.err = Some(err);
                            self.next = None;
                        }
                        Ok(e) => {
                            self.next = (self.next_fn)(e);
                        }
                    };
                };
                Some(Ok(pathid))
            }
        }
    }
}

fn choose_queue(
    tree: &mut WritableOpenTree<'_>,
    marks: &impl MarkReadOperations,
    pathid: PathId,
) -> Result<LruQueueId, StorageError> {
    let queue = match marks.get(tree, pathid)? {
        Mark::Watch => LruQueueId::WorkingArea,
        Mark::Keep | Mark::Own => LruQueueId::Protected,
    };
    Ok(queue)
}

/// Calculate disk usage for a blob file using Unix metadata.
fn calculate_disk_usage(metadata: &std::fs::Metadata) -> u64 {
    // block() takes into account actual usage, not the whole file
    // size, which might be sparse. Include the pathid size into the
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
    blob_table: &impl redb::ReadableTable<PathId, Holder<'static, BlobTableEntry>>,
    pathid: PathId,
) -> Result<Option<BlobTableEntry>, StorageError> {
    if let Some(entry) = blob_table.get(pathid)? {
        Ok(Some(entry.value().parse()?))
    } else {
        Ok(None)
    }
}

fn follow_queue_link(
    blob_table: &impl redb::ReadableTable<PathId, Holder<'static, BlobTableEntry>>,
    pathid: PathId,
) -> Result<BlobTableEntry, StorageError> {
    get_blob_entry(blob_table, pathid)?
        .ok_or_else(|| StorageError::InconsistentDatabase(format!("invalid queue link {pathid}")))
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
    shared: Arc<file::SharedBlobFile>,
    range_rx: watch::Receiver<file::ReadableRange>,

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
        let path = blob_dir.join(info.pathid.hex());
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(&path)?;
        let file_meta = file.metadata()?;
        if info.size != file_meta.len() {
            file.set_len(info.size)?;
            file.flush()?;
        }

        let blobs = db.blobs();
        let shared = blobs.registry.get(db, &path, &info)?;
        let range_rx = shared.subscribe();
        let _ = blobs.accessed_tx.send(info.pathid);

        Ok(Self {
            info,
            db: Arc::clone(db),
            file: tokio::fs::File::from_std(file),
            shared,
            offset: 0,
            range_rx,
        })
    }

    /// Get the size of the corresponding file.
    pub fn size(&self) -> u64 {
        self.info.size
    }

    /// Current read/write position within the file.
    ///
    /// This is the position relative to the start of the file.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Get the parts of the file that are available locally.
    ///
    /// This might not include ranges that have recently been updated,
    /// if [Blob::update_db] hasn't been called.
    pub fn available_range(&self) -> ByteRanges {
        match &*self.range_rx.borrow() {
            file::ReadableRange::Limited(r) => r.clone(),
            file::ReadableRange::Direct => ByteRanges::single(0, self.info.size),
        }
    }

    /// Return local availability for the blob.
    ///
    /// This is based on the [Blob::available_range] so might return a
    /// different result than if you checked the database.
    ///
    /// Returns None if the blob has been taken out of the cache (realized).
    pub async fn cache_status(&self) -> Option<CacheStatus> {
        self.shared.cache_status().await
    }

    /// Cache data from a remote peer locally, into the blob file.
    ///
    /// This call writes to the file and modifies the file offset. It
    /// does nothing if the range is already available.
    ///
    /// Return [StorageError::InvalidBlobState] if the blob is not
    /// updatable. This might happen even if the blob was updatable
    /// when the method was called, but was deleted or realized was
    /// being updated.
    pub async fn update(&mut self, offset: u64, buf: &[u8]) -> Result<(), StorageError> {
        self.shared.update(offset, buf).await
    }

    /// Repair the file content at [offset], without updating
    /// available range.
    ///
    /// Return [StorageError::InvalidBlobState] if the blob is not
    /// complete. This might happen even if the blob was complete when
    /// the method was called, but was deleted or realized while it
    /// was being repaired.
    pub async fn repair(&mut self, offset: u64, buf: &[u8]) -> Result<(), StorageError> {
        self.shared.repair(offset, buf).await
    }

    /// Return the information needed for fetching data for that blob
    /// from remote peers.
    ///
    /// Returns None if the data isn't available from
    /// any remote peers. The returned [FileAvailability] is guaranteed to have at least one peer.
    pub async fn remote_availability(&self) -> Result<Option<RemoteAvailability>, StorageError> {
        let db = Arc::clone(&self.db);
        let pathid = self.info.pathid;
        let hash = self.info.hash.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read()?;

            txn.read_cache()?
                .remote_availability(&txn.read_tree()?, pathid, &hash)
        })
        .await?
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
    async fn compute_hash(&mut self) -> Result<Hash, std::io::Error> {
        if self.offset != 0 {
            self.seek(SeekFrom::Start(0)).await?;
        }

        hash::hash_file(self).await
    }

    /// Check whether the file content matches the hash, if yes, mark
    /// the blob as verified and return true if the file was verified.
    ///
    /// Return:

    /// - Ok(true) if the file was verified successfully
    /// - Ok(false) if the hash didn't match.
    /// - Err([StorageError::InvalidBlobState]) if the blob is not
    ///   complete. This might happen even if the blob was complete
    ///   when the method was called, but was deleted or realized
    ///   while it was being verified.
    pub async fn verify(&mut self) -> Result<bool, StorageError> {
        self.shared.prepare_for_verification().await?;
        let hash = self.compute_hash().await?;
        if hash != self.info.hash {
            return Ok(false);
        }
        self.shared.mark_verified().await?;

        Ok(true)
    }

    /// Flush data and report any ranges written to to the database.
    pub async fn update_db(&mut self) -> Result<(), StorageError> {
        self.shared.update_db().await
    }

    /// Returns how much data, out of `requested_len` can be read at `offset`.
    ///
    /// - `Some(0)` means that reading would succeed, but would return nothing.
    /// - `None` means that reading would fail with the error [BlobIncomplete].
    pub fn readable_length(&self, offset: u64, requested_len: usize) -> Option<usize> {
        // It's always possible to read nothing, and nothing is what you'll get.
        // A file can always be read past its end, but yields no data.
        if requested_len == 0 {
            return Some(0);
        }
        match &*self.range_rx.borrow() {
            file::ReadableRange::Direct => Some(requested_len),
            file::ReadableRange::Limited(r) => {
                if offset >= self.size() {
                    return Some(0);
                }

                r.containing_range(offset)
                    .map(|r| min(requested_len, (r.end - offset) as usize))
            }
        }
    }

    /// Realize the blob, that is, move the file into the data
    /// directory, and return the file handle so that the file content
    /// can be modified.
    pub async fn realize(self) -> Result<tokio::fs::File, StorageError> {
        let Self { db, info, file, .. } = self;
        let _guard = db.cache().inhibit_watcher();
        let pathid = info.pathid;

        if self.shared.cache_status().await.is_none() {
            // Blob has already been realized, there's nothing to do
            return Ok(file);
        }

        // Realize the blob in the database, then move the file.
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_write()?;
            let source: PathBuf;
            let dest: PathBuf;
            {
                let mut tree = txn.write_tree()?;
                let mut blobs = txn.write_blobs()?;
                let mut cache = txn.write_cache()?;
                let mut dirty = txn.write_dirty()?;
                let mut history = txn.write_history()?;
                (source, dest) = cache.realize(
                    &mut tree,
                    &mut blobs,
                    &mut dirty,
                    &mut history,
                    pathid,
                    true,
                )?;

                if let Some(parent) = dest.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::rename(&source, &dest)?;
            }

            if let Err(err) = txn.commit() {
                let _ = std::fs::rename(&dest, &source);
                return Err(err);
            }

            Ok(())
        })
        .await??;

        Ok(file)
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

fn get_read_op(
    blob_table: &impl ReadableTable<PathId, Holder<'static, BlobTableEntry>>,
    pathid: PathId,
) -> Result<Option<BlobInfo>, StorageError> {
    if let Some(e) = blob_table.get(pathid)? {
        Ok(Some(BlobInfo::new(pathid, e.value().parse()?)))
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
    use crate::arena::cache::CacheExt;
    use crate::arena::db::{ArenaReadTransaction, ArenaWriteTransaction};
    use crate::arena::dirty::DirtyReadOperations;
    use crate::arena::types::Version;
    use crate::arena::update;
    use crate::utils::hash;
    use crate::{Mark, Notification, PathId};
    use assert_fs::TempDir;
    use assert_fs::fixture::ChildPath;
    use assert_fs::prelude::*;
    use realize_types::{Arena, ByteRange, Path, Peer, UnixTime};
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
            let blob_dir = tempdir.child(format!("{arena}/blobs"));
            blob_dir.create_dir_all()?;
            let datadir = tempdir.child(format!("{arena}/data"));
            datadir.create_dir_all()?;
            let db =
                ArenaDatabase::for_testing_single_arena(arena, blob_dir.path(), datadir.path())?;

            Ok(Self {
                arena,
                db,
                blob_dir,
                tempdir,
            })
        }

        /// Return the path to a blob file for test use.
        fn blob_path(&self, pathid: PathId) -> std::path::PathBuf {
            self.tempdir
                .child(format!("{}/blobs/{}", self.arena, pathid.hex()))
                .to_path_buf()
        }

        fn begin_read(&self) -> anyhow::Result<ArenaReadTransaction<'_>> {
            Ok(self.db.begin_read()?)
        }

        fn begin_write(&self) -> anyhow::Result<ArenaWriteTransaction<'_>> {
            Ok(self.db.begin_write()?)
        }

        fn get_blob_entry(&self, pathid: PathId) -> anyhow::Result<BlobTableEntry> {
            let txn = self.begin_write()?;
            let blobs = txn.write_blobs()?;
            Ok(get_blob_entry(&blobs.blob_table, pathid)?.ok_or(StorageError::NotFound)?)
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
                    let blob_path = self.blob_path(info.pathid);
                    std::fs::write(&blob_path, &test_data[0..partial])?;
                    blobs.extend_cache_status(
                        &tree,
                        info.pathid,
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

            let pathid = info.pathid;
            assert_eq!(tree.resolve(path)?, Some(pathid));
            assert_eq!(Some(info), blobs.get_with_pathid(pathid)?);

            let file_path = fixture.blob_path(pathid);
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
        let path = Path::parse("blob/test1.txt")?;

        let txn = fixture.begin_write()?;
        let old_info = {
            let mut blobs = txn.write_blobs()?;
            let marks = txn.write_marks()?;
            let mut tree = txn.write_tree()?;
            blobs.create(&mut tree, &marks, &path, &hash::digest("old"), 3)?
        };
        txn.commit()?;

        {
            let mut blob = Blob::open_with_info(&fixture.db, old_info)?;
            blob.update(0, b"old").await?;
            blob.update_db().await?;
        }

        let txn = fixture.begin_write()?;
        let new_info = {
            let mut blobs = txn.write_blobs()?;
            let marks = txn.write_marks()?;
            let mut tree = txn.write_tree()?;
            blobs.create(&mut tree, &marks, &path, &hash::digest("new!"), 4)?
        };
        txn.commit()?;

        let txn = fixture.begin_read()?;
        let blobs = txn.read_blobs()?;
        let actual_info = blobs.get_with_pathid(new_info.pathid)?.unwrap();
        assert_eq!(actual_info, new_info);

        let m = fixture.blob_path(actual_info.pathid).metadata()?;
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
            blob.update(0, b"old").await?;
            blob.file.flush().await?;
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
            std::fs::read_to_string(fixture.blob_path(actual_info.pathid))?
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
                .pathid;
        }
        txn.commit()?;

        let txn = fixture.begin_write()?;
        {
            let mut blobs = txn.write_blobs()?;
            let mut tree = txn.write_tree()?;

            let pathid = tree.expect(&path)?;
            blobs.delete(&mut tree, &path)?;

            assert_eq!(None, blobs.get_with_pathid(pathid)?);

            let file_path = fixture.blob_path(pathid);
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
    async fn writing_to_blob_updates_cache_status() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("baa/baa")?;
        fixture.create_blob_with_partial_data(&path, "Baa, baa, black sheep", 0)?;

        let mut blob = Blob::open(&fixture.db, &path)?;

        blob.update(0, b"Baa, baa").await?;
        // File hasn't been flushed and database hasn't been updated
        // yet.
        assert_eq!(true, blob.available_range().is_empty());

        let watch = fixture.db.blobs().watch_disk_usage();
        blob.update(8, b", black sheep").await?;
        // Database was updated, since the blob is complete
        assert_eq!(ByteRanges::single(0, 21), blob.available_range());

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
        blob.update(8, b", black sheep").await?;
        blob.update(0, b"Baa, baa").await?;
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
        blob.update(8, b", black sheep").await?;
        blob.update_db().await?;
        assert_eq!(
            ByteRanges::single(8, 21),
            fixture.blob_info(&path)?.unwrap().available_ranges
        );

        blob.update(0, b"Baa, baa").await?;

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

        blob.update(0, b"Baa, baa").await?;
        blob.update_db().await?;
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

        blob.update(8, b", black sheep").await?;
        blob.update_db().await?;
        assert_eq!(ByteRanges::single(0, 21), blob.available_range());
        assert_eq!(
            ByteRanges::new(),
            fixture.blob_info(&path)?.unwrap().available_ranges
        );

        assert_eq!(true, blob.verify().await?);
        assert_eq!(false, fixture.blob_info(&path)?.unwrap().verified);
        assert_eq!(ByteRanges::single(0, 21), blob.available_range());
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
        assert_eq!(ByteRanges::single(0, 21), blob.available_range());
        assert_eq!(true, blob.verify().await?);
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
        let BlobInfo { pathid, .. } = fixture.blob_info(&path)?.unwrap();

        assert_eq!(true, Blob::open(&fixture.db, &path)?.verify().await?);

        // export to dest
        let dest = fixture.tempdir.path().join("moved_blob");
        let txn = fixture.begin_write()?;
        {
            let mut blobs = txn.write_blobs()?;
            let mut tree = txn.write_tree()?;
            let source = blobs.realize(&mut tree, &path)?.unwrap();
            std::fs::rename(source, &dest)?;
        }
        let watch = fixture.db.blobs().watch_disk_usage();
        txn.commit()?;
        assert!(watch.has_changed()?);

        assert_eq!("Baa, baa, black sheep", std::fs::read_to_string(&dest)?);

        let txn = fixture.begin_read()?;
        let blobs = txn.read_blobs()?;
        assert!(blobs.get_with_pathid(pathid)?.is_none());

        // The LRU queue must have been updated properly.
        assert!(blobs.head(LruQueueId::WorkingArea).next().is_none());

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
            assert_eq!(None, blobs.realize(&mut tree, &path)?);
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
            blob.available_range()
        );
        let mut buf = String::new();
        blob.read_to_string(&mut buf).await?;
        assert_eq!(
            "When you light a candle, you also cast a shadow.".to_string(),
            buf
        );

        let BlobInfo {
            pathid, verified, ..
        } = fixture.blob_info(&path)?.unwrap();
        assert_eq!(false, verified);

        // Disk usage must be set.
        let blob_entry = fixture.get_blob_entry(pathid)?;
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
            blob.available_range()
        );
        let mut buf = String::new();
        blob.read_to_string(&mut buf).await?;
        assert_eq!(
            "When you light a candle, you also cast a shadow.".to_string(),
            buf
        );

        let BlobInfo {
            pathid, verified, ..
        } = fixture.blob_info(&path)?.unwrap();
        assert_eq!(false, verified);

        // Disk usage must be set.
        let blob_entry = fixture.get_blob_entry(pathid)?;
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
            .pathid;

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
            .pathid;

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
            .pathid;

        let two = blobs
            .create(
                &mut tree,
                &marks,
                Path::parse("two")?,
                &hash::digest("two"),
                3,
            )?
            .pathid;
        let three = blobs
            .create(
                &mut tree,
                &marks,
                Path::parse("three")?,
                &hash::digest("three"),
                3,
            )?
            .pathid;

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
                .pathid;

            let two = blobs
                .create(
                    &mut tree,
                    &marks,
                    Path::parse("two")?,
                    &hash::digest("two"),
                    3,
                )?
                .pathid;
            let three = blobs
                .create(
                    &mut tree,
                    &marks,
                    Path::parse("three")?,
                    &hash::digest("three"),
                    3,
                )?
                .pathid;

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

            let two_pathid = tree.resolve(Path::parse("two")?)?.unwrap();
            let three_pathid = tree.resolve(Path::parse("three")?)?.unwrap();
            let one_pathid = tree.resolve(Path::parse("one")?)?.unwrap();

            blobs.mark_accessed(two_pathid)?;

            assert_eq!(
                vec![two_pathid, three_pathid, one_pathid],
                blobs
                    .head(LruQueueId::WorkingArea)
                    .collect::<Result<Vec<_>, StorageError>>()?
            );

            blobs.mark_accessed(one_pathid)?;

            assert_eq!(
                vec![one_pathid, two_pathid, three_pathid],
                blobs
                    .head(LruQueueId::WorkingArea)
                    .collect::<Result<Vec<_>, StorageError>>()?
            );

            // make sure the list is correct both ways
            assert_eq!(
                vec![three_pathid, two_pathid, one_pathid],
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
        blob.update(0, &vec![1u8; 1 * blksize as usize]).await?;
        blob.update_db().await?;

        let mut blob = Blob::open(&fixture.db, protected2)?;
        blob.update(0, &vec![1u8; 2 * blksize as usize]).await?;
        blob.update_db().await?;

        let mut blob = Blob::open(&fixture.db, evictable1)?;
        blob.update(0, &vec![1u8; 3 * blksize as usize]).await?;
        blob.update_db().await?;

        let mut blob = Blob::open(&fixture.db, evictable2)?;
        blob.update(0, &vec![1u8; 4 * blksize as usize]).await?;
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
        blob.update(0, &vec![1u8; 4096]).await?;
        blob.update_db().await?;

        tokio::task::spawn_blocking(move || {
            let txn = fixture.begin_write()?;
            {
                let mut tree = txn.write_tree()?;
                let mut blobs = txn.write_blobs()?;

                assert!(blobs.disk_usage()?.total > 0);

                blobs.delete(&mut tree, &path)?;

                assert_eq!(0, blobs.disk_usage()?.total);
            }
            txn.commit()?;

            Ok::<_, anyhow::Error>(())
        })
        .await??;

        Ok(())
    }

    #[tokio::test]
    async fn cleanup() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let mut pathids = vec![PathId::ZERO; 4];
        let txn = fixture.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let mut blobs = txn.write_blobs()?;
            let marks = txn.read_marks()?;
            for i in 0..4 {
                pathids[i] = blobs
                    .create(
                        &mut tree,
                        &marks,
                        Path::parse(format!("{i}").as_str())?,
                        &test_hash(),
                        4096,
                    )?
                    .pathid;
            }

            // set LRU order, from least recently used (2) to most
            // recently used (0)
            blobs.mark_accessed(pathids[2])?;
            blobs.mark_accessed(pathids[1])?;
            blobs.mark_accessed(pathids[3])?;
            blobs.mark_accessed(pathids[0])?;
        }
        txn.commit()?;

        for i in 0..4 {
            let mut blob = Blob::open(&fixture.db, pathids[i])?;
            blob.update(0, &vec![1u8; 4096]).await?;
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
            assert!(blobs.get(&tree, pathids[2])?.is_none());
            assert!(blobs.get(&tree, pathids[1])?.is_none());

            blobs.cleanup(&mut tree, 0)?; // remove all
            assert_eq!(0, blobs.disk_usage()?.evictable);

            for i in 0..4 {
                assert!(blobs.get(&tree, pathids[i])?.is_none(), "blob {i}");
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
            blob.update(0, &vec![1u8; 4096]).await?;
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
        assert!(blobs.get_with_pathid(PathId(999))?.is_none());

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
        let blob_info = blobs.create(&mut tree, &mut mark, PathId(10), &test_hash(), 100)?;
        assert_eq!(blob_info.hash, test_hash());
        assert_eq!(blob_info.size, 100);

        Ok(())
    }

    #[test]
    fn cache_status_missing_when_no_blob() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.begin_write()?;
        let blobs = txn.write_blobs()?;
        let tree = txn.read_tree()?;

        // Test with a path that doesn't exist in the tree
        let path = Path::parse("nonexistent.txt")?;
        let availability = blobs.cache_status(&tree, &path)?;
        assert_eq!(availability, CacheStatus::Missing);

        // Test with an pathid that doesn't exist
        let availability = blobs.cache_status(&tree, PathId(99999))?;
        assert_eq!(availability, CacheStatus::Missing);

        Ok(())
    }

    #[test]
    fn cache_status_missing_when_blob_empty() -> anyhow::Result<()> {
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
        let availability = blobs.cache_status(&tree, &path)?;
        assert_eq!(availability, CacheStatus::Missing);

        Ok(())
    }

    #[test]
    fn cache_status_partial_when_blob_incomplete() -> anyhow::Result<()> {
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
        blobs.extend_cache_status(&tree, &path, &hash, &partial_ranges)?;

        // Test local availability - should be Partial
        let availability = blobs.cache_status(&tree, &path)?;
        match availability {
            CacheStatus::Partial(size, ranges) => {
                assert_eq!(size, 1000);
                assert_eq!(partial_ranges, ranges);
            }
            _ => panic!("Expected Partial availability, got {:?}", availability),
        }

        Ok(())
    }

    #[test]
    fn cache_status_complete_when_blob_full_but_unverified() -> anyhow::Result<()> {
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
        blobs.extend_cache_status(&tree, &path, &hash, &complete_range)?;

        // Test local availability - should be Complete (not verified)
        let availability = blobs.cache_status(&tree, &path)?;
        assert_eq!(availability, CacheStatus::Complete);

        Ok(())
    }

    #[test]
    fn cache_status_verified_when_blob_full_and_verified() -> anyhow::Result<()> {
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
        blobs.extend_cache_status(&tree, &path, &hash, &complete_range)?;

        // Mark as verified
        blobs.mark_verified(&tree, &mut dirty, &path, &hash)?;

        let availability = blobs.cache_status(&tree, &path)?;
        assert_eq!(availability, CacheStatus::Verified);

        Ok(())
    }

    #[test]
    fn cache_status_handles_zero_size_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.begin_write()?;
        let mut blobs = txn.write_blobs()?;
        let mut tree = txn.write_tree()?;
        let marks = txn.read_marks()?;

        let path = Path::parse("zero.txt")?;
        let hash = test_hash();
        blobs.create(&mut tree, &marks, &path, &hash, 0)?;

        let availability = blobs.cache_status(&tree, &path)?;
        assert_eq!(availability, CacheStatus::Complete);

        Ok(())
    }

    #[test]
    fn cache_status_handles_nonexistent_pathid() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.begin_write()?;
        let blobs = txn.write_blobs()?;
        let tree = txn.read_tree()?;

        // Test with a non-existent pathid
        let availability = blobs.cache_status(&tree, PathId(99999))?;
        assert_eq!(availability, CacheStatus::Missing);

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
                vec![blob_info.pathid],
                blobs
                    .head(LruQueueId::Protected)
                    .collect::<Result<Vec<_>, StorageError>>()?
            );
            assert_eq!(
                vec![blob_info.pathid],
                blobs
                    .tail(LruQueueId::Protected)
                    .collect::<Result<Vec<_>, StorageError>>()?
            );

            assert_eq!(
                Some(blob_info.pathid),
                dirty.next_dirty(0)?.map(|(pathid, _)| pathid)
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
            vec![blob_info.pathid],
            blobs
                .head(LruQueueId::WorkingArea)
                .collect::<Result<Vec<_>, StorageError>>()?
        );
        assert_eq!(
            vec![blob_info.pathid],
            blobs
                .tail(LruQueueId::WorkingArea)
                .collect::<Result<Vec<_>, StorageError>>()?
        );

        assert!(blobs.head(LruQueueId::Protected).next().is_none());

        assert_eq!(
            Some(blob_info.pathid),
            dirty.next_dirty(0)?.map(|(pathid, _)| pathid)
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
        assert_eq!(blob_info.pathid, updated_info.pathid);
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

        // After commit, disk usage should include the new pathid
        assert_eq!(
            DiskUsage {
                total: DiskUsage::INODE,
                evictable: DiskUsage::INODE
            },
            *fixture.db.blobs().watch_disk_usage().borrow()
        );

        let mut blob = Blob::open(&fixture.db, &path)?;
        blob.update(0, b"hello").await?;
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
        let mut pathids = vec![];
        {
            let marks = txn.read_marks()?;
            let mut blobs = txn.write_blobs()?;
            let mut tree = txn.write_tree()?;

            for i in 0..3 {
                let path = Path::parse(format!("blob{i}.txt"))?;
                let info = blobs.create(&mut tree, &marks, &path, &test_hash(), 100)?;
                pathids.push(info.pathid);
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
            drop(Blob::open(&fixture.db, pathids[0])?);
            let _ = fixture.db.blobs().accessed_tx.send(PathId(999)); // invalid; should be ignored
            drop(Blob::open(&fixture.db, pathids[2])?);
            drop(Blob::open(&fixture.db, pathids[1])?);
        }

        // Cooldown period plus some buffer
        tokio::time::sleep(Duration::from_millis(100)).await;

        let txn = fixture.begin_read()?;
        let blobs = txn.read_blobs()?;
        let queue_order: Vec<PathId> = blobs
            .head(LruQueueId::WorkingArea)
            .collect::<Result<Vec<_>, StorageError>>()?;
        assert_eq!(vec![pathids[1], pathids[2], pathids[0]], queue_order);

        shutdown.cancel();
        handle.await?;

        Ok(())
    }

    #[tokio::test]
    async fn realize_blob() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("test/file.txt")?;
        let test_data = "Hello, blob world!";
        let hash = hash::digest(test_data);

        update::apply(
            &fixture.db,
            Peer::from("peer"),
            Notification::Add {
                arena: fixture.arena,
                index: 0,
                path: path.clone(),
                mtime: UnixTime::from_secs(1234567890),
                size: test_data.len() as u64,
                hash: hash.clone(),
            },
        )?;
        let txn = fixture.db.begin_write()?;
        {
            let mut cache = txn.write_cache()?;
            cache.create_blob(
                &mut txn.write_tree()?,
                &mut txn.write_blobs()?,
                &txn.read_marks()?,
                &path,
            )?;
        }
        txn.commit()?;

        let mut blob = Blob::open(&fixture.db, &path)?;
        blob.update(0, test_data.as_bytes()).await?;
        blob.update_db().await?;

        // Realize the blob
        let mut file = blob.realize().await?;

        // Verify the file contains the downloaded content
        let mut buf = String::new();
        file.seek(SeekFrom::Start(0)).await?;
        file.read_to_string(&mut buf).await?;
        assert_eq!(test_data, buf);

        // An entry was added to the index with the old hash as base
        let txn = fixture.begin_read()?;
        let cache = txn.read_cache()?;
        let tree = txn.read_tree()?;
        let indexed = cache.indexed(&tree, &path)?.unwrap();
        assert_eq!(Version::Modified(Some(hash)), indexed.version);

        Ok(())
    }

    #[tokio::test]
    async fn realize_blob_with_open_blobs() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("test/file.txt")?;
        let test_data = "Hello, blob world!";
        let hash = hash::digest(test_data);

        update::apply(
            &fixture.db,
            Peer::from("peer"),
            Notification::Add {
                arena: fixture.arena,
                index: 0,
                path: path.clone(),
                mtime: UnixTime::from_secs(1234567890),
                size: test_data.len() as u64,
                hash: hash.clone(),
            },
        )?;
        let txn = fixture.db.begin_write()?;
        {
            let mut cache = txn.write_cache()?;
            cache.create_blob(
                &mut txn.write_tree()?,
                &mut txn.write_blobs()?,
                &txn.read_marks()?,
                &path,
            )?;
        }
        txn.commit()?;

        let mut blob = Blob::open(&fixture.db, &path)?;
        blob.update(0, test_data.as_bytes()).await?;

        let mut blobs = vec![];
        for _ in 0..4 {
            blobs.push(Blob::open(&fixture.db, &path)?);
        }
        for i in 0..4 {
            let mut buf = String::new();
            blobs[i].read_to_string(&mut buf).await?;
            assert_eq!(test_data, buf, "blobs[{i}]")
        }

        let mut file = blob.realize().await?;
        file.seek(SeekFrom::Start(1)).await?;
        file.write_all(&[b'u']).await?;
        file.flush().await?;

        // File is in the index, so no new blob can be opened.
        assert!(matches!(
            Blob::open(&fixture.db, &path).err(),
            Some(StorageError::NotFound)
        ));

        // Other handles are in the detached state.
        for i in 0..4 {
            assert_eq!(
                ByteRanges::single(0, test_data.len() as u64),
                blobs[i].available_range()
            );
            assert_eq!(None, blobs[i].cache_status().await);
        }

        // update_db can be called on other handles, but this has no effect.
        for i in 0..4 {
            blobs[i].update_db().await.unwrap();
        }

        // Other handles cannot be updated, verified or repaired
        for i in 0..4 {
            let ret = blobs[i].verify().await;
            assert!(
                matches!(ret, Err(StorageError::InvalidBlobState)),
                "{ret:?}"
            );

            let ret = blobs[i].update(0, b"abc").await;
            assert!(
                matches!(ret, Err(StorageError::InvalidBlobState)),
                "{ret:?}"
            );

            let ret = blobs[i].repair(0, b"abc").await;
            assert!(
                matches!(ret, Err(StorageError::InvalidBlobState)),
                "{ret:?}"
            );
        }

        // Other handles are still readable; they'll return the data
        // from the now local file.
        for i in 0..4 {
            let mut buf = String::new();
            blobs[i].seek(SeekFrom::Start(0)).await?;
            blobs[i].read_to_string(&mut buf).await?;
            assert_eq!("Hullo, blob world!", buf, "blobs[{i}]")
        }

        // Other handles can be used to write to the same file
        for (i, blob) in blobs.into_iter().enumerate() {
            let mut file = blob.realize().await?;
            file.seek(SeekFrom::Start(7)).await?;
            file.write_all(format!("blob {i} now").as_bytes()).await?;
            file.flush().await?;

            let mut buf = String::new();
            file.seek(SeekFrom::Start(0)).await?;
            file.read_to_string(&mut buf).await?;
            assert_eq!(format!("Hullo, blob {i} now!"), buf)
        }
        let mut buf = String::new();
        file.seek(SeekFrom::Start(0)).await?;
        file.read_to_string(&mut buf).await?;
        assert_eq!(format!("Hullo, blob 3 now!"), buf);

        Ok(())
    }

    #[tokio::test]
    async fn blobs_enforce_updatable_complete_verified_states() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("foobar")?;
        let hash = hash::digest("foobar");

        let txn = fixture.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let mut blobs = txn.write_blobs()?;
            let marks = txn.read_marks()?;
            blobs.create(&mut tree, &marks, &path, &hash, 6)?;
        }
        txn.commit()?;

        let mut blob = Blob::open(&fixture.db, &path)?;
        let mut otherblob = Blob::open(&fixture.db, &path)?;
        blob.update(0, b"foo").await.unwrap();

        // The blob is still incomplete, so verify and repair fail
        for blob in [&mut blob, &mut otherblob] {
            let ret = blob.verify().await;
            assert!(
                matches!(ret, Err(StorageError::InvalidBlobState)),
                "{ret:?}"
            );

            let ret = blob.repair(0, b"roo").await;
            assert!(
                matches!(ret, Err(StorageError::InvalidBlobState)),
                "{ret:?}"
            );
        }

        // Complete the blob
        blob.update(3, b"bar").await.unwrap();
        for blob in [&blob, &otherblob] {
            assert_eq!(Some(CacheStatus::Complete), blob.cache_status().await);
        }

        // Now that the  blob is complete, updating it again fails.
        for blob in [&mut blob, &mut otherblob] {
            let ret = blob.update(0, b"baa").await;
            assert!(
                matches!(ret, Err(StorageError::InvalidBlobState)),
                "{ret:?}"
            );
        }

        // update_db still succeeds, but does noting
        for blob in [&mut blob, &mut otherblob] {
            blob.update_db().await.unwrap();
        }

        // verify and repair works now that the blob is complete
        blob.repair(0, b"roo").await.unwrap();
        assert_eq!(false, blob.verify().await.unwrap());

        blob.repair(0, b"foo").await.unwrap();
        assert_eq!(true, blob.verify().await.unwrap());
        for blob in [&blob, &otherblob] {
            assert_eq!(Some(CacheStatus::Verified), blob.cache_status().await);
        }

        // verify and repair don't work anymore once the blob has been verified
        for blob in [&mut blob, &mut otherblob] {
            let ret = blob.verify().await;
            assert!(
                matches!(ret, Err(StorageError::InvalidBlobState)),
                "{ret:?}"
            );
            let ret = blob.repair(0, b"roo").await;
            assert!(
                matches!(ret, Err(StorageError::InvalidBlobState)),
                "{ret:?}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn delete_open_blob() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("foobar")?;

        let txn = fixture.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let mut blobs = txn.write_blobs()?;
            let marks = txn.read_marks()?;
            blobs.create(&mut tree, &marks, &path, &hash::digest("foobar"), 6)?;
        }
        txn.commit()?;

        let mut blob = Blob::open(&fixture.db, &path)?;
        blob.update(0, b"foo").await?;

        tokio::task::spawn_blocking(move || {
            let txn = fixture.begin_write()?;
            {
                let mut tree = txn.write_tree()?;
                let mut blobs = txn.write_blobs()?;

                blobs.delete(&mut tree, &path)?;
            }
            txn.commit()?;

            Ok::<_, anyhow::Error>(())
        })
        .await??;

        // Open blob file has been taken out of the cache
        assert_eq!(None, blob.cache_status().await);

        // It is still readable.
        let mut buf = String::new();
        blob.seek(SeekFrom::Start(0)).await?;
        blob.read_to_string(&mut buf).await?;
        assert_eq!("foo\0\0\0", buf);

        // It can even be realized and written to.
        let mut file = blob.realize().await?;
        file.seek(SeekFrom::Start(3)).await?;
        file.write_all(b"BAR").await?;
        file.seek(SeekFrom::Start(0)).await?;
        let mut buf = String::new();
        file.read_to_string(&mut buf).await?;
        assert_eq!("fooBAR", buf);

        Ok(())
    }
}
