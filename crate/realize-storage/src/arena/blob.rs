use super::db::{ArenaDatabase, ArenaReadTransaction, ArenaWriteTransaction};
use super::hasher::hash_file;
use super::types::{
    BlobTableEntry, FileTableEntry, LocalAvailability, LruQueueId, QueueTableEntry,
};
use crate::StorageError;
use crate::types::BlobId;
use crate::utils::holder::Holder;
use realize_types::{ByteRange, ByteRanges, Hash};
use redb::ReadableTable;
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

/// A blob store that handles blob-specific operations.
///
/// This struct contains blob-specific logic and database operations.
pub(crate) struct Blobstore {
    db: Arc<ArenaDatabase>,
    blob_dir: PathBuf,
    /// Reference counter for open blobs. Maps BlobId to the number of open handles.
    open_blobs: OpenBlobRefCounter,
}

impl Blobstore {
    /// Create a new Blobstore from a database and blob directory.
    pub(crate) fn new(
        db: Arc<ArenaDatabase>,
        blob_dir: &std::path::Path,
    ) -> Result<Arc<Self>, StorageError> {
        // Ensure the database has the required blob table
        {
            let txn = db.begin_write()?;
            txn.blob_table()?;
            txn.commit()?;
        }

        if !blob_dir.exists() {
            std::fs::create_dir_all(&blob_dir)?;
        }

        Ok(Arc::new(Self {
            db,
            blob_dir: blob_dir.to_path_buf(),
            open_blobs: OpenBlobRefCounter::new(),
        }))
    }

    /// Open an existing blob and returns its [Blob]
    pub(crate) fn open_blob(
        self: &Arc<Self>,
        txn: &ArenaReadTransaction,
        file_entry: FileTableEntry,
        blob_id: BlobId,
    ) -> Result<Blob, StorageError> {
        let blob_table = txn.blob_table()?;
        let blob_entry = get_blob_entry(&blob_table, blob_id)?;
        let file = self.open_blob_file(blob_id, file_entry.metadata.size, false)?;

        return Ok(Blob::new(
            blob_id,
            file_entry,
            blob_entry,
            file,
            Arc::clone(self),
        ));
    }

    /// Create a new blob and returns its [Blob]
    pub(crate) fn create_blob(
        self: &Arc<Self>,
        txn: &ArenaWriteTransaction,
        file_entry: FileTableEntry,
        queue: LruQueueId,
    ) -> Result<Blob, StorageError> {
        let mut blob_table = txn.blob_table()?;
        let mut blob_lru_queue_table = txn.blob_lru_queue_table()?;
        let mut blob_next_id_table = txn.blob_next_id_table()?;
        let blob_id = match file_entry.content.blob {
            Some(blob_id) => blob_id,
            None => alloc_blob_id(&mut blob_next_id_table)?,
        };
        let file = self.open_blob_file(blob_id, file_entry.metadata.size, true)?;
        let blob_entry = init_blob_entry(
            &mut blob_lru_queue_table,
            &mut blob_table,
            blob_id,
            queue,
            &file.metadata()?,
        )?;
        blob_table.insert(blob_id, Holder::new(&blob_entry)?)?;

        Ok(Blob::new(
            blob_id,
            file_entry,
            blob_entry,
            file,
            Arc::clone(self),
        ))
    }

    /// Open or create a file for the blob and make sure it has the
    /// right size.
    fn open_blob_file(
        &self,
        blob_id: BlobId,
        file_size: u64,
        new_file: bool,
    ) -> Result<std::fs::File, StorageError> {
        let path = self.blob_path(blob_id);
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(new_file)
            .create(true)
            .open(path)?;
        let file_meta = file.metadata()?;
        if file_size != file_meta.len() {
            file.set_len(file_size)?;
            file.flush()?;
        }

        Ok(file)
    }

    /// Return the path of the file for the given blob.
    fn blob_path(&self, blob_id: BlobId) -> PathBuf {
        self.blob_dir.join(blob_id.to_string())
    }

    /// Delete a blob and its associated file.
    pub(crate) fn delete_blob(
        &self,
        txn: &ArenaWriteTransaction,
        blob_id: BlobId,
    ) -> Result<(), StorageError> {
        let mut blob_table = txn.blob_table()?;
        let mut blob_lru_queue_table = txn.blob_lru_queue_table()?;
        remove_blob_entry(&mut blob_table, &mut blob_lru_queue_table, blob_id)?;

        let blob_path = self.blob_path(blob_id);
        if blob_path.exists() {
            std::fs::remove_file(&blob_path)?;
        }

        Ok(())
    }

    /// Mark a blob as accessed by moving it to the front of its queue.
    #[allow(dead_code)]
    pub(crate) fn mark_accessed(&self, blob_id: BlobId) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut blob_table = txn.blob_table()?;
            let mut blob_entry = get_blob_entry(&blob_table, blob_id)?;
            if blob_entry.prev.is_none() {
                // Already the head
                return Ok(());
            }

            let mut blob_lru_queue_table = txn.blob_lru_queue_table()?;
            move_to_front(
                &mut blob_lru_queue_table,
                &mut blob_table,
                blob_id,
                &mut blob_entry,
            )?;

            blob_table.insert(blob_id, Holder::with_content(blob_entry)?)?;
        }
        txn.commit()?;

        Ok(())
    }

    /// Extend the local availability of a blob.
    pub(crate) fn extend_local_availability(
        &self,
        blob_id: BlobId,
        new_range: &ByteRanges,
    ) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut blob_table = txn.blob_table()?;
            let mut blob_lru_queue_table = txn.blob_lru_queue_table()?;
            let mut blob_entry = get_blob_entry(&blob_table, blob_id)?;

            blob_entry.written_areas = blob_entry.written_areas.union(&new_range);
            log::debug!(
                "{blob_id} extended by {new_range}; available: {}",
                blob_entry.written_areas
            );
            if let Ok(metadata) = self.blob_path(blob_id).metadata() {
                update_disk_usage(&mut blob_lru_queue_table, &mut blob_entry, &metadata)?;
            }
            // This function is not an appropriate place to complain
            // about missing file; just ignore the issue for now.

            blob_table.insert(blob_id, Holder::new(&blob_entry)?)?;
        }
        txn.commit()?;

        Ok(())
    }

    fn set_content_hash(&self, blob_id: BlobId, hash: Hash) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut blob_table = txn.blob_table()?;
            let mut blob_entry = get_blob_entry(&blob_table, blob_id)?;
            log::debug!("{blob_id} content verified to be {hash}");
            blob_entry.content_hash = Some(hash);

            blob_table.insert(blob_id, Holder::with_content(blob_entry)?)?;
        }
        txn.commit()?;

        Ok(())
    }

    /// Move the file of `blob_id` to `dest` and delete the entry.
    ///
    /// Does nothing and return false unless the blob content hash is
    /// `content_hash`, which means that the corresponding file must
    /// have been fully downloaded and verified before moving.
    pub(crate) fn move_blob_if_matches(
        &self,
        txn: &ArenaWriteTransaction,
        blob_id: BlobId,
        content_hash: &Hash,
        dest: &std::path::Path,
    ) -> Result<bool, StorageError> {
        let mut blob_table = txn.blob_table()?;
        let blob_entry = get_blob_entry(&blob_table, blob_id)?;
        if !blob_entry
            .content_hash
            .as_ref()
            .map(|h| *h == *content_hash)
            .unwrap_or(false)
        {
            return Ok(false);
        }

        let mut blob_lru_queue_table = txn.blob_lru_queue_table()?;
        remove_blob_entry(&mut blob_table, &mut blob_lru_queue_table, blob_id)?;
        std::fs::rename(self.blob_path(blob_id), dest)?;

        Ok(true)
    }

    /// Setup the database to move some existing file into and return the
    /// path to write to.
    ///
    /// If `blob_id` is None, an entry is created.
    pub(crate) fn move_into_blob(
        &self,
        txn: &ArenaWriteTransaction,
        id: Option<BlobId>,
        queue: LruQueueId,
        metadata: &std::fs::Metadata,
    ) -> Result<(BlobId, PathBuf), StorageError> {
        let mut blob_table = txn.blob_table()?;
        let mut blob_lru_queue_table = txn.blob_lru_queue_table()?;
        let mut blob_next_id_table = txn.blob_next_id_table()?;
        let id = match id {
            Some(blob_id) => blob_id,
            None => alloc_blob_id(&mut blob_next_id_table)?,
        };
        let mut entry = init_blob_entry(
            &mut blob_lru_queue_table,
            &mut blob_table,
            id,
            queue,
            metadata,
        )?;
        entry.written_areas = ByteRanges::single(0, metadata.len());
        update_disk_usage(&mut blob_lru_queue_table, &mut entry, metadata)?;
        blob_table.insert(id, Holder::with_content(entry)?)?;

        Ok((id, self.blob_path(id)))
    }

    /// Remove blobs from the back of the given queue until the total
    /// disk usage is <= target_size.
    ///
    /// This method removes the least recently used blobs first (from the tail
    /// of the queue) and continues until the total disk usage is at or below
    /// the target size. Blobs that are currently open will be skipped.
    pub(crate) fn cleanup_cache(&self, target: u64) -> Result<(), StorageError> {
        self.cleanup_queue(LruQueueId::WorkingArea, target)
    }

    fn cleanup_queue(&self, queue_id: LruQueueId, target: u64) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut blob_lru_queue_table = txn.blob_lru_queue_table()?;
            // Get the WorkingArea queue
            let mut queue = match get_queue_if_available(&blob_lru_queue_table, queue_id)? {
                Some(q) => q,
                None => {
                    // No queue exists, nothing to clean up
                    return Ok(());
                }
            };

            log::debug!(
                "Starting cleanup of {queue_id:?} with disk usage: {}, target: {}",
                queue.disk_usage,
                target
            );

            let mut blob_table = txn.blob_table()?;
            let mut prev = queue.tail;
            let mut removed_count = 0;
            while let Some(current_id) = prev
                && queue.disk_usage > target
            {
                let current = get_blob_entry(&blob_table, current_id)?;
                prev = current.prev;
                if self.open_blobs.is_open(current_id) {
                    continue;
                }
                log::debug!(
                    "Evicted blob from {queue_id:?}: {current_id} ({} bytes)",
                    current.disk_usage
                );
                remove_from_queue_update_entry(&mut blob_table, &current, &mut queue)?;
                removed_count += 1;
                blob_table.remove(current_id)?;
                let blob_path = self.blob_path(current_id);
                if blob_path.exists() {
                    std::fs::remove_file(&blob_path)?;
                }
            }
            log::debug!(
                "Evicted {removed_count} entries from {queue_id:?} disk usage now {} (target: {target})",
                queue.disk_usage
            );
            if removed_count == 0 {
                return Ok(());
            }
            blob_lru_queue_table.insert(queue_id as u16, Holder::with_content(queue)?)?;
        }
        txn.commit()?;

        Ok(())
    }
}

/// Move the blob to the front of its queue
fn move_to_front(
    blob_lru_queue_table: &mut redb::Table<'_, u16, Holder<'static, QueueTableEntry>>,
    blob_table: &mut redb::Table<'_, BlobId, Holder<'static, BlobTableEntry>>,
    blob_id: BlobId,
    blob_entry: &mut BlobTableEntry,
) -> Result<(), StorageError> {
    if blob_entry.prev.is_none() {
        // Already at the head of its queue
        return Ok(());
    }

    log::debug!("{blob_id} moved to the front of {:?}", blob_entry.queue);
    let queue_id = blob_entry.queue;
    remove_from_queue(blob_lru_queue_table, blob_table, blob_entry)?;
    add_to_queue_front(
        blob_lru_queue_table,
        blob_table,
        queue_id,
        blob_id,
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
    blob_table: &mut redb::Table<'_, BlobId, Holder<'static, BlobTableEntry>>,
    blob_lru_queue_table: &mut redb::Table<'_, u16, Holder<'static, QueueTableEntry>>,
    blob_id: BlobId,
) -> Result<(), StorageError> {
    let removed = blob_table.remove(blob_id)?;
    let removed_entry = removed.map(|r| r.value().parse());
    Ok(if let Some(entry) = removed_entry {
        let entry = entry?;
        remove_from_queue(blob_lru_queue_table, blob_table, &entry)?;
    })
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

fn alloc_blob_id(
    blob_next_id_table: &mut redb::Table<'_, (), BlobId>,
) -> Result<BlobId, StorageError> {
    let blob_id = match blob_next_id_table.get(())? {
        Some(entry) => entry.value().plus(1),
        None => BlobId(1), // First blob ever
    };
    blob_next_id_table.insert((), blob_id)?;

    Ok(blob_id)
}

fn init_blob_entry(
    blob_lru_queue_table: &mut redb::Table<'_, u16, Holder<'static, QueueTableEntry>>,
    blob_table: &mut redb::Table<'_, BlobId, Holder<'static, BlobTableEntry>>,
    id: BlobId,
    queue: LruQueueId,
    metadata: &std::fs::Metadata,
) -> Result<BlobTableEntry, StorageError> {
    let mut entry = if let Some(e) = blob_table.get(id)? {
        e.value().parse()?
    } else {
        let mut entry = BlobTableEntry {
            written_areas: ByteRanges::new(),
            content_hash: None,
            queue,
            next: None,
            prev: None,
            disk_usage: 0,
        };
        add_to_queue_front(blob_lru_queue_table, blob_table, queue, id, &mut entry)?;

        entry
    };
    update_disk_usage(blob_lru_queue_table, &mut entry, metadata)?;

    Ok(entry)
}

pub(crate) fn blob_exists(txn: &ArenaReadTransaction, id: BlobId) -> Result<bool, StorageError> {
    Ok(txn.blob_table()?.get(id)?.is_some())
}

pub(crate) fn local_availability(
    txn: &ArenaReadTransaction,
    file_entry: &FileTableEntry,
) -> Result<LocalAvailability, StorageError> {
    match file_entry.content.blob {
        None => Ok(LocalAvailability::Missing),
        Some(blob_id) => {
            let blob_table = txn.blob_table()?;
            let blob_entry = match get_blob_entry(&blob_table, blob_id) {
                Ok(ret) => ret,
                Err(StorageError::NotFound) => {
                    return Ok(LocalAvailability::Missing);
                }
                Err(err) => {
                    return Err(err);
                }
            };
            let file_range = ByteRanges::single(0, file_entry.metadata.size);
            if file_range == file_range.intersection(&blob_entry.written_areas) {
                if let Some(content_hash) = &blob_entry.content_hash
                    && file_entry.content.hash == *content_hash
                {
                    return Ok(LocalAvailability::Verified);
                }
                return Ok(LocalAvailability::Complete);
            }
            if blob_entry.written_areas.is_empty() {
                return Ok(LocalAvailability::Missing);
            }

            Ok(LocalAvailability::Partial(
                file_entry.metadata.size,
                blob_entry.written_areas,
            ))
        }
    }
}

fn get_blob_entry(
    blob_table: &impl redb::ReadableTable<BlobId, Holder<'static, BlobTableEntry>>,
    blob_id: BlobId,
) -> Result<BlobTableEntry, StorageError> {
    let entry = blob_table
        .get(blob_id)?
        .ok_or(StorageError::NotFound)?
        .value()
        .parse()?;

    Ok(entry)
}

/// Add a blob to the front of the WorkingArea queue.
fn add_to_queue_front(
    blob_lru_queue_table: &mut redb::Table<'_, u16, Holder<'static, QueueTableEntry>>,
    blob_table: &mut redb::Table<'_, BlobId, Holder<'static, BlobTableEntry>>,
    queue_id: LruQueueId,
    blob_id: BlobId,
    blob_entry: &mut BlobTableEntry,
) -> Result<(), StorageError> {
    blob_entry.queue = queue_id;

    let mut queue = get_queue_if_available(blob_lru_queue_table, queue_id)?.unwrap_or_default();
    if let Some(head_id) = queue.head {
        let mut head = get_blob_entry(blob_table, head_id)?;
        head.prev = Some(blob_id);

        blob_table.insert(head_id, Holder::with_content(head)?)?;
    } else {
        // This is the first node in the queue, so both the head
        // and the tail
        queue.tail = Some(blob_id);
    }
    blob_entry.next = queue.head;
    queue.head = Some(blob_id);
    blob_entry.prev = None;

    // Update queue's total disk usage
    queue.disk_usage += blob_entry.disk_usage;

    blob_lru_queue_table.insert(queue_id as u16, Holder::with_content(queue)?)?;

    Ok(())
}

/// Remove a blob from its queue.
fn remove_from_queue(
    blob_lru_queue_table: &mut redb::Table<'_, u16, Holder<'static, QueueTableEntry>>,
    blob_table: &mut redb::Table<'_, BlobId, Holder<'static, BlobTableEntry>>,
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
    blob_table: &mut redb::Table<'_, BlobId, Holder<'static, BlobTableEntry>>,
    blob_entry: &BlobTableEntry,
    queue: &mut QueueTableEntry,
) -> Result<(), StorageError> {
    if let Some(prev_id) = blob_entry.prev {
        let mut prev = get_blob_entry(blob_table, prev_id)?;
        prev.next = blob_entry.next;
        blob_table.insert(prev_id, Holder::with_content(prev)?)?;
    } else {
        // This was the first node
        queue.head = blob_entry.next;
    }
    if let Some(next_id) = blob_entry.next {
        let mut next = get_blob_entry(blob_table, next_id)?;
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
    blob_id: BlobId,
    file: tokio::fs::File,
    size: u64,
    hash: Hash,
    blobstore: Arc<Blobstore>,

    /// Complete available range
    available_ranges: ByteRanges,
    /// Updates to the available range already integrated into
    /// available_range but not yet reported to update_tx.
    pending_ranges: ByteRanges,

    /// The read/write/seek position.
    offset: u64,
}

#[allow(dead_code)]
impl Blob {
    /// Create a new blob from a file and its available byte ranges.
    pub(crate) fn new(
        blob_id: BlobId,
        file_entry: FileTableEntry,
        blob_entry: BlobTableEntry,
        file: std::fs::File,
        blobstore: Arc<Blobstore>,
    ) -> Self {
        blobstore.open_blobs.increment(blob_id);
        Self {
            blob_id,
            file: tokio::fs::File::from_std(file),
            available_ranges: blob_entry.written_areas,
            blobstore,
            pending_ranges: ByteRanges::new(),
            size: file_entry.metadata.size,
            hash: file_entry.content.hash,
            offset: 0,
        }
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

    /// Get the blob ID on the database.
    ///
    /// The blob ID is also used to construct the file path.
    pub(crate) fn id(&self) -> BlobId {
        self.blob_id
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
    pub async fn mark_verified(&mut self) -> Result<(), StorageError> {
        self.update_db().await?;

        let blob_id = self.blob_id;
        let blobstore = self.blobstore.clone();
        let hash = self.hash.clone();
        tokio::task::spawn_blocking(move || blobstore.set_content_hash(blob_id, hash)).await?
    }

    /// Make sure any updated content is stored on disk before
    /// continuing.
    pub async fn flush_and_sync(&mut self) -> std::io::Result<()> {
        self.file.flush().await?;
        self.file.sync_all().await?;

        Ok(())
    }

    /// Flush data and report any ranges written to to the database.
    pub async fn update_db(&mut self) -> Result<(), StorageError> {
        if self.pending_ranges.is_empty() {
            return Ok(());
        }
        self.flush_and_sync().await?;

        let blob_id = self.blob_id;
        let ranges = std::mem::take(&mut self.pending_ranges);
        let blobstore = Arc::clone(&self.blobstore);
        let (res, ranges) = tokio::task::spawn_blocking(move || {
            (
                blobstore.extend_local_availability(blob_id, &ranges),
                ranges,
            )
        })
        .await?;
        if !res.is_ok() {
            self.pending_ranges.extend(ranges);
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
        self.blobstore.open_blobs.decrement(self.blob_id);
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
    counts: Mutex<HashMap<BlobId, u32>>,
}
impl OpenBlobRefCounter {
    fn new() -> Self {
        Self {
            counts: Mutex::new(HashMap::new()),
        }
    }

    fn increment(&self, id: BlobId) {
        let mut guard = self.counts.lock().unwrap();
        *guard.entry(id).or_insert(0) += 1;
        log::debug!("Blob {} opened, ref count: {}", id, guard[&id]);
    }
    fn decrement(&self, id: BlobId) {
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
    fn is_open(&self, id: BlobId) -> bool {
        let guard = self.counts.lock().unwrap();
        guard.contains_key(&id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arena::arena_cache::ArenaCache;
    use crate::arena::engine::DirtyPaths;
    use crate::arena::mark::PathMarks;
    use crate::utils::redb_utils;
    use crate::{GlobalDatabase, Inode, InodeAllocator, Mark, Notification};
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
        blob_dir: PathBuf,
        tempdir: TempDir,
    }
    impl Fixture {
        async fn setup() -> anyhow::Result<Fixture> {
            let _ = env_logger::try_init();
            let arena = test_arena();
            let tempdir = TempDir::new()?;
            let allocator =
                InodeAllocator::new(GlobalDatabase::new(redb_utils::in_memory()?)?, [arena])?;

            let child = tempdir.child(format!("{arena}-cache.db"));
            let blob_dir = tempdir.child(format!("{arena}/blobs"));
            if let Some(p) = child.parent() {
                std::fs::create_dir_all(p)?;
            }
            let db = ArenaDatabase::new(redb_utils::in_memory()?)?;
            let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;
            let acache = ArenaCache::new(
                arena,
                allocator,
                Arc::clone(&db),
                blob_dir.path(),
                Arc::clone(&dirty_paths),
            )?;

            Ok(Self {
                arena,
                db,
                acache,
                blob_dir: blob_dir.to_path_buf(),
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
        fn blob_file_exists(&self, blob_id: BlobId) -> bool {
            self.blob_path(blob_id).exists()
        }

        /// Return the path to a blob file for test use.
        fn blob_path(&self, blob_id: BlobId) -> std::path::PathBuf {
            self.tempdir
                .child(format!("{}/blobs/{blob_id}", self.arena))
                .to_path_buf()
        }

        fn begin_read(&self) -> anyhow::Result<ArenaReadTransaction> {
            Ok(self.db.begin_read()?)
        }

        fn begin_write(&self) -> anyhow::Result<ArenaWriteTransaction> {
            Ok(self.db.begin_write()?)
        }

        fn get_blob_entry(&self, blob_id: BlobId) -> anyhow::Result<BlobTableEntry> {
            let txn = self.begin_read()?;
            let blob_table = txn.blob_table()?;
            Ok(get_blob_entry(&blob_table, blob_id)?)
        }

        /// Check if a blob entry exists in the database.
        fn blob_entry_exists(&self, blob_id: BlobId) -> anyhow::Result<bool> {
            let txn = self.begin_read()?;
            let blob_table = txn.blob_table()?;
            match blob_table.get(blob_id)? {
                Some(_) => Ok(true),
                None => Ok(false),
            }
        }

        fn list_queue_content(&self, queue_id: LruQueueId) -> anyhow::Result<Vec<BlobId>> {
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
                current = get_blob_entry(&blob_table, id)?.next;
            }

            Ok(content)
        }

        fn list_queue_content_backward(&self, queue_id: LruQueueId) -> anyhow::Result<Vec<BlobId>> {
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
                current = get_blob_entry(&blob_table, id)?.prev;
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
    }

    #[tokio::test]
    async fn open_file_creates_multiple_blobs_and_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;

        let inode1 = fixture.add_file("test1.txt", 100)?;
        let inode2 = fixture.add_file("test2.txt", 200)?;
        let inode3 = fixture.add_file("test3.txt", 300)?;

        assert_eq!(BlobId(1), acache.open_file(inode1)?.blob_id);
        assert_eq!(BlobId(2), acache.open_file(inode2)?.blob_id);
        assert_eq!(BlobId(3), acache.open_file(inode3)?.blob_id);

        assert!(fixture.blob_entry_exists(BlobId(1))?);
        assert!(fixture.blob_entry_exists(BlobId(2))?);
        assert!(fixture.blob_entry_exists(BlobId(3))?);

        assert!(fixture.blob_path(BlobId(1)).exists());
        assert!(fixture.blob_path(BlobId(2)).exists());
        assert!(fixture.blob_path(BlobId(3)).exists());

        assert_eq!(100, fixture.blob_path(BlobId(1)).metadata()?.len());
        assert_eq!(200, fixture.blob_path(BlobId(2)).metadata()?.len());
        assert_eq!(300, fixture.blob_path(BlobId(3)).metadata()?.len());

        Ok(())
    }

    #[tokio::test]
    async fn read_blob_within_available_ranges() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;

        let inode = fixture.add_file("test.txt", 1000)?;

        // Allocate blob id and create file
        let blob_id = acache.open_file(inode)?.blob_id;

        // Write some test data to the blob file
        let blob_path = fixture.blob_path(blob_id);
        let test_data = b"Baa, baa, black sheep, have you any wool?";
        std::fs::write(&blob_path, test_data)?;

        acache
            .extend_local_availability(blob_id, &ByteRanges::single(0, test_data.len() as u64))?;

        // Read from blob
        let mut blob = fixture.acache.open_file(inode)?;

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
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;

        let inode = fixture.add_file("test.txt", 1000)?;

        // Allocate blob id and create file
        let blob_id = acache.open_file(inode)?.blob_id;

        // Write test data to the blob file
        let blob_path = fixture.blob_path(blob_id);
        let test_data = b"Baa, baa, black sheep, have you any wool?";
        std::fs::write(&blob_path, test_data)?;

        acache
            .extend_local_availability(blob_id, &ByteRanges::single(0, test_data.len() as u64))?;

        // Read from blob
        let mut blob = fixture.acache.open_file(inode)?;
        blob.seek(SeekFrom::Start(b"Baa, baa, black sheep, ".len() as u64))
            .await?;
        let mut buf = [0; 100];
        let n = blob.read(&mut buf).await?;
        assert_eq!(b"have you any wool?", &buf[0..n]);

        Ok(())
    }

    #[tokio::test]
    async fn read_blob_outside_available_ranges() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;
        let inode = fixture.add_file("test.txt", 1000)?;

        // Allocate blob id and create file
        let blob_id = acache.open_file(inode)?.blob_id;

        // Write test data to the blob file
        let blob_path = fixture.blob_path(blob_id);
        let test_data = b"baa, baa";
        std::fs::write(&blob_path, test_data)?;
        acache
            .extend_local_availability(blob_id, &ByteRanges::single(0, test_data.len() as u64))?;

        // Read from blob
        let mut blob = fixture.acache.open_file(inode)?;
        blob.seek(SeekFrom::Start(20)).await?;
        let mut buf = [0; 100];
        let res = blob.read(&mut buf).await;
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(BlobIncomplete::matches(&err));

        Ok(())
    }

    #[tokio::test]
    async fn writing_to_blob_updates_local_availability() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let inode = fixture.add_file("test.txt", 21)?;

        // Write to blob
        let mut blob = fixture.acache.open_file(inode)?;
        let blob_id = blob.id();

        blob.write(b"baa, baa").await?;
        assert_eq!(ByteRanges::single(0, 8), *blob.local_availability());

        let blob_entry = fixture.get_blob_entry(blob_id)?;
        assert_eq!(ByteRanges::new(), blob_entry.written_areas);

        blob.write(b", black sheep").await?;
        assert_eq!(ByteRanges::single(0, 21), *blob.local_availability());

        let blob_entry = fixture.get_blob_entry(blob_id)?;
        assert_eq!(ByteRanges::new(), blob_entry.written_areas);

        blob.update_db().await?;

        // Get written areas from blob table after update
        let txn = fixture.begin_read()?;
        let blob_entry = get_blob_entry(&txn.blob_table()?, blob_id)?;
        assert_eq!(ByteRanges::single(0, 21), blob_entry.written_areas);
        drop(blob);

        assert_eq!(
            "baa, baa, black sheep",
            fs::read_to_string(fixture.blob_path(blob_id)).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn writing_to_blob_out_of_order() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let inode = fixture.add_file("test.txt", 21)?;

        // Write to blob
        let mut blob = fixture.acache.open_file(inode)?;
        let blob_id = blob.id();

        blob.seek(SeekFrom::Start(8)).await?;
        blob.write(b", black sheep").await?;

        blob.seek(SeekFrom::Start(0)).await?;
        blob.write(b"baa, baa").await?;

        blob.update_db().await?;

        let blob_entry = fixture.get_blob_entry(blob_id)?;
        assert_eq!(ByteRanges::single(0, 21), blob_entry.written_areas);
        drop(blob);

        assert_eq!(
            "baa, baa, black sheep",
            fs::read_to_string(fixture.blob_path(blob_id)).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn writing_to_blob_then_reading_it() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let inode = fixture.add_file("test.txt", 100)?;

        let mut blob = fixture.acache.open_file(inode)?;

        blob.write(b"baa, baa, black sheep").await?;

        blob.seek(SeekFrom::Start(10)).await?;

        let mut buf = [0; 100];
        let n = blob.read(&mut buf).await?;
        assert_eq!(b"black sheep", &buf[0..n]);

        Ok(())
    }

    #[tokio::test]
    async fn open_file_creates_sparse_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;
        let file_path = Path::parse("foobar")?;

        fixture.add_file_with_mtime(&file_path, 10000, test_time())?;
        let inode = acache.expect(&file_path)?;

        let blob_id = {
            let blob = acache.open_file(inode)?;
            assert_eq!(BlobId(1), blob.id());

            let m = fixture.blob_path(blob.id()).metadata()?;

            // File should have the right size
            assert_eq!(10000, m.len());

            // File should be sparse
            assert_eq!(0, m.blocks());

            // Range empty for now
            assert_eq!(ByteRanges::new(), *blob.local_availability());

            blob.id()
        };

        // If called a second time, it should return a handle on the same file.
        let blob = acache.open_file(inode)?;
        assert_eq!(blob_id, blob.id());
        assert_eq!(ByteRanges::new(), *blob.local_availability());

        Ok(())
    }

    #[tokio::test]
    async fn blob_deleted_on_file_overwrite() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;

        // Create a file
        fixture.add_file_with_mtime(&file_path, 100, test_time())?;

        // Open the file to create a blob
        let inode = acache.expect(&file_path)?;
        let blob_id = acache.open_file(inode)?.id();

        // Verify the blob file was created
        assert!(fixture.blob_file_exists(blob_id));
        // Verify the blob entry exists in the database
        assert!(fixture.blob_entry_exists(blob_id)?);

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
        assert!(!fixture.blob_file_exists(blob_id));
        // Verify the blob entry has been deleted from the database
        assert!(!fixture.blob_entry_exists(blob_id)?);

        Ok(())
    }

    #[tokio::test]
    async fn blob_deleted_on_file_removal() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;
        let file_path = Path::parse("file.txt")?;

        // Create a file
        fixture.add_file_with_mtime(&file_path, 100, test_time())?;

        // Open the file to create a blob
        let inode = acache.expect(&file_path)?;
        let blob_id = acache.open_file(inode)?.id();

        // Verify the blob file was created
        assert!(fixture.blob_file_exists(blob_id));
        // Verify the blob entry exists in the database
        assert!(fixture.blob_entry_exists(blob_id)?);

        // Remove the file
        fixture.remove_file(&file_path)?;

        // Verify the blob file has been deleted
        assert!(!fixture.blob_file_exists(blob_id));
        // Verify the blob entry has been deleted from the database
        assert!(!fixture.blob_entry_exists(blob_id)?);

        Ok(())
    }

    #[tokio::test]
    async fn blob_deleted_on_catchup_removal() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;

        // Create a file
        fixture.add_file_with_mtime(&file_path, 100, test_time())?;

        // Open the file to create a blob
        let inode = acache.expect(&file_path)?;
        let blob_id = acache.open_file(inode)?.id();

        // Verify the blob file was created
        assert!(fixture.blob_file_exists(blob_id));
        // Verify the blob entry exists in the database
        assert!(fixture.blob_entry_exists(blob_id)?);

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
        assert!(!fixture.blob_file_exists(blob_id));
        // Verify the blob entry has been deleted from the database
        assert!(!fixture.blob_entry_exists(blob_id)?);

        Ok(())
    }

    #[tokio::test]
    async fn blob_update_extends_range() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;
        let file_path = Path::parse("file.txt")?;

        // Create a file
        fixture.add_file_with_mtime(&file_path, 1000, test_time())?;

        // Open the file to create a blob
        let inode = acache.expect(&file_path)?;
        let blob_id = acache.open_file(inode)?.id();

        // Initially, the blob should have empty written areas
        let txn = fixture.begin_read()?;
        let initial_blob_entry = get_blob_entry(&txn.blob_table()?, blob_id)?;
        assert!(initial_blob_entry.written_areas.is_empty());

        // Update the blob with some written areas
        let written_areas = ByteRanges::from_ranges(vec![
            ByteRange::new(0, 100),
            ByteRange::new(200, 300),
            ByteRange::new(500, 600),
        ]);

        acache.extend_local_availability(blob_id, &written_areas)?;

        // Verify the written areas were updated
        let txn = fixture.begin_read()?;
        let retrieved_blob_entry = get_blob_entry(&txn.blob_table()?, blob_id)?;
        assert_eq!(retrieved_blob_entry.written_areas, written_areas);

        acache.extend_local_availability(
            blob_id,
            &ByteRanges::from_ranges(vec![ByteRange::new(50, 210), ByteRange::new(200, 400)]),
        )?;

        // Verify the ranges were updated again
        let txn = fixture.begin_read()?;
        let final_blob_entry = get_blob_entry(&txn.blob_table()?, blob_id)?;
        assert_eq!(
            final_blob_entry.written_areas,
            ByteRanges::from_ranges(vec![ByteRange::new(0, 400), ByteRange::new(500, 600)])
        );

        // Test that getting ranges for a non-existent blob returns NotFound
        assert!(matches!(
            get_blob_entry(&txn.blob_table()?, BlobId(99999)),
            Err(StorageError::NotFound)
        ));

        // Test that updating ranges for a non-existent blob returns NotFound
        assert!(matches!(
            acache.extend_local_availability(BlobId(99999), &ByteRanges::new()),
            Err(StorageError::NotFound)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn move_blob_success() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;
        let file_path = Path::parse("test.txt")?;

        // Create a file and add it to the cache
        fixture.add_file_with_mtime(&file_path, 100, test_time())?;

        // Open the file to create a blob
        let inode = acache.expect(&file_path)?;
        let blob_id = acache.open_file(inode)?.id();

        // Write some data to the blob and mark it as verified
        let mut blob = fixture.acache.open_file(inode)?;
        blob.write(b"test content").await?;
        blob.update_db().await?;
        blob.mark_verified().await?;

        // Verify the blob file exists and has content
        assert!(fixture.blob_file_exists(blob_id));
        assert!(fixture.blob_entry_exists(blob_id)?);

        // Create destination path
        let dest_path = fixture.tempdir.child("moved_blob").to_path_buf();

        // Move the blob
        let txn = fixture.begin_write()?;
        let result = acache.move_blob_if_matches(&txn, &file_path, &test_hash(), &dest_path)?;
        txn.commit()?;

        // Verify the move was successful
        assert!(result);

        // Verify the blob file has been moved
        assert!(!fixture.blob_file_exists(blob_id));
        assert!(dest_path.exists());

        // Verify the blob entry has been deleted from the database
        assert!(!fixture.blob_entry_exists(blob_id)?);

        // Verify the content was moved correctly
        let moved_content = std::fs::read_to_string(&dest_path)?;
        assert_eq!("test content", moved_content.trim_end_matches('\0'));

        // Verify the LRU queue has been updated properly.
        assert!(
            fixture
                .list_queue_content(LruQueueId::WorkingArea)?
                .is_empty()
        );

        Ok(())
    }

    #[tokio::test]
    async fn move_blob_wrong_hash() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;
        let file_path = Path::parse("test.txt")?;

        // Create a file and add it to the cache
        fixture.add_file_with_mtime(&file_path, 100, test_time())?;

        // Open the file to create a blob
        let inode = acache.expect(&file_path)?;
        let blob_id = acache.open_file(inode)?.id();

        // Write some data to the blob and mark it as verified
        let mut blob = fixture.acache.open_file(inode)?;
        blob.write(b"test content").await?;
        blob.update_db().await?;
        blob.mark_verified().await?;

        // Verify the blob file exists
        assert!(fixture.blob_file_exists(blob_id));
        assert!(fixture.blob_entry_exists(blob_id)?);

        // Create destination path
        let dest_path = fixture.tempdir.child("moved_blob").to_path_buf();

        // Try to move the blob with wrong hash
        let wrong_hash = Hash([2u8; 32]);
        let txn = fixture.begin_write()?;
        let result = acache.move_blob_if_matches(&txn, &file_path, &wrong_hash, &dest_path)?;
        txn.commit()?;

        // Verify the move was not successful
        assert!(!result);

        // Verify the blob file still exists
        assert!(fixture.blob_file_exists(blob_id));
        assert!(fixture.blob_entry_exists(blob_id)?);

        // Verify the destination file was not created
        assert!(!dest_path.exists());

        Ok(())
    }

    #[tokio::test]
    async fn move_blob_not_verified() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;
        let file_path = Path::parse("test.txt")?;

        // Create a file and add it to the cache
        fixture.add_file_with_mtime(&file_path, 100, test_time())?;

        // Open the file to create a blob
        let inode = acache.expect(&file_path)?;
        let blob_id = acache.open_file(inode)?.id();

        // Write some data to the blob but don't mark it as verified
        let mut blob = fixture.acache.open_file(inode)?;
        blob.write(b"test content").await?;
        blob.update_db().await?;
        // Note: Not calling mark_verified()

        // Verify the blob file exists
        assert!(fixture.blob_file_exists(blob_id));
        assert!(fixture.blob_entry_exists(blob_id)?);

        // Create destination path
        let dest_path = fixture.tempdir.child("moved_blob").to_path_buf();

        // Try to move the blob
        let txn = fixture.begin_write()?;
        let result = acache.move_blob_if_matches(&txn, &file_path, &test_hash(), &dest_path)?;
        txn.commit()?;

        // Verify the move was not successful (not verified)
        assert!(!result);

        // Verify the blob file still exists
        assert!(fixture.blob_file_exists(blob_id));
        assert!(fixture.blob_entry_exists(blob_id)?);

        // Verify the destination file was not created
        assert!(!dest_path.exists());

        Ok(())
    }

    #[tokio::test]
    async fn move_blob_nonexistent_blob() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;

        // Create destination path
        let dest_path = fixture.tempdir.child("moved_blob").to_path_buf();

        // Try to move a non-existent blob
        let non_existent_path = Path::parse("non_existent.txt")?;
        let txn = fixture.begin_write()?;
        let result =
            acache.move_blob_if_matches(&txn, &non_existent_path, &test_hash(), &dest_path);
        txn.commit()?;

        // Verify the move failed with NotFound error
        assert!(matches!(result, Err(StorageError::NotFound)));

        // Verify the destination file was not created
        assert!(!dest_path.exists());

        Ok(())
    }

    #[tokio::test]
    async fn move_blob_verifies_hash_and_state() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;
        let file_path = Path::parse("test.txt")?;

        // Create a file and add it to the cache
        fixture.add_file_with_mtime(&file_path, 34, test_time())?;

        // Open the file to create a blob
        let inode = acache.expect(&file_path)?;
        let blob_id = acache.open_file(inode)?.id();

        // Write some data to the blob and mark it as verified
        let mut blob = fixture.acache.open_file(inode)?;
        let content = b"test content for hash verification";
        blob.write(content).await?;
        blob.update_db().await?;
        blob.mark_verified().await?;

        // Get the actual hash of the content
        let actual_hash = blob.hash().clone();

        // Verify the blob file exists
        assert!(fixture.blob_file_exists(blob_id));
        assert!(fixture.blob_entry_exists(blob_id)?);

        // Create destination path
        let dest_path = fixture.tempdir.child("moved_blob").to_path_buf();

        // Move the blob with the correct hash
        let txn = fixture.begin_write()?;
        let result = acache.move_blob_if_matches(&txn, &file_path, &actual_hash, &dest_path)?;
        txn.commit()?;

        // Verify the move was successful
        assert!(result);

        // Verify the blob file has been moved
        assert!(!fixture.blob_file_exists(blob_id));
        assert!(dest_path.exists());

        // Verify the blob entry has been deleted from the database
        assert!(!fixture.blob_entry_exists(blob_id)?);

        // Verify the content was moved correctly
        assert_eq!(
            "test content for hash verification",
            std::fs::read_to_string(&dest_path)?
        );

        Ok(())
    }

    #[tokio::test]
    async fn move_into_blob_if_matches_creates_new_blob() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
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
        let blob_entry = fixture.get_blob_entry(blob.id())?;
        assert!(blob_entry.disk_usage > INODE_DISK_USAGE);

        Ok(())
    }

    #[tokio::test]
    async fn move_into_blob_if_matches_creates_new_protected_blob() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;
        let file_path = Path::parse("test.txt")?;

        let inode = fixture.add_file(file_path.as_str(), 3)?;
        acache.set_mark(&file_path, Mark::Keep)?;

        let tmpfile = fixture.tempdir.child("tmp");
        tmpfile.write_str("foo")?;

        let txn = fixture.begin_write()?;
        acache
            .move_into_blob_if_matches(&txn, &file_path, &test_hash(), &tmpfile.path().metadata()?)?
            .unwrap();
        txn.commit()?;

        // Since the file is marked keep, the blob that was created
        // must be in the protected queue.
        let blob_id = acache.open_file(inode)?.id();
        assert_eq!(
            LruQueueId::Protected,
            fixture.get_blob_entry(blob_id).unwrap().queue
        );

        Ok(())
    }

    #[tokio::test]
    async fn move_into_blob_if_matches_overwrites_existing_blob() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;
        let file_path = Path::parse("test.txt")?;

        // Create a file and add it to the cache
        let inode = fixture.add_file(file_path.as_str(), 59)?;

        // First, create a blob through normal open_file
        let existing_blob_id = acache.open_file(inode)?.id();

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
        assert_eq!(existing_blob_id, blob.id());

        let mut buf = String::new();
        blob.read_to_string(&mut buf).await?;
        assert_eq!(
            "What sane person could live in this world and not be crazy?".to_string(),
            buf
        );
        assert_eq!(ByteRanges::single(0, 59), *blob.local_availability());

        // Disk usage must be set.
        let blob_entry = fixture.get_blob_entry(blob.id())?;
        assert!(blob_entry.disk_usage > INODE_DISK_USAGE);

        Ok(())
    }

    #[tokio::test]
    async fn move_into_blob_if_matches_reject_wrong_file_size() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
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
        let fixture = Fixture::setup().await?;
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
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;

        let one = acache.open_file(fixture.add_file("one", 100)?)?.id();
        assert_eq!(
            (vec![one], vec![one]),
            (
                fixture.list_queue_content(LruQueueId::WorkingArea)?,
                fixture.list_queue_content_backward(LruQueueId::WorkingArea)?
            )
        );

        let two = acache.open_file(fixture.add_file("two", 100)?)?.id();
        assert_eq!(
            (vec![two, one], vec![one, two]),
            (
                fixture.list_queue_content(LruQueueId::WorkingArea)?,
                fixture.list_queue_content_backward(LruQueueId::WorkingArea)?
            )
        );

        let three = acache.open_file(fixture.add_file("three", 100)?)?.id();

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
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;

        let one = acache.open_file(fixture.add_file("one", 100)?)?.id();
        let _two = acache.open_file(fixture.add_file("two", 100)?)?.id();
        let three = acache.open_file(fixture.add_file("three", 100)?)?.id();
        let four = acache.open_file(fixture.add_file("four", 100)?)?.id();

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
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;

        let one = acache.open_file(fixture.add_file("one", 100)?)?.id();
        let two = acache.open_file(fixture.add_file("two", 100)?)?.id();
        let three = acache.open_file(fixture.add_file("three", 100)?)?.id();
        let four = acache.open_file(fixture.add_file("four", 100)?)?.id();

        // four is now at the head, being the most recent. Let's move
        // two there, then one..
        let blobstore = Blobstore::new(Arc::clone(&fixture.db), &fixture.blob_dir)?;
        blobstore.mark_accessed(two)?;
        assert_eq!(
            (vec![two, four, three, one], vec![one, three, four, two]),
            (
                fixture.list_queue_content(LruQueueId::WorkingArea)?,
                fixture.list_queue_content_backward(LruQueueId::WorkingArea)?
            )
        );

        blobstore.mark_accessed(one)?;
        assert_eq!(
            (vec![one, two, four, three], vec![three, four, two, one]),
            (
                fixture.list_queue_content(LruQueueId::WorkingArea)?,
                fixture.list_queue_content_backward(LruQueueId::WorkingArea)?
            )
        );

        blobstore.mark_accessed(one)?;
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
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;

        // Create a blob and verify initial disk usage is 0
        let inode = fixture.add_file("test.txt", 1000)?;
        let blob_id = acache.open_file(inode)?.id();

        let blob_entry = fixture.get_blob_entry(blob_id)?;
        // 512 bytes for an empty file, to account for inode usage.
        assert_eq!(INODE_DISK_USAGE, blob_entry.disk_usage);

        // Write some data to the blob
        let mut blob = acache.open_file(inode)?;
        blob.write(b"test data").await?;
        blob.update_db().await?;

        // disk usage is now 1 block
        let blksize = fs::metadata(fixture.blob_path(blob.id())).await?.blksize();
        let blob_entry = fixture.get_blob_entry(blob_id)?;
        assert_eq!(blob_entry.disk_usage, blksize + INODE_DISK_USAGE);

        assert_eq!(
            blob_entry.disk_usage,
            fixture.queue_disk_usage(LruQueueId::WorkingArea)?
        );

        Ok(())
    }

    #[tokio::test]
    async fn disk_usage_updates_on_blob_deletion() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;

        // Create a blob and write some data
        let inode = fixture.add_file("test.txt", 1000)?;
        let blob_id = acache.open_file(inode)?.id();

        let mut blob = acache.open_file(inode)?;
        blob.write(b"test data").await?;
        blob.update_db().await?;

        // Get initial disk usage
        let blob_entry = fixture.get_blob_entry(blob_id)?;
        assert!(blob_entry.disk_usage > 0);

        let initial_disk_usage = fixture.queue_disk_usage(LruQueueId::WorkingArea)?;
        assert_eq!(initial_disk_usage, blob_entry.disk_usage);

        // Delete the blob
        fixture.remove_file(&Path::parse("test.txt")?)?;

        // Verify blob entry is gone
        assert!(!fixture.blob_entry_exists(blob_id)?);

        assert_eq!(0, fixture.queue_disk_usage(LruQueueId::WorkingArea)?);

        Ok(())
    }

    #[tokio::test]
    async fn disk_usage_tracking_multiple_blobs() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;

        // Create multiple blobs and write data
        let inode1 = fixture.add_file("test1.txt", 1000)?;
        let inode2 = fixture.add_file("test2.txt", 2000)?;

        let blob_id1 = acache.open_file(inode1)?.id();
        let blob_id2 = acache.open_file(inode2)?.id();

        let blksize = fs::metadata(fixture.blob_path(blob_id1)).await?.blksize();

        // Write data to first blob
        let mut blob1 = acache.open_file(inode1)?;
        blob1.write(b"data for blob 1").await?;
        blob1.update_db().await?;

        // Write data to second blob
        let mut blob2 = acache.open_file(inode2)?;
        blob2.write(b"data for blob 2 which is longer").await?;
        blob2.update_db().await?;

        // Get individual disk usages
        let blob_entry1 = fixture.get_blob_entry(blob_id1)?;
        let blob_entry2 = fixture.get_blob_entry(blob_id2)?;

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
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;

        // Create multiple blobs with data
        let inode1 = fixture.add_file("test1.txt", 1000)?;
        let inode2 = fixture.add_file("test2.txt", 2000)?;
        let inode3 = fixture.add_file("test3.txt", 3000)?;
        let inode4 = fixture.add_file("test4.txt", 4000)?;

        let blob_id1 = acache.open_file(inode1)?.id();
        let blob_id2 = acache.open_file(inode2)?.id();
        let blob_id3 = acache.open_file(inode3)?.id();
        let blob_id4 = acache.open_file(inode4)?.id();

        // Write data to all blobs to create disk usage
        let mut blob1 = acache.open_file(inode1)?;
        blob1.write(b"data for blob 1").await?;
        blob1.update_db().await?;

        let mut blob2 = acache.open_file(inode2)?;
        blob2.write(b"data for blob 2 which is longer").await?;
        blob2.update_db().await?;

        let mut blob3 = acache.open_file(inode3)?;
        blob3.write(b"data for blob 3 which is even longer").await?;
        blob3.update_db().await?;

        let mut blob4 = acache.open_file(inode4)?;
        blob4
            .write(b"data for blob 4 which is the longest of all")
            .await?;
        blob4.update_db().await?;

        let initial_disk_usage = fixture.queue_disk_usage(LruQueueId::WorkingArea)?;
        assert!(initial_disk_usage > 0);

        // Verify all blobs exist
        assert!(fixture.blob_entry_exists(blob_id1)?);
        assert!(fixture.blob_entry_exists(blob_id2)?);
        assert!(fixture.blob_entry_exists(blob_id3)?);
        assert!(fixture.blob_entry_exists(blob_id4)?);
        assert!(fixture.blob_file_exists(blob_id1));
        assert!(fixture.blob_file_exists(blob_id2));
        assert!(fixture.blob_file_exists(blob_id3));
        assert!(fixture.blob_file_exists(blob_id4));

        // Clean up cache to target size (should remove some blobs)
        let target_size = initial_disk_usage / 2; // Remove half the blobs
        let blobstore = Blobstore::new(Arc::clone(&fixture.db), &fixture.blob_dir)?;
        blobstore.cleanup_cache(target_size)?;

        // Verify that some blobs were removed (the least recently used ones)
        // The queue order should be [4, 3, 2, 1] so 1 and 2 should be removed first
        assert!(!fixture.blob_entry_exists(blob_id1)?);
        assert!(!fixture.blob_entry_exists(blob_id2)?);
        assert!(fixture.blob_entry_exists(blob_id3)?);
        assert!(fixture.blob_entry_exists(blob_id4)?);
        assert!(!fixture.blob_file_exists(blob_id1));
        assert!(!fixture.blob_file_exists(blob_id2));
        assert!(fixture.blob_file_exists(blob_id3));
        assert!(fixture.blob_file_exists(blob_id4));

        // Verify the queue now only contains the remaining blobs
        assert_eq!(
            vec![blob_id4, blob_id3],
            fixture.list_queue_content(LruQueueId::WorkingArea)?
        );

        // Verify the disk usage is now at or below target
        assert!(fixture.queue_disk_usage(LruQueueId::WorkingArea)? <= target_size);

        Ok(())
    }

    #[tokio::test]
    async fn cleanup_cache_removes_all_blobs_when_target_is_zero() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;

        // Create a few blobs with data
        let inode1 = fixture.add_file("test1.txt", 1000)?;
        let inode2 = fixture.add_file("test2.txt", 2000)?;

        let blob_id1 = acache.open_file(inode1)?.id();
        let blob_id2 = acache.open_file(inode2)?.id();

        // Write data so there is something to cleanup
        let mut blob1 = acache.open_file(inode1)?;
        blob1.write(b"data for blob 1").await?;
        blob1.update_db().await?;
        drop(blob1);

        // Verify blobs exist
        assert!(fixture.blob_entry_exists(blob_id1)?);
        assert!(fixture.blob_entry_exists(blob_id2)?);
        assert!(fixture.blob_file_exists(blob_id1));
        assert!(fixture.blob_file_exists(blob_id2));

        // Clean up cache with target size 0 (should remove all blobs)
        let blobstore = Blobstore::new(Arc::clone(&fixture.db), &fixture.blob_dir)?;
        blobstore.cleanup_cache(0)?;

        // Verify all blobs were removed
        assert!(!fixture.blob_entry_exists(blob_id1)?);
        assert!(!fixture.blob_entry_exists(blob_id2)?);
        assert!(!fixture.blob_file_exists(blob_id1));
        assert!(!fixture.blob_file_exists(blob_id2));

        // Verify the queue is now empty
        assert_eq!(
            vec![] as Vec<BlobId>,
            fixture.list_queue_content(LruQueueId::WorkingArea)?
        );

        assert_eq!(0, fixture.queue_disk_usage(LruQueueId::WorkingArea)?);

        Ok(())
    }

    #[tokio::test]
    async fn cleanup_cache_does_nothing_when_already_under_target() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;

        // Create a blob with data
        let inode = fixture.add_file("test.txt", 1000)?;
        let blob_id = acache.open_file(inode)?.id();

        let mut blob = acache.open_file(inode)?;
        blob.write(b"data for blob").await?;
        blob.update_db().await?;

        let current_disk_usage = fixture.queue_disk_usage(LruQueueId::WorkingArea)?;

        // Verify blob exists
        assert!(fixture.blob_entry_exists(blob_id)?);
        assert!(fixture.blob_file_exists(blob_id));

        // Clean up cache with target size higher than current usage
        let target_size = current_disk_usage * 2;
        let blobstore = Blobstore::new(Arc::clone(&fixture.db), &fixture.blob_dir)?;
        blobstore.cleanup_cache(target_size)?;

        // Verify blob still exists (nothing should have been removed)
        assert!(fixture.blob_entry_exists(blob_id)?);
        assert!(fixture.blob_file_exists(blob_id));

        // Verify the queue still contains the blob
        assert_eq!(
            vec![blob_id],
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
        let fixture = Fixture::setup().await?;

        // Create blobstore without any blobs
        let blobstore = Blobstore::new(Arc::clone(&fixture.db), &fixture.blob_dir)?;

        // Clean up cache - should not error
        blobstore.cleanup_cache(1000)?;

        // Verify the queue is still empty by checking that no queue entry exists
        let txn = fixture.begin_read()?;
        let queue_table = txn.blob_lru_queue_table()?;
        let queue_entry = queue_table.get(LruQueueId::WorkingArea as u16)?;
        assert!(queue_entry.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn cleanup_cache_removes_blobs_in_lru_order() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;

        // Create blobs in order
        let inode1 = fixture.add_file("test1.txt", 1000)?;
        let inode2 = fixture.add_file("test2.txt", 1000)?;
        let inode3 = fixture.add_file("test3.txt", 1000)?;

        let blob_id1 = acache.open_file(inode1)?.id();
        let blob_id2 = acache.open_file(inode2)?.id();
        let blob_id3 = acache.open_file(inode3)?.id();

        // Write data to all blobs
        let mut blob1 = acache.open_file(inode1)?;
        blob1.write(b"data1").await?;
        blob1.update_db().await?;

        let mut blob2 = acache.open_file(inode2)?;
        blob2.write(b"data2").await?;
        blob2.update_db().await?;

        let mut blob3 = acache.open_file(inode3)?;
        blob3.write(b"data3").await?;
        blob3.update_db().await?;

        // Access blob1 to move it to the front (make it most recently used)
        let blobstore = Blobstore::new(Arc::clone(&fixture.db), &fixture.blob_dir)?;
        blobstore.mark_accessed(blob_id1)?;

        // Verify the order is now [1, 3, 2] (1 is most recent, 2 is least recent)
        assert_eq!(
            vec![blob_id1, blob_id3, blob_id2],
            fixture.list_queue_content(LruQueueId::WorkingArea)?
        );

        // Get current disk usage
        let current_disk_usage = fixture.queue_disk_usage(LruQueueId::WorkingArea)?;

        // Clean up cache to remove one blob (should remove blob_id2 as it's least recently used)
        let target_size = current_disk_usage - 1; // Just under current usage
        blobstore.cleanup_cache(target_size)?;

        // Verify blob_id2 was removed (least recently used)
        assert!(!fixture.blob_entry_exists(blob_id2)?);
        assert!(!fixture.blob_file_exists(blob_id2));

        // Verify blob_id1 and blob_id3 still exist
        assert!(fixture.blob_entry_exists(blob_id1)?);
        assert!(fixture.blob_entry_exists(blob_id3)?);
        assert!(fixture.blob_file_exists(blob_id1));
        assert!(fixture.blob_file_exists(blob_id3));

        // Verify the queue now contains only the remaining blobs in correct order
        assert_eq!(
            vec![blob_id1, blob_id3],
            fixture.list_queue_content(LruQueueId::WorkingArea)?
        );

        Ok(())
    }

    #[tokio::test]
    async fn cleanup_cache_skips_open_blobs() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;

        // Create a single blob with data
        let inode = fixture.add_file("test.txt", 1000)?;
        let blob_id = acache.open_file(inode)?.id();

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
        assert!(fixture.blob_entry_exists(blob_id)?);
        assert!(fixture.blob_file_exists(blob_id));

        // Drop all blob handles
        drop(blob);
        drop(_open_blob);

        // Now clean up cache again - should delete the blob
        acache.cleanup_cache(target_size)?;

        // Verify the blob was deleted
        assert!(!fixture.blob_entry_exists(blob_id)?);
        assert!(!fixture.blob_file_exists(blob_id));

        Ok(())
    }

    #[tokio::test]
    async fn cleanup_cache_deletes_blob_after_it_is_closed() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;

        // Create a blob with data
        let inode = fixture.add_file("test.txt", 1000)?;
        let blob_id = acache.open_file(inode)?.id();

        let mut blob = acache.open_file(inode)?;
        blob.write(b"data for blob").await?;
        blob.update_db().await?;

        // Keep the blob open
        let _open_blob = acache.open_file(inode)?;

        // Try to clean up cache - should skip the open blob
        let target_size = 0;
        acache.cleanup_cache(target_size)?;

        // Verify the blob still exists (it was open)
        assert!(fixture.blob_entry_exists(blob_id)?);
        assert!(fixture.blob_file_exists(blob_id));

        // Drop all blob handles
        drop(blob);
        drop(_open_blob);

        // Now clean up cache again - should delete the blob
        acache.cleanup_cache(target_size)?;

        // Verify the blob was deleted
        assert!(!fixture.blob_entry_exists(blob_id)?);
        assert!(!fixture.blob_file_exists(blob_id));

        Ok(())
    }

    #[tokio::test]
    async fn cleanup_cache_leaves_protected_blobs_alone() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;
        let inode = fixture.add_file("test.txt", 6)?;
        acache.set_mark(&Path::parse("test.txt")?, Mark::Keep)?;

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
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;

        // Create a blob with data
        let inode = fixture.add_file("test.txt", 1000)?;
        let blob_id = acache.open_file(inode)?.id();

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
        assert!(fixture.blob_entry_exists(blob_id)?);
        assert!(fixture.blob_file_exists(blob_id));

        // Drop one handle
        drop(open_blob1);

        // Try to clean up again - should still skip (2 handles remain)
        acache.cleanup_cache(target_size)?;

        // Verify the blob still exists
        assert!(fixture.blob_entry_exists(blob_id)?);
        assert!(fixture.blob_file_exists(blob_id));

        // Drop another handle
        drop(open_blob2);

        // Try to clean up again - should still skip (1 handle remains)
        acache.cleanup_cache(target_size)?;

        // Verify the blob still exists
        assert!(fixture.blob_entry_exists(blob_id)?);
        assert!(fixture.blob_file_exists(blob_id));

        // Drop the last handle
        drop(open_blob3);

        // Drop the initial blob handle as well
        drop(blob);

        // Now clean up cache - should delete the blob
        acache.cleanup_cache(target_size)?;

        // Verify the blob was deleted
        assert!(!fixture.blob_entry_exists(blob_id)?);
        assert!(!fixture.blob_file_exists(blob_id));

        Ok(())
    }

    #[tokio::test]
    async fn reference_counting_with_mixed_open_and_closed_blobs() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;

        // Create multiple blobs
        let inode1 = fixture.add_file("test1.txt", 1000)?;
        let inode2 = fixture.add_file("test2.txt", 2000)?;
        let inode3 = fixture.add_file("test3.txt", 3000)?;

        let blob_id1 = acache.open_file(inode1)?.id();
        let blob_id2 = acache.open_file(inode2)?.id();
        let blob_id3 = acache.open_file(inode3)?.id();

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

        // Keep blob_id1 and blob_id3 open, but close blob_id2
        let _open_blob1 = acache.open_file(inode1)?;
        let _open_blob3 = acache.open_file(inode3)?;
        drop(blob2); // Close blob_id2

        // Clean up cache with target size 0 - should remove blob_id2 but keep blob_id1 and blob_id3
        acache.cleanup_cache(0)?;

        // Verify that blob_id1 and blob_id3 still exist (they were open)
        assert!(fixture.blob_entry_exists(blob_id1)?);
        assert!(fixture.blob_entry_exists(blob_id3)?);
        assert!(fixture.blob_file_exists(blob_id1));
        assert!(fixture.blob_file_exists(blob_id3));

        // Verify that blob_id2 was removed (it was closed)
        assert!(!fixture.blob_entry_exists(blob_id2)?);
        assert!(!fixture.blob_file_exists(blob_id2));

        // Drop all remaining blob handles
        drop(blob1);
        drop(blob3);
        drop(_open_blob1);
        drop(_open_blob3);

        Ok(())
    }

    #[tokio::test]
    async fn blob_ids_never_reused_after_deletion() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;

        // Create a file and get its blob ID
        let inode = fixture.add_file("test.txt", 100)?;
        let first_blob_id = acache.open_file(inode)?.id();
        assert_eq!(BlobId(1), first_blob_id);

        // Verify the blob exists
        assert!(fixture.blob_entry_exists(first_blob_id)?);
        assert!(fixture.blob_file_exists(first_blob_id));

        // Delete the file (which deletes the blob)
        fixture.remove_file(&Path::parse("test.txt")?)?;

        // Verify the blob is gone
        assert!(!fixture.blob_entry_exists(first_blob_id)?);
        assert!(!fixture.blob_file_exists(first_blob_id));

        // Create a new file and get its blob ID
        let inode2 = fixture.add_file("test2.txt", 200)?;
        let second_blob_id = acache.open_file(inode2)?.id();

        // Verify the new blob ID is different from the deleted one
        assert_ne!(first_blob_id, second_blob_id);
        assert_eq!(BlobId(2), second_blob_id);

        // Verify the new blob exists
        assert!(fixture.blob_entry_exists(second_blob_id)?);
        assert!(fixture.blob_file_exists(second_blob_id));

        Ok(())
    }

    #[tokio::test]
    async fn blob_next_id_table_tracks_allocation_correctly() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        // Initially, the next ID table should be empty
        let txn = fixture.begin_read()?;
        let next_id_table = txn.blob_next_id_table()?;
        assert!(next_id_table.get(())?.is_none());

        // Create a blob and verify the next ID is updated
        let inode = fixture.add_file("test.txt", 100)?;
        let blob_id = fixture.acache.open_file(inode)?.id();
        assert_eq!(BlobId(1), blob_id);

        // Check that the next ID table now contains the next ID to allocate
        let txn = fixture.begin_read()?;
        let next_id_table = txn.blob_next_id_table()?;
        let next_id = next_id_table.get(())?.unwrap().value();
        assert_eq!(BlobId(1), next_id); // The table stores the last allocated ID

        // Create another blob and verify the next ID is incremented
        let inode2 = fixture.add_file("test2.txt", 200)?;
        let blob_id2 = fixture.acache.open_file(inode2)?.id();
        assert_eq!(BlobId(2), blob_id2);

        // Check that the next ID table is updated again
        let txn = fixture.begin_read()?;
        let next_id_table = txn.blob_next_id_table()?;
        let next_id = next_id_table.get(())?.unwrap().value();
        assert_eq!(BlobId(2), next_id); // The table stores the last allocated ID

        Ok(())
    }

    #[tokio::test]
    async fn open_file_handles_blob_deleted_by_cleanup_cache() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;

        // 1. Add a file
        let inode = fixture.add_file("test.txt", 100)?;

        // 2. Call open_file() and drop the returned blob
        {
            let mut blob = acache.open_file(inode)?;
            let blob_id = blob.id();
            assert!(fixture.blob_entry_exists(blob_id)?);
            assert!(fixture.blob_file_exists(blob_id));

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
        let blob_id = BlobId(1); // First blob should have ID 1
        assert!(!fixture.blob_entry_exists(blob_id)?);
        assert!(!fixture.blob_file_exists(blob_id));

        // 4. Call open_file() and make sure the returned blob can be written to normally
        let mut new_blob = acache.open_file(inode)?;

        // It's not actually a new blob; the id is reused
        assert_eq!(blob_id, new_blob.id());

        // Verify the new blob exists and it has been added to the LRU queue.
        assert!(fixture.blob_entry_exists(blob_id)?);
        assert!(fixture.blob_file_exists(blob_id));
        assert_eq!(
            vec![blob_id],
            fixture.list_queue_content(LruQueueId::WorkingArea)?
        );

        // Test that we can write to the new blob normally
        let test_data = b"Hello, world!";
        let bytes_written = new_blob.write(test_data).await?;
        assert_eq!(test_data.len(), bytes_written);

        Ok(())
    }
}
