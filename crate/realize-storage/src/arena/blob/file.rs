#![allow(dead_code)] // WIP
use super::BlobInfo;
use crate::arena::db::ArenaDatabase;
use crate::{CacheStatus, PathId, StorageError};
use realize_types::{ByteRange, ByteRanges, Hash};
use std::io::SeekFrom;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::{Arc, Weak};
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{Mutex, MutexGuard, watch};
use weak_table::WeakValueHashMap;

/// Interval at which to update the database during downloads, in
/// bytes.
const UPDATE_DB_INTERVAL_BYTES: u64 = 4 * 1024 * 1024; // 4M

/// Registry of open [BlobFile].
///
/// The [BlobFile] instances in this registry are kept only as long as
/// there are references to it outside of the registry.
pub(crate) struct BlobFileRegistry {
    files: std::sync::Mutex<WeakValueHashMap<BlobFileId, Weak<SharedBlobFile>>>,
}

impl BlobFileRegistry {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            files: std::sync::Mutex::new(WeakValueHashMap::new()),
        })
    }

    /// Get a [BlobFile] for a blob, given the blob file `path` and
    /// its [BlobInfo].
    ///
    /// The caller is responsible for making sure that file at `path`
    /// exists and that it has the right size.
    pub(crate) fn get(
        &self,
        db: &Arc<ArenaDatabase>,
        path: &std::path::Path,
        info: &BlobInfo,
    ) -> Result<Arc<SharedBlobFile>, StorageError> {
        let id = BlobFileId::from(&path.metadata()?);
        let mut guard = self.files.lock().unwrap();
        let blob_file = guard
            .entry(id.clone())
            .or_insert_with(|| SharedBlobFile::new(id, db, path, info));

        Ok(blob_file)
    }

    /// If any [SharedBlobFile] exists for the given file, realize it.
    pub(crate) fn realize_blocking(&self, m: &std::fs::Metadata) {
        let guard = self.files.lock().unwrap();
        if let Some(blob_file) = guard.get(&BlobFileId::from(m)) {
            blob_file.realize_blocking();
        }
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub(crate) struct BlobFileId {
    dev: u64,
    ino: u64,
}

impl BlobFileId {
    pub(crate) fn from(m: &std::fs::Metadata) -> Self {
        Self {
            dev: m.dev(),
            ino: m.ino(),
        }
    }

    /// Checks whether the given `path` matches this ID.
    pub(crate) fn matches(&self, m: Result<std::fs::Metadata, std::io::Error>) -> bool {
        match m {
            Ok(m) => *self == BlobFileId::from(&m),
            Err(_) => false,
        }
    }
}

/// Instance that keeps track of state shared between different [Blob]
/// file handle with the same underlying file.
///
/// This type makes sure that updates, verifications, repair and
/// realize happen at the right time and do not interfere with each
/// other.
///
/// [BlobFile] keeps track of the readable portion of the underlying
/// file, exposed as a [ReadableRange] available through
/// [BlobFile::subscribe]. The readable range can be expanded, but not
/// shrunk, so the portion of the file that is readable will continue
/// to be readable no matter what. The portion of the file that is not
/// readable, on the other hand, might become readable at any time.
pub(crate) struct SharedBlobFile {
    id: BlobFileId,
    db: Arc<ArenaDatabase>,
    path: PathBuf,
    pathid: PathId,
    size: u64,
    hash: Hash,
    tx_readable: watch::Sender<ReadableRange>,
    guarded: Mutex<BlobFileGuarded>,
}
struct BlobFileGuarded {
    available_ranges: ByteRanges,
    pending_ranges: ByteRanges,
    bytes_since_last_update: u64,
    state: BlobFileState,
    fh: UnderlyingFile,
}

/// A handle of the file use for update and repair.
enum UnderlyingFile {
    /// The file hasn't been opened yet.
    Unset,
    /// The file handle is available.
    Open(File),
    /// No file is available for update and repair anymore; it has
    /// been deleted or moved.
    Gone,
}

#[derive(Copy, Eq, PartialEq, Clone, Debug)]
enum BlobFileState {
    Updatable,
    Complete,
    Verified,
    Realized,
}

/// Byte ranges that can safely be read in the blob file.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum ReadableRange {
    /// Reads must be limited to the given range.
    Limited(ByteRanges),

    /// Reads must not be limited.
    Direct,
}

impl SharedBlobFile {
    fn new(
        id: BlobFileId,
        db: &Arc<ArenaDatabase>,
        path: &std::path::Path,
        info: &BlobInfo,
    ) -> Arc<Self> {
        let state: BlobFileState;
        let readable_range: ReadableRange;
        if info
            .available_ranges
            .contains_range(&ByteRange::new(0, info.size))
        {
            if info.verified {
                state = BlobFileState::Verified;
            } else {
                state = BlobFileState::Complete;
            }
            readable_range = ReadableRange::Direct;
        } else {
            state = BlobFileState::Updatable;
            readable_range = ReadableRange::Limited(info.available_ranges.clone());
        };
        let (tx_readable, _) = watch::channel(readable_range);
        Arc::new(Self {
            id,
            db: Arc::clone(db),
            path: path.to_path_buf(),
            pathid: info.pathid,
            size: info.size,
            hash: info.hash.clone(),
            guarded: Mutex::new(BlobFileGuarded {
                state,
                available_ranges: info.available_ranges.clone(),
                pending_ranges: ByteRanges::new(),
                bytes_since_last_update: 0,
                fh: UnderlyingFile::Unset,
            }),
            tx_readable,
        })
    }

    pub(crate) fn id(&self) -> &BlobFileId {
        &self.id
    }

    pub(crate) fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Watch the [ReadableRange] for this blob file.
    pub(crate) fn subscribe(&self) -> watch::Receiver<ReadableRange> {
        self.tx_readable.subscribe()
    }

    /// Returns the [CacheStatus] from internal state.
    ///
    /// The partial status includes the synced range, as reported by
    /// [SharedBlobFile::available_ranges], so it might be a good idea
    /// to call [SharedBlobFile::update_db] first to update it.
    ///
    /// None indicates that the blob has been realized, that is, taken
    /// out of the cache.
    pub(crate) async fn cache_status(&self) -> Option<CacheStatus> {
        let guard = self.guarded.lock().await;
        match &guard.state {
            BlobFileState::Updatable => {
                if guard.available_ranges.is_empty() {
                    Some(CacheStatus::Missing)
                } else {
                    Some(CacheStatus::Partial(
                        self.size,
                        guard.available_ranges.clone(),
                    ))
                }
            }
            BlobFileState::Complete => Some(CacheStatus::Complete),
            BlobFileState::Verified => Some(CacheStatus::Verified),
            BlobFileState::Realized => None,
        }
    }

    /// Update the content of this blob file.
    ///
    /// Does nothing if the given range has already been updated. Use
    /// [BlobFile::repair] if a written range needs to be fixed.
    ///
    /// Note that updates written to the file might still be left in
    /// cache when the [BlobFile] is dropped; this is similar to what
    /// happens to data written to [tokio::fs::File]. Call
    /// [BlobFile::update_db] before dropping an incomplete file to
    /// avoid losing data.
    ///
    /// Fails with [StorageError::InvalidBlobState] if the file is not in
    /// a state that allows updates.
    pub(crate) async fn update(&self, offset: u64, buf: &[u8]) -> Result<(), StorageError> {
        let mut guard = self.guarded.lock().await;
        if guard.state != BlobFileState::Updatable {
            return Err(StorageError::InvalidBlobState);
        }
        if buf.len() == 0 {
            return Ok(());
        }
        let start = offset;
        let end = start + buf.len() as u64;
        let range = ByteRange::new(start, end);
        if guard.full_available_range().contains_range(&range) {
            return Ok(());
        }

        let file = self.underlying_file(&mut guard).await?;
        file.seek(SeekFrom::Start(offset)).await?;
        file.write_all(buf).await?;

        guard.pending_ranges.add(&range);
        guard.bytes_since_last_update += range.bytecount();
        if guard.bytes_since_last_update >= UPDATE_DB_INTERVAL_BYTES || {
            guard
                .full_available_range()
                .contains_range(&ByteRange::new(0, self.size))
        } {
            self.update_db_internal(guard).await?;
        }
        Ok(())
    }

    /// Store any range updates to the database.
    ///
    /// Calls in an invalid state or calls made when there are no
    /// pending ranges do nothing, so this is safe to call from any
    /// states.
    pub(crate) async fn update_db(&self) -> Result<(), StorageError> {
        let guard = self.guarded.lock().await;
        if guard.state != BlobFileState::Updatable || guard.pending_ranges.is_empty() {
            return Ok(());
        }

        self.update_db_internal(guard).await?;

        Ok(())
    }

    /// Check the file state and prepare it for verification.
    ///
    /// This must be called before starting the verification process.
    pub(crate) async fn prepare_for_verification(&self) -> Result<(), StorageError> {
        let mut guard = self.guarded.lock().await;
        if guard.state != BlobFileState::Complete {
            return Err(StorageError::InvalidBlobState);
        }
        // Any updates would have been flushed when reaching the state
        // complete, but repairs might not have been.
        flush_and_sync(&mut guard).await?;

        Ok(())
    }

    /// Marks the file as having been verified.
    ///
    /// Fails with [StorageError::InvalidBlobState] if the file is not in
    /// a state that allows verification.
    pub(crate) async fn mark_verified(&self) -> Result<(), StorageError> {
        let mut guard = self.guarded.lock().await;
        match guard.state {
            BlobFileState::Verified => Ok(()),
            BlobFileState::Complete => {
                let pathid = self.pathid;
                let db = self.db.clone();
                let hash = self.hash.clone();
                let join_handle = tokio::task::spawn_blocking(move || {
                    let txn = db.begin_write()?;
                    {
                        let mut blobs = txn.write_blobs()?;
                        if !blobs.mark_verified(
                            &mut txn.read_tree()?,
                            &mut txn.write_dirty()?,
                            pathid,
                            &hash,
                        )? {
                            return Ok(());
                        }
                    }
                    txn.commit()?;
                    Ok::<_, StorageError>(())
                });
                guard.state = BlobFileState::Verified;
                flush_and_sync(&mut guard).await?;
                guard.fh = UnderlyingFile::Unset; // Not needed anymore

                // It's important not to wait for the block to
                // return while holding the guard, as that would
                // create a lock loops between self.guarded and the
                // write transaction mutex.
                drop(guard);
                join_handle.await??;
                Ok(())
            }
            _ => Err(StorageError::InvalidBlobState),
        }
    }

    /// Repair a portion of the file.
    ///
    /// The file must be complete and not yet verified to be
    /// repairable.
    ///
    /// Fails with [StorageError::InvalidBlobState] if the file is not in
    /// a state that allows verification.
    pub(crate) async fn repair(&self, offset: u64, buf: &[u8]) -> Result<(), StorageError> {
        let mut guard = self.guarded.lock().await;
        if guard.state != BlobFileState::Complete {
            return Err(StorageError::InvalidBlobState);
        }
        if buf.len() == 0 {
            return Ok(());
        }
        let file = self.underlying_file(&mut guard).await?;
        file.seek(SeekFrom::Start(offset)).await?;
        file.write_all(buf).await?;

        Ok(())
    }

    /// Realize the file, that is, turn it from a blob into a normal
    /// file.
    ///
    /// This can happen at any time. It's the responsibility of the
    /// caller to make sure that the relevant portion of the file
    /// contains the appropriate data before calling this function.
    ///
    /// Returns true if the file was realized by this operation and
    /// false if it was already realized.
    pub(crate) fn realize_blocking(&self) -> bool {
        let mut guard = self.guarded.blocking_lock();
        self.realize_internal(&mut guard)
    }

    fn realize_internal(&self, guard: &mut MutexGuard<'_, BlobFileGuarded>) -> bool {
        if guard.state == BlobFileState::Realized {
            return false;
        }
        guard.state = BlobFileState::Realized;
        guard.fh = UnderlyingFile::Unset;
        let _ = self.tx_readable.send(ReadableRange::Direct);

        true
    }

    /// Return a handle on the underlying file, open it if necessary.
    ///
    /// It might not be possible to open the file anymore, because it
    /// has been deleted or replaced. In such case, the blob switches
    /// to realized state and the calling operation returns [StorageError::InvalidBlobState].
    async fn underlying_file<'a>(
        &self,
        guard: &'a mut MutexGuard<'_, BlobFileGuarded>,
    ) -> Result<&'a mut tokio::fs::File, StorageError> {
        if matches!(guard.fh, UnderlyingFile::Unset) {
            match File::options()
                .write(true)
                .create(false)
                .truncate(false)
                .open(&self.path)
                .await
            {
                Ok(file) => {
                    if self.id != BlobFileId::from(&file.metadata().await?) {
                        // File has been deleted or moved. Update or
                        // repair is impossible, but reading is still
                        // possible for open handles.
                        guard.fh = UnderlyingFile::Gone;
                        return Err(StorageError::NotFound);
                    }
                    guard.fh = UnderlyingFile::Open(file);
                }
                Err(err) => {
                    if err.kind() == std::io::ErrorKind::NotFound {
                        guard.fh = UnderlyingFile::Gone;
                        return Err(StorageError::NotFound);
                    }
                    return Err(err.into());
                }
            }
        }
        if let UnderlyingFile::Open(file) = &mut guard.fh {
            Ok(file)
        } else {
            Err(StorageError::NotFound)
        }
    }

    async fn update_db_internal(
        &self,
        mut guard: MutexGuard<'_, BlobFileGuarded>,
    ) -> Result<(), StorageError> {
        if guard.pending_ranges.is_empty() {
            return Ok(());
        }
        flush_and_sync(&mut guard).await?;

        let pathid = self.pathid;
        let ranges = guard.pending_ranges.clone();
        let db = Arc::clone(&self.db);
        let hash = self.hash.clone();

        let join_handle = tokio::task::spawn_blocking(move || {
            let txn = db.begin_write()?;
            txn.write_blobs()?.extend_cache_status(
                &mut txn.read_tree()?,
                pathid,
                &hash,
                &ranges,
            )?;
            txn.commit()?;

            Ok::<_, StorageError>(())
        });

        let pending = std::mem::take(&mut guard.pending_ranges);
        guard.available_ranges.extend(pending);
        if guard
            .available_ranges
            .contains_range(&ByteRange::new(0, self.size))
        {
            guard.state = BlobFileState::Complete;
            let _ = self.tx_readable.send(ReadableRange::Direct);
        } else {
            let _ = self
                .tx_readable
                .send(ReadableRange::Limited(guard.available_ranges.clone()));
        }

        // It's important not to wait for the block to
        // return while holding the guard, as that would
        // create a lock loops between self.guarded and the
        // write transaction mutex.
        drop(guard);
        join_handle.await??;

        Ok(())
    }

    /// Make changes to the database.
    ///
    /// It's important *not* to wait for the spawn_blocking to return,
    /// as that would create a lock loops between self.guarded and the
    /// write transaction mutex. It does mean that SharedBlobFile
    /// might have more up-to-date information than what's on the
    /// database - temporarily unless the database has problems.
    fn write_to_db(
        &self,
        cb: impl FnOnce(&Arc<ArenaDatabase>) -> Result<(), StorageError> + Send + 'static,
    ) {
        let db = Arc::clone(&self.db);
        let pathid = self.pathid;
        tokio::task::spawn_blocking(move || {
            if let Err(err) = cb(&db) {
                log::debug!("Updating database for blob {pathid} failed: {err}")
            }
        });
    }
}

async fn flush_and_sync(guard: &mut MutexGuard<'_, BlobFileGuarded>) -> Result<(), StorageError> {
    Ok(if let UnderlyingFile::Open(fh) = &mut guard.fh {
        fh.flush().await?;
        fh.sync_all().await?;
    })
}

impl BlobFileGuarded {
    fn full_available_range(&self) -> ByteRanges {
        self.available_ranges.union(&self.pending_ranges)
    }
}
