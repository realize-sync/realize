use super::db::{ArenaDatabase, ArenaReadTransaction, ArenaWriteTransaction};
use super::hasher::hash_file;
use super::types::{BlobTableEntry, LocalAvailability, LruQueueId};
use crate::StorageError;
use crate::global::types::FileTableEntry;
use crate::types::{BlobId, Inode};
use crate::utils::holder::Holder;
use realize_types::{ByteRange, ByteRanges, Hash};
use redb::ReadableTable;
use std::cmp::min;
use std::io::{SeekFrom, Write};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt as _, ReadBuf};

/// A blob store that handles blob-specific operations.
///
/// This struct contains blob-specific logic and database operations.
pub(crate) struct Blobstore {
    db: Arc<ArenaDatabase>,
    blob_dir: PathBuf,
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
        }))
    }

    /// Return an [Blob] entry given a blob id.
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
        inode: Inode,
        txn: &ArenaWriteTransaction,
        file_entry: FileTableEntry,
    ) -> Result<Blob, StorageError> {
        let mut blob_table = txn.blob_table()?;
        let (blob_id, blob_entry) = if let Some(blob_id) = file_entry.content.blob {
            let blob_entry = get_blob_entry(&blob_table, blob_id)?;

            (blob_id, blob_entry)
        } else {
            let (blob_id, blob_entry) = do_create_blob_entry(&mut blob_table)?;
            log::debug!(
                "assigned blob {blob_id} to file {inode} {}",
                file_entry.content.hash
            );

            (blob_id, blob_entry)
        };
        let file = self.open_blob_file(blob_id, file_entry.metadata.size, true)?;
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
        blob_table.remove(blob_id)?;
        let blob_path = self.blob_path(blob_id);
        if blob_path.exists() {
            std::fs::remove_file(&blob_path)?;
        }

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
            let mut blob_entry = get_blob_entry(&blob_table, blob_id)?;
            blob_entry.written_areas = blob_entry.written_areas.union(&new_range);
            log::debug!(
                "{blob_id} extended by {new_range}; available: {}",
                blob_entry.written_areas
            );

            blob_table.insert(blob_id, Holder::with_content(blob_entry)?)?;
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

        blob_table.remove(blob_id)?;
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
        blob_id: Option<BlobId>,
        size: u64,
    ) -> Result<(BlobId, PathBuf), StorageError> {
        let mut blob_table = txn.blob_table()?;
        let (blob_id, mut entry) = match blob_id {
            Some(blob_id) => {
                let entry = get_blob_entry(&blob_table, blob_id)?;

                (blob_id, entry)
            }
            None => do_create_blob_entry(&mut blob_table)?,
        };
        let dest = self.blob_path(blob_id);
        entry.written_areas = ByteRanges::single(0, size);
        blob_table.insert(blob_id, Holder::new(&entry)?)?;

        Ok((blob_id, dest))
    }
}

fn do_create_blob_entry(
    blob_table: &mut redb::Table<'_, BlobId, Holder<'static, BlobTableEntry>>,
) -> Result<(BlobId, BlobTableEntry), StorageError> {
    let blob_id = blob_table
        .last()?
        .map(|(k, _)| k.value().plus(1))
        .unwrap_or(BlobId(1));
    let blob_entry = BlobTableEntry {
        written_areas: ByteRanges::new(),
        content_hash: None,
        queue: LruQueueId::WorkingArea,
        next: None,
        prev: None,
        disk_usage: 0,
    };
    blob_table.insert(blob_id, Holder::new(&blob_entry)?)?;
    Ok((blob_id, blob_entry))
}

pub(crate) fn local_availability(
    txn: &ArenaReadTransaction,
    file_entry: &FileTableEntry,
) -> Result<LocalAvailability, StorageError> {
    match file_entry.content.blob {
        None => Ok(LocalAvailability::Missing),
        Some(blob_id) => {
            let blob_table = txn.blob_table()?;
            let blob_entry = get_blob_entry(&blob_table, blob_id)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arena::arena_cache::ArenaCache;
    use crate::utils::redb_utils;
    use crate::{DirtyPaths, GlobalDatabase, Inode, InodeAllocator, Notification};
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
            )?;

            let (inode, _) = self.acache.lookup_path(&path)?;

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
        let (inode, _) = acache.lookup_path(&file_path)?;

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
        let (inode, _) = acache.lookup_path(&file_path)?;
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
        let (inode, _) = acache.lookup_path(&file_path)?;
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
        let (inode, _) = acache.lookup_path(&file_path)?;
        let blob_id = acache.open_file(inode)?.id();

        // Verify the blob file was created
        assert!(fixture.blob_file_exists(blob_id));
        // Verify the blob entry exists in the database
        assert!(fixture.blob_entry_exists(blob_id)?);

        // Do a catchup that doesn't include this file (simulating file removal)
        acache.update(test_peer(), Notification::CatchupStart(arena))?;
        // Note: No Catchup notification for the file, so it will be deleted
        acache.update(
            test_peer(),
            Notification::CatchupComplete {
                arena: arena,
                index: 0,
            },
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
        let (inode, _) = acache.lookup_path(&file_path)?;
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
        let (inode, _) = acache.lookup_path(&file_path)?;
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
        let (inode, _) = acache.lookup_path(&file_path)?;
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
        let (inode, _) = acache.lookup_path(&file_path)?;
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
        let (inode, _) = acache.lookup_path(&file_path)?;
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

        // Use move_into_blob_if_matches to get a path to write to
        let txn = fixture.begin_write()?;
        let path_in_cache = acache
            .move_into_blob_if_matches(&txn, &file_path, &test_hash())?
            .unwrap();
        txn.commit()?;

        // Write some test data to the returned file path
        std::fs::write(
            &path_in_cache,
            b"When you light a candle, you also cast a shadow.",
        )?;

        // Now open the file through the normal open_file mechanism
        let mut blob = acache.open_file(inode)?;
        let mut buf = String::new();
        blob.read_to_string(&mut buf).await?;
        assert_eq!(
            "When you light a candle, you also cast a shadow.".to_string(),
            buf
        );
        assert_eq!(ByteRanges::single(0, 48), *blob.local_availability());

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
        let txn = fixture.begin_write()?;
        let dest_path = acache
            .move_into_blob_if_matches(&txn, &file_path, &test_hash())?
            .unwrap();
        txn.commit()?;

        // Write test data to the returned file path
        std::fs::write(
            &dest_path,
            b"What sane person could live in this world and not be crazy?",
        )?;

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

        Ok(())
    }

    #[tokio::test]
    async fn move_into_blob_if_matches_rejects_wrong_hash() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;
        let file_path = Path::parse("test.txt")?;

        // Create a file and add it to the cache
        fixture.add_file(file_path.as_str(), 10)?;

        let txn = fixture.begin_write()?;
        // hash is test_hash(), which is not [99; 32]
        assert_eq!(
            None,
            acache.move_into_blob_if_matches(&txn, &file_path, &Hash([99; 32]))?
        );

        Ok(())
    }
}
