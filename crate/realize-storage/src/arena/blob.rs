use super::db::{ArenaDatabase, ArenaReadTransaction, ArenaWriteTransaction};
use super::types::BlobTableEntry;
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
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, AsyncWriteExt as _, ReadBuf};

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
        blob_dir: PathBuf,
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

        Ok(Arc::new(Self { db, blob_dir }))
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
            let blob_id = blob_table
                .last()?
                .map(|(k, _)| k.value())
                .unwrap_or(BlobId(1));
            log::debug!(
                "assigned blob {blob_id} to file {inode} {}",
                file_entry.content.hash
            );
            let blob_entry = BlobTableEntry {
                written_areas: ByteRanges::new(),
            };

            (blob_id, blob_entry)
        };
        let file = self.open_blob_file(blob_id, file_entry.metadata.size, true)?;
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
    pub(crate) fn delete_blob(&self, blob_id: BlobId) -> Result<(), StorageError> {
        let blob_path = self.blob_path(blob_id);
        if blob_path.exists() {
            std::fs::remove_file(&blob_path)?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn local_availability(&self, blob_id: BlobId) -> Result<ByteRanges, StorageError> {
        let txn = self.db.begin_read()?;
        local_availability(&txn, blob_id)
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
}

pub(crate) fn local_availability(
    txn: &ArenaReadTransaction,
    blob_id: BlobId,
) -> Result<ByteRanges, StorageError> {
    let blob_table = txn.blob_table()?;

    Ok(get_blob_entry(&blob_table, blob_id)?.written_areas)
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

    pub async fn update_db(&mut self) -> Result<(), StorageError> {
        if self.pending_ranges.is_empty() {
            return Ok(());
        }
        self.file.flush().await?;
        self.file.sync_all().await?;

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
    use crate::global::cache::UnrealCacheBlocking;
    use crate::utils::redb_utils;
    use crate::{DirtyPaths, Inode, Notification, UnrealCacheAsync};
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
        async_cache: UnrealCacheAsync,
        cache: Arc<UnrealCacheBlocking>,
        tempdir: TempDir,
    }
    impl Fixture {
        async fn setup_with_arena(arena: Arena) -> anyhow::Result<Fixture> {
            let _ = env_logger::try_init();
            let tempdir = TempDir::new()?;
            let mut cache = UnrealCacheBlocking::new(redb_utils::in_memory()?)?;

            let child = tempdir.child(format!("{arena}-cache.db"));
            let blob_dir = tempdir.child(format!("{arena}/blobs"));
            if let Some(p) = child.parent() {
                std::fs::create_dir_all(p)?;
            }
            let db = ArenaDatabase::new(redb_utils::in_memory()?)?;
            let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;
            cache.add_arena(arena, db, blob_dir.to_path_buf(), dirty_paths)?;

            let async_cache = cache.into_async();
            let cache = async_cache.blocking();

            Ok(Self {
                arena,
                async_cache,
                cache,
                tempdir,
            })
        }

        fn arena_cache(&self) -> anyhow::Result<&ArenaCache> {
            Ok(self.cache.arena_cache(self.arena)?)
        }

        fn add_file(&self, path: &str, size: u64) -> anyhow::Result<Inode> {
            let path = Path::parse(path)?;

            self.cache.update(
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

            let (inode, _) = self.cache.lookup_path(self.arena, &path)?;

            Ok(inode)
        }

        fn add_file_with_mtime(
            &self,
            path: &Path,
            size: u64,
            mtime: &UnixTime,
        ) -> anyhow::Result<()> {
            self.cache.update(
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
            self.cache.update(
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
        fn blob_file_exists(&self, arena: Arena, blob_id: BlobId) -> bool {
            self.blob_path(arena, blob_id).exists()
        }

        /// Return the path to a blob file for test use.
        fn blob_path(&self, arena: Arena, blob_id: BlobId) -> std::path::PathBuf {
            self.tempdir
                .child(format!("{arena}/blobs/{blob_id}"))
                .to_path_buf()
        }
    }

    #[tokio::test]
    async fn read_blob_within_available_ranges() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena).await?;
        let acache = fixture.arena_cache()?;

        let inode = fixture.add_file("test.txt", 1000)?;

        // Allocate blob id and create file
        let blob_id = acache.open_file(inode)?.blob_id;

        // Write some test data to the blob file
        let blob_path = fixture.blob_path(arena, blob_id);
        let test_data = b"Baa, baa, black sheep, have you any wool?";
        std::fs::write(&blob_path, test_data)?;

        acache
            .extend_local_availability(blob_id, &ByteRanges::single(0, test_data.len() as u64))?;

        // Read from blob
        let mut blob = fixture.async_cache.open_file(inode).await?;

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
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena).await?;
        let acache = fixture.arena_cache()?;

        let inode = fixture.add_file("test.txt", 1000)?;

        // Allocate blob id and create file
        let blob_id = acache.open_file(inode)?.blob_id;

        // Write test data to the blob file
        let blob_path = fixture.blob_path(arena, blob_id);
        let test_data = b"Baa, baa, black sheep, have you any wool?";
        std::fs::write(&blob_path, test_data)?;

        acache
            .extend_local_availability(blob_id, &ByteRanges::single(0, test_data.len() as u64))?;

        // Read from blob
        let mut blob = fixture.async_cache.open_file(inode).await?;
        blob.seek(SeekFrom::Start(b"Baa, baa, black sheep, ".len() as u64))
            .await?;
        let mut buf = [0; 100];
        let n = blob.read(&mut buf).await?;
        assert_eq!(b"have you any wool?", &buf[0..n]);

        Ok(())
    }

    #[tokio::test]
    async fn read_blob_outside_available_ranges() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena).await?;
        let acache = fixture.arena_cache()?;
        let inode = fixture.add_file("test.txt", 1000)?;

        // Allocate blob id and create file
        let blob_id = acache.open_file(inode)?.blob_id;

        // Write test data to the blob file
        let blob_path = fixture.blob_path(arena, blob_id);
        let test_data = b"baa, baa";
        std::fs::write(&blob_path, test_data)?;
        acache
            .extend_local_availability(blob_id, &ByteRanges::single(0, test_data.len() as u64))?;

        // Read from blob
        let mut blob = fixture.async_cache.open_file(inode).await?;
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
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena).await?;
        let acache = fixture.arena_cache()?;
        let inode = fixture.add_file("test.txt", 21)?;

        // Write to blob
        let mut blob = fixture.async_cache.open_file(inode).await?;
        let blob_id = blob.id();

        blob.write(b"baa, baa").await?;
        assert_eq!(ByteRanges::single(0, 8), *blob.local_availability());
        assert_eq!(ByteRanges::new(), acache.local_availability(blob_id)?);

        blob.write(b", black sheep").await?;
        assert_eq!(ByteRanges::single(0, 21), *blob.local_availability());
        assert_eq!(ByteRanges::new(), acache.local_availability(blob_id)?);

        blob.update_db().await?;
        assert_eq!(
            ByteRanges::single(0, 21),
            acache.local_availability(blob_id)?
        );
        drop(blob);

        assert_eq!(
            "baa, baa, black sheep",
            fs::read_to_string(fixture.blob_path(arena, blob_id)).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn writing_to_blob_out_of_order() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena).await?;
        let acache = fixture.arena_cache()?;
        let inode = fixture.add_file("test.txt", 21)?;

        // Write to blob
        let mut blob = fixture.async_cache.open_file(inode).await?;
        let blob_id = blob.id();

        blob.seek(SeekFrom::Start(8)).await?;
        blob.write(b", black sheep").await?;

        blob.seek(SeekFrom::Start(0)).await?;
        blob.write(b"baa, baa").await?;

        blob.update_db().await?;
        assert_eq!(
            ByteRanges::single(0, 21),
            acache.local_availability(blob_id)?
        );
        drop(blob);

        assert_eq!(
            "baa, baa, black sheep",
            fs::read_to_string(fixture.blob_path(arena, blob_id)).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn writing_to_blob_then_reading_it() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena).await?;
        let inode = fixture.add_file("test.txt", 100)?;

        let mut blob = fixture.async_cache.open_file(inode).await?;

        blob.write(b"baa, baa, black sheep").await?;

        blob.seek(SeekFrom::Start(10)).await?;

        let mut buf = [0; 100];
        let n = blob.read(&mut buf).await?;
        assert_eq!(b"black sheep", &buf[0..n]);

        Ok(())
    }

    #[tokio::test]
    async fn open_file_creates_sparse_file() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena).await?;
        let acache = fixture.arena_cache()?;
        let file_path = Path::parse("foobar")?;

        fixture.add_file_with_mtime(&file_path, 10000, &test_time())?;
        let (inode, _) = acache.lookup_path(&file_path)?;

        let blob_id = {
            let blob = acache.open_file(inode)?;
            assert_eq!(BlobId(1), blob.id());

            let m = fixture.blob_path(arena, blob.id()).metadata()?;

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
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let cache = &fixture.cache;
        let acache = fixture.arena_cache()?;
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;

        // Create a file
        fixture.add_file_with_mtime(&file_path, 100, &test_time())?;

        // Open the file to create a blob
        let (inode, _) = acache.lookup_path(&file_path)?;
        let blob_id = acache.open_file(inode)?.id();

        // Verify the blob file was created
        assert!(fixture.blob_file_exists(arena, blob_id));

        // Overwrite the file with a new version
        cache.update(
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
        assert!(!fixture.blob_file_exists(arena, blob_id));

        Ok(())
    }

    #[tokio::test]
    async fn blob_deleted_on_file_removal() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let acache = fixture.arena_cache()?;
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;

        // Create a file
        fixture.add_file_with_mtime(&file_path, 100, &test_time())?;

        // Open the file to create a blob
        let (inode, _) = acache.lookup_path(&file_path)?;
        let blob_id = acache.open_file(inode)?.id();

        // Verify the blob file was created
        assert!(fixture.blob_file_exists(arena, blob_id));

        // Remove the file
        fixture.remove_file(&file_path)?;

        // Verify the blob file has been deleted
        assert!(!fixture.blob_file_exists(arena, blob_id));

        Ok(())
    }

    #[tokio::test]
    async fn blob_deleted_on_catchup_removal() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let cache = &fixture.cache;
        let acache = fixture.arena_cache()?;
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;

        // Create a file
        fixture.add_file_with_mtime(&file_path, 100, &test_time())?;

        // Open the file to create a blob
        let (inode, _) = acache.lookup_path(&file_path)?;
        let blob_id = acache.open_file(inode)?.id();

        // Verify the blob file was created
        assert!(fixture.blob_file_exists(arena, blob_id));

        // Do a catchup that doesn't include this file (simulating file removal)
        cache.update(test_peer(), Notification::CatchupStart(arena))?;
        // Note: No Catchup notification for the file, so it will be deleted
        cache.update(
            test_peer(),
            Notification::CatchupComplete {
                arena: arena,
                index: 0,
            },
        )?;

        // Verify the blob file has been deleted
        assert!(!fixture.blob_file_exists(arena, blob_id));

        Ok(())
    }

    #[tokio::test]
    async fn blob_update_extends_range() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena).await?;
        let acache = fixture.arena_cache()?;
        let file_path = Path::parse("file.txt")?;

        // Create a file
        fixture.add_file_with_mtime(&file_path, 1000, &test_time())?;

        // Open the file to create a blob
        let (inode, _) = acache.lookup_path(&file_path)?;
        let blob_id = acache.open_file(inode)?.id();

        // Initially, the blob should have empty written areas
        let initial_ranges = acache.local_availability(blob_id)?;
        assert!(initial_ranges.is_empty());

        // Update the blob with some written areas
        let written_areas = ByteRanges::from_ranges(vec![
            ByteRange::new(0, 100),
            ByteRange::new(200, 300),
            ByteRange::new(500, 600),
        ]);

        acache.extend_local_availability(blob_id, &written_areas)?;

        // Verify the written areas were updated
        let retrieved_ranges = acache.local_availability(blob_id)?;
        assert_eq!(retrieved_ranges, written_areas);

        acache.extend_local_availability(
            blob_id,
            &ByteRanges::from_ranges(vec![ByteRange::new(50, 210), ByteRange::new(200, 400)]),
        )?;

        // Verify the ranges were updated again
        let final_ranges = acache.local_availability(blob_id)?;
        assert_eq!(
            final_ranges,
            ByteRanges::from_ranges(vec![ByteRange::new(0, 400), ByteRange::new(500, 600)])
        );

        // Test that getting ranges for a non-existent blob returns NotFound
        assert!(matches!(
            acache.local_availability(BlobId(99999)),
            Err(StorageError::NotFound)
        ));

        // Test that updating ranges for a non-existent blob returns NotFound
        assert!(matches!(
            acache.extend_local_availability(BlobId(99999), &ByteRanges::new()),
            Err(StorageError::NotFound)
        ));

        Ok(())
    }
}
