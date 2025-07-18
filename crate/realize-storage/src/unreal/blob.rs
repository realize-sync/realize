use super::cache::UnrealCacheBlocking;
use super::types::{BlobId, BlobTableEntry, FileTableEntry};
use crate::StorageError;
use realize_types::{Arena, ByteRange, ByteRanges, Hash};
use std::cmp::min;
use std::io::SeekFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, AsyncWriteExt as _, ReadBuf};

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

pub(crate) struct OpenBlob {
    pub blob_id: BlobId,
    pub file_entry: FileTableEntry,
    pub blob_entry: BlobTableEntry,
    pub file: std::fs::File,
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
    cache: Arc<UnrealCacheBlocking>,
    arena: Arena,

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
    pub(crate) fn new(def: OpenBlob, cache: Arc<UnrealCacheBlocking>, arena: Arena) -> Self {
        Self {
            blob_id: def.blob_id,
            file: tokio::fs::File::from_std(def.file),
            available_ranges: def.blob_entry.written_areas,
            cache,
            arena,
            pending_ranges: ByteRanges::new(),
            size: def.file_entry.metadata.size,
            hash: def.file_entry.content.hash,
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

        let cache = Arc::clone(&self.cache);
        let blob_id = self.blob_id;
        let arena = self.arena;
        let ranges = std::mem::take(&mut self.pending_ranges);
        let (res, ranges) = tokio::task::spawn_blocking(move || {
            (
                match cache.arena_cache(arena) {
                    Err(err) => Err(err),
                    Ok(cache) => cache.extend_local_availability(blob_id, &ranges),
                },
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
    use super::super::cache::UnrealCacheBlocking;
    use super::*;
    use crate::unreal::arena_cache::ArenaCache;
    use crate::utils::redb_utils;
    use crate::{Inode, Notification, UnrealCacheAsync};
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use realize_types::{Arena, Path, Peer, UnixTime};
    use std::io::SeekFrom;
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

    struct Fixture {
        arena: Arena,
        async_cache: UnrealCacheAsync,
        cache: Arc<UnrealCacheBlocking>,
        tempdir: TempDir,
    }
    impl Fixture {
        fn setup_with_arena(arena: Arena) -> anyhow::Result<Fixture> {
            let _ = env_logger::try_init();
            let tempdir = TempDir::new()?;
            let mut cache = UnrealCacheBlocking::new(redb_utils::in_memory()?)?;

            let child = tempdir.child(format!("{arena}-cache.db"));
            let blob_dir = tempdir.child(format!("{arena}/blobs"));
            if let Some(p) = child.parent() {
                std::fs::create_dir_all(p)?;
            }
            cache.add_arena(arena, redb_utils::in_memory()?, blob_dir.to_path_buf())?;

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
                    mtime: UnixTime::from_secs(1234567890),
                    size,
                    hash: test_hash(),
                },
            )?;

            let (inode, _) = self.cache.lookup_path(self.arena, &path)?;

            Ok(inode)
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
        let fixture = Fixture::setup_with_arena(arena)?;
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
        let fixture = Fixture::setup_with_arena(arena)?;
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
        let fixture = Fixture::setup_with_arena(arena)?;
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
        let fixture = Fixture::setup_with_arena(arena)?;
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
        let fixture = Fixture::setup_with_arena(arena)?;
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
        let fixture = Fixture::setup_with_arena(arena)?;
        let inode = fixture.add_file("test.txt", 100)?;

        let mut blob = fixture.async_cache.open_file(inode).await?;

        blob.write(b"baa, baa, black sheep").await?;

        blob.seek(SeekFrom::Start(10)).await?;

        let mut buf = [0; 100];
        let n = blob.read(&mut buf).await?;
        assert_eq!(b"black sheep", &buf[0..n]);

        Ok(())
    }
}
