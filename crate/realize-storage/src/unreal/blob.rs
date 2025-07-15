use super::types::{BlobId, OpenBlob};
use realize_types::{ByteRanges, Hash};
use std::io::SeekFrom;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

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
    available_ranges: ByteRanges,
    size: u64,
    hash: Hash,

    /// The read/write/seek position.
    position: u64,
}

#[allow(dead_code)]
impl Blob {
    /// Create a new blob from a file and its available byte ranges.
    pub(crate) fn new(def: OpenBlob) -> Self {
        Self {
            blob_id: def.blob_id,
            file: tokio::fs::File::from_std(def.file),
            available_ranges: def.blob_entry.written_areas,
            size: def.file_entry.metadata.size,
            hash: def.file_entry.content.hash,
            position: 0,
        }
    }

    /// Get the size of the corresponding file.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Get the parts of the file that are available locally.
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

    /// Adjust the len to cover only the available portion of the requested range.
    fn adjusted_len(&self, requested_len: usize) -> Option<usize> {
        let available = ByteRanges::single(self.position, requested_len as u64)
            .intersection(&self.available_ranges);
        if let Some(range) = available.iter().next()
            && range.start == self.position
        {
            return Some(range.bytecount() as usize);
        }

        // Len cannot be ajusted; the request should not go through.
        None
    }
}

impl AsyncRead for Blob {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let requested_len = buf.remaining();
        if requested_len == 0 {
            return Poll::Ready(Ok(()));
        }

        match self.adjusted_len(requested_len) {
            None => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                BlobIncomplete,
            ))),
            Some(adjusted_len) => {
                let mut shortbuf = buf.take(adjusted_len);
                match Pin::new(&mut self.file).poll_read(cx, &mut shortbuf) {
                    Poll::Ready(Ok(())) => {
                        let filled = shortbuf.filled().len();
                        let initialized = shortbuf.initialized().len();
                        self.position += filled as u64;
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
                    Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
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
                self.position = pos;
                Poll::Ready(Ok(pos))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::cache::UnrealCacheBlocking;
    use super::*;
    use crate::unreal::arena_cache::ArenaCache;
    use crate::{Inode, Notification, UnrealCacheAsync};
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use realize_types::ByteRange;
    use realize_types::UnixTime;
    use realize_types::{Arena, Path, Peer};
    use redb::Database;
    use std::io::SeekFrom;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

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
            let path = tempdir.path().join("unreal.db");
            let mut cache = UnrealCacheBlocking::open(&path)?;

            let child = tempdir.child(format!("{arena}-cache.db"));
            let blob_dir = tempdir.child(format!("{arena}/blobs"));
            if let Some(p) = child.parent() {
                std::fs::create_dir_all(p)?;
            }
            cache.add_arena(
                arena,
                Database::create(child.path())?,
                blob_dir.to_path_buf(),
            )?;

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

        acache.extend_local_availability(
            blob_id,
            ByteRanges::from_ranges(vec![ByteRange::new(0, test_data.len() as u64)]),
        )?;

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

        acache.extend_local_availability(
            blob_id,
            ByteRanges::from_ranges(vec![ByteRange::new(0, test_data.len() as u64)]),
        )?;

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
        acache.extend_local_availability(
            blob_id,
            ByteRanges::from_ranges(vec![ByteRange::new(0, test_data.len() as u64)]),
        )?;

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
}
