use crate::rpc::{ExecutionMode, Household};
use realize_storage::{Blob, Filesystem, FsLoc, StorageError};
use realize_types::{Arena, ByteRange, ByteRanges, Path, Peer};
use std::cmp::min;
use std::collections::VecDeque;
use std::io::SeekFrom;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_stream::StreamExt;
use tokio_util::bytes::BufMut;

/// Minimum size of a chunk read from a remote peer.
///
/// This should be a multiple of 4k, to match the most common
/// filesystem block sizes.
const MIN_CHUNK_SIZE: u64 = 8 * 1024;

/// Maximum size of a chunk read from a remote peer.
///
/// This should be a multiple of MIN_CHUNK_SIZE.
const MAX_CHUNK_SIZE: u64 = 4 * MIN_CHUNK_SIZE;

#[derive(Clone)]
pub struct Downloader {
    household: Arc<Household>,
    cache: Arc<Filesystem>,
}

impl Downloader {
    pub fn new(household: Arc<Household>, cache: Arc<Filesystem>) -> Self {
        Self { household, cache }
    }

    pub async fn reader<L: Into<FsLoc>>(&self, loc: L) -> Result<Download, StorageError> {
        let blob = self.cache.open_file(loc).await?;
        let avail = blob
            .remote_availability()
            .await?
            .ok_or(StorageError::NotFound)?;

        // TODO: check hash
        Ok(Download::new(
            self.household.clone(),
            avail.peers,
            avail.arena,
            avail.path,
            avail.size,
            blob,
        ))
    }
}

/// Make file data available for read.
///
/// If data is available locally, return it, otherwise fetch it from a
/// remote peer and store it for later.
///
/// Call [Download::update_db] when you're done to update the database
/// with any data that had to be downloaded, to keep it for later.
pub struct Download {
    household: Arc<Household>,
    peers: Vec<Peer>,
    arena: Arena,
    path: Path,
    size: u64,

    /// Chunks kept in memory, sorted by ByteRange.start.
    ///
    /// The first range should always contain offset.
    /// This should be continguous.
    ///
    buffer: VecDeque<(ByteRange, Vec<u8>)>, // TODO: consider using a Buf/BufMut
    blob: Blob,
}

impl Download {
    fn new(
        household: Arc<Household>,
        peers: Vec<Peer>,
        arena: Arena,
        path: Path,
        size: u64,
        blob: Blob,
    ) -> Self {
        Self {
            household,
            peers,
            arena,
            path,
            size,
            buffer: VecDeque::new(),
            blob,
        }
    }

    /// Update local availability in the database.
    pub async fn update_db(&mut self) -> Result<(), StorageError> {
        self.blob.update_db().await?;
        Ok(())
    }

    /// Get the parts of the file that are available locally.
    ///
    /// This might differ from local availability as reported by the
    /// cache, since this:
    /// - includes ranges that have been written to the file, but
    ///   haven't been flushed yet.
    /// - does not include ranges written through other file handles
    pub fn local_availability(&self) -> &ByteRanges {
        self.blob.available_range()
    }

    /// Read remote peer data at the given offset.
    ///
    /// Data is downloaded or read from local store.
    ///
    /// This function fills `buf` to capacity, even if it means making
    /// several read operations. It stops only if `buf` is full or
    /// when reaching EOF.
    pub async fn read_all_at<B: BufMut + ?Sized>(
        &mut self,
        mut offset: u64,
        buf: &mut B,
    ) -> Result<usize, std::io::Error> {
        let mut bytes_read = 0;
        loop {
            let n = self.read_some_at(offset, buf).await?;
            if n == 0 {
                // EOF
                break;
            }
            bytes_read += n;
            offset += n as u64;
        }
        Ok(bytes_read)
    }

    /// Read or download a chunk of data and put it into `buf`.
    ///
    /// Returns 0 when EOF is reached or buf is full. Short reads are
    /// possible
    async fn read_some_at<B: BufMut + ?Sized>(
        &mut self,
        offset: u64,
        buf: &mut B,
    ) -> Result<usize, std::io::Error> {
        let capacity = buf.remaining_mut();
        if capacity == 0 || offset >= self.size {
            return Ok(0);
        }
        let filled = self.fill_from_buffer(offset, buf);
        if filled > 0 {
            // at least partially available in memory
            return Ok(filled);
        }
        if let Some(readable_length) = self.blob.readable_length(offset, buf.remaining_mut()) {
            // at least partially available in the blob
            self.blob.seek(SeekFrom::Start(offset)).await?;
            return self.blob.read_buf(&mut buf.limit(readable_length)).await;
        }

        // download into buffer, optionally store into the blob
        let ranges = self.download_range(offset, capacity).await?;
        for (range, data) in ranges {
            let _ = self.blob.update(range.start, &data).await; // best effort
            self.buffer.push_back((range, data));
        }
        self.buffer.make_contiguous().sort_by_key(|elt| elt.0.start);

        Ok(self.fill_from_buffer(offset, buf))
    }

    /// Fill `buf` with previously downloaded data, kept in memory.
    fn fill_from_buffer<B: BufMut + ?Sized>(&mut self, offset: u64, buf: &mut B) -> usize {
        while self
            .buffer
            .front()
            .map(|(range, _)| !range.contains(offset))
            .unwrap_or(false)
        {
            self.buffer.pop_front();
        }

        let requested = ByteRange::new_with_size(offset, buf.remaining_mut() as u64);
        let mut filled_bytes = 0;
        if let Some((range, data)) = self.buffer.front() {
            let intersection = requested.intersection(range);
            if !intersection.is_empty() && intersection.start == offset {
                buf.put_slice(
                    &data[(intersection.start - range.start) as usize
                        ..(intersection.end - range.start) as usize],
                );
                filled_bytes += intersection.bytecount();

                if intersection.end == range.end {
                    self.buffer.pop_front();
                }
            }
        }

        filled_bytes as usize
    }

    /// Download a range of data for reading `bufsize` at `offset`.
    ///
    /// Downloaded data will intersect with the requested range, but
    /// it might not match exactly, as more data than strictly
    /// necessary can be downloaded to keep for later.
    async fn download_range(
        &mut self,
        offset: u64,
        requested_bufsize: usize,
    ) -> Result<VecDeque<(ByteRange, Vec<u8>)>, std::io::Error> {
        let end = self.size;
        let mut next = offset;

        // Align to MIN_CHUNK_SIZE, so the blocks start and end at
        // consistent positions. This will also match filesystem
        // blocks, to avoid wasting disk space when the blocks are
        // stored in sparse files.
        let align = next % MIN_CHUNK_SIZE;
        next -= align;
        let mut bufsize = requested_bufsize as u64 + align;
        bufsize += MIN_CHUNK_SIZE - (bufsize % MIN_CHUNK_SIZE);
        if bufsize > MAX_CHUNK_SIZE {
            bufsize = MAX_CHUNK_SIZE;
        }

        let range = ByteRange::new(next, min(end, next + bufsize));

        log::debug!(
            "[{}] Download \"{}\" {range} for read [{offset},{})",
            self.arena,
            self.path,
            offset + requested_bufsize as u64
        );

        let mut result = VecDeque::new();
        let mut stream = self.household.read(
            self.peers.clone(),
            ExecutionMode::Interactive,
            self.arena,
            self.path.clone(),
            range.start,
            Some(range.bytecount()),
        )?;
        while let Some(chunk_result) = stream.next().await {
            match chunk_result {
                Ok((offset, chunk)) => {
                    result.push_back((ByteRange::new_with_size(offset, chunk.len() as u64), chunk));
                }
                Err(err) => return Err(err.into_io()),
            }
        }
        result.make_contiguous().sort_by_key(|elt| elt.0.start);

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::testing::{self, HouseholdFixture};
    use rand::rngs::SmallRng;
    use rand::{RngCore, SeedableRng};
    use realize_storage::utils::hash;
    use realize_types::Path;
    use std::io::Write as _;
    use tokio::fs;

    struct Fixture {
        inner: HouseholdFixture,
    }

    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();
            let household_fixture = HouseholdFixture::setup().await?;

            Ok(Self {
                inner: household_fixture,
            })
        }

        async fn download_file_from_b(
            &self,
            downloader: &Downloader,
            path_str: &str,
            content: &str,
        ) -> anyhow::Result<Download> {
            let path = Path::parse(path_str)?;

            let b = HouseholdFixture::b();
            let b_dir = self.inner.arena_root(b);
            fs::write(path.within(&b_dir), content).await?;

            // Wait for the file to appear in peer A's cache
            let a = HouseholdFixture::a();
            self.inner
                .wait_for_file_in_cache(a, path_str, &hash::digest(content.as_bytes()))
                .await?;

            // Get the pathid for the file
            self.reader(downloader, path_str).await
        }

        async fn reader(
            &self,
            downloader: &Downloader,
            path_str: &str,
        ) -> Result<Download, anyhow::Error> {
            let arena = HouseholdFixture::test_arena();
            let path = Path::parse(path_str)?;

            Ok(downloader.reader((arena, &path)).await?)
        }
    }

    #[tokio::test]
    async fn read_small_file() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let downloader = Downloader::new(
                    household_a,
                    fixture.inner.cache(HouseholdFixture::a())?.clone(),
                );
                let mut reader = fixture
                    .download_file_from_b(&downloader, "test.txt", "File content")
                    .await?;
                assert_eq!("File content", read_string(0, &mut reader).await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn read_after_disconnection() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let b = HouseholdFixture::b();
                testing::connect(&household_a, b).await?;

                let downloader = Downloader::new(
                    household_a.clone(),
                    fixture.inner.cache(HouseholdFixture::a())?.clone(),
                );
                let mut reader = fixture
                    .download_file_from_b(&downloader, "test.txt", "File content")
                    .await?;
                assert_eq!("File content", read_string(0, &mut reader).await?);

                // The entire content of test.txt is available
                // locally. We don't need a connection anymore.
                testing::disconnect(&household_a, b).await?;

                // Reading back from the reader works.
                assert_eq!("File content", read_string(0, &mut reader).await?);

                // Reading back from another reader works, once the
                // database has been updated..
                reader.update_db().await?;
                drop(reader);

                let mut reader = fixture.reader(&downloader, "test.txt").await?;
                assert_eq!("File content", read_string(0, &mut reader).await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn read_at_offset() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let downloader = Downloader::new(
                    household_a,
                    fixture.inner.cache(HouseholdFixture::a())?.clone(),
                );
                let mut reader = fixture
                    .download_file_from_b(&downloader, "test.txt", "baa, baa, black sheep")
                    .await?;

                assert_eq!("black sheep", read_string(10, &mut reader).await?);
                assert_eq!("baa, black sheep", read_string(5, &mut reader).await?);
                assert_eq!("", read_string(25, &mut reader).await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn seek_past_end() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let downloader = Downloader::new(
                    household_a,
                    fixture.inner.cache(HouseholdFixture::a())?.clone(),
                );
                let mut reader = fixture
                    .download_file_from_b(&downloader, "test.txt", "baa, baa, black sheep")
                    .await?;

                assert_eq!("", read_string(100, &mut reader).await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn seek_mixed_with_read() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                let b_dir = fixture.inner.arena_root(b);
                let path = b_dir.join("large_file");
                {
                    let mut f = std::fs::File::create(&path)?;
                    f.write(&[1u8; 8 * 1024])?;
                    f.write(&[0u8; 8 * 1024])?;
                    f.write(&[2u8; 8 * 1024])?;
                }

                // Calculate hash for the large file content
                let mut large_file_content = Vec::new();
                large_file_content.extend_from_slice(&[1u8; 8 * 1024]);
                large_file_content.extend_from_slice(&[0u8; 8 * 1024]);
                large_file_content.extend_from_slice(&[2u8; 8 * 1024]);

                fixture
                    .inner
                    .wait_for_file_in_cache(a, "large_file", &hash::digest(&large_file_content))
                    .await?;

                let downloader = Downloader::new(household_a, fixture.inner.cache(a)?.clone());

                let mut reader = fixture.reader(&downloader, "large_file").await?;

                let mut buf = vec![].limit(16);
                reader.read_all_at(0, &mut buf).await?;
                assert_eq!(vec![1u8; 16], buf.into_inner());

                let mut buf = vec![].limit(16);
                reader.read_all_at(3 * 8 * 1024 - 100, &mut buf).await?;
                assert_eq!(vec![2u8; 16], buf.into_inner());

                let mut buf = vec![].limit(16);
                reader.read_all_at(100, &mut buf).await?;
                assert_eq!(vec![1u8; 16], buf.into_inner());

                let mut buf = vec![].limit(16);
                reader.read_all_at(8 * 1024, &mut buf).await?;
                assert_eq!(vec![0u8; 16], buf.into_inner());

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn read_partially_available_file_offline() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                testing::connect(&household_a, b).await?;

                let b_dir = fixture.inner.arena_root(b);
                let path = b_dir.join("large_file");
                {
                    let mut f = std::fs::File::create(&path)?;
                    f.write(&[1u8; MIN_CHUNK_SIZE as usize])?;
                    f.write(&[0u8; MIN_CHUNK_SIZE as usize])?;
                    f.write(&[2u8; MIN_CHUNK_SIZE as usize])?;
                }

                // Calculate hash for the large file content
                let mut large_file_content = Vec::new();
                large_file_content.extend_from_slice(&[1u8; MIN_CHUNK_SIZE as usize]);
                large_file_content.extend_from_slice(&[0u8; MIN_CHUNK_SIZE as usize]);
                large_file_content.extend_from_slice(&[2u8; MIN_CHUNK_SIZE as usize]);

                fixture
                    .inner
                    .wait_for_file_in_cache(a, "large_file", &hash::digest(&large_file_content))
                    .await?;

                let downloader =
                    Downloader::new(household_a.clone(), fixture.inner.cache(a)?.clone());

                let mut reader = fixture.reader(&downloader, "large_file").await?;

                // Read near start; stores the 1st block
                reader.read_all_at(100, &mut vec![].limit(16)).await?;

                // Read near end; stores the last block
                reader
                    .read_all_at(3 * MIN_CHUNK_SIZE - 100, &mut vec![].limit(16))
                    .await?;

                testing::disconnect(&household_a, b).await?;

                let block = MIN_CHUNK_SIZE;
                assert_eq!(
                    ByteRanges::from_ranges(vec![
                        ByteRange::new(0, block),
                        ByteRange::new(2 * block, 3 * block)
                    ]),
                    *reader.local_availability()
                );

                // Read from first block succeeds.
                let mut buf = vec![].limit(16);
                reader.read_all_at(16, &mut buf).await?;
                assert_eq!(vec![1u8; 16], buf.into_inner());

                // Read from last block succeeds.
                let mut buf = vec![].limit(16);
                reader.read_all_at(3 * block - 100, &mut buf).await?;
                assert_eq!(vec![2u8; 16], buf.into_inner());

                // Read from middle block fails.
                assert!(
                    reader
                        .read_all_at(block, &mut vec![].limit(16))
                        .await
                        .is_err()
                );

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn read_large_file_small_buffer() -> anyhow::Result<()> {
        let mut fixture = HouseholdFixture::setup().await?;
        fixture
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                test_read_file(&fixture, household_a, 12 * 1024, 1024).await?;
                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn read_large_file_unusual_buffer_size() -> anyhow::Result<()> {
        let mut fixture = HouseholdFixture::setup().await?;
        fixture
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                test_read_file(&fixture, household_a, 12 * 1024, 2000).await?;
                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn read_large_file_large_buffer() -> anyhow::Result<()> {
        let mut fixture = HouseholdFixture::setup().await?;
        fixture
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                test_read_file(&fixture, household_a, 48 * 1024, 32 * 1024).await?;
                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn read_large_file_overly_large_buffer() -> anyhow::Result<()> {
        let mut fixture = HouseholdFixture::setup().await?;
        fixture
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                test_read_file(&fixture, household_a, 64 * 1024, 48 * 1024).await?;
                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    async fn test_read_file(
        fixture: &HouseholdFixture,
        household: Arc<Household>,
        file_size: usize,
        buf_size: usize,
    ) -> anyhow::Result<()> {
        let a = HouseholdFixture::a();
        let b = HouseholdFixture::b();

        let b_dir = fixture.arena_root(b);
        let path = b_dir.join("large_file");
        let mut rng = SmallRng::seed_from_u64(1191);
        let mut file_content = vec![0; file_size];
        rng.fill_bytes(file_content.as_mut_slice());
        std::fs::write(&path, &mut file_content)?;

        fixture
            .wait_for_file_in_cache(a, "large_file", &hash::digest(&file_content))
            .await?;

        let cache = fixture.cache(a)?;
        let downloader = Downloader::new(household, Arc::clone(cache));

        let arena = HouseholdFixture::test_arena();
        let mut reader = downloader
            .reader((arena, &Path::parse("large_file")?))
            .await?;

        for i in 0..(file_size / buf_size) {
            let offset = i * buf_size;
            let mut buf = vec![].limit(buf_size);
            assert_eq!(buf_size, reader.read_all_at(offset as u64, &mut buf).await?);
            assert_eq!(
                file_content[offset..(offset + buf_size)],
                buf.into_inner(),
                "chunk {i}, offset {offset}"
            );
        }
        let rest = file_size % buf_size;
        if rest > 0 {
            let offset = file_size - rest;
            let mut buf = vec![].limit(buf_size);
            assert_eq!(rest, reader.read_all_at(offset as u64, &mut buf).await?);
            assert_eq!(
                file_content[offset..],
                buf.into_inner(),
                "last chunk, offset {offset}"
            );
        }

        assert_eq!(0, reader.read_all_at(file_size as u64, &mut vec![]).await?);

        Ok(())
    }

    async fn read_string(offset: u64, reader: &mut Download) -> anyhow::Result<String> {
        let mut buffer = vec![];
        reader.read_all_at(offset, &mut buffer).await?;
        Ok(String::from_utf8(buffer)?)
    }
}
