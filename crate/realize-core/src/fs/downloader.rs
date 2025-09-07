use crate::rpc::{ExecutionMode, Household};
use futures::Future;
use realize_storage::{Blob, GlobalCache, GlobalLoc, StorageError};
use realize_types::{Arena, ByteRange, ByteRanges, Path, Peer};
use std::cmp::min;
use std::collections::VecDeque;
use std::io::{ErrorKind, SeekFrom};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite as _, ReadBuf};
use tokio_stream::StreamExt;

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
    cache: Arc<GlobalCache>,
}

impl Downloader {
    pub fn new(household: Arc<Household>, cache: Arc<GlobalCache>) -> Self {
        Self { household, cache }
    }

    pub async fn reader<L: Into<GlobalLoc>>(&self, loc: L) -> Result<Download, StorageError> {
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
    offset: u64,
    pending_seek: Option<u64>,

    /// Chunks kept in memory, sorted by ByteRange.start.
    ///
    /// The first range should always contain offset.
    avail: VecDeque<(ByteRange, Vec<u8>)>,
    read: ReadState,
    blob: Blob,
}

/// States for AsyncRead::poll_read.
#[derive(Default)]
enum ReadState {
    /// Answer [tokio::io::AsyncRead::poll_read] immediately or dispatch to
    /// another state to get the data.
    #[default]
    Default,

    /// Run [tokio::io::AsyncSeek::poll_complete] on the blob, then
    /// [tokio::io::AsyncSeek::start_seek].
    StartSeek(SeekFrom, Box<ReadState>),

    /// [tokio::io::AsyncSeek::poll_complete] on the blob after
    /// calling [tokio::io::AsyncSeek::start_seek].
    Seek(Box<ReadState>),

    /// Call [tokio::io::AsyncRead::poll_read] on the blob
    Read,

    /// Call [tokio::io::AsyncWrite::poll_write] on the blob.
    ///
    /// Once this is done, schedule the ranges left in the deque for
    /// writing, if any.
    Write(ByteRange, Vec<u8>, VecDeque<(ByteRange, Vec<u8>)>),

    /// A future that downloads the given range from a remote peer.
    Download(ByteRange, PendingDownload),
}

type PendingDownload = Pin<
    Box<dyn Future<Output = Result<VecDeque<(ByteRange, Vec<u8>)>, std::io::Error>> + Send + Sync>,
>;

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
            offset: 0,
            pending_seek: None,
            avail: VecDeque::new(),
            read: ReadState::Default,
            blob,
        }
    }

    /// Check whether the current offset is at or past the file end.
    pub fn at_end(&self) -> bool {
        self.offset >= self.size
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

    fn fill(&mut self, buf: &mut ReadBuf<'_>) -> bool {
        let requested = ByteRange::new_with_size(self.offset, buf.remaining() as u64);
        let mut filled = false;
        while let Some((range, data)) = self.avail.front() {
            let intersection = requested.intersection(range);
            if !intersection.is_empty() && intersection.start == self.offset {
                buf.put_slice(
                    &data[(intersection.start - range.start) as usize
                        ..(intersection.end - range.start) as usize],
                );
                self.offset += intersection.bytecount();

                filled = true;
                if intersection.end < range.end {
                    return true;
                }
                self.avail.pop_front();
                if buf.remaining() == 0 {
                    return true;
                }
            }
        }

        filled
    }

    fn next_request(&mut self, bufsize: usize) -> (ByteRange, PendingDownload) {
        let offset = self.offset;
        let end = self.size;
        let mut next = self
            .avail
            .back()
            .map(|(range, _)| range.end)
            .unwrap_or(offset);

        // Align to MIN_CHUNK_SIZE, so the blocks start and end at
        // consistent positions. This will also match filesystem
        // blocks, to avoid wasting disk space when the blocks are
        // stored in sparse files.
        let align = next % MIN_CHUNK_SIZE;
        next -= align;
        let mut bufsize = bufsize as u64 + align;
        bufsize += MIN_CHUNK_SIZE - (bufsize % MIN_CHUNK_SIZE);
        if bufsize > MAX_CHUNK_SIZE {
            bufsize = MAX_CHUNK_SIZE;
        }

        let range = ByteRange::new(next, min(end, next + bufsize));

        log::debug!("[{}] Download \"{}\" {range}", self.arena, self.path);

        let fut = {
            let household = self.household.clone();
            let peers = self.peers.clone();
            let arena = self.arena;
            let path = self.path.clone();
            let range = range.clone();

            Box::pin(async move {
                let mut result = VecDeque::new();
                let mut stream = household.read(
                    peers,
                    ExecutionMode::Interactive,
                    arena,
                    path,
                    range.start,
                    Some(range.bytecount()),
                )?;
                while let Some(chunk_result) = stream.next().await {
                    match chunk_result {
                        Ok((offset, chunk)) => {
                            result.push_back((
                                ByteRange::new_with_size(offset, chunk.len() as u64),
                                chunk,
                            ));
                        }
                        Err(err) => return Err(err.into_io()),
                    }
                }
                result.make_contiguous().sort_by_key(|elt| elt.0.start);

                Ok(result)
            })
        };

        (range, fut)
    }

    /// State-based handler for poll_read.
    ///
    /// Returns the next state and optionally a poll value to return.
    ///
    /// ## [ReadState] transitions
    ///
    /// - `Default` → ( `StartSeek` → `Seek` ) → `Read` → `Default`
    ///
    /// - `Default` → `Download` → ( `StartSeek` → `Seek` ) → `Write` → `Default`
    ///
    /// On error, go back to `Default`.
    ///
    fn handle_poll_read(
        self: &mut Pin<&mut Self>,
        state: ReadState,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> (ReadState, Option<Poll<std::io::Result<()>>>) {
        match state {
            // Return the data or dispatch to Read or Download.
            ReadState::Default => {
                if self.offset >= self.size || self.fill(buf) {
                    return (ReadState::Default, Some(Poll::Ready(Ok(()))));
                }

                let readable = self.blob.readable_length(self.offset, buf.remaining());
                if readable.is_none() {
                    let (r, fut) = self.next_request(buf.capacity());

                    return (ReadState::Download(r, fut), None);
                }

                (
                    if self.blob.offset() == self.offset {
                        ReadState::Read
                    } else {
                        ReadState::StartSeek(
                            SeekFrom::Start(self.offset),
                            Box::new(ReadState::Read),
                        )
                    },
                    None,
                )
            }

            // Call poll_complete on the blob, then start_seek.
            //
            // As the documentation of start_seek recommends, this
            // implementation always call poll_complete before
            // start_seek to make sure any pending operations are
            // done.
            ReadState::StartSeek(position, next) => {
                match Pin::new(&mut self.blob).poll_complete(cx) {
                    Poll::Pending => (ReadState::StartSeek(position, next), Some(Poll::Pending)),
                    Poll::Ready(Err(err)) => (ReadState::Default, Some(Poll::Ready(Err(err)))),
                    Poll::Ready(Ok(_)) => {
                        if let Err(err) = Pin::new(&mut self.blob).start_seek(position) {
                            return (ReadState::Default, Some(Poll::Ready(Err(err))));
                        }
                        (ReadState::Seek(next), None)
                    }
                }
            }

            // Call poll_complete on the blob after start_seek
            ReadState::Seek(next) => match Pin::new(&mut self.blob).poll_complete(cx) {
                Poll::Pending => (ReadState::Seek(next), Some(Poll::Pending)),
                Poll::Ready(Err(err)) => (ReadState::Default, Some(Poll::Ready(Err(err)))),
                Poll::Ready(Ok(_)) => (*next, None),
            },

            // Read data from the blob
            ReadState::Read => {
                let start = buf.filled().len();
                match Pin::new(&mut self.blob).poll_read(cx, buf) {
                    Poll::Pending => (ReadState::Read, Some(Poll::Pending)),
                    Poll::Ready(Err(err)) => (ReadState::Default, Some(Poll::Ready(Err(err)))),
                    Poll::Ready(Ok(())) => {
                        let n = buf.filled().len() - start;
                        self.offset += n as u64;

                        (ReadState::Default, Some(Poll::Ready(Ok(()))))
                    }
                }
            }

            // Download data from another peer
            ReadState::Download(r, mut fut) => match fut.as_mut().poll(cx) {
                Poll::Pending => (ReadState::Download(r, fut), Some(Poll::Pending)),
                Poll::Ready(Err(err)) => (ReadState::Default, Some(Poll::Ready(Err(err)))),
                Poll::Ready(Ok(chunks)) => (self.write_chunks_state(chunks), None),
            },

            // Write data to the blob.
            ReadState::Write(r, data, chunks) => {
                match Pin::new(&mut self.blob).poll_write(cx, data.as_slice()) {
                    Poll::Pending => (ReadState::Write(r, data, chunks), Some(Poll::Pending)),
                    Poll::Ready(Err(err)) => (ReadState::Default, Some(Poll::Ready(Err(err)))),
                    Poll::Ready(Ok(_)) => {
                        // Keep in memory so data is immediately available for reading
                        self.avail.push_back((r, data));
                        if chunks.is_empty() {
                            self.avail.make_contiguous().sort_by_key(|elt| elt.0.start);
                        }

                        (self.write_chunks_state(chunks), None)
                    }
                }
            }
        }
    }

    /// Build a state appropriate to write the given chunk list.
    fn write_chunks_state(&self, mut chunks: VecDeque<(ByteRange, Vec<u8>)>) -> ReadState {
        if let Some((chunk_range, chunk_data)) = chunks.pop_front() {
            let start = chunk_range.start;
            let write_state = ReadState::Write(chunk_range, chunk_data, chunks);
            if start == self.blob.offset() {
                write_state
            } else {
                ReadState::StartSeek(SeekFrom::Start(start), Box::new(write_state))
            }
        } else {
            ReadState::Default
        }
    }
}

/// Add the given offset to the base.
///
/// Return an error if the shift would be below 0.
fn relative_offset(offset: i64, base: u64) -> Result<u64, std::io::Error> {
    if offset < 0 {
        let diff = (-offset) as u64;
        if diff > base {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                "Seek before start",
            ));
        }

        Ok(base - diff)
    } else {
        Ok(base + offset as u64)
    }
}

impl AsyncSeek for Download {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        let end = self.size;
        let goal = std::cmp::min(
            end,
            match position {
                SeekFrom::Current(offset) => relative_offset(offset, self.offset)?,
                SeekFrom::End(offset) => relative_offset(offset, end)?,
                SeekFrom::Start(offset) => offset,
            },
        );
        self.get_mut().pending_seek = Some(goal);

        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        let this = self.get_mut();

        match this.pending_seek.take() {
            None => Poll::Ready(Ok(this.offset)),
            Some(goal) => {
                if let ReadState::Download(range, _) = &this.read
                    && !range.contains(goal)
                {
                    this.read = ReadState::Default;
                }
                while this
                    .avail
                    .front()
                    .map(|(range, _)| !range.contains(goal))
                    .unwrap_or(false)
                {
                    this.avail.pop_front();
                }
                this.offset = goal;

                Poll::Ready(Ok(this.offset))
            }
        }
    }
}

impl AsyncRead for Download {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        loop {
            let prev = std::mem::take(&mut self.read);
            let (next, ret) = self.handle_poll_read(prev, cx, buf);
            self.read = next;
            if let Some(ret) = ret {
                return ret;
            }
        }
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
    use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};

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
                assert_eq!("File content", read_string(&mut reader).await?);

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
                assert_eq!("File content", read_string(&mut reader).await?);

                // The entire content of test.txt is available
                // locally. We don't need a connection anymore.
                testing::disconnect(&household_a, b).await?;

                // Reading back from the reader works.
                reader.seek(SeekFrom::Start(0)).await?;
                assert_eq!("File content", read_string(&mut reader).await?);

                // Reading back from another reader works, once the
                // database has been updated..
                reader.update_db().await?;
                drop(reader);

                let mut reader = fixture.reader(&downloader, "test.txt").await?;
                assert_eq!("File content", read_string(&mut reader).await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn seek() -> anyhow::Result<()> {
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

                reader.seek(SeekFrom::Start(10)).await?;
                assert_eq!("black sheep", read_string(&mut reader).await?);

                reader.seek(SeekFrom::Start(5)).await?;
                assert_eq!("baa, black sheep", read_string(&mut reader).await?);

                reader.seek(SeekFrom::Start(5)).await?;
                reader.seek(SeekFrom::Current(5)).await?;
                assert_eq!("black sheep", read_string(&mut reader).await?);

                reader.seek(SeekFrom::Current(-5)).await?;
                assert_eq!("sheep", read_string(&mut reader).await?);

                reader.seek(SeekFrom::End(-11)).await?;
                assert_eq!("black sheep", read_string(&mut reader).await?);

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

                reader.seek(SeekFrom::Start(100)).await?;
                assert_eq!("", read_string(&mut reader).await?);

                reader.seek(SeekFrom::End(5)).await?;
                assert_eq!("", read_string(&mut reader).await?);

                reader.seek(SeekFrom::Start(5)).await?;
                reader.seek(SeekFrom::Current(100)).await?;
                assert_eq!("", read_string(&mut reader).await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn seek_before_start() -> anyhow::Result<()> {
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

                assert!(reader.seek(SeekFrom::End(-100)).await.is_err());
                assert!(reader.seek(SeekFrom::Current(-100)).await.is_err());

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

                let mut buf = [0u8; 16];
                reader.read_exact(&mut buf).await?;
                assert_eq!([1u8; 16], buf);

                reader.seek(SeekFrom::End(-100)).await?;
                reader.read_exact(&mut buf).await?;
                assert_eq!([2u8; 16], buf);

                reader.seek(SeekFrom::Start(100)).await?;
                reader.read_exact(&mut buf).await?;
                assert_eq!([1u8; 16], buf);

                reader.seek(SeekFrom::Start(8 * 1024)).await?;
                reader.read_exact(&mut buf).await?;
                assert_eq!([0u8; 16], buf);

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
                reader.seek(SeekFrom::Start(100)).await?;
                let mut buf = [0u8; 16];
                reader.read_exact(&mut buf).await?;

                // Read near end; stores the last block
                reader.seek(SeekFrom::End(-100)).await?;
                reader.read_exact(&mut buf).await?;

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
                reader.seek(SeekFrom::Start(16)).await?;
                let mut buf = [0u8; 16];
                reader.read_exact(&mut buf).await?;
                assert_eq!([1u8; 16], buf);

                // Read from last block succeeds.
                reader.seek(SeekFrom::End(-32)).await?;
                reader.read_exact(&mut buf).await?;
                assert_eq!([2u8; 16], buf);

                // Read from middle block fails.
                reader.seek(SeekFrom::Start(block)).await?;
                assert!(reader.read_exact(&mut buf).await.is_err());

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
                test_read_file(&fixture, household_a, 12 * 1024, 1024, false).await?;
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
                test_read_file(&fixture, household_a, 12 * 1024, 2000, true).await?;
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
                test_read_file(&fixture, household_a, 48 * 1024, 32 * 1024, false).await?;
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
                test_read_file(&fixture, household_a, 64 * 1024, 48 * 1024, true).await?;
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
        allow_short_reads: bool,
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

        if allow_short_reads {
            let mut bytes_read = 0;
            let mut offset = 0;
            while bytes_read < file_size {
                let mut buf = vec![0u8; buf_size];
                let n = reader.read(&mut buf).await?;
                assert!(n > 0);
                for i in 0..n {
                    assert_eq!(file_content[offset + i], buf[i], "at offset {}", offset + i);
                }
                for i in n..buf_size {
                    assert_eq!(0, buf[i], "at offset {}", offset + i);
                }
                bytes_read += n;
                offset += n;
            }
        } else {
            for i in 0..(file_size / buf_size) {
                let offset = i * buf_size;
                let mut buf = vec![0u8; buf_size];
                assert_eq!(buf_size, reader.read(&mut buf).await?);
                assert_eq!(
                    file_content[offset..(offset + buf_size)],
                    buf,
                    "chunk {i}, offset {offset}"
                );
            }
            let rest = file_size % buf_size;
            if rest > 0 {
                let offset = file_size - rest;
                let mut buf = vec![0u8; buf_size];
                assert_eq!(rest, reader.read(&mut buf).await?);
                assert_eq!(
                    file_content[offset..],
                    buf[0..rest],
                    "last chunk, offset {offset}"
                );
                for i in rest..buf_size {
                    assert_eq!(0, buf[i]);
                }
            }
        }

        assert_eq!(0, reader.read(&mut vec![0u8; buf_size]).await?);

        Ok(())
    }

    async fn read_string(reader: &mut Download) -> anyhow::Result<String> {
        let mut buffer = String::new();
        reader.read_to_string(&mut buffer).await?;
        Ok(buffer)
    }
}
