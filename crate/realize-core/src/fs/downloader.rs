use crate::rpc::Household;
use futures::Future;
use realize_storage::{StorageError, UnrealCacheAsync};
use realize_types::{Arena, ByteRange, Path, Peer};
use std::cmp::min;
use std::collections::VecDeque;
use std::io::{ErrorKind, SeekFrom};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};
use tokio_stream::StreamExt;

const MIN_CHUNK_SIZE: usize = 8 * 1024;
const MAX_CHUNK_SIZE: usize = 32 * 1024;

#[derive(Clone)]
pub struct Downloader {
    household: Household,
    cache: UnrealCacheAsync,
}

impl Downloader {
    pub fn new(household: Household, cache: UnrealCacheAsync) -> Self {
        Self { household, cache }
    }

    pub async fn reader(&self, inode: u64) -> Result<Download, StorageError> {
        let avail = self.cache.file_availability(inode).await?;

        // TODO: check hash
        Ok(Download::new(
            self.household.clone(),
            avail.peers,
            avail.arena,
            avail.path,
            avail.metadata.size,
        ))
    }
}

pub struct Download {
    household: Household,
    peers: Vec<Peer>,
    arena: Arena,
    path: Path,
    size: u64,
    offset: u64,
    pending_seek: Option<u64>,
    avail: VecDeque<(ByteRange, Vec<u8>)>,
    pending: Option<(ByteRange, PendingDownload)>,
}

type PendingDownload = Pin<Box<dyn Future<Output = Result<Vec<u8>, std::io::Error>> + Send + Sync>>;

impl Download {
    fn new(household: Household, peers: Vec<Peer>, arena: Arena, path: Path, size: u64) -> Self {
        Self {
            household,
            peers,
            arena,
            path,
            size,
            offset: 0,
            pending_seek: None,
            avail: VecDeque::new(),
            pending: None,
        }
    }

    /// Check whether the current offset is at or past the file end.
    pub fn at_end(&self) -> bool {
        self.offset >= self.size
    }

    fn fill(&mut self, buf: &mut ReadBuf<'_>) -> bool {
        let requested = ByteRange::new(self.offset, self.offset + buf.remaining() as u64);
        let mut filled = false;
        while let Some((range, data)) = self.avail.front() {
            let intersection = requested.intersection(range);
            if !intersection.is_empty() && intersection.start == self.offset {
                log::debug!("Return [{}]/{} {intersection}", self.arena, self.path);

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
        let next = self
            .avail
            .back()
            .map(|(range, _)| range.end)
            .unwrap_or(offset);

        let range = ByteRange::new(
            next,
            min(
                end,
                next + bufsize.clamp(MIN_CHUNK_SIZE, MAX_CHUNK_SIZE) as u64,
            ),
        );

        log::debug!("Download [{}]/{} {range}", self.arena, self.path);

        let fut = {
            let household = self.household.clone();
            let peers = self.peers.clone();
            let arena = self.arena.clone();
            let path = self.path.clone();
            let range = range.clone();

            Box::pin(async move {
                let mut stream =
                    household.read(peers, arena, path, range.start, Some(range.bytecount()))?;
                let mut data = Vec::new();
                while let Some(chunk_result) = stream.next().await {
                    match chunk_result {
                        Ok(mut chunk) => data.append(&mut chunk),
                        Err(err) => return Err(err),
                    }
                }
                Ok(data)
            })
        };

        (range, fut)
    }

    fn inner_poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.offset >= self.size || self.fill(buf) {
            return Poll::Ready(Ok(()));
        }
        let (range, mut fut) = match self.pending.take() {
            Some(existing) => existing,
            None => self.next_request(buf.capacity()),
        };
        match fut.as_mut().poll(cx) {
            Poll::Ready(Ok(data)) => {
                self.avail.push_back((range, data));

                self.inner_poll_read(cx, buf)
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => {
                self.pending = Some((range, fut));

                Poll::Pending
            }
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
                if let Some((range, _)) = &this.pending {
                    if !range.contains(goal) {
                        this.pending = None;
                    }
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
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.get_mut().inner_poll_read(cx, buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::testing::HouseholdFixture;
    use realize_types::Path;
    use std::io::Write as _;
    use tokio::fs;
    use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};

    #[tokio::test]
    async fn read_small_file() -> anyhow::Result<()> {
        let mut fixture = HouseholdFixture::setup().await?;
        fixture
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                // Create a file in peer B's arena
                let b_dir = fixture.arena_root(&b);
                fs::write(&b_dir.join("test.txt"), "File content").await?;

                // Wait for the file to appear in peer A's cache
                fixture.wait_for_file_in_cache(&a, "test.txt").await?;

                // Get the cache and create a downloader
                let cache = fixture.cache(&a)?;
                let downloader = Downloader::new(household_a, cache.clone());

                // Get the inode for the file
                let arena = HouseholdFixture::test_arena();
                let (inode, _) = cache.lookup_path(&arena, &Path::parse("test.txt")?).await?;

                let mut reader = downloader.reader(inode).await?;
                assert_eq!("File content", read_string(&mut reader).await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn seek() -> anyhow::Result<()> {
        let mut fixture = HouseholdFixture::setup().await?;
        fixture
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                let b_dir = fixture.arena_root(&b);
                fs::write(&b_dir.join("test.txt"), "baa, baa, black sheep").await?;

                fixture.wait_for_file_in_cache(&a, "test.txt").await?;

                let cache = fixture.cache(&a)?;
                let downloader = Downloader::new(household_a, cache.clone());

                let arena = HouseholdFixture::test_arena();
                let (inode, _) = cache.lookup_path(&arena, &Path::parse("test.txt")?).await?;

                let mut reader = downloader.reader(inode).await?;

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
        let mut fixture = HouseholdFixture::setup().await?;
        fixture
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                let b_dir = fixture.arena_root(&b);
                fs::write(&b_dir.join("test.txt"), "baa, baa, black sheep").await?;

                fixture.wait_for_file_in_cache(&a, "test.txt").await?;

                let cache = fixture.cache(&a)?;
                let downloader = Downloader::new(household_a, cache.clone());

                let arena = HouseholdFixture::test_arena();
                let (inode, _) = cache.lookup_path(&arena, &Path::parse("test.txt")?).await?;

                let mut reader = downloader.reader(inode).await?;

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
        let mut fixture = HouseholdFixture::setup().await?;
        fixture
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                let b_dir = fixture.arena_root(&b);
                fs::write(&b_dir.join("test.txt"), "baa, baa, black sheep").await?;

                fixture.wait_for_file_in_cache(&a, "test.txt").await?;

                let cache = fixture.cache(&a)?;
                let downloader = Downloader::new(household_a, cache.clone());

                let arena = HouseholdFixture::test_arena();
                let (inode, _) = cache.lookup_path(&arena, &Path::parse("test.txt")?).await?;

                let mut reader = downloader.reader(inode).await?;

                assert!(reader.seek(SeekFrom::End(-100)).await.is_err());
                assert!(reader.seek(SeekFrom::Current(-100)).await.is_err());

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn seek_mixed_with_read() -> anyhow::Result<()> {
        let mut fixture = HouseholdFixture::setup().await?;
        fixture
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                let b_dir = fixture.arena_root(&b);
                let path = b_dir.join("large_file");
                {
                    let mut f = std::fs::File::create(&path)?;
                    f.write(&[1u8; 8 * 1024])?;
                    f.write(&[0u8; 8 * 1024])?;
                    f.write(&[2u8; 8 * 1024])?;
                }

                fixture.wait_for_file_in_cache(&a, "large_file").await?;

                let cache = fixture.cache(&a)?;
                let downloader = Downloader::new(household_a, cache.clone());

                let arena = HouseholdFixture::test_arena();
                let (inode, _) = cache
                    .lookup_path(&arena, &Path::parse("large_file")?)
                    .await?;

                let mut reader = downloader.reader(inode).await?;

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
    async fn read_large_file_small_buffer() -> anyhow::Result<()> {
        let mut fixture = HouseholdFixture::setup().await?;
        fixture
            .with_two_peers()
            .await?
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
            .run(async |household_a, _household_b| {
                test_read_file(&fixture, household_a, 64 * 1024, 48 * 1024, true).await?;
                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    async fn test_read_file(
        fixture: &HouseholdFixture,
        household: Household,
        file_size: usize,
        buf_size: usize,
        allow_short_reads: bool,
    ) -> anyhow::Result<()> {
        let a = HouseholdFixture::a();
        let b = HouseholdFixture::b();

        let b_dir = fixture.arena_root(&b);
        let path = b_dir.join("large_file");
        std::fs::write(&path, vec![0xAB; file_size])?;

        fixture.wait_for_file_in_cache(&a, "large_file").await?;

        let cache = fixture.cache(&a)?;
        let downloader = Downloader::new(household, cache.clone());

        let arena = HouseholdFixture::test_arena();
        let (inode, _) = cache
            .lookup_path(&arena, &Path::parse("large_file")?)
            .await?;

        let mut reader = downloader.reader(inode).await?;

        if allow_short_reads {
            let mut bytes_read = 0;
            while bytes_read < file_size {
                let mut buf = vec![0u8; buf_size];
                let n = reader.read(&mut buf).await?;
                assert!(n > 0);
                for i in 0..n {
                    assert_eq!(0xAB, buf[i]);
                }
                for i in n..buf_size {
                    assert_eq!(0, buf[i]);
                }
                bytes_read += n;
            }
        } else {
            for i in 0..(file_size / buf_size) {
                let mut buf = vec![0u8; buf_size];
                assert_eq!(buf_size, reader.read(&mut buf).await?);
                assert_eq!(vec![0xAB; buf_size], buf, "chunk {i}");
            }
            let rest = file_size % buf_size;
            if rest > 0 {
                let mut buf = vec![0u8; buf_size];
                assert_eq!(rest, reader.read(&mut buf).await?);
                for i in 0..rest {
                    assert_eq!(0xAB, buf[i]);
                }
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
