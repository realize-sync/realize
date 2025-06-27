use std::{
    cmp::{max, min},
    collections::VecDeque,
    io::{ErrorKind, SeekFrom},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use moka::future::{Cache, CacheBuilder};
use tarpc::{client::RpcError, context};
use tokio::{
    io::{AsyncRead, AsyncSeek, ReadBuf},
    task::JoinSet,
};

use crate::{
    model::{ByteRange, Peer},
    network::{
        Networking,
        rpc::realstore::{
            self,
            client::{ClientOptions, RealStoreClient},
        },
    },
    storage::real::{self, RealStoreError},
};

use super::{FileTableEntry, UnrealCacheAsync, UnrealError};

const MIN_CHUNK_SIZE: usize = 8 * 1024;
const MAX_CHUNK_SIZE: usize = 32 * 1024;

#[derive(Clone)]
pub struct Downloader {
    networking: Networking,
    cache: UnrealCacheAsync,
    clients: Cache<Peer, Arc<RealStoreClient>>,
}

impl Downloader {
    pub fn new(networking: Networking, cache: UnrealCacheAsync) -> Self {
        Self {
            networking,
            cache,
            clients: CacheBuilder::new(8)
                .time_to_idle(Duration::from_secs(5 * 60))
                .build(),
        }
    }

    pub async fn reader(&self, inode: u64) -> Result<Download, UnrealError> {
        let avail = self.cache.file_availability(inode).await?;
        if avail.is_empty() {
            return Err(UnrealError::NotFound);
        }

        let (client, peer, entry) = self.choose(avail).await?;

        Ok(Download::new(client, peer, entry))
    }

    async fn choose(
        &self,
        avail: Vec<(Peer, FileTableEntry)>,
    ) -> Result<(Arc<RealStoreClient>, Peer, FileTableEntry), UnrealError> {
        for (peer, entry) in &avail {
            if let Some(client) = self.clients.get(peer).await {
                // TODO: check if client is still connected, if no,
                // connecting to another client would be better.
                log::debug!(
                    "Reusing connection to {peer} to download {:?}",
                    entry.content,
                );
                return Ok((client, peer.clone(), entry.clone()));
            }
        }

        let mut set = JoinSet::new();
        for (peer, entry) in avail {
            let networking = self.networking.clone();
            set.spawn(async move {
                let client =
                    realstore::client::connect(&networking, &peer, ClientOptions::default())
                        .await?;

                log::debug!("Connected to {peer} to download {:?}", entry.content);

                Ok::<_, anyhow::Error>((client, peer, entry))
            });
        }
        while let Some(res) = set.join_next().await {
            if let Ok(res) = res {
                if let Ok((client, peer, entry)) = res {
                    let client = Arc::new(client);
                    self.clients.insert(peer.clone(), Arc::clone(&client)).await;
                    // TODO: consider keeping any other successful
                    // connections here instead of discarding them.
                    return Ok((client, peer, entry));
                }
            }
        }
        Err(UnrealError::Unavailable)
    }
}

pub struct Download {
    client: Arc<RealStoreClient>,
    peer: Peer,
    entry: FileTableEntry,
    offset: u64,
    pending_seek: Option<u64>,
    avail: VecDeque<(ByteRange, Vec<u8>)>,
    pending: Option<(
        ByteRange,
        Pin<
            Box<
                dyn Future<Output = Result<Result<Vec<u8>, RealStoreError>, RpcError>>
                    + Send
                    + Sync,
            >,
        >,
    )>,
}

impl Download {
    fn new(client: Arc<RealStoreClient>, peer: Peer, entry: FileTableEntry) -> Self {
        Self {
            client,
            peer,
            entry,
            offset: 0,
            pending_seek: None,
            avail: VecDeque::new(),
            pending: None,
        }
    }

    /// Check whether the current offset is at or past the file end.
    pub fn at_end(&self) -> bool {
        self.offset >= self.entry.metadata.size
    }

    fn fill(&mut self, buf: &mut ReadBuf<'_>) -> bool {
        let requested = ByteRange::new(self.offset, self.offset + buf.remaining() as u64);
        let mut filled = false;
        while let Some((range, data)) = self.avail.front() {
            let intersection = requested.intersection(range);
            if !intersection.is_empty() && intersection.start == self.offset {
                log::debug!(
                    "From {}, return {:?} {}",
                    self.peer,
                    self.entry.content,
                    intersection
                );

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

    fn next_request(
        &mut self,
        bufsize: usize,
    ) -> (
        ByteRange,
        Pin<
            Box<
                dyn Future<Output = Result<Result<Vec<u8>, RealStoreError>, RpcError>>
                    + Send
                    + Sync,
            >,
        >,
    ) {
        let offset = self.offset;
        let end = self.entry.metadata.size;
        let next = self
            .avail
            .back()
            .map(|(range, _)| range.end)
            .unwrap_or(offset);

        let range = ByteRange::new(
            next,
            min(
                end,
                next + min(MAX_CHUNK_SIZE, max(MIN_CHUNK_SIZE, bufsize)) as u64,
            ),
        );

        log::debug!(
            "From {}, download {:?} {}",
            self.peer,
            self.entry.content,
            range
        );

        let fut = {
            let client = Arc::clone(&self.client);
            let arena = self.entry.content.arena.clone();
            let path = self.entry.content.path.clone();
            let range = range.clone();

            Box::pin(async move {
                client
                    .read(
                        context::current(),
                        arena,
                        path,
                        range,
                        real::Options::default(),
                    )
                    .await
            })
        };

        (range, fut)
    }

    fn inner_poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.offset >= self.entry.metadata.size || self.fill(buf) {
            return Poll::Ready(Ok(()));
        }
        let (range, mut fut) = match self.pending.take() {
            Some(existing) => existing,
            None => self.next_request(buf.capacity()),
        };
        match fut.as_mut().poll(cx) {
            Poll::Ready(Err(err)) => Poll::Ready(Err(rpc_to_io_error(err))),
            Poll::Ready(Ok(Err(err))) => Poll::Ready(Err(real_store_to_io_error(err))),
            Poll::Ready(Ok(Ok(data))) => {
                self.avail.push_back((range, data));

                self.inner_poll_read(cx, buf)
            }
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
        let end = self.entry.metadata.size;
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

fn rpc_to_io_error(err: RpcError) -> std::io::Error {
    match &err {
        RpcError::Shutdown => std::io::Error::new(ErrorKind::ConnectionReset, err),
        RpcError::Server(_) => std::io::Error::new(ErrorKind::ConnectionReset, err),
        RpcError::DeadlineExceeded => std::io::Error::new(ErrorKind::TimedOut, err),
        _ => std::io::Error::other(err),
    }
}

fn real_store_to_io_error(err: RealStoreError) -> std::io::Error {
    std::io::Error::other(err)
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use assert_fs::{
        TempDir,
        prelude::{FileWriteBin as _, FileWriteStr as _, PathChild as _},
    };
    use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};

    use crate::{
        model::{Arena, Path, UnixTime},
        network::{self, Server, hostport::HostPort},
        storage::{self, real::RealStore},
    };

    use super::*;

    struct Fixture {
        tempdir: TempDir,
        arena: Arena,
        _local: RealStore,
        _server: Arc<Server>,
        networking: Networking,
        cache: UnrealCacheAsync,
    }
    impl Fixture {
        async fn setup() -> anyhow::Result<Fixture> {
            let _ = env_logger::try_init();
            let tempdir = TempDir::new()?;
            let arena = Arena::from("test");
            let local = RealStore::new(vec![(arena.clone(), tempdir.path().to_path_buf())]);
            let mut server = Server::new(network::testing::server_networking()?);
            realstore::server::register(&mut server, local.clone());
            let server = Arc::new(server);
            let addr = server.listen(&HostPort::localhost(0)).await?;
            let networking = network::testing::client_networking(addr)?;

            let mut cache = storage::testing::in_memory_cache()?;
            cache.add_arena(&arena)?;

            Ok(Self {
                tempdir,
                arena,
                _local: local,
                _server: server,
                networking,
                cache: cache.into_async(),
            })
        }

        async fn add_file_with_content(&self, name: &str, content: &str) -> anyhow::Result<u64> {
            let path = Path::parse(name)?;
            let file = self.tempdir.child(path.name());
            file.write_str(content)?;

            self.add_file(path).await
        }

        async fn add_file(&self, path: Path) -> Result<u64, anyhow::Error> {
            let metadata = tokio::fs::metadata(path.within(self.tempdir.path())).await?;
            self.cache
                .link(
                    &network::testing::server_peer(),
                    &self.arena,
                    &path,
                    metadata.len(),
                    &UnixTime::from_system_time(metadata.modified()?)?,
                )
                .await?;
            let (inode, _) = self
                .cache
                .lookup_path(self.cache.arena_root(&self.arena)?, &path)
                .await?;

            Ok(inode)
        }
    }

    #[tokio::test]
    async fn read_small_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let inode = fixture
            .add_file_with_content("test.txt", "File content")
            .await?;
        let downloader = Downloader::new(fixture.networking.clone(), fixture.cache.clone());
        let mut reader = downloader.reader(inode).await?;

        assert_eq!("File content", read_string(&mut reader).await?);
        Ok(())
    }

    #[tokio::test]
    async fn seek() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let inode = fixture
            .add_file_with_content("test.txt", "baa, baa, black sheep")
            .await?;
        let downloader = Downloader::new(fixture.networking.clone(), fixture.cache.clone());
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

        Ok(())
    }

    #[tokio::test]
    async fn seek_past_end() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let inode = fixture
            .add_file_with_content("test.txt", "baa, baa, black sheep")
            .await?;
        let downloader = Downloader::new(fixture.networking.clone(), fixture.cache.clone());
        let mut reader = downloader.reader(inode).await?;

        reader.seek(SeekFrom::Start(100)).await?;
        assert_eq!("", read_string(&mut reader).await?);

        reader.seek(SeekFrom::End(5)).await?;
        assert_eq!("", read_string(&mut reader).await?);

        reader.seek(SeekFrom::Start(5)).await?;
        reader.seek(SeekFrom::Current(100)).await?;
        assert_eq!("", read_string(&mut reader).await?);

        Ok(())
    }

    #[tokio::test]
    async fn seek_before_start() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let inode = fixture
            .add_file_with_content("test.txt", "baa, baa, black sheep")
            .await?;
        let downloader = Downloader::new(fixture.networking.clone(), fixture.cache.clone());
        let mut reader = downloader.reader(inode).await?;

        assert!(reader.seek(SeekFrom::End(-100)).await.is_err());
        assert!(reader.seek(SeekFrom::Current(-100)).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn seek_mixed_with_read() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let path = Path::parse("large_file")?;
        {
            let mut f = std::fs::File::create(path.within(fixture.tempdir.path()))?;
            f.write(&[1u8; 8 * 1024])?;
            f.write(&[0u8; 8 * 1024])?;
            f.write(&[2u8; 8 * 1024])?;
        }

        let inode = fixture.add_file(path).await?;
        let downloader = Downloader::new(fixture.networking.clone(), fixture.cache.clone());
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

        Ok(())
    }

    #[tokio::test]
    async fn read_large_file_small_buffer() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        test_read_file(&fixture, 12 * 1024, 1024, false).await?;
        Ok(())
    }

    #[tokio::test]
    async fn read_large_file_unusual_buffer_size() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        test_read_file(&fixture, 12 * 1024, 2000, true).await?;
        Ok(())
    }

    #[tokio::test]
    async fn read_large_file_large_buffer() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        test_read_file(&fixture, 48 * 1024, 32 * 1024, false).await?;
        Ok(())
    }

    #[tokio::test]
    async fn read_large_file_overly_large_buffer() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        test_read_file(&fixture, 64 * 1024, 48 * 1024, true).await?;
        Ok(())
    }

    async fn test_read_file(
        fixture: &Fixture,
        file_size: usize,
        buf_size: usize,
        allow_short_reads: bool,
    ) -> anyhow::Result<()> {
        let path = Path::parse("large_file")?;
        fixture
            .tempdir
            .child(path.to_string())
            .write_binary(&vec![0xAB; file_size])?;

        let inode = fixture.add_file(path).await?;
        let downloader = Downloader::new(fixture.networking.clone(), fixture.cache.clone());
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
