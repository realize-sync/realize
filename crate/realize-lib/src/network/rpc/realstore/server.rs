//! RealStoreService server implementation for Realize - Symmetric File Syncer
//!
//! This module implements the RealStoreService trait, providing directory management,
//! file operations, and in-process server/client utilities. It is robust to interruptions
//! and supports secure, restartable sync.

use crate::model;
use crate::model::Arena;
use crate::model::ByteRange;
use crate::model::Hash;
use crate::network::rpc::realstore::metrics::{MetricsRealizeClient, MetricsRealizeServer};
use crate::network::rpc::realstore::Config;
use crate::network::rpc::realstore::Options;
use crate::network::rpc::realstore::{
    RealStoreService, RealStoreServiceError, RsyncOperation, SyncedFile,
};
use crate::network::rpc::realstore::{
    RealStoreServiceClient, RealStoreServiceRequest, RealStoreServiceResponse,
};
use crate::network::Server;
use crate::storage::real::LocalStorage;
use crate::storage::real::PathResolver;
use crate::storage::real::PathType;
use crate::storage::real::StorageAccess;
use crate::utils::hash;
use async_speed_limit::clock::StandardClock;
use async_speed_limit::Limiter;
use fast_rsync::{
    apply_limited as rsync_apply_limited, diff as rsync_diff, Signature as RsyncSignature,
    SignatureOptions,
};
use futures::StreamExt;
use std::cmp::min;
use std::os::unix::fs::MetadataExt as _;
use std::path::Path;
use tarpc::client::stub::Stub;
use tarpc::client::RpcError;
use tarpc::context;
use tarpc::server::Channel;
use tarpc::tokio_serde::formats::Bincode;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncSeekExt as _;
use tokio::io::AsyncWriteExt as _;
use tokio::io::SeekFrom;
use tokio::task::JoinError;
use walkdir::WalkDir;

// Move this to the top-level, outside any impl
const RSYNC_BLOCK_SIZE: usize = 4096;

/// Type shortcut for client type.
pub type InProcessRealStoreServiceClient = RealStoreServiceClient<InProcessStub>;

/// Creates a in-process client that works on the given directories.
pub fn create_inprocess_client(storage: LocalStorage) -> InProcessRealStoreServiceClient {
    RealizeServer::new(storage).as_inprocess_client()
}

pub struct InProcessStub {
    inner: MetricsRealizeClient<
        tarpc::client::Channel<RealStoreServiceRequest, RealStoreServiceResponse>,
    >,
}

impl Stub for InProcessStub {
    type Req = RealStoreServiceRequest;
    type Resp = RealStoreServiceResponse;

    async fn call(
        &self,
        ctx: context::Context,
        request: RealStoreServiceRequest,
    ) -> std::result::Result<RealStoreServiceResponse, RpcError> {
        self.inner.call(ctx, request).await
    }
}

pub fn register(server: &mut Server, realize_storage: LocalStorage) {
    server.register_server(super::TAG, move |_peer, limiter, framed| {
        let storage = realize_storage.clone();
        let transport = tarpc::serde_transport::new(framed, Bincode::default());
        let server = RealizeServer::new_limited(storage, limiter);
        let serve_fn = MetricsRealizeServer::new(RealizeServer::serve(server.clone()));

        tarpc::server::BaseChannel::with_defaults(transport).execute(serve_fn)
    });
}

#[derive(Clone)]
pub(crate) struct RealizeServer {
    pub(crate) storage: LocalStorage,
    pub(crate) limiter: Option<Limiter<StandardClock>>,
}

impl RealizeServer {
    pub(crate) fn new(storage: LocalStorage) -> Self {
        Self {
            storage,
            limiter: None,
        }
    }

    pub(crate) fn new_limited(storage: LocalStorage, limiter: Limiter<StandardClock>) -> Self {
        Self {
            storage,
            limiter: Some(limiter),
        }
    }

    fn path_resolver(
        &self,
        arena: &Arena,
        opts: &Options,
    ) -> Result<PathResolver, RealStoreServiceError> {
        self.storage
            .path_resolver(
                arena,
                if opts.ignore_partial {
                    StorageAccess::Read
                } else {
                    StorageAccess::ReadWrite
                },
            )
            .ok_or(unknown_arena())
    }

    /// Create an in-process RealStoreServiceClient for this server instance.
    pub(crate) fn as_inprocess_client(self) -> InProcessRealStoreServiceClient {
        let (client_transport, server_transport) = tarpc::transport::channel::unbounded();
        let server = tarpc::server::BaseChannel::with_defaults(server_transport);
        tokio::spawn(
            server
                .execute(MetricsRealizeServer::new(RealizeServer::serve(self)))
                .for_each(|fut| async move {
                    tokio::spawn(fut);
                }),
        );
        let client = tarpc::client::new(tarpc::client::Config::default(), client_transport).spawn();
        let stub = InProcessStub {
            inner: MetricsRealizeClient::new(client),
        };

        RealStoreServiceClient::from(stub)
    }
}

impl RealStoreService for RealizeServer {
    // IMPORTANT: Use async tokio::fs operations or use
    // spawn_blocking, do *not* use blocking std::fs operations
    // outside of spawn_blocking.

    async fn list(
        self,
        _: tarpc::context::Context,
        arena: Arena,
        options: Options,
    ) -> Result<Vec<SyncedFile>, RealStoreServiceError> {
        let resolver = self.path_resolver(&arena, &options)?.clone();

        tokio::task::spawn_blocking(move || {
            let mut files = std::collections::BTreeMap::new();
            for entry in WalkDir::new(resolver.root()).into_iter().flatten() {
                if !entry.metadata().is_ok_and(|m| m.is_file()) {
                    continue;
                }
                if let Some((path_type, path)) = resolver.reverse(entry.path()) {
                    if path_type == PathType::Final && files.contains_key(&path) {
                        continue;
                    }
                    let metadata = entry.metadata()?;
                    let size = metadata.size();
                    let mtime = metadata.modified().expect("OS must support mtime");
                    files.insert(path.clone(), SyncedFile { path, size, mtime });
                }
            }

            Ok::<Vec<SyncedFile>, RealStoreServiceError>(files.into_values().collect())
        })
        .await?
    }

    async fn read(
        self,
        _: tarpc::context::Context,
        arena: Arena,
        relative_path: model::Path,
        range: ByteRange,
        options: Options,
    ) -> Result<Vec<u8>, RealStoreServiceError> {
        let resolver = self.path_resolver(&arena, &options)?;
        let actual = resolver.resolve(&relative_path).await?;
        let mut file = open_for_range_read(&actual).await?;
        let mut buffer = vec![0; range.bytecount() as usize];
        read_padded(&mut file, &range, &mut buffer).await?;

        Ok(buffer)
    }

    async fn send(
        self,
        _: tarpc::context::Context,
        arena: Arena,
        relative_path: model::Path,
        range: ByteRange,
        data: Vec<u8>,
        options: Options,
    ) -> Result<(), RealStoreServiceError> {
        let resolver = self.path_resolver(&arena, &options)?;
        let path = resolver.partial_path(&relative_path).ok_or(bad_path())?;
        let mut file = open_for_range_write(&path).await?;
        write_at_offset(&mut file, range.start, &data).await?;

        Ok(())
    }

    async fn finish(
        self,
        _: tarpc::context::Context,
        arena: Arena,
        relative_path: model::Path,
        options: Options,
    ) -> Result<(), RealStoreServiceError> {
        if options.ignore_partial {
            return Err(RealStoreServiceError::BadRequest(
                "Invalid option for finish: ignore_partial=true".to_string(),
            ));
        }
        let resolver = self.path_resolver(&arena, &options)?;
        let partial_path = resolver.partial_path(&relative_path).ok_or(bad_path())?;
        let actual = resolver.resolve(&relative_path).await?;
        if actual == partial_path {
            fs::rename(
                actual,
                resolver.final_path(&relative_path).ok_or(bad_path())?,
            )
            .await?;
        }
        Ok(())
    }

    async fn hash(
        self,
        _ctx: tarpc::context::Context,
        arena: Arena,
        relative_path: model::Path,
        range: ByteRange,
        options: Options,
    ) -> Result<Hash, RealStoreServiceError> {
        let resolver = self.path_resolver(&arena, &options)?;
        let path = resolver.resolve(&relative_path).await?;

        hash_large_range_exact(&path, &range).await
    }

    async fn delete(
        self,
        _ctx: tarpc::context::Context,
        arena: Arena,
        relative_path: model::Path,
        options: Options,
    ) -> Result<(), RealStoreServiceError> {
        let resolver = self.path_resolver(&arena, &options)?;
        let final_path = resolver.final_path(&relative_path).ok_or(bad_path())?;
        if fs::metadata(&final_path).await.is_ok() {
            fs::remove_file(final_path).await?;
        }

        let partial_path = resolver.partial_path(&relative_path).ok_or(bad_path())?;
        if fs::metadata(&partial_path).await.is_ok() {
            fs::remove_file(partial_path).await?;
        }
        delete_containing_dir(resolver.root(), &relative_path).await;
        Ok(())
    }

    async fn calculate_signature(
        self,
        _ctx: tarpc::context::Context,
        arena: Arena,
        relative_path: model::Path,
        range: ByteRange,
        options: Options,
    ) -> Result<crate::model::Signature, RealStoreServiceError> {
        let resolver = self.path_resolver(&arena, &options)?;
        let actual = resolver.resolve(&relative_path).await?;
        let mut file = File::open(&actual).await?;
        let mut buffer = vec![0u8; range.bytecount() as usize];
        read_padded(&mut file, &range, &mut buffer).await?;
        let opts = SignatureOptions {
            block_size: RSYNC_BLOCK_SIZE as u32,
            crypto_hash_size: 8,
        };
        let sig = RsyncSignature::calculate(&buffer, opts);
        Ok(crate::model::Signature(sig.into_serialized()))
    }

    async fn diff(
        self,
        _ctx: tarpc::context::Context,
        arena: Arena,
        relative_path: model::Path,
        range: ByteRange,
        signature: crate::model::Signature,
        options: Options,
    ) -> Result<(crate::model::Delta, Hash), RealStoreServiceError> {
        let resolver = self.path_resolver(&arena, &options)?;
        let actual = resolver.resolve(&relative_path).await?;
        let mut file = open_for_range_read(&actual).await?;
        let mut buffer = vec![0; range.bytecount() as usize];
        read_padded(&mut file, &range, &mut buffer).await?;

        let hash = hash::digest(&buffer);

        let sig = RsyncSignature::deserialize(signature.0)?;
        let mut delta = Vec::new();
        rsync_diff(&sig.index(), &buffer, &mut delta)?;

        Ok((crate::model::Delta(delta), hash))
    }

    async fn apply_delta(
        self,
        _ctx: tarpc::context::Context,
        arena: Arena,
        relative_path: model::Path,
        range: ByteRange,
        delta: crate::model::Delta,
        hash: Hash,
        options: Options,
    ) -> Result<(), RealStoreServiceError> {
        let resolver = self.path_resolver(&arena, &options)?;
        let path = resolver.partial_path(&relative_path).ok_or(bad_path())?;
        let mut file = open_for_range_write(&path).await?;
        let mut base = vec![0u8; range.bytecount() as usize];
        read_padded(&mut file, &range, &mut base).await?;
        let mut out = Vec::new();
        rsync_apply_limited(&base, &delta.0, &mut out, range.bytecount() as usize)?;
        if out.len() as u64 != range.bytecount() {
            return Err(RealStoreServiceError::BadRequest(
                "Delta output size mismatch".to_string(),
            ));
        }
        if hash::digest(&out) != hash {
            return Err(RealStoreServiceError::HashMismatch);
        }
        write_at_offset(&mut file, range.start, &out).await?;

        return Ok(());
    }

    async fn truncate(
        self,
        _ctx: tarpc::context::Context,
        arena: Arena,
        relative_path: model::Path,
        file_size: u64,
        options: Options,
    ) -> Result<(), RealStoreServiceError> {
        let resolver = self.path_resolver(&arena, &options)?;
        let path = resolver.partial_path(&relative_path).ok_or(bad_path())?;
        let mut file = open_for_range_write(&path).await?;
        shorten_file(&mut file, file_size).await?;

        return Ok(());
    }

    async fn configure(
        self,
        _ctx: tarpc::context::Context,
        config: crate::network::rpc::realstore::Config,
    ) -> Result<crate::network::rpc::realstore::Config, RealStoreServiceError> {
        if let (Some(limiter), Some(limit)) = (self.limiter.as_ref(), config.write_limit) {
            limiter.set_speed_limit(limit as f64);
        }
        Ok(Config {
            write_limit: self.limiter.as_ref().and_then(|l| {
                let lim = l.speed_limit();
                if lim.is_finite() {
                    Some(lim as u64)
                } else {
                    None
                }
            }),
        })
    }
}

async fn open_for_range_write(path: &Path) -> Result<File, RealStoreServiceError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    Ok(OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .truncate(false)
        .open(path)
        .await?)
}
async fn open_for_range_read(path: &Path) -> Result<File, RealStoreServiceError> {
    Ok(OpenOptions::new()
        .create(false)
        .read(true)
        .write(false)
        .open(path)
        .await?)
}

/// Hash a range that is too large to be kept in memory.
///
/// Return a zero hash if data is missing.
async fn hash_large_range_exact(
    path: &Path,
    range: &ByteRange,
) -> Result<Hash, RealStoreServiceError> {
    let mut file = open_for_range_read(&path).await?;
    let file_size = file.metadata().await?.len();
    if range.end > file_size {
        // We return an empty hash instead of failing, because we
        // want hash comparison to fail if the file isn't of the
        // expected size, not get an I/O error.
        return Ok(Hash::zero());
    }

    file.seek(std::io::SeekFrom::Start(range.start)).await?;

    let mut hasher = hash::running();

    // Using a large buffer as async operations are more
    // expensive. Using a spawn_block would make the computation
    // of the hash run on the limited block threads, which is also
    // a problem.
    let mut buffer = vec![0u8; 1024 * 1024];
    let mut limited = file.take(range.bytecount());
    loop {
        let n = limited.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[0..n]);
    }

    Ok(hasher.finalize())
}

async fn read_padded(
    file: &mut File,
    range: &ByteRange,
    buf: &mut [u8],
) -> std::result::Result<(), std::io::Error> {
    let initial_size = file.metadata().await?.size();
    if initial_size > range.start {
        file.seek(std::io::SeekFrom::Start(range.start)).await?;
        let read_end = (min(range.end, initial_size) - range.start) as usize;
        file.read_exact(&mut buf[0..read_end]).await?;
        buf[read_end..].fill(0);
    } else {
        buf.fill(0);
    }
    Ok(())
}

async fn shorten_file(file: &mut File, file_size: u64) -> Result<(), RealStoreServiceError> {
    if file.metadata().await?.len() > file_size {
        file.set_len(file_size).await?;
        file.flush().await?;
        file.sync_all().await?;
    }

    Ok(())
}

async fn write_at_offset(
    file: &mut File,
    offset: u64,
    data: &Vec<u8>,
) -> Result<(), RealStoreServiceError> {
    file.seek(SeekFrom::Start(offset)).await?;
    file.write_all(&data).await?;
    file.flush().await?;
    file.sync_all().await?;

    Ok(())
}

impl From<walkdir::Error> for RealStoreServiceError {
    fn from(err: walkdir::Error) -> Self {
        if err.io_error().is_some() {
            // We know err contains an io error; unwrap will succeed.
            err.into_io_error().unwrap().into()
        } else {
            anyhow::Error::new(err).into()
        }
    }
}

impl From<JoinError> for RealStoreServiceError {
    fn from(err: JoinError) -> Self {
        anyhow::Error::new(err).into()
    }
}

// Add error conversions for fast_rsync errors
impl From<fast_rsync::DiffError> for RealStoreServiceError {
    fn from(e: fast_rsync::DiffError) -> Self {
        RealStoreServiceError::Rsync(RsyncOperation::Diff, e.to_string())
    }
}
impl From<fast_rsync::ApplyError> for RealStoreServiceError {
    fn from(e: fast_rsync::ApplyError) -> Self {
        RealStoreServiceError::Rsync(RsyncOperation::Apply, e.to_string())
    }
}

impl From<fast_rsync::SignatureParseError> for RealStoreServiceError {
    fn from(e: fast_rsync::SignatureParseError) -> Self {
        RealStoreServiceError::Rsync(RsyncOperation::Sign, e.to_string())
    }
}

/// Remove empty parent directories of [relative_path].
///
/// Errors are ignored. Deleting just stops.
async fn delete_containing_dir(root: &Path, relative_path: &model::Path) {
    let mut current = relative_path.clone();
    while let Some(parent) = current.parent() {
        let full_path = parent.within(root);
        let is_empty = is_empty_dir(&full_path).await;
        if !is_empty || fs::remove_dir(full_path).await.is_err() {
            return;
        }
        current = parent;
    }
}

async fn is_empty_dir(path: &Path) -> bool {
    let path = path.to_path_buf();
    let ret = tokio::task::spawn_blocking(move || {
        std::fs::read_dir(path)
            .map(|mut i| i.next().is_none())
            .unwrap_or(false)
    })
    .await;

    if let Ok(is_empty) = ret {
        return is_empty;
    }
    return false;
}

fn unknown_arena() -> RealStoreServiceError {
    RealStoreServiceError::BadRequest("Unknown arena".to_string())
}

fn bad_path() -> RealStoreServiceError {
    RealStoreServiceError::BadRequest("Path is invalid".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Hash;
    use assert_fs::prelude::*;
    use assert_fs::TempDir;
    use assert_unordered::assert_eq_unordered;
    use std::ffi::OsString;
    use std::fs;
    use std::io::Read as _;
    use std::path;
    use std::path::PathBuf;

    struct LogicalPath {
        arena_path: PathBuf,
        path: model::Path,
    }

    impl LogicalPath {
        fn new(arena_path: &path::Path, path: &model::Path) -> Self {
            Self {
                arena_path: arena_path.to_path_buf(),
                path: path.clone(),
            }
        }

        fn final_path(&self) -> PathBuf {
            self.path.within(&self.arena_path)
        }

        fn partial_path(&self) -> PathBuf {
            let mut path = self.final_path();
            let mut part_filename = OsString::from(".");
            // Since the path is built from a model path, there will
            // be a filename.
            part_filename.push(path.file_name().expect("no file name"));
            part_filename.push(OsString::from(".part"));
            path.set_file_name(part_filename);

            path
        }
    }

    fn setup_server() -> anyhow::Result<(RealizeServer, TempDir, Arena)> {
        let temp = TempDir::new()?;
        let arena = Arena::from("testdir");

        Ok((
            RealizeServer::new(LocalStorage::single(&arena, temp.path())),
            temp,
            arena,
        ))
    }

    #[tokio::test]
    async fn list_empty() -> anyhow::Result<()> {
        let (server, _temp, arena) = setup_server()?;
        let files = server
            .clone()
            .list(tarpc::context::current(), arena.clone(), Options::default())
            .await?;
        assert!(files.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn list_files_and_partial() -> anyhow::Result<()> {
        let (server, temp, arena) = setup_server()?;

        fs::create_dir_all(temp.child("subdir"))?;

        let foo = temp.child("foo.txt");
        foo.write_str("hello")?;
        let foo2 = temp.child("subdir/foo2.txt");
        foo2.write_str("hello")?;
        let bar = temp.child(".bar.txt.part");
        bar.write_str("partial")?;
        let bar2 = temp.child("subdir/.bar2.txt.part");
        bar2.write_str("partial")?;

        let files = server
            .list(tarpc::context::current(), arena.clone(), Options::default())
            .await?;
        assert_eq_unordered!(
            files,
            vec![
                SyncedFile {
                    path: model::Path::parse("foo.txt")?,
                    size: 5,
                    mtime: foo.metadata()?.modified()?,
                },
                SyncedFile {
                    path: model::Path::parse("subdir/foo2.txt")?,
                    size: 5,
                    mtime: foo2.metadata()?.modified()?,
                },
                SyncedFile {
                    path: model::Path::parse("bar.txt")?,
                    size: 7,
                    mtime: bar.metadata()?.modified()?,
                },
                SyncedFile {
                    path: model::Path::parse("subdir/bar2.txt")?,
                    size: 7,
                    mtime: bar2.metadata()?.modified()?,
                },
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn send_wrong_order() -> anyhow::Result<()> {
        let (server, temp, arena) = setup_server()?;
        let fpath = LogicalPath::new(temp.path(), &model::Path::parse("wrong_order.txt")?);
        let data1 = b"fghij".to_vec();
        server
            .clone()
            .send(
                tarpc::context::current(),
                arena.clone(),
                fpath.path.clone(),
                ByteRange { start: 5, end: 10 },
                data1.clone(),
                Options::default(),
            )
            .await?;
        assert!(fpath.partial_path().exists());
        assert!(!fpath.final_path().exists());
        assert_eq!(
            std::fs::read_to_string(fpath.partial_path())?,
            "\0\0\0\0\0fghij"
        );
        let data2 = b"abcde".to_vec();
        server
            .clone()
            .send(
                tarpc::context::current(),
                arena.clone(),
                fpath.path.clone(),
                ByteRange { start: 0, end: 5 },
                data2.clone(),
                Options::default(),
            )
            .await?;
        assert!(fpath.partial_path().exists());
        assert!(!fpath.final_path().exists());
        assert_eq!(std::fs::read_to_string(fpath.partial_path())?, "abcdefghij");
        Ok(())
    }

    #[tokio::test]
    async fn read() -> anyhow::Result<()> {
        let (server, temp, arena) = setup_server()?;

        let fpath = LogicalPath::new(temp.path(), &model::Path::parse("wrong_order.txt")?);
        fs::write(fpath.final_path(), "abcdefghij")?;

        let data = server
            .clone()
            .read(
                tarpc::context::current(),
                arena.clone(),
                fpath.path.clone(),
                ByteRange { start: 5, end: 10 },
                Options::default(),
            )
            .await?;
        assert_eq!(String::from_utf8(data)?, "fghij");

        let data = server
            .clone()
            .read(
                tarpc::context::current(),
                arena.clone(),
                fpath.path.clone(),
                ByteRange { start: 0, end: 5 },
                Options::default(),
            )
            .await?;
        assert_eq!(String::from_utf8(data)?, "abcde");

        let result = server
            .clone()
            .read(
                tarpc::context::current(),
                arena.clone(),
                fpath.path.clone(),
                ByteRange { start: 0, end: 15 },
                Options::default(),
            )
            .await?;
        assert_eq!(result.len(), 15);
        assert_eq!(result, b"abcdefghij\0\0\0\0\0");

        Ok(())
    }

    #[tokio::test]
    async fn finish_partial() -> anyhow::Result<()> {
        let (server, temp, arena) = setup_server()?;

        let fpath = LogicalPath::new(temp.path(), &model::Path::parse("finish_partial.txt")?);
        fs::write(fpath.partial_path(), "abcde")?;

        server
            .finish(
                tarpc::context::current(),
                arena.clone(),
                fpath.path.clone(),
                Options::default(),
            )
            .await?;

        assert!(fpath.final_path().exists());
        assert!(!fpath.partial_path().exists());
        assert_eq!(std::fs::read_to_string(fpath.final_path())?, "abcde");

        Ok(())
    }

    #[tokio::test]
    async fn finish_final() -> anyhow::Result<()> {
        let (server, temp, arena) = setup_server()?;

        let fpath = LogicalPath::new(temp.path(), &model::Path::parse("finish_partial.txt")?);
        fs::write(fpath.final_path(), "abcde")?;

        server
            .finish(
                tarpc::context::current(),
                arena.clone(),
                fpath.path.clone(),
                Options::default(),
            )
            .await?;

        assert!(fpath.final_path().exists());
        assert!(!fpath.partial_path().exists());
        assert_eq!(std::fs::read_to_string(fpath.final_path())?, "abcde");

        Ok(())
    }

    #[tokio::test]
    async fn truncate() -> anyhow::Result<()> {
        let (server, temp, arena) = setup_server()?;
        let fpath = LogicalPath::new(temp.path(), &model::Path::parse("truncate.txt")?);
        std::fs::write(fpath.partial_path(), b"abcdefghij")?;
        server
            .clone()
            .truncate(
                tarpc::context::current(),
                arena.clone(),
                fpath.path.clone(),
                7,
                Options::default(),
            )
            .await?;
        let content = std::fs::read_to_string(fpath.partial_path())?;
        assert_eq!(content, "abcdefg");
        Ok(())
    }

    #[tokio::test]
    async fn hash_final_and_partial() -> anyhow::Result<()> {
        let (server, temp, arena) = setup_server()?;
        let content = b"hello world";
        let fpath = LogicalPath::new(temp.path(), &model::Path::parse("foo.txt")?);
        std::fs::write(fpath.final_path(), content)?;
        let hash = server
            .clone()
            .hash(
                tarpc::context::current(),
                arena.clone(),
                model::Path::parse("foo.txt")?,
                ByteRange {
                    start: 0,
                    end: content.len() as u64,
                },
                Options::default(),
            )
            .await?;
        let expected = hash::digest(content);
        assert_eq!(hash, expected);

        // Now test partial
        let content2 = b"partial content";
        let fpath2 = LogicalPath::new(temp.path(), &model::Path::parse("bar.txt")?);
        std::fs::write(fpath2.partial_path(), content2)?;
        let hash2 = server
            .clone()
            .hash(
                tarpc::context::current(),
                arena.clone(),
                model::Path::parse("bar.txt")?,
                ByteRange {
                    start: 0,
                    end: content2.len() as u64,
                },
                Options::default(),
            )
            .await?;
        let expected2 = hash::digest(content2);
        assert_eq!(hash2, expected2);
        Ok(())
    }

    #[tokio::test]
    async fn hash_past_file_end() -> anyhow::Result<()> {
        let (server, temp, arena) = setup_server()?;
        let content = b"hello world";
        let fpath = LogicalPath::new(temp.path(), &model::Path::parse("foo.txt")?);
        std::fs::write(fpath.final_path(), content)?;
        let hash = server
            .clone()
            .hash(
                tarpc::context::current(),
                arena.clone(),
                model::Path::parse("foo.txt")?,
                ByteRange {
                    start: 100,
                    end: 200,
                },
                Options::default(),
            )
            .await?;
        assert_eq!(hash, Hash::zero());

        Ok(())
    }

    #[tokio::test]
    async fn hash_short_read() -> anyhow::Result<()> {
        let (server, temp, arena) = setup_server()?;
        let content = b"hello world";
        let fpath = LogicalPath::new(temp.path(), &model::Path::parse("foo.txt")?);
        std::fs::write(fpath.final_path(), content)?;
        let hash = server
            .clone()
            .hash(
                tarpc::context::current(),
                arena.clone(),
                model::Path::parse("foo.txt")?,
                ByteRange { start: 0, end: 200 },
                Options::default(),
            )
            .await?;
        assert_eq!(hash, Hash::zero());

        Ok(())
    }

    #[tokio::test]
    async fn delete_final_and_partial() -> anyhow::Result<()> {
        let (server, temp, arena) = setup_server()?;
        let fpath = LogicalPath::new(temp.path(), &model::Path::parse("foo.txt")?);
        std::fs::write(fpath.final_path(), b"data")?;
        std::fs::write(fpath.partial_path(), b"data2")?;
        assert!(fpath.final_path().exists());
        assert!(fpath.partial_path().exists());
        server
            .clone()
            .delete(
                tarpc::context::current(),
                arena.clone(),
                model::Path::parse("foo.txt")?,
                Options::default(),
            )
            .await?;
        assert!(!fpath.final_path().exists());
        assert!(!fpath.partial_path().exists());
        // Should succeed if called again (idempotent)
        server
            .clone()
            .delete(
                tarpc::context::current(),
                arena.clone(),
                model::Path::parse("foo.txt")?,
                Options::default(),
            )
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn tarpc_rpc_inprocess() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let client =
            create_inprocess_client(LocalStorage::single(&Arena::from("testdir"), temp.path()));
        let list = client
            .list(
                tarpc::context::current(),
                Arena::from("testdir"),
                Options::default(),
            )
            .await??;
        assert_eq!(list.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn calculate_signature_and_diff_and_apply_delta() -> anyhow::Result<()> {
        let (server, temp, arena) = setup_server()?;
        let file_path = model::Path::parse("foo.txt")?;
        let file_content = b"hello world, this is a test of rsync signature!";
        temp.child("foo.txt").write_binary(file_content)?;

        // Calculate signature for the whole file
        let sig = server
            .clone()
            .calculate_signature(
                tarpc::context::current(),
                arena.clone(),
                file_path.clone(),
                ByteRange {
                    start: 0,
                    end: file_content.len() as u64,
                },
                Options::default(),
            )
            .await?;

        // Change file content and write to a new file
        let new_content = b"hello world, this is A tost of rsync signature! (changed)";
        temp.child("foo.txt").write_binary(new_content)?;

        // Diff: get delta from old signature to new content
        let delta = server
            .clone()
            .diff(
                tarpc::context::current(),
                arena.clone(),
                file_path.clone(),
                ByteRange {
                    start: 0,
                    end: new_content.len() as u64,
                },
                sig.clone(),
                Options::default(),
            )
            .await?;
        assert!(!delta.0 .0.is_empty());

        // Revert file to old content, then apply delta
        temp.child(".foo.txt.part").write_binary(file_content)?;
        let res = server
            .clone()
            .apply_delta(
                tarpc::context::current(),
                arena.clone(),
                file_path.clone(),
                ByteRange {
                    start: 0,
                    end: new_content.len() as u64,
                },
                delta.0,
                delta.1,
                Options::default(),
            )
            .await;
        assert!(res.is_ok());
        let mut buf = Vec::new();
        std::fs::File::open(temp.child(".foo.txt.part").path())?.read_to_end(&mut buf)?;
        assert_eq!(&buf[..new_content.len()], new_content);

        Ok(())
    }

    #[tokio::test]
    async fn signature_on_nonexisting_file() -> anyhow::Result<()> {
        let (server, _temp, arena) = setup_server()?;
        // Nonexistent file
        let result = server
            .clone()
            .calculate_signature(
                tarpc::context::current(),
                arena.clone(),
                model::Path::parse("notfound.txt")?,
                ByteRange { start: 0, end: 10 },
                Options::default(),
            )
            .await;
        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn apply_delta_error_case() -> anyhow::Result<()> {
        let (server, temp, arena) = setup_server()?;
        let file_path = model::Path::parse("foo.txt")?;
        temp.child("foo.txt").write_str("abc")?;
        // Try to apply a bogus delta
        let result = server
            .clone()
            .apply_delta(
                tarpc::context::current(),
                arena.clone(),
                file_path,
                ByteRange { start: 0, end: 3 },
                crate::model::Delta(vec![1, 2, 3]),
                Hash::zero(),
                Options::default(),
            )
            .await;
        if let Err(ref e) = result {
            let msg = format!("{e}");
            println!("Error: {msg}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn list_partial_vs_final() -> anyhow::Result<()> {
        let (server, temp, arena) = setup_server()?;
        temp.child("foo.txt").write_str("final")?;
        temp.child(".foo.txt.part").write_str("partial")?;
        // Default: partial preferred
        let files = server
            .clone()
            .list(
                tarpc::context::current(),
                arena.clone(),
                Options {
                    ignore_partial: false,
                },
            )
            .await?;
        assert_eq!(files.len(), 1);
        // With ignore_partial: final preferred
        let files = server
            .clone()
            .list(
                tarpc::context::current(),
                arena.clone(),
                Options {
                    ignore_partial: true,
                },
            )
            .await?;
        assert_eq!(files.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn list_ignore_partial() -> anyhow::Result<()> {
        let (server, temp, arena) = setup_server()?;
        temp.child(".foo.txt.part").write_str("partial")?;
        // Default: return partial
        let files = server
            .clone()
            .list(
                tarpc::context::current(),
                arena.clone(),
                Options {
                    ignore_partial: false,
                },
            )
            .await?;
        assert_eq!(files.len(), 1);
        // With ignore_partial: don't return partial
        let files = server
            .clone()
            .list(
                tarpc::context::current(),
                arena.clone(),
                Options {
                    ignore_partial: true,
                },
            )
            .await?;
        assert_eq!(files.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn read_partial_vs_final() -> anyhow::Result<()> {
        let (server, temp, arena) = setup_server()?;
        temp.child("foo.txt").write_str("final")?;
        temp.child(".foo.txt.part").write_str("partial")?;
        // Default: partial preferred
        let data = server
            .clone()
            .read(
                tarpc::context::current(),
                arena.clone(),
                model::Path::parse("foo.txt")?,
                ByteRange { start: 0, end: 7 },
                Options {
                    ignore_partial: false,
                },
            )
            .await?;
        assert_eq!(String::from_utf8(data)?, "partial");
        // With ignore_partial: final preferred
        let data = server
            .clone()
            .read(
                tarpc::context::current(),
                arena.clone(),
                model::Path::parse("foo.txt")?,
                ByteRange { start: 0, end: 5 },
                Options {
                    ignore_partial: true,
                },
            )
            .await?;
        assert_eq!(String::from_utf8(data)?, "final");
        Ok(())
    }

    #[tokio::test]
    async fn delete_removes_empty_parent_dirs() -> anyhow::Result<()> {
        let (server, temp, arena) = setup_server()?;
        // Create nested directories: a/b/c/file.txt
        let nested_dir = temp.child("a/b/c");
        std::fs::create_dir_all(nested_dir.path())?;
        let file_path = model::Path::parse("a/b/c/file.txt")?;
        let logical = LogicalPath::new(temp.path(), &file_path);
        std::fs::write(logical.final_path(), b"data")?;
        // Delete the file
        server
            .clone()
            .delete(
                tarpc::context::current(),
                arena.clone(),
                file_path.clone(),
                Options::default(),
            )
            .await?;
        // All parent dirs except the root should be deleted
        assert!(!logical.final_path().exists());
        assert!(!temp.child("a/b/c").exists());
        assert!(!temp.child("a/b").exists());
        assert!(!temp.child("a").exists());
        // Root dir should still exist
        assert!(temp.path().exists());
        Ok(())
    }

    #[tokio::test]
    async fn delete_does_not_remove_nonempty_parent_dirs() -> anyhow::Result<()> {
        let (server, temp, arena) = setup_server()?;
        // Create nested directories: a/b/c/file.txt and a/b/c/keep.txt
        let nested_dir = temp.child("a/b/c");
        std::fs::create_dir_all(nested_dir.path())?;
        let file_path = model::Path::parse("a/b/c/file.txt")?;
        let keep_path = temp.child("a/b/c/keep.txt");
        let logical = LogicalPath::new(temp.path(), &file_path);
        std::fs::write(logical.final_path(), b"data")?;
        std::fs::write(keep_path.path(), b"keep")?;
        // Delete the file
        server
            .clone()
            .delete(
                tarpc::context::current(),
                arena.clone(),
                file_path.clone(),
                Options::default(),
            )
            .await?;
        // Only the file should be deleted, parent dirs should remain
        assert!(!logical.final_path().exists());
        assert!(temp.child("a/b/c").exists());
        assert!(temp.child("a/b").exists());
        assert!(temp.child("a").exists());
        assert!(temp.child("a/b/c/keep.txt").exists());
        Ok(())
    }

    #[tokio::test]
    async fn configure_noop_returns_none() {
        let server = RealizeServer::new(LocalStorage::single(
            &Arena::from("testdir"),
            &PathBuf::from("/tmp/testdir"),
        ));
        let returned = server
            .clone()
            .configure(
                tarpc::context::current(),
                Config {
                    write_limit: Some(12345),
                },
            )
            .await
            .unwrap();
        assert_eq!(returned.write_limit, None);
    }

    #[tokio::test]
    async fn configure_limited_sets_and_returns_limit() {
        let dirs = LocalStorage::single(&Arena::from("testdir"), &PathBuf::from("/tmp/testdir"));
        let limiter = Limiter::<StandardClock>::new(f64::INFINITY);
        let server = RealizeServer::new_limited(dirs, limiter.clone());
        let limit = 55555u64;
        let returned = server
            .clone()
            .configure(
                tarpc::context::current(),
                Config {
                    write_limit: Some(limit),
                },
            )
            .await
            .unwrap();
        assert_eq!(returned.write_limit, Some(limit));
    }
}
