//! RealizeService server implementation for Realize - Symmetric File Syncer
//!
//! This module implements the RealizeService trait, providing directory management,
//! file operations, and in-process server/client utilities. It is robust to interruptions
//! and supports secure, restartable sync.

use crate::config::{Arena, LocalArena, LocalArenas};
use crate::network::rpc::realize::metrics::{self, MetricsRealizeClient, MetricsRealizeServer};
use crate::network::rpc::realize::Options;
use crate::network::rpc::realize::{Config, Hash};
use crate::network::rpc::realize::{
    RealizeService, RealizeServiceError, Result, RsyncOperation, SyncedFile, SyncedFileState,
};
use crate::network::rpc::realize::{
    RealizeServiceClient, RealizeServiceRequest, RealizeServiceResponse,
};
use crate::utils::byterange::ByteRange;
use crate::utils::hash;
use async_speed_limit::clock::StandardClock;
use async_speed_limit::Limiter;
use fast_rsync::{
    apply_limited as rsync_apply_limited, diff as rsync_diff, Signature as RsyncSignature,
    SignatureOptions,
};
use futures::StreamExt;
use std::cmp::min;
use std::ffi::OsString;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tarpc::client::stub::Stub;
use tarpc::client::RpcError;
use tarpc::context;
use tarpc::server::Channel;
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
pub type InProcessRealizeServiceClient = RealizeServiceClient<InProcessStub>;

/// Creates a in-process client that works on the given directories.
pub fn create_inprocess_client(dirs: LocalArenas) -> InProcessRealizeServiceClient {
    RealizeServer::new(dirs).as_inprocess_client()
}

pub struct InProcessStub {
    inner:
        MetricsRealizeClient<tarpc::client::Channel<RealizeServiceRequest, RealizeServiceResponse>>,
}

impl Stub for InProcessStub {
    type Req = RealizeServiceRequest;
    type Resp = RealizeServiceResponse;

    async fn call(
        &self,
        ctx: context::Context,
        request: RealizeServiceRequest,
    ) -> std::result::Result<RealizeServiceResponse, RpcError> {
        self.inner.call(ctx, request).await
    }
}

#[derive(Clone)]
pub(crate) struct RealizeServer {
    pub(crate) dirs: LocalArenas,
    pub(crate) limiter: Option<Limiter<StandardClock>>,
}

impl RealizeServer {
    pub(crate) fn new(dirs: LocalArenas) -> Self {
        Self {
            dirs,
            limiter: None,
        }
    }

    pub(crate) fn new_limited(dirs: LocalArenas, limiter: Limiter<StandardClock>) -> Self {
        Self {
            dirs,
            limiter: Some(limiter),
        }
    }

    fn find_directory(&self, arena: &Arena) -> Result<&Arc<LocalArena>> {
        self.dirs.get(arena).ok_or_else(|| {
            RealizeServiceError::BadRequest(format!("Unknown directory \"{}\"", arena))
        })
    }

    /// Create an in-process RealizeServiceClient for this server instance.
    pub(crate) fn as_inprocess_client(self) -> InProcessRealizeServiceClient {
        let (client_transport, server_transport) = tarpc::transport::channel::unbounded();
        let server = tarpc::server::BaseChannel::with_defaults(server_transport);
        tokio::spawn(
            server
                .execute(MetricsRealizeServer::new(RealizeServer::serve(self)))
                .for_each(|fut| async move {
                    tokio::spawn(metrics::track_in_flight_request(fut));
                }),
        );
        let client = tarpc::client::new(tarpc::client::Config::default(), client_transport).spawn();
        let stub = InProcessStub {
            inner: MetricsRealizeClient::new(client),
        };

        RealizeServiceClient::from(stub)
    }
}

impl RealizeService for RealizeServer {
    // IMPORTANT: Use async tokio::fs operations or use
    // spawn_blocking, do *not* use blocking std::fs operations
    // outside of spawn_blocking.

    async fn list(
        self,
        _: tarpc::context::Context,
        arena: Arena,
        options: Options,
    ) -> Result<Vec<SyncedFile>> {
        let dir = self.find_directory(&arena)?.clone();

        tokio::task::spawn_blocking(move || {
            let mut files = std::collections::BTreeMap::new();
            for entry in WalkDir::new(dir.path()).into_iter().flatten() {
                if let Some((state, logical)) = LogicalPath::from_actual(&dir, entry.path()) {
                    if options.ignore_partial && state == SyncedFileState::Partial {
                        continue;
                    }
                    let path = logical.relative_path().to_path_buf();
                    if state == SyncedFileState::Final && files.contains_key(&path) {
                        continue;
                    }
                    let size = entry.metadata()?.size();
                    files.insert(path.clone(), SyncedFile { path, size, state });
                }
            }

            Ok::<Vec<SyncedFile>, RealizeServiceError>(files.into_values().collect())
        })
        .await?
    }

    async fn read(
        self,
        _: tarpc::context::Context,
        arena: Arena,
        relative_path: PathBuf,
        range: ByteRange,
        options: Options,
    ) -> Result<Vec<u8>> {
        let dir = self.find_directory(&arena)?;
        let logical = LogicalPath::new(dir, &relative_path)?;
        let (_, actual) = logical.find(&options).await?;
        let mut file = open_for_range_read(&actual).await?;
        let mut buffer = vec![0; range.bytecount() as usize];
        read_padded(&mut file, &range, &mut buffer).await?;

        Ok(buffer)
    }

    async fn send(
        self,
        _: tarpc::context::Context,
        arena: Arena,
        relative_path: PathBuf,
        range: ByteRange,
        data: Vec<u8>,
        options: Options,
    ) -> Result<()> {
        let dir = self.find_directory(&arena)?;
        let logical = LogicalPath::new(dir, &relative_path)?;
        let path = prepare_for_write(&options, logical).await?;
        let mut file = open_for_range_write(&path).await?;
        write_at_offset(&mut file, range.start, &data).await?;

        Ok(())
    }

    async fn finish(
        self,
        _: tarpc::context::Context,
        arena: Arena,
        relative_path: PathBuf,
        options: Options,
    ) -> Result<()> {
        let dir = self.find_directory(&arena)?;
        let logical = LogicalPath::new(dir, &relative_path)?;
        if options.ignore_partial {
            return Err(RealizeServiceError::BadRequest(
                "Invalid option for finish: ignore_partial=true".to_string(),
            ));
        }
        if let (SyncedFileState::Partial, real_path) = logical.find(&options).await? {
            fs::rename(real_path, logical.final_path()).await?;
        }
        Ok(())
    }

    async fn hash(
        self,
        _ctx: tarpc::context::Context,
        arena: Arena,
        relative_path: PathBuf,
        range: ByteRange,
        options: Options,
    ) -> Result<Hash> {
        let dir = self.find_directory(&arena)?;
        let logical = LogicalPath::new(dir, &relative_path)?;
        let (_, path) = logical.find(&options).await?;

        hash_large_range_exact(&path, &range).await
    }

    async fn delete(
        self,
        _ctx: tarpc::context::Context,
        arena: Arena,
        relative_path: PathBuf,
        options: Options,
    ) -> Result<()> {
        let dir = self.find_directory(&arena)?;
        let logical = LogicalPath::new(dir, &relative_path)?;
        let final_path = logical.final_path();
        let partial_path = logical.partial_path();
        if final_path.exists() {
            fs::remove_file(&final_path).await?;
        }
        if partial_path.exists() && !options.ignore_partial {
            fs::remove_file(&partial_path).await?;
        }
        delete_containing_dir(dir.path(), &relative_path).await;
        Ok(())
    }

    async fn calculate_signature(
        self,
        _ctx: tarpc::context::Context,
        arena: Arena,
        relative_path: PathBuf,
        range: ByteRange,
        options: Options,
    ) -> Result<crate::network::rpc::realize::Signature> {
        let dir = self.find_directory(&arena)?;
        let logical = LogicalPath::new(dir, &relative_path)?;
        let (_, actual) = logical.find(&options).await?;
        let mut file = File::open(&actual).await?;
        let mut buffer = vec![0u8; range.bytecount() as usize];
        read_padded(&mut file, &range, &mut buffer).await?;
        let opts = SignatureOptions {
            block_size: RSYNC_BLOCK_SIZE as u32,
            crypto_hash_size: 8,
        };
        let sig = RsyncSignature::calculate(&buffer, opts);
        Ok(crate::network::rpc::realize::Signature(
            sig.into_serialized(),
        ))
    }

    async fn diff(
        self,
        _ctx: tarpc::context::Context,
        arena: Arena,
        relative_path: PathBuf,
        range: ByteRange,
        signature: crate::network::rpc::realize::Signature,
        options: Options,
    ) -> Result<(crate::network::rpc::realize::Delta, Hash)> {
        let dir = self.find_directory(&arena)?;
        let logical = LogicalPath::new(dir, &relative_path)?;
        let (_, actual) = logical.find(&options).await?;
        let mut file = open_for_range_read(&actual).await?;
        let mut buffer = vec![0; range.bytecount() as usize];
        read_padded(&mut file, &range, &mut buffer).await?;

        let hash = hash::digest(&buffer);

        let sig = RsyncSignature::deserialize(signature.0)?;
        let mut delta = Vec::new();
        rsync_diff(&sig.index(), &buffer, &mut delta)?;

        Ok((crate::network::rpc::realize::Delta(delta), hash))
    }

    async fn apply_delta(
        self,
        _ctx: tarpc::context::Context,
        arena: Arena,
        relative_path: PathBuf,
        range: ByteRange,
        delta: crate::network::rpc::realize::Delta,
        hash: Hash,
        options: Options,
    ) -> Result<()> {
        let dir = self.find_directory(&arena)?;
        let logical = LogicalPath::new(dir, &relative_path)?;
        let path = prepare_for_write(&options, logical).await?;

        let mut file = open_for_range_write(&path).await?;
        let mut base = vec![0u8; range.bytecount() as usize];
        read_padded(&mut file, &range, &mut base).await?;
        let mut out = Vec::new();
        rsync_apply_limited(&base, &delta.0, &mut out, range.bytecount() as usize)?;
        if out.len() as u64 != range.bytecount() {
            return Err(RealizeServiceError::BadRequest(
                "Delta output size mismatch".to_string(),
            ));
        }
        if hash::digest(&out) != hash {
            return Err(RealizeServiceError::HashMismatch);
        }
        write_at_offset(&mut file, range.start, &out).await?;

        return Ok(());
    }

    async fn truncate(
        self,
        _ctx: tarpc::context::Context,
        arena: Arena,
        relative_path: PathBuf,
        file_size: u64,
        options: Options,
    ) -> Result<()> {
        let dir = self.find_directory(&arena)?;
        let logical = LogicalPath::new(dir, &relative_path)?;
        let path = prepare_for_write(&options, logical).await?;

        let mut file = open_for_range_write(&path).await?;
        shorten_file(&mut file, file_size).await?;

        return Ok(());
    }

    async fn configure(
        self,
        _ctx: tarpc::context::Context,
        config: crate::network::rpc::realize::Config,
    ) -> Result<crate::network::rpc::realize::Config> {
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

async fn open_for_range_write(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .truncate(false)
        .open(path)
        .await?)
}
async fn open_for_range_read(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .create(false)
        .read(true)
        .write(false)
        .open(path)
        .await?)
}

async fn prepare_for_write(options: &Options, logical: LogicalPath) -> Result<PathBuf> {
    let path = logical.partial_path();
    if let Ok((state, actual)) = logical.find(&options).await {
        if state == SyncedFileState::Final {
            fs::rename(actual, &path).await?;
        }
    } else {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
    }
    Ok(path)
}

/// Hash a range that is too large to be kept in memory.
///
/// Return a zero hash if data is missing.
async fn hash_large_range_exact(path: &Path, range: &ByteRange) -> Result<Hash> {
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

async fn shorten_file(file: &mut File, file_size: u64) -> Result<()> {
    if file.metadata().await?.len() > file_size {
        file.set_len(file_size).await?;
        file.flush().await?;
        file.sync_all().await?;
    }

    Ok(())
}

async fn write_at_offset(file: &mut File, offset: u64, data: &Vec<u8>) -> Result<()> {
    file.seek(SeekFrom::Start(offset)).await?;
    file.write_all(&data).await?;
    file.flush().await?;
    file.sync_all().await?;

    Ok(())
}

/// A logical path is a relative path inside of a [LocalArena].
//
/// The corresponding actual file might be in
/// [SyncedFileState::Partial] or [SyncedFileState::Final] form. Check
/// with [LogicalPath::find].
struct LogicalPath(Arc<LocalArena>, PathBuf);

impl LogicalPath {
    /// Create a new logical path.
    ///
    /// The given path must be relative and cannot reference any
    /// hidden directory or file.
    fn new(dir: &Arc<LocalArena>, path: &Path) -> std::result::Result<Self, RealizeServiceError> {
        if path.as_os_str().is_empty() {
            return Err(RealizeServiceError::BadRequest(
                "Invalid Relative path; empty".to_string(),
            ));
        }
        for component in path.components() {
            match component {
                std::path::Component::Normal(_) => {}
                _ => {
                    return Err(RealizeServiceError::BadRequest(
                        "Invalid relative path".to_string(),
                    ));
                }
            }
        }
        Ok(Self(Arc::clone(dir), path.to_path_buf()))
    }

    /// Create a logical path from an actual partial or final path.
    fn from_actual(dir: &Arc<LocalArena>, actual: &Path) -> Option<(SyncedFileState, Self)> {
        if !actual.is_file() {
            return None;
        }

        let relative = pathdiff::diff_paths(actual, dir.path());
        if let Some(mut relative) = relative {
            if let Some(filename) = actual.file_name() {
                let name_bytes = filename.to_string_lossy();
                if name_bytes.starts_with(".") && name_bytes.ends_with(".part") {
                    let stripped_name =
                        OsString::from(name_bytes[1..name_bytes.len() - 5].to_string());
                    relative.set_file_name(&stripped_name);
                    return Some((SyncedFileState::Partial, Self(Arc::clone(dir), relative)));
                }
                return Some((SyncedFileState::Final, Self(Arc::clone(dir), relative)));
            }
        }

        None
    }

    // Look for a file for the logical path.
    //
    // The path can be found in final or partial found. Return both
    // the [SyncedFileState] and the actual path of the file that was
    // found.
    //
    // File with an I/O error of kind [std::io::ErrorKind::NotFound]
    // if no file exists for the logical path.
    async fn find(&self, options: &Options) -> Result<(SyncedFileState, PathBuf)> {
        if !options.ignore_partial {
            let partial = self.partial_path();
            if fs::metadata(&partial).await.is_ok() {
                return Ok((SyncedFileState::Partial, partial));
            }
        }
        let fpath = self.final_path();
        if fs::metadata(&fpath).await.is_ok() {
            return Ok((SyncedFileState::Final, fpath));
        }
        Err(std::io::Error::from(std::io::ErrorKind::NotFound).into())
    }

    /// Return the final form of the logical path.
    fn final_path(&self) -> PathBuf {
        self.0.path().join(&self.1)
    }

    /// Return the partial form of the logical path.
    fn partial_path(&self) -> PathBuf {
        let mut partial = self.0.path().to_path_buf();
        partial.push(&self.1);

        let mut part_filename = OsString::from(".");
        if let Some(fname) = self.1.file_name() {
            part_filename.push(OsString::from(fname.to_string_lossy().to_string()));
        }
        part_filename.push(OsString::from(".part"));
        partial.set_file_name(part_filename);

        partial
    }

    /// Return the relative path of the logical path.
    fn relative_path(&self) -> &Path {
        &self.1
    }
}

impl From<walkdir::Error> for RealizeServiceError {
    fn from(err: walkdir::Error) -> Self {
        if err.io_error().is_some() {
            // We know err contains an io error; unwrap will succeed.
            err.into_io_error().unwrap().into()
        } else {
            anyhow::Error::new(err).into()
        }
    }
}

impl From<JoinError> for RealizeServiceError {
    fn from(err: JoinError) -> Self {
        anyhow::Error::new(err).into()
    }
}

// Add error conversions for fast_rsync errors
impl From<fast_rsync::DiffError> for RealizeServiceError {
    fn from(e: fast_rsync::DiffError) -> Self {
        RealizeServiceError::Rsync(RsyncOperation::Diff, e.to_string())
    }
}
impl From<fast_rsync::ApplyError> for RealizeServiceError {
    fn from(e: fast_rsync::ApplyError) -> Self {
        RealizeServiceError::Rsync(RsyncOperation::Apply, e.to_string())
    }
}

impl From<fast_rsync::SignatureParseError> for RealizeServiceError {
    fn from(e: fast_rsync::SignatureParseError) -> Self {
        RealizeServiceError::Rsync(RsyncOperation::Sign, e.to_string())
    }
}

/// Remove empty parent directories of [relative_path].
///
/// Errors are ignored. Deleting just stops.
async fn delete_containing_dir(root: &Path, relative_path: &Path) {
    let mut current = relative_path;
    while let Some(parent) = current.parent() {
        if parent.as_os_str().is_empty() {
            return;
        }
        let full_path = root.join(parent);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::rpc::realize::Hash;
    use assert_fs::prelude::*;
    use assert_fs::TempDir;
    use assert_unordered::assert_eq_unordered;
    use std::fs;
    use std::io::Read as _;

    fn setup_server_with_dir() -> anyhow::Result<(RealizeServer, TempDir, Arc<LocalArena>)> {
        let temp = TempDir::new()?;
        let arena = Arena::from("testdir");
        let dir = Arc::new(LocalArena::new(&arena, temp.path()));

        Ok((
            RealizeServer::new(LocalArenas::single(&arena, dir.path())),
            temp,
            dir,
        ))
    }

    #[tokio::test]
    async fn test_final_and_partial_paths() -> anyhow::Result<()> {
        let dir = Arc::new(LocalArena::new(
            &Arena::from("testdir"),
            &PathBuf::from("/doesnotexist/testdir"),
        ));

        let file1 = LogicalPath::new(&dir, &PathBuf::from("file1.txt"))?;
        assert_eq!(
            PathBuf::from("/doesnotexist/testdir/file1.txt"),
            file1.final_path()
        );
        assert_eq!(
            PathBuf::from("/doesnotexist/testdir/.file1.txt.part"),
            file1.partial_path()
        );

        let file2 = LogicalPath::new(&dir, &PathBuf::from("subdir/file2.txt"))?;
        assert_eq!(
            PathBuf::from("/doesnotexist/testdir/subdir/file2.txt"),
            file2.final_path()
        );
        assert_eq!(
            PathBuf::from("/doesnotexist/testdir/subdir/.file2.txt.part"),
            file2.partial_path()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_find_logical_path() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let dir = Arc::new(LocalArena::new(&Arena::from("testdir"), temp.path()));
        let opts = Options::default();

        temp.child("foo.txt").write_str("test")?;
        assert_eq!(
            (SyncedFileState::Final, temp.child("foo.txt").to_path_buf()),
            LogicalPath::new(&dir, &PathBuf::from("foo.txt"))?
                .find(&opts)
                .await?
        );

        temp.child("subdir/foo2.txt").write_str("test")?;
        assert_eq!(
            (
                SyncedFileState::Final,
                temp.child("subdir/foo2.txt").to_path_buf()
            ),
            LogicalPath::new(&dir, &PathBuf::from("subdir/foo2.txt"))?
                .find(&opts)
                .await?
        );

        temp.child(".bar.txt.part").write_str("test")?;
        assert_eq!(
            (
                SyncedFileState::Partial,
                temp.child(".bar.txt.part").to_path_buf()
            ),
            LogicalPath::new(&dir, &PathBuf::from("bar.txt"))?
                .find(&opts)
                .await?
        );

        temp.child("subdir/.bar2.txt.part").write_str("test")?;
        assert_eq!(
            (
                SyncedFileState::Partial,
                temp.child("subdir/.bar2.txt.part").to_path_buf()
            ),
            LogicalPath::new(&dir, &PathBuf::from("subdir/bar2.txt"))?
                .find(&opts)
                .await?
        );

        assert!(matches!(
            LogicalPath::new(&dir, &PathBuf::from("notfound.txt"))?
                .find(&opts)
                .await,
            Err(RealizeServiceError::Io(_))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_find_logical_path_partial_and_final() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let dir = Arc::new(LocalArena::new(&Arena::from("testdir"), temp.path()));
        let opts = Options::default();
        let nopartial = Options {
            ignore_partial: true,
        };

        temp.child(".foo.txt.part").write_str("test")?;
        temp.child("foo.txt").write_str("test")?;
        assert_eq!(
            (
                SyncedFileState::Partial,
                temp.child(".foo.txt.part").to_path_buf()
            ),
            LogicalPath::new(&dir, &PathBuf::from("foo.txt"))?
                .find(&opts)
                .await?
        );
        assert_eq!(
            (SyncedFileState::Final, temp.child("foo.txt").to_path_buf()),
            LogicalPath::new(&dir, &PathBuf::from("foo.txt"))?
                .find(&nopartial)
                .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_find_logical_path_ignore_partial() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let dir = Arc::new(LocalArena::new(&Arena::from("testdir"), temp.path()));
        let nopartial = Options {
            ignore_partial: true,
        };

        temp.child(".foo.txt.part").write_str("test")?;
        assert!(LogicalPath::new(&dir, &PathBuf::from("foo.txt"))?
            .find(&nopartial)
            .await
            .is_err());

        temp.child("bar.txt").write_str("test")?;
        assert_eq!(
            (SyncedFileState::Final, temp.child("bar.txt").to_path_buf()),
            LogicalPath::new(&dir, &PathBuf::from("bar.txt"))?
                .find(&nopartial)
                .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_logical_path_validation_relative() {
        let dir = Arc::new(LocalArena::new(
            &Arena::from("testdir"),
            &PathBuf::from("/tmp/testdir"),
        ));

        // Empty path
        let empty = PathBuf::from("");
        assert!(matches!(
            LogicalPath::new(&dir, &empty),
            Err(RealizeServiceError::BadRequest(_))
        ));

        // Absolute path
        let abs = PathBuf::from("/foo/bar.txt");
        assert!(matches!(
            LogicalPath::new(&dir, &abs),
            Err(RealizeServiceError::BadRequest(_))
        ));

        // Path with '..'
        let dotdot = PathBuf::from("foo/../bar.txt");
        assert!(matches!(
            LogicalPath::new(&dir, &dotdot),
            Err(RealizeServiceError::BadRequest(_))
        ));

        // Valid path
        let valid = PathBuf::from("foo/bar.txt");
        assert!(LogicalPath::new(&dir, &valid).is_ok());
        let valid2 = PathBuf::from("subdir/file.txt");
        assert!(LogicalPath::new(&dir, &valid2).is_ok());
        let hidden_file = PathBuf::from("foo/.bar.txt");
        assert!(LogicalPath::new(&dir, &hidden_file).is_ok());
        let hidden_dir = PathBuf::from(".subdir/file.txt");
        assert!(LogicalPath::new(&dir, &hidden_dir).is_ok());
    }

    #[tokio::test]
    async fn list_empty() -> anyhow::Result<()> {
        let (server, _temp, dir) = setup_server_with_dir()?;
        let files = server
            .clone()
            .list(
                tarpc::context::current(),
                dir.arena().clone(),
                Options::default(),
            )
            .await?;
        assert!(files.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn list_files_and_partial() -> anyhow::Result<()> {
        let (server, temp, dir) = setup_server_with_dir()?;

        fs::create_dir_all(temp.child("subdir"))?;

        temp.child("foo.txt").write_str("hello")?;
        temp.child("subdir/foo2.txt").write_str("hello")?;
        temp.child(".bar.txt.part").write_str("partial")?;
        temp.child("subdir/.bar2.txt.part").write_str("partial")?;

        let files = server
            .list(
                tarpc::context::current(),
                dir.arena().clone(),
                Options::default(),
            )
            .await?;
        assert_eq_unordered!(
            files,
            vec![
                SyncedFile {
                    path: PathBuf::from("foo.txt"),
                    size: 5,
                    state: SyncedFileState::Final
                },
                SyncedFile {
                    path: PathBuf::from("subdir/foo2.txt"),
                    size: 5,
                    state: SyncedFileState::Final
                },
                SyncedFile {
                    path: PathBuf::from("bar.txt"),
                    size: 7,
                    state: SyncedFileState::Partial
                },
                SyncedFile {
                    path: PathBuf::from("subdir/bar2.txt"),
                    size: 7,
                    state: SyncedFileState::Partial
                },
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn send_wrong_order() -> anyhow::Result<()> {
        let (server, _temp, dir) = setup_server_with_dir()?;
        let fpath = LogicalPath::new(&dir, &PathBuf::from("wrong_order.txt"))?;
        let data1 = b"fghij".to_vec();
        server
            .clone()
            .send(
                tarpc::context::current(),
                dir.arena().clone(),
                fpath.relative_path().to_path_buf(),
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
                dir.arena().clone(),
                fpath.relative_path().to_path_buf(),
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
        let (server, _temp, dir) = setup_server_with_dir()?;

        let fpath = LogicalPath::new(&dir, &PathBuf::from("wrong_order.txt"))?;
        fs::write(fpath.final_path(), "abcdefghij")?;

        let data = server
            .clone()
            .read(
                tarpc::context::current(),
                dir.arena().clone(),
                fpath.relative_path().to_path_buf(),
                ByteRange { start: 5, end: 10 },
                Options::default(),
            )
            .await?;
        assert_eq!(String::from_utf8(data)?, "fghij");

        let data = server
            .clone()
            .read(
                tarpc::context::current(),
                dir.arena().clone(),
                fpath.relative_path().to_path_buf(),
                ByteRange { start: 0, end: 5 },
                Options::default(),
            )
            .await?;
        assert_eq!(String::from_utf8(data)?, "abcde");

        let result = server
            .clone()
            .read(
                tarpc::context::current(),
                dir.arena().clone(),
                fpath.relative_path().to_path_buf(),
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
        let (server, _temp, dir) = setup_server_with_dir()?;

        let fpath = LogicalPath::new(&dir, &PathBuf::from("finish_partial.txt"))?;
        fs::write(fpath.partial_path(), "abcde")?;

        server
            .finish(
                tarpc::context::current(),
                dir.arena().clone(),
                fpath.relative_path().to_path_buf(),
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
        let (server, _temp, dir) = setup_server_with_dir()?;

        let fpath = LogicalPath::new(&dir, &PathBuf::from("finish_partial.txt"))?;
        fs::write(fpath.final_path(), "abcde")?;

        server
            .finish(
                tarpc::context::current(),
                dir.arena().clone(),
                fpath.relative_path().to_path_buf(),
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
        let (server, _temp, dir) = setup_server_with_dir()?;
        let fpath = LogicalPath::new(&dir, &PathBuf::from("truncate.txt"))?;
        std::fs::write(fpath.partial_path(), b"abcdefghij")?;
        server
            .clone()
            .truncate(
                tarpc::context::current(),
                dir.arena().clone(),
                fpath.relative_path().to_path_buf(),
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
        let (server, _temp, dir) = setup_server_with_dir()?;
        let content = b"hello world";
        let fpath = LogicalPath::new(&dir, &PathBuf::from("foo.txt"))?;
        std::fs::write(fpath.final_path(), content)?;
        let hash = server
            .clone()
            .hash(
                tarpc::context::current(),
                dir.arena().clone(),
                PathBuf::from("foo.txt"),
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
        let fpath2 = LogicalPath::new(&dir, &PathBuf::from("bar.txt"))?;
        std::fs::write(fpath2.partial_path(), content2)?;
        let hash2 = server
            .clone()
            .hash(
                tarpc::context::current(),
                dir.arena().clone(),
                PathBuf::from("bar.txt"),
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
        let (server, _temp, dir) = setup_server_with_dir()?;
        let content = b"hello world";
        let fpath = LogicalPath::new(&dir, &PathBuf::from("foo.txt"))?;
        std::fs::write(fpath.final_path(), content)?;
        let hash = server
            .clone()
            .hash(
                tarpc::context::current(),
                dir.arena().clone(),
                PathBuf::from("foo.txt"),
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
        let (server, _temp, dir) = setup_server_with_dir()?;
        let content = b"hello world";
        let fpath = LogicalPath::new(&dir, &PathBuf::from("foo.txt"))?;
        std::fs::write(fpath.final_path(), content)?;
        let hash = server
            .clone()
            .hash(
                tarpc::context::current(),
                dir.arena().clone(),
                PathBuf::from("foo.txt"),
                ByteRange { start: 0, end: 200 },
                Options::default(),
            )
            .await?;
        assert_eq!(hash, Hash::zero());

        Ok(())
    }

    #[tokio::test]
    async fn delete_final_and_partial() -> anyhow::Result<()> {
        let (server, _temp, dir) = setup_server_with_dir()?;
        let fpath = LogicalPath::new(&dir, &PathBuf::from("foo.txt"))?;
        std::fs::write(fpath.final_path(), b"data")?;
        std::fs::write(fpath.partial_path(), b"data2")?;
        assert!(fpath.final_path().exists());
        assert!(fpath.partial_path().exists());
        server
            .clone()
            .delete(
                tarpc::context::current(),
                dir.arena().clone(),
                PathBuf::from("foo.txt"),
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
                dir.arena().clone(),
                PathBuf::from("foo.txt"),
                Options::default(),
            )
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn tarpc_rpc_inprocess() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let client =
            create_inprocess_client(LocalArenas::single(&Arena::from("testdir"), temp.path()));
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
        let (server, temp, dir) = setup_server_with_dir()?;
        let file_path = PathBuf::from("foo.txt");
        let file_content = b"hello world, this is a test of rsync signature!";
        temp.child("foo.txt").write_binary(file_content)?;

        // Calculate signature for the whole file
        let sig = server
            .clone()
            .calculate_signature(
                tarpc::context::current(),
                dir.arena().clone(),
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
                dir.arena().clone(),
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
        temp.child("foo.txt").write_binary(file_content)?;
        let res = server
            .clone()
            .apply_delta(
                tarpc::context::current(),
                dir.arena().clone(),
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
        // File should now match new_content
        let logical = LogicalPath::new(&dir, &file_path)?;
        let (_state, actual) = logical.find(&Options::default()).await?;
        let mut buf = Vec::new();
        std::fs::File::open(&actual)?.read_to_end(&mut buf)?;
        assert_eq!(&buf[..new_content.len()], new_content);

        Ok(())
    }

    #[tokio::test]
    async fn signature_on_nonexisting_file() -> anyhow::Result<()> {
        let (server, _temp, dir) = setup_server_with_dir()?;
        // Nonexistent file
        let result = server
            .clone()
            .calculate_signature(
                tarpc::context::current(),
                dir.arena().clone(),
                PathBuf::from("notfound.txt"),
                ByteRange { start: 0, end: 10 },
                Options::default(),
            )
            .await;
        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn apply_delta_error_case() -> anyhow::Result<()> {
        let (server, temp, dir) = setup_server_with_dir()?;
        let file_path = PathBuf::from("foo.txt");
        temp.child("foo.txt").write_str("abc")?;
        // Try to apply a bogus delta
        let result = server
            .clone()
            .apply_delta(
                tarpc::context::current(),
                dir.arena().clone(),
                file_path,
                ByteRange { start: 0, end: 3 },
                crate::network::rpc::realize::Delta(vec![1, 2, 3]),
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
        let (server, temp, dir) = setup_server_with_dir()?;
        temp.child("foo.txt").write_str("final")?;
        temp.child(".foo.txt.part").write_str("partial")?;
        // Default: partial preferred
        let files = server
            .clone()
            .list(
                tarpc::context::current(),
                dir.arena().clone(),
                Options {
                    ignore_partial: false,
                },
            )
            .await?;
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].state, SyncedFileState::Partial);
        // With ignore_partial: final preferred
        let files = server
            .clone()
            .list(
                tarpc::context::current(),
                dir.arena().clone(),
                Options {
                    ignore_partial: true,
                },
            )
            .await?;
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].state, SyncedFileState::Final);
        Ok(())
    }

    #[tokio::test]
    async fn list_ignore_partial() -> anyhow::Result<()> {
        let (server, temp, dir) = setup_server_with_dir()?;
        temp.child(".foo.txt.part").write_str("partial")?;
        // Default: return partial
        let files = server
            .clone()
            .list(
                tarpc::context::current(),
                dir.arena().clone(),
                Options {
                    ignore_partial: false,
                },
            )
            .await?;
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].state, SyncedFileState::Partial);
        // With ignore_partial: don't return partial
        let files = server
            .clone()
            .list(
                tarpc::context::current(),
                dir.arena().clone(),
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
        let (server, temp, dir) = setup_server_with_dir()?;
        temp.child("foo.txt").write_str("final")?;
        temp.child(".foo.txt.part").write_str("partial")?;
        // Default: partial preferred
        let data = server
            .clone()
            .read(
                tarpc::context::current(),
                dir.arena().clone(),
                PathBuf::from("foo.txt"),
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
                dir.arena().clone(),
                PathBuf::from("foo.txt"),
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
        let (server, temp, dir) = setup_server_with_dir()?;
        // Create nested directories: a/b/c/file.txt
        let nested_dir = temp.child("a/b/c");
        std::fs::create_dir_all(nested_dir.path())?;
        let file_path = PathBuf::from("a/b/c/file.txt");
        let logical = LogicalPath::new(&dir, &file_path)?;
        std::fs::write(logical.final_path(), b"data")?;
        // Delete the file
        server
            .clone()
            .delete(
                tarpc::context::current(),
                dir.arena().clone(),
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
        let (server, temp, dir) = setup_server_with_dir()?;
        // Create nested directories: a/b/c/file.txt and a/b/c/keep.txt
        let nested_dir = temp.child("a/b/c");
        std::fs::create_dir_all(nested_dir.path())?;
        let file_path = PathBuf::from("a/b/c/file.txt");
        let keep_path = temp.child("a/b/c/keep.txt");
        let logical = LogicalPath::new(&dir, &file_path)?;
        std::fs::write(logical.final_path(), b"data")?;
        std::fs::write(keep_path.path(), b"keep")?;
        // Delete the file
        server
            .clone()
            .delete(
                tarpc::context::current(),
                dir.arena().clone(),
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
        let server = RealizeServer::new(LocalArenas::single(
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
        let dirs = LocalArenas::single(&Arena::from("testdir"), &PathBuf::from("/tmp/testdir"));
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
