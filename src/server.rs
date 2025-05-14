use crate::client::DeadlineSetter;
use crate::model::service::Options;
use crate::model::service::{
    ByteRange, DirectoryId, RealizeError, RealizeService, Result, RsyncOperation, SyncedFile,
    SyncedFileState,
};
use crate::model::service::{RealizeServiceClient, RealizeServiceRequest, RealizeServiceResponse};
use fast_rsync::{
    Signature as RsyncSignature, SignatureOptions, apply_limited as rsync_apply_limited,
    diff as rsync_diff,
};
use futures::StreamExt;
use sha2::{Digest, Sha256};
use std::cmp::min;
use std::collections::HashMap;
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tarpc::server::Channel;
use walkdir::WalkDir;

// Move this to the top-level, outside any impl
const RSYNC_BLOCK_SIZE: usize = 4096;

#[derive(Clone)]
pub struct RealizeServer {
    /// Maps directory IDs to local paths
    dirs: HashMap<DirectoryId, Arc<Directory>>,
}

impl RealizeServer {
    pub fn new<T>(dirs: T) -> Self
    where
        T: IntoIterator<Item = Directory>,
    {
        Self {
            dirs: dirs
                .into_iter()
                .map(|dir| (dir.id.clone(), Arc::new(dir)))
                .collect(),
        }
    }

    pub fn for_dir(id: &DirectoryId, path: &Path) -> Self {
        RealizeServer::new(vec![Directory::new(id, path)])
    }

    fn find_directory(&self, dir_id: &DirectoryId) -> Result<&Arc<Directory>> {
        self.dirs.get(dir_id).ok_or_else(|| {
            RealizeError::BadRequest(format!("Unknown directory ID: '{:?}'", dir_id))
        })
    }

    /// Create an in-process RealizeServiceClient for this server instance.
    pub fn as_inprocess_client(
        self,
    ) -> RealizeServiceClient<
        DeadlineSetter<tarpc::client::Channel<RealizeServiceRequest, RealizeServiceResponse>>,
    > {
        let (client_transport, server_transport) = tarpc::transport::channel::unbounded();
        let server = tarpc::server::BaseChannel::with_defaults(server_transport);
        tokio::spawn(
            server
                .execute(RealizeServer::serve(self))
                .for_each(|response| async move {
                    tokio::spawn(response);
                }),
        );
        let client = tarpc::client::new(tarpc::client::Config::default(), client_transport).spawn();

        RealizeServiceClient::from(DeadlineSetter::new(client))
    }
}

impl RealizeService for RealizeServer {
    async fn list(
        self,
        _: tarpc::context::Context,
        dir_id: DirectoryId,
        options: Options,
    ) -> Result<Vec<SyncedFile>> {
        let dir = self.find_directory(&dir_id)?;
        let mut files = std::collections::BTreeMap::new();
        for entry in WalkDir::new(dir.path()).into_iter().flatten() {
            if let Some((state, logical)) = LogicalPath::from_actual(dir, entry.path()) {
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

        Ok(files.into_values().collect())
    }

    async fn read(
        self,
        _: tarpc::context::Context,
        dir_id: DirectoryId,
        relative_path: PathBuf,
        range: ByteRange,
        options: Options,
    ) -> Result<Vec<u8>> {
        let dir = self.find_directory(&dir_id)?;
        let logical = LogicalPath::new(dir, &relative_path);
        let (_, actual) = logical.find(&options)?;
        let mut file = OpenOptions::new()
            .create(false)
            .read(true)
            .write(false)
            .open(&actual)?;
        let mut buffer = vec![0; (range.1 - range.0) as usize];
        partial_read(&mut file, &range, &mut buffer)?;
        Ok(buffer)
    }

    async fn send(
        self,
        _: tarpc::context::Context,
        dir_id: DirectoryId,
        relative_path: PathBuf,
        range: ByteRange,
        file_size: u64,
        data: Vec<u8>,
        options: Options,
    ) -> Result<()> {
        let dir = self.find_directory(&dir_id)?;
        let logical = LogicalPath::new(dir, &relative_path);
        let path = logical.partial_path();
        if let Ok((state, actual)) = logical.find(&options) {
            // File already exists
            if state == SyncedFileState::Final {
                fs::rename(actual, &path)?;
            }
        } else {
            // Open will need to create the file. Ensure parent
            // directory exists beforehand.
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
        }
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&path)?;
        file.seek(SeekFrom::Start(range.0))?;
        file.write_all(&data)?;

        let file_len = file.metadata()?.len();
        if file_len > file_size {
            file.set_len(file_size)?;
        }

        Ok(())
    }

    async fn finish(
        self,
        _: tarpc::context::Context,
        dir_id: DirectoryId,
        relative_path: PathBuf,
        options: Options,
    ) -> Result<()> {
        let dir = self.find_directory(&dir_id)?;
        let logical = LogicalPath::new(dir, &relative_path);
        if options.ignore_partial {
            return Err(RealizeError::BadRequest(
                "Invalid option for finish: ignore_partial=true".to_string(),
            ));
        }
        if let (SyncedFileState::Partial, real_path) = logical.find(&options)? {
            fs::rename(real_path, logical.final_path())?;
        }
        Ok(())
    }

    async fn hash(
        self,
        _ctx: tarpc::context::Context,
        dir_id: DirectoryId,
        relative_path: PathBuf,
        options: Options,
    ) -> Result<crate::model::service::Hash> {
        let dir = self.find_directory(&dir_id)?;
        let logical = LogicalPath::new(dir, &relative_path);
        let (_state, actual) = logical.find(&options)?;
        let mut file = fs::File::open(&actual)?;
        let mut hasher = Sha256::new();
        let mut buffer = [0u8; 8192];
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hasher.update(&buffer[..n]);
        }
        let hash = hasher.finalize();
        Ok(crate::model::service::Hash(hash.into()))
    }

    async fn delete(
        self,
        _ctx: tarpc::context::Context,
        dir_id: DirectoryId,
        relative_path: PathBuf,
        options: Options,
    ) -> Result<()> {
        let dir = self.find_directory(&dir_id)?;
        let logical = LogicalPath::new(dir, &relative_path);
        let mut _any_deleted = false;
        let final_path = logical.final_path();
        let partial_path = logical.partial_path();
        if final_path.exists() {
            fs::remove_file(&final_path)?;
            _any_deleted = true;
        }
        if partial_path.exists() && !options.ignore_partial {
            fs::remove_file(&partial_path)?;
            _any_deleted = true;
        }
        // Succeed even if neither existed
        Ok(())
    }

    async fn calculate_signature(
        self,
        _ctx: tarpc::context::Context,
        dir_id: DirectoryId,
        relative_path: PathBuf,
        range: ByteRange,
        options: Options,
    ) -> Result<crate::model::service::Signature> {
        let dir = self.find_directory(&dir_id)?;
        let logical = LogicalPath::new(dir, &relative_path);
        let (_, actual) = logical.find(&options)?;
        let mut file = std::fs::File::open(&actual)?;
        let mut buffer = vec![0u8; (range.1 - range.0) as usize];
        partial_read(&mut file, &range, &mut buffer)?;
        let opts = SignatureOptions {
            block_size: RSYNC_BLOCK_SIZE as u32,
            crypto_hash_size: 8,
        };
        let sig = RsyncSignature::calculate(&buffer, opts);
        Ok(crate::model::service::Signature(sig.into_serialized()))
    }

    async fn diff(
        self,
        _ctx: tarpc::context::Context,
        dir_id: DirectoryId,
        relative_path: PathBuf,
        range: ByteRange,
        signature: crate::model::service::Signature,
        options: Options,
    ) -> Result<crate::model::service::Delta> {
        let dir = self.find_directory(&dir_id)?;
        let logical = LogicalPath::new(dir, &relative_path);
        let mut buffer = vec![0u8; (range.1 - range.0) as usize];
        if let Ok((_, actual)) = logical.find(&options) {
            let mut file = std::fs::File::open(&actual)?;
            partial_read(&mut file, &range, &mut buffer)?;
        }
        let sig = RsyncSignature::deserialize(signature.0)?;
        let mut delta = Vec::new();
        rsync_diff(&sig.index(), &buffer, &mut delta)?;
        Ok(crate::model::service::Delta(delta))
    }

    async fn apply_delta(
        self,
        _ctx: tarpc::context::Context,
        dir_id: DirectoryId,
        relative_path: PathBuf,
        range: ByteRange,
        file_size: u64,
        delta: crate::model::service::Delta,
        options: Options,
    ) -> Result<()> {
        let dir = self.find_directory(&dir_id)?;
        let logical = LogicalPath::new(dir, &relative_path);
        let (state, actual) = logical.find(&options)?;
        // Always apply to partial file
        let partial_path = logical.partial_path();
        if state == SyncedFileState::Final {
            std::fs::rename(&actual, &partial_path)?;
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&partial_path)?;
        let mut base = vec![0u8; (range.1 - range.0) as usize];
        partial_read(&mut file, &range, &mut base)?;
        let mut out = Vec::new();
        rsync_apply_limited(&base, &delta.0, &mut out, (range.1 - range.0) as usize)?;
        if out.len() as u64 != range.1 - range.0 {
            return Err(RealizeError::BadRequest(
                "Delta output size mismatch".to_string(),
            ));
        }
        file.seek(std::io::SeekFrom::Start(range.0))?;
        file.write_all(&out)?;
        let file_len = file.metadata()?.len();
        if file_len > file_size {
            file.set_len(file_size)?;
        }
        Ok(())
    }
}

fn partial_read(
    file: &mut File,
    range: &ByteRange,
    buf: &mut [u8],
) -> std::result::Result<(), std::io::Error> {
    let initial_size = file.metadata()?.size();
    if initial_size > range.0 {
        file.seek(std::io::SeekFrom::Start(range.0))?;
        let read_end = (min(range.1, initial_size) - range.0) as usize;
        file.read_exact(&mut buf[0..read_end])?;
        buf[read_end..].fill(0);
    } else {
        buf.fill(0);
    }
    Ok(())
}

// A directory, stored in RealizeServer and in LogicalPath.
pub struct Directory {
    id: DirectoryId,
    path: PathBuf,
}

impl Directory {
    /// Create a directory with the given id and path.
    pub fn new(id: &DirectoryId, path: &Path) -> Self {
        Self {
            id: id.clone(),
            path: path.to_path_buf(),
        }
    }

    /// Directory ID, as found in Service calls.
    pub fn id(&self) -> &DirectoryId {
        &self.id
    }

    /// Local directory path that correspond to the ID.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// A logical path is a relative path inside of a [Directory].
//
/// The corresponding actual file might be in
/// [SyncedFileState::Partial] or [SyncedFileState::Final] form. Check
/// with [LogicalPath::find].
struct LogicalPath(Arc<Directory>, PathBuf);

impl LogicalPath {
    /// Create a new logical path, without checking it.
    fn new(dir: &Arc<Directory>, path: &Path) -> Self {
        // TODO: check that path is relative and doesn't use any ..
        Self(Arc::clone(dir), path.to_path_buf())
    }

    /// Create a logical path from an actual partial or final path.
    fn from_actual(dir: &Arc<Directory>, actual: &Path) -> Option<(SyncedFileState, Self)> {
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
                    return Some((SyncedFileState::Partial, LogicalPath::new(dir, &relative)));
                }
                return Some((SyncedFileState::Final, LogicalPath::new(dir, &relative)));
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
    fn find(&self, options: &Options) -> Result<(SyncedFileState, PathBuf)> {
        if !options.ignore_partial {
            let partial = self.partial_path();
            if partial.exists() {
                return Ok((SyncedFileState::Partial, partial));
            }
        }
        let fpath = self.final_path();
        if fpath.exists() {
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

impl From<walkdir::Error> for RealizeError {
    fn from(err: walkdir::Error) -> Self {
        if err.io_error().is_some() {
            // We know err contains an io error; unwrap will succeed.
            err.into_io_error().unwrap().into()
        } else {
            anyhow::Error::new(err).into()
        }
    }
}

// Add error conversions for fast_rsync errors
impl From<fast_rsync::DiffError> for RealizeError {
    fn from(e: fast_rsync::DiffError) -> Self {
        RealizeError::Rsync(RsyncOperation::Diff, e.to_string())
    }
}
impl From<fast_rsync::ApplyError> for RealizeError {
    fn from(e: fast_rsync::ApplyError) -> Self {
        RealizeError::Rsync(RsyncOperation::Apply, e.to_string())
    }
}

impl From<fast_rsync::SignatureParseError> for RealizeError {
    fn from(e: fast_rsync::SignatureParseError) -> Self {
        RealizeError::Rsync(RsyncOperation::Sign, e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::service::Hash as FileHash;
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use assert_unordered::assert_eq_unordered;
    use std::fs;

    fn setup_server_with_dir() -> anyhow::Result<(RealizeServer, TempDir, Arc<Directory>)> {
        let temp = TempDir::new()?;
        let dir = Arc::new(Directory::new(&DirectoryId::from("testdir"), temp.path()));

        Ok((RealizeServer::for_dir(dir.id(), dir.path()), temp, dir))
    }

    #[test]
    fn test_final_and_partial_paths() -> anyhow::Result<()> {
        let dir = Arc::new(Directory::new(
            &DirectoryId::from("testdir"),
            &PathBuf::from("/doesnotexist/testdir"),
        ));

        let file1 = LogicalPath::new(&dir, &PathBuf::from("file1.txt"));
        assert_eq!(
            PathBuf::from("/doesnotexist/testdir/file1.txt"),
            file1.final_path()
        );
        assert_eq!(
            PathBuf::from("/doesnotexist/testdir/.file1.txt.part"),
            file1.partial_path()
        );

        let file2 = LogicalPath::new(&dir, &PathBuf::from("subdir/file2.txt"));
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

    #[test]
    fn test_find_logical_path() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let dir = Arc::new(Directory::new(&DirectoryId::from("testdir"), temp.path()));
        let opts = Options::default();

        temp.child("foo.txt").write_str("test")?;
        assert_eq!(
            (SyncedFileState::Final, temp.child("foo.txt").to_path_buf()),
            LogicalPath::new(&dir, &PathBuf::from("foo.txt")).find(&opts)?
        );

        temp.child("subdir/foo2.txt").write_str("test")?;
        assert_eq!(
            (
                SyncedFileState::Final,
                temp.child("subdir/foo2.txt").to_path_buf()
            ),
            LogicalPath::new(&dir, &PathBuf::from("subdir/foo2.txt")).find(&opts)?
        );

        temp.child(".bar.txt.part").write_str("test")?;
        assert_eq!(
            (
                SyncedFileState::Partial,
                temp.child(".bar.txt.part").to_path_buf()
            ),
            LogicalPath::new(&dir, &PathBuf::from("bar.txt")).find(&opts)?
        );

        temp.child("subdir/.bar2.txt.part").write_str("test")?;
        assert_eq!(
            (
                SyncedFileState::Partial,
                temp.child("subdir/.bar2.txt.part").to_path_buf()
            ),
            LogicalPath::new(&dir, &PathBuf::from("subdir/bar2.txt")).find(&opts)?
        );

        assert!(matches!(
            LogicalPath::new(&dir, &PathBuf::from("notfound.txt")).find(&opts),
            Err(RealizeError::Io(_))
        ));

        Ok(())
    }

    #[test]
    fn test_find_logical_path_partial_and_final() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let dir = Arc::new(Directory::new(&DirectoryId::from("testdir"), temp.path()));
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
            LogicalPath::new(&dir, &PathBuf::from("foo.txt")).find(&opts)?
        );
        assert_eq!(
            (SyncedFileState::Final, temp.child("foo.txt").to_path_buf()),
            LogicalPath::new(&dir, &PathBuf::from("foo.txt")).find(&nopartial)?
        );

        Ok(())
    }

    #[test]
    fn test_find_logical_path_ignore_partial() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let dir = Arc::new(Directory::new(&DirectoryId::from("testdir"), temp.path()));
        let nopartial = Options {
            ignore_partial: true,
        };

        temp.child(".foo.txt.part").write_str("test")?;
        assert!(
            LogicalPath::new(&dir, &PathBuf::from("foo.txt"))
                .find(&nopartial)
                .is_err()
        );

        temp.child("bar.txt").write_str("test")?;
        assert_eq!(
            (SyncedFileState::Final, temp.child("bar.txt").to_path_buf()),
            LogicalPath::new(&dir, &PathBuf::from("bar.txt")).find(&nopartial)?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_list_empty() -> anyhow::Result<()> {
        let (server, _temp, dir) = setup_server_with_dir()?;
        let files = server
            .clone()
            .list(
                tarpc::context::current(),
                dir.id().clone(),
                Options::default(),
            )
            .await?;
        assert!(files.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_list_files_and_partial() -> anyhow::Result<()> {
        let (server, temp, dir) = setup_server_with_dir()?;

        fs::create_dir_all(temp.child("subdir"))?;

        temp.child("foo.txt").write_str("hello")?;
        temp.child("subdir/foo2.txt").write_str("hello")?;
        temp.child(".bar.txt.part").write_str("partial")?;
        temp.child("subdir/.bar2.txt.part").write_str("partial")?;

        let files = server
            .list(
                tarpc::context::current(),
                dir.id().clone(),
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
    async fn test_send_wrong_order() -> anyhow::Result<()> {
        let (server, _temp, dir) = setup_server_with_dir()?;

        let fpath = LogicalPath::new(&dir, &PathBuf::from("wrong_order.txt"));

        server
            .clone()
            .send(
                tarpc::context::current(),
                dir.id().clone(),
                fpath.relative_path().to_path_buf(),
                (5, 10),
                10,
                b"fghij".to_vec(),
                Options::default(),
            )
            .await?;
        assert!(fpath.partial_path().exists());
        assert!(!fpath.final_path().exists());
        assert_eq!(
            std::fs::read_to_string(fpath.partial_path())?,
            "\0\0\0\0\0fghij"
        );

        server
            .clone()
            .send(
                tarpc::context::current(),
                dir.id().clone(),
                fpath.relative_path().to_path_buf(),
                (0, 5),
                10,
                b"abcde".to_vec(),
                Options::default(),
            )
            .await?;
        assert!(fpath.partial_path().exists());
        assert!(!fpath.final_path().exists());
        assert_eq!(std::fs::read_to_string(fpath.partial_path())?, "abcdefghij");

        Ok(())
    }

    #[tokio::test]
    async fn test_read() -> anyhow::Result<()> {
        let (server, _temp, dir) = setup_server_with_dir()?;

        let fpath = LogicalPath::new(&dir, &PathBuf::from("wrong_order.txt"));
        fs::write(fpath.final_path(), "abcdefghij")?;

        let data = server
            .clone()
            .read(
                tarpc::context::current(),
                dir.id().clone(),
                fpath.relative_path().to_path_buf(),
                (5, 10),
                Options::default(),
            )
            .await?;
        assert_eq!(String::from_utf8(data)?, "fghij");

        let data = server
            .clone()
            .read(
                tarpc::context::current(),
                dir.id().clone(),
                fpath.relative_path().to_path_buf(),
                (0, 5),
                Options::default(),
            )
            .await?;
        assert_eq!(String::from_utf8(data)?, "abcde");

        let result = server
            .clone()
            .read(
                tarpc::context::current(),
                dir.id().clone(),
                fpath.relative_path().to_path_buf(),
                (0, 15),
                Options::default(),
            )
            .await?;
        assert_eq!(result.len(), 15);
        assert_eq!(result, b"abcdefghij\0\0\0\0\0");

        Ok(())
    }

    #[tokio::test]
    async fn test_finish_partial() -> anyhow::Result<()> {
        let (server, _temp, dir) = setup_server_with_dir()?;

        let fpath = LogicalPath::new(&dir, &PathBuf::from("finish_partial.txt"));
        fs::write(fpath.partial_path(), "abcde")?;

        server
            .finish(
                tarpc::context::current(),
                dir.id().clone(),
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
    async fn test_finish_final() -> anyhow::Result<()> {
        let (server, _temp, dir) = setup_server_with_dir()?;

        let fpath = LogicalPath::new(&dir, &PathBuf::from("finish_partial.txt"));
        fs::write(fpath.final_path(), "abcde")?;

        server
            .finish(
                tarpc::context::current(),
                dir.id().clone(),
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
    async fn test_send_truncates_file() -> anyhow::Result<()> {
        let (server, _temp, dir) = setup_server_with_dir()?;
        let fpath = LogicalPath::new(&dir, &PathBuf::from("truncate.txt"));
        // Write a file with more data than we will send
        std::fs::write(fpath.partial_path(), b"abcdefghij")?;
        // Now send a chunk that should truncate the file to 7 bytes
        server
            .clone()
            .send(
                tarpc::context::current(),
                dir.id().clone(),
                fpath.relative_path().to_path_buf(),
                (0, 7),
                7,
                b"1234567".to_vec(),
                Options::default(),
            )
            .await?;
        let content = std::fs::read_to_string(fpath.partial_path())?;
        assert_eq!(content, "1234567");
        Ok(())
    }

    #[tokio::test]
    async fn test_hash_final_and_partial() -> anyhow::Result<()> {
        let (server, _temp, dir) = setup_server_with_dir()?;
        let content = b"hello world";
        let fpath = LogicalPath::new(&dir, &PathBuf::from("foo.txt"));
        std::fs::write(fpath.final_path(), content)?;
        let hash = server
            .clone()
            .hash(
                tarpc::context::current(),
                dir.id().clone(),
                PathBuf::from("foo.txt"),
                Options::default(),
            )
            .await?;
        let mut hasher = Sha256::new();
        hasher.update(content);
        let expected = FileHash(hasher.finalize().into());
        assert_eq!(hash, expected);

        // Now test partial
        let content2 = b"partial content";
        let fpath2 = LogicalPath::new(&dir, &PathBuf::from("bar.txt"));
        std::fs::write(fpath2.partial_path(), content2)?;
        let hash2 = server
            .clone()
            .hash(
                tarpc::context::current(),
                dir.id().clone(),
                PathBuf::from("bar.txt"),
                Options::default(),
            )
            .await?;
        let mut hasher2 = Sha256::new();
        hasher2.update(content2);
        let expected2 = FileHash(hasher2.finalize().into());
        assert_eq!(hash2, expected2);
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_final_and_partial() -> anyhow::Result<()> {
        let (server, _temp, dir) = setup_server_with_dir()?;
        let fpath = LogicalPath::new(&dir, &PathBuf::from("foo.txt"));
        std::fs::write(fpath.final_path(), b"data")?;
        std::fs::write(fpath.partial_path(), b"data2")?;
        assert!(fpath.final_path().exists());
        assert!(fpath.partial_path().exists());
        server
            .clone()
            .delete(
                tarpc::context::current(),
                dir.id().clone(),
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
                dir.id().clone(),
                PathBuf::from("foo.txt"),
                Options::default(),
            )
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_tarpc_rpc_inprocess() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let server_impl = RealizeServer::new(vec![Directory::new(
            &DirectoryId::from("testdir"),
            temp.path(),
        )]);
        let client = server_impl.as_inprocess_client();

        let list = client
            .list(
                tarpc::context::current(),
                DirectoryId::from("testdir"),
                Options::default(),
            )
            .await??;
        assert_eq!(list.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_calculate_signature_and_diff_and_apply_delta() -> anyhow::Result<()> {
        let (server, temp, dir) = setup_server_with_dir()?;
        let file_path = PathBuf::from("foo.txt");
        let file_content = b"hello world, this is a test of rsync signature!";
        temp.child("foo.txt").write_binary(file_content)?;

        // Calculate signature for the whole file
        let sig = server
            .clone()
            .calculate_signature(
                tarpc::context::current(),
                dir.id().clone(),
                file_path.clone(),
                (0, file_content.len() as u64),
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
                dir.id().clone(),
                file_path.clone(),
                (0, new_content.len() as u64),
                sig.clone(),
                Options::default(),
            )
            .await?;
        assert!(!delta.0.is_empty());

        // Revert file to old content, then apply delta
        temp.child("foo.txt").write_binary(file_content)?;
        let res = server
            .clone()
            .apply_delta(
                tarpc::context::current(),
                dir.id().clone(),
                file_path.clone(),
                (0, new_content.len() as u64),
                new_content.len() as u64,
                delta,
                Options::default(),
            )
            .await;
        assert!(res.is_ok());
        // File should now match new_content
        let logical = LogicalPath::new(&dir, &file_path);
        let (_state, actual) = logical.find(&Options::default())?;
        let mut buf = Vec::new();
        std::fs::File::open(&actual)?.read_to_end(&mut buf)?;
        assert_eq!(&buf[..new_content.len()], new_content);

        Ok(())
    }

    #[tokio::test]
    async fn test_signature_on_nonexisting_file() -> anyhow::Result<()> {
        let (server, _temp, dir) = setup_server_with_dir()?;
        // Nonexistent file
        let result = server
            .clone()
            .calculate_signature(
                tarpc::context::current(),
                dir.id().clone(),
                PathBuf::from("notfound.txt"),
                (0, 10),
                Options::default(),
            )
            .await;
        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_delta_error_case() -> anyhow::Result<()> {
        let (server, temp, dir) = setup_server_with_dir()?;
        let file_path = PathBuf::from("foo.txt");
        temp.child("foo.txt").write_str("abc")?;
        // Try to apply a bogus delta
        let result = server
            .clone()
            .apply_delta(
                tarpc::context::current(),
                dir.id().clone(),
                file_path,
                (0, 3),
                3,
                crate::model::service::Delta(vec![1, 2, 3]),
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
    async fn test_diff_and_apply_delta_with_shorter_content() -> anyhow::Result<()> {
        let (server, temp, dir) = setup_server_with_dir()?;
        let file_path = PathBuf::from("shorten.txt");
        let original_content = b"this is the original, longer content";
        temp.child("shorten.txt").write_binary(original_content)?;

        // Calculate signature for the whole file
        let sig = server
            .clone()
            .calculate_signature(
                tarpc::context::current(),
                dir.id().clone(),
                file_path.clone(),
                (0, original_content.len() as u64),
                Options::default(),
            )
            .await?;

        // Prepare shorter new content
        let new_content = b"short";
        temp.child("shorten.txt").write_binary(new_content)?;

        // Diff: get delta from old signature to new (shorter) content
        let delta = server
            .clone()
            .diff(
                tarpc::context::current(),
                dir.id().clone(),
                file_path.clone(),
                (0, new_content.len() as u64),
                sig.clone(),
                Options::default(),
            )
            .await?;
        assert!(!delta.0.is_empty());

        // Revert file to original content, then apply delta
        temp.child("shorten.txt").write_binary(original_content)?;
        let res = server
            .clone()
            .apply_delta(
                tarpc::context::current(),
                dir.id().clone(),
                file_path.clone(),
                (0, new_content.len() as u64),
                new_content.len() as u64,
                delta,
                Options::default(),
            )
            .await;
        assert!(res.is_ok());
        // File should now match new_content and be truncated
        let logical = LogicalPath::new(&dir, &file_path);
        let (_state, actual) = logical.find(&Options::default())?;
        let mut buf = Vec::new();
        std::fs::File::open(&actual)?.read_to_end(&mut buf)?;
        assert_eq!(&buf, new_content);
        Ok(())
    }

    #[tokio::test]
    async fn test_list_partial_vs_final() -> anyhow::Result<()> {
        let (server, temp, dir) = setup_server_with_dir()?;
        temp.child("foo.txt").write_str("final")?;
        temp.child(".foo.txt.part").write_str("partial")?;
        // Default: partial preferred
        let files = server
            .clone()
            .list(
                tarpc::context::current(),
                dir.id().clone(),
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
                dir.id().clone(),
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
    async fn test_list_ignore_partial() -> anyhow::Result<()> {
        let (server, temp, dir) = setup_server_with_dir()?;
        temp.child(".foo.txt.part").write_str("partial")?;
        // Default: return partial
        let files = server
            .clone()
            .list(
                tarpc::context::current(),
                dir.id().clone(),
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
                dir.id().clone(),
                Options {
                    ignore_partial: true,
                },
            )
            .await?;
        assert_eq!(files.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_read_partial_vs_final() -> anyhow::Result<()> {
        let (server, temp, dir) = setup_server_with_dir()?;
        temp.child("foo.txt").write_str("final")?;
        temp.child(".foo.txt.part").write_str("partial")?;
        // Default: partial preferred
        let data = server
            .clone()
            .read(
                tarpc::context::current(),
                dir.id().clone(),
                PathBuf::from("foo.txt"),
                (0, 7),
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
                dir.id().clone(),
                PathBuf::from("foo.txt"),
                (0, 5),
                Options {
                    ignore_partial: true,
                },
            )
            .await?;
        assert_eq!(String::from_utf8(data)?, "final");
        Ok(())
    }
}
