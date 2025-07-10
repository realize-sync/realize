use realize_types::{self, Arena, ByteRange, Delta, Hash, Signature};
use crate::config::ArenaConfig;
use crate::utils::hash;
use fast_rsync::SignatureOptions;
use std::cmp::min;
use std::collections::HashMap;
use std::ffi::OsString;
use std::io::{self, SeekFrom};
use std::iter;
use std::os::unix::fs::MetadataExt as _;
use std::path::{self, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _};
use tokio::task::JoinError;
use walkdir::WalkDir;

const RSYNC_BLOCK_SIZE: usize = 4096;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum StorageAccess {
    Read,
    ReadWrite,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum PathType {
    Partial,
    Final,
}

/// Configures the behavior of a method on [RealStoreService].
#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct Options {
    /// If true, only take final files into account. This is usually
    /// set on the source instance, to only move final files.
    pub ignore_partial: bool,
}

/// A file that can be synced through the service, within a directory.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SyncedFile {
    /// Relative path to a file within the directory.
    ///
    /// Absolute paths and .. are not supported.
    pub path: realize_types::Path,

    /// Size of the file on the current instance, in bytes.
    pub size: u64,

    /// Modification time of the file.
    pub mtime: SystemTime,
}

/// Local storage, for all [Arena]s.
#[derive(Clone)]
pub struct RealStore {
    map: Arc<HashMap<Arena, PathBuf>>,
}

impl RealStore {
    /// Create a [RealStore] from a set of [ArenaConfig].
    pub fn from_config(arenas: &HashMap<Arena, ArenaConfig>) -> Self {
        Self::new(
            arenas
                .iter()
                .map(|(arena, config)| (arena.clone(), config.path.to_path_buf())),
        )
    }

    /// Create and fill a [RealStore].
    pub fn new<T>(arenas: T) -> Self
    where
        T: IntoIterator<Item = (Arena, PathBuf)>,
    {
        Self {
            map: Arc::new(arenas.into_iter().collect()),
        }
    }

    /// Define a single local arena.
    pub fn single(arena: &Arena, path: &path::Path) -> Self {
        Self::new(iter::once((arena.clone(), path.to_path_buf())))
    }

    /// Returns the arenas configured in this store.
    pub fn arenas(&self) -> Vec<Arena> {
        self.map.keys().cloned().collect()
    }

    /// List files in a directory
    pub async fn list(
        &self,
        arena: &Arena,
        options: &Options,
    ) -> Result<Vec<SyncedFile>, RealStoreError> {
        let resolver = self.path_resolver_from_opts(arena, options)?.clone();

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

            Ok::<Vec<SyncedFile>, RealStoreError>(files.into_values().collect())
        })
        .await?
    }

    /// Send a byte range of a file.
    pub async fn send(
        &self,
        arena: &Arena,
        relative_path: &realize_types::Path,
        range: &ByteRange,
        data: Vec<u8>,
        options: &Options,
    ) -> Result<(), RealStoreError> {
        let resolver = self.path_resolver_from_opts(arena, options)?;
        let path = resolver.partial_path(relative_path).ok_or(bad_path())?;
        let mut file = open_for_range_write(&path).await?;
        write_at_offset(&mut file, range.start, &data).await?;

        Ok(())
    }

    /// Read a byte range from a file, returning the data.
    ///
    /// Data outside of the range [0, file_size) is returned filled with 0.
    pub async fn read(
        &self,
        arena: &Arena,
        relative_path: &realize_types::Path,
        range: &ByteRange,
        options: &Options,
    ) -> Result<Vec<u8>, RealStoreError> {
        let resolver = self.path_resolver_from_opts(arena, options)?;
        let actual = resolver.resolve(relative_path).await?;
        let mut file = open_for_range_read(&actual).await?;
        let mut buffer = vec![0; range.bytecount() as usize];
        read_padded(&mut file, range, &mut buffer).await?;

        Ok(buffer)
    }

    /// Mark a partial file as complete
    pub async fn finish(
        &self,
        arena: &Arena,
        relative_path: &realize_types::Path,
        options: &Options,
    ) -> Result<(), RealStoreError> {
        if options.ignore_partial {
            return Err(RealStoreError::BadRequest(
                "Invalid option for finish: ignore_partial=true".to_string(),
            ));
        }
        let resolver = self.path_resolver_from_opts(arena, options)?;
        let partial_path = resolver.partial_path(relative_path).ok_or(bad_path())?;
        let actual = resolver.resolve(relative_path).await?;
        if actual == partial_path {
            fs::rename(
                actual,
                resolver.final_path(relative_path).ok_or(bad_path())?,
            )
            .await?;
        }
        Ok(())
    }

    /// Compute a SHA-256 hash of the file at the given path (final or partial).
    pub async fn hash(
        &self,
        arena: &Arena,
        relative_path: &realize_types::Path,
        range: &ByteRange,
        options: &Options,
    ) -> Result<Hash, RealStoreError> {
        let resolver = self.path_resolver_from_opts(arena, options)?;
        let path = resolver.resolve(relative_path).await?;

        hash_large_range_exact(&path, range).await
    }

    /// Delete the file at the given path (both partial and final forms).
    pub async fn delete(
        &self,
        arena: &Arena,
        relative_path: &realize_types::Path,
        options: &Options,
    ) -> Result<(), RealStoreError> {
        let resolver = self.path_resolver_from_opts(arena, options)?;
        let final_path = resolver.final_path(relative_path).ok_or(bad_path())?;
        if fs::metadata(&final_path).await.is_ok() {
            fs::remove_file(final_path).await?;
        }

        let partial_path = resolver.partial_path(relative_path).ok_or(bad_path())?;
        if fs::metadata(&partial_path).await.is_ok() {
            fs::remove_file(partial_path).await?;
        }
        delete_containing_dir(resolver.root(), relative_path).await;
        Ok(())
    }

    /// Calculate a signature for the file at the given path and byte range.
    pub async fn calculate_signature(
        &self,
        arena: &Arena,
        relative_path: &realize_types::Path,
        range: &ByteRange,
        options: &Options,
    ) -> Result<Signature, RealStoreError> {
        let resolver = self.path_resolver_from_opts(arena, options)?;
        let actual = resolver.resolve(relative_path).await?;
        let mut file = File::open(&actual).await?;
        let mut buffer = vec![0u8; range.bytecount() as usize];
        read_padded(&mut file, range, &mut buffer).await?;
        let opts = SignatureOptions {
            block_size: RSYNC_BLOCK_SIZE as u32,
            crypto_hash_size: 8,
        };
        let sig = fast_rsync::Signature::calculate(&buffer, opts);
        Ok(realize_types::Signature(sig.into_serialized()))
    }

    /// Compute a delta from the file at the given path and a given signature.
    ///
    /// Returns the delta and the hash of the data used to compute it.
    pub async fn diff(
        &self,
        arena: &Arena,
        relative_path: &realize_types::Path,
        range: &ByteRange,
        signature: Signature,
        options: &Options,
    ) -> Result<(Delta, Hash), RealStoreError> {
        let resolver = self.path_resolver_from_opts(arena, options)?;
        let actual = resolver.resolve(relative_path).await?;
        let mut file = open_for_range_read(&actual).await?;
        let mut buffer = vec![0; range.bytecount() as usize];
        read_padded(&mut file, range, &mut buffer).await?;

        let hash = hash::digest(&buffer);

        let sig = fast_rsync::Signature::deserialize(signature.0)?;
        let mut delta = Vec::new();
        fast_rsync::diff(&sig.index(), &buffer, &mut delta)?;

        Ok((realize_types::Delta(delta), hash))
    }

    /// Apply a delta to the file at the given path and byte range, verifying the hash
    ///
    /// Returns the error [RealizeError::HashMismatch] if, after
    /// applying the patch, the data doesn't match the given hash.
    pub async fn apply_delta(
        &self,
        arena: &Arena,
        relative_path: &realize_types::Path,
        range: &ByteRange,
        delta: Delta,
        hash: &Hash,
        options: &Options,
    ) -> Result<(), RealStoreError> {
        let resolver = self.path_resolver_from_opts(arena, options)?;
        let path = resolver.partial_path(relative_path).ok_or(bad_path())?;
        let mut file = open_for_range_write(&path).await?;
        let mut base = vec![0u8; range.bytecount() as usize];
        read_padded(&mut file, range, &mut base).await?;
        let mut out = Vec::new();
        fast_rsync::apply_limited(&base, &delta.0, &mut out, range.bytecount() as usize)?;
        if out.len() as u64 != range.bytecount() {
            return Err(RealStoreError::BadRequest(
                "Delta output size mismatch".to_string(),
            ));
        }
        if hash::digest(&out) != *hash {
            return Err(RealStoreError::HashMismatch);
        }
        write_at_offset(&mut file, range.start, &out).await?;

        Ok(())
    }

    /// Truncate file to the given size.
    pub async fn truncate(
        &self,
        arena: &Arena,
        relative_path: &realize_types::Path,
        file_size: u64,
        options: &Options,
    ) -> Result<(), RealStoreError> {
        let resolver = self.path_resolver_from_opts(arena, options)?;
        let path = resolver.partial_path(relative_path).ok_or(bad_path())?;
        let mut file = open_for_range_write(&path).await?;
        shorten_file(&mut file, file_size).await?;

        Ok(())
    }

    /// Builds a path resolver for the given arena.
    fn path_resolver(&self, arena: &Arena, access: StorageAccess) -> Option<PathResolver> {
        self.map
            .get(arena)
            .map(|p| PathResolver::new(p.clone(), access))
    }

    fn path_resolver_from_opts(
        &self,
        arena: &Arena,
        opts: &Options,
    ) -> Result<PathResolver, RealStoreError> {
        self.path_resolver(
            arena,
            if opts.ignore_partial {
                StorageAccess::Read
            } else {
                StorageAccess::ReadWrite
            },
        )
        .ok_or(unknown_arena())
    }
}

#[derive(Clone)]
pub struct PathResolver {
    path: PathBuf,
    access: StorageAccess,
}

impl PathResolver {
    fn new(path: PathBuf, access: StorageAccess) -> Self {
        Self { path, access }
    }

    /// Return the OS path that'll be the parent of all returned OS
    /// paths.
    pub fn root(&self) -> &path::Path {
        self.path.as_ref()
    }

    pub fn partial_path(&self, path: &realize_types::Path) -> Option<path::PathBuf> {
        let full_path = to_full_path(&self.path, path)?;

        Some(to_partial(&full_path))
    }

    pub fn final_path(&self, path: &realize_types::Path) -> Option<path::PathBuf> {
        to_full_path(&self.path, path)
    }

    /// Resolve a model path into an OS path.
    ///
    /// The OS path might not exist or some error might prevent it
    /// from being accessible.
    pub async fn resolve(&self, path: &realize_types::Path) -> Result<path::PathBuf, io::Error> {
        let full_path = to_full_path(&self.path, path).ok_or(not_found())?;

        if self.access == StorageAccess::ReadWrite {
            let partial_path = to_partial(&full_path);
            if fs::metadata(&partial_path).await.is_ok() {
                return Ok(partial_path);
            }
        }

        fs::metadata(&full_path).await?; // Make sure it exists

        Ok(full_path)
    }

    /// Resolve an OS path into a model path.
    ///
    /// The OS path might not be part of the model, so reverse
    /// sometimes return None.
    pub fn reverse(&self, actual: &path::Path) -> Option<(PathType, realize_types::Path)> {
        if self.access == StorageAccess::Read && is_partial(actual) {
            return None;
        }
        let relative = pathdiff::diff_paths(actual, &self.path);
        if let Some(mut relative) = relative {
            let mut path_type = PathType::Final;
            if self.access == StorageAccess::ReadWrite {
                if let Some(non_partial) = non_partial_name(&relative) {
                    relative.set_file_name(OsString::from(non_partial.to_string()));
                    path_type = PathType::Partial;
                }
            }
            if let Ok(p) = realize_types::Path::from_real_path(&relative) {
                return Some((path_type, p));
            }
        }

        None
    }
}

/// Check whether a path is partial
fn is_partial(path: &path::Path) -> bool {
    non_partial_name(path).is_some()
}

/// Convert a path to its partial equivalent.
fn to_partial(path: &path::Path) -> path::PathBuf {
    let mut path = path.to_path_buf();

    if let Some(filename) = path.file_name() {
        let mut part_filename = OsString::from(".");
        part_filename.push(filename);
        part_filename.push(OsString::from(".part"));
        path.set_file_name(part_filename);
    }

    path
}

/// Extract the non-partial name from a partial path.
///
/// Returns None unless given a valid partial path.
fn non_partial_name(path: &path::Path) -> Option<&str> {
    if let Some(filename) = path.file_name() {
        if let Some(name) = filename.to_str() {
            if name.starts_with(".") && name.ends_with(".part") {
                return Some(&name[1..name.len() - 5]);
            }
        }
    }

    None
}

/// Build a full path from a root and relative model path.
///
/// This function refuses to return partial paths, even if the model
/// path is trying to point to a partial path, as partial paths never
/// exist in this view of the Arena.
fn to_full_path(root: &path::Path, relative: &realize_types::Path) -> Option<path::PathBuf> {
    let full_path = relative.within(root);
    if is_partial(&full_path) {
        return None;
    }

    Some(full_path)
}

fn not_found() -> std::io::Error {
    std::io::Error::from(std::io::ErrorKind::NotFound)
}

/// Error type used by [RealStoreService].
///
/// The data stored in this error is limited, to be serializable and
/// remain usable through tarpc.
#[derive(thiserror::Error, Debug, serde::Serialize, serde::Deserialize)]
pub enum RealStoreError {
    /// Returned by the RealStoreService when given an invalid request.
    #[error("Bad request: {0}")]
    BadRequest(String),

    #[error("I/O error: {0}")]
    Io(String),

    #[error("RSync {0:?} error: {1}")]
    Rsync(RsyncOperation, String),

    #[error("Unexpected: {0}")]
    Other(String),

    /// Returned by [RealStoreService::apply_delta] when the resulting
    /// patch didn't match the hash created by [RealStoreService::diff].
    #[error("Hash mismatch after rsync")]
    HashMismatch,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum RsyncOperation {
    Diff,
    Apply,
    Sign,
}

impl From<std::io::Error> for RealStoreError {
    fn from(value: std::io::Error) -> Self {
        RealStoreError::Io(value.to_string())
    }
}

impl From<anyhow::Error> for RealStoreError {
    fn from(value: anyhow::Error) -> Self {
        RealStoreError::Other(value.to_string())
    }
}

async fn open_for_range_write(path: &std::path::Path) -> Result<File, RealStoreError> {
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
async fn open_for_range_read(path: &std::path::Path) -> Result<File, RealStoreError> {
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
    path: &std::path::Path,
    range: &ByteRange,
) -> Result<Hash, RealStoreError> {
    let mut file = open_for_range_read(path).await?;
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

async fn shorten_file(file: &mut File, file_size: u64) -> Result<(), RealStoreError> {
    if file.metadata().await?.len() > file_size {
        file.set_len(file_size).await?;
        file.flush().await?;
        file.sync_all().await?;
    }

    Ok(())
}

async fn write_at_offset(file: &mut File, offset: u64, data: &[u8]) -> Result<(), RealStoreError> {
    file.seek(SeekFrom::Start(offset)).await?;
    file.write_all(data).await?;
    file.flush().await?;
    file.sync_all().await?;

    Ok(())
}

impl From<walkdir::Error> for RealStoreError {
    fn from(err: walkdir::Error) -> Self {
        if err.io_error().is_some() {
            // We know err contains an io error; unwrap will succeed.
            err.into_io_error().unwrap().into()
        } else {
            anyhow::Error::new(err).into()
        }
    }
}

impl From<JoinError> for RealStoreError {
    fn from(err: JoinError) -> Self {
        anyhow::Error::new(err).into()
    }
}

// Add error conversions for fast_rsync errors
impl From<fast_rsync::DiffError> for RealStoreError {
    fn from(e: fast_rsync::DiffError) -> Self {
        RealStoreError::Rsync(RsyncOperation::Diff, e.to_string())
    }
}
impl From<fast_rsync::ApplyError> for RealStoreError {
    fn from(e: fast_rsync::ApplyError) -> Self {
        RealStoreError::Rsync(RsyncOperation::Apply, e.to_string())
    }
}

impl From<fast_rsync::SignatureParseError> for RealStoreError {
    fn from(e: fast_rsync::SignatureParseError) -> Self {
        RealStoreError::Rsync(RsyncOperation::Sign, e.to_string())
    }
}

/// Remove empty parent directories of [relative_path].
///
/// Errors are ignored. Deleting just stops.
async fn delete_containing_dir(root: &std::path::Path, relative_path: &realize_types::Path) {
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

async fn is_empty_dir(path: &std::path::Path) -> bool {
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
    false
}

fn unknown_arena() -> RealStoreError {
    RealStoreError::BadRequest("Unknown arena".to_string())
}

fn bad_path() -> RealStoreError {
    RealStoreError::BadRequest("Path is invalid".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use realize_types::Hash;
    use crate::utils::hash;
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use assert_unordered::assert_eq_unordered;
    use std::ffi::OsString;
    use std::io::Read as _;
    use std::path::PathBuf;
    use std::{fs, path};

    #[tokio::test]
    async fn partial_and_final_paths() -> anyhow::Result<()> {
        let arena = &Arena::from("test");
        let resolver = RealStore::single(arena, path::Path::new("/doesnotexist/test"))
            .path_resolver(arena, StorageAccess::Read)
            .ok_or(not_found())?;

        assert_eq!(
            Some(path::PathBuf::from("/doesnotexist/test/foo.txt")),
            resolver.final_path(&realize_types::Path::parse("foo.txt")?)
        );
        assert_eq!(
            Some(path::PathBuf::from("/doesnotexist/test/.foo.txt.part")),
            resolver.partial_path(&realize_types::Path::parse("foo.txt")?)
        );
        assert_eq!(
            Some(path::PathBuf::from("/doesnotexist/test/subdir/.foo.txt")),
            resolver.final_path(&realize_types::Path::parse("subdir/.foo.txt")?)
        );
        assert_eq!(
            Some(path::PathBuf::from(
                "/doesnotexist/test/subdir/..foo.txt.part"
            )),
            resolver.partial_path(&realize_types::Path::parse("subdir/.foo.txt")?)
        );

        Ok(())
    }

    #[tokio::test]
    async fn resolve_in_readonly_arena() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let arena = &Arena::from("test");
        let resolver = RealStore::single(arena, temp.path())
            .path_resolver(arena, StorageAccess::Read)
            .ok_or(not_found())?;

        let simple_file = &realize_types::Path::parse("foo.txt")?;
        assert!(matches!(
            resolver.resolve(simple_file).await,
            Err(std::io::Error { .. })
        ));

        temp.child(".foo.txt.part").write_str("")?;
        assert!(matches!(
            resolver.resolve(simple_file).await,
            Err(std::io::Error { .. })
        ));

        temp.child("foo.txt").write_str("")?;
        assert_eq!(
            temp.child("foo.txt").path().to_path_buf(),
            resolver.resolve(simple_file).await?,
        );

        let file_in_subdir = &realize_types::Path::parse("foo/bar.txt")?;
        assert!(matches!(
            resolver.resolve(file_in_subdir).await,
            Err(std::io::Error { .. })
        ));

        temp.child("foo/.bar.txt.part").write_str("")?;
        assert!(matches!(
            resolver.resolve(file_in_subdir).await,
            Err(std::io::Error { .. })
        ));

        temp.child("foo/bar.txt").write_str("")?;
        assert_eq!(
            temp.child("foo/bar.txt").path().to_path_buf(),
            resolver.resolve(file_in_subdir).await?,
        );

        let hidden_file = &realize_types::Path::parse(".foo/.bar.txt")?;
        assert!(matches!(
            resolver.resolve(hidden_file).await,
            Err(std::io::Error { .. })
        ));

        temp.child(".foo/.bar.txt").write_str("")?;
        assert_eq!(
            temp.child(".foo/.bar.txt").path().to_path_buf(),
            resolver.resolve(hidden_file).await?,
        );

        Ok(())
    }

    #[tokio::test]
    async fn ignore_model_path_with_partial_name_in_readonly_area() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let arena = &Arena::from("test");
        let resolver = RealStore::single(arena, temp.path())
            .path_resolver(arena, StorageAccess::Read)
            .ok_or(not_found())?;

        let simple_file = &realize_types::Path::parse(".foo.txt.part")?;
        assert!(matches!(
            resolver.resolve(simple_file).await,
            Err(std::io::Error { .. })
        ));

        temp.child(".foo.txt").write_str("")?;
        assert!(matches!(
            resolver.resolve(simple_file).await,
            Err(std::io::Error { .. })
        ));

        let file_in_subdir = &realize_types::Path::parse("foo/.bar.txt.part")?;
        assert!(matches!(
            resolver.resolve(file_in_subdir).await,
            Err(std::io::Error { .. })
        ));

        temp.child("foo/.bar.txt.part").write_str("")?;
        assert!(matches!(
            resolver.resolve(file_in_subdir).await,
            Err(std::io::Error { .. })
        ));

        Ok(())
    }

    #[tokio::test]
    async fn resolve_in_writable_arena() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let arena = &Arena::from("test");
        let resolver = RealStore::single(arena, temp.path())
            .path_resolver(arena, StorageAccess::ReadWrite)
            .ok_or(not_found())?;

        let simple_file = &realize_types::Path::parse("foo.txt")?;
        assert!(matches!(
            resolver.resolve(simple_file).await,
            Err(std::io::Error { .. })
        ));

        temp.child("foo.txt").write_str("")?;
        assert_eq!(
            temp.child("foo.txt").path().to_path_buf(),
            resolver.resolve(simple_file).await?,
        );

        temp.child(".foo.txt.part").write_str("")?;
        assert_eq!(
            temp.child(".foo.txt.part").path().to_path_buf(),
            resolver.resolve(simple_file).await?,
        );

        let file_in_subdir = &realize_types::Path::parse("foo/bar.txt")?;
        assert!(matches!(
            resolver.resolve(file_in_subdir).await,
            Err(std::io::Error { .. })
        ));

        temp.child("foo/bar.txt").write_str("")?;
        assert_eq!(
            temp.child("foo/bar.txt").path().to_path_buf(),
            resolver.resolve(file_in_subdir).await?,
        );

        temp.child("foo/.bar.txt.part").write_str("")?;
        assert_eq!(
            temp.child("foo/.bar.txt.part").path().to_path_buf(),
            resolver.resolve(file_in_subdir).await?,
        );

        let hidden_file = &realize_types::Path::parse(".foo/.bar.txt")?;
        assert!(matches!(
            resolver.resolve(hidden_file).await,
            Err(std::io::Error { .. })
        ));

        temp.child(".foo/.bar.txt").write_str("")?;
        assert_eq!(
            temp.child(".foo/.bar.txt").path().to_path_buf(),
            resolver.resolve(hidden_file).await?,
        );

        temp.child(".foo/..bar.txt.part").write_str("")?;
        assert_eq!(
            temp.child(".foo/..bar.txt.part").path().to_path_buf(),
            resolver.resolve(hidden_file).await?,
        );

        Ok(())
    }

    #[tokio::test]
    async fn ignore_model_path_with_partial_name_in_writable_arena() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let arena = &Arena::from("test");
        let resolver = RealStore::single(arena, temp.path())
            .path_resolver(arena, StorageAccess::ReadWrite)
            .ok_or(not_found())?;

        let simple_file = &realize_types::Path::parse(".foo.txt.part")?;
        assert!(matches!(
            resolver.resolve(simple_file).await,
            Err(std::io::Error { .. })
        ));

        temp.child(".foo.txt").write_str("")?;
        assert!(matches!(
            resolver.resolve(simple_file).await,
            Err(std::io::Error { .. })
        ));

        let file_in_subdir = &realize_types::Path::parse("foo/.bar.txt.part")?;
        assert!(matches!(
            resolver.resolve(file_in_subdir).await,
            Err(std::io::Error { .. })
        ));

        temp.child("foo/.bar.txt.part").write_str("")?;
        assert!(matches!(
            resolver.resolve(file_in_subdir).await,
            Err(std::io::Error { .. })
        ));

        Ok(())
    }

    #[tokio::test]
    async fn reverse_in_readonly_arena() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let arena = &Arena::from("test");
        let storage = RealStore::single(arena, temp.path())
            .path_resolver(arena, StorageAccess::Read)
            .ok_or(not_found())?;

        assert_eq!(
            Some((PathType::Final, realize_types::Path::parse("foo/bar")?)),
            storage.reverse(temp.child("foo/bar").path())
        );

        assert_eq!(None, storage.reverse(temp.child("foo/.bar.part").path()));
        assert_eq!(None, storage.reverse(temp.child("../foo/bar").path()));
        assert_eq!(
            None,
            storage.reverse(path::Path::new("/some/other/dir/foo/bar"))
        );

        Ok(())
    }

    #[tokio::test]
    async fn reverse_in_writable_arena() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let arena = &Arena::from("test");
        let storage = RealStore::single(arena, temp.path())
            .path_resolver(arena, StorageAccess::ReadWrite)
            .ok_or(not_found())?;

        assert_eq!(
            Some((PathType::Final, realize_types::Path::parse("foo/bar")?)),
            storage.reverse(temp.child("foo/bar").path())
        );

        assert_eq!(
            Some((PathType::Partial, realize_types::Path::parse("foo/bar")?)),
            storage.reverse(temp.child("foo/.bar.part").path())
        );

        assert_eq!(None, storage.reverse(temp.child("../foo/bar").path()));

        assert_eq!(
            None,
            storage.reverse(path::Path::new("/some/other/dir/foo/bar"))
        );

        Ok(())
    }

    struct LogicalPath {
        arena_path: PathBuf,
        path: realize_types::Path,
    }

    impl LogicalPath {
        fn new(arena_path: &path::Path, path: &realize_types::Path) -> Self {
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

    fn setup_store() -> anyhow::Result<(RealStore, TempDir, Arena)> {
        let tempdir = TempDir::new()?;
        let arena = Arena::from("testdir");

        Ok((RealStore::single(&arena, tempdir.path()), tempdir, arena))
    }

    #[tokio::test]
    async fn list_empty() -> anyhow::Result<()> {
        let (store, _temp, arena) = setup_store()?;
        let files = store.list(&arena, &Options::default()).await?;
        assert!(files.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn list_files_and_partial() -> anyhow::Result<()> {
        let (store, temp, arena) = setup_store()?;

        fs::create_dir_all(temp.child("subdir"))?;

        let foo = temp.child("foo.txt");
        foo.write_str("hello")?;
        let foo2 = temp.child("subdir/foo2.txt");
        foo2.write_str("hello")?;
        let bar = temp.child(".bar.txt.part");
        bar.write_str("partial")?;
        let bar2 = temp.child("subdir/.bar2.txt.part");
        bar2.write_str("partial")?;

        let files = store.list(&arena, &Options::default()).await?;
        assert_eq_unordered!(
            files,
            vec![
                SyncedFile {
                    path: realize_types::Path::parse("foo.txt")?,
                    size: 5,
                    mtime: foo.metadata()?.modified()?,
                },
                SyncedFile {
                    path: realize_types::Path::parse("subdir/foo2.txt")?,
                    size: 5,
                    mtime: foo2.metadata()?.modified()?,
                },
                SyncedFile {
                    path: realize_types::Path::parse("bar.txt")?,
                    size: 7,
                    mtime: bar.metadata()?.modified()?,
                },
                SyncedFile {
                    path: realize_types::Path::parse("subdir/bar2.txt")?,
                    size: 7,
                    mtime: bar2.metadata()?.modified()?,
                },
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn send_wrong_order() -> anyhow::Result<()> {
        let (store, temp, arena) = setup_store()?;
        let fpath = LogicalPath::new(temp.path(), &realize_types::Path::parse("wrong_order.txt")?);
        let data1 = b"fghij".to_vec();
        store
            .send(
                &arena,
                &fpath.path,
                &ByteRange { start: 5, end: 10 },
                data1.clone(),
                &Options::default(),
            )
            .await?;
        assert!(fpath.partial_path().exists());
        assert!(!fpath.final_path().exists());
        assert_eq!(
            std::fs::read_to_string(fpath.partial_path())?,
            "\0\0\0\0\0fghij"
        );
        let data2 = b"abcde".to_vec();
        store
            .send(
                &arena,
                &fpath.path,
                &ByteRange { start: 0, end: 5 },
                data2.clone(),
                &Options::default(),
            )
            .await?;
        assert!(fpath.partial_path().exists());
        assert!(!fpath.final_path().exists());
        assert_eq!(std::fs::read_to_string(fpath.partial_path())?, "abcdefghij");
        Ok(())
    }

    #[tokio::test]
    async fn read() -> anyhow::Result<()> {
        let (store, temp, arena) = setup_store()?;

        let fpath = LogicalPath::new(temp.path(), &realize_types::Path::parse("wrong_order.txt")?);
        fs::write(fpath.final_path(), "abcdefghij")?;

        let data = store
            .read(
                &arena,
                &fpath.path,
                &ByteRange { start: 5, end: 10 },
                &Options::default(),
            )
            .await?;
        assert_eq!(String::from_utf8(data)?, "fghij");

        let data = store
            .read(
                &arena,
                &fpath.path,
                &ByteRange { start: 0, end: 5 },
                &Options::default(),
            )
            .await?;
        assert_eq!(String::from_utf8(data)?, "abcde");

        let result = store
            .read(
                &arena,
                &fpath.path,
                &ByteRange { start: 0, end: 15 },
                &Options::default(),
            )
            .await?;
        assert_eq!(result.len(), 15);
        assert_eq!(result, b"abcdefghij\0\0\0\0\0");

        Ok(())
    }

    #[tokio::test]
    async fn finish_partial() -> anyhow::Result<()> {
        let (store, temp, arena) = setup_store()?;

        let fpath = LogicalPath::new(temp.path(), &realize_types::Path::parse("finish_partial.txt")?);
        fs::write(fpath.partial_path(), "abcde")?;

        store
            .finish(&arena, &fpath.path, &Options::default())
            .await?;

        assert!(fpath.final_path().exists());
        assert!(!fpath.partial_path().exists());
        assert_eq!(std::fs::read_to_string(fpath.final_path())?, "abcde");

        Ok(())
    }

    #[tokio::test]
    async fn finish_final() -> anyhow::Result<()> {
        let (store, temp, arena) = setup_store()?;

        let fpath = LogicalPath::new(temp.path(), &realize_types::Path::parse("finish_partial.txt")?);
        fs::write(fpath.final_path(), "abcde")?;

        store
            .finish(&arena, &fpath.path, &Options::default())
            .await?;

        assert!(fpath.final_path().exists());
        assert!(!fpath.partial_path().exists());
        assert_eq!(std::fs::read_to_string(fpath.final_path())?, "abcde");

        Ok(())
    }

    #[tokio::test]
    async fn truncate() -> anyhow::Result<()> {
        let (store, temp, arena) = setup_store()?;
        let fpath = LogicalPath::new(temp.path(), &realize_types::Path::parse("truncate.txt")?);
        std::fs::write(fpath.partial_path(), b"abcdefghij")?;
        store
            .truncate(&arena, &fpath.path, 7, &Options::default())
            .await?;
        let content = std::fs::read_to_string(fpath.partial_path())?;
        assert_eq!(content, "abcdefg");
        Ok(())
    }

    #[tokio::test]
    async fn hash_final_and_partial() -> anyhow::Result<()> {
        let (store, temp, arena) = setup_store()?;
        let content = b"hello world";
        let fpath = LogicalPath::new(temp.path(), &realize_types::Path::parse("foo.txt")?);
        std::fs::write(fpath.final_path(), content)?;
        let hash = store
            .hash(
                &arena,
                &realize_types::Path::parse("foo.txt")?,
                &ByteRange {
                    start: 0,
                    end: content.len() as u64,
                },
                &Options::default(),
            )
            .await?;
        let expected = hash::digest(content);
        assert_eq!(hash, expected);

        // Now test partial
        let content2 = b"partial content";
        let fpath2 = LogicalPath::new(temp.path(), &realize_types::Path::parse("bar.txt")?);
        std::fs::write(fpath2.partial_path(), content2)?;
        let hash2 = store
            .hash(
                &arena,
                &realize_types::Path::parse("bar.txt")?,
                &ByteRange {
                    start: 0,
                    end: content2.len() as u64,
                },
                &Options::default(),
            )
            .await?;
        let expected2 = hash::digest(content2);
        assert_eq!(hash2, expected2);
        Ok(())
    }

    #[tokio::test]
    async fn hash_past_file_end() -> anyhow::Result<()> {
        let (store, temp, arena) = setup_store()?;
        let content = b"hello world";
        let fpath = LogicalPath::new(temp.path(), &realize_types::Path::parse("foo.txt")?);
        std::fs::write(fpath.final_path(), content)?;
        let hash = store
            .hash(
                &arena,
                &realize_types::Path::parse("foo.txt")?,
                &ByteRange {
                    start: 100,
                    end: 200,
                },
                &Options::default(),
            )
            .await?;
        assert_eq!(hash, Hash::zero());

        Ok(())
    }

    #[tokio::test]
    async fn hash_short_read() -> anyhow::Result<()> {
        let (store, temp, arena) = setup_store()?;
        let content = b"hello world";
        let fpath = LogicalPath::new(temp.path(), &realize_types::Path::parse("foo.txt")?);
        std::fs::write(fpath.final_path(), content)?;
        let hash = store
            .hash(
                &arena,
                &realize_types::Path::parse("foo.txt")?,
                &ByteRange { start: 0, end: 200 },
                &Options::default(),
            )
            .await?;
        assert_eq!(hash, Hash::zero());

        Ok(())
    }

    #[tokio::test]
    async fn delete_final_and_partial() -> anyhow::Result<()> {
        let (store, temp, arena) = setup_store()?;
        let fpath = LogicalPath::new(temp.path(), &realize_types::Path::parse("foo.txt")?);
        std::fs::write(fpath.final_path(), b"data")?;
        std::fs::write(fpath.partial_path(), b"data2")?;
        assert!(fpath.final_path().exists());
        assert!(fpath.partial_path().exists());
        store
            .delete(&arena, &realize_types::Path::parse("foo.txt")?, &Options::default())
            .await?;
        assert!(!fpath.final_path().exists());
        assert!(!fpath.partial_path().exists());
        // Should succeed if called again (idempotent)
        store
            .delete(&arena, &realize_types::Path::parse("foo.txt")?, &Options::default())
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn calculate_signature_and_diff_and_apply_delta() -> anyhow::Result<()> {
        let (store, temp, arena) = setup_store()?;
        let file_path = realize_types::Path::parse("foo.txt")?;
        let file_content = b"hello world, this is a test of rsync signature!";
        temp.child("foo.txt").write_binary(file_content)?;

        // Calculate signature for the whole file
        let sig = store
            .calculate_signature(
                &arena,
                &file_path.clone(),
                &ByteRange {
                    start: 0,
                    end: file_content.len() as u64,
                },
                &Options::default(),
            )
            .await?;

        // Change file content and write to a new file
        let new_content = b"hello world, this is A tost of rsync signature! (changed)";
        temp.child("foo.txt").write_binary(new_content)?;

        // Diff: get delta from old signature to new content
        let delta = store
            .diff(
                &arena,
                &file_path,
                &ByteRange {
                    start: 0,
                    end: new_content.len() as u64,
                },
                sig.clone(),
                &Options::default(),
            )
            .await?;
        assert!(!delta.0.0.is_empty());

        // Revert file to old content, then apply delta
        temp.child(".foo.txt.part").write_binary(file_content)?;
        let res = store
            .apply_delta(
                &arena,
                &file_path,
                &ByteRange {
                    start: 0,
                    end: new_content.len() as u64,
                },
                delta.0,
                &delta.1,
                &Options::default(),
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
        let (server, _temp, arena) = setup_store()?;
        // Nonexistent file
        let result = server
            .calculate_signature(
                &arena,
                &realize_types::Path::parse("notfound.txt")?,
                &ByteRange { start: 0, end: 10 },
                &Options::default(),
            )
            .await;
        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn apply_delta_error_case() -> anyhow::Result<()> {
        let (store, temp, arena) = setup_store()?;
        let file_path = realize_types::Path::parse("foo.txt")?;
        temp.child("foo.txt").write_str("abc")?;
        // Try to apply a bogus delta
        let result = store
            .apply_delta(
                &arena,
                &file_path,
                &ByteRange { start: 0, end: 3 },
                realize_types::Delta(vec![1, 2, 3]),
                &Hash::zero(),
                &Options::default(),
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
        let (store, temp, arena) = setup_store()?;
        temp.child("foo.txt").write_str("final")?;
        temp.child(".foo.txt.part").write_str("partial")?;
        // Default: partial preferred
        let files = store
            .list(
                &arena,
                &Options {
                    ignore_partial: false,
                },
            )
            .await?;
        assert_eq!(files.len(), 1);
        // With ignore_partial: final preferred
        let files = store
            .list(
                &arena,
                &Options {
                    ignore_partial: true,
                },
            )
            .await?;
        assert_eq!(files.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn list_ignore_partial() -> anyhow::Result<()> {
        let (store, temp, arena) = setup_store()?;
        temp.child(".foo.txt.part").write_str("partial")?;
        // Default: return partial
        let files = store
            .list(
                &arena,
                &Options {
                    ignore_partial: false,
                },
            )
            .await?;
        assert_eq!(files.len(), 1);
        // With ignore_partial: don't return partial
        let files = store
            .list(
                &arena,
                &Options {
                    ignore_partial: true,
                },
            )
            .await?;
        assert_eq!(files.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn read_partial_vs_final() -> anyhow::Result<()> {
        let (store, temp, arena) = setup_store()?;
        temp.child("foo.txt").write_str("final")?;
        temp.child(".foo.txt.part").write_str("partial")?;
        // Default: partial preferred
        let data = store
            .read(
                &arena,
                &realize_types::Path::parse("foo.txt")?,
                &ByteRange { start: 0, end: 7 },
                &Options {
                    ignore_partial: false,
                },
            )
            .await?;
        assert_eq!(String::from_utf8(data)?, "partial");
        // With ignore_partial: final preferred
        let data = store
            .read(
                &arena,
                &realize_types::Path::parse("foo.txt")?,
                &ByteRange { start: 0, end: 5 },
                &Options {
                    ignore_partial: true,
                },
            )
            .await?;
        assert_eq!(String::from_utf8(data)?, "final");
        Ok(())
    }

    #[tokio::test]
    async fn delete_removes_empty_parent_dirs() -> anyhow::Result<()> {
        let (store, temp, arena) = setup_store()?;
        // Create nested directories: a/b/c/file.txt
        let nested_dir = temp.child("a/b/c");
        std::fs::create_dir_all(nested_dir.path())?;
        let file_path = realize_types::Path::parse("a/b/c/file.txt")?;
        let logical = LogicalPath::new(temp.path(), &file_path);
        std::fs::write(logical.final_path(), b"data")?;
        // Delete the file
        store
            .delete(&arena, &file_path, &Options::default())
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
        let (store, temp, arena) = setup_store()?;
        // Create nested directories: a/b/c/file.txt and a/b/c/keep.txt
        let nested_dir = temp.child("a/b/c");
        std::fs::create_dir_all(nested_dir.path())?;
        let file_path = realize_types::Path::parse("a/b/c/file.txt")?;
        let keep_path = temp.child("a/b/c/keep.txt");
        let logical = LogicalPath::new(temp.path(), &file_path);
        std::fs::write(logical.final_path(), b"data")?;
        std::fs::write(keep_path.path(), b"keep")?;
        // Delete the file
        store
            .delete(&arena, &file_path, &Options::default())
            .await?;
        // Only the file should be deleted, parent dirs should remain
        assert!(!logical.final_path().exists());
        assert!(temp.child("a/b/c").exists());
        assert!(temp.child("a/b").exists());
        assert!(temp.child("a").exists());
        assert!(temp.child("a/b/c/keep.txt").exists());
        Ok(())
    }
}
