#![allow(dead_code)] // work in progress

use super::db::ArenaDatabase;
use super::dirty::WritableOpenDirty;
use super::history::{HistoryReadOperations, WritableOpenHistory};
use super::tree::{TreeExt, TreeLoc, TreeReadOperations, WritableOpenTree};
use super::types::{HistoryTableEntry, IndexedFile};
use crate::StorageError;
use crate::arena::blob::WritableOpenBlob;
use crate::arena::cache::{CacheExt, CacheReadOperations, WritableOpenCache};
use crate::utils::fs_utils;
use realize_types::{self, Hash, UnixTime};
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task;
use tokio_stream::wrappers::ReceiverStream;

/// Return the path for the given file if its current version matches `hash`.
///
/// If `hash` is none, the file must not exist. if `hash` is not none,
/// its version in the index must match and its size and modification
/// time must match those in the index.
pub(crate) fn indexed_file_path<'b, L: Into<TreeLoc<'b>>>(
    cache: &impl CacheReadOperations,
    tree: &impl TreeReadOperations,
    loc: L,
    hash: Option<&Hash>,
) -> Result<Option<std::path::PathBuf>, StorageError> {
    let loc = loc.into();
    let entry = cache.indexed(tree, loc.borrow())?;
    if let Some(path) = tree.backtrack(loc)? {
        let file_path = path.within(cache.datadir());
        match hash {
            Some(hash) => {
                if let Some(entry) = entry
                    && entry.version.matches_hash(hash)
                    && entry.matches_file(&file_path)
                {
                    return Ok(Some(file_path));
                }
            }
            None => {
                if entry.is_none()
                    && fs_utils::metadata_no_symlink_blocking(cache.datadir(), &path).is_err()
                {
                    return Ok(Some(file_path));
                }
            }
        }
    }
    Ok(None)
}

/// Create a hard link from `source` to `dest` if possible.
///
/// This function creates a hard link, possibly overwriting `dest`,
/// and returns `true` if the following conditions are met:
///
/// - `source` must exist and have hash `hash`
/// - `dest` must match `old_hash`, that is, if `old_hash` is None, it
///   must not exist and otherwise it must have hash `old_hash`
///
/// Otherwise, it does nothing and returns `false`.
pub(crate) fn branch<'b, L1: Into<TreeLoc<'b>>, L2: Into<TreeLoc<'b>>>(
    cache: &mut WritableOpenCache,
    tree: &mut WritableOpenTree,
    blobs: &mut WritableOpenBlob,
    history: &mut WritableOpenHistory,
    dirty: &mut WritableOpenDirty,
    source: L1,
    dest: L2,
    hash: &Hash,
) -> Result<bool, StorageError> {
    let source = source.into();
    let dest = dest.into();
    if cache.metadata(tree, dest.borrow())?.is_some() {
        return Err(StorageError::AlreadyExists);
    }
    let dest_path = match tree.backtrack(dest.borrow())? {
        Some(p) => p,
        None => return Ok(false),
    };
    let dest_realpath = dest_path.within(cache.datadir());
    let source_path = match tree.backtrack(source.borrow())? {
        Some(p) => p,
        None => return Ok(false),
    };
    let source_realpath = source_path.within(cache.datadir());
    if let Some(indexed) = cache.indexed(tree, source)?
        && indexed.version.matches_hash(hash)
        && indexed.matches_file(&source_realpath)
    {
        if let Some(parent) = dest_realpath.parent() {
            std::fs::create_dir_all(parent)?;
        }

        if dest_realpath.exists() {
            return Err(StorageError::AlreadyExists);
        }
        std::fs::hard_link(source_realpath, dest_realpath)?;

        cache.index(
            tree,
            blobs,
            history,
            dirty,
            dest,
            indexed.size,
            indexed.mtime,
            hash.clone(),
        )?;

        return Ok(true);
    }

    Ok(false)
}

/// Move a file from `source` to `dest` if possible.
///
/// This function moves a file, possibly overwriting `dest`, and
/// returns `true` if the following conditions are met:
///
/// - `source` must exist, and have hash `hash`
/// - `dest` must match `old_hash`, that is, if `old_hash` is None, it
///   must not exist and otherwise it must have hash `old_hash`
///
/// Otherwise, it does nothing and returns `false`.
pub(crate) fn rename<'b, 'c, L1: Into<TreeLoc<'b>>, L2: Into<TreeLoc<'c>>>(
    cache: &mut WritableOpenCache,
    tree: &mut WritableOpenTree,
    blobs: &mut WritableOpenBlob,
    history: &mut WritableOpenHistory,
    dirty: &mut WritableOpenDirty,
    source: L1,
    dest: L2,
    hash: &Hash,
) -> Result<bool, StorageError> {
    let source = source.into();
    let dest = dest.into();
    if cache.metadata(tree, dest.borrow())?.is_some() {
        return Err(StorageError::AlreadyExists);
    }
    let dest_path = match tree.backtrack(dest.borrow())? {
        Some(p) => p,
        None => return Ok(false),
    };
    let dest_realpath = dest_path.within(cache.datadir());
    let source_path = match tree.backtrack(source.borrow())? {
        Some(p) => p,
        None => return Ok(false),
    };
    let source_realpath = source_path.within(cache.datadir());
    if let Some(indexed) = cache.indexed(tree, source.borrow())?
        && indexed.version.matches_hash(hash)
        && indexed.matches_file(&source_realpath)
    {
        if let Some(parent) = dest_realpath.parent() {
            std::fs::create_dir_all(parent)?;
        }

        if dest_realpath.exists() {
            return Err(StorageError::AlreadyExists);
        }

        std::fs::rename(source_realpath, dest_realpath)?;

        // Add to index first, then remove. This order makes sure the
        // destination is added before the source is deleted, so it
        // won't be gone entirely from peers during the transition.
        cache.index(
            tree,
            blobs,
            history,
            dirty,
            dest,
            indexed.size,
            indexed.mtime,
            hash.clone(),
        )?;
        cache.remove_from_index(tree, blobs, history, dirty, source)?;

        return Ok(true);
    }

    Ok(false)
}

pub(crate) struct Index {
    datadir: PathBuf,
}
impl Index {
    pub(crate) fn new(datadir: &std::path::Path) -> Self {
        Self {
            datadir: datadir.to_path_buf(),
        }
    }

    pub(crate) fn datadir(&self) -> &std::path::Path {
        &self.datadir
    }
}

/// Get a file entry.
pub(crate) fn get_file(
    db: &Arc<ArenaDatabase>,
    path: &realize_types::Path,
) -> Result<Option<IndexedFile>, StorageError> {
    let txn = db.begin_read()?;
    let cache = txn.read_cache()?;
    let tree = txn.read_tree()?;

    cache.indexed(&tree, path)
}

/// Get a file entry
pub async fn get_file_async(
    db: &Arc<ArenaDatabase>,
    path: &realize_types::Path,
) -> Result<Option<IndexedFile>, StorageError> {
    let db = Arc::clone(db);
    let path = path.clone();

    task::spawn_blocking(move || get_file(&db, &path)).await?
}

/// Check whether a given file is in the index already.
pub(crate) fn has_file(
    db: &Arc<ArenaDatabase>,
    path: &realize_types::Path,
) -> Result<bool, StorageError> {
    let txn = db.begin_read()?;
    let cache = txn.read_cache()?;
    let tree = txn.read_tree()?;

    cache.has_local_file(&tree, path)
}

/// Check whether a given file is in the index already.
pub async fn has_file_async<T>(db: &Arc<ArenaDatabase>, path: T) -> Result<bool, StorageError>
where
    T: AsRef<realize_types::Path>,
{
    let path = path.as_ref();
    let db = Arc::clone(db);
    let path = path.clone();

    task::spawn_blocking(move || has_file(&db, &path)).await?
}

/// Check whether a given file is in the index with the given size and mtime.
pub(crate) fn has_matching_file(
    db: &Arc<ArenaDatabase>,
    path: &realize_types::Path,
    size: u64,
    mtime: UnixTime,
) -> Result<bool, StorageError> {
    let txn = db.begin_read()?;
    let cache = txn.read_cache()?;
    let tree = txn.read_tree()?;
    let ret = cache.indexed(&tree, path)?.map(|e| e.matches(size, mtime));

    Ok(ret.unwrap_or(false))
}

/// Check whether a given file is in the index already with the given size and mtime.
pub async fn has_matching_file_async(
    db: &Arc<ArenaDatabase>,
    path: &realize_types::Path,
    size: u64,
    mtime: UnixTime,
) -> Result<bool, StorageError> {
    let db = Arc::clone(db);
    let path = path.clone();

    task::spawn_blocking(move || has_matching_file(&db, &path, size, mtime)).await?
}

/// Mark the file as modified, but not yet indexed.
pub(crate) fn preindex(
    db: &Arc<ArenaDatabase>,
    path: &realize_types::Path,
) -> Result<(), StorageError> {
    let txn = db.begin_write()?;
    {
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut blobs = txn.write_blobs()?;

        cache.preindex(&mut tree, &mut blobs, &mut dirty, path)?;
    }

    txn.commit()?;

    Ok(())
}

pub async fn preindex_async(
    db: &Arc<ArenaDatabase>,
    path: &realize_types::Path,
) -> Result<(), StorageError> {
    let db = Arc::clone(db);
    let path = path.clone();

    task::spawn_blocking(move || preindex(&db, &path)).await?
}

/// Add a file entry with the given values. Replace one if it exists.
pub(crate) fn add_file(
    db: &Arc<ArenaDatabase>,
    path: &realize_types::Path,
    size: u64,
    mtime: UnixTime,
    hash: Hash,
) -> Result<(), StorageError> {
    let txn = db.begin_write()?;
    {
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut blobs = txn.write_blobs()?;
        let mut history = txn.write_history()?;

        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            path,
            size,
            mtime,
            hash,
        )?;
    }

    txn.commit()?;

    Ok(())
}

pub async fn add_file_async(
    db: &Arc<ArenaDatabase>,
    path: &realize_types::Path,
    size: u64,
    mtime: UnixTime,
    hash: Hash,
) -> Result<(), StorageError> {
    let db = Arc::clone(db);
    let path = path.clone();

    task::spawn_blocking(move || add_file(&db, &path, size, mtime, hash)).await?
}

/// Add a file entry if it matches the file on disk.
pub(crate) fn add_file_if_matches(
    db: &Arc<ArenaDatabase>,
    path: &realize_types::Path,
    size: u64,
    mtime: UnixTime,
    hash: Hash,
) -> Result<bool, StorageError> {
    match add_file(db, path, size, mtime, hash) {
        Ok(_) => Ok(true),
        Err(StorageError::LocalFileMismatch) => Ok(false),
        Err(err) => Err(err),
    }
}

pub async fn add_file_if_matches_async(
    db: &Arc<ArenaDatabase>,
    path: &realize_types::Path,
    size: u64,
    mtime: UnixTime,
    hash: Hash,
) -> Result<bool, StorageError> {
    let db = Arc::clone(db);
    let path = path.clone();

    task::spawn_blocking(move || add_file_if_matches(&db, &path, size, mtime, hash)).await?
}

/// Remove a file entry if the file is missing from disk.
pub(crate) fn remove_file_if_missing(
    db: &Arc<ArenaDatabase>,
    path: &realize_types::Path,
) -> Result<bool, StorageError> {
    let txn = db.begin_write()?;
    if fs_utils::metadata_no_symlink_blocking(db.index().datadir(), path).is_err() {
        {
            let mut tree = txn.write_tree()?;
            let mut blobs = txn.write_blobs()?;
            let mut cache = txn.write_cache()?;
            let mut dirty = txn.write_dirty()?;
            let mut history = txn.write_history()?;
            cache.remove_from_index(&mut tree, &mut blobs, &mut history, &mut dirty, path)?;
        }
        txn.commit()?;
        return Ok(true);
    }

    Ok(false)
}

pub async fn remove_file_if_missing_async(
    db: &Arc<ArenaDatabase>,
    path: &realize_types::Path,
) -> Result<bool, StorageError> {
    let db = Arc::clone(db);
    let path = path.clone();

    task::spawn_blocking(move || remove_file_if_missing(&db, &path)).await?
}

/// Send all valid entries of the file table to the given channel.
fn all_files(
    db: &Arc<ArenaDatabase>,
    tx: mpsc::Sender<(realize_types::Path, IndexedFile)>,
) -> Result<(), StorageError> {
    let txn = db.begin_read()?;
    let cache = txn.read_cache()?;
    let tree = txn.read_tree()?;
    for entry in cache.all_indexed() {
        let (pathid, indexed) = entry?;
        if let Some(path) = tree.backtrack(pathid)? {
            if let Err(_) = tx.blocking_send((path, indexed)) {
                break;
            }
        }
    }

    Ok(())
}

/// Return all valid file entries as a stream.
pub fn all_files_stream(
    db: &Arc<ArenaDatabase>,
) -> ReceiverStream<(realize_types::Path, IndexedFile)> {
    let (tx, rx) = mpsc::channel(100);

    let db = Arc::clone(db);
    task::spawn_blocking(move || all_files(&db, tx));

    ReceiverStream::new(rx)
}

/// Remove a path that can be a file or a directory.
///
/// If the path is a directory, all files within that directory
/// are removed, recursively.
pub(crate) fn remove_file_or_dir(
    db: &Arc<ArenaDatabase>,
    path: &realize_types::Path,
) -> Result<(), StorageError> {
    let txn = db.begin_write()?;
    {
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut cache = txn.write_cache()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;
        cache.remove_from_index_recursively(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            path,
        )?;
    }
    txn.commit()?;

    Ok(())
}

/// Grab a range of history entries.
pub fn history_stream(
    db: &Arc<ArenaDatabase>,
    range: impl RangeBounds<u64> + Send + 'static,
) -> ReceiverStream<Result<(u64, HistoryTableEntry), StorageError>> {
    let (tx, rx) = mpsc::channel(100);

    let db = Arc::clone(db);
    let range = Box::new(range);
    task::spawn_blocking(move || {
        let txn = match db.begin_read() {
            Ok(v) => v,
            Err(err) => {
                // Send any global error to the channel, so it ends up
                // in the stream instead of getting lost.
                let _ = tx.blocking_send(Err(err.into()));
                return;
            }
        };
        let history = match txn.read_history() {
            Ok(v) => v,
            Err(err) => {
                let _ = tx.blocking_send(Err(err));
                return;
            }
        };
        for res in history.history(*range) {
            if tx.blocking_send(res).is_err() {
                return;
            }
        }
    });

    ReceiverStream::new(rx)
}

/// Remove a path that can be a file or a directory.
///
/// If the path is a directory, all files within that directory
/// are removed, recursively.
pub async fn remove_file_or_dir_async<T>(
    db: &Arc<ArenaDatabase>,
    path: T,
) -> Result<(), StorageError>
where
    T: AsRef<realize_types::Path>,
{
    let path = path.as_ref();
    let db = Arc::clone(db);
    let path = path.clone();

    task::spawn_blocking(move || remove_file_or_dir(&db, &path)).await?
}

/// Index of the last history entry that was written.
pub(crate) fn last_history_index(db: &Arc<ArenaDatabase>) -> Result<u64, StorageError> {
    db.begin_read()?.read_history()?.last_history_index()
}

/// Index of the last history entry that was written.
pub(crate) async fn last_history_index_async(db: &Arc<ArenaDatabase>) -> Result<u64, StorageError> {
    let db = Arc::clone(db);

    task::spawn_blocking(move || last_history_index(&db)).await?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arena::db::ArenaDatabase;
    use crate::arena::types::Version;
    use crate::utils::hash;
    use assert_fs::TempDir;
    use assert_fs::fixture::ChildPath;
    use assert_fs::prelude::*;
    use realize_types::Arena;
    use realize_types::Path;
    use realize_types::Peer;
    use std::collections::HashMap;
    use std::fs;
    use std::os::unix::fs::MetadataExt;
    use std::sync::Arc;

    struct Fixture {
        db: Arc<ArenaDatabase>,
        datadir: ChildPath,
        _tempdir: TempDir,
    }

    impl Fixture {
        fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();
            let arena = Arena::from("myarena");
            let tempdir = TempDir::new()?;
            let blob_dir = tempdir.child("blobs");
            blob_dir.create_dir_all()?;
            let datadir = tempdir.child("data");
            datadir.create_dir_all()?;
            let db = ArenaDatabase::for_testing_single_arena(arena, blob_dir, datadir.path())?;

            Ok(Self {
                db,
                datadir,
                _tempdir: tempdir,
            })
        }

        fn setup_file(&self, path: &Path, content: &str) -> anyhow::Result<(u64, UnixTime, Hash)> {
            let childpath = self.datadir.child(path.as_str());
            childpath.write_str(content)?;
            let m = childpath.metadata()?;

            Ok((m.len(), UnixTime::mtime(&m), hash::digest(content)))
        }
    }

    fn collect_history_entries(
        history: &impl crate::arena::history::HistoryReadOperations,
    ) -> Result<Vec<HistoryTableEntry>, StorageError> {
        history
            .history(0..)
            .map(|res| res.map(|(_, e)| e))
            .collect()
    }

    #[test]
    fn index_add_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = realize_types::Path::parse("foo/bar.txt")?;
        let (size, mtime, hash) = fixture.setup_file(&path, "file content")?;
        super::add_file(&fixture.db, &path, size, mtime, hash.clone())?;

        // Verify the file was added
        assert!(super::has_file(&fixture.db, &path)?);
        let entry = super::get_file(&fixture.db, &path)?.unwrap();
        assert_eq!(entry.size, size);
        assert_eq!(entry.mtime, mtime);
        assert_eq!(entry.version, Version::Indexed(hash));

        Ok(())
    }

    #[test]
    fn index_add_file_if_matches_success() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let datadir = fixture.db.index().datadir();
        let foo = datadir.join("foo");
        fs::write(&foo, b"foo")?;

        assert!(super::add_file_if_matches(
            &fixture.db,
            &realize_types::Path::parse("foo")?,
            3,
            UnixTime::mtime(&foo.metadata()?),
            hash::digest("foo"),
        )?);
        assert!(super::has_file(
            &fixture.db,
            &realize_types::Path::parse("foo")?
        )?);

        Ok(())
    }

    #[test]
    fn index_add_file_if_matches_time_mismatch() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let datadir = fixture.db.index().datadir();
        let file_path = datadir.join("foo");
        fs::write(&file_path, b"foo")?;

        assert!(!super::add_file_if_matches(
            &fixture.db,
            &realize_types::Path::parse("foo")?,
            3,
            UnixTime::from_secs(1234567890),
            hash::digest("foo"),
        )?);
        assert!(!super::has_file(
            &fixture.db,
            &realize_types::Path::parse("foo")?
        )?);

        Ok(())
    }

    #[test]
    fn index_add_file_if_matches_size_mismatch() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let datadir = fixture.db.index().datadir();
        let file_path = datadir.join("foo");
        fs::write(&file_path, b"foo")?;

        assert!(!super::add_file_if_matches(
            &fixture.db,
            &realize_types::Path::parse("foo")?,
            2,
            UnixTime::mtime(&file_path.metadata()?),
            hash::digest("foo"),
        )?);
        assert!(!super::has_file(
            &fixture.db,
            &realize_types::Path::parse("foo")?
        )?);

        Ok(())
    }

    #[test]
    fn index_add_file_if_matches_missing() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let datadir = fixture.db.index().datadir();
        let file_path = datadir.join("foo");
        fs::write(&file_path, b"foo")?;
        let mtime = UnixTime::mtime(&file_path.metadata()?);
        std::fs::remove_file(&file_path)?;

        assert!(!super::add_file_if_matches(
            &fixture.db,
            &realize_types::Path::parse("foo")?,
            3,
            mtime,
            hash::digest("foo"),
        )?);
        assert!(!super::has_file(
            &fixture.db,
            &realize_types::Path::parse("foo")?
        )?);

        Ok(())
    }

    #[test]
    fn index_replace_file_in_index() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let path = realize_types::Path::parse("foo/bar.txt")?;

        // Add first version
        let content1 = "first version content";
        let (size1, mtime1, hash1) = fixture.setup_file(&path, content1)?;
        super::add_file(&fixture.db, &path, size1, mtime1, hash1)?;

        // Replace with second version
        let content2 = "second version content that is longer";
        let (size2, mtime2, hash2) = fixture.setup_file(&path, content2)?;
        super::add_file(&fixture.db, &path, size2, mtime2, hash2.clone())?;

        // Verify the file was replaced
        assert!(super::has_file(&fixture.db, &path)?);
        let entry = super::get_file(&fixture.db, &path)?.unwrap();
        assert_eq!(entry.size, size2);
        assert_eq!(entry.mtime, mtime2);
        assert_eq!(entry.version, Version::Indexed(hash2));

        Ok(())
    }

    #[test]
    fn index_has_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let path = realize_types::Path::parse("foo.txt")?;
        let content = "test content";
        let (size, mtime, hash) = fixture.setup_file(&path, content)?;
        super::add_file(&fixture.db, &path, size, mtime, hash)?;

        assert!(super::has_file(&fixture.db, &path)?);
        assert!(!super::has_file(
            &fixture.db,
            &realize_types::Path::parse("bar.txt")?
        )?);
        assert!(!super::has_file(
            &fixture.db,
            &realize_types::Path::parse("other.txt")?
        )?);

        Ok(())
    }

    #[test]
    fn index_get_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let path = realize_types::Path::parse("foo/bar")?;
        let content = "test content for foo/bar";
        let (size, mtime, hash) = fixture.setup_file(&path, content)?;
        super::add_file(&fixture.db, &path, size, mtime, hash.clone())?;

        let entry = super::get_file(&fixture.db, &path)?.unwrap();
        assert_eq!(entry.size, size);
        assert_eq!(entry.mtime, mtime);
        assert_eq!(entry.version, Version::Indexed(hash));

        Ok(())
    }

    #[test]
    fn index_has_matching_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let path = realize_types::Path::parse("foo/bar")?;
        let content = "test content";
        let (size, mtime, hash) = fixture.setup_file(&path, content)?;
        super::add_file(&fixture.db, &path, size, mtime, hash)?;

        assert!(super::has_matching_file(&fixture.db, &path, size, mtime)?);
        assert!(!super::has_matching_file(
            &fixture.db,
            &realize_types::Path::parse("other")?,
            size,
            mtime
        )?);
        assert!(!super::has_matching_file(
            &fixture.db,
            &path,
            size + 100,
            mtime
        )?);
        assert!(!super::has_matching_file(
            &fixture.db,
            &path,
            size,
            UnixTime::from_secs(1234567891)
        )?);

        Ok(())
    }

    #[test]
    fn index_remove_file_or_dir() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let path = realize_types::Path::parse("foo/bar.txt")?;
        let content = "test content to remove";
        let (size, mtime, hash) = fixture.setup_file(&path, content)?;
        super::add_file(&fixture.db, &path, size, mtime, hash)?;
        super::remove_file_or_dir(&fixture.db, &path)?;

        assert!(!super::has_file(&fixture.db, &path)?);

        Ok(())
    }

    #[test]
    fn index_remove_file_if_missing_success() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let path = realize_types::Path::parse("bar.txt")?;
        let content = "test content that will be missing";
        let (size, mtime, hash) = fixture.setup_file(&path, content)?;
        super::add_file(&fixture.db, &path, size, mtime, hash)?;

        // Remove the actual file to simulate it being missing
        std::fs::remove_file(fixture.datadir.child("bar.txt").path())?;

        assert!(super::remove_file_if_missing(&fixture.db, &path)?);
        assert!(!super::has_file(&fixture.db, &path)?);

        Ok(())
    }

    #[test]
    fn index_remove_file_if_missing_failure() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let path = realize_types::Path::parse("bar.txt")?;
        let content = "content";
        let (size, mtime, hash) = fixture.setup_file(&path, content)?;
        super::add_file(&fixture.db, &path, size, mtime, hash)?;

        // File still exists on disk (created by setup_file), so removal should fail
        assert!(!super::remove_file_if_missing(&fixture.db, &path)?);
        assert!(super::has_file(&fixture.db, &path)?);

        Ok(())
    }

    #[test]
    fn index_remove_dir_from_index() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let path_a = realize_types::Path::parse("foo/a")?;
        let path_b = realize_types::Path::parse("foo/b")?;
        let path_c = realize_types::Path::parse("foo/c")?;
        let path_foobar = realize_types::Path::parse("foobar")?;

        let (size_a, mtime_a, hash_a) = fixture.setup_file(&path_a, "content a")?;
        let (size_b, mtime_b, hash_b) = fixture.setup_file(&path_b, "content b")?;
        let (size_c, mtime_c, hash_c) = fixture.setup_file(&path_c, "content c")?;
        let (size_foobar, mtime_foobar, hash_foobar) =
            fixture.setup_file(&path_foobar, "content foobar")?;

        super::add_file(&fixture.db, &path_a, size_a, mtime_a, hash_a)?;
        super::add_file(&fixture.db, &path_b, size_b, mtime_b, hash_b)?;
        super::add_file(&fixture.db, &path_c, size_c, mtime_c, hash_c)?;
        super::add_file(
            &fixture.db,
            &path_foobar,
            size_foobar,
            mtime_foobar,
            hash_foobar,
        )?;

        // Remove the directory
        super::remove_file_or_dir(&fixture.db, &realize_types::Path::parse("foo")?)?;

        // Verify all files in the directory were removed
        assert!(!super::has_file(&fixture.db, &path_a)?);
        assert!(!super::has_file(&fixture.db, &path_b)?);
        assert!(!super::has_file(&fixture.db, &path_c)?);

        // But the file outside the directory should remain
        assert!(super::has_file(&fixture.db, &path_foobar)?);

        Ok(())
    }

    #[tokio::test]
    async fn index_all_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let path1 = realize_types::Path::parse("foo/a")?;
        let path2 = realize_types::Path::parse("foo/b")?;
        let path3 = realize_types::Path::parse("bar.txt")?;

        let (size1, mtime1, hash1) = fixture.setup_file(&path1, "content for a")?;
        let (size2, mtime2, hash2) = fixture.setup_file(&path2, "content for b that is longer")?;
        let (size3, mtime3, hash3) = fixture.setup_file(&path3, "content for bar.txt file")?;

        super::add_file(&fixture.db, &path1, size1, mtime1, hash1)?;
        super::add_file(&fixture.db, &path2, size2, mtime2, hash2)?;
        super::add_file(&fixture.db, &path3, size3, mtime3, hash3)?;

        let (tx, mut rx) = mpsc::channel(10);
        let task = tokio::task::spawn_blocking({
            let db = fixture.db.clone();

            move || super::all_files(&db, tx)
        });

        let mut files = HashMap::new();
        while let Some((path, entry)) = rx.recv().await {
            files.insert(path, entry);
        }
        task.await??; // make sure there are no errors

        assert_eq!(files.len(), 3);
        assert!(files.contains_key(&path1));
        assert!(files.contains_key(&path2));
        assert!(files.contains_key(&path3));

        Ok(())
    }

    #[tokio::test]
    async fn index_watch_history() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let mut rx = fixture.db.history().watch();
        let initial = *rx.borrow();

        let path = realize_types::Path::parse("foo/bar.txt")?;
        let content = "test content for watch history";
        let (size, mtime, hash) = fixture.setup_file(&path, content)?;
        super::add_file(&fixture.db, &path, size, mtime, hash)?;

        // Wait for the history to be updated
        let timeout = tokio::time::Duration::from_millis(100);
        let _ = tokio::time::timeout(timeout, rx.changed()).await;

        let updated = *rx.borrow();
        assert!(updated > initial);

        Ok(())
    }

    #[test]
    fn index_last_history_index() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let initial = super::last_history_index(&fixture.db)?;

        let path = realize_types::Path::parse("foo/bar.txt")?;
        let content = "test content for history";
        let (size, mtime, hash) = fixture.setup_file(&path, content)?;
        super::add_file(&fixture.db, &path, size, mtime, hash)?;

        let updated = super::last_history_index(&fixture.db)?;
        assert!(updated > initial);

        Ok(())
    }

    #[test]
    fn branch_succeeds() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let datadir = fixture.db.index().datadir();

        // Create source file on disk
        let source_path = datadir.join("source.txt");
        let source_content = "source_content";
        std::fs::write(&source_path, source_content)?;
        let source_mtime = UnixTime::mtime(&source_path.metadata()?);
        let source_hash = hash::digest(source_content);

        // Add source file to index
        let source_index_path = Path::parse("source.txt")?;
        super::add_file(
            &fixture.db,
            &source_index_path,
            source_content.len() as u64, // size of "source content"
            source_mtime,
            source_hash.clone(),
        )?;

        // Destination does not exist yet (neither in filesystem nor in cache)
        let dest_index_path = Path::parse("dest.txt")?;
        assert!(!super::has_file(&fixture.db, &dest_index_path)?);

        // Test successful branch (create hard link)
        let txn = fixture.db.begin_write()?;
        let result = {
            let mut cache = txn.write_cache()?;
            let mut tree = txn.write_tree()?;
            let mut blobs = txn.write_blobs()?;
            let mut dirty = txn.write_dirty()?;
            let mut history = txn.write_history()?;

            super::branch(
                &mut cache,
                &mut tree,
                &mut blobs,
                &mut history,
                &mut dirty,
                &source_index_path,
                &dest_index_path,
                &source_hash,
            )
        }?;

        assert!(
            result,
            "Branch should succeed when destination doesn't exist in cache"
        );

        // Commit the transaction and verify cache state
        txn.commit()?;

        // Verify destination now exists in cache after commit
        assert!(
            super::has_file(&fixture.db, &dest_index_path)?,
            "Destination should be in cache after branch"
        );

        let dest_entry = super::get_file(&fixture.db, &dest_index_path)?.unwrap();
        assert_eq!(dest_entry.version, Version::Indexed(source_hash));
        assert_eq!(dest_entry.size, source_content.len() as u64);
        assert_eq!(dest_entry.mtime, source_mtime);

        // Verify hard link was created by checking pathid numbers
        let dest_path = datadir.join("dest.txt");
        let source_metadata = std::fs::metadata(source_path)?;
        let dest_metadata = std::fs::metadata(dest_path)?;
        assert_eq!(
            source_metadata.ino(),
            dest_metadata.ino(),
            "Files should have same pathid after hard link"
        );

        Ok(())
    }

    #[test]
    fn branch_fails_if_destination_already_exists() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let datadir = fixture.db.index().datadir();

        // Create source file on disk
        let source_path = datadir.join("source.txt");
        std::fs::write(&source_path, "source content")?;
        let source_mtime = UnixTime::mtime(&source_path.metadata()?);
        let source_hash = hash::digest("source content");

        // Create destination file on disk with different content
        let dest_path = datadir.join("dest.txt");
        std::fs::write(&dest_path, "dest content")?;
        let dest_mtime = UnixTime::mtime(&dest_path.metadata()?);
        let dest_hash = hash::digest("dest content");

        // Add source file to index
        let source_index_path = Path::parse("source.txt")?;
        super::add_file(
            &fixture.db,
            &source_index_path,
            "source content".len() as u64,
            source_mtime,
            source_hash.clone(),
        )?;

        // Add destination file to index
        let dest_index_path = Path::parse("dest.txt")?;
        super::add_file(
            &fixture.db,
            &dest_index_path,
            "dest content".len() as u64,
            dest_mtime,
            dest_hash.clone(),
        )?;

        // Verify both files are in the index
        assert!(super::has_file(&fixture.db, &source_index_path)?);
        assert!(super::has_file(&fixture.db, &dest_index_path)?);

        // Test branch fails when destination exists in cache
        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        let result = super::branch(
            &mut cache,
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &source_index_path,
            &dest_index_path,
            &source_hash,
        );

        // Assert that it returns AlreadyExists error
        assert!(
            matches!(result, Err(StorageError::AlreadyExists)),
            "Branch should fail with AlreadyExists when destination exists in cache"
        );

        Ok(())
    }

    #[test]
    fn branch_fails_if_source_hash_doesnt_match() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let datadir = fixture.db.index().datadir();

        // Create source file on disk
        let source_path = datadir.join("source.txt");
        std::fs::write(&source_path, "source content")?;
        let source_mtime = UnixTime::mtime(&source_path.metadata()?);
        let source_hash = hash::digest("source content");

        // Add source file to index
        let source_index_path = Path::parse("source.txt")?;
        super::add_file(
            &fixture.db,
            &source_index_path,
            "source content".len() as u64,
            source_mtime,
            source_hash.clone(),
        )?;

        // Test branch fails when destination exists in cache
        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        assert_eq!(
            false,
            super::branch(
                &mut cache,
                &mut tree,
                &mut blobs,
                &mut history,
                &mut dirty,
                &source_index_path,
                &Path::parse("dest.txt")?,
                &hash::digest("some other hash"),
            )
            .unwrap()
        );

        Ok(())
    }

    #[test]
    fn branch_fails_if_source_not_found() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        // Test branch fails when destination exists in cache
        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        assert_eq!(
            false,
            super::branch(
                &mut cache,
                &mut tree,
                &mut blobs,
                &mut history,
                &mut dirty,
                &Path::parse("doesnotexist")?,
                &Path::parse("dest")?,
                &hash::digest("some hash"),
            )
            .unwrap()
        );

        Ok(())
    }

    #[test]
    fn branch_fails_if_destination_is_a_directory() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let datadir = fixture.db.index().datadir();

        // Create source file on disk
        let source_path = datadir.join("source.txt");
        std::fs::write(&source_path, "source content")?;
        let source_mtime = UnixTime::mtime(&source_path.metadata()?);
        let source_hash = hash::digest("source content");

        // Add source file to index
        let source_index_path = Path::parse("source.txt")?;
        super::add_file(
            &fixture.db,
            &source_index_path,
            "source content".len() as u64,
            source_mtime,
            source_hash.clone(),
        )?;

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        cache.mkdir(&mut tree, Path::parse("dir")?)?;

        let result = super::branch(
            &mut cache,
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &source_index_path,
            &Path::parse("dir")?,
            &source_hash,
        );

        assert!(matches!(result, Err(StorageError::AlreadyExists)),);

        Ok(())
    }

    #[test]
    fn branch_fails_if_destination_is_in_cache() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let datadir = fixture.db.index().datadir();

        // Create source file on disk
        let source_path = datadir.join("source.txt");
        std::fs::write(&source_path, "source content")?;
        let source_mtime = UnixTime::mtime(&source_path.metadata()?);
        let source_hash = hash::digest("source content");

        // Add source file to index
        let source_index_path = Path::parse("source.txt")?;
        super::add_file(
            &fixture.db,
            &source_index_path,
            "source content".len() as u64,
            source_mtime,
            source_hash.clone(),
        )?;

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        cache.notify_added(
            &mut tree,
            &mut blobs,
            &mut dirty,
            Peer::from("peer"),
            Path::parse("dest.txt")?,
            UnixTime::from_secs(1234567890),
            10,
            Hash([1u8; 32]),
        )?;

        let result = super::branch(
            &mut cache,
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &source_index_path,
            &Path::parse("dest.txt")?,
            &source_hash,
        );

        assert!(matches!(result, Err(StorageError::AlreadyExists)));

        Ok(())
    }

    #[test]
    fn branch_fails_if_destination_file_exists_but_not_yet_indexed() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let datadir = fixture.db.index().datadir();

        // Create source file on disk and in cache
        let source_path = datadir.join("source.txt");
        std::fs::write(&source_path, "source content")?;
        let source_mtime = UnixTime::mtime(&source_path.metadata()?);
        let source_hash = hash::digest("source content");

        let source_index_path = Path::parse("source.txt")?;
        super::add_file(
            &fixture.db,
            &source_index_path,
            14,
            source_mtime,
            source_hash.clone(),
        )?;

        // Create destination file on disk BUT NOT in cache
        let dest_path = datadir.join("dest.txt");
        std::fs::write(&dest_path, "dest content")?;
        let dest_index_path = Path::parse("dest.txt")?;

        // Verify dest is not in cache
        assert!(!super::has_file(&fixture.db, &dest_index_path)?);

        // Test branch should fail
        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        assert!(matches!(
            super::branch(
                &mut cache,
                &mut tree,
                &mut blobs,
                &mut history,
                &mut dirty,
                &source_index_path,
                &dest_index_path,
                &source_hash,
            ),
            Err(StorageError::AlreadyExists)
        ));

        Ok(())
    }

    #[test]
    fn rename_succeeds() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let datadir = fixture.db.index().datadir();

        // Create source file on disk
        let source_realpath = datadir.join("source.txt");
        std::fs::write(&source_realpath, "source content")?;
        let source_mtime = UnixTime::mtime(&source_realpath.metadata()?);
        let source_hash = hash::digest("source content");

        // Add source file to index
        let source_path = Path::parse("source.txt")?;
        super::add_file(
            &fixture.db,
            &source_path,
            "source content".len() as u64,
            source_mtime,
            source_hash.clone(),
        )?;

        let dest_realpath = datadir.join("dest.txt");
        let dest_path = Path::parse("dest.txt")?;

        // Test successful rename
        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        assert_eq!(
            true,
            super::rename(
                &mut cache,
                &mut tree,
                &mut blobs,
                &mut history,
                &mut dirty,
                &source_path,
                &dest_path,
                &source_hash,
            )
            .unwrap()
        );

        assert!(dest_realpath.exists());
        assert!(!source_realpath.exists());
        assert_eq!("source content", std::fs::read_to_string(&dest_realpath)?);

        assert!(cache.metadata(&tree, &source_path)?.is_none());
        assert_eq!(
            Some(IndexedFile {
                mtime: source_mtime,
                version: Version::Indexed(source_hash),
                size: "source content".len() as u64
            }),
            cache.indexed(&tree, &dest_path)?
        );

        Ok(())
    }

    #[test]
    fn rename_fails_when_destination_exists() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let datadir = fixture.db.index().datadir();

        // Create source file on disk
        let source_path = datadir.join("source.txt");
        std::fs::write(&source_path, "source content")?;
        let source_mtime = UnixTime::mtime(&source_path.metadata()?);
        let source_hash = hash::digest("source content");

        // Create destination file on disk with different content
        let dest_path = datadir.join("dest.txt");
        std::fs::write(&dest_path, "dest content")?;
        let dest_mtime = UnixTime::mtime(&dest_path.metadata()?);
        let dest_hash = hash::digest("dest content");

        // Add source file to index
        let source_index_path = Path::parse("source.txt")?;
        super::add_file(
            &fixture.db,
            &source_index_path,
            "source content".len() as u64,
            source_mtime,
            source_hash.clone(),
        )?;

        // Add destination file to index
        let dest_index_path = Path::parse("dest.txt")?;
        super::add_file(
            &fixture.db,
            &dest_index_path,
            "dest content".len() as u64,
            dest_mtime,
            dest_hash.clone(),
        )?;

        // Verify both files are in the index
        assert!(super::has_file(&fixture.db, &source_index_path)?);
        assert!(super::has_file(&fixture.db, &dest_index_path)?);

        // Test rename fails when destination exists in cache
        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        let result = super::rename(
            &mut cache,
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &source_index_path,
            &dest_index_path,
            &source_hash,
        );

        assert!(
            matches!(result, Err(StorageError::AlreadyExists)),
            "Rename should fail with AlreadyExists when destination exists in cache"
        );

        // Verify original files are preserved
        assert!(source_path.exists(), "Source file should still exist");
        assert_eq!(
            "dest content",
            std::fs::read_to_string(&dest_path)?,
            "Destination file should be unchanged"
        );

        Ok(())
    }

    #[test]
    fn rename_fails_when_destination_exists_on_disk_but_not_yet_indexed() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let datadir = fixture.db.index().datadir();

        // Create source file on disk and in cache
        let source_path = datadir.join("source.txt");
        std::fs::write(&source_path, "source content")?;
        let source_mtime = UnixTime::mtime(&source_path.metadata()?);
        let source_hash = hash::digest("source content");

        let source_index_path = Path::parse("source.txt")?;
        super::add_file(
            &fixture.db,
            &source_index_path,
            "source content".len() as u64,
            source_mtime,
            source_hash.clone(),
        )?;

        // Create destination file on disk BUT NOT in cache
        let dest_path = datadir.join("dest.txt");
        std::fs::write(&dest_path, "dest content")?;
        let dest_index_path = Path::parse("dest.txt")?;

        // Verify dest is not in cache
        assert!(!super::has_file(&fixture.db, &dest_index_path)?);

        // Test rename should succeed (dest exists on disk but not in cache)
        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        assert!(matches!(
            super::rename(
                &mut cache,
                &mut tree,
                &mut blobs,
                &mut history,
                &mut dirty,
                &source_index_path,
                &dest_index_path,
                &source_hash,
            ),
            Err(StorageError::AlreadyExists)
        ));

        Ok(())
    }

    #[test]
    fn rename_rejects_source() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let datadir = fixture.db.index().datadir();

        // Create source file on disk
        let source_path = datadir.join("source.txt");
        std::fs::write(&source_path, "source content")?;
        let source_mtime = UnixTime::mtime(&source_path.metadata()?);
        let source_hash = hash::digest("source content");

        // Add source file to index
        let source_index_path = Path::parse("source.txt")?;
        super::add_file(
            &fixture.db,
            &source_index_path,
            "source content".len() as u64,
            source_mtime,
            source_hash.clone(),
        )?;

        // Create a destination path that isn't in cache
        let dest_index_path = Path::parse("dest.txt")?;

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut history = txn.write_history()?;
        let mut dirty = txn.write_dirty()?;

        // Test rename failure when source hash doesn't match
        let wrong_hash = Hash([0x99; 32]);
        let result = super::rename(
            &mut cache,
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &source_index_path,
            &dest_index_path,
            &wrong_hash,
        )?;

        assert!(!result, "Rename should fail when source hash doesn't match");

        // Test rename failure when source doesn't exist in tree
        let nonexistent_path = Path::parse("nonexistent.txt")?;
        let result = super::rename(
            &mut cache,
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &nonexistent_path,
            &dest_index_path,
            &source_hash,
        )?;

        assert!(
            !result,
            "Rename should fail when source doesn't exist in tree"
        );

        Ok(())
    }
}
