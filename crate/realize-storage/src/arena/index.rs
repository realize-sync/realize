#![allow(dead_code)] // work in progress

use super::db::ArenaDatabase;
use super::dirty::WritableOpenDirty;
use super::history::{HistoryReadOperations, WritableOpenHistory};
use super::tree::{TreeExt, TreeLoc, TreeReadOperations, WritableOpenTree};
use super::types::{HistoryTableEntry, IndexedFile};
use crate::arena::cache::{CacheReadOperations, WritableOpenCache};
use crate::utils::fs_utils;
use crate::{PathId, StorageError};
use realize_types::{self, Hash, Path, UnixTime};
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
                    && entry.hash == *hash
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
    cache: &impl CacheReadOperations,
    tree: &impl TreeReadOperations,
    source: L1,
    dest: L2,
    hash: &Hash,
    old_hash: Option<&Hash>,
) -> Result<bool, StorageError> {
    let source = match tree.backtrack(source)? {
        Some(p) => p,
        None => return Ok(false),
    };
    let source_realpath = source.within(cache.datadir());
    if let Some(indexed) = cache.indexed(tree, source)?
        && indexed.hash == *hash
        && indexed.matches_file(&source_realpath)
    {
        if let Some(dest_realpath) = indexed_file_path(cache, tree, dest, old_hash)? {
            if let Some(parent) = dest_realpath.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let _ = std::fs::remove_file(&dest_realpath);
            std::fs::hard_link(source_realpath, dest_realpath)?;
            return Ok(true);
        }
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
    history: &mut WritableOpenHistory,
    dirty: &mut WritableOpenDirty,
    source: L1,
    dest: L2,
    hash: &Hash,
    old_hash: Option<&Hash>,
) -> Result<bool, StorageError> {
    let root = cache.datadir();
    let source = source.into();
    let dest = dest.into();
    let source_path = match tree.backtrack(source.borrow())? {
        Some(p) => p,
        None => return Ok(false),
    };
    let source_realpath = source_path.within(root);
    if let Some(indexed) = cache.indexed(tree, source.borrow())?
        && indexed.hash == *hash
        && indexed.matches_file(&source_realpath)
    {
        if let Some(dest_realpath) = indexed_file_path(cache, tree, dest.borrow(), old_hash)? {
            if let Some(parent) = dest_realpath.parent() {
                std::fs::create_dir_all(parent)?;
            }
            // This makes sure the destination is added before the
            // source is deleted, so it won't be gone entirely from
            // peers during the transition.
            cache.index(
                tree,
                history,
                dirty,
                dest,
                indexed.size,
                indexed.mtime,
                hash.clone(),
            )?;
            cache.remove_from_index(tree, history, dirty, source)?;
            std::fs::rename(source_realpath, dest_realpath)?;
            return Ok(true);
        }
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

/// Read operations for index. See also [IndexExt].
pub(crate) trait IndexReadOperations {
    /// Get a file entry by path.
    fn indexed<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<Option<IndexedFile>, StorageError>;

    fn is_indexed<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<bool, StorageError>;
}

impl<T: CacheReadOperations> IndexReadOperations for T {
    fn indexed<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<Option<IndexedFile>, StorageError> {
        if let Some(pathid) = tree.resolve(loc)? {
            self.index_entry_at_pathid(pathid)
        } else {
            Ok(None)
        }
    }
    fn is_indexed<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<bool, StorageError> {
        if let Some(pathid) = tree.resolve(loc)? {
            return Ok(self.index_entry_at_pathid(pathid)?.is_some());
        }

        Ok(false)
    }
}

pub(crate) trait IndexWriteOperations {
    fn index<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        history: &mut WritableOpenHistory,
        dirty: &mut WritableOpenDirty,
        loc: L,
        size: u64,
        mtime: UnixTime,
        hash: Hash,
    ) -> Result<PathId, StorageError>;

    fn index_silently<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        loc: L,
        size: u64,
        mtime: UnixTime,
        hash: Hash,
    ) -> Result<PathId, StorageError>;

    /// Remove a file entry at the given location, if it exists.
    ///
    /// Return true if something was removed.
    fn remove_from_index<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        history: &mut WritableOpenHistory,
        dirty: &mut WritableOpenDirty,
        loc: L,
    ) -> Result<bool, StorageError>;

    /// Remove a file entry at the given location, if it exists and
    /// report it as dropped.
    ///
    /// Return true if something was removed.
    fn drop_from_index<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        history: &mut WritableOpenHistory,
        dirty: &mut WritableOpenDirty,
        loc: L,
    ) -> Result<bool, StorageError>;

    /// Remove a tree location (path or pathid) that can be a file or a
    /// directory.
    ///
    /// If the location is a directory, all files within that
    /// directory are removed, recursively.
    fn remove_from_index_recursively<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        history: &mut WritableOpenHistory,
        dirty: &mut WritableOpenDirty,
        loc: L,
    ) -> Result<(), StorageError>;

    /// Record that the given file and version has been outdated by
    /// `new_hash`.
    ///
    /// Does nothing if the file is missing or if its version is not
    /// `old_hash` or a version derived from it.
    fn record_outdated<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        dirty: &mut WritableOpenDirty,
        loc: L,
        old_hash: &Hash,
        new_hash: &Hash,
    ) -> Result<(), StorageError>;
}

impl<'a> IndexWriteOperations for WritableOpenCache<'a> {
    fn index<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        history: &mut WritableOpenHistory,
        dirty: &mut WritableOpenDirty,
        loc: L,
        size: u64,
        mtime: UnixTime,
        hash: Hash,
    ) -> Result<PathId, StorageError> {
        self.add_internal(tree, Some(history), Some(dirty), loc, size, mtime, hash)
    }

    fn index_silently<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        loc: L,
        size: u64,
        mtime: UnixTime,
        hash: Hash,
    ) -> Result<PathId, StorageError> {
        self.add_internal(tree, None, None, loc, size, mtime, hash)
    }

    /// Remove a file entry at the given location, if it exists.
    ///
    /// Return true if something was removed.
    fn remove_from_index<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        history: &mut WritableOpenHistory,
        dirty: &mut WritableOpenDirty,
        loc: L,
    ) -> Result<bool, StorageError> {
        let loc = loc.into();
        if let Some(pathid) = tree.resolve(loc.borrow())? {
            if let Some(old_hash) = self.indexed_hash(pathid) {
                if let Some(path) = tree.backtrack(loc)? {
                    history.report_removed(&path, &old_hash)?;
                }
                self.remove_index_entry(tree, pathid)?;
                dirty.mark_dirty(pathid, "removed_from_index")?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Remove a file entry at the given location, if it exists and
    /// report it as dropped.
    ///
    /// Return true if something was removed.
    fn drop_from_index<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        history: &mut WritableOpenHistory,
        dirty: &mut WritableOpenDirty,
        loc: L,
    ) -> Result<bool, StorageError> {
        let loc = loc.into();
        if let Some(pathid) = tree.resolve(loc.borrow())? {
            if let Some(old_hash) = self.indexed_hash(pathid) {
                if let Some(path) = tree.backtrack(loc)? {
                    history.report_dropped(&path, &old_hash)?;
                }
                self.remove_index_entry(tree, pathid)?;
                dirty.mark_dirty(pathid, "dropped_from_index")?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Remove a tree location (path or pathid) that can be a file or a
    /// directory.
    ///
    /// If the location is a directory, all files within that
    /// directory are removed, recursively.
    fn remove_from_index_recursively<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        history: &mut WritableOpenHistory,
        dirty: &mut WritableOpenDirty,
        loc: L,
    ) -> Result<(), StorageError> {
        let pathid = match tree.resolve(loc)? {
            Some(p) => p,
            None => return Ok(()),
        };
        if self.remove_from_index(tree, history, dirty, pathid)? {
            log::debug!("removed {pathid:?}");
        }
        let children = tree.readdir_pathid(pathid).collect::<Result<Vec<_>, _>>()?;
        log::debug!("children: {children:?}");
        for (_, pathid) in children.into_iter() {
            self.remove_from_index_recursively(tree, history, dirty, pathid)?;
        }
        Ok(())
    }

    /// Record that the given file and version has been outdated by
    /// `new_hash`.
    ///
    /// Does nothing if the file is missing or if its version is not
    /// `old_hash` or a version derived from it.
    fn record_outdated<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        dirty: &mut WritableOpenDirty,
        loc: L,
        old_hash: &Hash,
        new_hash: &Hash,
    ) -> Result<(), StorageError> {
        if let Some(pathid) = tree.resolve(loc)? {
            if let Some(mut entry) = self.index_entry_at_pathid(pathid)?
                && entry.is_outdated_by(old_hash)
            {
                // Just remember that a newer version exist in a
                // remote peer. This information is going to be used
                // to download that newer version later on.
                entry.outdated_by = Some(new_hash.clone());
                log::debug!(
                    "[{}] outdated: {} {old_hash}, new version: {new_hash})",
                    tree.arena(),
                    pathid
                );

                self.add_index_entry(tree, pathid, entry)?;
                dirty.mark_dirty(pathid, "outdated")?;
            }
        }

        Ok(())
    }
}

impl<'a> WritableOpenCache<'a> {
    fn add_internal<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        history: Option<&mut WritableOpenHistory>,
        dirty: Option<&mut WritableOpenDirty>,
        loc: L,
        size: u64,
        mtime: UnixTime,
        hash: Hash,
    ) -> Result<PathId, StorageError> {
        let loc = loc.into();
        let pathid = tree.setup(loc.borrow())?;
        let old_hash = self.indexed_hash(pathid);
        if hash.matches(old_hash.as_ref()) {
            return Ok(pathid);
        }
        if let Some(path) = tree.backtrack(loc.borrow())? {
            self.add_index_entry(
                tree,
                pathid,
                IndexedFile {
                    hash,
                    mtime,
                    size,
                    outdated_by: None,
                },
            )?;
            if let Some(dirty) = dirty {
                dirty.mark_dirty(pathid, "indexed")?;
            }
            if let Some(history) = history {
                history.report_added(&path, old_hash.as_ref())?;
            }
        }
        Ok(pathid)
    }

    /// Hash of the indexed file or None.
    fn indexed_hash(&mut self, pathid: PathId) -> Option<Hash> {
        if let Ok(Some(e)) = self.index_entry_at_pathid(pathid) {
            return Some(e.hash);
        }

        None
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

    cache.is_indexed(&tree, path)
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
    let mtime = mtime.clone();

    task::spawn_blocking(move || has_matching_file(&db, &path, size, mtime)).await?
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
        let mut history = txn.write_history()?;

        cache.index(&mut tree, &mut history, &mut dirty, path, size, mtime, hash)?;
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
    let mtime = mtime.clone();

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
    if let Ok(m) = path.within(db.index().datadir()).metadata()
        && m.len() == size
        && UnixTime::mtime(&m) == mtime
    {
        let txn = db.begin_write()?;
        {
            let mut cache = txn.write_cache()?;
            let mut tree = txn.write_tree()?;
            let mut dirty = txn.write_dirty()?;
            let mut history = txn.write_history()?;
            cache.index(&mut tree, &mut history, &mut dirty, path, size, mtime, hash)?;
        }
        txn.commit()?;

        return Ok(true);
    }

    Ok(false)
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
    let mtime = mtime.clone();

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
            let mut cache = txn.write_cache()?;
            let mut dirty = txn.write_dirty()?;
            let mut history = txn.write_history()?;
            cache.remove_from_index(&mut tree, &mut history, &mut dirty, path)?;
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
    for entry in cache.all_indexed() {
        let (path, indexed) = entry?;
        if let Err(_) = tx.blocking_send((path, indexed)) {
            break;
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
        let mut cache = txn.write_cache()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;
        cache.remove_from_index_recursively(&mut tree, &mut history, &mut dirty, path)?;
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
    use crate::arena::dirty::DirtyReadOperations;
    use crate::utils::hash;
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use realize_types::Arena;
    use std::collections::{HashMap, HashSet};
    use std::fs;
    use std::os::unix::fs::MetadataExt;
    use std::sync::Arc;

    struct Fixture {
        db: Arc<ArenaDatabase>,
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
            let db = ArenaDatabase::for_testing_single_arena(arena, blob_dir, datadir)?;

            Ok(Self {
                db,
                _tempdir: tempdir,
            })
        }
    }

    fn dirty_pathids(
        dirty: &impl crate::arena::dirty::DirtyReadOperations,
    ) -> Result<HashSet<PathId>, StorageError> {
        let mut start = 0;
        let mut ret = HashSet::new();
        while let Some((pathid, counter)) = dirty.next_dirty(start)? {
            ret.insert(pathid);
            start = counter + 1;
        }
        Ok(ret)
    }

    fn dirty_paths(
        dirty: &impl DirtyReadOperations,
        tree: &impl TreeReadOperations,
    ) -> Result<HashSet<Path>, StorageError> {
        Ok(dirty_pathids(dirty)?
            .into_iter()
            .filter_map(|i| tree.backtrack(i).ok().flatten())
            .collect())
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
    fn read_txn_with_index_compiles() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_read()?;
        let cache = txn.read_cache()?;
        let tree = txn.read_tree()?;
        let path = Path::parse("test.txt")?;
        assert_eq!(None, cache.indexed(&tree, &path)?);

        Ok(())
    }

    #[test]
    fn write_txn_with_index_compiles() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let cache = txn.read_cache()?;
        let tree = txn.read_tree()?;
        let path = Path::parse("test.txt")?;
        assert_eq!(None, cache.indexed(&tree, &path)?);

        Ok(())
    }

    #[test]
    fn write_index_compiles() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;
        let path = Path::parse("test.txt")?;
        cache.index(
            &mut tree,
            &mut history,
            &mut dirty,
            &path,
            100,
            UnixTime::now(),
            Hash::zero(),
        )?;

        Ok(())
    }

    #[test]
    fn add_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let mtime = UnixTime::from_secs(1234567890);
        let path = Path::parse("foo/bar.txt")?;

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;
        cache.index(
            &mut tree,
            &mut history,
            &mut dirty,
            &path,
            100,
            mtime,
            Hash([0xfa; 32]),
        )?;

        assert!(cache.is_indexed(&tree, &path)?);
        let entry = cache.indexed(&tree, &path)?.unwrap();
        assert_eq!(entry.size, 100);
        assert_eq!(entry.mtime, mtime);
        assert_eq!(entry.hash, Hash([0xfa; 32]));

        Ok(())
    }

    #[test]
    fn replace_file_in_index() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let mtime1 = UnixTime::from_secs(1234567890);
        let mtime2 = UnixTime::from_secs(1234567891);
        let path = Path::parse("foo/bar.txt")?;

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;
        cache.index(
            &mut tree,
            &mut history,
            &mut dirty,
            &path,
            100,
            mtime1,
            Hash([0xfa; 32]),
        )?;
        cache.index(
            &mut tree,
            &mut history,
            &mut dirty,
            &path,
            200,
            mtime2,
            Hash([0x07; 32]),
        )?;

        // Verify the file was replaced
        assert!(cache.is_indexed(&tree, &path)?);
        let entry = cache.indexed(&tree, &path)?.unwrap();
        assert_eq!(entry.size, 200);
        assert_eq!(entry.mtime, mtime2);
        assert_eq!(entry.hash, Hash([0x07; 32]));

        Ok(())
    }

    #[test]
    fn has_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let mtime = UnixTime::from_secs(1234567890);
        let path = Path::parse("foo.txt")?;

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;
        cache.index(
            &mut tree,
            &mut history,
            &mut dirty,
            &path,
            100,
            mtime,
            Hash([0xfa; 32]),
        )?;

        assert!(cache.is_indexed(&tree, &path)?);
        assert!(!cache.is_indexed(&tree, &Path::parse("bar.txt")?)?);
        assert!(!cache.is_indexed(&tree, &Path::parse("other.txt")?)?);

        Ok(())
    }

    #[test]
    fn get_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let mtime = UnixTime::from_secs(1234567890);
        let path = Path::parse("foo/bar")?;
        let hash = Hash([0xfa; 32]);
        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;
        cache.index(
            &mut tree,
            &mut history,
            &mut dirty,
            &path,
            100,
            mtime,
            hash.clone(),
        )?;

        let entry = cache.indexed(&tree, &path)?.unwrap();
        assert_eq!(entry.size, 100);
        assert_eq!(entry.mtime, mtime);
        assert_eq!(entry.hash, hash);

        Ok(())
    }

    #[test]
    fn remove_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let mtime = UnixTime::from_secs(1234567890);
        let path = Path::parse("foo/bar.txt")?;

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        cache.index(
            &mut tree,
            &mut history,
            &mut dirty,
            &path,
            100,
            mtime,
            Hash([0xfa; 32]),
        )?;

        cache.remove_from_index_recursively(&mut tree, &mut history, &mut dirty, &path)?;

        assert!(!cache.is_indexed(&tree, &path)?);

        Ok(())
    }

    #[test]
    fn remove_dir() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let mtime = UnixTime::from_secs(1234567890);

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        cache.index(
            &mut tree,
            &mut history,
            &mut dirty,
            Path::parse("foo/bar1.txt")?,
            100,
            mtime,
            Hash([0xfa; 32]),
        )?;
        cache.index(
            &mut tree,
            &mut history,
            &mut dirty,
            Path::parse("foo/bar2.txt")?,
            100,
            mtime,
            Hash([0xfa; 32]),
        )?;

        cache.remove_from_index_recursively(
            &mut tree,
            &mut history,
            &mut dirty,
            Path::parse("foo")?,
        )?;

        assert!(!cache.is_indexed(&tree, Path::parse("foo/bar1.txt")?)?);
        assert!(!cache.is_indexed(&tree, Path::parse("foo/bar2.txt")?)?);

        Ok(())
    }

    #[tokio::test]
    async fn add_file_marks_dirty_and_adds_history() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let mtime = UnixTime::from_secs(1234567890);
        let path = Path::parse("foo/bar.txt")?;
        let hash = Hash([0xfa; 32]);

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        // Clear any existing dirty entries
        dirty.delete_range(0, 999)?;

        // Add a file
        cache.index(
            &mut tree,
            &mut history,
            &mut dirty,
            &path,
            100,
            mtime,
            hash.clone(),
        )?;

        // Verify the file was added to the index
        assert!(cache.is_indexed(&tree, &path)?);

        // Verify the path was marked dirty
        assert!(dirty_paths(&dirty, &tree)?.contains(&path));

        // Verify history entry was added (Add entry)
        let history_entries = collect_history_entries(&history)?;
        assert_eq!(history_entries.len(), 1);
        match &history_entries[0] {
            HistoryTableEntry::Add(entry_path) => {
                assert_eq!(entry_path, &path);
            }
            _ => panic!("Expected Add history entry"),
        }

        Ok(())
    }

    #[test]
    fn replace_file_marks_dirty_and_adds_history() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let mtime1 = UnixTime::from_secs(1234567890);
        let mtime2 = UnixTime::from_secs(1234567891);
        let path = Path::parse("foo/bar.txt")?;
        let hash1 = Hash([0xfa; 32]);
        let hash2 = Hash([0x07; 32]);

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        // Clear any existing dirty entries
        dirty.delete_range(0, 999)?;

        // Add initial file
        cache.index(
            &mut tree,
            &mut history,
            &mut dirty,
            &path,
            100,
            mtime1,
            hash1.clone(),
        )?;

        // Clear dirty entries from first add
        dirty.delete_range(0, 999)?;

        // Replace the file
        cache.index(
            &mut tree,
            &mut history,
            &mut dirty,
            &path,
            200,
            mtime2,
            hash2.clone(),
        )?;

        // Verify the file was replaced in the index
        let entry = cache.indexed(&tree, &path)?.unwrap();
        assert_eq!(entry.hash, hash2);

        // Verify the path was marked dirty
        let dirty_paths = dirty_paths(&dirty, &tree)?;
        assert!(dirty_paths.contains(&path));

        // Verify history entries were added (Add + Replace entries)
        let history_entries = collect_history_entries(&history)?;
        assert_eq!(history_entries.len(), 2);
        match &history_entries[0] {
            HistoryTableEntry::Add(entry_path) => {
                assert_eq!(entry_path, &path);
            }
            _ => panic!("Expected Add history entry"),
        }
        match &history_entries[1] {
            HistoryTableEntry::Replace(entry_path, old_hash) => {
                assert_eq!(entry_path, &path);
                assert_eq!(old_hash, &hash1);
            }
            _ => panic!("Expected Replace history entry"),
        }

        Ok(())
    }

    #[test]
    fn remove_file_marks_dirty_and_adds_history() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let mtime = UnixTime::from_secs(1234567890);
        let path = Path::parse("foo/bar.txt")?;
        let hash = Hash([0xfa; 32]);

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        // Add a file first
        let pathid = cache.index(
            &mut tree,
            &mut history,
            &mut dirty,
            &path,
            100,
            mtime,
            hash.clone(),
        )?;

        // Clear dirty entries from add
        dirty.delete_range(0, 999)?;

        // Remove the file
        let removed = cache.remove_from_index(&mut tree, &mut history, &mut dirty, &path)?;
        assert!(removed);

        // Verify the file was removed from the index
        assert!(!cache.is_indexed(&tree, &path)?);

        // Verify the path was marked dirty
        assert_eq!(HashSet::from([pathid]), dirty_pathids(&dirty)?);

        // Verify history entries were added (Add + Remove entries)
        let history_entries = collect_history_entries(&history)?;
        assert_eq!(history_entries.len(), 2);
        match &history_entries[0] {
            HistoryTableEntry::Add(entry_path) => {
                assert_eq!(entry_path, &path);
            }
            _ => panic!("Expected Add history entry"),
        }
        match &history_entries[1] {
            HistoryTableEntry::Remove(entry_path, removed_hash) => {
                assert_eq!(entry_path, &path);
                assert_eq!(removed_hash, &hash);
            }
            _ => panic!("Expected Remove history entry"),
        }

        Ok(())
    }

    #[test]
    fn remove_file_or_dir_marks_dirty_and_adds_history() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let mtime = UnixTime::from_secs(1234567890);

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        // Add files in a directory
        let path1 = Path::parse("foo/bar1.txt")?;
        let path2 = Path::parse("foo/bar2.txt")?;
        let hash1 = Hash([0xfa; 32]);
        let hash2 = Hash([0xfb; 32]);

        let pathid1 = cache.index(
            &mut tree,
            &mut history,
            &mut dirty,
            &path1,
            100,
            mtime,
            hash1.clone(),
        )?;
        let pathid2 = cache.index(
            &mut tree,
            &mut history,
            &mut dirty,
            &path2,
            200,
            mtime,
            hash2.clone(),
        )?;

        // Clear dirty entries from adds
        dirty.delete_range(0, 999)?;

        // Remove the directory
        cache.remove_from_index_recursively(
            &mut tree,
            &mut history,
            &mut dirty,
            Path::parse("foo")?,
        )?;

        // Verify the files were removed from the index
        assert!(!cache.is_indexed(&tree, &path1)?);
        assert!(!cache.is_indexed(&tree, &path2)?);

        // Verify the paths were marked dirty
        assert_eq!(HashSet::from([pathid1, pathid2]), dirty_pathids(&dirty)?);

        // Verify history entries were added (2 Add + 2 Remove entries)
        let history_entries = collect_history_entries(&history)?;
        assert_unordered::assert_eq_unordered!(
            vec![
                HistoryTableEntry::Add(path1.clone()),
                HistoryTableEntry::Add(path2.clone()),
                HistoryTableEntry::Remove(path1.clone(), hash1.clone()),
                HistoryTableEntry::Remove(path2.clone(), hash2.clone()),
            ],
            history_entries
        );

        Ok(())
    }

    #[test]
    fn record_outdated_updates_entry() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let mtime = UnixTime::from_secs(1234567890);
        let path = Path::parse("foo/bar.txt")?;
        let old_hash = Hash([0xfa; 32]);
        let new_hash = Hash([0x07; 32]);

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        // Add a file with the old hash
        cache.index(
            &mut tree,
            &mut history,
            &mut dirty,
            &path,
            100,
            mtime,
            old_hash.clone(),
        )?;

        // Record that this version is outdated by the new hash
        cache.record_outdated(&mut tree, &mut dirty, &path, &old_hash, &new_hash)?;

        // Verify the entry was updated with outdated_by field
        let entry = cache.indexed(&tree, &path)?.unwrap();
        assert_eq!(entry.hash, old_hash);
        assert_eq!(entry.outdated_by, Some(new_hash));

        Ok(())
    }

    #[test]
    fn record_outdated_ignores_unrelated_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let mtime = UnixTime::from_secs(1234567890);
        let path = Path::parse("foo/bar.txt")?;
        let file_hash = Hash([0xfa; 32]);
        let unrelated_hash = Hash([0xbb; 32]);
        let new_hash = Hash([0x07; 32]);

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        // Add a file with file_hash
        cache.index(
            &mut tree,
            &mut history,
            &mut dirty,
            &path,
            100,
            mtime,
            file_hash.clone(),
        )?;

        // Record that an unrelated hash is outdated by the new hash
        cache.record_outdated(&mut tree, &mut dirty, &path, &unrelated_hash, &new_hash)?;

        // Verify the entry was NOT updated (outdated_by should remain None)
        let entry = cache.indexed(&tree, &path)?.unwrap();
        assert_eq!(entry.hash, file_hash);
        assert_eq!(entry.outdated_by, None);

        Ok(())
    }

    #[test]
    fn index_add_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar.txt")?;
        super::add_file(&fixture.db, &path, 100, mtime, Hash([0xfa; 32]))?;

        // Verify the file was added
        assert!(super::has_file(&fixture.db, &path)?);
        let entry = super::get_file(&fixture.db, &path)?.unwrap();
        assert_eq!(entry.size, 100);
        assert_eq!(entry.mtime, mtime);
        assert_eq!(entry.hash, Hash([0xfa; 32]));

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

        let mtime1 = UnixTime::from_secs(1234567890);
        let mtime2 = UnixTime::from_secs(1234567891);
        let path = realize_types::Path::parse("foo/bar.txt")?;
        super::add_file(&fixture.db, &path, 100, mtime1, Hash([0xfa; 32]))?;
        super::add_file(&fixture.db, &path, 200, mtime2, Hash([0x07; 32]))?;

        // Verify the file was replaced
        assert!(super::has_file(&fixture.db, &path)?);
        let entry = super::get_file(&fixture.db, &path)?.unwrap();
        assert_eq!(entry.size, 200);
        assert_eq!(entry.mtime, mtime2);
        assert_eq!(entry.hash, Hash([0x07; 32]));

        Ok(())
    }

    #[test]
    fn index_has_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo.txt")?;
        super::add_file(&fixture.db, &path, 100, mtime, Hash([0xfa; 32]))?;

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

        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar")?;
        let hash = Hash([0xfa; 32]);
        super::add_file(&fixture.db, &path, 100, mtime, hash.clone())?;

        let entry = super::get_file(&fixture.db, &path)?.unwrap();
        assert_eq!(entry.size, 100);
        assert_eq!(entry.mtime, mtime);
        assert_eq!(entry.hash, hash);

        Ok(())
    }

    #[test]
    fn index_has_matching_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar")?;
        super::add_file(&fixture.db, &path, 100, mtime, Hash([0xfa; 32]))?;

        assert!(super::has_matching_file(&fixture.db, &path, 100, mtime)?);
        assert!(!super::has_matching_file(
            &fixture.db,
            &realize_types::Path::parse("other")?,
            100,
            mtime
        )?);
        assert!(!super::has_matching_file(&fixture.db, &path, 200, mtime)?);
        assert!(!super::has_matching_file(
            &fixture.db,
            &path,
            100,
            UnixTime::from_secs(1234567891)
        )?);

        Ok(())
    }

    #[test]
    fn index_remove_file_or_dir() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar.txt")?;
        super::add_file(&fixture.db, &path, 100, mtime, Hash([0xfa; 32]))?;
        super::remove_file_or_dir(&fixture.db, &path)?;

        assert!(!super::has_file(&fixture.db, &path)?);

        Ok(())
    }

    #[test]
    fn index_remove_file_if_missing_success() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("bar.txt")?;
        super::add_file(&fixture.db, &path, 100, mtime, Hash([0xfa; 32]))?;
        assert!(super::remove_file_if_missing(&fixture.db, &path)?);

        assert!(!super::has_file(&fixture.db, &path)?);

        Ok(())
    }

    #[test]
    fn index_remove_file_if_missing_failure() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("bar.txt")?;
        super::add_file(&fixture.db, &path, 100, mtime, Hash([0xfa; 32]))?;

        // Create the file on disk
        let file_path = fixture.db.index().datadir().join("bar.txt");
        std::fs::write(&file_path, "content")?;

        assert!(!super::remove_file_if_missing(&fixture.db, &path)?);
        assert!(super::has_file(&fixture.db, &path)?);

        Ok(())
    }

    #[test]
    fn index_remove_dir_from_index() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let mtime = UnixTime::from_secs(1234567890);

        super::add_file(
            &fixture.db,
            &realize_types::Path::parse("foo/a")?,
            100,
            mtime,
            Hash([1; 32]),
        )?;
        super::add_file(
            &fixture.db,
            &realize_types::Path::parse("foo/b")?,
            100,
            mtime,
            Hash([2; 32]),
        )?;
        super::add_file(
            &fixture.db,
            &realize_types::Path::parse("foo/c")?,
            100,
            mtime,
            Hash([3; 32]),
        )?;
        super::add_file(
            &fixture.db,
            &realize_types::Path::parse("foobar")?,
            100,
            mtime,
            Hash([0x04; 32]),
        )?;

        // Remove the directory
        super::remove_file_or_dir(&fixture.db, &realize_types::Path::parse("foo")?)?;

        // Verify all files in the directory were removed
        assert!(!super::has_file(
            &fixture.db,
            &realize_types::Path::parse("foo/a")?
        )?);
        assert!(!super::has_file(
            &fixture.db,
            &realize_types::Path::parse("foo/b")?
        )?);
        assert!(!super::has_file(
            &fixture.db,
            &realize_types::Path::parse("foo/c")?
        )?);

        // But the file outside the directory should remain
        assert!(super::has_file(
            &fixture.db,
            &realize_types::Path::parse("foobar")?
        )?);

        Ok(())
    }

    #[tokio::test]
    async fn index_all_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let mtime = UnixTime::from_secs(1234567890);
        let path1 = realize_types::Path::parse("foo/a")?;
        let path2 = realize_types::Path::parse("foo/b")?;
        let path3 = realize_types::Path::parse("bar.txt")?;

        super::add_file(&fixture.db, &path1, 100, mtime, Hash([1; 32]))?;
        super::add_file(&fixture.db, &path2, 200, mtime, Hash([2; 32]))?;
        super::add_file(&fixture.db, &path3, 300, mtime, Hash([3; 32]))?;

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
        let db = fixture.db;

        let mut rx = db.history().watch();
        let initial = *rx.borrow();

        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar.txt")?;
        super::add_file(&db, &path, 100, mtime, Hash([0xfa; 32]))?;

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

        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar.txt")?;
        super::add_file(&fixture.db, &path, 100, mtime, Hash([0xfa; 32]))?;

        let updated = super::last_history_index(&fixture.db)?;
        assert!(updated > initial);

        Ok(())
    }

    #[test]
    fn test_branch_function() -> anyhow::Result<()> {
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
            14,
            source_mtime,
            source_hash.clone(),
        )?;

        // Add destination file to index
        let dest_index_path = Path::parse("dest.txt")?;
        super::add_file(
            &fixture.db,
            &dest_index_path,
            12,
            dest_mtime,
            dest_hash.clone(),
        )?;

        // Verify both files are in the index
        assert!(super::has_file(&fixture.db, &source_index_path)?);
        assert!(super::has_file(&fixture.db, &dest_index_path)?);

        // Test successful branch (create hard link)
        let txn = fixture.db.begin_read()?;
        let cache = txn.read_cache()?;
        let tree = txn.read_tree()?;

        let result = super::branch(
            &cache,
            &tree,
            &source_index_path,
            &dest_index_path,
            &source_hash,
            Some(&dest_hash), // old_hash matches current dest
        )?;

        assert!(result, "Branch should succeed when conditions are met");

        // Verify hard link was created by checking pathid numbers
        let source_metadata = std::fs::metadata(source_path)?;
        let dest_metadata = std::fs::metadata(dest_path)?;
        assert_eq!(
            source_metadata.ino(),
            dest_metadata.ino(),
            "Files should have same pathid after hard link"
        );

        // Test branch failure when source hash doesn't match
        let wrong_hash = Hash([0x99; 32]);
        let result = super::branch(
            &cache,
            &tree,
            &source_index_path,
            &dest_index_path,
            &wrong_hash,
            Some(&dest_hash),
        )?;

        assert!(!result, "Branch should fail when source hash doesn't match");

        // Test branch failure when dest doesn't match old_hash
        let result = super::branch(
            &cache,
            &tree,
            &source_index_path,
            &dest_index_path,
            &source_hash,
            Some(&wrong_hash), // old_hash doesn't match current dest
        )?;

        assert!(
            !result,
            "Branch should fail when dest doesn't match old_hash"
        );

        // Test branch failure when source doesn't exist in tree
        let nonexistent_path = Path::parse("nonexistent.txt")?;
        let result = super::branch(
            &cache,
            &tree,
            &nonexistent_path,
            &dest_index_path,
            &source_hash,
            Some(&dest_hash),
        )?;

        assert!(
            !result,
            "Branch should fail when source doesn't exist in tree"
        );

        Ok(())
    }

    #[test]
    fn rename_file() -> anyhow::Result<()> {
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
            14,
            source_mtime,
            source_hash.clone(),
        )?;

        // Add destination file to index
        let dest_index_path = Path::parse("dest.txt")?;
        super::add_file(
            &fixture.db,
            &dest_index_path,
            12,
            dest_mtime,
            dest_hash.clone(),
        )?;

        // Verify both files are in the index
        assert!(super::has_file(&fixture.db, &source_index_path)?);
        assert!(super::has_file(&fixture.db, &dest_index_path)?);

        // Test successful rename
        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        let result = super::rename(
            &mut cache,
            &mut tree,
            &mut history,
            &mut dirty,
            &source_index_path,
            &dest_index_path,
            &source_hash,
            Some(&dest_hash), // old_hash matches current dest
        )?;

        assert!(result, "Rename should succeed when conditions are met");

        assert!(!source_path.exists());
        assert_eq!("source content", std::fs::read_to_string(dest_path)?);

        Ok(())
    }

    #[test]
    fn rename_conditions_not_met() -> anyhow::Result<()> {
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
            14,
            source_mtime,
            source_hash.clone(),
        )?;

        // Add destination file to index
        let dest_index_path = Path::parse("dest.txt")?;
        super::add_file(
            &fixture.db,
            &dest_index_path,
            12,
            dest_mtime,
            dest_hash.clone(),
        )?;

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut history = txn.write_history()?;
        let mut dirty = txn.write_dirty()?;

        // Test rename failure when source hash doesn't match
        let wrong_hash = Hash([0x99; 32]);
        let result = super::rename(
            &mut cache,
            &mut tree,
            &mut history,
            &mut dirty,
            &source_index_path,
            &dest_index_path,
            &wrong_hash,
            Some(&dest_hash),
        )?;

        assert!(!result, "Rename should fail when source hash doesn't match");

        // Test rename failure when dest doesn't match old_hash
        let result = super::rename(
            &mut cache,
            &mut tree,
            &mut history,
            &mut dirty,
            &source_index_path,
            &dest_index_path,
            &source_hash,
            Some(&wrong_hash), // old_hash doesn't match current dest
        )?;

        assert!(
            !result,
            "Rename should fail when dest doesn't match old_hash"
        );

        // Test rename failure when source doesn't exist in tree
        let nonexistent_path = Path::parse("nonexistent.txt")?;
        let result = super::rename(
            &mut cache,
            &mut tree,
            &mut history,
            &mut dirty,
            &nonexistent_path,
            &dest_index_path,
            &source_hash,
            Some(&dest_hash),
        )?;

        assert!(
            !result,
            "Rename should fail when source doesn't exist in tree"
        );

        Ok(())
    }
}
