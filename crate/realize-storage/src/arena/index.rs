#![allow(dead_code)] // work in progress

use super::db::ArenaWriteTransaction;
use super::dirty::WritableOpenDirty;
use super::history::WritableOpenHistory;
use super::tree::{TreeExt, TreeLoc, TreeReadOperations, WritableOpenTree};
use super::types::{FileTableEntry, HistoryTableEntry};
use crate::utils::fs_utils;
use crate::utils::holder::Holder;
use crate::{Inode, StorageError};
use realize_types::{self, Arena, Hash, Path, UnixTime};
use redb::{ReadableTable, Table};
use std::ops::RangeBounds;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

/// Trait for index operations that abstracts over the implementation.
pub trait RealIndex: Send + Sync {
    /// Returns the database UUID.
    ///
    /// A UUID is set when a new database is created.
    fn uuid(&self) -> &Uuid;

    /// The arena tied to this index.
    fn arena(&self) -> Arena;

    /// Subscribe to a watch channel reporting the highest history entry index.
    ///
    /// Entries can be later queried using [RealIndex::history].
    ///
    /// The value itself is also available as [RealIndex::last_history_index].
    fn watch_history(&self) -> watch::Receiver<u64>;

    /// Index of the last history entry that was written.
    ///
    /// This is the value tracked by [RealIndex::watch_history].
    fn last_history_index(&self) -> Result<u64, StorageError>;

    /// Get a file entry.
    fn get_file(&self, path: &realize_types::Path) -> Result<Option<IndexedFile>, StorageError>;

    fn get_file_txn(
        &self,
        txn: &super::db::ArenaReadTransaction,
        path: &realize_types::Path,
    ) -> Result<Option<IndexedFile>, StorageError>;

    /// Check whether a given file is in the index already.
    fn has_file(&self, path: &realize_types::Path) -> Result<bool, StorageError>;

    /// Check whether a given file is in the index with the given size and mtime.
    fn has_matching_file(
        &self,
        path: &realize_types::Path,
        size: u64,
        mtime: UnixTime,
    ) -> Result<bool, StorageError>;

    /// Add a file entry with the given values. Replace one if it exists.
    fn add_file(
        &self,
        path: &realize_types::Path,
        size: u64,
        mtime: UnixTime,
        hash: Hash,
    ) -> Result<(), StorageError>;

    fn add_file_if_matches(
        &self,
        root: &std::path::Path,
        path: &realize_types::Path,
        size: u64,
        mtime: UnixTime,
        hash: Hash,
    ) -> Result<bool, StorageError>;

    fn remove_file_if_missing(
        &self,
        root: &std::path::Path,
        path: &realize_types::Path,
    ) -> Result<bool, StorageError>;

    /// Send all valid entries of the file table to the given channel.
    fn all_files(
        &self,
        tx: mpsc::Sender<(realize_types::Path, IndexedFile)>,
    ) -> Result<(), StorageError>;

    /// Grab a range of history entries.
    fn history(
        &self,
        range: std::ops::Range<u64>,
        tx: mpsc::Sender<Result<(u64, HistoryTableEntry), StorageError>>,
    ) -> Result<(), StorageError>;

    /// Remove a path that can be a file or a directory.
    ///
    /// If the path is a directory, all files within that directory
    /// are removed, recursively.
    fn remove_file_or_dir(&self, path: &realize_types::Path) -> Result<(), StorageError>;

    /// Remove `path` from the index if the hash and file match,
    /// report it as a drop in the history.
    fn drop_file_if_matches(
        &self,
        txn: &ArenaWriteTransaction,
        root: &std::path::Path,
        path: &realize_types::Path,
        hash: &Hash,
    ) -> Result<bool, StorageError>;
}

#[derive(Clone)]
pub struct RealIndexAsync {
    inner: Arc<dyn RealIndex>,
}

impl RealIndexAsync {
    pub fn new(inner: Arc<dyn RealIndex>) -> Self {
        Self { inner }
    }

    /// Returns the database UUID.
    ///
    /// A UUID is set when a new database is created.
    pub fn uuid(&self) -> &Uuid {
        self.inner.uuid()
    }

    /// The arena tied to this index.
    pub fn arena(&self) -> Arena {
        self.inner.arena()
    }

    /// Subscribe to a watch channel reporting the highest history entry index.
    ///
    /// Entries can be later queried using [RealIndexAsync::history].
    ///
    /// The value itself is also available as [RealIndexAsync::last_history_index].
    pub fn watch_history(&self) -> watch::Receiver<u64> {
        self.inner.watch_history()
    }

    /// Index of the last history entry that was written.
    ///
    /// This is the value tracked by [RealIndexAsync::watch_history].
    pub async fn last_history_index(&self) -> Result<u64, StorageError> {
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || inner.last_history_index()).await?
    }

    /// Return a reference to the underlying blocking instance.
    pub fn blocking(&self) -> Arc<dyn RealIndex> {
        Arc::clone(&self.inner)
    }

    /// Return all valid file entries as a stream.
    pub fn all_files(&self) -> ReceiverStream<(realize_types::Path, IndexedFile)> {
        let (tx, rx) = mpsc::channel(100);

        let inner = Arc::clone(&self.inner);
        task::spawn_blocking(move || inner.all_files(tx));

        ReceiverStream::new(rx)
    }

    /// Grab a range of history entries.
    pub fn history(
        &self,
        range: impl RangeBounds<u64> + Send + 'static,
    ) -> ReceiverStream<Result<(u64, HistoryTableEntry), StorageError>> {
        let (tx, rx) = mpsc::channel(100);

        let inner = Arc::clone(&self.inner);
        let range = Box::new(range);
        task::spawn_blocking(move || {
            let tx_clone = tx.clone();
            // Convert the generic range to a concrete Range<u64>
            let start = match range.start_bound() {
                std::ops::Bound::Included(&x) => x,
                std::ops::Bound::Excluded(&x) => x + 1,
                std::ops::Bound::Unbounded => 0,
            };
            let end = match range.end_bound() {
                std::ops::Bound::Included(&x) => x + 1,
                std::ops::Bound::Excluded(&x) => x,
                std::ops::Bound::Unbounded => u64::MAX,
            };
            let concrete_range = start..end;
            if let Err(err) = inner.history(concrete_range, tx) {
                // Send any global error to the channel, so it ends up
                // in the stream instead of getting lost.
                let _ = tx_clone.blocking_send(Err(err));
            }
        });

        ReceiverStream::new(rx)
    }

    /// Get a file entry
    pub async fn get_file(
        &self,
        path: &realize_types::Path,
    ) -> Result<Option<IndexedFile>, StorageError> {
        let inner = Arc::clone(&self.inner);
        let path = path.clone();

        task::spawn_blocking(move || inner.get_file(&path)).await?
    }

    /// Check whether a given file is in the index already.
    pub async fn has_file<T>(&self, path: T) -> Result<bool, StorageError>
    where
        T: AsRef<realize_types::Path>,
    {
        let path = path.as_ref();
        let inner = Arc::clone(&self.inner);
        let path = path.clone();

        task::spawn_blocking(move || inner.has_file(&path)).await?
    }

    /// Check whether a given file is in the index already with the given size and mtime.
    pub async fn has_matching_file(
        &self,
        path: &realize_types::Path,
        size: u64,
        mtime: UnixTime,
    ) -> Result<bool, StorageError> {
        let inner = Arc::clone(&self.inner);
        let path = path.clone();
        let mtime = mtime.clone();

        task::spawn_blocking(move || inner.has_matching_file(&path, size, mtime)).await?
    }

    /// Remove a path that can be a file or a directory.
    ///
    /// If the path is a directory, all files within that directory
    /// are removed, recursively.
    pub async fn remove_file_or_dir<T>(&self, path: T) -> Result<(), StorageError>
    where
        T: AsRef<realize_types::Path>,
    {
        let path = path.as_ref();
        let inner = Arc::clone(&self.inner);
        let path = path.clone();

        task::spawn_blocking(move || inner.remove_file_or_dir(&path)).await?
    }

    pub async fn add_file(
        &self,
        path: &realize_types::Path,
        size: u64,
        mtime: UnixTime,
        hash: Hash,
    ) -> Result<(), StorageError> {
        let inner = Arc::clone(&self.inner);
        let path = path.clone();
        let mtime = mtime.clone();

        task::spawn_blocking(move || inner.add_file(&path, size, mtime, hash)).await?
    }

    pub async fn add_file_if_matches(
        &self,
        root: &std::path::Path,
        path: &realize_types::Path,
        size: u64,
        mtime: UnixTime,
        hash: Hash,
    ) -> Result<bool, StorageError> {
        let inner = Arc::clone(&self.inner);
        let path = path.clone();
        let mtime = mtime.clone();
        let root = root.to_path_buf();

        task::spawn_blocking(move || inner.add_file_if_matches(&root, &path, size, mtime, hash))
            .await?
    }

    pub async fn remove_file_if_missing(
        &self,
        root: &std::path::Path,
        path: &realize_types::Path,
    ) -> Result<bool, StorageError> {
        let inner = Arc::clone(&self.inner);
        let root = root.to_path_buf();
        let path = path.clone();

        task::spawn_blocking(move || inner.remove_file_if_missing(&root, &path)).await?
    }
}

/// Return the path for the given file if its current version matches `hash`.
///
/// If `hash` is none, the file must not exist. if `hash` is not none,
/// its version in the index must match and its size and modification
/// time must match those in the index.
pub(crate) fn indexed_file_path<'b, L: Into<TreeLoc<'b>>, R: AsRef<std::path::Path>>(
    index: &impl IndexReadOperations,
    tree: &impl TreeReadOperations,
    root: R,
    loc: L,
    hash: Option<&Hash>,
) -> Result<Option<std::path::PathBuf>, StorageError> {
    let loc = loc.into();
    let entry = index.get(tree, loc.borrow())?;
    if let Ok(path) = tree.backtrack(loc) {
        let root = root.as_ref();
        let file_path = path.within(root);
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
                if entry.is_none() && fs_utils::metadata_no_symlink_blocking(root, &path).is_err() {
                    return Ok(Some(file_path));
                }
            }
        }
    }
    Ok(None)
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexedFile {
    pub hash: Hash,
    pub mtime: UnixTime,
    pub size: u64,

    // If set, a version is known to exist that replaces the version
    // in this entry.
    pub outdated_by: Option<Hash>,
}

impl IndexedFile {
    /// Return true if replacing `old_hash` makes this entry outdated.
    pub(crate) fn is_outdated_by(&self, old_hash: &Hash) -> bool {
        self.hash == *old_hash
            || self
                .outdated_by
                .as_ref()
                .map(|h| *h == *old_hash)
                .unwrap_or(false)
    }

    /// Check whether `file_path` size and mtime match this entry's.
    pub(crate) fn matches_file<P: AsRef<std::path::Path>>(&self, file_path: P) -> bool {
        if let Ok(m) = file_path.as_ref().metadata()
            && self.matches(m.len(), UnixTime::mtime(&m))
        {
            return true;
        }

        false
    }

    /// Check whether `file_path` size and mtime match this entry's.
    pub(crate) fn matches(&self, size: u64, mtime: UnixTime) -> bool {
        size == self.size && mtime == self.mtime
    }
}

impl From<FileTableEntry> for IndexedFile {
    fn from(value: FileTableEntry) -> Self {
        IndexedFile {
            hash: value.hash,
            mtime: value.mtime,
            size: value.size,
            outdated_by: value.outdated_by,
        }
    }
}
impl From<&FileTableEntry> for IndexedFile {
    fn from(value: &FileTableEntry) -> Self {
        IndexedFile {
            hash: value.hash.clone(),
            mtime: value.mtime,
            size: value.size,
            outdated_by: value.outdated_by.clone(),
        }
    }
}

pub(crate) struct ReadableOpenIndex<T>
where
    T: ReadableTable<Inode, Holder<'static, FileTableEntry>>,
{
    table: T,
}

impl<T> ReadableOpenIndex<T>
where
    T: ReadableTable<Inode, Holder<'static, FileTableEntry>>,
{
    pub(crate) fn new(table: T) -> Self {
        Self { table }
    }
}

pub(crate) struct WritableOpenIndex<'a> {
    table: Table<'a, Inode, Holder<'static, FileTableEntry>>,
}

impl<'a> WritableOpenIndex<'a> {
    pub(crate) fn new(table: Table<'a, Inode, Holder<FileTableEntry>>) -> Self {
        Self { table }
    }
}

/// Read operations for index. See also [IndexExt].
pub(crate) trait IndexReadOperations {
    /// Get a file entry by inode.
    fn get_at_inode(&self, inode: Inode) -> Result<Option<IndexedFile>, StorageError>;

    /// Check whether a given file is in the index already.
    fn has_at_inode(&self, inode: Inode) -> Result<bool, StorageError>;

    /// Get all files in the index.
    fn all(&self, tx: mpsc::Sender<(Path, IndexedFile)>) -> Result<(), StorageError>;
}

impl<T> IndexReadOperations for ReadableOpenIndex<T>
where
    T: ReadableTable<Inode, Holder<'static, FileTableEntry>>,
{
    fn get_at_inode(&self, inode: Inode) -> Result<Option<IndexedFile>, StorageError> {
        get_at_inode(&self.table, inode)
    }

    fn has_at_inode(&self, inode: Inode) -> Result<bool, StorageError> {
        has_at_inode(&self.table, inode)
    }

    fn all(&self, tx: mpsc::Sender<(Path, IndexedFile)>) -> Result<(), StorageError> {
        all(&self.table, tx)
    }
}

impl<'a> IndexReadOperations for WritableOpenIndex<'a> {
    fn get_at_inode(&self, inode: Inode) -> Result<Option<IndexedFile>, StorageError> {
        get_at_inode(&self.table, inode)
    }

    fn has_at_inode(&self, inode: Inode) -> Result<bool, StorageError> {
        has_at_inode(&self.table, inode)
    }

    fn all(&self, tx: mpsc::Sender<(Path, IndexedFile)>) -> Result<(), StorageError> {
        all(&self.table, tx)
    }
}

/// Extend [IndexReadOperations] with convenience functions for working
/// with [Path].
pub(crate) trait IndexExt {
    /// Get a file entry by path.
    fn get<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<Option<IndexedFile>, StorageError>;

    fn has<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<bool, StorageError>;
}

impl<T: IndexReadOperations> IndexExt for T {
    fn get<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<Option<IndexedFile>, StorageError> {
        if let Some(inode) = tree.resolve(loc)? {
            self.get_at_inode(inode)
        } else {
            Ok(None)
        }
    }
    fn has<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<bool, StorageError> {
        if let Some(inode) = tree.resolve(loc)? {
            self.has_at_inode(inode)
        } else {
            Ok(false)
        }
    }
}

impl<'a> WritableOpenIndex<'a> {
    /// Add a file entry with the given values. Replace one if it exists.
    pub(crate) fn add<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        history: &mut WritableOpenHistory,
        dirty: &mut WritableOpenDirty,
        loc: L,
        size: u64,
        mtime: UnixTime,
        hash: Hash,
    ) -> Result<Inode, StorageError> {
        let loc = loc.into();
        let inode = tree.setup(loc.borrow())?;
        let old_hash = self
            .table
            .get(inode)
            .ok()
            .flatten()
            .map(|e| e.value().parse().ok())
            .flatten()
            .map(|e| e.hash);
        let same_hash = old_hash.as_ref().map(|h| *h == hash).unwrap_or(false);
        if same_hash {
            return Ok(inode);
        }
        if let Ok(path) = tree.backtrack(loc) {
            tree.insert_and_incref(
                inode,
                &mut self.table,
                inode,
                Holder::with_content(FileTableEntry::new(path.clone(), size, mtime, hash))?,
            )?;
            dirty.mark_dirty(inode)?;
            history.report_added(&path, old_hash.as_ref())?;
        }
        Ok(inode)
    }

    /// Remove a file entry at the given location, if it exists.
    ///
    /// Return true if something was removed.
    pub(crate) fn remove<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        history: &mut WritableOpenHistory,
        dirty: &mut WritableOpenDirty,
        loc: L,
    ) -> Result<bool, StorageError> {
        let loc = loc.into();
        if let Some(inode) = tree.resolve(loc)? {
            let FileTableEntry { path, hash, .. } = match self.table.get(inode)? {
                None => {
                    // Nothing to do
                    return Ok(false);
                }
                Some(existing) => existing.value().parse()?,
            };
            if tree.remove_and_decref(inode, &mut self.table, inode)? {
                dirty.mark_dirty(inode)?;
                history.report_removed(&path, &hash)?;

                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Remove a file entry at the given location, if it exists and
    /// report it as dropped.
    ///
    /// Return true if something was removed.
    pub(crate) fn drop<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        history: &mut WritableOpenHistory,
        dirty: &mut WritableOpenDirty,
        loc: L,
    ) -> Result<bool, StorageError> {
        let loc = loc.into();
        if let Some(inode) = tree.resolve(loc)? {
            let FileTableEntry { path, hash, .. } = match self.table.get(inode)? {
                None => {
                    // Nothing to do
                    return Ok(false);
                }
                Some(existing) => existing.value().parse()?,
            };
            if tree.remove_and_decref(inode, &mut self.table, inode)? {
                dirty.mark_dirty(inode)?;
                history.report_dropped(&path, &hash)?;

                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Remove a tree location (path or inode) that can be a file or a
    /// directory.
    ///
    /// If the location is a directory, all files within that
    /// directory are removed, recursively.
    pub(crate) fn remove_file_or_dir<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        history: &mut WritableOpenHistory,
        dirty: &mut WritableOpenDirty,
        loc: L,
    ) -> Result<(), StorageError> {
        let loc = loc.into();
        if self.remove(tree, history, dirty, loc.borrow())? {
            return Ok(()); // it was a file
        }

        tree.remove_recursive_and_decref_checked(
            loc,
            &mut self.table,
            |inode| inode,
            |inode, v| {
                let FileTableEntry { path, hash, .. } = v.parse()?;
                dirty.mark_dirty(inode)?;
                history.report_removed(&path, &hash)?;

                Ok(true)
            },
        )?;

        Ok(())
    }

    /// Record that the given file and version has been outdated by
    /// `new_hash`.
    ///
    /// Does nothing if the file is missing or if its version is not
    /// `old_hash` or a version derived from it.
    pub(crate) fn record_outdated<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &impl TreeReadOperations,
        loc: L,
        old_hash: &Hash,
        new_hash: &Hash,
    ) -> Result<(), StorageError> {
        if let Some(inode) = tree.resolve(loc)? {
            let entry = match self.table.get(inode)? {
                None => None,
                Some(v) => Some(v.value().parse()?),
            };
            if let Some(mut entry) = entry
                && IndexedFile::from(&entry).is_outdated_by(old_hash)
            {
                // Just remember that a newer version exist in a
                // remote peer. This information is going to be used
                // to download that newer version later on.
                entry.outdated_by = Some(new_hash.clone());

                self.table.insert(inode, Holder::with_content(entry)?)?;
            }
        }

        Ok(())
    }
}

fn get_at_inode(
    index_table: &impl ReadableTable<Inode, Holder<'static, FileTableEntry>>,
    inode: Inode,
) -> Result<Option<IndexedFile>, StorageError> {
    match index_table.get(inode)? {
        None => Ok(None),
        Some(v) => Ok(Some(v.value().parse()?.into())),
    }
}

fn has_at_inode(
    index_table: &impl ReadableTable<Inode, Holder<'static, FileTableEntry>>,
    inode: Inode,
) -> Result<bool, StorageError> {
    Ok(index_table.get(inode)?.is_some())
}

fn all(
    index_table: &impl ReadableTable<Inode, Holder<'static, FileTableEntry>>,
    tx: mpsc::Sender<(Path, IndexedFile)>,
) -> Result<(), StorageError> {
    for entry in index_table.iter()? {
        let (_, v) = entry?;
        if let Ok(entry) = v.value().parse() {
            let file_path = entry.path.clone();
            let indexed_entry = IndexedFile::from(entry);
            if let Err(_) = tx.blocking_send((file_path, indexed_entry)) {
                break;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arena::db::ArenaDatabase;
    use crate::arena::dirty::DirtyReadOperations;
    use realize_types::Arena;
    use std::collections::HashSet;
    use std::sync::Arc;

    struct Fixture {
        db: Arc<ArenaDatabase>,
    }

    impl Fixture {
        fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();
            let arena = Arena::from("myarena");
            let db = ArenaDatabase::for_testing_single_arena(arena)?;

            Ok(Self { db })
        }
    }
    fn dirty_inodes(
        dirty: &impl crate::arena::dirty::DirtyReadOperations,
    ) -> Result<HashSet<Inode>, StorageError> {
        let mut start = 0;
        let mut ret = HashSet::new();
        while let Some((inode, counter)) = dirty.next_dirty(start)? {
            ret.insert(inode);
            start = counter + 1;
        }
        Ok(ret)
    }

    fn dirty_paths(
        dirty: &impl DirtyReadOperations,
        tree: &impl TreeReadOperations,
    ) -> Result<HashSet<Path>, StorageError> {
        Ok(dirty_inodes(dirty)?
            .into_iter()
            .filter_map(|i| tree.backtrack(i).ok())
            .collect())
    }

    fn collect_history_entries(
        history: &impl crate::arena::history::HistoryReadOperations,
    ) -> Result<Vec<HistoryTableEntry>, StorageError> {
        let (tx, mut rx) = mpsc::channel(100);

        // Collect history entries in a blocking manner
        history.history(0.., tx)?;

        let mut entries = Vec::new();
        while let Some(Ok((_, entry))) = rx.blocking_recv() {
            entries.push(entry);
        }

        Ok(entries)
    }

    #[test]
    fn read_txn_with_index_compiles() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_read()?;
        let index = txn.read_index()?;
        let tree = txn.read_tree()?;
        let path = Path::parse("test.txt")?;
        assert_eq!(None, index.get(&tree, &path)?);

        Ok(())
    }

    #[test]
    fn write_txn_with_index_compiles() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let index = txn.read_index()?;
        let tree = txn.read_tree()?;
        let path = Path::parse("test.txt")?;
        assert_eq!(None, index.get(&tree, &path)?);

        Ok(())
    }

    #[test]
    fn write_index_compiles() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut index = txn.write_index()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;
        let path = Path::parse("test.txt")?;
        index.add(
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
        let mut index = txn.write_index()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;
        index.add(
            &mut tree,
            &mut history,
            &mut dirty,
            &path,
            100,
            mtime,
            Hash([0xfa; 32]),
        )?;

        assert!(index.has(&tree, &path)?);
        let entry = index.get(&tree, &path)?.unwrap();
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
        let mut index = txn.write_index()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;
        index.add(
            &mut tree,
            &mut history,
            &mut dirty,
            &path,
            100,
            mtime1,
            Hash([0xfa; 32]),
        )?;
        index.add(
            &mut tree,
            &mut history,
            &mut dirty,
            &path,
            200,
            mtime2,
            Hash([0x07; 32]),
        )?;

        // Verify the file was replaced
        assert!(index.has(&tree, &path)?);
        let entry = index.get(&tree, &path)?.unwrap();
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
        let mut index = txn.write_index()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;
        index.add(
            &mut tree,
            &mut history,
            &mut dirty,
            &path,
            100,
            mtime,
            Hash([0xfa; 32]),
        )?;

        assert!(index.has(&tree, &path)?);
        assert!(!index.has(&tree, &Path::parse("bar.txt")?)?);
        assert!(!index.has(&tree, &Path::parse("other.txt")?)?);

        Ok(())
    }

    #[test]
    fn get_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let mtime = UnixTime::from_secs(1234567890);
        let path = Path::parse("foo/bar")?;
        let hash = Hash([0xfa; 32]);
        let txn = fixture.db.begin_write()?;
        let mut index = txn.write_index()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;
        index.add(
            &mut tree,
            &mut history,
            &mut dirty,
            &path,
            100,
            mtime,
            hash.clone(),
        )?;

        let entry = index.get(&tree, &path)?.unwrap();
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
        let mut index = txn.write_index()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        index.add(
            &mut tree,
            &mut history,
            &mut dirty,
            &path,
            100,
            mtime,
            Hash([0xfa; 32]),
        )?;

        index.remove_file_or_dir(&mut tree, &mut history, &mut dirty, &path)?;

        assert!(!index.has(&tree, &path)?);

        Ok(())
    }

    #[test]
    fn remove_dir() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let mtime = UnixTime::from_secs(1234567890);

        let txn = fixture.db.begin_write()?;
        let mut index = txn.write_index()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        index.add(
            &mut tree,
            &mut history,
            &mut dirty,
            Path::parse("foo/bar1.txt")?,
            100,
            mtime,
            Hash([0xfa; 32]),
        )?;
        index.add(
            &mut tree,
            &mut history,
            &mut dirty,
            Path::parse("foo/bar2.txt")?,
            100,
            mtime,
            Hash([0xfa; 32]),
        )?;

        index.remove_file_or_dir(&mut tree, &mut history, &mut dirty, Path::parse("foo")?)?;

        assert!(!index.has(&tree, Path::parse("foo/bar1.txt")?)?);
        assert!(!index.has(&tree, Path::parse("foo/bar2.txt")?)?);

        Ok(())
    }

    #[tokio::test]
    async fn all_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let mtime = UnixTime::from_secs(1234567890);
        let path1 = Path::parse("foo/a")?;
        let path2 = Path::parse("foo/b")?;
        let path3 = Path::parse("bar.txt")?;

        let txn = fixture.db.begin_write()?;
        {
            let mut index = txn.write_index()?;
            let mut tree = txn.write_tree()?;
            let mut history = txn.write_history()?;
            let mut dirty = txn.write_dirty()?;

            index.add(
                &mut tree,
                &mut history,
                &mut dirty,
                &path1,
                100,
                mtime,
                Hash([1; 32]),
            )?;
            index.add(
                &mut tree,
                &mut history,
                &mut dirty,
                &path2,
                200,
                mtime,
                Hash([2; 32]),
            )?;
            index.add(
                &mut tree,
                &mut history,
                &mut dirty,
                &path3,
                300,
                mtime,
                Hash([3; 32]),
            )?;
        }
        txn.commit()?;

        let (tx, mut rx) = mpsc::channel(10);
        let task = tokio::task::spawn_blocking({
            let db = fixture.db.clone();

            move || {
                let txn = db.begin_read()?;
                let index = txn.read_index()?;

                index.all(tx)
            }
        });

        let mut files = HashSet::new();
        while let Some((path, _)) = rx.recv().await {
            files.insert(path);
        }
        task.await??; // make sure there are no errors

        assert_eq!(
            HashSet::from([path1.clone(), path2.clone(), path3.clone()]),
            files
        );

        Ok(())
    }

    #[test]
    fn add_file_marks_dirty_and_adds_history() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let mtime = UnixTime::from_secs(1234567890);
        let path = Path::parse("foo/bar.txt")?;
        let hash = Hash([0xfa; 32]);

        let txn = fixture.db.begin_write()?;
        let mut index = txn.write_index()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        // Clear any existing dirty entries
        dirty.delete_range(0, 999)?;

        // Add a file
        index.add(
            &mut tree,
            &mut history,
            &mut dirty,
            &path,
            100,
            mtime,
            hash.clone(),
        )?;

        // Verify the file was added to the index
        assert!(index.has(&tree, &path)?);

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
        let mut index = txn.write_index()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        // Clear any existing dirty entries
        dirty.delete_range(0, 999)?;

        // Add initial file
        index.add(
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
        index.add(
            &mut tree,
            &mut history,
            &mut dirty,
            &path,
            200,
            mtime2,
            hash2.clone(),
        )?;

        // Verify the file was replaced in the index
        let entry = index.get(&tree, &path)?.unwrap();
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
        let mut index = txn.write_index()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        // Add a file first
        let inode = index.add(
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
        let removed = index.remove(&mut tree, &mut history, &mut dirty, &path)?;
        assert!(removed);

        // Verify the file was removed from the index
        assert!(!index.has(&tree, &path)?);

        // Verify the path was marked dirty
        assert_eq!(HashSet::from([inode]), dirty_inodes(&dirty)?);

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
        let mut index = txn.write_index()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        // Add files in a directory
        let path1 = Path::parse("foo/bar1.txt")?;
        let path2 = Path::parse("foo/bar2.txt")?;
        let hash1 = Hash([0xfa; 32]);
        let hash2 = Hash([0xfb; 32]);

        let inode1 = index.add(
            &mut tree,
            &mut history,
            &mut dirty,
            &path1,
            100,
            mtime,
            hash1.clone(),
        )?;
        let inode2 = index.add(
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
        index.remove_file_or_dir(&mut tree, &mut history, &mut dirty, Path::parse("foo")?)?;

        // Verify the files were removed from the index
        assert!(!index.has(&tree, &path1)?);
        assert!(!index.has(&tree, &path2)?);

        // Verify the paths were marked dirty
        assert_eq!(HashSet::from([inode1, inode2]), dirty_inodes(&dirty)?);

        // Verify history entries were added (2 Add + 2 Remove entries)
        let history_entries = collect_history_entries(&history)?;
        assert_eq!(history_entries.len(), 4);

        // Check that we have 2 Add entries
        let add_entries: Vec<_> = history_entries
            .iter()
            .filter_map(|entry| {
                if let HistoryTableEntry::Add(path) = entry {
                    Some(path)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(add_entries.len(), 2);
        assert!(add_entries.contains(&&path1));
        assert!(add_entries.contains(&&path2));

        // Check that we have 2 Remove entries
        let remove_entries: Vec<_> = history_entries
            .iter()
            .filter_map(|entry| {
                if let HistoryTableEntry::Remove(path, hash) = entry {
                    Some((path, hash))
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(remove_entries.len(), 2);
        assert!(remove_entries.contains(&(&path1, &hash1)));
        assert!(remove_entries.contains(&(&path2, &hash2)));

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
        let mut index = txn.write_index()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        // Add a file with the old hash
        index.add(
            &mut tree,
            &mut history,
            &mut dirty,
            &path,
            100,
            mtime,
            old_hash.clone(),
        )?;

        // Record that this version is outdated by the new hash
        index.record_outdated(&tree, &path, &old_hash, &new_hash)?;

        // Verify the entry was updated with outdated_by field
        let entry = index.get(&tree, &path)?.unwrap();
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
        let mut index = txn.write_index()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        // Add a file with file_hash
        index.add(
            &mut tree,
            &mut history,
            &mut dirty,
            &path,
            100,
            mtime,
            file_hash.clone(),
        )?;

        // Record that an unrelated hash is outdated by the new hash
        index.record_outdated(&tree, &path, &unrelated_hash, &new_hash)?;

        // Verify the entry was NOT updated (outdated_by should remain None)
        let entry = index.get(&tree, &path)?.unwrap();
        assert_eq!(entry.hash, file_hash);
        assert_eq!(entry.outdated_by, None);

        Ok(())
    }
}
