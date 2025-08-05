#![allow(dead_code)] // work in progress

use super::db::{ArenaDatabase, ArenaWriteTransaction};
use super::types::{HistoryTableEntry, IndexedFileTableEntry};
use crate::arena::engine::DirtyPaths;
use crate::utils::fs_utils;
use crate::utils::holder::{ByteConversionError, Holder};
use crate::{Notification, StorageError};
use realize_types::{self, Arena, Hash, UnixTime};
use redb::ReadableTable as _;
use std::ops::RangeBounds;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio::task;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

/// File hash index, blocking version.
pub struct RealIndexBlocking {
    db: Arc<ArenaDatabase>,
    arena: Arena,
    history_tx: watch::Sender<u64>,
    uuid: Uuid,
    dirty_paths: Arc<DirtyPaths>,

    /// This is just to keep history_tx alive and up-to-date.
    _history_rx: watch::Receiver<u64>,
}

impl RealIndexBlocking {
    pub fn new(
        arena: Arena,
        db: Arc<ArenaDatabase>,
        dirty_paths: Arc<DirtyPaths>,
    ) -> Result<Self, StorageError> {
        let index: u64;
        let uuid: Uuid;
        {
            let txn = db.begin_write()?;
            {
                txn.index_file_table()?;
                let history_table = txn.index_history_table()?;
                index = last_history_index(&history_table)?;
            }
            {
                let mut settings_table = txn.index_settings_table()?;
                if let Some(value) = settings_table.get("uuid")? {
                    let bytes: uuid::Bytes = value
                        .value()
                        .try_into()
                        .map_err(|_| ByteConversionError::Invalid("uuid"))?;
                    uuid = Uuid::from_bytes(bytes);
                } else {
                    uuid = Uuid::now_v7();
                    let bytes: &[u8] = uuid.as_bytes();
                    settings_table.insert("uuid", &bytes)?;
                }
            }
            txn.commit()?;
        }

        let (history_tx, history_rx) = watch::channel(index);

        Ok(Self {
            arena,
            db,
            uuid,
            history_tx,
            dirty_paths,
            _history_rx: history_rx,
        })
    }

    /// Returns the database UUID.
    ///
    /// A UUID is set when a new database is created.
    pub fn uuid(&self) -> &Uuid {
        &self.uuid
    }

    /// The arena tied to this index.
    pub fn arena(&self) -> Arena {
        self.arena
    }

    /// Subscribe to a watch channel reporting the highest history entry index.
    ///
    /// Entries can be later queried using [RealIndexBlocking::history].
    ///
    /// The value itself is also available as [RealIndexBlocking::last_history_index].
    pub fn watch_history(&self) -> watch::Receiver<u64> {
        self.history_tx.subscribe()
    }

    /// Index of the last history entry that was written.
    ///
    /// This is the value tracked by [RealIndexBlocking::watch_history].
    pub fn last_history_index(&self) -> Result<u64, StorageError> {
        let txn = self.db.begin_read()?;
        let history_table = txn.index_history_table()?;

        last_history_index(&history_table)
    }

    /// Turn this instance into an async one.
    pub fn into_async(self) -> RealIndexAsync {
        RealIndexAsync::new(self)
    }

    /// Get a file entry.
    pub fn get_file(
        &self,
        path: &realize_types::Path,
    ) -> Result<Option<IndexedFileTableEntry>, StorageError> {
        let txn = self.db.begin_read()?;
        get_file_entry(&txn, path)
    }

    /// Check whether a given file is in the index already.
    pub fn has_file(&self, path: &realize_types::Path) -> Result<bool, StorageError> {
        let txn = self.db.begin_read()?;
        let file_table = txn.index_file_table()?;

        Ok(file_table.get(path.as_str())?.is_some())
    }

    /// Check whether a given file is in the index with the given size and mtime.
    pub fn has_matching_file(
        &self,
        path: &realize_types::Path,
        size: u64,
        mtime: &UnixTime,
    ) -> Result<bool, StorageError> {
        Ok(self
            .get_file(path)?
            .map(|e| e.size == size && e.mtime == *mtime)
            .unwrap_or(false))
    }

    /// Add a file entry with the given values. Replace one if it exists.
    pub fn add_file(
        &self,
        path: &realize_types::Path,
        size: u64,
        mtime: &UnixTime,
        hash: Hash,
    ) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        self.do_add_file(&txn, path, size, mtime, hash)?;
        txn.commit()?;

        Ok(())
    }

    pub fn add_file_if_matches(
        &self,
        root: &std::path::Path,
        path: &realize_types::Path,
        size: u64,
        mtime: &UnixTime,
        hash: Hash,
    ) -> Result<bool, StorageError> {
        let txn = self.db.begin_write()?;
        let entry = self.do_add_file(&txn, path, size, mtime, hash)?;
        if file_matches_index(&entry, root, path) {
            txn.commit()?;
            return Ok(true);
        }

        Ok(false)
    }

    fn do_add_file(
        &self,
        txn: &ArenaWriteTransaction,
        path: &realize_types::Path,
        size: u64,
        mtime: &UnixTime,
        hash: Hash,
    ) -> Result<IndexedFileTableEntry, StorageError> {
        let mut file_table = txn.index_file_table()?;
        let mut history_table = txn.index_history_table()?;
        let old_hash = file_table
            .get(path.as_str())?
            .map(|e| e.value().parse().ok())
            .flatten()
            .map(|e| e.hash);
        let same_hash = old_hash.as_ref().map(|h| *h == hash).unwrap_or(false);
        let entry = IndexedFileTableEntry {
            size,
            mtime: mtime.clone(),
            hash,
            outdated_by: None,
        };
        file_table.insert(path.as_str(), Holder::new(&entry)?)?;
        if !same_hash {
            (&self.dirty_paths).mark_dirty(txn, path)?;
            let index = self.allocate_history_index(txn, &history_table)?;
            let ev = if let Some(old_hash) = old_hash {
                HistoryTableEntry::Replace(path.clone(), old_hash)
            } else {
                HistoryTableEntry::Add(path.clone())
            };
            log::debug!("[{}] History #{index}: {ev:?}", self.arena);
            history_table.insert(index, Holder::with_content(ev)?)?;
        }

        Ok(entry)
    }

    pub fn remove_file_if_missing(
        &self,
        root: &std::path::Path,
        path: &realize_types::Path,
    ) -> Result<bool, StorageError> {
        let txn = self.db.begin_write()?;
        if fs_utils::metadata_no_symlink_blocking(root, path).is_err() {
            {
                let mut file_table = txn.index_file_table()?;
                let removed = file_table.remove(path.as_str())?;
                if let Some(removed) = removed {
                    let old_hash = removed.value().parse()?.hash;
                    self.report_removed(
                        &txn,
                        &mut txn.index_history_table()?,
                        path.clone(),
                        old_hash,
                    )?;
                }
            }
            txn.commit()?;
            return Ok(true);
        }

        Ok(false)
    }

    /// Send all valid entries of the file table to the given channel.
    pub fn all_files(
        &self,
        tx: mpsc::Sender<(realize_types::Path, IndexedFileTableEntry)>,
    ) -> Result<(), StorageError> {
        let txn = self.db.begin_read()?;
        let file_table = txn.index_file_table()?;
        for (path, entry) in file_table
            .iter()?
            .flatten()
            // Skip any entry with errors
            .flat_map(|(k, v)| {
                if let (Ok(path), Ok(entry)) =
                    (realize_types::Path::parse(k.value()), v.value().parse())
                {
                    Some((path, entry))
                } else {
                    None
                }
            })
        {
            if let Err(_) = tx.blocking_send((path, entry)) {
                break;
            }
        }

        Ok(())
    }

    /// Grab a range of history entries.
    pub fn history(
        &self,
        range: impl RangeBounds<u64>,
        tx: mpsc::Sender<Result<(u64, HistoryTableEntry), StorageError>>,
    ) -> Result<(), StorageError> {
        let txn = self.db.begin_read()?;
        let history_table = txn.index_history_table()?;
        for res in history_table.range(range)?.map(|res| match res {
            Err(err) => Err(StorageError::from(err)),
            Ok((k, v)) => match v.value().parse() {
                Ok(v) => Ok((k.value(), v)),
                Err(err) => Err(StorageError::from(err)),
            },
        }) {
            if let Err(_) = tx.blocking_send(res) {
                break;
            }
        }

        Ok(())
    }

    /// Remove a path that can be a file or a directory.
    ///
    /// If the path is a directory, all files within that directory
    /// are removed, recursively.
    pub fn remove_file_or_dir(&self, path: &realize_types::Path) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut file_table = txn.index_file_table()?;
            let mut history_table = txn.index_history_table()?;
            let path_prefix = PathPrefix::new(&path);

            for entry in
                file_table.extract_from_if(path_prefix.range(), |k, _| path_prefix.accept(k))?
            {
                let (k, v) = entry?;
                let path = realize_types::Path::parse(k.value())?;
                let hash = v.value().parse()?.hash;
                self.report_removed(&txn, &mut history_table, path, hash)?;
            }
        }
        txn.commit()?;

        Ok(())
    }

    fn report_removed(
        &self,
        txn: &ArenaWriteTransaction,
        history_table: &mut redb::Table<'_, u64, Holder<'static, HistoryTableEntry>>,
        path: realize_types::Path,
        hash: Hash,
    ) -> Result<(), StorageError> {
        let index = self.allocate_history_index(txn, history_table)?;
        (&self.dirty_paths).mark_dirty(txn, &path)?;
        let ev = HistoryTableEntry::Remove(path, hash);
        log::debug!("[{}] History #{index}: {ev:?}", self.arena);
        history_table.insert(index, Holder::with_content(ev)?)?;
        Ok(())
    }

    /// Remove `path` from the index if the hash and file match,
    /// report it as a drop in the history.
    pub fn drop_file_if_matches(
        &self,
        txn: &ArenaWriteTransaction,
        root: &std::path::Path,
        path: &realize_types::Path,
        hash: &Hash,
    ) -> Result<bool, StorageError> {
        let mut file_table = txn.index_file_table()?;
        let mut history_table = txn.index_history_table()?;

        if let Some(entry) = do_get_file_entry(&file_table, path)?
            && entry.hash == *hash
            && file_matches_index(&entry, root, path)
        {
            file_table.remove(path.as_str())?;

            let index = self.allocate_history_index(&txn, &history_table)?;
            (&self.dirty_paths).mark_dirty(&txn, &path)?;
            let ev = HistoryTableEntry::Drop(path.clone(), hash.clone());
            log::debug!("[{}] History #{index}: {ev:?}", self.arena);
            history_table.insert(index, Holder::with_content(ev)?)?;
            return Ok(true);
        }

        Ok(false)
    }

    fn allocate_history_index(
        &self,
        txn: &ArenaWriteTransaction,
        history_table: &impl redb::ReadableTable<u64, Holder<'static, HistoryTableEntry>>,
    ) -> Result<u64, StorageError> {
        let entry_index = 1 + last_history_index(history_table)?;

        let history_tx = self.history_tx.clone();
        txn.after_commit(move || {
            let _ = history_tx.send(entry_index);
        });
        Ok(entry_index)
    }

    /// Take a remote change into account, if it applies to a file in
    /// the index.
    pub(crate) fn update(
        &self,
        notification: &Notification,
        root: &std::path::Path,
    ) -> Result<(), StorageError> {
        if notification.arena() != self.arena {
            return Ok(());
        }

        match notification {
            Notification::Replace {
                path,
                hash,
                old_hash,
                ..
            } => {
                let txn = self.db.begin_write()?;
                {
                    let mut file_table = txn.index_file_table()?;
                    if let Some(mut entry) = do_get_file_entry(&file_table, path)?
                        && replaces(&entry, old_hash)
                    {
                        // Just remember that a newer version exist in
                        // a remote peer. This information is going to
                        // be used to download that newer version later on.
                        entry.outdated_by = Some(hash.clone());
                        file_table.insert(path.as_str(), Holder::with_content(entry)?)?;
                    }
                }
                txn.commit()?;
            }
            Notification::Remove { path, old_hash, .. } => {
                let txn = self.db.begin_read()?;
                if let Some(entry) = get_file_entry(&txn, path)?
                    && replaces(&entry, old_hash)
                {
                    // This specific version has been removed
                    // remotely. Make sure that the file hasn't
                    // changed since it was indexed and if it hasn't,
                    // remove it locally as well.
                    if file_matches_index(&entry, root, path) {
                        std::fs::remove_file(&path.within(root))?;
                    }
                }
            }
            _ => {}
        };

        Ok(())
    }
}

pub(crate) fn get_indexed_file(
    txn: &ArenaWriteTransaction,
    root: &std::path::Path,
    path: &realize_types::Path,
    hash: Option<&Hash>,
) -> Result<Option<std::path::PathBuf>, StorageError> {
    let file_table = txn.index_file_table()?;
    match hash {
        Some(hash) => {
            if let Some(entry) = do_get_file_entry(&file_table, path)?
                && entry.hash == *hash
                && file_matches_index(&entry, root, path)
            {
                return Ok(Some(path.within(root)));
            }
        }
        None => {
            if !file_table.get(path.as_str())?.is_some()
                && fs_utils::metadata_no_symlink_blocking(root, path).is_err()
            {
                return Ok(Some(path.within(root)));
            }
        }
    }

    Ok(None)
}

fn file_matches_index(
    entry: &IndexedFileTableEntry,
    root: &std::path::Path,
    path: &realize_types::Path,
) -> bool {
    if let Ok(m) = fs_utils::metadata_no_symlink_blocking(root, path) {
        UnixTime::mtime(&m) == entry.mtime && m.len() == entry.size
    } else {
        false
    }
}

/// Check whether replacing `old_hash` replaces `entry`
fn replaces(entry: &IndexedFileTableEntry, old_hash: &Hash) -> bool {
    entry.hash == *old_hash
        || entry
            .outdated_by
            .as_ref()
            .map(|h| *h == *old_hash)
            .unwrap_or(false)
}

pub(crate) fn get_file_entry(
    txn: &super::db::ArenaReadTransaction,
    path: &realize_types::Path,
) -> Result<Option<IndexedFileTableEntry>, StorageError> {
    let file_table = txn.index_file_table()?;

    do_get_file_entry(&file_table, path)
}

pub(crate) fn has_file_entry(
    txn: &super::db::ArenaReadTransaction,
    path: &realize_types::Path,
) -> Result<bool, StorageError> {
    let file_table = txn.index_file_table()?;

    Ok(file_table.get(path.as_str())?.is_some())
}

fn do_get_file_entry(
    file_table: &impl redb::ReadableTable<&'static str, Holder<'static, IndexedFileTableEntry>>,
    path: &realize_types::Path,
) -> Result<Option<IndexedFileTableEntry>, StorageError> {
    if let Some(entry) = file_table.get(path.as_str())? {
        return Ok(Some(entry.value().parse()?));
    }

    Ok(None)
}

/// Helper for iterating over a range of paths.
///
/// First query the returned range, then check any result against
/// accept.
struct PathPrefix {
    range_end: String,
}
impl PathPrefix {
    fn new(prefix: &realize_types::Path) -> Self {
        let mut range_end = prefix.to_string();
        range_end.push('0' /* '/' + 1 */);

        Self { range_end }
    }

    fn prefix(&self) -> &str {
        &self.range_end[0..(self.range_end.len() - 1)]
    }

    fn range(&self) -> std::ops::Range<&str> {
        let start = self.prefix();
        let end = self.range_end.as_str();

        start..end
    }

    fn accept(&self, path: &str) -> bool {
        path.strip_prefix(self.prefix())
            .map(|rest| rest == "" || rest.starts_with('/'))
            .unwrap_or(false)
    }
}

fn last_history_index(
    history_table: &impl redb::ReadableTable<u64, Holder<'static, HistoryTableEntry>>,
) -> Result<u64, StorageError> {
    Ok(history_table.last()?.map(|(k, _)| k.value()).unwrap_or(0))
}

/// File hash index, async version.
#[derive(Clone)]
pub struct RealIndexAsync {
    inner: Arc<RealIndexBlocking>,
}

impl RealIndexAsync {
    pub fn new(inner: RealIndexBlocking) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Create an index using the given database. Initialize the database if necessary.
    pub async fn with_db(
        arena: Arena,
        db: Arc<ArenaDatabase>,
        dirty_paths: Arc<DirtyPaths>,
    ) -> Result<Self, StorageError> {
        task::spawn_blocking(move || {
            Ok(RealIndexAsync::new(RealIndexBlocking::new(
                arena,
                db,
                dirty_paths,
            )?))
        })
        .await?
    }

    /// Returns the database UUID.
    ///
    /// A UUID is set when a new database is created.
    pub fn uuid(&self) -> &Uuid {
        &self.inner.uuid
    }

    /// The arena tied to this index.
    pub fn arena(&self) -> Arena {
        self.inner.arena
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
    pub fn blocking(&self) -> Arc<RealIndexBlocking> {
        Arc::clone(&self.inner)
    }

    /// Return all valid file entries as a stream.
    pub fn all_files(&self) -> ReceiverStream<(realize_types::Path, IndexedFileTableEntry)> {
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
            if let Err(err) = inner.history(*range, tx) {
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
    ) -> Result<Option<IndexedFileTableEntry>, StorageError> {
        let inner = Arc::clone(&self.inner);
        let path = path.clone();

        task::spawn_blocking(move || inner.get_file(&path)).await?
    }

    /// Check whether a given file is in the index already.
    pub async fn has_file(&self, path: &realize_types::Path) -> Result<bool, StorageError> {
        let inner = Arc::clone(&self.inner);
        let path = path.clone();

        task::spawn_blocking(move || inner.has_file(&path)).await?
    }

    /// Check whether a given file is in the index already with the given size and mtime.
    pub async fn has_matching_file(
        &self,
        path: &realize_types::Path,
        size: u64,
        mtime: &UnixTime,
    ) -> Result<bool, StorageError> {
        let inner = Arc::clone(&self.inner);
        let path = path.clone();
        let mtime = mtime.clone();

        task::spawn_blocking(move || inner.has_matching_file(&path, size, &mtime)).await?
    }

    /// Remove a path that can be a file or a directory.
    ///
    /// If the path is a directory, all files within that directory
    /// are removed, recursively.
    pub async fn remove_file_or_dir(&self, path: &realize_types::Path) -> Result<(), StorageError> {
        let inner = Arc::clone(&self.inner);
        let path = path.clone();

        task::spawn_blocking(move || inner.remove_file_or_dir(&path)).await?
    }

    pub async fn add_file(
        &self,
        path: &realize_types::Path,
        size: u64,
        mtime: &UnixTime,
        hash: Hash,
    ) -> Result<(), StorageError> {
        let inner = Arc::clone(&self.inner);
        let path = path.clone();
        let mtime = mtime.clone();

        task::spawn_blocking(move || inner.add_file(&path, size, &mtime, hash)).await?
    }

    pub async fn add_file_if_matches(
        &self,
        root: &std::path::Path,
        path: &realize_types::Path,
        size: u64,
        mtime: &UnixTime,
        hash: Hash,
    ) -> Result<bool, StorageError> {
        let inner = Arc::clone(&self.inner);
        let path = path.clone();
        let mtime = mtime.clone();
        let root = root.to_path_buf();

        task::spawn_blocking(move || inner.add_file_if_matches(&root, &path, size, &mtime, hash))
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

    /// Take a remote change into account, if it applies to a file in
    /// the index.
    pub(crate) async fn update(
        &self,
        notification: Notification,
        root: &std::path::Path,
    ) -> Result<(), StorageError> {
        let inner = Arc::clone(&self.inner);
        let root = root.to_path_buf();

        task::spawn_blocking(move || inner.update(&notification, &root)).await?
    }
}

/// Mark paths within the index dirty.
///
/// If the path is a file in the index, it is marked dirty.
//
/// If there exists files within the index that are inside that
/// directory, directly or indirectly, they're marked dirty.
///
/// If the path is not in the index, the function does nothing.
pub(crate) fn mark_dirty_recursive(
    txn: &ArenaWriteTransaction,
    path: &realize_types::Path,
    dirty_paths: &Arc<DirtyPaths>,
) -> Result<(), StorageError> {
    let file_table = txn.index_file_table()?;
    let path_prefix = PathPrefix::new(&path);
    for entry in file_table.range(path_prefix.range())? {
        let (k, _) = entry?;
        let key = k.value();
        if !path_prefix.accept(key) {
            continue;
        }
        if let Ok(path) = realize_types::Path::parse(key) {
            dirty_paths.mark_dirty(txn, &path)?;
        }
    }

    Ok(())
}

/// Mark all files within the index dirty
pub(crate) fn make_all_dirty(
    txn: &ArenaWriteTransaction,
    dirty_paths: &Arc<DirtyPaths>,
) -> Result<(), StorageError> {
    let file_table = txn.index_file_table()?;
    for entry in file_table.iter()? {
        let (k, _) = entry?;
        if let Ok(path) = realize_types::Path::parse(k.value()) {
            dirty_paths.mark_dirty(txn, &path)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::utils::redb_utils;
    use crate::{arena::engine, utils::hash};
    use assert_fs::TempDir;
    use assert_fs::fixture::ChildPath;
    use assert_fs::prelude::*;
    use futures::{StreamExt as _, TryStreamExt as _};

    fn test_arena() -> Arena {
        Arena::from("arena")
    }

    struct Fixture {
        aindex: RealIndexAsync,
        index: Arc<RealIndexBlocking>,
        dirty_paths: Arc<DirtyPaths>,
        root: ChildPath,
        _tempdir: TempDir,
    }
    impl Fixture {
        async fn setup() -> anyhow::Result<Fixture> {
            let _ = env_logger::try_init();
            let arena = test_arena();
            let db = ArenaDatabase::new(redb_utils::in_memory()?)?;
            let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;
            let aindex = RealIndexBlocking::new(arena, db, Arc::clone(&dirty_paths))?.into_async();
            let tempdir = TempDir::new()?;
            let root = tempdir.child("root");
            root.create_dir_all()?;

            Ok(Self {
                index: aindex.blocking(),
                aindex,
                dirty_paths,
                root,
                _tempdir: tempdir,
            })
        }

        fn clear_all_dirty(&self) -> anyhow::Result<()> {
            let txn = self.index.db.begin_write()?;
            while engine::take_dirty(&txn)?.is_some() {}
            txn.commit()?;

            Ok(())
        }

        fn add_file_with_content(
            &self,
            path_str: &str,
            content: &str,
        ) -> anyhow::Result<(realize_types::Path, Hash)> {
            let path = realize_types::Path::parse(path_str)?;
            let hash = hash::digest(content);
            let child = self.root.child(path_str);
            child.write_str(content)?;
            let m = std::fs::metadata(child.path())?;
            self.index.add_file(
                &path,
                content.len() as u64,
                &UnixTime::mtime(&m),
                hash.clone(),
            )?;

            Ok((path, hash))
        }
    }

    #[tokio::test]
    async fn reopen_keeps_uuid() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;
        let path = tempdir.path().join("index.db");
        let db = ArenaDatabase::new(redb::Database::create(&path)?)?;
        let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;
        let arena = test_arena();
        let uuid = RealIndexBlocking::new(arena, db, dirty_paths)?
            .uuid()
            .clone();

        let db = ArenaDatabase::new(redb::Database::create(&path)?)?;
        let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;
        assert!(!uuid.is_nil());
        assert_eq!(
            uuid,
            RealIndexBlocking::new(arena, db, dirty_paths)?
                .uuid()
                .clone()
        );

        Ok(())
    }
    #[tokio::test]
    async fn add_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let index = &fixture.index;
        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar.txt")?;
        index.add_file(&path, 100, &mtime, Hash([0xfa; 32]))?;

        {
            let txn = index.db.begin_read()?;
            let file_table = txn.index_file_table()?;
            assert_eq!(
                IndexedFileTableEntry {
                    size: 100,
                    mtime,
                    hash: Hash([0xfa; 32]),
                    outdated_by: None,
                },
                file_table.get("foo/bar.txt")?.unwrap().value().parse()?
            );

            let history_table = txn.index_history_table()?;
            assert_eq!(
                HistoryTableEntry::Add(path.clone()),
                history_table.get(1)?.unwrap().value().parse()?
            );

            assert!(engine::is_dirty(&txn, &path)?);
        }

        Ok(())
    }

    #[tokio::test]
    async fn add_file_if_matches_success() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let index = &fixture.index;

        let foo = fixture.root.child("foo");
        foo.write_str("foo")?;
        assert!(index.add_file_if_matches(
            fixture.root.path(),
            &realize_types::Path::parse("foo")?,
            3,
            &UnixTime::mtime(&foo.path().metadata()?),
            hash::digest("foo"),
        )?);
        assert!(index.has_file(&realize_types::Path::parse("foo")?)?);

        Ok(())
    }

    #[tokio::test]
    async fn add_file_if_matches_time_mismatch() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let index = &fixture.index;

        let foo = fixture.root.child("foo");
        foo.write_str("foo")?;
        assert!(!index.add_file_if_matches(
            fixture.root.path(),
            &realize_types::Path::parse("foo")?,
            3,
            &UnixTime::from_secs(1234567890),
            hash::digest("foo"),
        )?);
        assert!(!index.has_file(&realize_types::Path::parse("foo")?)?);

        Ok(())
    }

    #[tokio::test]
    async fn add_file_if_matches_size_mismatch() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let index = &fixture.index;

        let foo = fixture.root.child("foo");
        foo.write_str("foo")?;
        assert!(!index.add_file_if_matches(
            fixture.root.path(),
            &realize_types::Path::parse("foo")?,
            2,
            &UnixTime::mtime(&foo.path().metadata()?),
            hash::digest("foo"),
        )?);
        assert!(!index.has_file(&realize_types::Path::parse("foo")?)?);

        Ok(())
    }

    #[tokio::test]
    async fn add_file_if_matches_missing() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let index = &fixture.index;

        let foo = fixture.root.child("foo");
        foo.write_str("foo")?;
        let mtime = UnixTime::mtime(&foo.path().metadata()?);
        fs::remove_file(foo.path())?;
        assert!(!index.add_file_if_matches(
            fixture.root.path(),
            &realize_types::Path::parse("foo")?,
            3,
            &mtime,
            hash::digest("foo"),
        )?);
        assert!(!index.has_file(&realize_types::Path::parse("foo")?)?);

        Ok(())
    }

    #[tokio::test]
    async fn replace_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let index = &fixture.index;
        let mtime1 = UnixTime::from_secs(1234567890);
        let mtime2 = UnixTime::from_secs(1234567891);
        let path = realize_types::Path::parse("foo/bar.txt")?;
        index.add_file(&path, 100, &mtime1, Hash([0xfa; 32]))?;
        index.add_file(&path, 200, &mtime2, Hash([0x07; 32]))?;

        {
            let txn = index.db.begin_read()?;
            let file_table = txn.index_file_table()?;
            assert_eq!(
                IndexedFileTableEntry {
                    size: 200,
                    mtime: mtime2,
                    hash: Hash([0x07; 32]),
                    outdated_by: None,
                },
                file_table.get("foo/bar.txt")?.unwrap().value().parse()?
            );

            let history_table = txn.index_history_table()?;
            assert_eq!(
                HistoryTableEntry::Replace(path.clone(), Hash([0xfa; 32])),
                history_table.get(2)?.unwrap().value().parse()?
            );

            assert!(engine::is_dirty(&txn, &path)?);
        }

        Ok(())
    }

    #[tokio::test]
    async fn has_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let index = &fixture.index;
        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar")?;
        index.add_file(&path, 100, &mtime, Hash([0xfa; 32]))?;

        assert_eq!(true, fixture.index.has_file(&path)?);
        assert_eq!(
            false,
            index.has_file(&realize_types::Path::parse("foo/bar/toto")?)?
        );
        assert_eq!(
            false,
            index.has_file(&realize_types::Path::parse("other.txt")?)?
        );

        Ok(())
    }

    #[tokio::test]
    async fn get_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let index = &fixture.index;
        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar")?;
        let hash = Hash([0xfa; 32]);
        index.add_file(&path, 100, &mtime, hash.clone())?;

        assert_eq!(
            Some(IndexedFileTableEntry {
                size: 100,
                mtime: mtime.clone(),
                hash: hash.clone(),
                outdated_by: None,
            }),
            fixture.index.get_file(&path)?
        );

        Ok(())
    }

    #[tokio::test]
    async fn has_matching_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let index = &fixture.index;
        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar")?;
        index.add_file(&path, 100, &mtime, Hash([0xfa; 32]))?;

        assert_eq!(true, fixture.index.has_matching_file(&path, 100, &mtime)?);
        assert_eq!(
            false,
            index.has_matching_file(&realize_types::Path::parse("other")?, 100, &mtime)?
        );
        assert_eq!(false, fixture.index.has_matching_file(&path, 200, &mtime)?);
        assert_eq!(
            false,
            index.has_matching_file(&path, 100, &UnixTime::from_secs(1234567891))?
        );

        Ok(())
    }

    #[tokio::test]
    async fn remove_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let index = &fixture.index;
        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar.txt")?;
        index.add_file(&path, 100, &mtime, Hash([0xfa; 32]))?;
        index.remove_file_or_dir(&path)?;

        assert_eq!(false, index.has_file(&path)?);
        {
            let txn = index.db.begin_read()?;
            let history_table = txn.index_history_table()?;
            assert_eq!(
                HistoryTableEntry::Remove(path.clone(), Hash([0xfa; 32])),
                history_table.get(2)?.unwrap().value().parse()?
            );

            assert!(engine::is_dirty(&txn, &path)?);
        }

        Ok(())
    }

    #[tokio::test]
    async fn remove_file_if_missing_success() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let index = &fixture.index;
        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("bar.txt")?;
        index.add_file(&path, 100, &mtime, Hash([0xfa; 32]))?;
        assert!(index.remove_file_if_missing(&fixture.root, &path)?);

        assert_eq!(false, index.has_file(&path)?);
        assert_eq!(2, index.last_history_index()?);

        Ok(())
    }

    #[tokio::test]
    async fn remove_file_if_missing_failure() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let index = &fixture.index;
        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("bar.txt")?;
        index.add_file(&path, 100, &mtime, Hash([0xfa; 32]))?;

        let realpath = path.within(&fixture.root);
        fs::write(&realpath, "foo")?;

        assert!(!index.remove_file_if_missing(&fixture.root, &path)?);

        assert_eq!(true, index.has_file(&path)?);
        assert_eq!(1, index.last_history_index()?);

        Ok(())
    }

    #[tokio::test]
    async fn remove_dir() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let index = &fixture.index;
        let mtime = UnixTime::from_secs(1234567890);

        index.add_file(
            &realize_types::Path::parse("foo/a")?,
            100,
            &mtime,
            Hash([1; 32]),
        )?;
        index.add_file(
            &realize_types::Path::parse("foo/b")?,
            100,
            &mtime,
            Hash([2; 32]),
        )?;
        index.add_file(
            &realize_types::Path::parse("foo/c")?,
            100,
            &mtime,
            Hash([3; 32]),
        )?;
        index.add_file(
            &realize_types::Path::parse("foobar")?,
            100,
            &mtime,
            Hash([0x04; 32]),
        )?;

        fixture.clear_all_dirty()?;

        index.remove_file_or_dir(&realize_types::Path::parse("foo")?)?;

        assert_eq!(
            true,
            index.has_file(&realize_types::Path::parse("foobar")?)?
        );
        assert_eq!(
            false,
            index.has_file(&realize_types::Path::parse("foo/a")?)?
        );
        assert_eq!(
            false,
            index.has_file(&realize_types::Path::parse("foo/b")?)?
        );
        assert_eq!(
            false,
            index.has_file(&realize_types::Path::parse("foo/c")?)?
        );

        {
            let txn = index.db.begin_read()?;
            let history_table = txn.index_history_table()?;
            assert_eq!(
                HistoryTableEntry::Remove(realize_types::Path::parse("foo/a")?, Hash([1; 32])),
                history_table.get(5)?.unwrap().value().parse()?
            );
            assert_eq!(
                HistoryTableEntry::Remove(realize_types::Path::parse("foo/b")?, Hash([2; 32])),
                history_table.get(6)?.unwrap().value().parse()?
            );
            assert_eq!(
                HistoryTableEntry::Remove(realize_types::Path::parse("foo/c")?, Hash([3; 32])),
                history_table.get(7)?.unwrap().value().parse()?
            );

            assert!(engine::is_dirty(
                &txn,
                &realize_types::Path::parse("foo/a")?
            )?);
            assert!(engine::is_dirty(
                &txn,
                &realize_types::Path::parse("foo/b")?
            )?);
            assert!(engine::is_dirty(
                &txn,
                &realize_types::Path::parse("foo/c")?
            )?);
        }

        Ok(())
    }

    #[tokio::test]
    async fn remove_nothing() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let index = &fixture.index;
        index.remove_file_or_dir(&realize_types::Path::parse("foo")?)?;

        {
            let txn = index.db.begin_read()?;
            let history_table = txn.index_history_table()?;
            assert!(history_table.last()?.is_none());
        }

        Ok(())
    }

    #[tokio::test]
    async fn all_files_stream() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let index = &fixture.aindex;
        let mtime = UnixTime::from_secs(1234567890);
        let hash = Hash([1; 32]);
        index
            .add_file(
                &realize_types::Path::parse("baa.txt")?,
                100,
                &mtime,
                hash.clone(),
            )
            .await?;
        index
            .add_file(
                &realize_types::Path::parse("baa/baa.txt")?,
                200,
                &mtime,
                hash.clone(),
            )
            .await?;
        index
            .add_file(
                &realize_types::Path::parse("baa/baa/black/sheep.txt")?,
                300,
                &mtime,
                hash.clone(),
            )
            .await?;

        let files = index
            .all_files()
            .map(|(p, entry)| (p.to_string(), entry.size))
            .collect::<Vec<(String, u64)>>()
            .await;

        assert_unordered::assert_eq_unordered!(
            vec![
                ("baa.txt".to_string(), 100),
                ("baa/baa.txt".to_string(), 200),
                ("baa/baa/black/sheep.txt".to_string(), 300)
            ],
            files
        );

        Ok(())
    }

    #[tokio::test]
    async fn last_history_index() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let index = &fixture.index;
        assert_eq!(0, index.last_history_index()?);

        let mtime = UnixTime::from_secs(1234567890);
        index.add_file(
            &realize_types::Path::parse("foo/a")?,
            100,
            &mtime,
            Hash([1; 32]),
        )?;
        index.add_file(
            &realize_types::Path::parse("foo/b")?,
            100,
            &mtime,
            Hash([2; 32]),
        )?;
        index.add_file(
            &realize_types::Path::parse("foo/c")?,
            100,
            &mtime,
            Hash([3; 32]),
        )?;
        assert_eq!(3, index.last_history_index()?);

        index.remove_file_or_dir(&realize_types::Path::parse("foo")?)?;
        assert_eq!(6, index.last_history_index()?);

        Ok(())
    }

    #[tokio::test]
    async fn history_stream() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let index = &fixture.index;

        let mtime = UnixTime::from_secs(1234567890);
        let foo_a = realize_types::Path::parse("foo/a")?;
        let foo_b = realize_types::Path::parse("foo/b")?;
        let foo_c = realize_types::Path::parse("foo/c")?;
        index.add_file(&foo_a, 100, &mtime, Hash([1; 32]))?;
        index.add_file(&foo_b, 100, &mtime, Hash([2; 32]))?;
        index.add_file(&foo_c, 100, &mtime, Hash([3; 32]))?;
        index.remove_file_or_dir(&realize_types::Path::parse("foo")?)?;

        let all = fixture.aindex.history(0..).try_collect::<Vec<_>>().await?;
        assert_eq!(
            vec![
                (1, HistoryTableEntry::Add(foo_a.clone())),
                (2, HistoryTableEntry::Add(foo_b.clone())),
                (3, HistoryTableEntry::Add(foo_c.clone())),
                (4, HistoryTableEntry::Remove(foo_a.clone(), Hash([1; 32]))),
                (5, HistoryTableEntry::Remove(foo_b.clone(), Hash([2; 32]))),
                (6, HistoryTableEntry::Remove(foo_c.clone(), Hash([3; 32]))),
            ],
            all
        );

        let add = fixture.aindex.history(1..4).try_collect::<Vec<_>>().await?;
        assert_eq!(
            vec![
                (1, HistoryTableEntry::Add(foo_a.clone())),
                (2, HistoryTableEntry::Add(foo_b.clone())),
                (3, HistoryTableEntry::Add(foo_c.clone())),
            ],
            add
        );

        let remove = fixture.aindex.history(4..).try_collect::<Vec<_>>().await?;
        assert_eq!(
            vec![
                (4, HistoryTableEntry::Remove(foo_a.clone(), Hash([1; 32]))),
                (5, HistoryTableEntry::Remove(foo_b.clone(), Hash([2; 32]))),
                (6, HistoryTableEntry::Remove(foo_c.clone(), Hash([3; 32]))),
            ],
            remove
        );

        Ok(())
    }

    #[tokio::test]
    async fn watch_history() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let index = &fixture.index;
        let mut history_rx = index.watch_history();
        assert_eq!(0, *history_rx.borrow_and_update());

        let mtime = UnixTime::from_secs(1234567890);
        index.add_file(
            &realize_types::Path::parse("foo/a")?,
            100,
            &mtime,
            Hash([1; 32]),
        )?;
        assert_eq!(true, history_rx.has_changed()?);

        index.add_file(
            &realize_types::Path::parse("foo/b")?,
            100,
            &mtime,
            Hash([2; 32]),
        )?;
        index.add_file(
            &realize_types::Path::parse("foo/c")?,
            100,
            &mtime,
            Hash([3; 32]),
        )?;
        assert_eq!(3, *history_rx.wait_for(|v| *v >= 3).await?);

        index.remove_file_or_dir(&realize_types::Path::parse("foo")?)?;
        history_rx.changed().await?;
        assert_eq!(6, *history_rx.borrow_and_update());

        Ok(())
    }

    #[tokio::test]
    async fn watch_history_later_on() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;

        let mtime = UnixTime::from_secs(1234567890);
        index.add_file(
            &realize_types::Path::parse("foo/a")?,
            100,
            &mtime,
            Hash([1; 32]),
        )?;
        index.add_file(
            &realize_types::Path::parse("foo/b")?,
            100,
            &mtime,
            Hash([2; 32]),
        )?;
        index.add_file(
            &realize_types::Path::parse("foo/c")?,
            100,
            &mtime,
            Hash([3; 32]),
        )?;

        let mut history_rx = index.watch_history();
        assert_eq!(3, *history_rx.borrow_and_update());

        Ok(())
    }
    #[tokio::test]
    async fn touch_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let index = &fixture.index;
        let mtime1 = UnixTime::from_secs(1234567890);
        let mtime2 = UnixTime::from_secs(1234567891);
        let path = realize_types::Path::parse("foo/bar.txt")?;
        index.add_file(&path, 100, &mtime1, Hash([0xfa; 32]))?;
        let hist_entry_count = index.last_history_index()?;
        index.add_file(&path, 100, &mtime2, Hash([0xfa; 32]))?;

        // No new history entry should have been added, since the file didn't really change.
        assert_eq!(hist_entry_count, index.last_history_index()?);

        // The new mtime should have been stored.
        assert!(index.has_matching_file(&path, 100, &mtime2)?);

        Ok(())
    }

    #[tokio::test]
    async fn mark_dirty_recursive_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;
        let mtime = UnixTime::from_secs(1234567890);

        // Add a single file
        let path = realize_types::Path::parse("foo/bar.txt")?;
        index.add_file(&path, 100, &mtime, Hash([0xfa; 32]))?;

        fixture.clear_all_dirty()?;

        // Mark the file dirty recursively
        {
            let txn = index.db.begin_write()?;
            mark_dirty_recursive(&txn, &path, &fixture.dirty_paths)?;
            txn.commit()?;
        }

        // Check that the file is marked dirty
        {
            let txn = index.db.begin_read()?;
            assert!(engine::is_dirty(&txn, &path)?);
        }

        Ok(())
    }

    #[tokio::test]
    async fn mark_dirty_recursive_directory() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;
        let mtime = UnixTime::from_secs(1234567890);

        let foo_a = realize_types::Path::parse("foo/a.txt")?;
        let foo_b = realize_types::Path::parse("foo/b.txt")?;
        let foo_c = realize_types::Path::parse("foo/subdir/c.txt")?;
        let foo_d = realize_types::Path::parse("foo/subdir/d.txt")?;
        let foo_file = realize_types::Path::parse("foo.txt")?;
        let foodie = realize_types::Path::parse("foodie/e.txt")?;
        let bar = realize_types::Path::parse("bar/f.txt")?;

        // Add files in a directory structure
        let files = vec![&foo_a, &foo_b, &foo_c, &foo_d, &foo_file, &foodie, &bar];
        for file in files {
            index.add_file(file, 100, &mtime, Hash([0xfa; 32]))?;
        }

        fixture.clear_all_dirty()?;

        // Mark the foo directory dirty recursively
        {
            let txn = index.db.begin_write()?;
            let foo_dir = realize_types::Path::parse("foo")?;
            mark_dirty_recursive(&txn, &foo_dir, &fixture.dirty_paths)?;
            txn.commit()?;
        }

        // Check that all files under foo are marked dirty
        {
            let txn = index.db.begin_read()?;
            assert!(engine::is_dirty(&txn, &foo_a)?);
            assert!(engine::is_dirty(&txn, &foo_b)?);
            assert!(engine::is_dirty(&txn, &foo_c)?);
            assert!(engine::is_dirty(&txn, &foo_d)?);

            // Files outside foo should not be dirty (even if their
            // name start with foo)
            assert!(!engine::is_dirty(&txn, &foo_file)?);
            assert!(!engine::is_dirty(&txn, &foodie)?);
            assert!(!engine::is_dirty(&txn, &bar)?);
        }

        Ok(())
    }

    #[tokio::test]
    async fn mark_dirty_recursive_nonexistent_path() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;

        // Mark a non-existent path dirty recursively; it shouldn't fail.
        {
            let txn = index.db.begin_write()?;
            let nonexistent = realize_types::Path::parse("nonexistent")?;
            mark_dirty_recursive(&txn, &nonexistent, &fixture.dirty_paths)?;
            txn.commit()?;
        }

        // Check that no files are marked dirty (function should do nothing)
        {
            let txn = index.db.begin_write()?;
            assert!(engine::take_dirty(&txn)?.is_none());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_make_all_dirty() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;
        let mtime = UnixTime::from_secs(1234567890);

        // Add multiple files in different directories
        let files = vec![
            realize_types::Path::parse("foo/a.txt")?,
            realize_types::Path::parse("foo/b.txt")?,
            realize_types::Path::parse("bar/c.txt")?,
            realize_types::Path::parse("baz/d.txt")?,
            realize_types::Path::parse("deep/nested/file.txt")?,
        ];

        for file in &files {
            index.add_file(file, 100, &mtime, Hash([0xfa; 32]))?;
        }

        fixture.clear_all_dirty()?;

        // Mark all files dirty
        {
            let txn = index.db.begin_write()?;
            make_all_dirty(&txn, &fixture.dirty_paths)?;
            txn.commit()?;
        }

        // Check that all files are marked dirty
        {
            let txn = index.db.begin_read()?;
            for file in &files {
                assert!(engine::is_dirty(&txn, &file)?, "{file} should be dirty",);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_make_all_dirty_empty_index() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;

        // Mark all files dirty on empty index; it shouldn't fail
        {
            let txn = index.db.begin_write()?;
            make_all_dirty(&txn, &fixture.dirty_paths)?;
            txn.commit()?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_make_all_dirty_with_invalid_paths() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;
        let mtime = UnixTime::from_secs(1234567890);

        // Add some valid files
        let valid_files = vec![
            realize_types::Path::parse("foo/a.txt")?,
            realize_types::Path::parse("bar/b.txt")?,
        ];

        for file in &valid_files {
            index.add_file(file, 100, &mtime, Hash([0xfa; 32]))?;
        }

        // Manually insert an invalid path into the file table; it should be skipped
        {
            let txn = index.db.begin_write()?;
            {
                let mut file_table = txn.index_file_table()?;
                file_table.insert(
                    "///invalid///path",
                    Holder::with_content(IndexedFileTableEntry {
                        size: 100,
                        mtime: mtime.clone(),
                        hash: Hash([0xfa; 32]),
                        outdated_by: None,
                    })?,
                )?;
            }
            txn.commit()?;
        }

        fixture.clear_all_dirty()?;

        // Mark all files dirty
        {
            let txn = index.db.begin_write()?;
            make_all_dirty(&txn, &fixture.dirty_paths)?;
            txn.commit()?;
        }

        // Check that valid files are marked dirty
        {
            let txn = index.db.begin_read()?;
            for file in &valid_files {
                assert!(engine::is_dirty(&txn, file)?, "{file} should be dirty",);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_update_different_arena() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;
        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar.txt")?;
        let hash = Hash([0xfa; 32]);

        // Add a file to the index
        index.add_file(&path, 100, &mtime, hash.clone())?;

        // Create a notification for a different arena
        let notification = Notification::Replace {
            arena: Arena::from("different_arena"),
            index: 1,
            path: path.clone(),
            mtime: mtime.clone(),
            size: 200,
            hash: Hash([0x07; 32]),
            old_hash: hash.clone(),
        };

        // Update should be ignored for different arena
        index.update(&notification, &std::path::Path::new("/tmp"))?;

        // Verify the file entry hasn't changed
        let entry = index.get_file(&path)?;
        assert_eq!(
            Some(IndexedFileTableEntry {
                size: 100,
                mtime: mtime.clone(),
                hash: hash.clone(),
                outdated_by: None,
            }),
            entry
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_update_replace_matching_hash() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;
        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar.txt")?;
        let old_hash = Hash([0xfa; 32]);
        let new_hash = Hash([0x07; 32]);

        // Add a file to the index
        index.add_file(&path, 100, &mtime, old_hash.clone())?;

        // Create a replace notification that matches the current hash
        let notification = Notification::Replace {
            arena: test_arena(),
            index: 1,
            path: path.clone(),
            mtime: UnixTime::from_secs(1234567891),
            size: 200,
            hash: new_hash.clone(),
            old_hash: old_hash.clone(),
        };

        // Update should mark the current version as outdated
        index.update(&notification, &std::path::Path::new("/tmp"))?;

        // Verify the file entry has been updated with outdated_by
        let entry = index.get_file(&path)?;
        assert_eq!(
            Some(IndexedFileTableEntry {
                size: 100,
                mtime: mtime.clone(),
                hash: old_hash.clone(),
                outdated_by: Some(new_hash.clone()),
            }),
            entry
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_update_replace_matching_outdated_by() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;
        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar.txt")?;
        let original_hash = Hash([0xfa; 32]);
        let outdated_hash = Hash([0x07; 32]);
        let newer_hash = Hash([0x42; 32]);

        // Add a file to the index with an outdated_by entry
        {
            let txn = index.db.begin_write()?;
            {
                let mut file_table = txn.index_file_table()?;
                file_table.insert(
                    path.as_str(),
                    Holder::with_content(IndexedFileTableEntry {
                        size: 100,
                        mtime: mtime.clone(),
                        hash: original_hash.clone(),
                        outdated_by: Some(outdated_hash.clone()),
                    })?,
                )?;
            }
            txn.commit()?;
        }

        // Create a replace notification that matches the outdated_by hash
        let notification = Notification::Replace {
            arena: test_arena(),
            index: 1,
            path: path.clone(),
            mtime: UnixTime::from_secs(1234567891),
            size: 200,
            hash: newer_hash.clone(),
            old_hash: outdated_hash.clone(),
        };

        // Update should mark the current version as outdated by the newer hash
        index.update(&notification, &std::path::Path::new("/tmp"))?;

        // Verify the file entry has been updated with the newer hash in outdated_by
        let entry = index.get_file(&path)?;
        assert_eq!(
            Some(IndexedFileTableEntry {
                size: 100,
                mtime: mtime.clone(),
                hash: original_hash.clone(),
                outdated_by: Some(newer_hash.clone()),
            }),
            entry
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_update_replace_non_matching_hash() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;
        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar.txt")?;
        let current_hash = Hash([0xfa; 32]);
        let different_hash = Hash([0x07; 32]);
        let new_hash = Hash([0x42; 32]);

        // Add a file to the index
        index.add_file(&path, 100, &mtime, current_hash.clone())?;

        // Create a replace notification that doesn't match the current hash
        let notification = Notification::Replace {
            arena: test_arena(),
            index: 1,
            path: path.clone(),
            mtime: UnixTime::from_secs(1234567891),
            size: 200,
            hash: new_hash.clone(),
            old_hash: different_hash.clone(),
        };

        // Update should be ignored since the hash doesn't match
        index.update(&notification, &std::path::Path::new("/tmp"))?;

        // Verify the file entry hasn't changed
        let entry = index.get_file(&path)?;
        assert_eq!(
            Some(IndexedFileTableEntry {
                size: 100,
                mtime: mtime.clone(),
                hash: current_hash.clone(),
                outdated_by: None,
            }),
            entry
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_update_replace_file_not_in_index() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;
        let path = realize_types::Path::parse("foo/bar.txt")?;
        let old_hash = Hash([0xfa; 32]);
        let new_hash = Hash([0x07; 32]);

        // Create a replace notification for a file not in the index
        let notification = Notification::Replace {
            arena: test_arena(),
            index: 1,
            path: path.clone(),
            mtime: UnixTime::from_secs(1234567890),
            size: 100,
            hash: new_hash.clone(),
            old_hash: old_hash.clone(),
        };

        // Update should be ignored since the file doesn't exist in the index
        index.update(&notification, &std::path::Path::new("/tmp"))?;

        // Verify the file still doesn't exist in the index
        assert_eq!(None, index.get_file(&path)?);

        Ok(())
    }

    #[tokio::test]
    async fn test_update_remove_matching_hash() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;
        let path = realize_types::Path::parse("foo/bar.txt")?;
        let hash = Hash([0xfa; 32]);

        // Create a file whose mtime and size match the entry.
        let tempdir = assert_fs::TempDir::new()?;
        let file_path = tempdir.path().join("foo").join("bar.txt");
        std::fs::create_dir_all(file_path.parent().unwrap())?;
        std::fs::write(&file_path, "test content")?;

        let metadata = std::fs::metadata(&file_path)?;
        let size = metadata.len();
        let mtime = UnixTime::mtime(&metadata);
        index.add_file(&path, size, &mtime, hash.clone())?;

        // Create a remove notification that matches the current hash
        let notification = Notification::Remove {
            arena: test_arena(),
            index: 1,
            path: path.clone(),
            old_hash: hash.clone(),
        };

        // Update should remove the file from the filesystem
        index.update(&notification, tempdir.path())?;

        // Verify the file has been removed from the filesystem
        assert!(!file_path.exists());

        // Verify the file entry still exists in the index (it's not removed from index)
        let entry = index.get_file(&path)?;
        assert_eq!(
            Some(IndexedFileTableEntry {
                size,
                mtime,
                hash: hash.clone(),
                outdated_by: None,
            }),
            entry
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_update_remove_matching_outdated_by() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;
        let path = realize_types::Path::parse("foo/bar.txt")?;
        let original_hash = Hash([0xfa; 32]);
        let outdated_hash = Hash([0x07; 32]);

        let tempdir = assert_fs::TempDir::new()?;
        let file_path = tempdir.path().join("foo").join("bar.txt");
        std::fs::create_dir_all(file_path.parent().unwrap())?;
        std::fs::write(&file_path, "test content")?;

        let metadata = std::fs::metadata(&file_path)?;
        let size = metadata.len();
        let mtime = UnixTime::mtime(&metadata);
        index.add_file(&path, size, &mtime, original_hash.clone())?;

        // Add a file to the index with an outdated_by entry
        {
            let txn = index.db.begin_write()?;
            {
                let mut file_table = txn.index_file_table()?;
                file_table.insert(
                    path.as_str(),
                    Holder::with_content(IndexedFileTableEntry {
                        size,
                        mtime: mtime.clone(),
                        hash: original_hash.clone(),
                        outdated_by: Some(outdated_hash.clone()),
                    })?,
                )?;
            }
            txn.commit()?;
        }

        // Create a remove notification that matches the outdated_by hash
        let notification = Notification::Remove {
            arena: test_arena(),
            index: 1,
            path: path.clone(),
            old_hash: outdated_hash.clone(),
        };

        // Update should remove the file from the filesystem
        index.update(&notification, tempdir.path())?;

        // Verify the file has been removed from the filesystem
        assert!(!file_path.exists());

        // Verify the file entry still exists in the index
        let entry = index.get_file(&path)?;
        assert_eq!(
            Some(IndexedFileTableEntry {
                size,
                mtime: mtime.clone(),
                hash: original_hash.clone(),
                outdated_by: Some(outdated_hash.clone()),
            }),
            entry
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_update_remove_non_matching_hash() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;
        let path = realize_types::Path::parse("foo/bar.txt")?;
        let current_hash = Hash([0xfa; 32]);
        let different_hash = Hash([0x07; 32]);

        let tempdir = assert_fs::TempDir::new()?;
        let file_path = tempdir.path().join("foo").join("bar.txt");
        std::fs::create_dir_all(file_path.parent().unwrap())?;
        std::fs::write(&file_path, "test content")?;

        let metadata = std::fs::metadata(&file_path)?;
        let size = metadata.len();
        let mtime = UnixTime::mtime(&metadata);
        index.add_file(&path, size, &mtime, current_hash.clone())?;

        // Create a remove notification that doesn't match the current hash
        let notification = Notification::Remove {
            arena: test_arena(),
            index: 1,
            path: path.clone(),
            old_hash: different_hash.clone(),
        };

        // Update should be ignored since the hash doesn't match
        index.update(&notification, tempdir.path())?;

        // Verify the file still exists in the filesystem
        assert!(file_path.exists());

        // Verify the file entry hasn't changed
        let entry = index.get_file(&path)?;
        assert_eq!(
            Some(IndexedFileTableEntry {
                size: 12,
                mtime: mtime.clone(),
                hash: current_hash.clone(),
                outdated_by: None,
            }),
            entry
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_update_remove_file_not_in_index() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;
        let path = realize_types::Path::parse("foo/bar.txt")?;
        let hash = Hash([0xfa; 32]);

        // Create a remove notification for a file not in the index
        let notification = Notification::Remove {
            arena: test_arena(),
            index: 1,
            path: path.clone(),
            old_hash: hash.clone(),
        };

        // Update should be ignored since the file doesn't exist in the index
        index.update(&notification, &std::path::Path::new("/tmp"))?;

        // Verify the file still doesn't exist in the index
        assert_eq!(None, index.get_file(&path)?);

        Ok(())
    }

    #[tokio::test]
    async fn test_update_remove_file_mismatched_metadata() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;
        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar.txt")?;
        let hash = Hash([0xfa; 32]);

        // Create a temporary file with different metadata than the index
        let tempdir = assert_fs::TempDir::new()?;
        let file_path = tempdir.path().join("foo").join("bar.txt");
        std::fs::create_dir_all(file_path.parent().unwrap())?;
        std::fs::write(&file_path, "different content")?;

        // Add a file to the index
        index.add_file(&path, 12, &mtime, hash.clone())?;

        // Create a remove notification that matches the current hash
        let notification = Notification::Remove {
            arena: test_arena(),
            index: 1,
            path: path.clone(),
            old_hash: hash.clone(),
        };

        // Update should be ignored since the file metadata doesn't match
        index.update(&notification, tempdir.path())?;

        // Verify the file still exists in the filesystem
        assert!(file_path.exists());

        // Verify the file entry hasn't changed
        let entry = index.get_file(&path)?;
        assert_eq!(
            Some(IndexedFileTableEntry {
                size: 12,
                mtime: mtime.clone(),
                hash: hash.clone(),
                outdated_by: None,
            }),
            entry
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_update_remove_file_does_not_exist() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;
        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar.txt")?;
        let hash = Hash([0xfa; 32]);

        // Add a file to the index
        index.add_file(&path, 12, &mtime, hash.clone())?;

        // Create a remove notification that matches the current hash
        let notification = Notification::Remove {
            arena: test_arena(),
            index: 1,
            path: path.clone(),
            old_hash: hash.clone(),
        };

        // Update should be ignored since the file doesn't exist on filesystem
        index.update(&notification, &std::path::Path::new("/tmp"))?;

        // Verify the file entry still exists in the index
        let entry = index.get_file(&path)?;
        assert_eq!(
            Some(IndexedFileTableEntry {
                size: 12,
                mtime: mtime.clone(),
                hash: hash.clone(),
                outdated_by: None,
            }),
            entry
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_update_other_notification_types() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;
        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar.txt")?;
        let hash = Hash([0xfa; 32]);

        // Add a file to the index
        index.add_file(&path, 12, &mtime, hash.clone())?;

        // Test Add notification (should be ignored)
        let add_notification = Notification::Add {
            arena: test_arena(),
            index: 1,
            path: path.clone(),
            mtime: mtime.clone(),
            size: 12,
            hash: hash.clone(),
        };

        index.update(&add_notification, &std::path::Path::new("/tmp"))?;

        // Verify the file entry hasn't changed
        let entry = index.get_file(&path)?;
        assert_eq!(
            Some(IndexedFileTableEntry {
                size: 12,
                mtime: mtime.clone(),
                hash: hash.clone(),
                outdated_by: None,
            }),
            entry
        );

        // Test CatchupStart notification (should be ignored)
        let catchup_notification = Notification::CatchupStart(test_arena());
        index.update(&catchup_notification, &std::path::Path::new("/tmp"))?;

        // Verify the file entry still hasn't changed
        let entry = index.get_file(&path)?;
        assert_eq!(
            Some(IndexedFileTableEntry {
                size: 12,
                mtime: mtime.clone(),
                hash: hash.clone(),
                outdated_by: None,
            }),
            entry
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_get_indexed_file_with_hash_success() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let root = &fixture.root;
        let (path, hash) = fixture.add_file_with_content("test.txt", "test content")?;

        let txn = fixture.index.db.begin_write()?;
        let result = get_indexed_file(&txn, root.path(), &path, Some(&hash))?;
        assert_eq!(result, Some(root.child("test.txt").path().to_path_buf()));

        Ok(())
    }

    #[tokio::test]
    async fn test_get_indexed_file_with_hash_wrong_hash() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let root = &fixture.root;
        let (path, _) = fixture.add_file_with_content("test.txt", "test content")?;
        let wrong_hash = hash::digest("different content");

        let txn = fixture.index.db.begin_write()?;
        let result = get_indexed_file(&txn, root.path(), &path, Some(&wrong_hash))?;
        assert_eq!(result, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_indexed_file_with_hash_file_not_in_index() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let root = &fixture.root;
        let path = realize_types::Path::parse("not_in_index.txt")?;
        let hash = hash::digest("some content");

        let txn = fixture.index.db.begin_write()?;
        let result = get_indexed_file(&txn, root.path(), &path, Some(&hash))?;
        assert_eq!(result, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_indexed_file_with_hash_file_modified_on_fs() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let root = &fixture.root;
        let (path, hash) = fixture.add_file_with_content("test.txt", "original content")?;

        // Modify the file on filesystem without updating the index
        //
        // Open issue:On some systems, the test may run too quickly
        // for the test to catch a fs time change alone, so this changes the content size.
        fixture
            .root
            .child("test.txt")
            .write_str("modified content!")?;

        let txn = fixture.index.db.begin_write()?;
        let result = get_indexed_file(&txn, root.path(), &path, Some(&hash))?;
        assert_eq!(result, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_indexed_file_with_hash_file_missing_on_fs() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let root = &fixture.root;
        let (path, hash) = fixture.add_file_with_content("test.txt", "test content")?;

        // Remove the file from filesystem
        std::fs::remove_file(root.child("test.txt").path())?;

        let txn = fixture.index.db.begin_write()?;
        let result = get_indexed_file(&txn, root.path(), &path, Some(&hash))?;
        assert_eq!(result, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_indexed_file_without_hash_file_not_exist() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let root = &fixture.root;
        let path = realize_types::Path::parse("does_not_exist.txt")?;

        let txn = fixture.index.db.begin_write()?;
        let result = get_indexed_file(&txn, root.path(), &path, None)?;
        assert_eq!(
            result,
            Some(root.child("does_not_exist.txt").path().to_path_buf())
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_get_indexed_file_without_hash_file_exists_in_index() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let root = &fixture.root;
        let (path, _) = fixture.add_file_with_content("test.txt", "test content")?;

        let txn = fixture.index.db.begin_write()?;
        let result = get_indexed_file(&txn, root.path(), &path, None)?;
        assert_eq!(result, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_indexed_file_without_hash_file_exists_on_fs_but_not_in_index()
    -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let root = &fixture.root;
        let path = realize_types::Path::parse("fs_only.txt")?;

        // Create file on filesystem without adding to index
        root.child("fs_only.txt").write_str("fs only content")?;

        let txn = fixture.index.db.begin_write()?;
        let result = get_indexed_file(&txn, root.path(), &path, None)?;
        assert_eq!(result, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_indexed_file_with_hash_metadata_mismatch() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let root = &fixture.root;
        let path = realize_types::Path::parse("test.txt")?;

        let content = "test content";
        let hash = hash::digest(content);
        fixture.root.child("test.txt").write_str(content)?;
        fixture.index.add_file(
            &path,
            content.len() as u64,
            &UnixTime::from_secs(1234567890),
            hash.clone(),
        )?;

        let txn = fixture.index.db.begin_write()?;
        let result = get_indexed_file(&txn, root.path(), &path, Some(&hash))?;
        assert_eq!(result, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_file_if_matches_success() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let (path, hash) = fixture.add_file_with_content("test.txt", "test content")?;

        // Verify file exists in index
        assert!(fixture.index.has_file(&path)?);

        // Drop the file
        {
            let txn = fixture.index.db.begin_write()?;
            let result =
                fixture
                    .index
                    .drop_file_if_matches(&txn, fixture.root.path(), &path, &hash)?;
            assert!(result);
            txn.commit()?;
        }

        // Verify file is removed from index
        assert!(!fixture.index.has_file(&path)?);

        // Verify history entry was created
        {
            let txn = fixture.index.db.begin_read()?;
            let history_table = txn.index_history_table()?;
            let last_index = fixture.index.last_history_index()?;
            let entry = history_table.get(last_index)?.unwrap().value().parse()?;
            assert_eq!(entry, HistoryTableEntry::Drop(path.clone(), hash.clone()));
        }

        // Verify file is marked dirty
        {
            let txn = fixture.index.db.begin_read()?;
            assert!(engine::is_dirty(&txn, &path)?);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_file_if_matches_wrong_hash() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let (path, _) = fixture.add_file_with_content("test.txt", "test content")?;
        let wrong_hash = hash::digest("different content");

        // Get the initial history count
        let initial_history_count = fixture.index.last_history_index()?;

        // Verify file exists in index
        assert!(fixture.index.has_file(&path)?);

        // Try to drop with wrong hash
        {
            let txn = fixture.index.db.begin_write()?;
            let result = fixture.index.drop_file_if_matches(
                &txn,
                fixture.root.path(),
                &path,
                &wrong_hash,
            )?;
            assert!(!result);
            txn.commit()?;
        }

        // Verify file still exists in index
        assert!(fixture.index.has_file(&path)?);

        // Verify no new history entry was created
        let final_history_count = fixture.index.last_history_index()?;
        assert_eq!(initial_history_count, final_history_count);

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_file_if_matches_file_not_in_index() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let path = realize_types::Path::parse("not_in_index.txt")?;
        let hash = hash::digest("some content");

        // Get the initial history count
        let initial_history_count = fixture.index.last_history_index()?;

        // Verify file doesn't exist in index
        assert!(!fixture.index.has_file(&path)?);

        // Try to drop file not in index
        {
            let txn = fixture.index.db.begin_write()?;
            let result =
                fixture
                    .index
                    .drop_file_if_matches(&txn, fixture.root.path(), &path, &hash)?;
            assert!(!result);
            txn.commit()?;
        }

        // Verify no new history entry was created
        let final_history_count = fixture.index.last_history_index()?;
        assert_eq!(initial_history_count, final_history_count);

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_file_if_matches_file_modified_on_fs() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let (path, hash) = fixture.add_file_with_content("test.txt", "original content")?;

        // Get the initial history count
        let initial_history_count = fixture.index.last_history_index()?;

        // Modify the file on filesystem without updating the index
        //
        // Open issue:On some systems, the test may run too quickly
        // for the test to catch a fs time change alone, so this changes the content size.
        fixture
            .root
            .child("test.txt")
            .write_str("modified content!")?;

        // Verify file exists in index
        assert!(fixture.index.has_file(&path)?);

        // Try to drop the file
        {
            let txn = fixture.index.db.begin_write()?;
            let result =
                fixture
                    .index
                    .drop_file_if_matches(&txn, fixture.root.path(), &path, &hash)?;
            assert!(!result);
            txn.commit()?;
        }

        // Verify file still exists in index
        assert!(fixture.index.has_file(&path)?);

        // Verify no new history entry was created
        let final_history_count = fixture.index.last_history_index()?;
        assert_eq!(initial_history_count, final_history_count);

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_file_if_matches_file_missing_on_fs() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let (path, hash) = fixture.add_file_with_content("test.txt", "test content")?;

        // Get the initial history count
        let initial_history_count = fixture.index.last_history_index()?;

        // Remove the file from filesystem
        std::fs::remove_file(fixture.root.child("test.txt").path())?;

        // Verify file exists in index
        assert!(fixture.index.has_file(&path)?);

        // Try to drop the file
        {
            let txn = fixture.index.db.begin_write()?;
            let result =
                fixture
                    .index
                    .drop_file_if_matches(&txn, fixture.root.path(), &path, &hash)?;
            assert!(!result);
            txn.commit()?;
        }

        // Verify file still exists in index
        assert!(fixture.index.has_file(&path)?);

        // Verify no new history entry was created
        let final_history_count = fixture.index.last_history_index()?;
        assert_eq!(initial_history_count, final_history_count);

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_file_if_matches_metadata_mismatch() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let path = realize_types::Path::parse("test.txt")?;
        let content = "test content";
        let hash = hash::digest(content);

        // Create file with different metadata than what's in index
        fixture.root.child("test.txt").write_str(content)?;
        fixture.index.add_file(
            &path,
            content.len() as u64,
            &UnixTime::from_secs(1234567890),
            hash.clone(),
        )?;

        // Get the initial history count
        let initial_history_count = fixture.index.last_history_index()?;

        // Verify file exists in index
        assert!(fixture.index.has_file(&path)?);

        // Try to drop the file
        {
            let txn = fixture.index.db.begin_write()?;
            let result =
                fixture
                    .index
                    .drop_file_if_matches(&txn, fixture.root.path(), &path, &hash)?;
            assert!(!result);
            txn.commit()?;
        }

        // Verify file still exists in index
        assert!(fixture.index.has_file(&path)?);

        // Verify no new history entry was created
        let final_history_count = fixture.index.last_history_index()?;
        assert_eq!(initial_history_count, final_history_count);

        Ok(())
    }
}
