#![allow(dead_code)] // work in progress

use super::types::{FileTableEntry, HistoryTableEntry};
use crate::StorageError;
use crate::arena::engine::DirtyPaths;
use crate::utils::holder::{ByteConversionError, Holder};
use realize_types::{self, Arena, Hash, UnixTime};
use redb::{Database, ReadableTable as _, TableDefinition, WriteTransaction};
use std::ops::RangeBounds;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio::task;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

/// Track hash and metadata of local files.
///
/// Key: realize_types::Path
/// Value: FileTableEntry
const FILE_TABLE: TableDefinition<&str, Holder<FileTableEntry>> =
    TableDefinition::new("index.file");

/// Local file history.
///
/// Key: u64 (monotonically increasing index value)
/// Value: HistoryTableEntry
const HISTORY_TABLE: TableDefinition<u64, Holder<HistoryTableEntry>> =
    TableDefinition::new("index.history");

/// Database settings.
///
/// Key: string
/// Value: depends on the setting
const SETTINGS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("index.settings");

/// File hash index, blocking version.
pub struct RealIndexBlocking {
    db: Arc<Database>,
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
        db: Arc<Database>,
        dirty_paths: Arc<DirtyPaths>,
    ) -> Result<Self, StorageError> {
        let index: u64;
        let uuid: Uuid;
        {
            let txn = db.begin_write()?;
            {
                txn.open_table(FILE_TABLE)?;
                let history_table = txn.open_table(HISTORY_TABLE)?;
                index = last_history_index(&history_table)?;
            }
            {
                let mut settings_table = txn.open_table(SETTINGS_TABLE)?;
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
        let txn = (&*self.db).begin_read()?;
        let history_table = txn.open_table(HISTORY_TABLE)?;

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
    ) -> Result<Option<FileTableEntry>, StorageError> {
        let txn = (&*self.db).begin_read()?;
        let file_table = txn.open_table(FILE_TABLE)?;

        if let Some(entry) = file_table.get(path.as_str())? {
            return Ok(Some(entry.value().parse()?));
        }

        Ok(None)
    }

    /// Check whether a given file is in the index already.
    pub fn has_file(&self, path: &realize_types::Path) -> Result<bool, StorageError> {
        let txn = (&*self.db).begin_read()?;
        let file_table = txn.open_table(FILE_TABLE)?;

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
        let txn = (&*self.db).begin_write()?;
        let mut history_index = None;
        do_add_file(
            &txn,
            path,
            size,
            mtime,
            hash,
            &mut history_index,
            &self.dirty_paths,
        )?;
        txn.commit()?;

        if let Some(index) = history_index {
            let _ = self.history_tx.send(index);
        }
        Ok(())
    }

    /// Send all valid entries of the file table to the given channel.
    pub fn all_files(
        &self,
        tx: mpsc::Sender<(realize_types::Path, FileTableEntry)>,
    ) -> Result<(), StorageError> {
        let txn = (&*self.db).begin_read()?;
        let file_table = txn.open_table(FILE_TABLE)?;
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
        let txn = (&*self.db).begin_read()?;
        let history_table = txn.open_table(HISTORY_TABLE)?;
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
        let txn = (&*self.db).begin_write()?;
        let mut history_index = None;
        do_remove_file_or_dir(&txn, path, &mut history_index, &self.dirty_paths)?;
        txn.commit()?;

        if let Some(index) = history_index {
            let _ = self.history_tx.send(index);
        }

        Ok(())
    }
}

fn do_remove_file_or_dir(
    txn: &WriteTransaction,
    path: &realize_types::Path,
    history_index: &mut Option<u64>,
    dirty_paths: &Arc<DirtyPaths>,
) -> Result<(), StorageError> {
    let mut file_table = txn.open_table(FILE_TABLE)?;
    let mut history_table = txn.open_table(HISTORY_TABLE)?;
    let path_prefix = PathPrefix::new(&path);

    for entry in file_table.extract_from_if(path_prefix.range(), |k, _| path_prefix.accept(k))? {
        let (k, v) = entry?;
        let index = next_history_index(&history_table)?;
        let path = realize_types::Path::parse(k.value())?;
        dirty_paths.mark_dirty(txn, &path)?;
        history_table.insert(
            index,
            Holder::with_content(HistoryTableEntry::Remove(path, v.value().parse()?.hash))?,
        )?;
        *history_index = Some(index);
    }

    Ok(())
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

fn do_add_file(
    txn: &WriteTransaction,
    path: &realize_types::Path,
    size: u64,
    mtime: &UnixTime,
    hash: Hash,
    history_index: &mut Option<u64>,
    dirty_paths: &Arc<DirtyPaths>,
) -> Result<(), StorageError> {
    let mut file_table = txn.open_table(FILE_TABLE)?;
    let mut history_table = txn.open_table(HISTORY_TABLE)?;

    let old_hash = file_table
        .get(path.as_str())?
        .map(|e| e.value().parse().ok())
        .flatten()
        .map(|e| e.hash);
    let same_hash = old_hash.as_ref().map(|h| *h == hash).unwrap_or(false);
    file_table.insert(
        path.as_str(),
        Holder::with_content(FileTableEntry {
            size,
            mtime: mtime.clone(),
            hash,
        })?,
    )?;
    if same_hash {
        return Ok(());
    }

    dirty_paths.mark_dirty(txn, path)?;
    let index = next_history_index(&history_table)?;
    history_table.insert(
        index,
        Holder::with_content(if let Some(old_hash) = old_hash {
            HistoryTableEntry::Replace(path.clone(), old_hash)
        } else {
            HistoryTableEntry::Add(path.clone())
        })?,
    )?;
    *history_index = Some(index);

    Ok(())
}

fn next_history_index(
    history_table: &impl redb::ReadableTable<u64, Holder<'static, HistoryTableEntry>>,
) -> Result<u64, StorageError> {
    let entry_index = 1 + last_history_index(history_table)?;
    Ok(entry_index)
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
        db: Arc<redb::Database>,
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
    pub fn all_files(&self) -> ReceiverStream<(realize_types::Path, FileTableEntry)> {
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
    ) -> Result<Option<FileTableEntry>, StorageError> {
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
    txn: &WriteTransaction,
    path: &realize_types::Path,
    dirty_paths: &Arc<DirtyPaths>,
) -> Result<(), StorageError> {
    let file_table = txn.open_table(FILE_TABLE)?;
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
    txn: &WriteTransaction,
    dirty_paths: &Arc<DirtyPaths>,
) -> Result<(), StorageError> {
    let file_table = txn.open_table(FILE_TABLE)?;
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
    use super::*;
    use crate::arena::engine;
    use crate::utils::redb_utils;
    use assert_fs::TempDir;
    use futures::{StreamExt as _, TryStreamExt as _};

    fn test_arena() -> Arena {
        Arena::from("arena")
    }

    struct Fixture {
        aindex: RealIndexAsync,
        index: Arc<RealIndexBlocking>,
        dirty_paths: Arc<DirtyPaths>,
    }
    impl Fixture {
        async fn setup() -> anyhow::Result<Fixture> {
            let _ = env_logger::try_init();
            let arena = test_arena();
            let db = redb_utils::in_memory()?;
            let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;
            let aindex = RealIndexBlocking::new(arena, db, Arc::clone(&dirty_paths))?.into_async();
            Ok(Self {
                index: aindex.blocking(),
                aindex,
                dirty_paths,
            })
        }

        fn clear_all_dirty(&self) -> anyhow::Result<()> {
            let txn = self.index.db.begin_write()?;
            while engine::take_dirty(&txn)?.is_some() {}
            txn.commit()?;

            Ok(())
        }
    }

    #[tokio::test]
    async fn open_creates_tables() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let db = &fixture.index.db;

        let txn = db.begin_read()?;
        assert!(txn.open_table(FILE_TABLE).is_ok());
        assert!(txn.open_table(HISTORY_TABLE).is_ok());
        assert!(txn.open_table(SETTINGS_TABLE).is_ok());

        assert!(!fixture.index.uuid().is_nil());
        Ok(())
    }

    #[tokio::test]
    async fn reopen_keeps_uuid() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;
        let path = tempdir.path().join("index.db");
        let db = Arc::new(redb::Database::create(&path)?);
        let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;
        let arena = test_arena();
        let uuid = RealIndexBlocking::new(arena, db, dirty_paths)?
            .uuid()
            .clone();

        let db = Arc::new(redb::Database::create(&path)?);
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
            let file_table = txn.open_table(FILE_TABLE)?;
            assert_eq!(
                FileTableEntry {
                    size: 100,
                    mtime,
                    hash: Hash([0xfa; 32])
                },
                file_table.get("foo/bar.txt")?.unwrap().value().parse()?
            );

            let history_table = txn.open_table(HISTORY_TABLE)?;
            assert_eq!(
                HistoryTableEntry::Add(path.clone()),
                history_table.get(1)?.unwrap().value().parse()?
            );

            assert!(engine::is_dirty(&txn, &path)?);
        }

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
            let file_table = txn.open_table(FILE_TABLE)?;
            assert_eq!(
                FileTableEntry {
                    size: 200,
                    mtime: mtime2,
                    hash: Hash([0x07; 32])
                },
                file_table.get("foo/bar.txt")?.unwrap().value().parse()?
            );

            let history_table = txn.open_table(HISTORY_TABLE)?;
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
            Some(FileTableEntry {
                size: 100,
                mtime: mtime.clone(),
                hash: hash.clone()
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
            let history_table = txn.open_table(HISTORY_TABLE)?;
            assert_eq!(
                HistoryTableEntry::Remove(path.clone(), Hash([0xfa; 32])),
                history_table.get(2)?.unwrap().value().parse()?
            );

            assert!(engine::is_dirty(&txn, &path)?);
        }

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
            let history_table = txn.open_table(HISTORY_TABLE)?;
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
            let history_table = txn.open_table(HISTORY_TABLE)?;
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
                let mut file_table = txn.open_table(FILE_TABLE)?;
                file_table.insert(
                    "///invalid///path",
                    Holder::with_content(FileTableEntry {
                        size: 100,
                        mtime: mtime.clone(),
                        hash: Hash([0xfa; 32]),
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
}
