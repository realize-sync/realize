#![allow(dead_code)] // work in progress

use super::real_capnp;
use crate::StorageError;
use crate::utils::holder::{ByteConversionError, ByteConvertible, Holder, NamedType};
use capnp::message::ReaderOptions;
use capnp::serialize_packed;
use realize_types::{self, Arena, Hash, UnixTime};
use redb::{Database, ReadableTable as _, TableDefinition, WriteTransaction};
use std::ops::RangeBounds;
use std::path;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio::task;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

/// Track hash and metadata of local files.
///
/// Key: realize_types::Path
/// Value: FileTableEntry
const FILE_TABLE: TableDefinition<&str, Holder<FileTableEntry>> = TableDefinition::new("file");

/// Local file history.
///
/// Key: u64 (monotonically increasing index value)
/// Value: HistoryTableEntry
const HISTORY_TABLE: TableDefinition<u64, Holder<HistoryTableEntry>> =
    TableDefinition::new("history");

/// Database settings.
///
/// Key: string
/// Value: depends on the setting
const SETTINGS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("settings");

/// File hash index, blocking version.
pub struct RealIndexBlocking {
    db: Database,
    arena: Arena,
    history_tx: watch::Sender<u64>,
    uuid: Uuid,

    /// This is just to keep history_tx alive and up-to-date.
    _history_rx: watch::Receiver<u64>,
}

impl RealIndexBlocking {
    pub fn new(arena: Arena, db: Database) -> Result<Self, StorageError> {
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
        let history_table = txn.open_table(HISTORY_TABLE)?;

        last_history_index(&history_table)
    }

    /// Turn this instance into an async one.
    pub fn into_async(self) -> RealIndexAsync {
        RealIndexAsync::new(self)
    }

    /// Open or create an index at the given path.
    pub fn open(arena: Arena, path: &path::Path) -> Result<Self, StorageError> {
        Self::new(arena, Database::create(path)?)
    }

    /// Get a file entry.
    pub fn get_file(
        &self,
        path: &realize_types::Path,
    ) -> Result<Option<FileTableEntry>, StorageError> {
        let txn = self.db.begin_read()?;
        let file_table = txn.open_table(FILE_TABLE)?;

        if let Some(entry) = file_table.get(path.as_str())? {
            return Ok(Some(entry.value().parse()?));
        }

        Ok(None)
    }

    /// Check whether a given file is in the index already.
    pub fn has_file(&self, path: &realize_types::Path) -> Result<bool, StorageError> {
        let txn = self.db.begin_read()?;
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
        let txn = self.db.begin_write()?;
        let mut history_index = None;
        do_add_file(&txn, path, size, mtime, hash, &mut history_index)?;
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
        let txn = self.db.begin_read()?;
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
        let txn = self.db.begin_read()?;
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
        let txn = self.db.begin_write()?;
        let mut history_index = None;
        do_remove_file_or_dir(&txn, path, &mut history_index)?;
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
) -> Result<(), StorageError> {
    let mut file_table = txn.open_table(FILE_TABLE)?;
    let mut history_table = txn.open_table(HISTORY_TABLE)?;
    let path_str = path.as_str();
    let mut range_end = path.to_string();
    range_end.push('0' /* '/' + 1 */);

    for entry in file_table.extract_from_if(path_str..range_end.as_ref(), |k, _| {
        k.strip_prefix(path_str)
            .map(|rest| rest == "" || rest.starts_with('/'))
            .unwrap_or(false)
    })? {
        let (k, v) = entry?;
        let index = next_history_index(&history_table)?;
        history_table.insert(
            index,
            Holder::with_content(HistoryTableEntry::Remove(
                realize_types::Path::parse(k.value())?,
                v.value().parse()?.hash,
            ))?,
        )?;
        *history_index = Some(index);
    }

    Ok(())
}

fn do_add_file(
    txn: &WriteTransaction,
    path: &realize_types::Path,
    size: u64,
    mtime: &UnixTime,
    hash: Hash,
    history_index: &mut Option<u64>,
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

    /// Create an inedx using the database at the given path. Create it if necessary.
    pub async fn open(arena: Arena, path: &path::Path) -> Result<Self, StorageError> {
        let path = path.to_path_buf();

        task::spawn_blocking(move || {
            Ok(RealIndexAsync::new(RealIndexBlocking::open(arena, &path)?))
        })
        .await?
    }

    /// Create an index using the given database. Initialize the database if necessary.
    pub async fn with_db(arena: Arena, db: redb::Database) -> Result<Self, StorageError> {
        task::spawn_blocking(move || Ok(RealIndexAsync::new(RealIndexBlocking::new(arena, db)?)))
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

/// An entry in the file table.
#[derive(Debug, Clone, PartialEq)]
pub struct FileTableEntry {
    pub hash: Hash,
    pub mtime: UnixTime,
    pub size: u64,
}

impl NamedType for FileTableEntry {
    fn typename() -> &'static str {
        "index.file"
    }
}

impl ByteConvertible<FileTableEntry> for FileTableEntry {
    fn from_bytes(data: &[u8]) -> Result<FileTableEntry, ByteConversionError> {
        let message_reader = serialize_packed::read_message(&mut &data[..], ReaderOptions::new())?;
        let msg: real_capnp::file_table_entry::Reader =
            message_reader.get_root::<real_capnp::file_table_entry::Reader>()?;

        let mtime = msg.get_mtime()?;
        let hash: &[u8] = msg.get_hash()?;
        let hash = parse_hash(hash)?;
        Ok(FileTableEntry {
            hash,
            mtime: UnixTime::new(mtime.get_secs(), mtime.get_nsecs()),
            size: msg.get_size(),
        })
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let mut builder: real_capnp::file_table_entry::Builder =
            message.init_root::<real_capnp::file_table_entry::Builder>();

        builder.set_size(self.size);
        builder.set_hash(&self.hash.0);

        let mut mtime = builder.init_mtime();
        mtime.set_secs(self.mtime.as_secs());
        mtime.set_nsecs(self.mtime.subsec_nanos());

        let mut buffer: Vec<u8> = Vec::new();
        serialize_packed::write_message(&mut buffer, &message)?;

        Ok(buffer)
    }
}

/// An entry in the file table.
#[derive(Debug, Clone, PartialEq)]
pub enum HistoryTableEntry {
    Add(realize_types::Path),
    Replace(realize_types::Path, Hash),
    Remove(realize_types::Path, Hash),
}

impl NamedType for HistoryTableEntry {
    fn typename() -> &'static str {
        "index.file"
    }
}

impl ByteConvertible<HistoryTableEntry> for HistoryTableEntry {
    fn from_bytes(data: &[u8]) -> Result<HistoryTableEntry, ByteConversionError> {
        let message_reader = serialize_packed::read_message(&mut &data[..], ReaderOptions::new())?;
        let msg: real_capnp::history_table_entry::Reader =
            message_reader.get_root::<real_capnp::history_table_entry::Reader>()?;
        match msg.get_kind()? {
            real_capnp::history_table_entry::Kind::Add => {
                Ok(HistoryTableEntry::Add(parse_path(msg.get_path()?)?))
            }
            real_capnp::history_table_entry::Kind::Replace => Ok(HistoryTableEntry::Replace(
                parse_path(msg.get_path()?)?,
                parse_hash(msg.get_old_hash()?)?,
            )),
            real_capnp::history_table_entry::Kind::Remove => Ok(HistoryTableEntry::Remove(
                parse_path(msg.get_path()?)?,
                parse_hash(msg.get_old_hash()?)?,
            )),
        }
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let mut builder: real_capnp::history_table_entry::Builder =
            message.init_root::<real_capnp::history_table_entry::Builder>();

        match self {
            HistoryTableEntry::Add(path) => {
                builder.set_kind(real_capnp::history_table_entry::Kind::Add);
                builder.set_path(path.as_str());
            }
            HistoryTableEntry::Replace(path, old_hash) => {
                builder.set_kind(real_capnp::history_table_entry::Kind::Replace);
                builder.set_path(path.as_str());
                builder.set_old_hash(&old_hash.0);
            }
            HistoryTableEntry::Remove(path, old_hash) => {
                builder.set_kind(real_capnp::history_table_entry::Kind::Remove);
                builder.set_path(path.as_str());
                builder.set_old_hash(&old_hash.0);
            }
        }

        let mut buffer: Vec<u8> = Vec::new();
        serialize_packed::write_message(&mut buffer, &message)?;

        Ok(buffer)
    }
}

fn parse_hash(hash: &[u8]) -> Result<Hash, ByteConversionError> {
    let hash: [u8; 32] = hash
        .try_into()
        .map_err(|_| ByteConversionError::Invalid("hash"))?;
    let hash = Hash(hash);
    Ok(hash)
}

fn parse_path(path: capnp::text::Reader<'_>) -> Result<realize_types::Path, ByteConversionError> {
    realize_types::Path::parse(path.to_str()?).map_err(|_| ByteConversionError::Invalid("path"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_fs::TempDir;
    use futures::{StreamExt as _, TryStreamExt as _};

    fn test_arena() -> Arena {
        Arena::from("arena")
    }

    struct Fixture {
        aindex: RealIndexAsync,
        index: Arc<RealIndexBlocking>,
        _tempdir: TempDir,
    }
    impl Fixture {
        fn setup() -> anyhow::Result<Fixture> {
            let _ = env_logger::try_init();
            let tempdir = TempDir::new()?;
            let path = tempdir.path().join("index.db");
            let aindex = RealIndexBlocking::open(test_arena(), &path)?.into_async();
            Ok(Self {
                index: aindex.blocking(),
                aindex,
                _tempdir: tempdir,
            })
        }
    }

    #[test]
    fn open_creates_tables() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let db = &fixture.index.db;

        let txn = db.begin_read()?;
        assert!(txn.open_table(FILE_TABLE).is_ok());
        assert!(txn.open_table(HISTORY_TABLE).is_ok());
        assert!(txn.open_table(SETTINGS_TABLE).is_ok());

        assert!(!fixture.index.uuid().is_nil());
        Ok(())
    }

    #[test]
    fn reopen_keeps_uuid() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;
        let path = tempdir.path().join("index.db");
        let uuid = RealIndexBlocking::open(test_arena(), &path)?.uuid().clone();

        assert!(!uuid.is_nil());
        assert_eq!(
            uuid,
            RealIndexBlocking::open(test_arena(), &path)?.uuid().clone()
        );

        Ok(())
    }

    #[test]
    fn convert_file_table_entry() -> anyhow::Result<()> {
        let entry = FileTableEntry {
            size: 200,
            mtime: UnixTime::new(1234567890, 111),
            hash: Hash([0xf0; 32]),
        };

        assert_eq!(
            entry,
            FileTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }

    #[test]
    fn convert_history_table_entry() -> anyhow::Result<()> {
        let add = HistoryTableEntry::Add(realize_types::Path::parse("foo/bar.txt")?);
        assert_eq!(
            add,
            HistoryTableEntry::from_bytes(add.clone().to_bytes()?.as_slice())?
        );

        let remove =
            HistoryTableEntry::Remove(realize_types::Path::parse("foo/bar.txt")?, Hash([0xfa; 32]));
        assert_eq!(
            remove,
            HistoryTableEntry::from_bytes(remove.clone().to_bytes()?.as_slice())?
        );

        let replace = HistoryTableEntry::Replace(
            realize_types::Path::parse("foo/bar.txt")?,
            Hash([0x1a; 32]),
        );
        assert_eq!(
            replace,
            HistoryTableEntry::from_bytes(replace.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }

    #[test]
    fn add_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

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
        }

        Ok(())
    }

    #[test]
    fn replace_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

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
        }

        Ok(())
    }

    #[test]
    fn has_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

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

    #[test]
    fn get_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

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

    #[test]
    fn has_matching_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

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

    #[test]
    fn remove_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

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
        }

        Ok(())
    }

    #[test]
    fn remove_dir() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

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
        }

        Ok(())
    }

    #[test]
    fn remove_nothing() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

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
        let fixture = Fixture::setup()?;

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

    #[test]
    fn last_history_index() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

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
        let fixture = Fixture::setup()?;

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
        let fixture = Fixture::setup()?;

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
        let fixture = Fixture::setup()?;
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
    #[test]
    fn touch_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

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
}
