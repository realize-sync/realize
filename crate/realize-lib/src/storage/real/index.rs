#![allow(dead_code)] // work in progress

use super::real_capnp;
use crate::model::{self, Arena, Hash, UnixTime};
use crate::utils::holder::{ByteConversionError, ByteConvertible, Holder, NamedType};
use capnp::message::ReaderOptions;
use capnp::serialize_packed;
use redb::{Database, ReadableTable as _, TableDefinition, WriteTransaction};
use std::path;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task;
use tokio_stream::wrappers::ReceiverStream;

/// Track hash and metadata of local files.
///
/// Key: model::Path
/// Value: FileTableEntry
const FILE_TABLE: TableDefinition<&str, Holder<FileTableEntry>> = TableDefinition::new("file");

/// Local file history.
///
/// Key: u64 (monotonically increasing index value)
/// Value: HistoryTableEntry
const HISTORY_TABLE: TableDefinition<u64, Holder<HistoryTableEntry>> =
    TableDefinition::new("history");

/// File hash index, blocking version.
pub struct RealIndexBlocking {
    db: Database,
    arena: Arena,
}

impl RealIndexBlocking {
    pub fn new(arena: Arena, db: Database) -> anyhow::Result<Self> {
        {
            let txn = db.begin_write()?;
            txn.open_table(FILE_TABLE)?;
            txn.open_table(HISTORY_TABLE)?;
            txn.commit()?;
        }

        Ok(Self { arena, db })
    }

    /// The arena tied to this index.
    pub fn arena(&self) -> &Arena {
        &self.arena
    }

    /// Turn this instance into an async one.
    pub fn into_async(self) -> RealIndexAsync {
        RealIndexAsync::new(self)
    }

    /// Open or create an index at the given path.
    pub fn open(arena: Arena, path: &path::Path) -> anyhow::Result<Self> {
        Self::new(arena, Database::create(path)?)
    }

    /// Check whether a given file is in the index already.
    pub fn has_file(&self, path: &model::Path) -> anyhow::Result<bool> {
        let txn = self.db.begin_read()?;
        let file_table = txn.open_table(FILE_TABLE)?;

        Ok(file_table.get(path.as_str())?.is_some())
    }

    /// Check whether a given file is in the index with the given size and mtime.
    pub fn has_matching_file(
        &self,
        path: &model::Path,
        size: u64,
        mtime: &UnixTime,
    ) -> anyhow::Result<bool> {
        let txn = self.db.begin_read()?;
        let file_table = txn.open_table(FILE_TABLE)?;

        if let Some(entry) = file_table.get(path.as_str())? {
            let entry = entry.value().parse()?;
            return Ok(entry.size == size && entry.mtime == *mtime);
        }

        Ok(false)
    }

    /// Add a file entry with the given values. Replace one if it exists.
    pub fn add_file(
        &self,
        path: &model::Path,
        size: u64,
        mtime: &UnixTime,
        hash: Hash,
    ) -> anyhow::Result<()> {
        let txn = self.db.begin_write()?;
        do_add_file(&txn, path, size, mtime, hash)?;
        txn.commit()?;

        Ok(())
    }

    /// Send all valid entries of the file table to the given channel.
    pub fn all_files(&self, tx: mpsc::Sender<(model::Path, FileTableEntry)>) -> anyhow::Result<()> {
        let txn = self.db.begin_read()?;
        let file_table = txn.open_table(FILE_TABLE)?;
        for (path, entry) in file_table
            .iter()?
            .flatten()
            // Skip any entry with errors
            .flat_map(|(k, v)| {
                if let (Ok(path), Ok(entry)) = (model::Path::parse(k.value()), v.value().parse()) {
                    Some((path, entry))
                } else {
                    None
                }
            })
        {
            tx.blocking_send((path, entry))?;
        }

        Ok(())
    }

    /// Remove a path that can be a file or a directory.
    ///
    /// If the path is a directory, all files within that directory
    /// are removed, recursively.
    pub fn remove_file_or_dir(&self, path: &model::Path) -> anyhow::Result<()> {
        let txn = self.db.begin_write()?;
        do_remove_file_or_dir(&txn, path)?;
        txn.commit()?;

        Ok(())
    }
}

fn do_remove_file_or_dir(txn: &WriteTransaction, path: &model::Path) -> anyhow::Result<()> {
    let mut file_table = txn.open_table(FILE_TABLE)?;
    let mut history_table = txn.open_table(HISTORY_TABLE)?;
    let path_str = path.as_str();
    let mut range_end = path.to_string();
    range_end.push('0' /* '/' + 1 */);

    for entry in file_table.extract_from_if(path_str..range_end.as_ref(), |k, _| {
        k.strip_prefix(path_str)
            .map(|rest| rest == "" || rest.starts_with("/"))
            .unwrap_or(false)
    })? {
        let (k, v) = entry?;
        history_table.insert(
            next_history_index(&history_table)?,
            Holder::new(HistoryTableEntry::Remove(
                model::Path::parse(k.value())?,
                v.value().parse()?.hash,
            ))?,
        )?;
    }

    Ok(())
}

fn do_add_file(
    txn: &WriteTransaction,
    path: &model::Path,
    size: u64,
    mtime: &UnixTime,
    hash: Hash,
) -> anyhow::Result<()> {
    let mut file_table = txn.open_table(FILE_TABLE)?;
    let mut history_table = txn.open_table(HISTORY_TABLE)?;

    let existing = file_table.insert(
        path.as_str(),
        Holder::new(FileTableEntry {
            size,
            mtime: mtime.clone(),
            hash,
        })?,
    )?;
    let old_hash = existing
        .map(|e| e.value().parse().ok())
        .flatten()
        .map(|e| e.hash);
    history_table.insert(
        next_history_index(&history_table)?,
        Holder::new(if let Some(old_hash) = old_hash {
            HistoryTableEntry::Replace(path.clone(), old_hash)
        } else {
            HistoryTableEntry::Add(path.clone())
        })?,
    )?;

    Ok(())
}

fn next_history_index(
    history_table: &impl redb::ReadableTable<u64, Holder<'static, HistoryTableEntry>>,
) -> Result<u64, anyhow::Error> {
    let entry_index = 1 + history_table.last()?.map(|(k, _)| k.value()).unwrap_or(0);
    Ok(entry_index)
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

    /// The arena tied to this index.
    pub fn arena(&self) -> &Arena {
        &self.inner.arena
    }

    /// Open or create an index at the given path.
    pub async fn open(arena: Arena, path: &path::Path) -> anyhow::Result<RealIndexAsync> {
        let path = path.to_path_buf();
        Ok(Self::new(
            task::spawn_blocking(move || RealIndexBlocking::open(arena, &path)).await??,
        ))
    }

    /// Return a reference to the underlying blocking instance.
    pub fn blocking(&self) -> Arc<RealIndexBlocking> {
        Arc::clone(&self.inner)
    }

    /// Return all valid file entries as a stream.
    pub fn all_files(&self) -> ReceiverStream<(model::Path, FileTableEntry)> {
        let (tx, rx) = mpsc::channel(100);

        let inner = Arc::clone(&self.inner);
        task::spawn_blocking(move || inner.all_files(tx));

        ReceiverStream::new(rx)
    }

    /// Check whether a given file is in the index already.
    pub async fn has_file(&self, path: &model::Path) -> anyhow::Result<bool> {
        let inner = Arc::clone(&self.inner);
        let path = path.clone();

        task::spawn_blocking(move || inner.has_file(&path)).await?
    }

    /// Check whether a given file is in the index already with the given size and mtime.
    pub async fn has_matching_file(
        &self,
        path: &model::Path,
        size: u64,
        mtime: &UnixTime,
    ) -> anyhow::Result<bool> {
        let inner = Arc::clone(&self.inner);
        let path = path.clone();
        let mtime = mtime.clone();

        task::spawn_blocking(move || inner.has_matching_file(&path, size, &mtime)).await?
    }

    /// Remove a path that can be a file or a directory.
    ///
    /// If the path is a directory, all files within that directory
    /// are removed, recursively.
    pub async fn remove_file_or_dir(&self, path: &model::Path) -> anyhow::Result<()> {
        let inner = Arc::clone(&self.inner);
        let path = path.clone();

        task::spawn_blocking(move || inner.remove_file_or_dir(&path)).await?
    }

    pub async fn add_file(
        &self,
        path: &model::Path,
        size: u64,
        mtime: &UnixTime,
        hash: Hash,
    ) -> anyhow::Result<()> {
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

    fn to_bytes(self) -> Result<Vec<u8>, ByteConversionError> {
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
    Add(model::Path),
    Replace(model::Path, Hash),
    Remove(model::Path, Hash),
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

    fn to_bytes(self) -> Result<Vec<u8>, ByteConversionError> {
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

fn parse_path(path: capnp::text::Reader<'_>) -> Result<model::Path, ByteConversionError> {
    model::Path::parse(path.to_str()?).map_err(|_| ByteConversionError::Invalid("path"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_fs::TempDir;
    use futures::StreamExt as _;

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
        let add = HistoryTableEntry::Add(model::Path::parse("foo/bar.txt")?);
        assert_eq!(
            add,
            HistoryTableEntry::from_bytes(add.clone().to_bytes()?.as_slice())?
        );

        let remove =
            HistoryTableEntry::Remove(model::Path::parse("foo/bar.txt")?, Hash([0xfa; 32]));
        assert_eq!(
            remove,
            HistoryTableEntry::from_bytes(remove.clone().to_bytes()?.as_slice())?
        );

        let replace =
            HistoryTableEntry::Replace(model::Path::parse("foo/bar.txt")?, Hash([0x1a; 32]));
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
        let path = model::Path::parse("foo/bar.txt")?;
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
        let path = model::Path::parse("foo/bar.txt")?;
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
        let path = model::Path::parse("foo/bar")?;
        index.add_file(&path, 100, &mtime, Hash([0xfa; 32]))?;

        assert_eq!(true, fixture.index.has_file(&path)?);
        assert_eq!(false, index.has_file(&model::Path::parse("foo/bar/toto")?)?);
        assert_eq!(false, index.has_file(&model::Path::parse("other.txt")?)?);

        Ok(())
    }

    #[test]
    fn has_matching_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let index = &fixture.index;
        let mtime = UnixTime::from_secs(1234567890);
        let path = model::Path::parse("foo/bar")?;
        index.add_file(&path, 100, &mtime, Hash([0xfa; 32]))?;

        assert_eq!(true, fixture.index.has_matching_file(&path, 100, &mtime)?);
        assert_eq!(
            false,
            index.has_matching_file(&model::Path::parse("other")?, 100, &mtime)?
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
        let path = model::Path::parse("foo/bar.txt")?;
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

        index.add_file(&model::Path::parse("foo/a")?, 100, &mtime, Hash([0x01; 32]))?;
        index.add_file(&model::Path::parse("foo/b")?, 100, &mtime, Hash([0x02; 32]))?;
        index.add_file(&model::Path::parse("foo/c")?, 100, &mtime, Hash([0x03; 32]))?;
        index.add_file(
            &model::Path::parse("foobar")?,
            100,
            &mtime,
            Hash([0x04; 32]),
        )?;

        index.remove_file_or_dir(&model::Path::parse("foo")?)?;

        assert_eq!(true, index.has_file(&model::Path::parse("foobar")?)?);
        assert_eq!(false, index.has_file(&model::Path::parse("foo/a")?)?);
        assert_eq!(false, index.has_file(&model::Path::parse("foo/b")?)?);
        assert_eq!(false, index.has_file(&model::Path::parse("foo/c")?)?);

        {
            let txn = index.db.begin_read()?;
            let history_table = txn.open_table(HISTORY_TABLE)?;
            assert_eq!(
                HistoryTableEntry::Remove(model::Path::parse("foo/a")?, Hash([0x01; 32])),
                history_table.get(5)?.unwrap().value().parse()?
            );
            assert_eq!(
                HistoryTableEntry::Remove(model::Path::parse("foo/b")?, Hash([0x02; 32])),
                history_table.get(6)?.unwrap().value().parse()?
            );
            assert_eq!(
                HistoryTableEntry::Remove(model::Path::parse("foo/c")?, Hash([0x03; 32])),
                history_table.get(7)?.unwrap().value().parse()?
            );
        }

        Ok(())
    }

    #[test]
    fn remove_nothing() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let index = &fixture.index;
        index.remove_file_or_dir(&model::Path::parse("foo")?)?;

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
        let hash = Hash([0x01; 32]);
        index
            .add_file(&model::Path::parse("baa.txt")?, 100, &mtime, hash.clone())
            .await?;
        index
            .add_file(
                &model::Path::parse("baa/baa.txt")?,
                200,
                &mtime,
                hash.clone(),
            )
            .await?;
        index
            .add_file(
                &model::Path::parse("baa/baa/black/sheep.txt")?,
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
}
