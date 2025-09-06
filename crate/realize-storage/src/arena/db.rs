use super::arena_cache::WritableOpenCache;
use super::blob::{BlobReadOperations, Blobs, ReadableOpenBlob, WritableOpenBlob};
use super::dirty::{Dirty, DirtyReadOperations, ReadableOpenDirty, WritableOpenDirty};
use super::history::{History, HistoryReadOperations, ReadableOpenHistory, WritableOpenHistory};
use super::index::{IndexReadOperations, ReadableOpenIndex, WritableOpenIndex};
use super::mark::{MarkReadOperations, ReadableOpenMark, WritableOpenMark};
use super::peer::{PeersReadOperations, ReadableOpenPeers, WritableOpenPeers};
use super::tree::{ReadableOpenTree, Tree, TreeReadOperations, WritableOpenTree};
use super::types::CacheTableKey;
use super::types::{
    BlobTableEntry, CacheTableEntry, FailedJobTableEntry, FileTableEntry, HistoryTableEntry,
    MarkTableEntry, PeerTableEntry, QueueTableEntry,
};
use crate::StorageError;
use crate::arena::arena_cache;
use crate::utils::holder::{ByteConversionError, Holder};
use crate::{PathId, PathIdAllocator};
use realize_types::Arena;
use redb::{ReadableTable, Table, TableDefinition};
use std::cell::RefCell;
use std::panic::Location;
use std::sync::Arc;
use uuid::Uuid;

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

/// Tree branches and leaves, associated to pathids.
///
/// Key: (pathid, name)
/// Value: pathid
pub(crate) const TREE_TABLE: TableDefinition<(PathId, &str), PathId> = TableDefinition::new("tree");

/// Refcount for tree nodes
///
/// Key: pathid
/// Value: u32 (refcount)
pub(crate) const TREE_REFCOUNT_TABLE: TableDefinition<PathId, u32> =
    TableDefinition::new("tree.refcount");

/// Track peer files.
///
/// Each known peer file has an entry in this table, keyed with the
/// file pathid and the peer name. More than one peer might have the
/// same entry.
///
/// An pathid available in no peers should be remove from all
/// directories.
///
/// Key: CacheTableKey (pathid, default|local|peer, peer)
/// Value: CacheTableEntry
const CACHE_TABLE: TableDefinition<CacheTableKey, Holder<CacheTableEntry>> =
    TableDefinition::new("cache.file");

/// Track local indexed files.
///
/// Each locally indexed file has an entry in this table, keyed with
/// the file pathid.
///
/// Key: PathId
/// Value: FileTableEntry
const INDEX_TABLE: TableDefinition<PathId, Holder<FileTableEntry>> =
    TableDefinition::new("index.file");

/// Track peer files that might have been deleted remotely.
///
/// When a peer starts catchup of an arena, all its files are added to
/// this table. Calls to catchup for that peer and arena removes the
/// corresponding entry in the table. At the end of catchup, files
/// still in this table are deleted.
///
/// Key: (peer, file pathid)
/// Value: ()
const PENDING_CATCHUP_TABLE: TableDefinition<(&str, PathId), ()> =
    TableDefinition::new("acache.pending_catchup");

/// Track Peer UUIDs.
///
/// This table tracks the store UUID for each peer.
///
/// Key: &str (Peer)
/// Value: PeerTableEntry
const PEER_TABLE: TableDefinition<&str, Holder<PeerTableEntry>> =
    TableDefinition::new("acache.peer");

/// Track last seen notification index.
///
/// This table tracks the last seen notification index for each peer.
///
/// Key: &str (Peer)
/// Value: last seen index
const NOTIFICATION_TABLE: TableDefinition<&str, u64> = TableDefinition::new("acache.notification");

/// Track blobs.
///
/// Key: BlodId
/// Value: BlobTableEntry
const BLOB_TABLE: TableDefinition<PathId, Holder<BlobTableEntry>> = TableDefinition::new("blob");

/// Track the next blob ID to be allocated.
///
/// Key: () (unit key)
/// Value: PathId (next ID to allocate)

/// Track LRU queue for blobs.
///
/// Key: u16 (LRU Queue ID)
/// Value: QueueTableEntry
const BLOB_LRU_QUEUE_TABLE: TableDefinition<u16, Holder<QueueTableEntry>> =
    TableDefinition::new("blob.lru_queue");

/// Track current pathid range for each arena.
///
/// The current pathid is the last pathid that was allocated for the
/// arena.
///
/// Key: ()
/// Value: (PathId, PathId) (last pathid allocated, end of range)
pub(crate) const CURRENT_INODE_RANGE_TABLE: TableDefinition<(), (PathId, PathId)> =
    TableDefinition::new("acache.current_pathid_range");

/// Mark table for storing file marks within an Arena.
///
/// Key: &str (path)
/// Value: Holder<MarkTableEntry>
const MARK_TABLE: TableDefinition<PathId, Holder<MarkTableEntry>> = TableDefinition::new("mark");

/// Path marked dirty, indexed by path.
///
/// The path can be in the index, in the cache or both.
///
/// Each entry in this table has a corresponding entry in DIRTY_LOG_TABLE.
///
/// Key: &str (path)
/// Value: dirty counter (key of DIRTY_LOG_TABLE)
const DIRTY_TABLE: TableDefinition<PathId, u64> = TableDefinition::new("dirty");

/// Path marked dirty, indexed by an increasing counter.
///
/// Key: u64 (increasing counter)
/// Value: &str (path)
const DIRTY_LOG_TABLE: TableDefinition<u64, PathId> = TableDefinition::new("dirty_log");

/// Highest counter value for DIRTY_LOG_TABLE.
///
/// Can only be cleared if DIRTY_TABLE, DIRTY_LOG_TABLE and JOB_TABLE
/// are empty and there is no active stream of Jobs.
const DIRTY_COUNTER_TABLE: TableDefinition<(), u64> = TableDefinition::new("dirty_counter");

/// Stores job failures.
///
/// Key: u64 (key of the corresponding DIRTY_LOG_TABLE entry)
/// Value: FailedJobTableEntry
const FAILED_JOB_TABLE: TableDefinition<u64, Holder<FailedJobTableEntry>> =
    TableDefinition::new("failed_job");

pub(crate) struct ArenaDatabase {
    db: redb::Database,
    uuid: Uuid,
    arena: Arena,
    subsystems: Subsystems,
}

struct Subsystems {
    tree: Tree,
    dirty: Dirty,
    history: History,
    blobs: Blobs,
}

impl ArenaDatabase {
    #[cfg(test)]
    pub fn for_testing_single_arena<P: AsRef<std::path::Path>>(
        arena: realize_types::Arena,
        blob_dir: P,
    ) -> anyhow::Result<Arc<Self>> {
        ArenaDatabase::for_testing(
            arena,
            crate::PathIdAllocator::new(
                crate::GlobalDatabase::new(crate::utils::redb_utils::in_memory()?)?,
                [arena],
            )?,
            blob_dir,
        )
    }

    #[cfg(test)]
    pub fn for_testing<P: AsRef<std::path::Path>>(
        arena: realize_types::Arena,
        allocator: Arc<crate::PathIdAllocator>,
        blob_dir: P,
    ) -> anyhow::Result<Arc<Self>> {
        Ok(ArenaDatabase::new(
            crate::utils::redb_utils::in_memory()?,
            arena,
            allocator,
            blob_dir,
        )?)
    }

    pub fn new<P: AsRef<std::path::Path>>(
        db: redb::Database,
        arena: Arena,
        allocator: Arc<PathIdAllocator>,
        blob_dir: P,
    ) -> Result<Arc<Self>, StorageError> {
        let tree = Tree::new(arena, allocator)?;
        let dirty: Dirty;
        let history: History;
        let blobs: Blobs;
        let uuid: Uuid;
        let txn = db.begin_write()?;
        {
            // Create tables so they can safely be queried in read
            // transactions in an empty database.
            let history_table = txn.open_table(HISTORY_TABLE)?;
            let mut settings_table = txn.open_table(SETTINGS_TABLE)?;
            txn.open_table(TREE_TABLE)?;
            txn.open_table(TREE_REFCOUNT_TABLE)?;
            let mut cache_table = txn.open_table(CACHE_TABLE)?;
            txn.open_table(INDEX_TABLE)?;
            txn.open_table(PENDING_CATCHUP_TABLE)?;
            txn.open_table(PEER_TABLE)?;
            txn.open_table(NOTIFICATION_TABLE)?;
            txn.open_table(CURRENT_INODE_RANGE_TABLE)?;
            txn.open_table(BLOB_TABLE)?;
            let blob_lru_queue_table = txn.open_table(BLOB_LRU_QUEUE_TABLE)?;
            txn.open_table(MARK_TABLE)?;
            txn.open_table(DIRTY_TABLE)?;
            let dirty_log_table = txn.open_table(DIRTY_LOG_TABLE)?;
            txn.open_table(DIRTY_COUNTER_TABLE)?;
            txn.open_table(FAILED_JOB_TABLE)?;

            dirty = Dirty::setup(&dirty_log_table)?;
            history = History::setup(arena, &history_table)?;
            blobs = Blobs::setup(blob_dir.as_ref(), &blob_lru_queue_table)?;
            uuid = load_or_assign_uuid(&mut settings_table)?;
            arena_cache::init(&mut cache_table, tree.root())?;
        }
        txn.commit()?;

        Ok(Arc::new(Self {
            db,
            arena,
            uuid,
            subsystems: Subsystems {
                tree,
                dirty,
                history,
                blobs,
            },
        }))
    }

    pub fn uuid(&self) -> &Uuid {
        &self.uuid
    }

    #[allow(dead_code)]
    pub fn arena(&self) -> Arena {
        self.arena
    }

    /// Return handle on the Tree subsystem.
    #[allow(dead_code)]
    pub fn tree(&self) -> &Tree {
        &self.subsystems.tree
    }

    /// Return handle on the Dirty subsystem.
    #[allow(dead_code)]
    pub fn dirty(&self) -> &Dirty {
        &self.subsystems.dirty
    }

    /// Return handle on the History subsystem.
    #[allow(dead_code)]
    pub fn history(&self) -> &History {
        &self.subsystems.history
    }

    /// Return handle on the Blobs subsystem.
    pub fn blobs(&self) -> &Blobs {
        &self.subsystems.blobs
    }

    pub fn begin_write(&self) -> Result<ArenaWriteTransaction<'_>, StorageError> {
        Ok(ArenaWriteTransaction {
            inner: self.db.begin_write()?,
            arena: self.arena,
            subsystems: &self.subsystems,
            before_commit: BeforeCommit::new(),
            after_commit: AfterCommit::new(),
        })
    }

    pub fn begin_read(&self) -> Result<ArenaReadTransaction<'_>, StorageError> {
        Ok(ArenaReadTransaction {
            inner: self.db.begin_read()?,
            arena: self.arena,
            subsystems: &self.subsystems,
        })
    }
}

pub struct ArenaWriteTransaction<'db> {
    inner: redb::WriteTransaction,
    arena: Arena,
    subsystems: &'db Subsystems,

    /// Callbacks to be run after the transaction is committed.
    ///
    /// Using a RefCell to avoid issues, as transactions are pretty
    /// much always borrowed immutably, with the tables it would be
    /// impractical to have pass around mutable references to
    /// transactions.
    after_commit: AfterCommit,

    /// Callbacks to be run before the transaction is committed.
    ///
    /// These callbacks can interrupt the commit by returning an
    /// error.
    before_commit: BeforeCommit,
}

impl<'db> ArenaWriteTransaction<'db> {
    /// Commit the changes.
    ///
    /// If the transaction is successfully committed, functions
    /// registered by after_commit are run, and these may fail.
    pub fn commit(self) -> Result<(), StorageError> {
        self.before_commit.run_all(&self)?;
        self.inner.commit()?;
        self.after_commit.run_all();
        Ok(())
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn read_cache(
        &self,
    ) -> Result<impl crate::arena::arena_cache::CacheReadOperations, StorageError> {
        Ok(crate::arena::arena_cache::ReadableOpenCache::new(
            self.inner
                .open_table(CACHE_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.arena,
        ))
    }

    #[track_caller]
    pub(crate) fn write_cache(
        &self,
    ) -> Result<crate::arena::arena_cache::WritableOpenCache<'_>, StorageError> {
        Ok(WritableOpenCache::new(
            self.inner
                .open_table(CACHE_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.inner.open_table(PENDING_CATCHUP_TABLE)?,
            self.arena,
        ))
    }

    #[track_caller]
    #[allow(dead_code)]
    pub(crate) fn read_peers(&self) -> Result<impl PeersReadOperations, StorageError> {
        Ok(ReadableOpenPeers::new(
            self.inner
                .open_table(PEER_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.inner.open_table(NOTIFICATION_TABLE)?,
        ))
    }

    #[track_caller]
    pub(crate) fn write_peers(&self) -> Result<WritableOpenPeers<'_>, StorageError> {
        Ok(WritableOpenPeers::new(
            self.inner
                .open_table(PEER_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.inner.open_table(NOTIFICATION_TABLE)?,
        ))
    }

    pub fn after_commit(&self, cb: impl FnOnce() -> () + Send + 'static) {
        self.after_commit.add(cb)
    }

    #[track_caller]
    pub(crate) fn read_tree(&self) -> Result<impl TreeReadOperations, StorageError> {
        Ok(ReadableOpenTree::new(
            self.inner
                .open_table(TREE_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            &self.subsystems.tree,
        ))
    }

    #[track_caller]
    pub(crate) fn write_tree(&self) -> Result<WritableOpenTree<'_>, StorageError> {
        Ok(WritableOpenTree::new(
            &self.before_commit,
            self.inner
                .open_table(TREE_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.inner.open_table(TREE_REFCOUNT_TABLE)?,
            self.inner.open_table(CURRENT_INODE_RANGE_TABLE)?,
            &self.subsystems.tree,
        ))
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn read_dirty(&self) -> Result<impl DirtyReadOperations, StorageError> {
        Ok(ReadableOpenDirty::new(
            self.inner
                .open_table(DIRTY_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.inner.open_table(DIRTY_LOG_TABLE)?,
            self.inner.open_table(FAILED_JOB_TABLE)?,
        ))
    }

    #[track_caller]
    pub(crate) fn write_dirty(&self) -> Result<WritableOpenDirty<'_>, StorageError> {
        Ok(WritableOpenDirty::new(
            &self.after_commit,
            &self.subsystems.dirty,
            self.inner
                .open_table(DIRTY_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.inner.open_table(DIRTY_LOG_TABLE)?,
            self.inner.open_table(FAILED_JOB_TABLE)?,
            self.inner.open_table(DIRTY_COUNTER_TABLE)?,
            self.arena,
        ))
    }
    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn read_history(&self) -> Result<impl HistoryReadOperations, StorageError> {
        Ok(ReadableOpenHistory::new(
            self.inner
                .open_table(HISTORY_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
        ))
    }

    #[track_caller]
    pub(crate) fn write_history(&self) -> Result<WritableOpenHistory<'_>, StorageError> {
        Ok(WritableOpenHistory::new(
            &self.after_commit,
            &self.subsystems.history,
            self.inner
                .open_table(HISTORY_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
        ))
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn read_marks(&self) -> Result<impl MarkReadOperations, StorageError> {
        Ok(ReadableOpenMark::new(
            self.inner
                .open_table(MARK_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
        ))
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn write_marks(&self) -> Result<WritableOpenMark<'_>, StorageError> {
        Ok(WritableOpenMark::new(
            self.inner
                .open_table(MARK_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
        ))
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn read_blobs(&self) -> Result<impl BlobReadOperations, StorageError> {
        Ok(ReadableOpenBlob::new(
            self.inner
                .open_table(BLOB_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.inner.open_table(BLOB_LRU_QUEUE_TABLE)?,
        ))
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn write_blobs(&self) -> Result<WritableOpenBlob<'_>, StorageError> {
        Ok(WritableOpenBlob::new(
            &self.before_commit,
            self.inner
                .open_table(BLOB_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.inner.open_table(BLOB_LRU_QUEUE_TABLE)?,
            &self.subsystems.blobs,
            self.arena,
        ))
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn read_index(&self) -> Result<impl IndexReadOperations, StorageError> {
        Ok(ReadableOpenIndex::new(
            self.inner
                .open_table(INDEX_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
        ))
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn write_index(&self) -> Result<WritableOpenIndex<'_>, StorageError> {
        Ok(WritableOpenIndex::new(
            self.inner
                .open_table(INDEX_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
        ))
    }
}

pub struct ArenaReadTransaction<'db> {
    inner: redb::ReadTransaction,
    arena: Arena,
    subsystems: &'db Subsystems,
}

impl<'db> ArenaReadTransaction<'db> {
    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn read_tree(&self) -> Result<impl TreeReadOperations, StorageError> {
        Ok(ReadableOpenTree::new(
            self.inner
                .open_table(TREE_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            &self.subsystems.tree,
        ))
    }

    #[track_caller]
    pub(crate) fn read_dirty(&self) -> Result<impl DirtyReadOperations, StorageError> {
        Ok(ReadableOpenDirty::new(
            self.inner
                .open_table(DIRTY_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.inner.open_table(DIRTY_LOG_TABLE)?,
            self.inner.open_table(FAILED_JOB_TABLE)?,
        ))
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn read_history(&self) -> Result<impl HistoryReadOperations, StorageError> {
        Ok(ReadableOpenHistory::new(
            self.inner
                .open_table(HISTORY_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
        ))
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn read_marks(&self) -> Result<impl MarkReadOperations, StorageError> {
        Ok(ReadableOpenMark::new(
            self.inner
                .open_table(MARK_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
        ))
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn read_blobs(&self) -> Result<impl BlobReadOperations, StorageError> {
        Ok(ReadableOpenBlob::new(
            self.inner
                .open_table(BLOB_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.inner.open_table(BLOB_LRU_QUEUE_TABLE)?,
        ))
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn read_index(&self) -> Result<impl IndexReadOperations, StorageError> {
        Ok(ReadableOpenIndex::new(
            self.inner
                .open_table(INDEX_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
        ))
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn read_cache(
        &self,
    ) -> Result<impl crate::arena::arena_cache::CacheReadOperations, StorageError> {
        Ok(crate::arena::arena_cache::ReadableOpenCache::new(
            self.inner
                .open_table(CACHE_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.arena,
        ))
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn read_peers(&self) -> Result<impl PeersReadOperations, StorageError> {
        Ok(ReadableOpenPeers::new(
            self.inner
                .open_table(PEER_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.inner.open_table(NOTIFICATION_TABLE)?,
        ))
    }
}

/// Callbacks that run after a transaction is committed.
///
/// These callbacks cannot fail and are run after the transaction has been
/// successfully committed to the database.
pub(crate) struct AfterCommit {
    inner: RefCell<Vec<Box<dyn FnOnce() -> () + Send + 'static>>>,
}

impl AfterCommit {
    /// Create a new empty after-commit callback collection.
    pub(crate) fn new() -> Self {
        Self {
            inner: RefCell::new(vec![]),
        }
    }

    /// Add a callback to be run after the transaction is committed.
    pub(crate) fn add(&self, cb: impl FnOnce() -> () + Send + 'static) {
        self.inner.borrow_mut().push(Box::new(cb));
    }

    /// Run all registered callbacks.
    ///
    /// This consumes the callbacks, so they can only be run once.
    pub(crate) fn run_all(self) {
        for cb in self.inner.into_inner() {
            cb();
        }
    }
}

/// Callbacks that run before a transaction is committed.
///
/// These callbacks can interrupt the commit by returning an error.
pub(crate) struct BeforeCommit {
    inner: RefCell<
        Vec<Box<dyn FnOnce(&ArenaWriteTransaction) -> Result<(), StorageError> + Send + 'static>>,
    >,
}

impl BeforeCommit {
    /// Create a new empty before-commit callback collection.
    pub(crate) fn new() -> Self {
        Self {
            inner: RefCell::new(vec![]),
        }
    }

    /// Add a callback to be run before the transaction is committed.
    pub(crate) fn add(
        &self,
        cb: impl FnOnce(&ArenaWriteTransaction) -> Result<(), StorageError> + Send + 'static,
    ) {
        self.inner.borrow_mut().push(Box::new(cb));
    }

    /// Run all registered callbacks in order.
    ///
    /// Returns an error if any callback fails, which will interrupt the commit.
    pub(crate) fn run_all(&self, txn: &ArenaWriteTransaction) -> Result<(), StorageError> {
        while let cbs = self.inner.take()
            && !cbs.is_empty()
        {
            for cb in cbs {
                cb(txn)?;
            }
        }
        Ok(())
    }
}

fn load_or_assign_uuid(
    settings_table: &mut Table<'_, &'static str, &'static [u8]>,
) -> Result<Uuid, StorageError> {
    if let Some(value) = settings_table.get("uuid")? {
        let bytes: uuid::Bytes = value
            .value()
            .try_into()
            .map_err(|_| ByteConversionError::Invalid("uuid"))?;

        Ok(Uuid::from_bytes(bytes))
    } else {
        let uuid = Uuid::now_v7();
        let bytes: &[u8] = uuid.as_bytes();
        settings_table.insert("uuid", &bytes)?;

        Ok(uuid)
    }
}

#[cfg(test)]
mod tests {
    use crate::{GlobalDatabase, utils::redb_utils};

    use super::*;
    use assert_fs::TempDir;
    use realize_types::Arena;
    use redb::{ReadOnlyTable, ReadableTable};

    const TEST_TABLE: TableDefinition<&str, &str> = TableDefinition::new("test");

    struct Fixture {
        db: Arc<ArenaDatabase>,
    }

    impl Fixture {
        fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();

            let db = ArenaDatabase::for_testing_single_arena(
                Arena::from("myarena"),
                std::path::Path::new("/dev/null"),
            )?;

            Ok(Self { db })
        }
    }

    #[test]
    fn create_databases() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        // Make sure the tables can be opened in a read transaction.
        let txn = fixture.db.begin_read()?;
        txn.read_tree()?;
        txn.read_blobs()?;
        txn.read_marks()?;
        txn.read_dirty()?;
        txn.read_history()?;
        txn.read_index()?;

        Ok(())
    }

    fn test_table_content(
        test_table: ReadOnlyTable<&str, &str>,
    ) -> Result<Vec<(String, String)>, anyhow::Error> {
        let result = test_table
            .iter()?
            .map(|r| r.and_then(|(k, v)| Ok((k.value().to_string(), v.value().to_string()))))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(result)
    }

    #[test]
    fn before_commit() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let txn = fixture.db.begin_write()?;
        {
            let mut test_table = txn.inner.open_table(TEST_TABLE)?;
            txn.before_commit.add(|txn| {
                let mut test_table = txn.inner.open_table(TEST_TABLE)?;
                test_table.insert("2", "before_commit")?;
                Ok(())
            });
            test_table.insert("1", "normal")?;
        }
        txn.commit()?;

        let txn = fixture.db.begin_read()?;
        let test_table = txn.inner.open_table(TEST_TABLE)?;
        let result = test_table_content(test_table)?;
        assert_eq!(
            vec![
                ("1".to_string(), "normal".to_string()),
                ("2".to_string(), "before_commit".to_string())
            ],
            result
        );

        Ok(())
    }

    #[test]
    fn before_commit_registers_another() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let txn = fixture.db.begin_write()?;
        {
            let mut test_table = txn.inner.open_table(TEST_TABLE)?;
            txn.before_commit.add(|txn| {
                let mut test_table = txn.inner.open_table(TEST_TABLE)?;
                test_table.insert("2", "before_commit 1")?;

                txn.before_commit.add(|txn| {
                    let mut test_table = txn.inner.open_table(TEST_TABLE)?;
                    test_table.insert("3", "before_commit 2")?;

                    Ok(())
                });
                Ok(())
            });
            test_table.insert("1", "normal")?;
        }
        txn.commit()?;

        let txn = fixture.db.begin_read()?;
        let test_table = txn.inner.open_table(TEST_TABLE)?;
        let result = test_table_content(test_table)?;
        assert_eq!(
            vec![
                ("1".to_string(), "normal".to_string()),
                ("2".to_string(), "before_commit 1".to_string()),
                ("3".to_string(), "before_commit 2".to_string())
            ],
            result
        );

        Ok(())
    }

    #[test]
    fn reopen_keeps_uuid() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        let tempdir = TempDir::new()?;
        let dbpath = tempdir.join("myarena.db");
        let blob_dir = tempdir.join("blobs");
        let arena = Arena::from("myarena");
        let allocator =
            PathIdAllocator::new(GlobalDatabase::new(redb_utils::in_memory()?)?, [arena])?;
        let db = ArenaDatabase::new(
            redb::Database::create(&dbpath)?,
            arena,
            Arc::clone(&allocator),
            &blob_dir,
        )?;

        let uuid = db.uuid().clone();
        assert!(!uuid.is_nil());
        drop(db);

        let db = ArenaDatabase::new(
            redb::Database::create(&dbpath)?,
            arena,
            Arc::clone(&allocator),
            &blob_dir,
        )?;
        assert_eq!(uuid, *db.uuid());

        Ok(())
    }
}
