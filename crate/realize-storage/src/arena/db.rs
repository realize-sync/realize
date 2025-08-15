use super::dirty::{Dirty, DirtyReadOperations, ReadableOpenDirty, WritableOpenDirty};
use super::history::{History, HistoryReadOperations, ReadableOpenHistory, WritableOpenHistory};
use super::tree::{ReadableOpenTree, Tree, TreeReadOperations, WritableOpenTree};
use super::types::CacheTableKey;
use super::types::{
    BlobTableEntry, CacheTableEntry, FailedJobTableEntry, FileTableEntry, HistoryTableEntry,
    MarkTableEntry, PeerTableEntry, QueueTableEntry,
};
use crate::StorageError;
use crate::types::BlobId;
use crate::utils::holder::Holder;
use crate::{Inode, InodeAllocator};
use realize_types::Arena;
use redb::{ReadOnlyTable, Table, TableDefinition};
use std::cell::RefCell;
use std::sync::Arc;

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
const SETTIGS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("index.settings");

/// Tree branches and leaves, associated to inodes.
///
/// Key: (inode, name)
/// Value: inode
pub(crate) const TREE_TABLE: TableDefinition<(Inode, &str), Inode> = TableDefinition::new("tree");

/// Refcount for tree nodes
///
/// Key: inode
/// Value: u32 (refcount)
pub(crate) const TREE_REFCOUNT_TABLE: TableDefinition<Inode, u32> =
    TableDefinition::new("tree.refcount");

/// Track peer files.
///
/// Each known peer file has an entry in this table, keyed with the
/// file inode and the peer name. More than one peer might have the
/// same entry.
///
/// An inode available in no peers should be remove from all
/// directories.
///
/// Key: CacheTableKey (inode, default|local|peer, peer)
/// Value: CacheTableEntry
const CACHE_TABLE: TableDefinition<CacheTableKey, Holder<CacheTableEntry>> =
    TableDefinition::new("cache.file");

/// Track local indexed files.
///
/// Each locally indexed file has an entry in this table, keyed with
/// the file inode.
///
/// Key: Inode
/// Value: FileTableEntry
const INDEX_TABLE: TableDefinition<Inode, Holder<FileTableEntry>> =
    TableDefinition::new("index.file");

/// Track peer files that might have been deleted remotely.
///
/// When a peer starts catchup of an arena, all its files are added to
/// this table. Calls to catchup for that peer and arena removes the
/// corresponding entry in the table. At the end of catchup, files
/// still in this table are deleted.
///
/// Key: (peer, file inode)
/// Value: ()
const PENDING_CATCHUP_TABLE: TableDefinition<(&str, Inode), ()> =
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
const BLOB_TABLE: TableDefinition<BlobId, Holder<BlobTableEntry>> = TableDefinition::new("blob");

/// Track the next blob ID to be allocated.
///
/// Key: () (unit key)
/// Value: BlobId (next ID to allocate)


/// Track LRU queue for blobs.
///
/// Key: u16 (LRU Queue ID)
/// Value: QueueTableEntry
const BLOB_LRU_QUEUE_TABLE: TableDefinition<u16, Holder<QueueTableEntry>> =
    TableDefinition::new("blob.lru_queue");

/// Track current inode range for each arena.
///
/// The current inode is the last inode that was allocated for the
/// arena.
///
/// Key: ()
/// Value: (Inode, Inode) (last inode allocated, end of range)
pub(crate) const CURRENT_INODE_RANGE_TABLE: TableDefinition<(), (Inode, Inode)> =
    TableDefinition::new("acache.current_inode_range");

/// Mark table for storing file marks within an Arena.
///
/// Key: &str (path)
/// Value: Holder<MarkTableEntry>
const MARK_TABLE: TableDefinition<Inode, Holder<MarkTableEntry>> = TableDefinition::new("mark");

/// Path marked dirty, indexed by path.
///
/// The path can be in the index, in the cache or both.
///
/// Each entry in this table has a corresponding entry in DIRTY_LOG_TABLE.
///
/// Key: &str (path)
/// Value: dirty counter (key of DIRTY_LOG_TABLE)
const DIRTY_TABLE: TableDefinition<&str, u64> = TableDefinition::new("engine.dirty");

/// Path marked dirty, indexed by an increasing counter.
///
/// Key: u64 (increasing counter)
/// Value: &str (path)
const DIRTY_LOG_TABLE: TableDefinition<u64, &str> = TableDefinition::new("engine.dirty_log");

/// Highest counter value for DIRTY_LOG_TABLE.
///
/// Can only be cleared if DIRTY_TABLE, DIRTY_LOG_TABLE and JOB_TABLE
/// are empty and there is no active stream of Jobs.
const DIRTY_COUNTER_TABLE: TableDefinition<(), u64> = TableDefinition::new("engine.dirty_counter");

/// Stores job failures.
///
/// Key: u64 (key of the corresponding DIRTY_LOG_TABLE entry)
/// Value: FailedJobTableEntry
const FAILED_JOB_TABLE: TableDefinition<u64, Holder<FailedJobTableEntry>> =
    TableDefinition::new("engine.failed_job");

pub(crate) struct ArenaDatabase {
    db: redb::Database,
    arena: Arena,
    subsystems: Subsystems,
}

struct Subsystems {
    tree: Tree,
    dirty: Dirty,
    history: History,
}

impl ArenaDatabase {
    #[cfg(test)]
    pub fn for_testing_single_arena(arena: realize_types::Arena) -> anyhow::Result<Arc<Self>> {
        ArenaDatabase::for_testing(
            arena,
            crate::InodeAllocator::new(
                crate::GlobalDatabase::new(crate::utils::redb_utils::in_memory()?)?,
                [arena],
            )?,
        )
    }

    #[cfg(test)]
    pub fn for_testing(
        arena: realize_types::Arena,
        allocator: Arc<crate::InodeAllocator>,
    ) -> anyhow::Result<Arc<Self>> {
        Ok(ArenaDatabase::new(
            crate::utils::redb_utils::in_memory()?,
            arena,
            allocator,
        )?)
    }

    pub fn new(
        db: redb::Database,
        arena: Arena,
        allocator: Arc<InodeAllocator>,
    ) -> Result<Arc<Self>, StorageError> {
        let tree = Tree::new(arena, allocator)?;
        let dirty: Dirty;
        let history: History;
        let txn = db.begin_write()?;
        {
            // Create tables so they can safely be queried in read
            // transactions in an empty database.
            let history_table = txn.open_table(HISTORY_TABLE)?;
            txn.open_table(SETTIGS_TABLE)?;
            txn.open_table(TREE_TABLE)?;
            txn.open_table(TREE_REFCOUNT_TABLE)?;
            txn.open_table(CACHE_TABLE)?;
            txn.open_table(INDEX_TABLE)?;
            txn.open_table(PENDING_CATCHUP_TABLE)?;
            txn.open_table(PEER_TABLE)?;
            txn.open_table(NOTIFICATION_TABLE)?;
            txn.open_table(CURRENT_INODE_RANGE_TABLE)?;
            txn.open_table(BLOB_TABLE)?;
    
            txn.open_table(BLOB_LRU_QUEUE_TABLE)?;
            txn.open_table(MARK_TABLE)?;
            txn.open_table(DIRTY_TABLE)?;
            txn.open_table(DIRTY_LOG_TABLE)?;
            let dirty_counter_table = txn.open_table(DIRTY_COUNTER_TABLE)?;
            txn.open_table(FAILED_JOB_TABLE)?;

            dirty = Dirty::new(&dirty_counter_table)?;
            history = History::new(arena, &history_table)?;
        }
        txn.commit()?;

        Ok(Arc::new(Self {
            db,
            arena,
            subsystems: Subsystems {
                tree,
                dirty,
                history,
            },
        }))
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

    pub fn begin_write(&self) -> Result<ArenaWriteTransaction<'_>, StorageError> {
        Ok(ArenaWriteTransaction {
            inner: self.db.begin_write()?,
            subsystems: &self.subsystems,
            before_commit: RefCell::new(vec![]),
            after_commit: RefCell::new(vec![]),
        })
    }

    pub fn begin_read(&self) -> Result<ArenaReadTransaction<'_>, StorageError> {
        Ok(ArenaReadTransaction {
            inner: self.db.begin_read()?,
            subsystems: &self.subsystems,
        })
    }
}

pub struct ArenaWriteTransaction<'db> {
    inner: redb::WriteTransaction,
    subsystems: &'db Subsystems,

    /// Callbacks to be run after the transaction is committed.
    ///
    /// Using a RefCell to avoid issues, as transactions are pretty
    /// much always borrowed immutably, with the tables it would be
    /// impractical to have pass around mutable references to
    /// transactions.
    after_commit: RefCell<Vec<Box<dyn FnOnce() -> () + Send + 'static>>>,

    /// Callbacks to be run before the transaction is committed.
    ///
    /// These callbacks can interrupt the commit by returning an
    /// error.
    before_commit: RefCell<
        Vec<Box<dyn FnOnce(&ArenaWriteTransaction) -> Result<(), StorageError> + Send + 'static>>,
    >,
}

impl<'db> ArenaWriteTransaction<'db> {
    /// Commit the changes.
    ///
    /// If the transaction is successfully committed, functions
    /// registered by after_commit are run, and these may fail.
    pub fn commit(self) -> Result<(), StorageError> {
        while let cbs = self.before_commit.take()
            && !cbs.is_empty()
        {
            for cb in cbs {
                (cb)(&self)?;
            }
        }
        self.inner.commit()?;
        for cb in self.after_commit.into_inner() {
            cb();
        }
        Ok(())
    }

    /// Register a function to be run just before committing the current transaction.
    ///
    /// Before commit functions are run in order from the time commit() is called until either
    /// there isn't one left or one of them fails..
    pub fn before_commit(
        &self,
        cb: impl FnOnce(&ArenaWriteTransaction) -> Result<(), StorageError> + Send + 'static,
    ) {
        self.before_commit.borrow_mut().push(Box::new(cb));
    }

    /// Register a function to be run after the current transaction
    /// has been successfully committed.
    ///
    /// After commit functions are run in order after a successful commit.
    pub fn after_commit(&self, cb: impl FnOnce() -> () + Send + 'static) {
        self.after_commit.borrow_mut().push(Box::new(cb));
    }

    pub fn settings_table<'txn>(
        &'txn self,
    ) -> Result<Table<'txn, &'static str, &'static [u8]>, StorageError> {
        Ok(self.inner.open_table(SETTIGS_TABLE)?)
    }

    pub fn cache_table<'txn>(
        &'txn self,
    ) -> Result<Table<'txn, CacheTableKey, Holder<'static, CacheTableEntry>>, StorageError> {
        Ok(self.inner.open_table(CACHE_TABLE)?)
    }

    pub fn index_table<'txn>(
        &'txn self,
    ) -> Result<Table<'txn, Inode, Holder<'static, FileTableEntry>>, StorageError> {
        Ok(self.inner.open_table(INDEX_TABLE)?)
    }

    pub fn pending_catchup_table<'txn>(
        &'txn self,
    ) -> Result<Table<'txn, (&'static str, Inode), ()>, StorageError> {
        Ok(self.inner.open_table(PENDING_CATCHUP_TABLE)?)
    }

    pub fn peer_table<'txn>(
        &'txn self,
    ) -> Result<Table<'txn, &'static str, Holder<'static, PeerTableEntry>>, StorageError> {
        Ok(self.inner.open_table(PEER_TABLE)?)
    }

    pub fn notification_table<'txn>(
        &'txn self,
    ) -> Result<Table<'txn, &'static str, u64>, StorageError> {
        Ok(self.inner.open_table(NOTIFICATION_TABLE)?)
    }

    pub fn blob_table<'txn>(
        &'txn self,
    ) -> Result<Table<'txn, BlobId, Holder<'static, BlobTableEntry>>, StorageError> {
        Ok(self.inner.open_table(BLOB_TABLE)?)
    }

    pub fn blob_lru_queue_table<'txn>(
        &'txn self,
    ) -> Result<Table<'txn, u16, Holder<'static, QueueTableEntry>>, StorageError> {
        Ok(self.inner.open_table(BLOB_LRU_QUEUE_TABLE)?)
    }



    pub fn mark_table<'txn>(
        &'txn self,
    ) -> Result<Table<'txn, Inode, Holder<'static, MarkTableEntry>>, StorageError> {
        Ok(self.inner.open_table(MARK_TABLE)?)
    }

    pub(crate) fn read_tree(&self) -> Result<impl TreeReadOperations, StorageError> {
        Ok(ReadableOpenTree::new(
            self.inner.open_table(TREE_TABLE)?,
            &self.subsystems.tree,
        ))
    }

    pub(crate) fn write_tree(&self) -> Result<WritableOpenTree<'_>, StorageError> {
        Ok(WritableOpenTree::new(
            &self,
            self.inner.open_table(TREE_TABLE)?,
            self.inner.open_table(TREE_REFCOUNT_TABLE)?,
            self.inner.open_table(CURRENT_INODE_RANGE_TABLE)?,
            &self.subsystems.tree,
        ))
    }

    #[allow(dead_code)]
    pub(crate) fn read_dirty(&self) -> Result<impl DirtyReadOperations, StorageError> {
        Ok(ReadableOpenDirty::new(
            self.inner.open_table(DIRTY_TABLE)?,
            self.inner.open_table(DIRTY_LOG_TABLE)?,
            self.inner.open_table(FAILED_JOB_TABLE)?,
        ))
    }

    pub(crate) fn write_dirty(&self) -> Result<WritableOpenDirty<'_>, StorageError> {
        Ok(WritableOpenDirty::new(
            &self,
            &self.subsystems.dirty,
            self.inner.open_table(DIRTY_TABLE)?,
            self.inner.open_table(DIRTY_LOG_TABLE)?,
            self.inner.open_table(FAILED_JOB_TABLE)?,
            self.inner.open_table(DIRTY_COUNTER_TABLE)?,
        ))
    }
    #[allow(dead_code)]
    pub(crate) fn read_history(&self) -> Result<impl HistoryReadOperations, StorageError> {
        Ok(ReadableOpenHistory::new(
            self.inner.open_table(HISTORY_TABLE)?,
        ))
    }

    pub(crate) fn write_history(&self) -> Result<WritableOpenHistory<'_>, StorageError> {
        Ok(WritableOpenHistory::new(
            &self,
            &self.subsystems.history,
            self.inner.open_table(HISTORY_TABLE)?,
        ))
    }
}

pub struct ArenaReadTransaction<'db> {
    inner: redb::ReadTransaction,
    subsystems: &'db Subsystems,
}

impl<'db> ArenaReadTransaction<'db> {
    pub fn cache_table(
        &self,
    ) -> Result<ReadOnlyTable<CacheTableKey, Holder<'static, CacheTableEntry>>, StorageError> {
        Ok(self.inner.open_table(CACHE_TABLE)?)
    }

    pub fn index_table(
        &self,
    ) -> Result<ReadOnlyTable<Inode, Holder<'static, FileTableEntry>>, StorageError> {
        Ok(self.inner.open_table(INDEX_TABLE)?)
    }

    pub fn peer_table(
        &self,
    ) -> Result<ReadOnlyTable<&'static str, Holder<'static, PeerTableEntry>>, StorageError> {
        Ok(self.inner.open_table(PEER_TABLE)?)
    }

    pub fn notification_table(&self) -> Result<ReadOnlyTable<&'static str, u64>, StorageError> {
        Ok(self.inner.open_table(NOTIFICATION_TABLE)?)
    }

    pub fn blob_table(
        &self,
    ) -> Result<ReadOnlyTable<BlobId, Holder<'static, BlobTableEntry>>, StorageError> {
        Ok(self.inner.open_table(BLOB_TABLE)?)
    }

    #[allow(dead_code)]
    pub fn blob_lru_queue_table(
        &self,
    ) -> Result<ReadOnlyTable<u16, Holder<'static, QueueTableEntry>>, StorageError> {
        Ok(self.inner.open_table(BLOB_LRU_QUEUE_TABLE)?)
    }

    #[allow(dead_code)]


    pub fn mark_table(
        &self,
    ) -> Result<ReadOnlyTable<Inode, Holder<'static, MarkTableEntry>>, StorageError> {
        Ok(self.inner.open_table(MARK_TABLE)?)
    }

    pub(crate) fn read_tree(&self) -> Result<impl TreeReadOperations, StorageError> {
        Ok(ReadableOpenTree::new(
            self.inner.open_table(TREE_TABLE)?,
            &self.subsystems.tree,
        ))
    }

    pub(crate) fn read_dirty(&self) -> Result<impl DirtyReadOperations, StorageError> {
        Ok(ReadableOpenDirty::new(
            self.inner.open_table(DIRTY_TABLE)?,
            self.inner.open_table(DIRTY_LOG_TABLE)?,
            self.inner.open_table(FAILED_JOB_TABLE)?,
        ))
    }

    #[allow(dead_code)]
    pub(crate) fn read_history(&self) -> Result<impl HistoryReadOperations, StorageError> {
        Ok(ReadableOpenHistory::new(
            self.inner.open_table(HISTORY_TABLE)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use realize_types::Arena;
    use redb::ReadableTable;

    const TEST_TABLE: TableDefinition<&str, &str> = TableDefinition::new("test");

    struct Fixture {
        db: Arc<ArenaDatabase>,
    }

    impl Fixture {
        fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();

            let db = ArenaDatabase::for_testing_single_arena(Arena::from("myarena"))?;

            Ok(Self { db })
        }
    }

    #[test]
    fn create_databases() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        // Make sure the tables can be opened in a read transaction.
        let txn = fixture.db.begin_read()?;
        txn.read_tree()?;
        txn.cache_table()?;
        txn.peer_table()?;
        txn.notification_table()?;
        txn.blob_table()?;
        txn.blob_lru_queue_table()?;

        txn.mark_table()?;

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
            txn.before_commit(|txn| {
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
            txn.before_commit(|txn| {
                let mut test_table = txn.inner.open_table(TEST_TABLE)?;
                test_table.insert("2", "before_commit 1")?;

                txn.before_commit(|txn| {
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
}
