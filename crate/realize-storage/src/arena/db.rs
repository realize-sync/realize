use super::types::FileTableKey;
use super::types::{
    BlobTableEntry, DirTableEntry, FailedJobTableEntry, FileTableEntry, HistoryTableEntry,
    MarkTableEntry, PeerTableEntry, QueueTableEntry,
};
use crate::Inode;
use crate::StorageError;
use crate::types::BlobId;
use crate::utils::holder::Holder;
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

/// Tracks directory content.
///
/// Each entry in a directory has an entry in this table, keyed with
/// the directory inode and the entry name.
///
/// To list directory content, do a range scan.
///
/// The special name "." store information about the current
/// directory, in a DirTableEntry::Self.
///
/// Key: (inode, name)
/// Value: DirTableEntry
pub(crate) const DIR_TABLE: TableDefinition<(Inode, &str), Holder<DirTableEntry>> =
    TableDefinition::new("acache.dir");

/// Track peer files.
///
/// Each known peer file has an entry in this table, keyed with the
/// file inode and the peer name. More than one peer might have the
/// same entry.
///
/// An inode available in no peers should be remove from all
/// directories.
///
/// Key: FileTableKey (inode, default|local|peer, peer)
/// Value: FileTableEntry
const FILE_TABLE: TableDefinition<FileTableKey, Holder<FileTableEntry>> =
    TableDefinition::new("acache.file");

/// Track peer files that might have been deleted remotely.
///
/// When a peer starts catchup of an arena, all its files are added to
/// this table. Calls to catchup for that peer and arena removes the
/// corresponding entry in the table. At the end of catchup, files
/// still in this table are deleted.
///
/// Key: (peer, file inode)
/// Value: parent dir inode
const PENDING_CATCHUP_TABLE: TableDefinition<(&str, Inode), Inode> =
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
const BLOB_NEXT_ID_TABLE: TableDefinition<(), BlobId> = TableDefinition::new("blob.next_id");

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
}

impl ArenaDatabase {
    pub fn new(db: redb::Database) -> Result<Arc<Self>, StorageError> {
        let txn = db.begin_write()?;
        {
            // Create tables so they can safely be queried in read
            // transactions in an empty database.
            txn.open_table(HISTORY_TABLE)?;
            txn.open_table(SETTIGS_TABLE)?;
            txn.open_table(DIR_TABLE)?;
            txn.open_table(FILE_TABLE)?;
            txn.open_table(PENDING_CATCHUP_TABLE)?;
            txn.open_table(PEER_TABLE)?;
            txn.open_table(NOTIFICATION_TABLE)?;
            txn.open_table(CURRENT_INODE_RANGE_TABLE)?;
            txn.open_table(BLOB_TABLE)?;
            txn.open_table(BLOB_NEXT_ID_TABLE)?;
            txn.open_table(BLOB_LRU_QUEUE_TABLE)?;
            txn.open_table(MARK_TABLE)?;
            txn.open_table(DIRTY_TABLE)?;
            txn.open_table(DIRTY_LOG_TABLE)?;
            txn.open_table(DIRTY_COUNTER_TABLE)?;
            txn.open_table(FAILED_JOB_TABLE)?;
        }
        txn.commit()?;

        Ok(Arc::new(Self { db }))
    }

    pub fn begin_write(&self) -> Result<ArenaWriteTransaction, StorageError> {
        Ok(ArenaWriteTransaction {
            inner: self.db.begin_write()?,
            after_commit: RefCell::new(vec![]),
        })
    }

    pub fn begin_read(&self) -> Result<ArenaReadTransaction, StorageError> {
        Ok(ArenaReadTransaction {
            inner: self.db.begin_read()?,
        })
    }
}

pub struct ArenaWriteTransaction {
    inner: redb::WriteTransaction,

    /// Callbacks to be run after the transaction is committed.
    ///
    /// Using a RefCell to avoid issues, as transactions are pretty
    /// much always borrowed immutably, with the tables it would be
    /// impractical to have pass around mutable references to
    /// transactions.
    after_commit: RefCell<Vec<Box<dyn FnOnce() -> () + Send + 'static>>>,
}

impl ArenaWriteTransaction {
    /// Commit the changes.
    ///
    /// If the transaction is successfully committed, functions
    /// registered by after_commit are run, and these may fail.
    pub fn commit(self) -> Result<(), StorageError> {
        self.inner.commit()?;
        for cb in self.after_commit.into_inner() {
            cb();
        }
        Ok(())
    }

    /// Register a function to be run after the current transaction
    /// has been successfully committed.
    ///
    /// After commit functions are run in order after a successful commit.
    pub fn after_commit(&self, cb: impl FnOnce() -> () + Send + 'static) {
        self.after_commit.borrow_mut().push(Box::new(cb));
    }

    pub fn history_table<'txn>(
        &'txn self,
    ) -> Result<Table<'txn, u64, Holder<'static, HistoryTableEntry>>, StorageError> {
        Ok(self.inner.open_table(HISTORY_TABLE)?)
    }

    pub fn settings_table<'txn>(
        &'txn self,
    ) -> Result<Table<'txn, &'static str, &'static [u8]>, StorageError> {
        Ok(self.inner.open_table(SETTIGS_TABLE)?)
    }

    pub fn dir_table<'txn>(
        &'txn self,
    ) -> Result<Table<'txn, (Inode, &'static str), Holder<'static, DirTableEntry>>, StorageError>
    {
        Ok(self.inner.open_table(DIR_TABLE)?)
    }

    pub fn file_table<'txn>(
        &'txn self,
    ) -> Result<Table<'txn, FileTableKey, Holder<'static, FileTableEntry>>, StorageError> {
        Ok(self.inner.open_table(FILE_TABLE)?)
    }

    pub fn pending_catchup_table<'txn>(
        &'txn self,
    ) -> Result<Table<'txn, (&'static str, Inode), Inode>, StorageError> {
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

    pub fn current_inode_range_table<'txn>(
        &'txn self,
    ) -> Result<Table<'txn, (), (Inode, Inode)>, StorageError> {
        Ok(self.inner.open_table(CURRENT_INODE_RANGE_TABLE)?)
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

    pub fn blob_next_id_table<'txn>(&'txn self) -> Result<Table<'txn, (), BlobId>, StorageError> {
        Ok(self.inner.open_table(BLOB_NEXT_ID_TABLE)?)
    }

    pub fn mark_table<'txn>(
        &'txn self,
    ) -> Result<Table<'txn, Inode, Holder<'static, MarkTableEntry>>, StorageError> {
        Ok(self.inner.open_table(MARK_TABLE)?)
    }

    pub fn dirty_table<'txn>(&'txn self) -> Result<Table<'txn, &'static str, u64>, StorageError> {
        Ok(self.inner.open_table(DIRTY_TABLE)?)
    }

    pub fn dirty_log_table<'txn>(
        &'txn self,
    ) -> Result<Table<'txn, u64, &'static str>, StorageError> {
        Ok(self.inner.open_table(DIRTY_LOG_TABLE)?)
    }

    pub fn dirty_counter_table<'txn>(&'txn self) -> Result<Table<'txn, (), u64>, StorageError> {
        Ok(self.inner.open_table(DIRTY_COUNTER_TABLE)?)
    }

    pub fn failed_job_table<'txn>(
        &'txn self,
    ) -> Result<Table<'txn, u64, Holder<'static, FailedJobTableEntry>>, StorageError> {
        Ok(self.inner.open_table(FAILED_JOB_TABLE)?)
    }
}

pub struct ArenaReadTransaction {
    inner: redb::ReadTransaction,
}

impl ArenaReadTransaction {
    pub fn history_table(
        &self,
    ) -> Result<ReadOnlyTable<u64, Holder<'static, HistoryTableEntry>>, StorageError> {
        Ok(self.inner.open_table(HISTORY_TABLE)?)
    }

    pub fn dir_table(
        &self,
    ) -> Result<ReadOnlyTable<(Inode, &'static str), Holder<'static, DirTableEntry>>, StorageError>
    {
        Ok(self.inner.open_table(DIR_TABLE)?)
    }

    pub fn file_table(
        &self,
    ) -> Result<ReadOnlyTable<FileTableKey, Holder<'static, FileTableEntry>>, StorageError> {
        Ok(self.inner.open_table(FILE_TABLE)?)
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
    pub fn blob_next_id_table(&self) -> Result<ReadOnlyTable<(), BlobId>, StorageError> {
        Ok(self.inner.open_table(BLOB_NEXT_ID_TABLE)?)
    }

    pub fn mark_table(
        &self,
    ) -> Result<ReadOnlyTable<Inode, Holder<'static, MarkTableEntry>>, StorageError> {
        Ok(self.inner.open_table(MARK_TABLE)?)
    }

    pub fn dirty_table(&self) -> Result<ReadOnlyTable<&'static str, u64>, StorageError> {
        Ok(self.inner.open_table(DIRTY_TABLE)?)
    }

    pub fn dirty_log_table(&self) -> Result<ReadOnlyTable<u64, &'static str>, StorageError> {
        Ok(self.inner.open_table(DIRTY_LOG_TABLE)?)
    }

    pub fn failed_job_table(
        &self,
    ) -> Result<ReadOnlyTable<u64, Holder<'static, FailedJobTableEntry>>, StorageError> {
        Ok(self.inner.open_table(FAILED_JOB_TABLE)?)
    }
}
