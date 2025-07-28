use crate::global::types::DirTableEntry;
use crate::utils::holder::Holder;
use crate::{Inode, StorageError};
use redb::{ReadOnlyTable, Table, TableDefinition};
use std::sync::Arc;

/// Maps arena to their root directory inode.
///
/// Arenas in this table can also be accessed as subdirectories of the
/// root directory (1).
///
/// Key: arena name
/// Value: root inode of arena
const ARENA_TABLE: TableDefinition<&str, Inode> = TableDefinition::new("cache.arena");

/// Track inode range allocation for arenas.
///
/// This table allocates increasing ranges of inodes to arenas.
/// Key: Inode (inode - end of range)
/// Value: Inode (arena root, 1 for the no-arena range)
///
/// To find which arena a given inode N belongs to, lookup the range [N..];
/// the first element returned is the end of the current range to which N belongs.
const INODE_RANGE_ALLOCATION_TABLE: TableDefinition<Inode, Inode> =
    TableDefinition::new("cache.inode_range_allocation");

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
const DIRECTORY_TABLE: TableDefinition<(Inode, &str), Holder<DirTableEntry>> =
    TableDefinition::new("acache.directory");

/// Track current inode range for each arena.
///
/// The current inode is the last inode that was allocated for the
/// arena.
///
/// Key: ()
/// Value: (Inode, Inode) (last inode allocated, end of range)
const CURRENT_INODE_RANGE_TABLE: TableDefinition<(), (Inode, Inode)> =
    TableDefinition::new("acache.current_inode_range");

pub(crate) struct GlobalDatabase {
    db: redb::Database,
}

impl GlobalDatabase {
    pub fn new(db: redb::Database) -> Result<Arc<Self>, StorageError> {
        let txn = db.begin_write()?;
        {
            // Create tables so they can safely be queried in read
            // transactions in an empty database.
            txn.open_table(ARENA_TABLE)?;
            txn.open_table(INODE_RANGE_ALLOCATION_TABLE)?;
            txn.open_table(DIRECTORY_TABLE)?;
            txn.open_table(CURRENT_INODE_RANGE_TABLE)?;
        }
        txn.commit()?;

        Ok(Arc::new(Self { db }))
    }

    pub fn begin_write(&self) -> Result<GlobalWriteTransaction, StorageError> {
        Ok(GlobalWriteTransaction {
            inner: self.db.begin_write()?,
        })
    }

    pub fn begin_read(&self) -> Result<GlobalReadTransaction, StorageError> {
        Ok(GlobalReadTransaction {
            inner: self.db.begin_read()?,
        })
    }
}

pub struct GlobalWriteTransaction {
    inner: redb::WriteTransaction,
}

impl GlobalWriteTransaction {
    /// Commit the changes.
    ///
    /// If the transaction is successfully committed, functions
    /// registered by after_commit are run, and these may fail.
    pub fn commit(self) -> Result<(), StorageError> {
        self.inner.commit()?;

        Ok(())
    }

    pub fn arena_table<'txn>(&'txn self) -> Result<Table<'txn, &'static str, Inode>, StorageError> {
        Ok(self.inner.open_table(ARENA_TABLE)?)
    }

    pub fn inode_range_allocation_table<'txn>(
        &'txn self,
    ) -> Result<Table<'txn, Inode, Inode>, StorageError> {
        Ok(self.inner.open_table(INODE_RANGE_ALLOCATION_TABLE)?)
    }

    pub fn directory_table<'txn>(
        &'txn self,
    ) -> Result<Table<'txn, (Inode, &'static str), Holder<'static, DirTableEntry>>, StorageError>
    {
        Ok(self.inner.open_table(DIRECTORY_TABLE)?)
    }

    pub fn current_inode_range_table<'txn>(
        &'txn self,
    ) -> Result<Table<'txn, (), (Inode, Inode)>, StorageError> {
        Ok(self.inner.open_table(CURRENT_INODE_RANGE_TABLE)?)
    }
}

pub struct GlobalReadTransaction {
    inner: redb::ReadTransaction,
}

impl GlobalReadTransaction {
    pub fn arena_table(&self) -> Result<ReadOnlyTable<&'static str, Inode>, StorageError> {
        Ok(self.inner.open_table(ARENA_TABLE)?)
    }

    pub fn inode_range_allocation_table(
        &self,
    ) -> Result<ReadOnlyTable<Inode, Inode>, StorageError> {
        Ok(self.inner.open_table(INODE_RANGE_ALLOCATION_TABLE)?)
    }

    pub fn directory_table(
        &self,
    ) -> Result<ReadOnlyTable<(Inode, &'static str), Holder<'static, DirTableEntry>>, StorageError>
    {
        Ok(self.inner.open_table(DIRECTORY_TABLE)?)
    }
}
