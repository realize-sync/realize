use crate::global::types::PathTableEntry;
use crate::types::PartialPathId;
use crate::utils::holder::Holder;
use crate::{Inode, PathId, StorageError};
use redb::{ReadOnlyTable, Table, TableDefinition};
use std::sync::Arc;

/// Track current pathid range for each arena.
///
/// The current pathid is the last pathid that was allocated for the
/// arena.
///
/// Key: ()
/// Value: (PathId, PathId) (last pathid allocated, end of range)
const PATHID_RANGE_TABLE: TableDefinition<(), (PartialPathId, PartialPathId)> =
    TableDefinition::new("pathid_range");

/// Maps arena to their root directory pathid.
///
/// Key: arena name
/// Value: root pathid of arena
const ARENA_TABLE: TableDefinition<&str, PathId> = TableDefinition::new("cache.arena");

/// Tracks mapping of pathid to path and mtime for global directories
/// (non-arena).
///
/// This table stores the path and modification time for the root
/// pathid (1) as well as any intermediate directories, if arenas
/// contain slashes. (For example, if the arena is "documents/letters"
/// the intermediate directory "documents" is in path_table, but
/// "letters" isn't because it's an arena root and is in the arena
/// table.)
///
/// Key: &str (path or "" for root)
/// Value: PathTableEntry (pathid  and mtime)
const PATH_TABLE: TableDefinition<Inode, Holder<PathTableEntry>> = TableDefinition::new("path");

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
            txn.open_table(PATHID_RANGE_TABLE)?;
            txn.open_table(PATH_TABLE)?;
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

    pub fn arena_table<'txn>(
        &'txn self,
    ) -> Result<Table<'txn, &'static str, PathId>, StorageError> {
        Ok(self.inner.open_table(ARENA_TABLE)?)
    }

    pub fn pathid_range_table<'txn>(
        &'txn self,
    ) -> Result<Table<'txn, (), (PartialPathId, PartialPathId)>, StorageError> {
        Ok(self.inner.open_table(PATHID_RANGE_TABLE)?)
    }

    pub fn path_table<'txn>(
        &'txn self,
    ) -> Result<Table<'txn, Inode, Holder<'static, PathTableEntry>>, StorageError> {
        Ok(self.inner.open_table(PATH_TABLE)?)
    }
}

pub struct GlobalReadTransaction {
    inner: redb::ReadTransaction,
}

impl GlobalReadTransaction {
    pub fn arena_table(&self) -> Result<ReadOnlyTable<&'static str, PathId>, StorageError> {
        Ok(self.inner.open_table(ARENA_TABLE)?)
    }
}
