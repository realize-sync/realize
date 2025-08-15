use super::db::ArenaWriteTransaction;
use super::types::HistoryTableEntry;
use crate::StorageError;
use crate::utils::holder::Holder;
use realize_types::Arena;
use realize_types::{Hash, Path};
use redb::ReadableTable;
use std::ops::RangeBounds;
use tokio::sync::mpsc;
use tokio::sync::watch;

/// A subsystem that track local changes for reporting to remote peers.
///
/// Each change is assigned an increasing index that represents its
/// place in the history.
///
/// The watch channel reports the latest (highest) index, and
/// [HistoryReadOperations::history] can then be used to send
/// up-to-date history entries to a channel.
pub(crate) struct History {
    arena: Arena,
    tx: watch::Sender<u64>,
    _rx: watch::Receiver<u64>,
}

impl History {
    pub(crate) fn new(
        arena: Arena,
        table: &impl redb::ReadableTable<u64, Holder<'static, HistoryTableEntry>>,
    ) -> Result<Self, StorageError> {
        let last_index = last_history_index(table)?;
        let (tx, rx) = watch::channel(last_index);

        Ok(Self { arena, tx, _rx: rx })
    }

    /// Get a watch channel that reports changes (increases) of the last history index.
    ///
    /// The last history index that is watched is also available as
    /// [HistoryReadOperations::last_history_index].
    pub(crate) fn watch(&self) -> tokio::sync::watch::Receiver<u64> {
        self.tx.subscribe()
    }
}

pub(crate) struct ReadableOpenHistory<T>
where
    T: ReadableTable<u64, Holder<'static, HistoryTableEntry>>,
{
    table: T,
}

impl<T> ReadableOpenHistory<T>
where
    T: ReadableTable<u64, Holder<'static, HistoryTableEntry>>,
{
    pub(crate) fn new(table: T) -> Self {
        Self { table }
    }
}

pub(crate) struct WritableOpenHistory<'a> {
    txn: &'a ArenaWriteTransaction<'a>,
    history: &'a History,
    table: redb::Table<'a, u64, Holder<'static, HistoryTableEntry>>,
}

impl<'a> WritableOpenHistory<'a> {
    pub(crate) fn new(
        txn: &'a ArenaWriteTransaction,
        history: &'a History,
        table: redb::Table<'a, u64, Holder<'static, HistoryTableEntry>>,
    ) -> Self {
        Self {
            txn,
            history,
            table,
        }
    }
}
pub(crate) trait HistoryReadOperations {
    /// Returns the index of the last history entry that was added.
    ///
    /// This is always the highest index value.
    fn last_history_index(&self) -> Result<u64, StorageError>;

    /// Grab a range of history entries and send them to a channel.
    ///
    /// This function will not block, if the requested range is higher
    /// than the highest history index, it will just return up to the
    /// highest index that is currently available. Use
    /// [History::watcher] or
    /// [HistoryReadOperations::last_history_index] to get the highest
    /// index.
    fn history(
        &self,
        range: impl RangeBounds<u64>,
        tx: mpsc::Sender<Result<(u64, HistoryTableEntry), StorageError>>,
    ) -> Result<(), StorageError>;
}

impl<T> HistoryReadOperations for ReadableOpenHistory<T>
where
    T: ReadableTable<u64, Holder<'static, HistoryTableEntry>>,
{
    fn last_history_index(&self) -> Result<u64, StorageError> {
        last_history_index(&self.table)
    }

    fn history(
        &self,
        range: impl RangeBounds<u64>,
        tx: mpsc::Sender<Result<(u64, HistoryTableEntry), StorageError>>,
    ) -> Result<(), StorageError> {
        history(&self.table, range, tx)
    }
}

impl<'a> HistoryReadOperations for WritableOpenHistory<'a> {
    fn last_history_index(&self) -> Result<u64, StorageError> {
        last_history_index(&self.table)
    }

    fn history(
        &self,
        range: impl RangeBounds<u64>,
        tx: mpsc::Sender<Result<(u64, HistoryTableEntry), StorageError>>,
    ) -> Result<(), StorageError> {
        history(&self.table, range, tx)
    }
}

impl<'a> WritableOpenHistory<'a> {
    /// Record a Remove history entry.
    ///
    /// This tells remote peers that a file was deleted locally by the
    /// user and they should mirror this change.
    pub(crate) fn report_removed(&mut self, path: &Path, hash: &Hash) -> Result<(), StorageError> {
        let index = self.allocate_history_index()?;
        let ev = HistoryTableEntry::Remove(path.clone(), hash.clone());
        log::debug!("[{}] History #{index}: {ev:?}", self.history.arena);
        self.table.insert(index, Holder::with_content(ev)?)?;
        Ok(())
    }

    /// Record a Drop history entry.
    ///
    /// This tells remote peers that a previously available version is
    /// not available anymore. Peers should not mirror this change.
    pub(crate) fn report_dropped(&mut self, path: &Path, hash: &Hash) -> Result<(), StorageError> {
        let index = self.allocate_history_index()?;
        let ev = HistoryTableEntry::Drop(path.clone(), hash.clone());
        log::debug!("[{}] History #{index}: {ev:?}", self.history.arena);
        self.table.insert(index, Holder::with_content(ev)?)?;
        Ok(())
    }

    /// Record a Add or Replace history entry.
    ///
    /// This tells remote peers that a new version is available. Peers
    /// should mirror this change.
    pub(crate) fn report_added(
        &mut self,
        path: &Path,
        old_hash: Option<&Hash>,
    ) -> Result<(), StorageError> {
        let index = self.allocate_history_index()?;
        let ev = if let Some(old_hash) = old_hash {
            HistoryTableEntry::Replace(path.clone(), old_hash.clone())
        } else {
            HistoryTableEntry::Add(path.clone())
        };
        log::debug!("[{}] History #{index}: {ev:?}", self.history.arena);
        self.table.insert(index, Holder::with_content(ev)?)?;

        Ok(())
    }

    /// Allocate a new index and ping the watch channel.
    fn allocate_history_index(&mut self) -> Result<u64, StorageError> {
        let entry_index = 1 + last_history_index(&mut self.table)?;
        let tx = self.history.tx.clone();
        self.txn.after_commit(move || {
            let _ = tx.send(entry_index);
        });

        Ok(entry_index)
    }
}

fn last_history_index(
    table: &impl redb::ReadableTable<u64, Holder<'static, HistoryTableEntry>>,
) -> Result<u64, StorageError> {
    Ok(table.last()?.map(|(k, _)| k.value()).unwrap_or(0))
}

/// Grab a range of history entries and send them to the given channel.
fn history(
    table: &impl redb::ReadableTable<u64, Holder<'static, HistoryTableEntry>>,
    range: impl RangeBounds<u64>,
    tx: mpsc::Sender<Result<(u64, HistoryTableEntry), StorageError>>,
) -> Result<(), StorageError> {
    for res in table.range(range)?.map(|res| match res {
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
