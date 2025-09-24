use super::db::AfterCommit;
use super::types::{HistoryTableEntry, Version};
use crate::utils::holder::Holder;
use crate::{StorageError, arena::db::Tag};
use realize_types::{Hash, Path};
use redb::ReadableTable;
use std::ops::RangeBounds;
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
    tx: watch::Sender<u64>,

    /// Kept to not lose history when there are no receivers.
    _rx: watch::Receiver<u64>,
}

impl History {
    pub(crate) fn setup(
        table: &impl redb::ReadableTable<u64, Holder<'static, HistoryTableEntry>>,
    ) -> Result<Self, StorageError> {
        let last_index = last_history_index(table)?;
        let (tx, rx) = watch::channel(last_index);

        Ok(Self { tx, _rx: rx })
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
    tag: Tag,
    after_commit: &'a AfterCommit,
    history: &'a History,
    table: redb::Table<'a, u64, Holder<'static, HistoryTableEntry>>,
}

impl<'a> WritableOpenHistory<'a> {
    pub(crate) fn new(
        tag: Tag,
        after_commit: &'a AfterCommit,
        history: &'a History,
        table: redb::Table<'a, u64, Holder<'static, HistoryTableEntry>>,
    ) -> Self {
        Self {
            tag,
            after_commit,
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

    /// Grab a range of history entries.
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
    ) -> impl Iterator<Item = Result<(u64, HistoryTableEntry), StorageError>>;
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
    ) -> impl Iterator<Item = Result<(u64, HistoryTableEntry), StorageError>> {
        HistoryIterator::new(&self.table, range)
    }
}

impl<'a> HistoryReadOperations for WritableOpenHistory<'a> {
    fn last_history_index(&self) -> Result<u64, StorageError> {
        last_history_index(&self.table)
    }

    fn history(
        &self,
        range: impl RangeBounds<u64>,
    ) -> impl Iterator<Item = Result<(u64, HistoryTableEntry), StorageError>> {
        HistoryIterator::new(&self.table, range)
    }
}

impl<'a> WritableOpenHistory<'a> {
    /// Record a Remove history entry.
    ///
    /// This tells remote peers that a file was deleted locally by the
    /// user and they should mirror this change.
    pub(crate) fn report_removed(
        &mut self,
        path: &Path,
        version: &Version,
    ) -> Result<(), StorageError> {
        if let Some(hash) = version.base_hash() {
            let index = self.allocate_history_index()?;
            let ev = HistoryTableEntry::Remove(path.clone(), hash.clone());
            log::info!("[{}] History #{index}: {ev:?}", self.tag);
            self.table.insert(index, Holder::with_content(ev)?)?;
        }
        Ok(())
    }

    /// Record a Drop history entry.
    ///
    /// This tells remote peers that a previously available version is
    /// not available anymore. Peers should not mirror this change.
    ///
    /// The hash must be the hash of a remote file version.
    pub(crate) fn report_dropped(&mut self, path: &Path, hash: &Hash) -> Result<(), StorageError> {
        let index = self.allocate_history_index()?;
        let ev = HistoryTableEntry::Drop(path.clone(), hash.clone());
        log::info!("[{}] History #{index}: {ev:?}", self.tag);
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
        old_version: Option<&Version>,
    ) -> Result<(), StorageError> {
        let index = self.allocate_history_index()?;
        let ev = if let Some(old_hash) = old_version.and_then(|v| v.base_hash()) {
            HistoryTableEntry::Replace(path.clone(), old_hash.clone())
        } else {
            HistoryTableEntry::Add(path.clone())
        };
        log::info!("[{}] History #{index}: {ev:?}", self.tag);
        self.table.insert(index, Holder::with_content(ev)?)?;

        Ok(())
    }

    /// Ask the owner of the files to branch source to dest.
    ///
    /// The hash must refer to a version of a remote file.
    pub(crate) fn request_branch(
        &mut self,
        source: &Path,
        dest: &Path,
        hash: &Hash,
    ) -> Result<(), StorageError> {
        let index = self.allocate_history_index()?;
        let ev = HistoryTableEntry::Branch(source.clone(), dest.clone(), hash.clone());
        log::info!("[{}] History #{index}: {ev:?}", self.tag);
        self.table.insert(index, Holder::with_content(ev)?)?;

        Ok(())
    }

    /// Ask the owner of the files to rename source to dest.
    ///
    /// The hash must refer to a version of a remote file.
    pub(crate) fn request_rename(
        &mut self,
        source: &Path,
        dest: &Path,
        hash: &Hash,
    ) -> Result<(), StorageError> {
        let index = self.allocate_history_index()?;
        let ev = HistoryTableEntry::Rename(source.clone(), dest.clone(), hash.clone());
        log::info!("[{}] History #{index}: {ev:?}", self.tag);
        self.table.insert(index, Holder::with_content(ev)?)?;

        Ok(())
    }

    /// Allocate a new index and ping the watch channel.
    fn allocate_history_index(&mut self) -> Result<u64, StorageError> {
        let entry_index = 1 + last_history_index(&mut self.table)?;
        let tx = self.history.tx.clone();
        self.after_commit.add(move || {
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

struct HistoryIterator<'a> {
    range: Option<Result<redb::Range<'a, u64, Holder<'static, HistoryTableEntry>>, StorageError>>,
}

impl<'a> HistoryIterator<'a> {
    fn new(
        table: &'a impl redb::ReadableTable<u64, Holder<'static, HistoryTableEntry>>,
        range: impl RangeBounds<u64>,
    ) -> Self {
        Self {
            range: Some(table.range(range).map_err(StorageError::from)),
        }
    }
}

impl<'a> Iterator for HistoryIterator<'a> {
    type Item = Result<(u64, HistoryTableEntry), StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.range.take() {
            None => None,
            Some(Err(err)) => Some(Err(err)),
            Some(Ok(mut range)) => match range.next() {
                None => None,
                Some(Err(e)) => Some(Err(e.into())),
                Some(Ok((k, v))) => {
                    self.range = Some(Ok(range));
                    let index = k.value();
                    Some(
                        v.value()
                            .parse()
                            .map_err(StorageError::from)
                            .map(|e| (index, e)),
                    )
                }
            },
        }
    }
}
