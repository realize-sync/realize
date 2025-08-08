#![allow(dead_code)] // work in progress

use super::db::ArenaWriteTransaction;
use super::types::{HistoryTableEntry, IndexedFileTableEntry};
use crate::{Notification, StorageError};
use realize_types::{self, Arena, Hash, UnixTime};
use std::ops::RangeBounds;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio::task;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

/// Trait for index operations that abstracts over the implementation.
pub trait RealIndex: Send + Sync {
    /// Returns the database UUID.
    ///
    /// A UUID is set when a new database is created.
    fn uuid(&self) -> &Uuid;

    /// The arena tied to this index.
    fn arena(&self) -> Arena;

    /// Subscribe to a watch channel reporting the highest history entry index.
    ///
    /// Entries can be later queried using [RealIndex::history].
    ///
    /// The value itself is also available as [RealIndex::last_history_index].
    fn watch_history(&self) -> watch::Receiver<u64>;

    /// Index of the last history entry that was written.
    ///
    /// This is the value tracked by [RealIndex::watch_history].
    fn last_history_index(&self) -> Result<u64, StorageError>;

    /// Get a file entry.
    fn get_file(
        &self,
        path: &realize_types::Path,
    ) -> Result<Option<IndexedFileTableEntry>, StorageError>;

    fn get_file_txn(
        &self,
        txn: &super::db::ArenaReadTransaction,
        path: &realize_types::Path,
    ) -> Result<Option<IndexedFileTableEntry>, StorageError>;

    fn get_indexed_file_txn(
        &self,
        txn: &ArenaWriteTransaction,
        root: &std::path::Path,
        path: &realize_types::Path,
        hash: Option<&Hash>,
    ) -> Result<Option<std::path::PathBuf>, StorageError>;

    /// Check whether a given file is in the index already.
    fn has_file(&self, path: &realize_types::Path) -> Result<bool, StorageError>;

    /// Check whether a given file is in the index with the given size and mtime.
    fn has_matching_file(
        &self,
        path: &realize_types::Path,
        size: u64,
        mtime: UnixTime,
    ) -> Result<bool, StorageError>;

    /// Add a file entry with the given values. Replace one if it exists.
    fn add_file(
        &self,
        path: &realize_types::Path,
        size: u64,
        mtime: UnixTime,
        hash: Hash,
    ) -> Result<(), StorageError>;

    fn add_file_if_matches(
        &self,
        root: &std::path::Path,
        path: &realize_types::Path,
        size: u64,
        mtime: UnixTime,
        hash: Hash,
    ) -> Result<bool, StorageError>;

    fn remove_file_if_missing(
        &self,
        root: &std::path::Path,
        path: &realize_types::Path,
    ) -> Result<bool, StorageError>;

    /// Send all valid entries of the file table to the given channel.
    fn all_files(
        &self,
        tx: mpsc::Sender<(realize_types::Path, IndexedFileTableEntry)>,
    ) -> Result<(), StorageError>;

    /// Grab a range of history entries.
    fn history(
        &self,
        range: std::ops::Range<u64>,
        tx: mpsc::Sender<Result<(u64, HistoryTableEntry), StorageError>>,
    ) -> Result<(), StorageError>;

    /// Remove a path that can be a file or a directory.
    ///
    /// If the path is a directory, all files within that directory
    /// are removed, recursively.
    fn remove_file_or_dir(&self, path: &realize_types::Path) -> Result<(), StorageError>;

    /// Remove `path` from the index if the hash and file match,
    /// report it as a drop in the history.
    fn drop_file_if_matches(
        &self,
        txn: &ArenaWriteTransaction,
        root: &std::path::Path,
        path: &realize_types::Path,
        hash: &Hash,
    ) -> Result<bool, StorageError>;

    /// Take a remote change into account, if it applies to a file in
    /// the index.
    fn update(
        &self,
        notification: &Notification,
        root: &std::path::Path,
    ) -> Result<(), StorageError>;
}

#[derive(Clone)]
pub struct RealIndexAsync {
    inner: Arc<dyn RealIndex>,
}

impl RealIndexAsync {
    pub fn new(inner: Arc<dyn RealIndex>) -> Self {
        Self { inner }
    }

    /// Returns the database UUID.
    ///
    /// A UUID is set when a new database is created.
    pub fn uuid(&self) -> &Uuid {
        self.inner.uuid()
    }

    /// The arena tied to this index.
    pub fn arena(&self) -> Arena {
        self.inner.arena()
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
    pub fn blocking(&self) -> Arc<dyn RealIndex> {
        Arc::clone(&self.inner)
    }

    /// Return all valid file entries as a stream.
    pub fn all_files(&self) -> ReceiverStream<(realize_types::Path, IndexedFileTableEntry)> {
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
            // Convert the generic range to a concrete Range<u64>
            let start = match range.start_bound() {
                std::ops::Bound::Included(&x) => x,
                std::ops::Bound::Excluded(&x) => x + 1,
                std::ops::Bound::Unbounded => 0,
            };
            let end = match range.end_bound() {
                std::ops::Bound::Included(&x) => x + 1,
                std::ops::Bound::Excluded(&x) => x,
                std::ops::Bound::Unbounded => u64::MAX,
            };
            let concrete_range = start..end;
            if let Err(err) = inner.history(concrete_range, tx) {
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
    ) -> Result<Option<IndexedFileTableEntry>, StorageError> {
        let inner = Arc::clone(&self.inner);
        let path = path.clone();

        task::spawn_blocking(move || inner.get_file(&path)).await?
    }

    /// Check whether a given file is in the index already.
    pub async fn has_file<T>(&self, path: T) -> Result<bool, StorageError>
    where
        T: AsRef<realize_types::Path>,
    {
        let path = path.as_ref();
        let inner = Arc::clone(&self.inner);
        let path = path.clone();

        task::spawn_blocking(move || inner.has_file(&path)).await?
    }

    /// Check whether a given file is in the index already with the given size and mtime.
    pub async fn has_matching_file(
        &self,
        path: &realize_types::Path,
        size: u64,
        mtime: UnixTime,
    ) -> Result<bool, StorageError> {
        let inner = Arc::clone(&self.inner);
        let path = path.clone();
        let mtime = mtime.clone();

        task::spawn_blocking(move || inner.has_matching_file(&path, size, mtime)).await?
    }

    /// Remove a path that can be a file or a directory.
    ///
    /// If the path is a directory, all files within that directory
    /// are removed, recursively.
    pub async fn remove_file_or_dir<T>(&self, path: T) -> Result<(), StorageError>
    where
        T: AsRef<realize_types::Path>,
    {
        let path = path.as_ref();
        let inner = Arc::clone(&self.inner);
        let path = path.clone();

        task::spawn_blocking(move || inner.remove_file_or_dir(&path)).await?
    }

    pub async fn add_file(
        &self,
        path: &realize_types::Path,
        size: u64,
        mtime: UnixTime,
        hash: Hash,
    ) -> Result<(), StorageError> {
        let inner = Arc::clone(&self.inner);
        let path = path.clone();
        let mtime = mtime.clone();

        task::spawn_blocking(move || inner.add_file(&path, size, mtime, hash)).await?
    }

    pub async fn add_file_if_matches(
        &self,
        root: &std::path::Path,
        path: &realize_types::Path,
        size: u64,
        mtime: UnixTime,
        hash: Hash,
    ) -> Result<bool, StorageError> {
        let inner = Arc::clone(&self.inner);
        let path = path.clone();
        let mtime = mtime.clone();
        let root = root.to_path_buf();

        task::spawn_blocking(move || inner.add_file_if_matches(&root, &path, size, mtime, hash))
            .await?
    }

    pub async fn remove_file_if_missing(
        &self,
        root: &std::path::Path,
        path: &realize_types::Path,
    ) -> Result<bool, StorageError> {
        let inner = Arc::clone(&self.inner);
        let root = root.to_path_buf();
        let path = path.clone();

        task::spawn_blocking(move || inner.remove_file_if_missing(&root, &path)).await?
    }

    /// Take a remote change into account, if it applies to a file in
    /// the index.
    pub(crate) async fn update(
        &self,
        notification: Notification,
        root: &std::path::Path,
    ) -> Result<(), StorageError> {
        let inner = Arc::clone(&self.inner);
        let root = root.to_path_buf();

        task::spawn_blocking(move || inner.update(&notification, &root)).await?
    }
}
