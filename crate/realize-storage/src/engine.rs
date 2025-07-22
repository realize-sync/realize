#![allow(dead_code)] // work in progress

use crate::unreal::{arena_cache, blob};
use crate::{Inode, StorageError};
use realize_types::{ByteRanges, Hash, Path};
use redb::{Database, ReadTransaction, TableDefinition, WriteTransaction};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task;
use tokio_stream::wrappers::ReceiverStream;

/// Stores paths marked dirty.
///
/// The path can be in the index, in the cache or both.
///
/// Key: &str (path)
/// Value: Holder<DecisionTableEntry>
const DIRTY_TABLE: TableDefinition<&str, u64> = TableDefinition::new("engine.dirty");
const DIRTY_LOG_TABLE: TableDefinition<u64, &str> = TableDefinition::new("engine.dirty_log");
const DIRTY_COUNTER_TABLE: TableDefinition<(), u64> = TableDefinition::new("engine.dirty_counter");

pub(crate) enum Job {
    Download(Path, u64, Hash),
}

pub(crate) struct DirtyPaths {
    watch_tx: watch::Sender<u64>,
    _watch_rx: watch::Receiver<u64>,
}

impl DirtyPaths {
    pub(crate) async fn new(db: Arc<Database>) -> Result<Arc<DirtyPaths>, StorageError> {
        let last_counter = task::spawn_blocking(move || {
            let counter: u64;

            let txn = db.begin_write()?;
            {
                txn.open_table(DIRTY_TABLE)?;
                txn.open_table(DIRTY_LOG_TABLE)?;
                let dirty_counter_table = txn.open_table(DIRTY_COUNTER_TABLE)?;
                counter = last_counter(&dirty_counter_table)?;
            }
            txn.commit()?;

            Ok::<_, StorageError>(counter)
        })
        .await??;
        let (watch_tx, watch_rx) = watch::channel(last_counter);

        Ok(Arc::new(Self {
            watch_tx,
            _watch_rx: watch_rx,
        }))
    }

    fn subscribe(&self) -> watch::Receiver<u64> {
        self.watch_tx.subscribe()
    }

    /// Mark a path dirty.
    ///
    /// This does nothing if the path is already dirty.
    pub(crate) fn mark_dirty(
        &self,
        txn: &WriteTransaction,
        path: &Path,
    ) -> Result<(), StorageError> {
        let mut dirty_table = txn.open_table(DIRTY_TABLE)?;
        let mut dirty_log_table = txn.open_table(DIRTY_LOG_TABLE)?;
        let mut dirty_counter_table = txn.open_table(DIRTY_COUNTER_TABLE)?;

        let counter = last_counter(&dirty_counter_table)? + 1;
        dirty_counter_table.insert((), counter)?;
        dirty_log_table.insert(counter, path.as_str())?;
        let prev = dirty_table.insert(path.as_str(), counter)?;
        if let Some(prev) = prev {
            dirty_log_table.remove(prev.value())?;
        }
        // TODO: do that only after transaction is committed
        let _ = self.watch_tx.send(counter);

        Ok(())
    }
}
pub(crate) struct Engine {
    db: Arc<Database>,
    dirty_paths: Arc<DirtyPaths>,
    arena_root: Inode,
}

impl Engine {
    pub(crate) fn new(
        db: Arc<Database>,
        dirty_paths: Arc<DirtyPaths>,
        arena_root: Inode,
    ) -> Arc<Engine> {
        Arc::new(Self {
            db,
            dirty_paths,
            arena_root,
        })
    }

    pub(crate) async fn job_stream(self: &Arc<Engine>) -> ReceiverStream<anyhow::Result<Job>> {
        let (tx, rx) = mpsc::channel(0);

        let this = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(err) = this.build_jobs(tx.clone()).await {
                let _ = tx.send(Err(err)).await;
            }
        });

        ReceiverStream::new(rx)
    }

    async fn build_jobs(
        self: &Arc<Engine>,
        tx: mpsc::Sender<anyhow::Result<Job>>,
    ) -> anyhow::Result<()> {
        let mut start_counter = 0;

        loop {
            let (job, last_counter) = task::spawn_blocking({
                let this = Arc::clone(self);
                let start_counter = start_counter;

                move || {
                    let (job, last_counter) = this.next_job(start_counter)?;
                    this.delete_range(start_counter, last_counter)?;

                    Ok::<_, StorageError>((job, last_counter))
                }
            })
            .await??;

            start_counter = last_counter + 1;
            if let Some(job) = job {
                if tx.send(Ok(job)).await.is_err() {
                    // Broken channel; normal end of this loop
                    return Ok(());
                }
            } else {
                // Wait for more dirty paths, then continue
                let mut watch = self.dirty_paths.subscribe();
                tokio::select!(
                    _ = tx.closed() => {
                        return Ok(());
                    }
                    ret = watch.wait_for(|v| *v > last_counter) => {
                        ret?;
                    }
                );
            }
        }
    }

    fn next_job(&self, start_counter: u64) -> Result<(Option<Job>, u64), StorageError> {
        let txn = self.db.begin_read()?;
        let dirty_log_table = txn.open_table(DIRTY_LOG_TABLE)?;
        let mut last_counter = 0;
        for entry in dirty_log_table.range(start_counter..)? {
            let (key, value) = entry?;
            let counter = key.value();
            last_counter = counter;
            let path = match Path::parse(value.value()) {
                Ok(p) => p,
                Err(_) => {
                    continue;
                }
            };
            if let Some(job) = self.build_job(&txn, path, counter)? {
                return Ok((Some(job), last_counter));
            }
        }
        return Ok((None, last_counter));
    }

    fn build_job(
        &self,
        txn: &ReadTransaction,
        path: Path,
        counter: u64,
    ) -> Result<Option<Job>, StorageError> {
        let file_entry = match arena_cache::get_file_entry_for_path(txn, self.arena_root, &path) {
            Ok(e) => e,
            Err(_) => {
                return Ok(None);
            }
        };
        if let Some(blob_id) = file_entry.content.blob {
            let file_range = ByteRanges::single(0, file_entry.metadata.size);
            if blob::local_availability(&txn, blob_id)?.intersection(&file_range) == file_range {
                // Already fully available
                return Ok(None);
            }
        }
        Ok(Some(Job::Download(path, counter, file_entry.content.hash)))
    }

    fn delete_range(&self, beg: u64, end: u64) -> Result<(), StorageError> {
        if !(beg < end) {
            return Ok(());
        }
        let txn = self.db.begin_write()?;
        {
            let mut dirty_log_table = txn.open_table(DIRTY_LOG_TABLE)?;
            let mut dirty_table = txn.open_table(DIRTY_TABLE)?;

            for counter in beg..end {
                let prev = dirty_log_table.remove(counter)?;
                if let Some(prev) = prev {
                    dirty_table.remove(prev.value())?;
                }
            }
        }
        txn.commit()?;
        Ok(())
    }
}

fn last_counter(
    dirty_counter_table: &impl redb::ReadableTable<(), u64>,
) -> Result<u64, StorageError> {
    Ok(dirty_counter_table.get(())?.map(|v| v.value()).unwrap_or(0))
}

/// Check the dirty mark on a path.
pub(crate) fn is_dirty(txn: &ReadTransaction, path: &Path) -> Result<bool, StorageError> {
    let dirty_table = txn.open_table(DIRTY_TABLE)?;

    Ok(dirty_table.get(path.as_str())?.is_some())
}

/// Check whether a path is dirty and if it is return its dirty mark counter.
pub(crate) fn get_dirty(txn: &ReadTransaction, path: &Path) -> Result<Option<u64>, StorageError> {
    let dirty_table = txn.open_table(DIRTY_TABLE)?;

    let entry = dirty_table.get(path.as_str())?;

    Ok(entry.map(|e| e.value()))
}

/// Clear the dirty mark on a path.
///
/// This does nothing if the path has no dirty mark.
pub(crate) fn clear_dirty(txn: &WriteTransaction, path: &Path) -> Result<(), StorageError> {
    let mut dirty_table = txn.open_table(DIRTY_TABLE)?;
    let mut dirty_log_table = txn.open_table(DIRTY_LOG_TABLE)?;
    let prev = dirty_table.remove(path.as_str())?;
    if let Some(prev) = prev {
        dirty_log_table.remove(prev.value())?;
    }

    Ok(())
}

/// Get a dirty path from the table for processing.
///
/// The returned path is removed from the dirty table when the
/// transaction is committed.
pub(crate) fn take_dirty(txn: &WriteTransaction) -> Result<Option<(Path, u64)>, StorageError> {
    let mut dirty_table = txn.open_table(DIRTY_TABLE)?;
    let mut dirty_log_table = txn.open_table(DIRTY_LOG_TABLE)?;
    loop {
        // This loop skips invalid paths.
        match dirty_log_table.pop_first()? {
            None => {
                return Ok(None);
            }
            Some((k, v)) => {
                let counter = k.value();
                let path = v.value();
                dirty_table.remove(path)?;
                if let Ok(path) = Path::parse(path) {
                    return Ok(Some((path, counter)));
                }
                // otherwise, pop another
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::utils::redb_utils;

    struct Fixture {
        db: Arc<redb::Database>,
        dirty_paths: Arc<DirtyPaths>,
    }
    impl Fixture {
        async fn setup() -> anyhow::Result<Fixture> {
            let _ = env_logger::try_init();
            let db = redb_utils::in_memory()?;
            let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;
            Ok(Self { db, dirty_paths })
        }
    }

    #[tokio::test]
    async fn mark_and_take() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let txn = fixture.db.begin_write()?;
        let path1 = Path::parse("path1")?;
        let path2 = Path::parse("path2")?;

        fixture.dirty_paths.mark_dirty(&txn, &path1)?;
        fixture.dirty_paths.mark_dirty(&txn, &path2)?;

        assert_eq!(Some((path1, 1)), take_dirty(&txn)?);
        assert_eq!(Some((path2, 2)), take_dirty(&txn)?);
        assert_eq!(None, take_dirty(&txn)?);
        assert_eq!(None, take_dirty(&txn)?);

        txn.commit()?;

        Ok(())
    }

    #[tokio::test]
    async fn increase_counter() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let txn = fixture.db.begin_write()?;
        let path1 = Path::parse("path1")?;
        let path2 = Path::parse("path2")?;

        fixture.dirty_paths.mark_dirty(&txn, &path1)?;
        fixture.dirty_paths.mark_dirty(&txn, &path2)?;
        fixture.dirty_paths.mark_dirty(&txn, &path1)?;
        fixture.dirty_paths.mark_dirty(&txn, &path1)?;

        // return path2 first, because its counter is lower
        assert_eq!(Some((path2, 2)), take_dirty(&txn)?);
        assert_eq!(Some((path1, 4)), take_dirty(&txn)?);
        assert_eq!(None, take_dirty(&txn)?);
        assert_eq!(None, take_dirty(&txn)?);

        txn.commit()?;

        Ok(())
    }

    #[tokio::test]
    async fn check_dirty() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let path1 = Path::parse("path1")?;
        let path2 = Path::parse("path2")?;

        let txn = fixture.db.begin_write()?;
        fixture.dirty_paths.mark_dirty(&txn, &path2)?;
        txn.commit()?;

        let txn = fixture.db.begin_read()?;
        assert_eq!(false, is_dirty(&txn, &path1)?);
        assert_eq!(true, is_dirty(&txn, &path2)?);
        Ok(())
    }

    #[tokio::test]
    async fn clear_mark() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let txn = fixture.db.begin_write()?;
        let path1 = Path::parse("path1")?;
        let path2 = Path::parse("path2")?;

        fixture.dirty_paths.mark_dirty(&txn, &path1)?;
        fixture.dirty_paths.mark_dirty(&txn, &path2)?;
        clear_dirty(&txn, &path1)?;

        assert_eq!(Some((path2, 2)), take_dirty(&txn)?);
        assert_eq!(None, take_dirty(&txn)?);

        txn.commit()?;
        Ok(())
    }

    #[tokio::test]
    async fn skip_invalid_path() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let txn = fixture.db.begin_write()?;
        txn.open_table(DIRTY_TABLE)?.insert("///", 1)?;
        txn.open_table(DIRTY_COUNTER_TABLE)?.insert((), 1)?;
        txn.open_table(DIRTY_LOG_TABLE)?.insert(1, "///")?;
        let path = Path::parse("path")?;

        fixture.dirty_paths.mark_dirty(&txn, &path)?;

        assert_eq!(Some((path, 2)), take_dirty(&txn)?);
        assert_eq!(None, take_dirty(&txn)?);

        txn.commit()?;
        Ok(())
    }
}
