#![allow(dead_code)]

use super::db::AfterCommit;
use super::tree::{TreeExt, TreeLoc, TreeReadOperations};
use super::types::{FailedJobTableEntry, RetryJob};
use crate::{JobId, StorageError, utils::holder::Holder};
use realize_types::{Path, UnixTime};
use redb::{ReadableTable, Table};
use std::time::Duration;
use tokio::sync::watch;

pub(crate) struct Dirty {
    watch_tx: watch::Sender<u64>,
    _watch_rx: watch::Receiver<u64>,
}

impl Dirty {
    pub(crate) fn new(counter_table: &impl ReadableTable<(), u64>) -> Result<Self, StorageError> {
        let counter = last_counter(counter_table)?;
        let (watch_tx, watch_rx) = watch::channel(counter);

        Ok(Self {
            watch_tx,
            _watch_rx: watch_rx,
        })
    }
    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.watch_tx.subscribe()
    }
}

pub(crate) struct ReadableOpenDirty<DT, LT, FT>
where
    DT: ReadableTable<&'static str, u64>,
    LT: ReadableTable<u64, &'static str>,
    FT: ReadableTable<u64, Holder<'static, FailedJobTableEntry>>,
{
    table: DT,
    log_table: LT,
    failed_job_table: FT,
}

impl<DT, LT, FT> ReadableOpenDirty<DT, LT, FT>
where
    DT: ReadableTable<&'static str, u64>,
    LT: ReadableTable<u64, &'static str>,
    FT: ReadableTable<u64, Holder<'static, FailedJobTableEntry>>,
{
    pub(crate) fn new(dirty_table: DT, log_table: LT, failed_job_table: FT) -> Self {
        Self {
            table: dirty_table,
            log_table,
            failed_job_table,
        }
    }
}

pub(crate) struct WritableOpenDirty<'a> {
    after_commit: &'a AfterCommit,
    dirty: &'a Dirty,
    table: Table<'a, &'static str, u64>,
    log_table: Table<'a, u64, &'static str>,
    failed_job_table: Table<'a, u64, Holder<'static, FailedJobTableEntry>>,
    counter_table: Table<'a, (), u64>,
}

impl<'a> WritableOpenDirty<'a> {
    pub(crate) fn new(
        after_commit: &'a AfterCommit,
        dirty: &'a Dirty,
        dirty_table: Table<'a, &str, u64>,
        log_table: Table<'a, u64, &str>,
        failed_job_table: Table<'a, u64, Holder<FailedJobTableEntry>>,
        counter_table: Table<'a, (), u64>,
    ) -> Self {
        Self {
            after_commit,
            dirty,
            table: dirty_table,
            log_table,
            failed_job_table,
            counter_table,
        }
    }
}

pub(crate) trait DirtyReadOperations {
    fn next_dirty_path(&self, start_counter: u64) -> Result<Option<(Path, u64)>, StorageError>;
    fn get_last_counter(&self, start_counter: u64) -> Result<Option<u64>, StorageError>;
    fn get_lowest_counter(&self) -> Result<Option<u64>, StorageError>;
    fn get_path_for_counter(&self, counter: u64) -> Result<Option<Path>, StorageError>;
    fn get_counter_for_path(&self, path: &Path) -> Result<Option<u64>, StorageError>;
    fn is_job_failed(&self, job_id: JobId) -> Result<bool, StorageError>;
    fn get_retryable_jobs(&self, after_time: UnixTime) -> Result<Vec<u64>, StorageError>;
    fn get_jobs_waiting_for_peers(&self) -> Result<Vec<u64>, StorageError>;
    fn get_earliest_backoff(
        &self,
        lower_bound: Option<UnixTime>,
    ) -> Result<Option<(UnixTime, Vec<u64>)>, StorageError>;
    fn is_dirty(&self, path: &Path) -> Result<bool, StorageError>;
    fn get_dirty(&self, path: &Path) -> Result<Option<u64>, StorageError>;
}

impl<DT, LT, FT> DirtyReadOperations for ReadableOpenDirty<DT, LT, FT>
where
    DT: ReadableTable<&'static str, u64>,
    LT: ReadableTable<u64, &'static str>,
    FT: ReadableTable<u64, Holder<'static, FailedJobTableEntry>>,
{
    fn next_dirty_path(&self, start_counter: u64) -> Result<Option<(Path, u64)>, StorageError> {
        next_dirty_path(&self.log_table, start_counter)
    }

    fn get_last_counter(&self, start_counter: u64) -> Result<Option<u64>, StorageError> {
        get_last_counter(&self.log_table, start_counter)
    }

    fn get_lowest_counter(&self) -> Result<Option<u64>, StorageError> {
        get_lowest_counter(&self.log_table)
    }

    fn get_path_for_counter(&self, counter: u64) -> Result<Option<Path>, StorageError> {
        get_path_for_counter(&self.log_table, counter)
    }

    fn get_counter_for_path(&self, path: &Path) -> Result<Option<u64>, StorageError> {
        get_counter_for_path(&self.table, path)
    }

    fn is_job_failed(&self, job_id: JobId) -> Result<bool, StorageError> {
        is_job_failed(&self.failed_job_table, job_id)
    }

    fn get_retryable_jobs(&self, after_time: UnixTime) -> Result<Vec<u64>, StorageError> {
        get_retryable_jobs(&self.failed_job_table, after_time)
    }

    fn get_jobs_waiting_for_peers(&self) -> Result<Vec<u64>, StorageError> {
        get_jobs_waiting_for_peers(&self.failed_job_table)
    }

    fn get_earliest_backoff(
        &self,
        lower_bound: Option<UnixTime>,
    ) -> Result<Option<(UnixTime, Vec<u64>)>, StorageError> {
        get_earliest_backoff(&self.failed_job_table, lower_bound)
    }

    fn is_dirty(&self, path: &Path) -> Result<bool, StorageError> {
        is_dirty(&self.table, path)
    }

    fn get_dirty(&self, path: &Path) -> Result<Option<u64>, StorageError> {
        get_dirty(&self.table, path)
    }
}

impl<'a> DirtyReadOperations for WritableOpenDirty<'a> {
    fn next_dirty_path(&self, start_counter: u64) -> Result<Option<(Path, u64)>, StorageError> {
        next_dirty_path(&self.log_table, start_counter)
    }

    fn get_last_counter(&self, start_counter: u64) -> Result<Option<u64>, StorageError> {
        get_last_counter(&self.log_table, start_counter)
    }

    fn get_lowest_counter(&self) -> Result<Option<u64>, StorageError> {
        get_lowest_counter(&self.log_table)
    }

    fn get_path_for_counter(&self, counter: u64) -> Result<Option<Path>, StorageError> {
        get_path_for_counter(&self.log_table, counter)
    }

    fn get_counter_for_path(&self, path: &Path) -> Result<Option<u64>, StorageError> {
        get_counter_for_path(&self.table, path)
    }

    fn is_job_failed(&self, job_id: JobId) -> Result<bool, StorageError> {
        is_job_failed(&self.failed_job_table, job_id)
    }

    fn get_retryable_jobs(&self, after_time: UnixTime) -> Result<Vec<u64>, StorageError> {
        get_retryable_jobs(&self.failed_job_table, after_time)
    }

    fn get_jobs_waiting_for_peers(&self) -> Result<Vec<u64>, StorageError> {
        get_jobs_waiting_for_peers(&self.failed_job_table)
    }

    fn get_earliest_backoff(
        &self,
        lower_bound: Option<UnixTime>,
    ) -> Result<Option<(UnixTime, Vec<u64>)>, StorageError> {
        get_earliest_backoff(&self.failed_job_table, lower_bound)
    }

    fn is_dirty(&self, path: &Path) -> Result<bool, StorageError> {
        is_dirty(&self.table, path)
    }

    fn get_dirty(&self, path: &Path) -> Result<Option<u64>, StorageError> {
        get_dirty(&self.table, path)
    }
}

impl<'a> WritableOpenDirty<'a> {
    pub(crate) fn mark_job_done(&mut self, job_id: JobId) -> Result<bool, StorageError> {
        if let Some(removed) = self.log_table.remove(job_id.as_u64())? {
            self.failed_job_table.remove(job_id.as_u64())?;
            self.table.remove(removed.value())?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Mark a job as failed with retry strategy.
    /// Returns the time after which the job will be retried, or None if abandoned.
    pub(crate) fn mark_job_failed(
        &mut self,
        job_id: JobId,
        retry_strategy: &dyn Fn(u32) -> Option<Duration>,
    ) -> Result<Option<UnixTime>, StorageError> {
        let path = match self.log_table.get(job_id.as_u64())? {
            Some(v) => v.value().to_string(),
            None => {
                return Ok(None);
            }
        };
        let counter = job_id.as_u64();
        let failure_count = if let Some(existing) = self.failed_job_table.get(counter)? {
            let existing = existing.value().parse()?;
            existing.failure_count + 1
        } else {
            1
        };

        match retry_strategy(failure_count) {
            None => {
                self.log_table.remove(counter)?;
                self.failed_job_table.remove(counter)?;
                self.table.remove(path.as_str())?;

                Ok(None)
            }
            Some(delay) => {
                let backoff_until = UnixTime::now().plus(delay);
                self.failed_job_table.insert(
                    counter,
                    Holder::with_content(FailedJobTableEntry {
                        failure_count,
                        retry: RetryJob::After(backoff_until.clone()),
                    })?,
                )?;

                Ok(Some(backoff_until))
            }
        }
    }

    /// Mark a job as waiting for peer connection.
    pub(crate) fn mark_job_missing_peers(&mut self, job_id: JobId) -> Result<(), StorageError> {
        let _path = match self.log_table.get(job_id.as_u64())? {
            Some(v) => v.value().to_string(),
            None => {
                return Ok(());
            }
        };
        let counter = job_id.as_u64();

        // This isn't considered a failure, but keep the existing failure count.
        let failure_count = if let Some(existing) = self.failed_job_table.get(counter)? {
            let existing = existing.value().parse()?;
            existing.failure_count
        } else {
            0
        };

        self.failed_job_table.insert(
            counter,
            Holder::with_content(FailedJobTableEntry {
                failure_count,
                retry: RetryJob::WhenPeerConnects,
            })?,
        )?;

        Ok(())
    }

    /// Delete a range of counters (for cleanup).
    pub(crate) fn delete_range(&mut self, beg: u64, end: u64) -> Result<(), StorageError> {
        if !(beg < end) {
            return Ok(());
        }

        for counter in beg..end {
            let prev = self.log_table.remove(counter)?;
            if let Some(prev) = prev {
                self.table.remove(prev.value())?;
            }
            self.failed_job_table.remove(counter)?;
        }

        Ok(())
    }

    /// Clear the dirty mark for a specific path.
    pub(crate) fn clear_dirty_for_path(&mut self, path: &Path) -> Result<(), StorageError> {
        let prev = self.table.remove(path.as_str())?;
        if let Some(prev) = prev {
            let counter = prev.value();
            self.log_table.remove(counter)?;
            self.failed_job_table.remove(counter)?;
        }

        Ok(())
    }

    /// Take the next dirty path for processing (removes it from tracking).
    pub(crate) fn take_next_dirty_path(&mut self) -> Result<Option<(Path, u64)>, StorageError> {
        loop {
            // This loop skips invalid paths.
            match self.log_table.pop_first()? {
                None => {
                    return Ok(None);
                }
                Some((k, v)) => {
                    let counter = k.value();
                    let path = v.value();
                    self.table.remove(path)?;
                    if let Ok(path) = Path::parse(path) {
                        return Ok(Some((path, counter)));
                    }
                    // otherwise, pop another
                }
            }
        }
    }

    /// Mark a tree location and all its children dirty.
    pub(crate) fn mark_dirty_recursive<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<(), StorageError> {
        if let Some(start) = tree.resolve(loc)? {
            for inode in std::iter::once(Ok(start)).chain(tree.recurse(start, |_| true)) {
                let inode = inode?;
                if let Ok(path) = tree.backtrack(inode) {
                    self.mark_dirty(path)?;
                }
            }
        }

        Ok(())
    }

    /// Mark a path dirty.
    ///
    /// This does nothing if the path is already dirty.
    pub(crate) fn mark_dirty<T>(&mut self, path: T) -> Result<(), StorageError>
    where
        T: AsRef<Path>,
    {
        let path = path.as_ref();
        log::debug!("dirty: {path}");
        let counter = last_counter(&self.counter_table)? + 1;
        self.counter_table.insert((), counter)?;
        self.log_table.insert(counter, path.as_str())?;
        let prev = self.table.insert(path.as_str(), counter)?;
        if let Some(prev) = prev {
            let counter = prev.value();
            self.log_table.remove(counter)?;
            self.failed_job_table.remove(counter)?;
        }
        let watch_tx = self.dirty.watch_tx.clone();
        self.after_commit.add(move || {
            let _ = watch_tx.send(counter);
        });

        Ok(())
    }
}

fn next_dirty_path(
    log_table: &impl ReadableTable<u64, &'static str>,
    start_counter: u64,
) -> Result<Option<(Path, u64)>, StorageError> {
    for entry in log_table.range(start_counter..)? {
        let (key, value) = entry?;
        let counter = key.value();
        let path = match Path::parse(value.value()) {
            Ok(p) => p,
            Err(_) => {
                continue;
            }
        };
        return Ok(Some((path, counter)));
    }
    Ok(None)
}

fn get_last_counter(
    log_table: &impl ReadableTable<u64, &'static str>,
    start_counter: u64,
) -> Result<Option<u64>, StorageError> {
    let mut last_counter = None;
    for entry in log_table.range(start_counter..)? {
        let (key, _) = entry?;
        last_counter = Some(key.value());
    }
    Ok(last_counter)
}

fn get_lowest_counter(
    log_table: &impl ReadableTable<u64, &'static str>,
) -> Result<Option<u64>, StorageError> {
    Ok(log_table.first()?.map(|(k, _)| k.value()))
}

fn get_path_for_counter(
    log_table: &impl ReadableTable<u64, &'static str>,
    counter: u64,
) -> Result<Option<Path>, StorageError> {
    if let Some(v) = log_table.get(counter)? {
        let path = Path::parse(v.value())?;
        Ok(Some(path))
    } else {
        Ok(None)
    }
}

fn get_counter_for_path(
    table: &impl ReadableTable<&'static str, u64>,
    path: &Path,
) -> Result<Option<u64>, StorageError> {
    if let Some(entry) = table.get(path.as_str())? {
        Ok(Some(entry.value()))
    } else {
        Ok(None)
    }
}

fn is_job_failed(
    failed_job_table: &impl ReadableTable<u64, Holder<'static, FailedJobTableEntry>>,
    job_id: JobId,
) -> Result<bool, StorageError> {
    Ok(failed_job_table.get(job_id.as_u64())?.is_some())
}

fn get_retryable_jobs(
    failed_job_table: &impl ReadableTable<u64, Holder<'static, FailedJobTableEntry>>,
    after_time: UnixTime,
) -> Result<Vec<u64>, StorageError> {
    let mut jobs = vec![];
    for e in failed_job_table.iter()? {
        let (key, val) = e?;
        let counter = key.value();
        let entry = val.value().parse()?;
        let backoff_until = match entry.retry {
            RetryJob::After(time) => time,
            RetryJob::WhenPeerConnects => {
                // Skip jobs that are waiting for peer connection
                continue;
            }
        };
        if backoff_until <= after_time {
            jobs.push(counter);
        }
    }
    Ok(jobs)
}

fn get_jobs_waiting_for_peers(
    failed_job_table: &impl ReadableTable<u64, Holder<'static, FailedJobTableEntry>>,
) -> Result<Vec<u64>, StorageError> {
    let mut jobs = vec![];
    for e in failed_job_table.iter()? {
        let (key, val) = e?;
        let counter = key.value();
        let entry = val.value().parse()?;
        match entry.retry {
            RetryJob::After(_) => {
                continue;
            }
            RetryJob::WhenPeerConnects => {
                jobs.push(counter);
            }
        }
    }
    Ok(jobs)
}

fn get_earliest_backoff(
    failed_job_table: &impl ReadableTable<u64, Holder<'static, FailedJobTableEntry>>,
    lower_bound: Option<UnixTime>,
) -> Result<Option<(UnixTime, Vec<u64>)>, StorageError> {
    let mut earliest_backoff = UnixTime::MAX;
    let mut counters = vec![];
    for e in failed_job_table.iter()? {
        let (key, val) = e?;
        let counter = key.value();
        let entry = val.value().parse()?;
        let backoff_until = match entry.retry {
            RetryJob::After(time) => time,
            RetryJob::WhenPeerConnects => {
                // Skip jobs that are waiting for peer connection
                continue;
            }
        };
        if let Some(lower_bound) = &lower_bound
            && backoff_until <= *lower_bound
        {
            continue;
        }
        if backoff_until > earliest_backoff {
            continue;
        }
        if backoff_until < earliest_backoff {
            earliest_backoff = backoff_until;
            counters.clear();
        }
        counters.push(counter);
    }
    Ok(if earliest_backoff < UnixTime::MAX {
        Some((earliest_backoff, counters))
    } else {
        None
    })
}

fn is_dirty(
    table: &impl ReadableTable<&'static str, u64>,
    path: &Path,
) -> Result<bool, StorageError> {
    Ok(table.get(path.as_str())?.is_some())
}

fn get_dirty(
    table: &impl ReadableTable<&'static str, u64>,
    path: &Path,
) -> Result<Option<u64>, StorageError> {
    let entry = table.get(path.as_str())?;
    Ok(entry.map(|e| e.value()))
}

fn last_counter(
    dirty_counter_table: &impl redb::ReadableTable<(), u64>,
) -> Result<u64, StorageError> {
    Ok(dirty_counter_table.get(())?.map(|v| v.value()).unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use realize_types::Arena;

    use super::*;
    use crate::JobId;
    use crate::arena::db::ArenaDatabase;
    use std::sync::Arc;
    use std::time::Duration;

    struct Fixture {
        db: Arc<ArenaDatabase>,
    }

    impl Fixture {
        fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();
            let arena = Arena::from("myarena");
            let db = ArenaDatabase::for_testing_single_arena(arena)?;

            Ok(Self { db })
        }
    }

    #[test]
    fn read_txn_with_dirty() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_read()?;
        let dirty = txn.read_dirty()?;

        // just make sure it doesn't fail
        dirty.is_dirty(&Path::parse("foo")?)?;

        Ok(())
    }

    #[test]
    fn write_txn_with_dirty() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let dirty = txn.read_dirty()?;

        // just make sure it doesn't fail
        dirty.is_dirty(&Path::parse("foo")?)?;

        Ok(())
    }

    #[test]
    fn write_dirty() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut dirty = txn.write_dirty()?;

        dirty.mark_dirty(&Path::parse("foo")?)?;

        Ok(())
    }

    #[test]
    fn mark_and_take() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let path1 = Path::parse("path1")?;
        let path2 = Path::parse("path2")?;

        {
            let mut dirty = txn.write_dirty()?;
            dirty.mark_dirty(&path1)?;
            dirty.mark_dirty(&path2)?;

            assert_eq!(Some((path1, 1)), dirty.take_next_dirty_path()?);
            assert_eq!(Some((path2, 2)), dirty.take_next_dirty_path()?);
            assert_eq!(None, dirty.take_next_dirty_path()?);
            assert_eq!(None, dirty.take_next_dirty_path()?);
        }

        txn.commit()?;

        Ok(())
    }

    #[test]
    fn increase_counter() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let path1 = Path::parse("path1")?;
        let path2 = Path::parse("path2")?;

        {
            let mut dirty = txn.write_dirty()?;
            dirty.mark_dirty(&path1)?;
            dirty.mark_dirty(&path2)?;
            dirty.mark_dirty(&path1)?;
            dirty.mark_dirty(&path1)?;

            // return path2 first, because its counter is lower
            assert_eq!(Some((path2, 2)), dirty.take_next_dirty_path()?);
            assert_eq!(Some((path1, 4)), dirty.take_next_dirty_path()?);
            assert_eq!(None, dirty.take_next_dirty_path()?);
            assert_eq!(None, dirty.take_next_dirty_path()?);
        }

        txn.commit()?;

        Ok(())
    }

    #[test]
    fn check_dirty() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path1 = Path::parse("path1")?;
        let path2 = Path::parse("path2")?;

        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.mark_dirty(&path2)?;
            }
            txn.commit()?;
        }

        let txn = fixture.db.begin_read()?;
        let dirty = txn.read_dirty()?;
        assert_eq!(false, dirty.is_dirty(&path1)?);
        assert_eq!(true, dirty.is_dirty(&path2)?);
        Ok(())
    }

    #[test]
    fn clear_mark() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let path1 = Path::parse("path1")?;
        let path2 = Path::parse("path2")?;

        {
            let mut dirty = txn.write_dirty()?;
            dirty.mark_dirty(&path1)?;
            dirty.mark_dirty(&path2)?;
            dirty.clear_dirty_for_path(&path1)?;

            assert_eq!(Some((path2, 2)), dirty.take_next_dirty_path()?);
            assert_eq!(None, dirty.take_next_dirty_path()?);
        }

        txn.commit()?;
        Ok(())
    }

    #[test]
    fn skip_invalid_path() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        {
            let mut dirty = txn.write_dirty()?;
            dirty.table.insert("///", 1)?;
            dirty.counter_table.insert((), 1)?;
            dirty.log_table.insert(1, "///")?;
        }
        txn.commit()?;

        let path = Path::parse("path")?;
        let txn = fixture.db.begin_write()?;
        {
            let mut dirty = txn.write_dirty()?;
            dirty.mark_dirty(&path)?;

            assert_eq!(Some((path, 2)), dirty.take_next_dirty_path()?);
            assert_eq!(None, dirty.take_next_dirty_path()?);
        }

        txn.commit()?;
        Ok(())
    }

    #[test]
    fn test_mark_job_done() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("test/path.txt")?;
        let job_id = JobId::from(1);

        // Mark path as dirty first
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.mark_dirty(&path)?;
            }
            txn.commit()?;
        }

        // Verify it's dirty
        {
            let txn = fixture.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            assert!(dirty.is_dirty(&path)?);
        }

        // Mark job as done
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                let was_dirty = dirty.mark_job_done(job_id)?;
                assert!(was_dirty);
            }
            txn.commit()?;
        }

        // Verify path is no longer dirty
        {
            let txn = fixture.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            assert!(!dirty.is_dirty(&path)?);
        }

        // Mark job as done again (should return false)
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                let was_dirty = dirty.mark_job_done(job_id)?;
                assert!(!was_dirty);
            }
            txn.commit()?;
        }

        Ok(())
    }

    #[test]
    fn test_mark_job_failed() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("test/path.txt")?;
        let job_id = JobId::from(1);

        // Mark path as dirty first
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.mark_dirty(&path)?;
            }
            txn.commit()?;
        }

        // Mark job as failed with retry strategy
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                let retry_strategy = |attempt: u32| {
                    if attempt < 3 {
                        Some(Duration::from_secs(attempt as u64 * 10))
                    } else {
                        None
                    }
                };

                let retry_after = dirty.mark_job_failed(job_id, &retry_strategy)?;
                assert!(retry_after.is_some());
            }
            txn.commit()?;
        }

        // Verify job is marked as failed
        {
            let txn = fixture.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            assert!(dirty.is_job_failed(job_id)?);
        }

        // Get retryable jobs (should be empty since we're before retry time)
        {
            let txn = fixture.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            let retryable = dirty.get_retryable_jobs(UnixTime::from_secs(0))?;
            assert!(retryable.is_empty());
        }

        Ok(())
    }

    #[test]
    fn test_mark_job_missing_peers() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("test/path.txt")?;
        let job_id = JobId::from(1);

        // First mark a path as dirty to create the job entry
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.mark_dirty(&path)?;
            }
            txn.commit()?;
        }

        // Mark job as missing peers
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.mark_job_missing_peers(job_id)?;
            }
            txn.commit()?;
        }

        // Verify job is waiting for peers
        let txn = fixture.db.begin_read()?;
        let dirty = txn.read_dirty()?;
        let waiting_jobs = dirty.get_jobs_waiting_for_peers()?;
        assert_eq!(waiting_jobs, vec![1]);

        Ok(())
    }

    #[test]
    fn test_next_dirty_path() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path1 = Path::parse("path1.txt")?;
        let path2 = Path::parse("path2.txt")?;

        // Mark multiple paths as dirty
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.mark_dirty(&path1)?;
                dirty.mark_dirty(&path2)?;
            }
            txn.commit()?;
        }

        // Get next dirty path starting from counter 0
        let txn = fixture.db.begin_read()?;
        let dirty = txn.read_dirty()?;
        let mut start_counter = 0;
        let result = dirty.next_dirty_path(start_counter)?;
        assert_eq!(result, Some((path1.clone(), 1)));
        start_counter = 2; // Start from counter 2 to get the next entry

        // Get next dirty path
        let result = dirty.next_dirty_path(start_counter)?;
        assert_eq!(result, Some((path2.clone(), 2)));
        start_counter = 3; // Start from counter 3 to get the next entry

        // No more dirty paths
        let result = dirty.next_dirty_path(start_counter)?;
        assert_eq!(result, None);

        Ok(())
    }

    #[test]
    fn test_get_path_for_counter() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("test/path.txt")?;

        // Mark path as dirty
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.mark_dirty(&path)?;
            }
            txn.commit()?;
        }

        // Get path for counter 1
        let txn = fixture.db.begin_read()?;
        let dirty = txn.read_dirty()?;
        let result = dirty.get_path_for_counter(1)?;
        assert_eq!(result, Some(path.clone()));

        // Get path for non-existent counter
        let result = dirty.get_path_for_counter(999)?;
        assert_eq!(result, None);

        Ok(())
    }

    #[test]
    fn test_get_counter_for_path() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("test/path.txt")?;

        // Mark path as dirty
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.mark_dirty(&path)?;
            }
            txn.commit()?;
        }

        // Get counter for path
        let txn = fixture.db.begin_read()?;
        let dirty = txn.read_dirty()?;
        let result = dirty.get_counter_for_path(&path)?;
        assert_eq!(result, Some(1));

        // Get counter for non-existent path
        let non_existent_path = Path::parse("non/existent.txt")?;
        let result = dirty.get_counter_for_path(&non_existent_path)?;
        assert_eq!(result, None);

        Ok(())
    }

    #[test]
    fn test_is_job_failed() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("test/path.txt")?;
        let job_id = JobId::from(1);

        // First mark a path as dirty to create the job entry
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.mark_dirty(&path)?;
            }
            txn.commit()?;
        }

        // Job should not be failed initially
        {
            let txn = fixture.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            assert!(!dirty.is_job_failed(job_id)?);
        }

        // Mark job as failed
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                let retry_strategy = |_| Some(Duration::from_secs(10));
                dirty.mark_job_failed(job_id, &retry_strategy)?;
            }
            txn.commit()?;
        }

        // Job should now be failed
        let txn = fixture.db.begin_read()?;
        let dirty = txn.read_dirty()?;
        assert!(dirty.is_job_failed(job_id)?);

        Ok(())
    }

    #[test]
    fn test_get_retryable_jobs() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path1 = Path::parse("path1.txt")?;
        let path2 = Path::parse("path2.txt")?;
        let job_id1 = JobId::from(1);
        let job_id2 = JobId::from(2);

        // First mark paths as dirty to create the job entries
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.mark_dirty(&path1)?;
                dirty.mark_dirty(&path2)?;
            }
            txn.commit()?;
        }

        // Mark jobs as failed with different retry times
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                let retry_strategy = |attempt: u32| {
                    if attempt < 3 {
                        Some(Duration::from_secs(attempt as u64 * 10))
                    } else {
                        None
                    }
                };

                dirty.mark_job_failed(job_id1, &retry_strategy)?;
                dirty.mark_job_failed(job_id2, &retry_strategy)?;
            }
            txn.commit()?;
        }

        // Get retryable jobs before retry time (should be empty)
        let txn = fixture.db.begin_read()?;
        let dirty = txn.read_dirty()?;
        let retryable = dirty.get_retryable_jobs(UnixTime::from_secs(0))?;
        assert!(retryable.is_empty());

        // Get retryable jobs after retry time
        let retryable = dirty.get_retryable_jobs(UnixTime::now().plus(Duration::from_secs(100)))?;
        assert_eq!(retryable, vec![1, 2]);

        Ok(())
    }

    #[test]
    fn test_get_earliest_backoff() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path1 = Path::parse("path1.txt")?;
        let path2 = Path::parse("path2.txt")?;
        let job_id1 = JobId::from(1);
        let job_id2 = JobId::from(2);

        // First mark paths as dirty to create the job entries
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.mark_dirty(&path1)?;
                dirty.mark_dirty(&path2)?;
            }
            txn.commit()?;
        }

        // Mark jobs as failed with different retry times
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                let retry_strategy = |attempt: u32| {
                    if attempt < 3 {
                        Some(Duration::from_secs(attempt as u64 * 10))
                    } else {
                        None
                    }
                };

                dirty.mark_job_failed(job_id1, &retry_strategy)?;
                dirty.mark_job_failed(job_id2, &retry_strategy)?;
            }
            txn.commit()?;
        }

        // Get earliest backoff time
        let txn = fixture.db.begin_read()?;
        let dirty = txn.read_dirty()?;
        let result = dirty.get_earliest_backoff(None)?;
        assert!(result.is_some());
        let (earliest_time, job_ids) = result.unwrap();
        assert_eq!(job_ids, vec![1, 2]);
        assert!(earliest_time > UnixTime::from_secs(0));

        Ok(())
    }

    #[test]
    fn test_get_jobs_waiting_for_peers() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path1 = Path::parse("path1.txt")?;
        let path2 = Path::parse("path2.txt")?;
        let job_id1 = JobId::from(1);
        let job_id2 = JobId::from(2);

        // First mark paths as dirty to create the job entries
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.mark_dirty(&path1)?;
                dirty.mark_dirty(&path2)?;
            }
            txn.commit()?;
        }

        // Mark jobs as waiting for peers
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.mark_job_missing_peers(job_id1)?;
                dirty.mark_job_missing_peers(job_id2)?;
            }
            txn.commit()?;
        }

        // Get jobs waiting for peers
        let txn = fixture.db.begin_read()?;
        let dirty = txn.read_dirty()?;
        let waiting_jobs = dirty.get_jobs_waiting_for_peers()?;
        assert_eq!(waiting_jobs, vec![1, 2]);

        Ok(())
    }

    #[test]
    fn test_delete_range() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path1 = Path::parse("path1.txt")?;
        let path2 = Path::parse("path2.txt")?;
        let path3 = Path::parse("path3.txt")?;

        // Mark multiple paths as dirty
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.mark_dirty(&path1)?;
                dirty.mark_dirty(&path2)?;
                dirty.mark_dirty(&path3)?;
            }
            txn.commit()?;
        }

        // Delete range 1-2 (counters 1 and 2)
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.delete_range(1, 3)?;
            }
            txn.commit()?;
        }

        // Verify path1 and path2 are gone, path3 remains
        {
            let txn = fixture.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            assert!(!dirty.is_dirty(&path1)?);
            assert!(!dirty.is_dirty(&path2)?);
            assert!(dirty.is_dirty(&path3)?);
        }

        Ok(())
    }

    #[test]
    fn test_clear_dirty_for_path() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("test/path.txt")?;

        // Mark path as dirty
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.mark_dirty(&path)?;
            }
            txn.commit()?;
        }

        // Verify it's dirty
        {
            let txn = fixture.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            assert!(dirty.is_dirty(&path)?);
        }

        // Clear dirty for path
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.clear_dirty_for_path(&path)?;
            }
            txn.commit()?;
        }

        // Verify it's no longer dirty
        let txn = fixture.db.begin_read()?;
        let dirty = txn.read_dirty()?;
        assert!(!dirty.is_dirty(&path)?);

        Ok(())
    }

    #[test]
    fn test_take_next_dirty_path() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path1 = Path::parse("path1.txt")?;
        let path2 = Path::parse("path2.txt")?;

        // Mark multiple paths as dirty
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.mark_dirty(&path1)?;
                dirty.mark_dirty(&path2)?;
            }
            txn.commit()?;
        }

        // Take next dirty path
        let txn = fixture.db.begin_write()?;
        {
            let mut dirty = txn.write_dirty()?;
            let result = dirty.take_next_dirty_path()?;
            assert_eq!(result, Some((path1.clone(), 1)));

            // Take next dirty path
            let result = dirty.take_next_dirty_path()?;
            assert_eq!(result, Some((path2.clone(), 2)));

            // No more dirty paths
            let result = dirty.take_next_dirty_path()?;
            assert_eq!(result, None);
        }

        txn.commit()?;
        Ok(())
    }

    #[test]
    fn test_get_lowest_counter() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path1 = Path::parse("path1.txt")?;
        let path2 = Path::parse("path2.txt")?;

        // Initially no counters
        {
            let txn = fixture.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            let lowest = dirty.get_lowest_counter()?;
            assert_eq!(lowest, None);
        }

        // Mark paths as dirty
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.mark_dirty(&path1)?;
                dirty.mark_dirty(&path2)?;
            }
            txn.commit()?;
        }

        // Get lowest counter
        {
            let txn = fixture.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            let lowest = dirty.get_lowest_counter()?;
            assert_eq!(lowest, Some(1));
        }

        // Take first path
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.take_next_dirty_path()?;
            }
            txn.commit()?;
        }

        // Get lowest counter again
        let txn = fixture.db.begin_read()?;
        let dirty = txn.read_dirty()?;
        let lowest = dirty.get_lowest_counter()?;
        assert_eq!(lowest, Some(2));

        Ok(())
    }

    #[test]
    fn test_get_last_counter() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path1 = Path::parse("path1.txt")?;
        let path2 = Path::parse("path2.txt")?;

        // Initially no counters
        {
            let txn = fixture.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            let last = dirty.get_last_counter(0)?;
            assert_eq!(last, None);
        }

        // Mark paths as dirty
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.mark_dirty(&path1)?;
                dirty.mark_dirty(&path2)?;
            }
            txn.commit()?;
        }

        // Get last counter
        let txn = fixture.db.begin_read()?;
        let dirty = txn.read_dirty()?;
        let last = dirty.get_last_counter(0)?;
        assert_eq!(last, Some(2));

        Ok(())
    }

    #[test]
    fn test_is_dirty() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("test/path.txt")?;

        // Initially not dirty
        {
            let txn = fixture.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            assert!(!dirty.is_dirty(&path)?);
        }

        // Mark as dirty
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.mark_dirty(&path)?;
            }
            txn.commit()?;
        }

        // Verify it's dirty
        {
            let txn = fixture.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            assert!(dirty.is_dirty(&path)?);
        }

        // Clear dirty
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.clear_dirty_for_path(&path)?;
            }
            txn.commit()?;
        }

        // Verify it's no longer dirty
        let txn = fixture.db.begin_read()?;
        let dirty = txn.read_dirty()?;
        assert!(!dirty.is_dirty(&path)?);

        Ok(())
    }

    #[test]
    fn test_get_dirty() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("test/path.txt")?;

        // Initially no dirty counter
        {
            let txn = fixture.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            let dirty_counter = dirty.get_dirty(&path)?;
            assert_eq!(dirty_counter, None);
        }

        // Mark as dirty
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.mark_dirty(&path)?;
            }
            txn.commit()?;
        }

        // Verify dirty counter
        {
            let txn = fixture.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            let dirty_counter = dirty.get_dirty(&path)?;
            assert_eq!(dirty_counter, Some(1));
        }

        // Clear dirty
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.clear_dirty_for_path(&path)?;
            }
            txn.commit()?;
        }

        // Verify no dirty counter
        let txn = fixture.db.begin_read()?;
        let dirty = txn.read_dirty()?;
        let dirty_counter = dirty.get_dirty(&path)?;
        assert_eq!(dirty_counter, None);

        Ok(())
    }

    #[test]
    fn test_job_lifecycle() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("test/path.txt")?;
        let job_id = JobId::from(1);

        // 1. Mark path as dirty
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.mark_dirty(&path)?;
            }
            txn.commit()?;
        }

        // Verify it's dirty
        {
            let txn = fixture.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            assert!(dirty.is_dirty(&path)?);
        }

        // 2. Get counter for path
        {
            let txn = fixture.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            let counter = dirty.get_counter_for_path(&path)?;
            assert_eq!(counter, Some(1));
        }

        // 3. Mark job as failed
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                let retry_strategy = |_| Some(Duration::from_secs(10));
                dirty.mark_job_failed(job_id, &retry_strategy)?;
            }
            txn.commit()?;
        }

        // Verify job is failed
        {
            let txn = fixture.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            assert!(dirty.is_job_failed(job_id)?);
        }

        // 4. Mark job as done
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                let was_dirty = dirty.mark_job_done(job_id)?;
                assert!(was_dirty);
            }
            txn.commit()?;
        }

        // Verify final state
        let txn = fixture.db.begin_read()?;
        let dirty = txn.read_dirty()?;
        assert!(!dirty.is_dirty(&path)?);
        assert!(!dirty.is_job_failed(job_id)?);

        Ok(())
    }

    #[test]
    fn test_multiple_jobs_same_path() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path1 = Path::parse("test/path1.txt")?;
        let path2 = Path::parse("test/path2.txt")?;
        let job_id1 = JobId::from(1);
        let job_id2 = JobId::from(2);

        // Mark different paths as dirty
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.mark_dirty(&path1)?;
                dirty.mark_dirty(&path2)?;
            }
            txn.commit()?;
        }

        // Get counters (should be 1 and 2)
        {
            let txn = fixture.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            let counter1 = dirty.get_counter_for_path(&path1)?;
            let counter2 = dirty.get_counter_for_path(&path2)?;
            assert_eq!(counter1, Some(1));
            assert_eq!(counter2, Some(2));
        }

        // Mark both jobs as failed
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                let retry_strategy = |_| Some(Duration::from_secs(10));
                dirty.mark_job_failed(job_id1, &retry_strategy)?;
                dirty.mark_job_failed(job_id2, &retry_strategy)?;
            }
            txn.commit()?;
        }

        // Both should be failed
        {
            let txn = fixture.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            assert!(dirty.is_job_failed(job_id1)?);
            assert!(dirty.is_job_failed(job_id2)?);
        }

        // Mark one as done
        {
            let txn = fixture.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                dirty.mark_job_done(job_id1)?;
            }
            txn.commit()?;
        }

        // Verify final state
        let txn = fixture.db.begin_read()?;
        let dirty = txn.read_dirty()?;
        assert!(!dirty.is_job_failed(job_id1)?);
        assert!(dirty.is_job_failed(job_id2)?);

        Ok(())
    }

    #[test]
    fn mark_dirty_recursive() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            tree.setup(Path::parse("foo/bar/baz")?)?;
            tree.setup(Path::parse("foo/bar/qux/quux")?)?;
            tree.setup(Path::parse("waldo")?)?;

            let mut dirty = txn.write_dirty()?;
            dirty.mark_dirty_recursive(&tree, Path::parse("foo/bar")?)?;
            assert!(!dirty.is_dirty(&Path::parse("foo")?)?);
            assert!(!dirty.is_dirty(&Path::parse("waldo")?)?);
            assert!(dirty.is_dirty(&Path::parse("foo/bar")?)?);
            assert!(dirty.is_dirty(&Path::parse("foo/bar/baz")?)?);
            assert!(dirty.is_dirty(&Path::parse("foo/bar/qux")?)?);
            assert!(dirty.is_dirty(&Path::parse("foo/bar/qux/quux")?)?);
        }

        Ok(())
    }
    #[test]
    fn mark_dirty_recursive_root() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            tree.setup(Path::parse("foo/bar")?)?;
            tree.setup(Path::parse("baz")?)?;

            let mut dirty = txn.write_dirty()?;
            dirty.mark_dirty_recursive(&tree, tree.root())?;
            assert!(dirty.is_dirty(&Path::parse("foo/bar")?)?);
            assert!(dirty.is_dirty(&Path::parse("baz")?)?);
        }

        Ok(())
    }
}
