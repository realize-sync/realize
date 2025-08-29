use super::db::AfterCommit;
use super::tree::{TreeExt, TreeLoc, TreeReadOperations};
use super::types::{FailedJobTableEntry, RetryJob};
use crate::Inode;
use crate::{JobId, StorageError, utils::holder::Holder};
use realize_types::{Arena, UnixTime};
use redb::{ReadableTable, Table};
use std::time::Duration;
use tokio::sync::watch;

pub(crate) struct Dirty {
    watch_tx: watch::Sender<u64>,
    _watch_rx: watch::Receiver<u64>,
}

impl Dirty {
    pub(crate) fn new(log_table: &impl ReadableTable<u64, Inode>) -> Result<Self, StorageError> {
        let last_counter = last_counter(log_table)?;
        let (watch_tx, watch_rx) = watch::channel(last_counter);

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
    DT: ReadableTable<Inode, u64>,
    LT: ReadableTable<u64, Inode>,
    FT: ReadableTable<u64, Holder<'static, FailedJobTableEntry>>,
{
    table: DT,
    log_table: LT,
    failed_job_table: FT,
}

impl<DT, LT, FT> ReadableOpenDirty<DT, LT, FT>
where
    DT: ReadableTable<Inode, u64>,
    LT: ReadableTable<u64, Inode>,
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
    table: Table<'a, Inode, u64>,
    log_table: Table<'a, u64, Inode>,
    failed_job_table: Table<'a, u64, Holder<'static, FailedJobTableEntry>>,
    counter_table: Table<'a, (), u64>,
    arena: Arena,
}

impl<'a> WritableOpenDirty<'a> {
    pub(crate) fn new(
        after_commit: &'a AfterCommit,
        dirty: &'a Dirty,
        dirty_table: Table<'a, Inode, u64>,
        log_table: Table<'a, u64, Inode>,
        failed_job_table: Table<'a, u64, Holder<FailedJobTableEntry>>,
        counter_table: Table<'a, (), u64>,
        arena: Arena,
    ) -> Self {
        Self {
            after_commit,
            dirty,
            table: dirty_table,
            log_table,
            failed_job_table,
            counter_table,
            arena,
        }
    }
}

pub(crate) trait DirtyReadOperations {
    fn next_dirty(&self, start_counter: u64) -> Result<Option<(Inode, u64)>, StorageError>;
    #[allow(dead_code)] // for testing
    fn last_counter(&self) -> Result<u64, StorageError>;
    fn get_inode_for_counter(&self, counter: u64) -> Result<Option<Inode>, StorageError>;
    fn get_counter(&self, inode: Inode) -> Result<Option<u64>, StorageError>;
    fn is_job_failed(&self, job_id: JobId) -> Result<bool, StorageError>;
    fn get_jobs_waiting_for_peers(&self) -> Result<Vec<u64>, StorageError>;
    fn get_earliest_backoff(
        &self,
        lower_bound: Option<UnixTime>,
    ) -> Result<Option<(UnixTime, Vec<u64>)>, StorageError>;
}

impl<DT, LT, FT> DirtyReadOperations for ReadableOpenDirty<DT, LT, FT>
where
    DT: ReadableTable<Inode, u64>,
    LT: ReadableTable<u64, Inode>,
    FT: ReadableTable<u64, Holder<'static, FailedJobTableEntry>>,
{
    fn next_dirty(&self, start_counter: u64) -> Result<Option<(Inode, u64)>, StorageError> {
        next_dirty(&self.log_table, start_counter)
    }

    fn last_counter(&self) -> Result<u64, StorageError> {
        last_counter(&self.log_table)
    }

    fn get_inode_for_counter(&self, counter: u64) -> Result<Option<Inode>, StorageError> {
        get_inode_for_counter(&self.log_table, counter)
    }

    fn get_counter(&self, inode: Inode) -> Result<Option<u64>, StorageError> {
        get_counter(&self.table, inode)
    }

    fn is_job_failed(&self, job_id: JobId) -> Result<bool, StorageError> {
        is_job_failed(&self.failed_job_table, job_id)
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
}

impl<'a> DirtyReadOperations for WritableOpenDirty<'a> {
    fn next_dirty(&self, start_counter: u64) -> Result<Option<(Inode, u64)>, StorageError> {
        next_dirty(&self.log_table, start_counter)
    }

    fn last_counter(&self) -> Result<u64, StorageError> {
        last_counter(&self.log_table)
    }

    fn get_inode_for_counter(&self, counter: u64) -> Result<Option<Inode>, StorageError> {
        get_inode_for_counter(&self.log_table, counter)
    }

    fn get_counter(&self, inode: Inode) -> Result<Option<u64>, StorageError> {
        get_counter(&self.table, inode)
    }

    fn is_job_failed(&self, job_id: JobId) -> Result<bool, StorageError> {
        is_job_failed(&self.failed_job_table, job_id)
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
        let inode = match self.log_table.get(job_id.as_u64())? {
            Some(v) => v.value(),
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
                self.table.remove(inode)?;

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

    /// Mark a tree location and all its children dirty.
    pub(crate) fn mark_dirty_recursive<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &impl TreeReadOperations,
        loc: L,
        reason: &'static str,
    ) -> Result<(), StorageError> {
        if let Some(start) = tree.resolve(loc)? {
            // Marking a directory does nothing, so let's skip marking
            // root. The other inodes, however, might be a file, at
            // least in some subsystems so should always be marked.
            if start != tree.root() {
                self.mark_dirty(start, reason)?;
            }
            for inode in tree.recurse(start, |_| true) {
                self.mark_dirty(inode?, reason)?;
            }
        }

        Ok(())
    }

    /// Mark a path dirty.
    ///
    /// This does nothing if the path is already dirty.
    pub(crate) fn mark_dirty(
        &mut self,
        inode: Inode,
        reason: &'static str,
    ) -> Result<(), StorageError> {
        let last_counter = self.counter_table.get(())?.map(|e| e.value()).unwrap_or(0);
        let counter = last_counter + 1;
        log::debug!("[{}] Dirty inode {inode} ({reason}) #{counter}", self.arena);
        self.counter_table.insert((), counter)?;
        self.log_table.insert(counter, inode)?;
        let prev = self.table.insert(inode, counter)?;
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

fn next_dirty(
    log_table: &impl ReadableTable<u64, Inode>,
    start_counter: u64,
) -> Result<Option<(Inode, u64)>, StorageError> {
    for entry in log_table.range(start_counter..)? {
        let (key, value) = entry?;
        let counter = key.value();
        let inode = value.value();
        return Ok(Some((inode, counter)));
    }
    Ok(None)
}

fn last_counter(log_table: &impl ReadableTable<u64, Inode>) -> Result<u64, StorageError> {
    Ok(log_table.last()?.map(|(k, _)| k.value()).unwrap_or(0))
}

fn get_inode_for_counter(
    log_table: &impl ReadableTable<u64, Inode>,
    counter: u64,
) -> Result<Option<Inode>, StorageError> {
    if let Some(v) = log_table.get(counter)? {
        Ok(Some(v.value()))
    } else {
        Ok(None)
    }
}

fn get_counter(
    table: &impl ReadableTable<Inode, u64>,
    inode: Inode,
) -> Result<Option<u64>, StorageError> {
    if let Some(entry) = table.get(inode)? {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arena::db::ArenaDatabase;
    use realize_types::{Arena, Path};
    use std::collections::HashSet;
    use std::sync::Arc;

    struct Fixture {
        db: Arc<ArenaDatabase>,
    }

    impl Fixture {
        fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();
            let arena = Arena::from("myarena");
            let db =
                ArenaDatabase::for_testing_single_arena(arena, std::path::Path::new("/dev/null"))?;

            Ok(Self { db })
        }
    }

    fn all(dirty: &impl DirtyReadOperations) -> Result<Vec<(Inode, u64)>, StorageError> {
        let mut start = 0;
        let mut vec = vec![];
        while let Some((inode, counter)) = dirty.next_dirty(start)? {
            vec.push((inode, counter));
            start = counter + 1;
        }

        Ok(vec)
    }

    fn all_inodes(dirty: &impl DirtyReadOperations) -> Result<HashSet<Inode>, StorageError> {
        Ok(all(dirty)?.into_iter().map(|(i, _)| i).collect())
    }

    fn all_paths(
        dirty: &impl DirtyReadOperations,
        tree: &impl TreeReadOperations,
    ) -> Result<HashSet<Path>, StorageError> {
        Ok(all(dirty)?
            .into_iter()
            .filter_map(|(inode, _)| tree.backtrack(inode).ok().flatten())
            .collect())
    }

    #[test]
    fn read_txn_with_dirty() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_read()?;
        let dirty = txn.read_dirty()?;

        // just make sure it doesn't fail
        dirty.next_dirty(0)?;

        Ok(())
    }

    #[test]
    fn write_txn_with_dirty() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let dirty = txn.read_dirty()?;

        // just make sure it doesn't fail
        dirty.next_dirty(0)?;

        Ok(())
    }

    #[test]
    fn write_dirty() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut dirty = txn.write_dirty()?;

        dirty.mark_dirty(Inode(1), "test")?;

        Ok(())
    }

    #[test]
    fn mark() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let path1 = tree.setup(Path::parse("path1")?)?;
        let path2 = tree.setup(Path::parse("path2")?)?;

        let mut dirty = txn.write_dirty()?;
        dirty.mark_dirty(path1, "test")?;
        dirty.mark_dirty(path2, "test")?;

        assert_eq!(vec![(path1, 1), (path2, 2)], all(&dirty)?);

        Ok(())
    }

    #[test]
    fn increase_counter() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut dirty = txn.write_dirty()?;
        let mut tree = txn.write_tree()?;
        let path1 = tree.setup(Path::parse("path1")?)?;
        let path2 = tree.setup(Path::parse("path2")?)?;

        dirty.mark_dirty(path1, "test")?;
        dirty.mark_dirty(path2, "test")?;
        dirty.mark_dirty(path1, "test")?;
        dirty.mark_dirty(path1, "test")?; // dup; just increase counter

        // return path2 first, because its counter is lower
        assert_eq!(vec![(path2, 2), (path1, 4)], all(&dirty)?);

        Ok(())
    }

    #[test]
    fn test_mark_job_done() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let path = tree.setup(Path::parse("test/path.txt")?)?;
        let job_id = JobId::from(1);

        // Mark path as dirty first
        dirty.mark_dirty(path, "test")?;

        // Verify it's dirty
        assert_eq!(HashSet::from([path]), all_inodes(&dirty)?);

        // Mark job as done
        let was_dirty = dirty.mark_job_done(job_id)?;
        assert!(was_dirty);

        // Verify path is no longer dirty
        assert_eq!(HashSet::new(), all_inodes(&dirty)?);

        // Mark job as done again (should return false)
        let was_dirty = dirty.mark_job_done(job_id)?;
        assert!(!was_dirty);

        Ok(())
    }

    #[test]
    fn test_mark_job_failed() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let path = tree.setup(Path::parse("test/path.txt")?)?;
        let job_id = JobId::from(1);

        // Mark path as dirty first
        dirty.mark_dirty(path, "test")?;

        // Mark job as failed with retry strategy
        let retry_strategy = |attempt: u32| {
            if attempt < 3 {
                Some(Duration::from_secs(attempt as u64 * 10))
            } else {
                None
            }
        };
        let retry_after = dirty.mark_job_failed(job_id, &retry_strategy)?;
        assert!(retry_after.is_some());

        // Verify job is marked as failed
        assert!(dirty.is_job_failed(job_id)?);

        Ok(())
    }

    #[test]
    fn test_mark_job_missing_peers() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let path = tree.setup(Path::parse("test/path.txt")?)?;
        let job_id = JobId::from(1);

        // First mark a path as dirty to create the job entry
        dirty.mark_dirty(path, "test")?;

        // Mark job as missing peers
        dirty.mark_job_missing_peers(job_id)?;

        // Verify job is waiting for peers
        let waiting_jobs = dirty.get_jobs_waiting_for_peers()?;
        assert_eq!(waiting_jobs, vec![1]);

        Ok(())
    }

    #[test]
    fn test_next_dirty_path() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut dirty = txn.write_dirty()?;
        let mut tree = txn.write_tree()?;
        let path1 = tree.setup(Path::parse("path1.txt")?)?;
        let path2 = tree.setup(Path::parse("path2.txt")?)?;

        // Mark multiple paths as dirty
        dirty.mark_dirty(path1, "test")?;
        dirty.mark_dirty(path2, "test")?;

        // Get next dirty path starting from counter 0
        let mut start_counter = 0;
        let result = dirty.next_dirty(start_counter)?;
        assert_eq!(result, Some((path1.clone(), 1)));
        start_counter = 2; // Start from counter 2 to get the next entry

        // Get next dirty path
        let result = dirty.next_dirty(start_counter)?;
        assert_eq!(result, Some((path2.clone(), 2)));
        start_counter = 3; // Start from counter 3 to get the next entry

        // No more dirty paths
        let result = dirty.next_dirty(start_counter)?;
        assert_eq!(result, None);

        Ok(())
    }

    #[test]
    fn test_get_path_for_counter() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut dirty = txn.write_dirty()?;
        let mut tree = txn.write_tree()?;

        let path = tree.setup(Path::parse("test/path.txt")?)?;

        // Mark path as dirty
        dirty.mark_dirty(path, "test")?;

        // Get path for counter 1
        let result = dirty.get_inode_for_counter(1)?;
        assert_eq!(result, Some(path.clone()));

        // Get path for non-existent counter
        let result = dirty.get_inode_for_counter(999)?;
        assert_eq!(result, None);

        Ok(())
    }

    #[test]
    fn test_get_counter() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut dirty = txn.write_dirty()?;
        let mut tree = txn.write_tree()?;
        let path = tree.setup(Path::parse("test/path.txt")?)?;

        // Mark path as dirty
        dirty.mark_dirty(path, "test")?;

        // Get counter for path
        let result = dirty.get_counter(path)?;
        assert_eq!(result, Some(1));

        // Get counter for non-existent path
        let result = dirty.get_counter(Inode(999))?;
        assert_eq!(result, None);

        Ok(())
    }

    #[test]
    fn test_is_job_failed() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut dirty = txn.write_dirty()?;
        let mut tree = txn.write_tree()?;
        let path = tree.setup(Path::parse("test/path.txt")?)?;
        let job_id = JobId::from(1);

        // First mark a path as dirty to create the job entry
        dirty.mark_dirty(path, "test")?;

        // Job should not be failed initially
        assert!(!dirty.is_job_failed(job_id)?);

        // Mark job as failed
        let retry_strategy = |_| Some(Duration::from_secs(10));
        dirty.mark_job_failed(job_id, &retry_strategy)?;

        // Job should now be failed
        assert!(dirty.is_job_failed(job_id)?);

        Ok(())
    }

    #[test]
    fn test_get_earliest_backoff() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut dirty = txn.write_dirty()?;
        let mut tree = txn.write_tree()?;
        let path1 = tree.setup(Path::parse("path1.txt")?)?;
        let path2 = tree.setup(Path::parse("path2.txt")?)?;
        let job_id1 = JobId::from(1);
        let job_id2 = JobId::from(2);

        // First mark paths as dirty to create the job entries
        dirty.mark_dirty(path1, "test")?;
        dirty.mark_dirty(path2, "test")?;

        // Mark jobs as failed with different retry times
        let retry_strategy = |attempt: u32| {
            if attempt < 3 {
                Some(Duration::from_secs(attempt as u64 * 10))
            } else {
                None
            }
        };

        dirty.mark_job_failed(job_id1, &retry_strategy)?;
        dirty.mark_job_failed(job_id2, &retry_strategy)?;

        // Get earliest backoff time
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
        let txn = fixture.db.begin_write()?;
        let mut dirty = txn.write_dirty()?;
        let mut tree = txn.write_tree()?;
        let path1 = tree.setup(Path::parse("path1.txt")?)?;
        let path2 = tree.setup(Path::parse("path2.txt")?)?;
        let job_id1 = JobId::from(1);
        let job_id2 = JobId::from(2);

        // First mark paths as dirty to create the job entries
        dirty.mark_dirty(path1, "test")?;
        dirty.mark_dirty(path2, "test")?;

        // Mark jobs as waiting for peers
        dirty.mark_job_missing_peers(job_id1)?;
        dirty.mark_job_missing_peers(job_id2)?;

        // Get jobs waiting for peers
        let waiting_jobs = dirty.get_jobs_waiting_for_peers()?;
        assert_eq!(waiting_jobs, vec![1, 2]);

        Ok(())
    }

    #[test]
    fn test_delete_range() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut dirty = txn.write_dirty()?;
        let mut tree = txn.write_tree()?;
        let path1 = tree.setup(Path::parse("path1.txt")?)?;
        let path2 = tree.setup(Path::parse("path2.txt")?)?;
        let path3 = tree.setup(Path::parse("path3.txt")?)?;
        dirty.mark_dirty(path1, "test")?;
        dirty.mark_dirty(path2, "test")?;
        dirty.mark_dirty(path3, "test")?;

        dirty.delete_range(1, 3)?;

        // path 1 and 2 should have been deleted but not 3, as range
        // end is exclusive.
        assert_eq!(HashSet::from([path3]), all_inodes(&dirty)?);

        Ok(())
    }

    #[test]
    fn test_last_counter() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut dirty = txn.write_dirty()?;
        let mut tree = txn.write_tree()?;
        let path1 = tree.setup(Path::parse("path1.txt")?)?;
        let path2 = tree.setup(Path::parse("path2.txt")?)?;

        assert_eq!(0, dirty.last_counter()?);

        // Mark paths as dirty
        dirty.mark_dirty(path1, "test")?;
        dirty.mark_dirty(path2, "test")?;

        // Get last counter
        assert_eq!(2, dirty.last_counter()?);

        Ok(())
    }

    #[test]
    fn test_job_lifecycle() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut dirty = txn.write_dirty()?;
        let mut tree = txn.write_tree()?;
        let path = tree.setup(Path::parse("test/path.txt")?)?;
        let job_id = JobId::from(1);

        dirty.mark_dirty(path, "test")?;
        let counter = dirty.get_counter(path)?;
        assert_eq!(counter, Some(1));

        let retry_strategy = |_| Some(Duration::from_secs(10));
        dirty.mark_job_failed(job_id, &retry_strategy)?;
        assert!(dirty.is_job_failed(job_id)?);

        let was_dirty = dirty.mark_job_done(job_id)?;
        assert!(was_dirty);
        assert_eq!(HashSet::new(), all_inodes(&dirty)?);
        assert!(!dirty.is_job_failed(job_id)?);

        Ok(())
    }

    #[test]
    fn test_multiple_jobs() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut dirty = txn.write_dirty()?;
        let mut tree = txn.write_tree()?;
        let path1 = tree.setup(Path::parse("test/path1.txt")?)?;
        let path2 = tree.setup(Path::parse("test/path2.txt")?)?;
        let job_id1 = JobId::from(1);
        let job_id2 = JobId::from(2);

        dirty.mark_dirty(path1, "test")?;
        dirty.mark_dirty(path2, "test")?;

        let counter1 = dirty.get_counter(path1)?;
        let counter2 = dirty.get_counter(path2)?;
        assert_eq!(counter1, Some(job_id1.as_u64()));
        assert_eq!(counter2, Some(job_id2.as_u64()));

        let retry_strategy = |_| Some(Duration::from_secs(10));
        dirty.mark_job_failed(job_id1, &retry_strategy)?;
        dirty.mark_job_failed(job_id2, &retry_strategy)?;
        assert!(dirty.is_job_failed(job_id1)?);
        assert!(dirty.is_job_failed(job_id2)?);

        dirty.mark_job_done(job_id1)?;

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
            dirty.mark_dirty_recursive(&tree, Path::parse("foo/bar")?, "test")?;
            assert_eq!(
                HashSet::from([
                    Path::parse("foo/bar")?,
                    Path::parse("foo/bar/baz")?,
                    Path::parse("foo/bar/qux")?,
                    Path::parse("foo/bar/qux/quux")?,
                ]),
                all_paths(&dirty, &tree)?
            );
            // foo and waldo are not dirty
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
            dirty.mark_dirty_recursive(&tree, tree.root(), "test")?;
            assert_eq!(
                HashSet::from([
                    Path::parse("foo")?,
                    Path::parse("foo/bar")?,
                    Path::parse("baz")?
                ]),
                all_paths(&dirty, &tree)?
            );
        }

        Ok(())
    }
}
