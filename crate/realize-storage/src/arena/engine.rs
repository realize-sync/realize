#![allow(dead_code)] // work in progress

use crate::arena::arena_cache;
use crate::arena::blob;
use crate::arena::mark;
use crate::utils::holder::{ByteConversionError, ByteConvertible, Holder, NamedType};
use crate::{Inode, Mark, StorageError};
use capnp::message::ReaderOptions;
use capnp::serialize_packed;
use realize_types::{ByteRanges, Hash, Path, UnixTime};
use redb::{Database, ReadTransaction, ReadableTable, TableDefinition, WriteTransaction};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::{task, time};
use tokio_stream::wrappers::ReceiverStream;

use super::engine_capnp;

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

#[derive(Eq, PartialEq, Debug, Clone)]
pub(crate) enum Job {
    Download(Path, u64, Hash),
}

impl Job {
    pub(crate) fn path(&self) -> &Path {
        match self {
            Job::Download(path, _, _) => path,
        }
    }
    pub(crate) fn counter(&self) -> u64 {
        match self {
            Job::Download(_, counter, _) => *counter,
        }
    }
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
                txn.open_table(FAILED_JOB_TABLE)?;
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
        let mut failed_job_table = txn.open_table(FAILED_JOB_TABLE)?;

        let counter = last_counter(&dirty_counter_table)? + 1;
        dirty_counter_table.insert((), counter)?;
        dirty_log_table.insert(counter, path.as_str())?;
        let prev = dirty_table.insert(path.as_str(), counter)?;
        if let Some(prev) = prev {
            let counter = prev.value();
            dirty_log_table.remove(counter)?;
            failed_job_table.remove(counter)?;
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
    job_retry_strategy: Box<dyn (Fn(u32) -> Option<Duration>) + Sync + Send + 'static>,

    /// A channel that triggers whenever a new failed job is created.
    ///
    /// This is used by wait_until_next_backoff to wait for a new
    /// failed job when there weren't any before.
    failed_job_tx: watch::Sender<()>,
}

impl Engine {
    pub(crate) fn new(
        db: Arc<Database>,
        dirty_paths: Arc<DirtyPaths>,
        arena_root: Inode,
        job_retry_strategy: impl (Fn(u32) -> Option<Duration>) + Sync + Send + 'static,
    ) -> Arc<Engine> {
        let (failed_job_tx, _) = watch::channel(());

        Arc::new(Self {
            db,
            dirty_paths,
            arena_root,
            job_retry_strategy: Box::new(job_retry_strategy),
            failed_job_tx,
        })
    }

    /// Return an infinite stream of jobs.
    ///
    /// Return a stream that looks at the dirty paths on the database
    /// and report jobs that need to be run.
    ///
    /// Uninteresting entries in the dirty path tables are deleted, but entries that
    /// correspond to jobs are left, so that if the process dies, the jobs will be
    /// returned again.
    ///
    /// This stream will wait for as long as necessary for changes on
    /// the database.
    ///
    /// Multiple streams will return the same results, even in the
    /// same process, as long as no job is marked done or failed.
    pub(crate) fn job_stream(self: &Arc<Engine>) -> ReceiverStream<anyhow::Result<Job>> {
        let (tx, rx) = mpsc::channel(1);
        // Using a small buffer to avoid buffering stale jobs.

        let this = Arc::clone(self);
        // The above means that the Engine will stay in memory as long
        // as there is at least one stream. Should dropping the engine
        // instead invalidate the streams?

        tokio::spawn(async move {
            if let Err(err) = this.build_jobs(tx.clone()).await {
                let _ = tx.send(Err(err)).await;
            }
        });

        ReceiverStream::new(rx)
    }

    /// Report that a job returned by a stream has been successfully executed.
    ///
    /// The dirty mark is cleared unless the path has been modified
    /// after the job was created.
    pub(crate) fn job_done(&self, job: &Job) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        let path = job.path();
        let current = current_path_counter(&txn, path)?;
        let counter = job.counter();
        if current == Some(counter) {
            txn.open_table(FAILED_JOB_TABLE)?.remove(counter)?;
            txn.open_table(DIRTY_LOG_TABLE)?.remove(counter)?;
            txn.open_table(DIRTY_TABLE)?.remove(path.as_str())?;
        }
        txn.commit()?;

        Ok(())
    }

    /// Report that a job returned by a stream has failed.
    ///
    /// As long as the corresponding path isn't modified again, the job will be returned by
    /// the job stream when there's nothing to do and the backoff period specified by the retry
    /// strategy passed to the constructor has passed.
    ///
    /// The job is abandoned if the corresponding path is modified
    /// again or according to the retry strategy says so.
    pub(crate) fn job_failed(&self, job: &Job) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let path = job.path();
            let counter = job.counter();
            if current_path_counter(&txn, path)? != Some(counter) {
                return Ok(());
            }

            let mut failed_job_table = txn.open_table(FAILED_JOB_TABLE)?;
            let failure_count = if let Some(existing) = failed_job_table.get(counter)? {
                let existing = existing.value().parse()?;
                existing.failure_count + 1
            } else {
                1
            };
            match (self.job_retry_strategy)(failure_count) {
                None => {
                    log::warn!("Giving up on {job:?} after {failure_count} attempts");
                    failed_job_table.remove(counter)?;
                    txn.open_table(DIRTY_LOG_TABLE)?.remove(counter)?;
                    txn.open_table(DIRTY_TABLE)?.remove(path.as_str())?;
                }
                Some(delay) => {
                    log::debug!(
                        "{job:?} failed ({failure_count} attempts). Will retry after {}s",
                        delay.as_secs()
                    );
                    failed_job_table.insert(
                        counter,
                        Holder::with_content(FailedJobTableEntry {
                            failure_count,
                            backoff_until: UnixTime::now().plus(delay),
                        })?,
                    )?;
                }
            };
        }
        txn.commit()?;
        let _ = self.failed_job_tx.send(());

        Ok(())
    }

    async fn build_jobs(
        self: &Arc<Engine>,
        tx: mpsc::Sender<anyhow::Result<Job>>,
    ) -> anyhow::Result<()> {
        let mut start_counter = 0;
        let mut retry_lower_bound = None;

        loop {
            let (job, last_counter) = task::spawn_blocking({
                let this = Arc::clone(self);
                let start_counter = start_counter;

                move || {
                    let (job, last_counter) = this.next_job(start_counter)?;
                    if let Some(last_counter) = last_counter
                        && last_counter > start_counter
                    {
                        this.delete_range(start_counter, last_counter)?;
                    }

                    Ok::<_, StorageError>((job, last_counter))
                }
            })
            .await??;

            if let Some(c) = last_counter {
                start_counter = c + 1;
            }
            if let Some(job) = job {
                if self.is_failed(&job).await? {
                    // Skip it for now; it'll be handled after a delay.
                    continue;
                }
                if tx.send(Ok(job)).await.is_err() {
                    // Broken channel; normal end of this loop
                    return Ok(());
                }
            } else {
                // Wait for more dirty paths, then continue.
                // Now is also a good time to retry failed jobs whose backoff period has passed.

                let mut watch = self.dirty_paths.subscribe();
                let mut jobs_to_retry = vec![];
                tokio::select!(
                    _ = tx.closed() => {
                        return Ok(());
                    }
                    ret = watch.wait_for(|v| *v >= start_counter) => {
                        ret?;
                    }

                    Ok(Some((backoff_until, mut jobs))) = self.wait_until_next_backoff(retry_lower_bound.as_ref()) => {
                        jobs_to_retry.append(&mut jobs);

                        // From now on, only retry jobs whose backoff
                        // period ends after backoff_until. This
                        // avoids retrying the same failed jobs within
                        // a single stream, relying on newly written
                        // backoffs always being in the future.
                        retry_lower_bound = Some(backoff_until);
                    }
                );
                for counter in jobs_to_retry {
                    let job = self.build_job_with_counter(counter).await.ok().flatten();
                    if let Some(job) = job {
                        if tx.send(Ok(job)).await.is_err() {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    fn next_job(&self, start_counter: u64) -> Result<(Option<Job>, Option<u64>), StorageError> {
        let txn = self.db.begin_read()?;
        let dirty_log_table = txn.open_table(DIRTY_LOG_TABLE)?;
        let mut last_counter = None;
        for entry in dirty_log_table.range(start_counter..)? {
            let (key, value) = entry?;
            let counter = key.value();
            last_counter = Some(counter);
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

    /// Lookup the path of given a counter and build a job, if possible.
    async fn build_job_with_counter(
        self: &Arc<Self>,
        counter: u64,
    ) -> Result<Option<Job>, StorageError> {
        let this = Arc::clone(self);
        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            let dirty_log_table = txn.open_table(DIRTY_LOG_TABLE)?;
            if let Some(v) = dirty_log_table.get(counter)? {
                drop(dirty_log_table);

                let path = Path::parse(v.value())?;
                return this.build_job(&txn, path, counter);
            }
            return Ok::<_, StorageError>(None);
        })
        .await?
    }

    /// Build a job from a path, if possible.
    fn build_job(
        &self,
        txn: &ReadTransaction,
        path: Path,
        counter: u64,
    ) -> Result<Option<Job>, StorageError> {
        match mark::get_mark(txn, &path)? {
            Mark::Watch => Ok(None),
            Mark::Keep | Mark::Own => {
                let file_entry =
                    match arena_cache::get_file_entry_for_path(txn, self.arena_root, &path) {
                        Ok(e) => e,
                        Err(_) => {
                            return Ok(None);
                        }
                    };
                if let Some(blob_id) = file_entry.content.blob {
                    let file_range = ByteRanges::single(0, file_entry.metadata.size);
                    if blob::local_availability(&txn, blob_id)?.intersection(&file_range)
                        == file_range
                    {
                        // Already fully available
                        return Ok(None);
                    }
                }
                Ok(Some(Job::Download(path, counter, file_entry.content.hash)))
            }
        }
    }

    fn delete_range(&self, beg: u64, end: u64) -> Result<(), StorageError> {
        if !(beg < end) {
            return Ok(());
        }
        let txn = self.db.begin_write()?;
        {
            let mut dirty_log_table = txn.open_table(DIRTY_LOG_TABLE)?;
            let mut dirty_table = txn.open_table(DIRTY_TABLE)?;
            let mut failed_job_table = txn.open_table(FAILED_JOB_TABLE)?;

            for counter in beg..end {
                let prev = dirty_log_table.remove(counter)?;
                if let Some(prev) = prev {
                    dirty_table.remove(prev.value())?;
                }
                failed_job_table.remove(counter)?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Check whether this is a failed job.
    async fn is_failed(self: &Arc<Self>, job: &Job) -> Result<bool, StorageError> {
        let this = Arc::clone(self);
        let counter = job.counter();
        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            let failed_job_table = txn.open_table(FAILED_JOB_TABLE)?;

            Ok::<_, StorageError>(failed_job_table.get(counter)?.is_some())
        })
        .await?
    }

    /// Wait until some failed jobs are ready to be retried.
    ///
    /// Returns the ID of the jobs to retry as well as the backoff
    /// time they were assigned.
    async fn wait_until_next_backoff(
        self: &Arc<Self>,
        lower_bound: Option<&UnixTime>,
    ) -> Result<Option<(UnixTime, Vec<u64>)>, StorageError> {
        let mut rx = self.failed_job_tx.subscribe();
        loop {
            let this = Arc::clone(self);
            let lower_bound = lower_bound.cloned();
            let current = task::spawn_blocking(move || {
                let txn = this.db.begin_read()?;
                let failed_job_table = txn.open_table(FAILED_JOB_TABLE)?;
                let mut earliest_backoff = UnixTime::MAX;
                let mut counters = vec![];
                for e in failed_job_table.iter()? {
                    let (key, val) = e?;
                    let counter = key.value();
                    let backoff_until = val.value().parse()?.backoff_until;
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
                Ok::<_, StorageError>(if earliest_backoff < UnixTime::MAX {
                    Some((earliest_backoff, counters))
                } else {
                    None
                })
            })
            .await??;

            if let Some((backoff, jobs)) = current {
                tokio::select!(
                    _ = time::sleep(backoff.duration_since(&UnixTime::now())) => {
                        return Ok(Some((backoff, jobs)));
                    }
                    _ = rx.changed() => {}
                );
            } else {
                let _ = rx.changed().await;
            }
            rx.borrow_and_update();
            // A new job has failed. Re-evaluate wait time.
        }
    }
}

fn current_path_counter(txn: &WriteTransaction, path: &Path) -> Result<Option<u64>, StorageError> {
    let current = txn
        .open_table(DIRTY_TABLE)?
        .get(path.as_str())?
        .map(|e| e.value());
    Ok(current)
}

fn last_counter(
    dirty_counter_table: &impl redb::ReadableTable<(), u64>,
) -> Result<u64, StorageError> {
    Ok(dirty_counter_table.get(())?.map(|v| v.value()).unwrap_or(0))
}

/// Return the time after which the next failed job can be retried and its key.
fn next_retry(
    failed_job_table: &impl ReadableTable<u64, Holder<'static, FailedJobTableEntry>>,
) -> Result<Option<(UnixTime, u64)>, StorageError> {
    let mut ret = None;
    for e in failed_job_table.iter()? {
        let (key, val) = e?;
        let backoff_until = val.value().parse()?.backoff_until;
        if let Some((t, _)) = &ret
            && backoff_until >= *t
        {
            continue;
        }
        ret = Some((backoff_until, key.value()));
    }

    Ok(ret)
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
    let mut failed_job_table = txn.open_table(FAILED_JOB_TABLE)?;

    let prev = dirty_table.remove(path.as_str())?;
    if let Some(prev) = prev {
        let counter = prev.value();
        dirty_log_table.remove(counter)?;
        failed_job_table.remove(counter)?;
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

#[derive(Debug, Clone, Eq, PartialEq)]
struct FailedJobTableEntry {
    backoff_until: UnixTime,
    failure_count: u32,
}

impl NamedType for FailedJobTableEntry {
    fn typename() -> &'static str {
        "engine.job"
    }
}

impl ByteConvertible<FailedJobTableEntry> for FailedJobTableEntry {
    fn from_bytes(data: &[u8]) -> Result<FailedJobTableEntry, ByteConversionError> {
        let message_reader = serialize_packed::read_message(&mut &data[..], ReaderOptions::new())?;
        let msg: engine_capnp::failed_job_table_entry::Reader =
            message_reader.get_root::<engine_capnp::failed_job_table_entry::Reader>()?;

        Ok(FailedJobTableEntry {
            failure_count: msg.get_failure_count(),
            backoff_until: UnixTime::from_secs(msg.get_backoff_until_secs()),
        })
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let mut builder: engine_capnp::failed_job_table_entry::Builder =
            message.init_root::<engine_capnp::failed_job_table_entry::Builder>();

        builder.set_failure_count(self.failure_count);
        builder.set_backoff_until_secs(self.backoff_until.as_secs());

        let mut buffer: Vec<u8> = Vec::new();
        serialize_packed::write_message(&mut buffer, &message)?;

        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Notification;
    use crate::arena::arena_cache::ArenaCache;
    use crate::arena::mark::PathMarks;
    use crate::utils::redb_utils;
    use assert_fs::TempDir;
    use futures::StreamExt as _;
    use realize_types::{Arena, Peer, UnixTime};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt as _;
    use tokio::time;

    fn test_hash() -> Hash {
        Hash([1; 32])
    }

    /// Fixture for testing DirtyPaths.
    struct DirtyPathsFixture {
        db: Arc<redb::Database>,
        dirty_paths: Arc<DirtyPaths>,
    }
    impl DirtyPathsFixture {
        async fn setup() -> anyhow::Result<DirtyPathsFixture> {
            let _ = env_logger::try_init();
            let db = redb_utils::in_memory()?;
            let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;
            Ok(Self { db, dirty_paths })
        }
    }

    /// Fixture for testing Engine.
    struct EngineFixture {
        arena: Arena,
        db: Arc<redb::Database>,
        dirty_paths: Arc<DirtyPaths>,
        acache: ArenaCache,
        pathmarks: PathMarks,
        engine: Arc<Engine>,
        _tempdir: TempDir,
    }
    impl EngineFixture {
        async fn setup() -> anyhow::Result<EngineFixture> {
            let _ = env_logger::try_init();

            let tempdir = TempDir::new()?;
            let db = redb_utils::in_memory()?;
            let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;

            let arena = Arena::from("myarena");
            let arena_root = Inode(300);
            let acache = ArenaCache::new(
                arena,
                arena_root,
                Arc::clone(&db),
                tempdir.path().join("blobs"),
                Arc::clone(&dirty_paths),
            )?;
            let pathmarks = PathMarks::new(Arc::clone(&db), arena_root, Arc::clone(&dirty_paths))?;
            let engine = Engine::new(
                Arc::clone(&db),
                Arc::clone(&dirty_paths),
                arena_root,
                |attempt| {
                    if attempt < 3 {
                        Some(Duration::from_secs(attempt as u64 * 10))
                    } else {
                        None
                    }
                },
            );

            Ok(Self {
                arena,
                db,
                dirty_paths,
                acache,
                engine,
                pathmarks,
                _tempdir: tempdir,
            })
        }

        /// Add a file to the cache for testing
        fn add_file_to_cache(&self, path: &Path) -> anyhow::Result<()> {
            self.update_cache(Notification::Add {
                arena: self.arena,
                index: 1,
                path: path.clone(),
                mtime: UnixTime::from_secs(1234567890),
                size: 4,
                hash: test_hash(),
            })
        }

        fn remove_file_from_cache(&self, path: &Path) -> anyhow::Result<()> {
            self.update_cache(Notification::Remove {
                arena: self.arena,
                index: 1,
                path: path.clone(),
                old_hash: test_hash(),
            })
        }

        fn update_cache(&self, notification: Notification) -> anyhow::Result<()> {
            let test_peer = Peer::from("other");
            self.acache
                .update(test_peer, notification, || Ok((Inode(1000), Inode(2000))))?;
            Ok(())
        }

        fn lowest_counter(&self) -> anyhow::Result<Option<u64>> {
            let txn = self.db.begin_read()?;
            let dirty_log_table = txn.open_table(DIRTY_LOG_TABLE)?;

            Ok(dirty_log_table.first()?.map(|(k, _)| k.value()))
        }

        fn mark_dirty(&self, path: &Path) -> anyhow::Result<()> {
            let txn = self.db.begin_write()?;
            self.dirty_paths.mark_dirty(&txn, path)?;
            txn.commit()?;

            Ok(())
        }
    }

    async fn next_with_timeout(
        stream: &mut ReceiverStream<anyhow::Result<Job>>,
    ) -> anyhow::Result<Option<Job>> {
        let job = time::timeout(Duration::from_secs(3), stream.next()).await?;

        job.transpose()
    }

    #[tokio::test]
    async fn mark_and_take() -> anyhow::Result<()> {
        let fixture = DirtyPathsFixture::setup().await?;
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
        let fixture = DirtyPathsFixture::setup().await?;
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
        let fixture = DirtyPathsFixture::setup().await?;

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
        let fixture = DirtyPathsFixture::setup().await?;
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
        let fixture = DirtyPathsFixture::setup().await?;
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

    #[tokio::test]
    async fn job_stream_new_file_to_keep() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foodir = Path::parse("foo")?;
        let barfile = Path::parse("foo/bar.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.pathmarks.set_mark(&foodir, Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;

        let job = next_with_timeout(&mut job_stream).await?;
        assert_eq!(Some(Job::Download(barfile, 1, test_hash())), job);

        Ok(())
    }

    #[tokio::test]
    async fn convert_failed_job_table_entry() -> anyhow::Result<()> {
        let entry = FailedJobTableEntry {
            failure_count: 3,
            backoff_until: UnixTime::from_secs(1234567890),
        };

        assert_eq!(
            entry,
            FailedJobTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }

    #[tokio::test]
    async fn multiple_job_streams() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foodir = Path::parse("foo")?;
        let barfile = Path::parse("foo/bar.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.pathmarks.set_mark(&foodir, Mark::Keep)?;
        let mut job_stream2 = fixture.engine.job_stream();
        fixture.add_file_to_cache(&barfile)?;

        let goal_job = Some(Job::Download(barfile, 1, test_hash()));
        assert_eq!(goal_job, next_with_timeout(&mut job_stream).await?);
        assert_eq!(goal_job, next_with_timeout(&mut job_stream2).await?);

        let mut job_stream3 = fixture.engine.job_stream();
        assert_eq!(goal_job, next_with_timeout(&mut job_stream3).await?);

        Ok(())
    }

    #[tokio::test]
    async fn job_stream_new_file_set_to_keep_afterwards() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foodir = Path::parse("foo")?;
        let barfile = Path::parse("foo/bar.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.add_file_to_cache(&barfile)?;
        fixture.pathmarks.set_mark(&foodir, Mark::Keep)?;

        let job = next_with_timeout(&mut job_stream).await?;
        // Counter is 2, because the job was created after the 2nd
        // change only.
        assert_eq!(Some(Job::Download(barfile, 2, test_hash())), job);

        // The entries before the job that have been deleted.
        assert_eq!(Some(2), fixture.lowest_counter()?);

        Ok(())
    }

    #[tokio::test]
    async fn job_stream_file_to_keep_partially_downloaded() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foodir = Path::parse("foo")?;
        let barfile = Path::parse("foo/bar.txt")?;

        fixture.pathmarks.set_mark(&foodir, Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;
        let (inode, _) = fixture.acache.lookup_path(&barfile)?;
        {
            let mut blob = fixture.acache.open_file(inode)?;
            blob.write_all(b"te").await?;
            blob.update_db().await?;
        }

        // The stream is created after downloading to be sure it sees
        // the download.
        let mut job_stream = fixture.engine.job_stream();

        let job = next_with_timeout(&mut job_stream).await?;
        assert_eq!(Some(Job::Download(barfile, 1, test_hash())), job);

        Ok(())
    }

    #[tokio::test]
    async fn job_stream_file_to_keep_fully_downloaded() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foodir = Path::parse("foo")?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;

        fixture.pathmarks.set_mark(&foodir, Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;
        let (inode, _) = fixture.acache.lookup_path(&barfile)?;
        {
            let mut blob = fixture.acache.open_file(inode)?;
            blob.write_all(b"test").await?;
            blob.update_db().await?;
        }

        // The stream is created after downloading to be sure it sees
        // the download.
        let mut job_stream = fixture.engine.job_stream();

        // Add something else for the stream to return, or it'll just
        // hang forever.
        fixture.add_file_to_cache(&otherfile)?;

        let job = next_with_timeout(&mut job_stream).await?;
        // 1 has been skipped, because it is fully downloaded
        assert_eq!(Some(Job::Download(otherfile, 2, test_hash())), job);

        // The entries before the job that have been deleted.
        assert_eq!(Some(2), fixture.lowest_counter()?);

        Ok(())
    }

    #[tokio::test]
    async fn job_stream_file_to_keep_already_removed() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foodir = Path::parse("foo")?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;

        fixture.pathmarks.set_mark(&foodir, Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;
        fixture.remove_file_from_cache(&barfile)?;

        // Add something else for the stream to return, or it'll just
        // hang forever.
        fixture.add_file_to_cache(&otherfile)?;

        // The stream is created after the deletion to avoid race conditions.
        let mut job_stream = fixture.engine.job_stream();

        let job = next_with_timeout(&mut job_stream).await?.unwrap();
        // 1 and 2 have been skipped; as they correspond to barfile
        // creation and deletion.
        assert_eq!(Job::Download(otherfile, 3, test_hash()), job);

        // The entries before the job that have been deleted.
        assert_eq!(Some(3), fixture.lowest_counter()?);

        Ok(())
    }

    #[tokio::test]
    async fn job_done() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foodir = Path::parse("foo")?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.pathmarks.set_mark(&foodir, Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;
        fixture.add_file_to_cache(&otherfile)?;

        let job = next_with_timeout(&mut job_stream).await?.unwrap();
        fixture.engine.job_done(&job)?;

        // Any future stream won't return this job (even though the
        // job isn't really done, since the file wasn't downloaded; this isn't checked).
        let mut job_stream = fixture.engine.job_stream();
        assert_eq!(
            Some(Job::Download(otherfile, 2, test_hash())),
            next_with_timeout(&mut job_stream).await?
        );

        // The dirty log entry that correspond to the job has been deleted.
        assert_eq!(Some(2), fixture.lowest_counter()?);

        Ok(())
    }

    #[tokio::test]
    async fn job_done_but_file_modified_again() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foodir = Path::parse("foo")?;
        let barfile = Path::parse("foo/bar.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.pathmarks.set_mark(&foodir, Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;

        let job1 = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(barfile, *job1.path());
        assert_eq!(1, job1.counter());
        fixture.mark_dirty(&barfile)?;

        let job2 = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(barfile, *job2.path());
        assert_eq!(2, job2.counter());

        // Job1 is done, but that changes nothing, because the path has been modified afterwards
        fixture.engine.job_done(&job1)?;
        assert_eq!(Some(2), fixture.lowest_counter()?);

        // Since the job wasn't really done, job2 is still returned by the next stream.
        assert_eq!(
            Some(2),
            next_with_timeout(&mut fixture.engine.job_stream())
                .await?
                .map(|j| j.counter()),
        );

        // Marking job2 done erases the dirty mark.
        fixture.engine.job_done(&job2)?;
        assert_eq!(None, fixture.lowest_counter()?);

        Ok(())
    }

    #[tokio::test]
    async fn job_failed() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foodir = Path::parse("foo")?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.pathmarks.set_mark(&foodir, Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;
        fixture.add_file_to_cache(&otherfile)?;

        let job = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(barfile, *job.path());
        fixture.engine.job_failed(&job)?;

        // barfile is still there
        assert_eq!(Some(1), fixture.lowest_counter()?);

        tokio::time::pause();
        // After barfile failed, first return otherfile, because first
        // attempts have priority, then try barfile twice before giving
        // up. Everything happens immediately, because of
        // tokio::time::pause(), but in normal state there's a delay
        // between the attempts.
        //
        // Not using next_with_timeout because that wouldn't help with
        // time paused.
        let job = job_stream.next().await.unwrap()?;
        assert_eq!(otherfile, *job.path());

        // 2nd attempt
        let job = job_stream.next().await.unwrap()?;
        assert_eq!(barfile, *job.path());
        fixture.engine.job_failed(&job)?;

        // 3rd and last attempt
        let job = job_stream.next().await.unwrap()?;
        assert_eq!(barfile, *job.path());
        fixture.engine.job_failed(&job)?;

        // barfile is gone, otherfile is left
        assert_eq!(Some(2), fixture.lowest_counter()?);

        Ok(())
    }

    #[tokio::test]
    async fn new_stream_delays_failed_job() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foodir = Path::parse("foo")?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.pathmarks.set_mark(&foodir, Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;
        fixture.add_file_to_cache(&otherfile)?;

        let job = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(barfile, *job.path());
        fixture.engine.job_failed(&job)?;

        // New stream starts with otherfile, because barfile is delayed.
        // This is what happens after a restart as well.
        let mut job_stream = fixture.engine.job_stream();

        let job = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(otherfile, *job.path());
        fixture.engine.job_failed(&job)?;

        tokio::time::pause();

        // 2nd attempt
        let job = job_stream.next().await.unwrap()?;
        assert_eq!(barfile, *job.path());
        fixture.engine.job_failed(&job)?;

        Ok(())
    }

    #[tokio::test]
    async fn job_failed_then_path_modified() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foodir = Path::parse("foo")?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.pathmarks.set_mark(&foodir, Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;
        fixture.add_file_to_cache(&otherfile)?;

        let job = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(barfile, *job.path());
        fixture.engine.job_failed(&job)?;
        assert_eq!(Some(1), fixture.lowest_counter()?);

        // Modifying the path gets rid of the failed job.
        fixture.mark_dirty(&barfile)?;
        assert_eq!(Some(2), fixture.lowest_counter()?);

        let job = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(otherfile, *job.path());
        assert_eq!(2, job.counter());

        let job = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(barfile, *job.path());
        assert_eq!(3, job.counter());

        Ok(())
    }

    #[tokio::test]
    async fn job_failed_after_path_modified() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foodir = Path::parse("foo")?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.pathmarks.set_mark(&foodir, Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;
        fixture.add_file_to_cache(&otherfile)?;

        let job = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(barfile, *job.path());
        fixture.mark_dirty(&barfile)?;
        fixture.engine.job_failed(&job)?; // This does nothing

        assert_eq!(Some(2), fixture.lowest_counter()?);

        let job = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(otherfile, *job.path());
        assert_eq!(2, job.counter());

        let job = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(barfile, *job.path());
        assert_eq!(3, job.counter());

        Ok(())
    }
}
