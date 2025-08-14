#![allow(dead_code)] // work in progress

use super::arena_cache::ArenaCache;
use super::db::{ArenaDatabase, ArenaReadTransaction, ArenaWriteTransaction};
use super::index::RealIndex;
use super::mark::PathMarks;
use super::types::{FailedJobTableEntry, LocalAvailability, RetryJob};
use crate::arena::blob;
use crate::types::JobId;
use crate::utils::holder::Holder;
use crate::{Inode, Mark, StorageError};
use realize_types::{Arena, Hash, Path, UnixTime};
use redb::ReadableTable;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::sync::{broadcast, mpsc};
use tokio::{task, time};
use tokio_stream::wrappers::ReceiverStream;

/// Some change the [Engine] finds necessary.
#[derive(Eq, PartialEq, Debug, Clone)]
pub enum Job {
    // TODO: use named fields
    Download(Path, Hash),

    /// Realize `Path` with `counter`, moving `Hash` from cache to the
    /// index, currently containing nothing or `Hash`.
    Realize(Path, Hash, Option<Hash>),
}

impl Job {
    /// Return the path the job is for.
    pub fn path(&self) -> &Path {
        match self {
            Self::Download(path, _) => path,
            Self::Realize(path, _, _) => path,
        }
    }

    /// Return the hash the job is for.
    pub fn hash(&self) -> &Hash {
        match self {
            Self::Download(_, h) => h,
            Self::Realize(_, h, _) => h,
        }
    }
}

/// An extended representation of [Job] that includes jobs not exposed
/// outside of this crate.
#[derive(Eq, PartialEq, Debug, Clone)]
pub(crate) enum StorageJob {
    External(Job),

    /// Move file containing the given version into the cache.
    Unrealize(Path, Hash),
}

impl StorageJob {
    /// Return the path the job is for.
    pub(crate) fn path(&self) -> &Path {
        match self {
            StorageJob::External(job) => job.path(),
            Self::Unrealize(path, _) => path,
        }
    }

    pub(crate) fn into_external(self) -> Option<Job> {
        match self {
            Self::External(job) => Some(job),
            _ => None,
        }
    }
}

/// The result of processing a job.
///
/// The full status has type `Result<JobStatus>`, to make it
/// convenient as a return value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JobStatus {
    /// The job was processed successfully.
    Done,

    /// The job was abandoned
    Abandoned(&'static str),

    /// The job was cancelled
    Cancelled,

    /// The job couldn't run because it required communicating with
    /// unconnected peers. The job should be retried after peers have
    /// connected.
    NoPeers,
}

pub(crate) struct DirtyPaths {
    watch_tx: watch::Sender<u64>,
    _watch_rx: watch::Receiver<u64>,
}

impl DirtyPaths {
    pub(crate) async fn new(db: Arc<ArenaDatabase>) -> Result<Arc<DirtyPaths>, StorageError> {
        Ok(task::spawn_blocking(move || DirtyPaths::new_blocking(db)).await??)
    }

    pub(crate) fn new_blocking(db: Arc<ArenaDatabase>) -> Result<Arc<DirtyPaths>, StorageError> {
        let last_counter = {
            let counter: u64;

            let txn = db.begin_write()?;
            {
                let dirty_counter_table = txn.dirty_counter_table()?;
                counter = last_counter(&dirty_counter_table)?;
            }
            txn.commit()?;

            Ok::<_, StorageError>(counter)
        }?;
        let (watch_tx, watch_rx) = watch::channel(last_counter);

        Ok(Arc::new(Self {
            watch_tx,
            _watch_rx: watch_rx,
        }))
    }

    fn subscribe(&self) -> watch::Receiver<u64> {
        self.watch_tx.subscribe()
    }

    /// Mark a job as completed and remove it from dirty tracking.
    /// Returns true if the job was found and removed, false if it was already gone.
    pub(crate) fn mark_job_done(
        &self,
        txn: &ArenaWriteTransaction,
        job_id: JobId,
    ) -> Result<bool, StorageError> {
        if let Some(removed) = txn.dirty_log_table()?.remove(job_id.as_u64())? {
            txn.failed_job_table()?.remove(job_id.as_u64())?;
            txn.dirty_table()?.remove(removed.value())?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Mark a job as failed with retry strategy.
    /// Returns the time after which the job will be retried, or None if abandoned.
    pub(crate) fn mark_job_failed(
        &self,
        txn: &ArenaWriteTransaction,
        job_id: JobId,
        retry_strategy: &dyn Fn(u32) -> Option<Duration>,
    ) -> Result<Option<UnixTime>, StorageError> {
        let mut dirty_log_table = txn.dirty_log_table()?;
        let path = match dirty_log_table.get(job_id.as_u64())? {
            Some(v) => v.value().to_string(),
            None => {
                return Ok(None);
            }
        };
        let counter = job_id.as_u64();
        let mut failed_job_table = txn.failed_job_table()?;
        let failure_count = if let Some(existing) = failed_job_table.get(counter)? {
            let existing = existing.value().parse()?;
            existing.failure_count + 1
        } else {
            1
        };

        match retry_strategy(failure_count) {
            None => {
                dirty_log_table.remove(counter)?;
                failed_job_table.remove(counter)?;
                txn.dirty_table()?.remove(path.as_str())?;

                Ok(None)
            }
            Some(delay) => {
                let backoff_until = UnixTime::now().plus(delay);
                failed_job_table.insert(
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
    pub(crate) fn mark_job_missing_peers(
        &self,
        txn: &ArenaWriteTransaction,
        job_id: JobId,
    ) -> Result<(), StorageError> {
        let dirty_log_table = txn.dirty_log_table()?;
        let _path = match dirty_log_table.get(job_id.as_u64())? {
            Some(v) => v.value().to_string(),
            None => {
                return Ok(());
            }
        };
        let counter = job_id.as_u64();
        let mut failed_job_table = txn.failed_job_table()?;

        // This isn't considered a failure, but keep the existing failure count.
        let failure_count = if let Some(existing) = failed_job_table.get(counter)? {
            let existing = existing.value().parse()?;
            existing.failure_count
        } else {
            0
        };

        failed_job_table.insert(
            counter,
            Holder::with_content(FailedJobTableEntry {
                failure_count,
                retry: RetryJob::WhenPeerConnects,
            })?,
        )?;

        Ok(())
    }

    /// Get the next dirty path starting from the given counter.
    /// Returns (path, counter) if found, or None if no more dirty paths.
    pub(crate) fn next_dirty_path(
        &self,
        txn: &ArenaReadTransaction,
        start_counter: u64,
    ) -> Result<Option<(Path, u64)>, StorageError> {
        let dirty_log_table = txn.dirty_log_table()?;
        for entry in dirty_log_table.range(start_counter..)? {
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

    /// Get the last counter from the dirty log table.
    /// Returns the highest counter value, or None if the table is empty.
    pub(crate) fn get_last_counter(
        &self,
        txn: &ArenaReadTransaction,
        start_counter: u64,
    ) -> Result<Option<u64>, StorageError> {
        let dirty_log_table = txn.dirty_log_table()?;
        let mut last_counter = None;
        for entry in dirty_log_table.range(start_counter..)? {
            let (key, _) = entry?;
            last_counter = Some(key.value());
        }
        Ok(last_counter)
    }

    /// Get the path for a specific job counter.
    pub(crate) fn get_path_for_counter(
        &self,
        txn: &ArenaReadTransaction,
        counter: u64,
    ) -> Result<Option<Path>, StorageError> {
        let dirty_log_table = txn.dirty_log_table()?;
        if let Some(v) = dirty_log_table.get(counter)? {
            let path = Path::parse(v.value())?;
            Ok(Some(path))
        } else {
            Ok(None)
        }
    }

    /// Get the counter for a specific path.
    pub(crate) fn get_counter_for_path(
        &self,
        txn: &ArenaReadTransaction,
        path: &Path,
    ) -> Result<Option<u64>, StorageError> {
        let dirty_table = txn.dirty_table()?;
        if let Some(entry) = dirty_table.get(path.as_str())? {
            Ok(Some(entry.value()))
        } else {
            Ok(None)
        }
    }

    /// Check if a job is marked as failed.
    pub(crate) fn is_job_failed(
        &self,
        txn: &ArenaReadTransaction,
        job_id: JobId,
    ) -> Result<bool, StorageError> {
        let failed_job_table = txn.failed_job_table()?;
        Ok(failed_job_table.get(job_id.as_u64())?.is_some())
    }

    /// Get all failed jobs that are ready to be retried after the given time.
    pub(crate) fn get_retryable_jobs(
        &self,
        txn: &ArenaReadTransaction,
        after_time: UnixTime,
    ) -> Result<Vec<u64>, StorageError> {
        let failed_job_table = txn.failed_job_table()?;
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

    /// Get all jobs waiting for peer connection.
    pub(crate) fn get_jobs_waiting_for_peers(
        &self,
        txn: &ArenaReadTransaction,
    ) -> Result<Vec<u64>, StorageError> {
        let failed_job_table = txn.failed_job_table()?;
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

    /// Get the earliest backoff time and associated jobs.
    /// Returns (earliest_backoff, job_counters) if any jobs are found, or None if no jobs.
    pub(crate) fn get_earliest_backoff(
        &self,
        txn: &ArenaReadTransaction,
        lower_bound: Option<UnixTime>,
    ) -> Result<Option<(UnixTime, Vec<u64>)>, StorageError> {
        let failed_job_table = txn.failed_job_table()?;
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

    /// Delete a range of counters (for cleanup).
    pub(crate) fn delete_range(
        &self,
        txn: &ArenaWriteTransaction,
        beg: u64,
        end: u64,
    ) -> Result<(), StorageError> {
        if !(beg < end) {
            return Ok(());
        }
        let mut dirty_log_table = txn.dirty_log_table()?;
        let mut dirty_table = txn.dirty_table()?;
        let mut failed_job_table = txn.failed_job_table()?;

        for counter in beg..end {
            let prev = dirty_log_table.remove(counter)?;
            if let Some(prev) = prev {
                dirty_table.remove(prev.value())?;
            }
            failed_job_table.remove(counter)?;
        }

        Ok(())
    }

    /// Clear the dirty mark for a specific path.
    pub(crate) fn clear_dirty_for_path(
        &self,
        txn: &ArenaWriteTransaction,
        path: &Path,
    ) -> Result<(), StorageError> {
        let mut dirty_table = txn.dirty_table()?;
        let mut dirty_log_table = txn.dirty_log_table()?;
        let mut failed_job_table = txn.failed_job_table()?;

        let prev = dirty_table.remove(path.as_str())?;
        if let Some(prev) = prev {
            let counter = prev.value();
            dirty_log_table.remove(counter)?;
            failed_job_table.remove(counter)?;
        }

        Ok(())
    }

    /// Take the next dirty path for processing (removes it from tracking).
    pub(crate) fn take_next_dirty_path(
        &self,
        txn: &ArenaWriteTransaction,
    ) -> Result<Option<(Path, u64)>, StorageError> {
        let mut dirty_table = txn.dirty_table()?;
        let mut dirty_log_table = txn.dirty_log_table()?;
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

    /// Get the lowest counter currently in the system.
    pub(crate) fn get_lowest_counter(
        &self,
        txn: &ArenaReadTransaction,
    ) -> Result<Option<u64>, StorageError> {
        let dirty_log_table = txn.dirty_log_table()?;
        Ok(dirty_log_table.first()?.map(|(k, _)| k.value()))
    }

    /// Check if a path is dirty.
    pub(crate) fn is_dirty<T>(
        &self,
        txn: &ArenaReadTransaction,
        path: T,
    ) -> Result<bool, StorageError>
    where
        T: AsRef<Path>,
    {
        let path = path.as_ref();
        let dirty_table = txn.dirty_table()?;
        Ok(dirty_table.get(path.as_str())?.is_some())
    }

    /// Get the dirty counter for a path.
    pub(crate) fn get_dirty<T>(
        &self,
        txn: &ArenaReadTransaction,
        path: T,
    ) -> Result<Option<u64>, StorageError>
    where
        T: AsRef<Path>,
    {
        let path = path.as_ref();
        let dirty_table = txn.dirty_table()?;
        let entry = dirty_table.get(path.as_str())?;
        Ok(entry.map(|e| e.value()))
    }

    /// Mark a path dirty.
    ///
    /// This does nothing if the path is already dirty.
    pub(crate) fn mark_dirty<T>(
        &self,
        txn: &ArenaWriteTransaction,
        path: T,
    ) -> Result<(), StorageError>
    where
        T: AsRef<Path>,
    {
        let path = path.as_ref();
        let mut dirty_table = txn.dirty_table()?;
        let mut dirty_log_table = txn.dirty_log_table()?;
        let mut dirty_counter_table = txn.dirty_counter_table()?;
        let mut failed_job_table = txn.failed_job_table()?;

        log::debug!("dirty: {path}");
        let counter = last_counter(&dirty_counter_table)? + 1;
        dirty_counter_table.insert((), counter)?;
        dirty_log_table.insert(counter, path.as_str())?;
        let prev = dirty_table.insert(path.as_str(), counter)?;
        if let Some(prev) = prev {
            let counter = prev.value();
            dirty_log_table.remove(counter)?;
            failed_job_table.remove(counter)?;
        }
        let watch_tx = self.watch_tx.clone();
        txn.after_commit(move || {
            let _ = watch_tx.send(counter);
        });

        Ok(())
    }
}

pub struct Engine {
    arena: Arena,
    db: Arc<ArenaDatabase>,
    dirty_paths: Arc<DirtyPaths>,
    index: Option<Arc<dyn RealIndex>>,
    cache: Arc<ArenaCache>,
    marks: Arc<dyn PathMarks>,
    arena_root: Inode,
    job_retry_strategy: Box<dyn (Fn(u32) -> Option<Duration>) + Sync + Send + 'static>,

    /// A channel that triggers whenever a new failed job is created.
    ///
    /// This is used by wait_until_next_backoff to wait for a new
    /// failed job when there weren't any before.
    failed_job_tx: watch::Sender<()>,

    retry_jobs_missing_peers: broadcast::Sender<()>,
}

impl Engine {
    pub(crate) fn new(
        arena: Arena,
        db: Arc<ArenaDatabase>,
        index: Option<Arc<dyn RealIndex>>,
        cache: Arc<ArenaCache>,
        marks: Arc<dyn PathMarks>,
        dirty_paths: Arc<DirtyPaths>,
        arena_root: Inode,
        job_retry_strategy: impl (Fn(u32) -> Option<Duration>) + Sync + Send + 'static,
    ) -> Arc<Engine> {
        let (failed_job_tx, _) = watch::channel(());
        let (retry_jobs_missing_peers, _) = broadcast::channel(8);
        Arc::new(Self {
            arena,
            db,
            index,
            cache,
            marks,
            dirty_paths,
            arena_root,
            job_retry_strategy: Box::new(job_retry_strategy),
            failed_job_tx,
            retry_jobs_missing_peers,
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
    pub fn job_stream(self: &Arc<Engine>) -> ReceiverStream<(JobId, StorageJob)> {
        let (tx, rx) = mpsc::channel(1);
        // Using a small buffer to avoid buffering stale jobs.

        let this = Arc::clone(self);
        // The above means that the Engine will stay in memory as long
        // as there is at least one stream. Should dropping the engine
        // instead invalidate- the streams?

        tokio::spawn(async move {
            if let Err(err) = this.build_jobs(tx).await {
                log::warn!("[{}] failed to collect jobs: {err}", this.arena)
            }
        });

        ReceiverStream::new(rx)
    }

    /// Report the result of processing the given job.
    pub fn job_finished(
        &self,
        job_id: JobId,
        status: anyhow::Result<JobStatus>,
    ) -> Result<(), StorageError> {
        match status {
            Ok(JobStatus::Done) => {
                log::debug!("[{}] DONE: {job_id}", self.arena);
                self.job_done(job_id)
            }
            Ok(JobStatus::Abandoned(reason)) => {
                log::debug!("[{}] ABANDONED: {job_id} ({reason})", self.arena);
                self.job_done(job_id)
            }
            Ok(JobStatus::Cancelled) => {
                log::debug!("[{}] CANCELLED: {job_id}", self.arena);
                self.job_failed(job_id)?;
                Ok(())
            }
            Ok(JobStatus::NoPeers) => {
                log::debug!("[{}] MISSING PEERS: {job_id}; will retry", self.arena);
                self.job_missing_peers(job_id)
            }
            Err(err) => match self.job_failed(job_id) {
                Ok(None) => {
                    log::warn!("[{}] FAILED; giving up: {job_id}: {err}", self.arena);

                    Ok(())
                }
                Ok(Some(backoff_time)) => {
                    log::debug!(
                        "[{}] FAILED; retry after {backoff_time:?}, in {}s: {job_id}: {err}",
                        self.arena,
                        backoff_time.duration_since(UnixTime::now()).as_secs()
                    );

                    Ok(())
                }
                Err(err) => {
                    log::warn!(
                        "[{}] FAILED; failed to reschedule: {job_id}: {err}",
                        self.arena
                    );

                    Err(err)
                }
            },
        }
    }

    /// Report that a job returned by a stream has been successfully executed.
    ///
    /// The dirty mark is cleared unless the path has been modified
    /// after the job was created.
    fn job_done(&self, job_id: JobId) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        self.dirty_paths.mark_job_done(&txn, job_id)?;
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
    ///
    /// Return the time after which the job will be retried or None if
    /// the job won't be retried.
    fn job_failed(&self, job_id: JobId) -> Result<Option<UnixTime>, StorageError> {
        let txn = self.db.begin_write()?;
        let backoff_until =
            self.dirty_paths
                .mark_job_failed(&txn, job_id, &self.job_retry_strategy)?;
        txn.commit()?;
        let _ = self.failed_job_tx.send(());

        Ok(backoff_until)
    }

    /// Tell the engine to retry job missing peers.
    ///
    /// This should be called after a new peer has become available.
    pub(crate) fn retry_jobs_missing_peers(&self) {
        let _ = self.retry_jobs_missing_peers.send(());
    }

    /// Report that a job returned had no peers to connect to.
    ///
    /// As long as the corresponding path isn't modified again, the job will be returned by
    /// the job stream after another peer has connected.
    ///
    /// The job is abandoned if the corresponding path is modified
    /// again or according to the retry strategy says so.
    fn job_missing_peers(&self, job_id: JobId) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        self.dirty_paths.mark_job_missing_peers(&txn, job_id)?;
        txn.commit()?;

        Ok(())
    }

    async fn build_jobs(
        self: &Arc<Engine>,
        tx: mpsc::Sender<(JobId, StorageJob)>,
    ) -> anyhow::Result<()> {
        let mut start_counter = 1;
        let mut retry_lower_bound = None;
        let mut retry_jobs_missing_peers = self.retry_jobs_missing_peers.subscribe();

        loop {
            log::debug!("[{}] collecting jobs: {start_counter}..", self.arena);
            let (job, last_counter) = task::spawn_blocking({
                let this = Arc::clone(self);
                let start_counter = start_counter;
                let arena = self.arena;
                move || {
                    let (job, last_counter) = this.next_job(start_counter)?;
                    if let Some(last_counter) = last_counter
                        && last_counter > start_counter
                    {
                        log::debug!("[{arena}] delete [{start_counter}, {last_counter})");
                        this.delete_range(start_counter, last_counter)?;
                    }

                    Ok::<_, StorageError>((job, last_counter))
                }
            })
            .await??;
            let arena = self.arena;
            if let Some(c) = last_counter {
                start_counter = c + 1;
            }
            if let Some((job_id, job)) = job {
                if self.is_failed(job_id).await? {
                    // Skip it for now; it'll be handled after a delay.
                    continue;
                }
                log::debug!("[{arena}] job {job_id} {job:?}");
                if tx.send((job_id, job)).await.is_err() {
                    // Broken channel; normal end of this loop
                    return Ok(());
                }
            } else {
                // Wait for more dirty paths, then continue.
                // Now is also a good time to retry failed jobs whose backoff period has passed.
                log::debug!("[{arena}] waiting...");
                let mut watch = self.dirty_paths.subscribe();
                let mut jobs_to_retry = vec![];
                tokio::select!(
                    _ = tx.closed() => {
                        return Ok(());
                    }

                    ret = watch.wait_for(|v| *v >= start_counter) => {
                        log::debug!("[{arena}] done waiting for v >= {start_counter} / {ret:?}");

                        ret?;
                    }

                    Ok(Some((backoff_until, mut jobs))) = self.wait_until_next_backoff(retry_lower_bound) => {
                        log::debug!("[{arena}] jobs to retry: {jobs:?}");
                        jobs_to_retry.append(&mut jobs);

                        // From now on, only retry jobs whose backoff
                        // period ends after backoff_until. This
                        // avoids retrying the same failed jobs within
                        // a single stream, relying on newly written
                        // backoffs always being in the future.
                        retry_lower_bound = Some(backoff_until);
                    }

                    Ok(mut jobs) = self.jobs_to_retry_missing_peers(&mut retry_jobs_missing_peers) => {
                        log::debug!("[{arena}] {} jobs to retry after peer connected", jobs.len());

                        jobs_to_retry.append(&mut jobs);
                    }
                );
                for counter in jobs_to_retry {
                    let job = self.build_job_with_counter(counter).await.ok().flatten();
                    log::debug!("[{arena}] retry {job:?}");
                    if let Some(job) = job {
                        if tx.send(job).await.is_err() {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    fn next_job(
        &self,
        mut start_counter: u64,
    ) -> Result<(Option<(JobId, StorageJob)>, Option<u64>), StorageError> {
        let txn = self.db.begin_read()?;

        while let Some((path, counter)) = self.dirty_paths.next_dirty_path(&txn, start_counter)? {
            if let Some(job) = self.build_job(&txn, path, counter)? {
                return Ok((Some(job), Some(counter)));
            }
            start_counter = counter + 1;
        }

        // Get the last counter for cleanup purposes
        let last_counter = self.dirty_paths.get_last_counter(&txn, start_counter)?;
        Ok((None, last_counter))
    }

    /// Lookup the path of given a counter and build a job, if possible.
    async fn build_job_with_counter(
        self: &Arc<Self>,
        counter: u64,
    ) -> Result<Option<(JobId, StorageJob)>, StorageError> {
        let this = Arc::clone(self);
        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            if let Some(path) = this.dirty_paths.get_path_for_counter(&txn, counter)? {
                return this.build_job(&txn, path, counter);
            }
            return Ok::<_, StorageError>(None);
        })
        .await?
    }

    /// Build a Job for the given path, if any.
    pub(crate) async fn job_for_path<T>(
        self: &Arc<Self>,
        path: T,
    ) -> Result<Option<(JobId, StorageJob)>, StorageError>
    where
        T: AsRef<Path>,
    {
        let this = Arc::clone(self);
        let path = path.as_ref().clone();
        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            if let Some(counter) = this.dirty_paths.get_counter_for_path(&txn, &path)? {
                return this.build_job(&txn, path, counter);
            }
            return Ok::<_, StorageError>(None);
        })
        .await?
    }

    /// Build a job from a path, if possible.
    fn build_job(
        &self,
        txn: &ArenaReadTransaction,
        path: Path,
        counter: u64,
    ) -> Result<Option<(JobId, StorageJob)>, StorageError> {
        match self.marks.get_mark_txn(txn, &path) {
            Err(_) => {
                return Ok(None);
            }
            Ok(Mark::Watch) => {
                if let (Ok(cached), Ok(Some(indexed))) = (
                    self.cache.get_file_entry_for_path(txn, &path),
                    self.get_indexed_file(txn, &path),
                ) && cached.hash == indexed.hash
                {
                    return Ok(Some((
                        JobId(counter),
                        StorageJob::Unrealize(path, cached.hash),
                    )));
                }
            }
            Ok(Mark::Keep) => {
                if let Ok(cached) = self.cache.get_file_entry_for_path(txn, &path) {
                    if let Ok(Some(indexed)) = self.get_indexed_file(txn, &path)
                        && cached.hash == indexed.hash
                    {
                        return Ok(Some((
                            JobId(counter),
                            StorageJob::Unrealize(path, cached.hash),
                        )));
                    }

                    if blob::local_availability(txn, &cached)? != LocalAvailability::Verified {
                        return Ok(Some((
                            JobId(counter),
                            StorageJob::External(Job::Download(path, cached.hash)),
                        )));
                    }
                }
            }
            Ok(Mark::Own) => {
                // TODO: treat is as Keep if there is no index.
                if let Ok(cached) = self.cache.get_file_entry_for_path(txn, &path) {
                    let from_index = self.get_indexed_file(txn, &path)?;
                    if from_index.is_none() {
                        // File is missing from the index, move it there.
                        return Ok(Some((
                            JobId(counter),
                            StorageJob::External(Job::Realize(path, cached.hash, None)),
                        )));
                    }
                    if let Some(indexed) = from_index
                        && indexed
                            .outdated_by
                            .as_ref()
                            .map(|h| *h == cached.hash)
                            .unwrap_or(false)
                    {
                        return Ok(Some((
                            JobId(counter),
                            StorageJob::External(Job::Realize(
                                path,
                                cached.hash,
                                Some(indexed.hash),
                            )),
                        )));
                    }
                }
            }
        };

        Ok(None)
    }

    fn get_indexed_file(
        &self,
        txn: &ArenaReadTransaction,
        path: &Path,
    ) -> Result<Option<super::types::IndexedFileTableEntry>, StorageError> {
        self.index
            .as_ref()
            .map(|i| i.get_file_txn(txn, &path))
            .unwrap_or(Ok(None))
    }

    fn delete_range(&self, beg: u64, end: u64) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        self.dirty_paths.delete_range(&txn, beg, end)?;
        txn.commit()?;
        Ok(())
    }

    /// Check whether this is a failed job.
    async fn is_failed(self: &Arc<Self>, job_id: JobId) -> Result<bool, StorageError> {
        let this = Arc::clone(self);
        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            this.dirty_paths.is_job_failed(&txn, job_id)
        })
        .await?
    }

    /// Wait until some failed jobs are ready to be retried.
    ///
    /// Returns the ID of the jobs to retry as well as the backoff
    /// time they were assigned.
    async fn wait_until_next_backoff(
        self: &Arc<Self>,
        lower_bound: Option<UnixTime>,
    ) -> Result<Option<(UnixTime, Vec<u64>)>, StorageError> {
        let mut rx = self.failed_job_tx.subscribe();
        loop {
            let this = Arc::clone(self);
            let current = task::spawn_blocking(move || {
                let txn = this.db.begin_read()?;
                this.dirty_paths.get_earliest_backoff(&txn, lower_bound)
            })
            .await??;

            if let Some((backoff, jobs)) = current {
                tokio::select!(
                    _ = time::sleep(backoff.duration_since(UnixTime::now())) => {
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

    /// Look for jobs to retry whenever a new peer connects and return
    /// them.
    ///
    /// Doesn't return an empty set of jobs if a peer connects and
    /// there's nothing to retry; it instead goes back to sleep.
    async fn jobs_to_retry_missing_peers(
        self: &Arc<Self>,
        rx: &mut broadcast::Receiver<()>,
    ) -> Result<Vec<u64>, StorageError> {
        loop {
            let _ = rx.recv().await;
            let this = Arc::clone(self);
            let jobs = task::spawn_blocking(move || {
                let txn = this.db.begin_read()?;
                this.dirty_paths.get_jobs_waiting_for_peers(&txn)
            })
            .await??;
            if !jobs.is_empty() {
                return Ok(jobs);
            }
        }
    }
}

fn last_counter(
    dirty_counter_table: &impl redb::ReadableTable<(), u64>,
) -> Result<u64, StorageError> {
    Ok(dirty_counter_table.get(())?.map(|v| v.value()).unwrap_or(0))
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::GlobalDatabase;
    use crate::InodeAllocator;
    use crate::Notification;
    use crate::arena::arena_cache::ArenaCache;
    use crate::arena::index::RealIndex;
    use crate::arena::mark::PathMarks;
    use crate::arena::tree::Tree;
    use crate::utils::redb_utils;
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use futures::StreamExt as _;
    use realize_types::{Arena, Peer, UnixTime};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt as _;
    use tokio::time;

    fn test_hash() -> Hash {
        Hash([1; 32])
    }

    /// Fixture for testing DirtyPaths.
    struct DirtyPathsFixture {
        db: Arc<ArenaDatabase>,
        dirty_paths: Arc<DirtyPaths>,
    }
    impl DirtyPathsFixture {
        async fn setup() -> anyhow::Result<DirtyPathsFixture> {
            let _ = env_logger::try_init();
            let arena = Arena::from("myarena");
            let db = ArenaDatabase::for_testing_single_arena(arena)?;
            let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;
            Ok(Self { db, dirty_paths })
        }
    }

    /// Fixture for testing Engine.
    struct EngineFixture {
        arena: Arena,
        db: Arc<ArenaDatabase>,
        dirty_paths: Arc<DirtyPaths>,
        acache: Arc<ArenaCache>,
        index: Arc<dyn RealIndex>,
        marks: Arc<dyn PathMarks>,
        engine: Arc<Engine>,
        arena_path: PathBuf,
        _tempdir: TempDir,
    }
    impl EngineFixture {
        async fn setup() -> anyhow::Result<EngineFixture> {
            let _ = env_logger::try_init();

            let tempdir = TempDir::new()?;
            let arena_path = tempdir.child("arena");
            arena_path.create_dir_all()?;

            let arena = Arena::from("myarena");
            let tree = Tree::new(
                arena,
                InodeAllocator::new(GlobalDatabase::new(redb_utils::in_memory()?)?, [arena])?,
            )?;
            let arena_root = tree.root();
            let db = ArenaDatabase::new(redb_utils::in_memory()?, tree)?;
            let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;
            let acache = ArenaCache::new(
                arena,
                arena_root,
                Arc::clone(&db),
                &tempdir.path().join("blobs"),
                Arc::clone(&dirty_paths),
            )?;
            let arena_root = acache.arena_root();
            let index = acache.as_index();
            let marks = acache.clone() as Arc<dyn PathMarks>;
            let engine = Engine::new(
                arena,
                Arc::clone(&db),
                Some(index.clone()),
                Arc::clone(&acache),
                Arc::clone(&marks),
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
                index,
                marks,
                engine,
                arena_path: arena_path.to_path_buf(),
                _tempdir: tempdir,
            })
        }

        /// Add a file to the cache for testing
        fn add_file_to_cache<T>(&self, path: T) -> anyhow::Result<()>
        where
            T: AsRef<Path>,
        {
            self.add_file_to_cache_with_version(path, test_hash())
        }

        fn add_file_to_cache_with_version<T>(&self, path: T, hash: Hash) -> anyhow::Result<()>
        where
            T: AsRef<Path>,
        {
            let path = path.as_ref();
            self.update_cache(Notification::Add {
                arena: self.arena,
                index: 1,
                path: path.clone(),
                mtime: UnixTime::from_secs(1234567890),
                size: 4,
                hash,
            })
        }

        fn remove_file_from_cache<T>(&self, path: T) -> anyhow::Result<()>
        where
            T: AsRef<Path>,
        {
            let path = path.as_ref();
            self.update_cache(Notification::Remove {
                arena: self.arena,
                index: 1,
                path: path.clone(),
                old_hash: test_hash(),
            })
        }

        // A new version comes in that replaces the version in the
        // cache/index.
        fn replace_in_cache_and_index<T>(
            &self,
            path: T,
            hash: Hash,
            old_hash: Hash,
        ) -> anyhow::Result<()>
        where
            T: AsRef<Path>,
        {
            let path = path.as_ref();
            let notification = Notification::Replace {
                arena: self.arena,
                index: 1,
                path: path.clone(),
                mtime: UnixTime::from_secs(1234567890),
                size: 4,
                hash,
                old_hash,
            };
            self.update_index(&notification)?;
            self.update_cache(notification)?;

            Ok(())
        }

        fn add_file_to_index_with_version<T>(&self, path: T, hash: Hash) -> anyhow::Result<()>
        where
            T: AsRef<Path>,
        {
            let path = path.as_ref();
            Ok(self
                .index
                .add_file(path, 100, UnixTime::from_secs(1234567889), hash)?)
        }

        fn update_cache(&self, notification: Notification) -> anyhow::Result<()> {
            let test_peer = Peer::from("other");
            self.acache.update(test_peer, notification, None)?;
            Ok(())
        }

        fn update_index(&self, notification: &Notification) -> anyhow::Result<()> {
            self.index.update(notification, &self.arena_path)?;

            Ok(())
        }

        fn lowest_counter(&self) -> anyhow::Result<Option<u64>> {
            let txn = self.db.begin_read()?;
            Ok(self.dirty_paths.get_lowest_counter(&txn)?)
        }

        fn mark_dirty<T>(&self, path: T) -> anyhow::Result<()>
        where
            T: AsRef<Path>,
        {
            let txn = self.db.begin_write()?;
            self.dirty_paths.mark_dirty(&txn, path)?;
            txn.commit()?;
            Ok(())
        }
    }

    async fn next_with_timeout<T>(stream: &mut ReceiverStream<T>) -> anyhow::Result<Option<T>> {
        let job = time::timeout(Duration::from_secs(3), stream.next()).await?;

        Ok(job)
    }

    #[tokio::test]
    async fn mark_and_take() -> anyhow::Result<()> {
        let fixture = DirtyPathsFixture::setup().await?;
        let txn = fixture.db.begin_write()?;
        let path1 = Path::parse("path1")?;
        let path2 = Path::parse("path2")?;

        fixture.dirty_paths.mark_dirty(&txn, &path1)?;
        fixture.dirty_paths.mark_dirty(&txn, &path2)?;

        assert_eq!(
            Some((path1, 1)),
            fixture.dirty_paths.take_next_dirty_path(&txn)?
        );
        assert_eq!(
            Some((path2, 2)),
            fixture.dirty_paths.take_next_dirty_path(&txn)?
        );
        assert_eq!(None, fixture.dirty_paths.take_next_dirty_path(&txn)?);
        assert_eq!(None, fixture.dirty_paths.take_next_dirty_path(&txn)?);

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
        assert_eq!(
            Some((path2, 2)),
            fixture.dirty_paths.take_next_dirty_path(&txn)?
        );
        assert_eq!(
            Some((path1, 4)),
            fixture.dirty_paths.take_next_dirty_path(&txn)?
        );
        assert_eq!(None, fixture.dirty_paths.take_next_dirty_path(&txn)?);
        assert_eq!(None, fixture.dirty_paths.take_next_dirty_path(&txn)?);

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
        assert_eq!(false, fixture.dirty_paths.is_dirty(&txn, &path1)?);
        assert_eq!(true, fixture.dirty_paths.is_dirty(&txn, &path2)?);
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
        fixture.dirty_paths.clear_dirty_for_path(&txn, &path1)?;

        assert_eq!(
            Some((path2, 2)),
            fixture.dirty_paths.take_next_dirty_path(&txn)?
        );
        assert_eq!(None, fixture.dirty_paths.take_next_dirty_path(&txn)?);

        txn.commit()?;
        Ok(())
    }

    #[tokio::test]
    async fn skip_invalid_path() -> anyhow::Result<()> {
        let fixture = DirtyPathsFixture::setup().await?;
        let txn = fixture.db.begin_write()?;
        txn.dirty_table()?.insert("///", 1)?;
        txn.dirty_counter_table()?.insert((), 1)?;
        txn.dirty_log_table()?.insert(1, "///")?;
        let path = Path::parse("path")?;

        fixture.dirty_paths.mark_dirty(&txn, &path)?;

        assert_eq!(
            Some((path, 2)),
            fixture.dirty_paths.take_next_dirty_path(&txn)?
        );
        assert_eq!(None, fixture.dirty_paths.take_next_dirty_path(&txn)?);

        txn.commit()?;
        Ok(())
    }

    #[tokio::test]
    async fn job_stream_new_file_to_keep() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let barfile = Path::parse("foo/bar.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.marks.set_arena_mark(Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;

        let job = next_with_timeout(&mut job_stream).await?;
        assert_eq!(
            Some((
                JobId(1),
                StorageJob::External(Job::Download(barfile, test_hash()))
            )),
            job
        );

        Ok(())
    }

    #[tokio::test]
    async fn multiple_job_streams() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let barfile = Path::parse("foo/bar.txt")?;

        fixture.marks.set_arena_mark(Mark::Keep)?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.add_file_to_cache(&barfile)?;
        let mut job_stream2 = fixture.engine.job_stream();

        let goal_job = Some((
            JobId(1),
            StorageJob::External(Job::Download(barfile, test_hash())),
        ));
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
        fixture.marks.set_mark(&foodir, Mark::Keep)?;

        let job = next_with_timeout(&mut job_stream).await?;
        // Counter is 2, because the job was created after the 2nd
        // change only.
        assert_eq!(
            Some((
                JobId(2),
                StorageJob::External(Job::Download(barfile, test_hash()))
            )),
            job
        );

        // The entries before the job that have been deleted.
        assert_eq!(Some(2), fixture.lowest_counter()?);

        Ok(())
    }

    #[tokio::test]
    async fn job_stream_file_to_keep_partially_downloaded() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let barfile = Path::parse("foo/bar.txt")?;

        fixture.marks.set_arena_mark(Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;
        let inode = fixture.acache.expect(&barfile)?;
        {
            let mut blob = fixture.acache.open_file(inode)?;
            blob.write_all(b"te").await?;
            blob.update_db().await?;
        }

        // The stream is created after downloading to be sure it sees
        // the download.
        let mut job_stream = fixture.engine.job_stream();

        let job = next_with_timeout(&mut job_stream).await?;
        assert_eq!(
            Some((
                JobId(1),
                StorageJob::External(Job::Download(barfile, test_hash()))
            )),
            job
        );

        Ok(())
    }

    #[tokio::test]
    async fn job_stream_file_to_keep_fully_downloaded() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let barfile = Path::parse("foo/bar.txt")?;

        fixture.marks.set_arena_mark(Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;
        let inode = fixture.acache.expect(&barfile)?;
        {
            let mut blob = fixture.acache.open_file(inode)?;
            blob.write_all(b"test").await?;
            blob.update_db().await?;
        }

        // The stream is created after downloading to be sure it sees
        // the download.
        let mut job_stream = fixture.engine.job_stream();

        let job = next_with_timeout(&mut job_stream).await?;
        assert_eq!(
            Some((
                JobId(1),
                StorageJob::External(Job::Download(barfile, test_hash()))
            )),
            job
        );

        Ok(())
    }

    #[tokio::test]
    async fn job_stream_file_to_keep_verified() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;

        fixture.marks.set_arena_mark(Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;
        let inode = fixture.acache.expect(&barfile)?;
        {
            let mut blob = fixture.acache.open_file(inode)?;
            blob.write_all(b"test").await?;
            blob.mark_verified().await?;
        }

        // The stream is created after downloading to be sure it sees
        // the download.
        let mut job_stream = fixture.engine.job_stream();

        // Add something else for the stream to return, or it'll just
        // hang forever.
        fixture.add_file_to_cache(&otherfile)?;

        let job = next_with_timeout(&mut job_stream).await?;
        // 1 has been skipped, because it is fully downloaded
        assert_eq!(
            Some((
                JobId(2),
                StorageJob::External(Job::Download(otherfile, test_hash()))
            )),
            job
        );

        // The entries before the job that have been deleted.
        assert_eq!(Some(2), fixture.lowest_counter()?);

        Ok(())
    }

    #[tokio::test]
    async fn job_stream_file_to_keep_already_removed() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;

        fixture.marks.set_arena_mark(Mark::Keep)?;
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
        assert_eq!(
            (
                JobId(3),
                StorageJob::External(Job::Download(otherfile, test_hash()))
            ),
            job
        );

        // The entries before the job that have been deleted.
        assert_eq!(Some(3), fixture.lowest_counter()?);

        Ok(())
    }

    #[tokio::test]
    async fn job_stream_file_to_own_not_in_index() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foobar = Path::parse("foo/bar")?;

        fixture.marks.set_arena_mark(Mark::Own)?;
        fixture.add_file_to_cache(&foobar)?;

        let mut job_stream = fixture.engine.job_stream();

        let job = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(
            (
                JobId(1),
                StorageJob::External(Job::Realize(foobar, test_hash(), None))
            ),
            job
        );

        Ok(())
    }

    #[tokio::test]
    async fn job_stream_file_to_own_in_index_with_same_version() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foobar = Path::parse("foo/bar")?;
        let otherfile = Path::parse("other")?;

        fixture.marks.set_arena_mark(Mark::Own)?;
        fixture.add_file_to_cache_with_version(&foobar, Hash([1; 32]))?;
        fixture.add_file_to_index_with_version(&foobar, Hash([1; 32]))?;

        // Add something else for the stream to return, or it'll just
        // hang forever.
        fixture.add_file_to_cache(&otherfile)?;

        let mut job_stream = fixture.engine.job_stream();

        let job = next_with_timeout(&mut job_stream).await?.unwrap();
        // 1 has been skipped, since the file is in the index
        assert_eq!(JobId(3), job.0);

        Ok(())
    }

    #[tokio::test]
    async fn job_stream_file_to_own_in_index_with_different_version() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foobar = Path::parse("foo/bar")?;
        let otherfile = Path::parse("other")?;

        fixture.marks.set_arena_mark(Mark::Own)?;
        fixture.add_file_to_cache_with_version(&foobar, Hash([1; 32]))?;
        fixture.add_file_to_index_with_version(&foobar, Hash([2; 32]))?;

        // Add something else for the stream to return, or it'll just
        // hang forever.
        fixture.add_file_to_cache(&otherfile)?;

        let mut job_stream = fixture.engine.job_stream();

        let job = next_with_timeout(&mut job_stream).await?.unwrap();
        // 1 has been skipped, since the file is in the index
        assert_eq!(JobId(3), job.0);

        Ok(())
    }

    #[tokio::test]
    async fn job_stream_file_to_own_in_index_with_outdated_version() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foobar = Path::parse("foo/bar")?;

        fixture.marks.set_arena_mark(Mark::Own)?;

        // Cache and index have the same version
        fixture.add_file_to_cache_with_version(&foobar, Hash([1; 32]))?;
        fixture.add_file_to_index_with_version(&foobar, Hash([1; 32]))?;
        fixture.replace_in_cache_and_index(&foobar, Hash([2; 32]), Hash([1; 32]))?;

        let mut job_stream = fixture.engine.job_stream();

        let job = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(
            (
                JobId(3),
                StorageJob::External(Job::Realize(foobar, Hash([2; 32]), Some(Hash([1; 32]))))
            ),
            job
        );

        Ok(())
    }

    #[tokio::test]
    async fn job_done() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.marks.set_arena_mark(Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;
        fixture.add_file_to_cache(&otherfile)?;

        let (job_id, _) = next_with_timeout(&mut job_stream).await?.unwrap();
        fixture.engine.job_finished(job_id, Ok(JobStatus::Done))?;

        // Any future stream won't return this job (even though the
        // job isn't really done, since the file wasn't downloaded; this isn't checked).
        let mut job_stream = fixture.engine.job_stream();
        assert_eq!(
            Some((
                JobId(2),
                StorageJob::External(Job::Download(otherfile, test_hash()))
            )),
            next_with_timeout(&mut job_stream).await?
        );

        // The dirty log entry that correspond to the job has been deleted.
        assert_eq!(Some(2), fixture.lowest_counter()?);

        Ok(())
    }

    #[tokio::test]
    async fn job_abandoned() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.marks.set_arena_mark(Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;
        fixture.add_file_to_cache(&otherfile)?;

        let (job_id, _) = next_with_timeout(&mut job_stream).await?.unwrap();
        fixture
            .engine
            .job_finished(job_id, Ok(JobStatus::Abandoned("test")))?;

        // This is handled the same as job_done. The only difference,
        // for now, is the logging.
        let mut job_stream = fixture.engine.job_stream();
        assert_eq!(
            Some((
                JobId(2),
                StorageJob::External(Job::Download(otherfile, test_hash()))
            )),
            next_with_timeout(&mut job_stream).await?
        );

        assert_eq!(Some(2), fixture.lowest_counter()?);

        Ok(())
    }

    #[tokio::test]
    async fn job_done_but_file_modified_again() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let barfile = Path::parse("foo/bar.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.marks.set_arena_mark(Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;

        let (job1_id, job1) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(barfile, *job1.path());
        assert_eq!(1, job1_id.as_u64());
        fixture.mark_dirty(&barfile)?;

        let (job2_id, job2) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(barfile, *job2.path());
        assert_eq!(2, job2_id.as_u64());

        // Job1 is done, but that changes nothing, because the path has been modified afterwards
        fixture.engine.job_finished(job1_id, Ok(JobStatus::Done))?;
        assert_eq!(Some(2), fixture.lowest_counter()?);

        // Since the job wasn't really done, job2 is still returned by the next stream.
        assert_eq!(
            Some(2),
            next_with_timeout(&mut fixture.engine.job_stream())
                .await?
                .map(|(id, _)| id.as_u64()),
        );

        // Marking job2 done erases the dirty mark.
        fixture.engine.job_finished(job2_id, Ok(JobStatus::Done))?;
        assert_eq!(None, fixture.lowest_counter()?);

        Ok(())
    }

    #[tokio::test]
    async fn job_failed() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.marks.set_arena_mark(Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;
        fixture.add_file_to_cache(&otherfile)?;

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(barfile, *job.path());
        fixture
            .engine
            .job_finished(job_id, Err(anyhow::anyhow!("fake")))?;

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
        let (_, job) = job_stream.next().await.unwrap();
        assert_eq!(otherfile, *job.path());

        // 2nd attempt
        let (job_id, job) = job_stream.next().await.unwrap();
        assert_eq!(barfile, *job.path());
        fixture
            .engine
            .job_finished(job_id, Err(anyhow::anyhow!("fake")))?;

        // 3rd and last attempt
        let (job_id, job) = job_stream.next().await.unwrap();
        assert_eq!(barfile, *job.path());
        fixture
            .engine
            .job_finished(job_id, Err(anyhow::anyhow!("fake")))?;

        // barfile is gone, otherfile is left
        assert_eq!(Some(2), fixture.lowest_counter()?);

        Ok(())
    }

    #[tokio::test]
    async fn job_missing_peers_should_be_retried() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.marks.set_arena_mark(Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;
        fixture.add_file_to_cache(&otherfile)?;

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(barfile, *job.path());
        fixture.engine.job_missing_peers(job_id)?;

        // barfile is still there
        assert_eq!(Some(1), fixture.lowest_counter()?);

        let (_, job) = job_stream.next().await.unwrap();
        assert_eq!(otherfile, *job.path());

        fixture.engine.retry_jobs_missing_peers();

        let (retry_job_id, retry_job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(barfile, *retry_job.path());
        assert_eq!(job_id, retry_job_id);

        Ok(())
    }

    #[tokio::test]
    async fn new_stream_delays_failed_job() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.marks.set_arena_mark(Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;
        fixture.add_file_to_cache(&otherfile)?;

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(barfile, *job.path());
        fixture
            .engine
            .job_finished(job_id, Err(anyhow::anyhow!("fake")))?;

        // New stream starts with otherfile, because barfile is delayed.
        // This is what happens after a restart as well.
        let mut job_stream = fixture.engine.job_stream();

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(otherfile, *job.path());
        fixture
            .engine
            .job_finished(job_id, Err(anyhow::anyhow!("fake")))?;

        tokio::time::pause();

        // 2nd attempt
        let (job_id, job) = job_stream.next().await.unwrap();
        assert_eq!(barfile, *job.path());
        fixture
            .engine
            .job_finished(job_id, Err(anyhow::anyhow!("fake")))?;

        Ok(())
    }

    #[tokio::test]
    async fn job_failed_then_path_modified() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.marks.set_arena_mark(Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;
        fixture.add_file_to_cache(&otherfile)?;

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(barfile, *job.path());
        fixture
            .engine
            .job_finished(job_id, Err(anyhow::anyhow!("fake")))?;
        assert_eq!(Some(1), fixture.lowest_counter()?);

        // Modifying the path gets rid of the failed job.
        fixture.mark_dirty(&barfile)?;
        assert_eq!(Some(2), fixture.lowest_counter()?);

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(otherfile, *job.path());
        assert_eq!(2, job_id.as_u64());

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(barfile, *job.path());
        assert_eq!(3, job_id.as_u64());

        Ok(())
    }

    #[tokio::test]
    async fn job_failed_after_path_modified() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.marks.set_arena_mark(Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;
        fixture.add_file_to_cache(&otherfile)?;

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(barfile, *job.path());
        fixture.mark_dirty(&barfile)?;
        fixture
            .engine
            .job_finished(job_id, Err(anyhow::anyhow!("fake")))?; // This does nothing

        assert_eq!(Some(2), fixture.lowest_counter()?);

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(otherfile, *job.path());
        assert_eq!(2, job_id.as_u64());

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(barfile, *job.path());
        assert_eq!(3, job_id.as_u64());

        Ok(())
    }

    #[tokio::test]
    async fn check_job_returns_same() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let barfile = Path::parse("foo/bar.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.marks.set_arena_mark(Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        let same_job = fixture.engine.job_for_path(job.path()).await?;
        assert_eq!(Some((job_id, job)), same_job);

        Ok(())
    }

    #[tokio::test]
    async fn check_job_returns_updated() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let barfile = Path::parse("foo/bar.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.marks.set_arena_mark(Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(1, job_id.as_u64());
        fixture.mark_dirty(&barfile)?;
        let (new_job_id, new_job) = fixture.engine.job_for_path(job.path()).await?.unwrap();
        assert_eq!(job, new_job);
        assert!(new_job_id > job_id);

        Ok(())
    }

    async fn check_job_returns_outdated_because_done() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let barfile = Path::parse("foo/bar.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.marks.set_arena_mark(Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;

        let (_, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        let inode = fixture.acache.expect(&barfile)?;
        {
            let mut blob = fixture.acache.open_file(inode)?;
            blob.write_all(b"test").await?;
            blob.update_db().await?;
        }
        assert_eq!(None, fixture.engine.job_for_path(job.path()).await?);

        Ok(())
    }

    async fn check_job_returns_outdated_because_irrelevant() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let barfile = Path::parse("foo/bar.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.marks.set_arena_mark(Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;

        let (_, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        fixture.remove_file_from_cache(&barfile)?;
        assert_eq!(None, fixture.engine.job_for_path(job.path()).await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_mark_job_done() -> anyhow::Result<()> {
        let fixture = DirtyPathsFixture::setup().await?;
        let path = Path::parse("test/path.txt")?;
        let job_id = JobId::from(1);

        // Mark path as dirty first
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.mark_dirty(&txn, &path)?;
            txn.commit()?;
        }

        // Verify it's dirty
        {
            let txn = fixture.db.begin_read()?;
            assert!(fixture.dirty_paths.is_dirty(&txn, &path)?);
        }

        // Mark job as done
        {
            let txn = fixture.db.begin_write()?;
            let was_dirty = fixture.dirty_paths.mark_job_done(&txn, job_id)?;
            assert!(was_dirty);
            txn.commit()?;
        }

        // Verify path is no longer dirty
        {
            let txn = fixture.db.begin_read()?;
            assert!(!fixture.dirty_paths.is_dirty(&txn, &path)?);
        }

        // Mark job as done again (should return false)
        {
            let txn = fixture.db.begin_write()?;
            let was_dirty = fixture.dirty_paths.mark_job_done(&txn, job_id)?;
            assert!(!was_dirty);
            txn.commit()?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_mark_job_failed() -> anyhow::Result<()> {
        let fixture = DirtyPathsFixture::setup().await?;
        let path = Path::parse("test/path.txt")?;
        let job_id = JobId::from(1);

        // Mark path as dirty first
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.mark_dirty(&txn, &path)?;
            txn.commit()?;
        }

        // Mark job as failed with retry strategy
        {
            let txn = fixture.db.begin_write()?;
            let retry_strategy = |attempt: u32| {
                if attempt < 3 {
                    Some(Duration::from_secs(attempt as u64 * 10))
                } else {
                    None
                }
            };

            let retry_after = fixture
                .dirty_paths
                .mark_job_failed(&txn, job_id, &retry_strategy)?;
            assert!(retry_after.is_some());
            txn.commit()?;
        }

        // Verify job is marked as failed
        {
            let txn = fixture.db.begin_read()?;
            assert!(fixture.dirty_paths.is_job_failed(&txn, job_id)?);
        }

        // Get retryable jobs (should be empty since we're before retry time)
        {
            let txn = fixture.db.begin_read()?;
            let retryable = fixture
                .dirty_paths
                .get_retryable_jobs(&txn, UnixTime::from_secs(0))?;
            assert!(retryable.is_empty());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_mark_job_missing_peers() -> anyhow::Result<()> {
        let fixture = DirtyPathsFixture::setup().await?;
        let path = Path::parse("test/path.txt")?;
        let job_id = JobId::from(1);

        // First mark a path as dirty to create the job entry
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.mark_dirty(&txn, &path)?;
            txn.commit()?;
        }

        // Mark job as missing peers
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.mark_job_missing_peers(&txn, job_id)?;
            txn.commit()?;
        }

        // Verify job is waiting for peers
        let txn = fixture.db.begin_read()?;
        let waiting_jobs = fixture.dirty_paths.get_jobs_waiting_for_peers(&txn)?;
        assert_eq!(waiting_jobs, vec![1]);

        Ok(())
    }

    #[tokio::test]
    async fn test_next_dirty_path() -> anyhow::Result<()> {
        let fixture = DirtyPathsFixture::setup().await?;
        let path1 = Path::parse("path1.txt")?;
        let path2 = Path::parse("path2.txt")?;

        // Mark multiple paths as dirty
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.mark_dirty(&txn, &path1)?;
            fixture.dirty_paths.mark_dirty(&txn, &path2)?;
            txn.commit()?;
        }

        // Get next dirty path starting from counter 0
        let txn = fixture.db.begin_read()?;
        let mut start_counter = 0;
        let result = fixture.dirty_paths.next_dirty_path(&txn, start_counter)?;
        assert_eq!(result, Some((path1.clone(), 1)));
        start_counter = 2; // Start from counter 2 to get the next entry

        // Get next dirty path
        let result = fixture.dirty_paths.next_dirty_path(&txn, start_counter)?;
        assert_eq!(result, Some((path2.clone(), 2)));
        start_counter = 3; // Start from counter 3 to get the next entry

        // No more dirty paths
        let result = fixture.dirty_paths.next_dirty_path(&txn, start_counter)?;
        assert_eq!(result, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_path_for_counter() -> anyhow::Result<()> {
        let fixture = DirtyPathsFixture::setup().await?;
        let path = Path::parse("test/path.txt")?;

        // Mark path as dirty
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.mark_dirty(&txn, &path)?;
            txn.commit()?;
        }

        // Get path for counter 1
        let txn = fixture.db.begin_read()?;
        let result = fixture.dirty_paths.get_path_for_counter(&txn, 1)?;
        assert_eq!(result, Some(path.clone()));

        // Get path for non-existent counter
        let result = fixture.dirty_paths.get_path_for_counter(&txn, 999)?;
        assert_eq!(result, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_counter_for_path() -> anyhow::Result<()> {
        let fixture = DirtyPathsFixture::setup().await?;
        let path = Path::parse("test/path.txt")?;

        // Mark path as dirty
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.mark_dirty(&txn, &path)?;
            txn.commit()?;
        }

        // Get counter for path
        let txn = fixture.db.begin_read()?;
        let result = fixture.dirty_paths.get_counter_for_path(&txn, &path)?;
        assert_eq!(result, Some(1));

        // Get counter for non-existent path
        let non_existent_path = Path::parse("non/existent.txt")?;
        let result = fixture
            .dirty_paths
            .get_counter_for_path(&txn, &non_existent_path)?;
        assert_eq!(result, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_is_job_failed() -> anyhow::Result<()> {
        let fixture = DirtyPathsFixture::setup().await?;
        let path = Path::parse("test/path.txt")?;
        let job_id = JobId::from(1);

        // First mark a path as dirty to create the job entry
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.mark_dirty(&txn, &path)?;
            txn.commit()?;
        }

        // Job should not be failed initially
        {
            let txn = fixture.db.begin_read()?;
            assert!(!fixture.dirty_paths.is_job_failed(&txn, job_id)?);
        }

        // Mark job as failed
        {
            let txn = fixture.db.begin_write()?;
            let retry_strategy = |_| Some(Duration::from_secs(10));
            fixture
                .dirty_paths
                .mark_job_failed(&txn, job_id, &retry_strategy)?;
            txn.commit()?;
        }

        // Job should now be failed
        let txn = fixture.db.begin_read()?;
        assert!(fixture.dirty_paths.is_job_failed(&txn, job_id)?);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_retryable_jobs() -> anyhow::Result<()> {
        let fixture = DirtyPathsFixture::setup().await?;
        let path1 = Path::parse("path1.txt")?;
        let path2 = Path::parse("path2.txt")?;
        let job_id1 = JobId::from(1);
        let job_id2 = JobId::from(2);

        // First mark paths as dirty to create the job entries
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.mark_dirty(&txn, &path1)?;
            fixture.dirty_paths.mark_dirty(&txn, &path2)?;
            txn.commit()?;
        }

        // Mark jobs as failed with different retry times
        {
            let txn = fixture.db.begin_write()?;
            let retry_strategy = |attempt: u32| {
                if attempt < 3 {
                    Some(Duration::from_secs(attempt as u64 * 10))
                } else {
                    None
                }
            };

            fixture
                .dirty_paths
                .mark_job_failed(&txn, job_id1, &retry_strategy)?;
            fixture
                .dirty_paths
                .mark_job_failed(&txn, job_id2, &retry_strategy)?;
            txn.commit()?;
        }

        // Get retryable jobs before retry time (should be empty)
        let txn = fixture.db.begin_read()?;
        let retryable = fixture
            .dirty_paths
            .get_retryable_jobs(&txn, UnixTime::from_secs(0))?;
        assert!(retryable.is_empty());

        // Get retryable jobs after retry time
        let retryable = fixture
            .dirty_paths
            .get_retryable_jobs(&txn, UnixTime::now().plus(Duration::from_secs(100)))?;
        assert_eq!(retryable, vec![1, 2]);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_earliest_backoff() -> anyhow::Result<()> {
        let fixture = DirtyPathsFixture::setup().await?;
        let path1 = Path::parse("path1.txt")?;
        let path2 = Path::parse("path2.txt")?;
        let job_id1 = JobId::from(1);
        let job_id2 = JobId::from(2);

        // First mark paths as dirty to create the job entries
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.mark_dirty(&txn, &path1)?;
            fixture.dirty_paths.mark_dirty(&txn, &path2)?;
            txn.commit()?;
        }

        // Mark jobs as failed with different retry times
        {
            let txn = fixture.db.begin_write()?;
            let retry_strategy = |attempt: u32| {
                if attempt < 3 {
                    Some(Duration::from_secs(attempt as u64 * 10))
                } else {
                    None
                }
            };

            fixture
                .dirty_paths
                .mark_job_failed(&txn, job_id1, &retry_strategy)?;
            fixture
                .dirty_paths
                .mark_job_failed(&txn, job_id2, &retry_strategy)?;
            txn.commit()?;
        }

        // Get earliest backoff time
        let txn = fixture.db.begin_read()?;
        let result = fixture.dirty_paths.get_earliest_backoff(&txn, None)?;
        assert!(result.is_some());
        let (earliest_time, job_ids) = result.unwrap();
        assert_eq!(job_ids, vec![1, 2]);
        assert!(earliest_time > UnixTime::from_secs(0));

        Ok(())
    }

    #[tokio::test]
    async fn test_get_jobs_waiting_for_peers() -> anyhow::Result<()> {
        let fixture = DirtyPathsFixture::setup().await?;
        let path1 = Path::parse("path1.txt")?;
        let path2 = Path::parse("path2.txt")?;
        let job_id1 = JobId::from(1);
        let job_id2 = JobId::from(2);

        // First mark paths as dirty to create the job entries
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.mark_dirty(&txn, &path1)?;
            fixture.dirty_paths.mark_dirty(&txn, &path2)?;
            txn.commit()?;
        }

        // Mark jobs as waiting for peers
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.mark_job_missing_peers(&txn, job_id1)?;
            fixture.dirty_paths.mark_job_missing_peers(&txn, job_id2)?;
            txn.commit()?;
        }

        // Get jobs waiting for peers
        let txn = fixture.db.begin_read()?;
        let waiting_jobs = fixture.dirty_paths.get_jobs_waiting_for_peers(&txn)?;
        assert_eq!(waiting_jobs, vec![1, 2]);

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_range() -> anyhow::Result<()> {
        let fixture = DirtyPathsFixture::setup().await?;
        let path1 = Path::parse("path1.txt")?;
        let path2 = Path::parse("path2.txt")?;
        let path3 = Path::parse("path3.txt")?;

        // Mark multiple paths as dirty
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.mark_dirty(&txn, &path1)?;
            fixture.dirty_paths.mark_dirty(&txn, &path2)?;
            fixture.dirty_paths.mark_dirty(&txn, &path3)?;
            txn.commit()?;
        }

        // Delete range 1-2 (counters 1 and 2)
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.delete_range(&txn, 1, 3)?;
            txn.commit()?;
        }

        // Verify path1 and path2 are gone, path3 remains
        {
            let txn = fixture.db.begin_read()?;
            assert!(!fixture.dirty_paths.is_dirty(&txn, &path1)?);
            assert!(!fixture.dirty_paths.is_dirty(&txn, &path2)?);
            assert!(fixture.dirty_paths.is_dirty(&txn, &path3)?);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_clear_dirty_for_path() -> anyhow::Result<()> {
        let fixture = DirtyPathsFixture::setup().await?;
        let path = Path::parse("test/path.txt")?;

        // Mark path as dirty
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.mark_dirty(&txn, &path)?;
            txn.commit()?;
        }

        // Verify it's dirty
        {
            let txn = fixture.db.begin_read()?;
            assert!(fixture.dirty_paths.is_dirty(&txn, &path)?);
        }

        // Clear dirty for path
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.clear_dirty_for_path(&txn, &path)?;
            txn.commit()?;
        }

        // Verify it's no longer dirty
        let txn = fixture.db.begin_read()?;
        assert!(!fixture.dirty_paths.is_dirty(&txn, &path)?);

        Ok(())
    }

    #[tokio::test]
    async fn test_take_next_dirty_path() -> anyhow::Result<()> {
        let fixture = DirtyPathsFixture::setup().await?;
        let path1 = Path::parse("path1.txt")?;
        let path2 = Path::parse("path2.txt")?;

        // Mark multiple paths as dirty
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.mark_dirty(&txn, &path1)?;
            fixture.dirty_paths.mark_dirty(&txn, &path2)?;
            txn.commit()?;
        }

        // Take next dirty path
        let txn = fixture.db.begin_write()?;
        let result = fixture.dirty_paths.take_next_dirty_path(&txn)?;
        assert_eq!(result, Some((path1.clone(), 1)));

        // Verify path1 is no longer dirty by checking the dirty table directly
        {
            let dirty_table = txn.dirty_table()?;
            assert!(dirty_table.get(path1.as_str())?.is_none());
        }

        // Take next dirty path
        let result = fixture.dirty_paths.take_next_dirty_path(&txn)?;
        assert_eq!(result, Some((path2.clone(), 2)));

        // No more dirty paths
        let result = fixture.dirty_paths.take_next_dirty_path(&txn)?;
        assert_eq!(result, None);

        txn.commit()?;
        Ok(())
    }

    #[tokio::test]
    async fn test_get_lowest_counter() -> anyhow::Result<()> {
        let fixture = DirtyPathsFixture::setup().await?;
        let path1 = Path::parse("path1.txt")?;
        let path2 = Path::parse("path2.txt")?;

        // Initially no counters
        {
            let txn = fixture.db.begin_read()?;
            let lowest = fixture.dirty_paths.get_lowest_counter(&txn)?;
            assert_eq!(lowest, None);
        }

        // Mark paths as dirty
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.mark_dirty(&txn, &path1)?;
            fixture.dirty_paths.mark_dirty(&txn, &path2)?;
            txn.commit()?;
        }

        // Get lowest counter
        {
            let txn = fixture.db.begin_read()?;
            let lowest = fixture.dirty_paths.get_lowest_counter(&txn)?;
            assert_eq!(lowest, Some(1));
        }

        // Take first path
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.take_next_dirty_path(&txn)?;
            txn.commit()?;
        }

        // Get lowest counter again
        let txn = fixture.db.begin_read()?;
        let lowest = fixture.dirty_paths.get_lowest_counter(&txn)?;
        assert_eq!(lowest, Some(2));

        Ok(())
    }

    #[tokio::test]
    async fn test_get_last_counter() -> anyhow::Result<()> {
        let fixture = DirtyPathsFixture::setup().await?;
        let path1 = Path::parse("path1.txt")?;
        let path2 = Path::parse("path2.txt")?;

        // Initially no counters
        {
            let txn = fixture.db.begin_read()?;
            let last = fixture.dirty_paths.get_last_counter(&txn, 0)?;
            assert_eq!(last, None);
        }

        // Mark paths as dirty
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.mark_dirty(&txn, &path1)?;
            fixture.dirty_paths.mark_dirty(&txn, &path2)?;
            txn.commit()?;
        }

        // Get last counter
        let txn = fixture.db.begin_read()?;
        let last = fixture.dirty_paths.get_last_counter(&txn, 0)?;
        assert_eq!(last, Some(2));

        Ok(())
    }

    #[tokio::test]
    async fn test_is_dirty() -> anyhow::Result<()> {
        let fixture = DirtyPathsFixture::setup().await?;
        let path = Path::parse("test/path.txt")?;

        // Initially not dirty
        {
            let txn = fixture.db.begin_read()?;
            assert!(!fixture.dirty_paths.is_dirty(&txn, &path)?);
        }

        // Mark as dirty
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.mark_dirty(&txn, &path)?;
            txn.commit()?;
        }

        // Verify it's dirty
        {
            let txn = fixture.db.begin_read()?;
            assert!(fixture.dirty_paths.is_dirty(&txn, &path)?);
        }

        // Clear dirty
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.clear_dirty_for_path(&txn, &path)?;
            txn.commit()?;
        }

        // Verify it's no longer dirty
        let txn = fixture.db.begin_read()?;
        assert!(!fixture.dirty_paths.is_dirty(&txn, &path)?);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_dirty() -> anyhow::Result<()> {
        let fixture = DirtyPathsFixture::setup().await?;
        let path = Path::parse("test/path.txt")?;

        // Initially no dirty counter
        {
            let txn = fixture.db.begin_read()?;
            let dirty_counter = fixture.dirty_paths.get_dirty(&txn, &path)?;
            assert_eq!(dirty_counter, None);
        }

        // Mark as dirty
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.mark_dirty(&txn, &path)?;
            txn.commit()?;
        }

        // Verify dirty counter
        {
            let txn = fixture.db.begin_read()?;
            let dirty_counter = fixture.dirty_paths.get_dirty(&txn, &path)?;
            assert_eq!(dirty_counter, Some(1));
        }

        // Clear dirty
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.clear_dirty_for_path(&txn, &path)?;
            txn.commit()?;
        }

        // Verify no dirty counter
        let txn = fixture.db.begin_read()?;
        let dirty_counter = fixture.dirty_paths.get_dirty(&txn, &path)?;
        assert_eq!(dirty_counter, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_job_lifecycle() -> anyhow::Result<()> {
        let fixture = DirtyPathsFixture::setup().await?;
        let path = Path::parse("test/path.txt")?;
        let job_id = JobId::from(1);

        // 1. Mark path as dirty
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.mark_dirty(&txn, &path)?;
            txn.commit()?;
        }

        // Verify it's dirty
        {
            let txn = fixture.db.begin_read()?;
            assert!(fixture.dirty_paths.is_dirty(&txn, &path)?);
        }

        // 2. Get counter for path
        {
            let txn = fixture.db.begin_read()?;
            let counter = fixture.dirty_paths.get_counter_for_path(&txn, &path)?;
            assert_eq!(counter, Some(1));
        }

        // 3. Mark job as failed
        {
            let txn = fixture.db.begin_write()?;
            let retry_strategy = |_| Some(Duration::from_secs(10));
            fixture
                .dirty_paths
                .mark_job_failed(&txn, job_id, &retry_strategy)?;
            txn.commit()?;
        }

        // Verify job is failed
        {
            let txn = fixture.db.begin_read()?;
            assert!(fixture.dirty_paths.is_job_failed(&txn, job_id)?);
        }

        // 4. Mark job as done
        {
            let txn = fixture.db.begin_write()?;
            let was_dirty = fixture.dirty_paths.mark_job_done(&txn, job_id)?;
            assert!(was_dirty);
            txn.commit()?;
        }

        // Verify final state
        let txn = fixture.db.begin_read()?;
        assert!(!fixture.dirty_paths.is_dirty(&txn, &path)?);
        assert!(!fixture.dirty_paths.is_job_failed(&txn, job_id)?);

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_jobs_same_path() -> anyhow::Result<()> {
        let fixture = DirtyPathsFixture::setup().await?;
        let path1 = Path::parse("test/path1.txt")?;
        let path2 = Path::parse("test/path2.txt")?;
        let job_id1 = JobId::from(1);
        let job_id2 = JobId::from(2);

        // Mark different paths as dirty
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.mark_dirty(&txn, &path1)?;
            fixture.dirty_paths.mark_dirty(&txn, &path2)?;
            txn.commit()?;
        }

        // Get counters (should be 1 and 2)
        {
            let txn = fixture.db.begin_read()?;
            let counter1 = fixture.dirty_paths.get_counter_for_path(&txn, &path1)?;
            let counter2 = fixture.dirty_paths.get_counter_for_path(&txn, &path2)?;
            assert_eq!(counter1, Some(1));
            assert_eq!(counter2, Some(2));
        }

        // Mark both jobs as failed
        {
            let txn = fixture.db.begin_write()?;
            let retry_strategy = |_| Some(Duration::from_secs(10));
            fixture
                .dirty_paths
                .mark_job_failed(&txn, job_id1, &retry_strategy)?;
            fixture
                .dirty_paths
                .mark_job_failed(&txn, job_id2, &retry_strategy)?;
            txn.commit()?;
        }

        // Both should be failed
        {
            let txn = fixture.db.begin_read()?;
            assert!(fixture.dirty_paths.is_job_failed(&txn, job_id1)?);
            assert!(fixture.dirty_paths.is_job_failed(&txn, job_id2)?);
        }

        // Mark one as done
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.mark_job_done(&txn, job_id1)?;
            txn.commit()?;
        }

        // Verify final state
        let txn = fixture.db.begin_read()?;
        assert!(!fixture.dirty_paths.is_job_failed(&txn, job_id1)?);
        assert!(fixture.dirty_paths.is_job_failed(&txn, job_id2)?);

        Ok(())
    }
}
