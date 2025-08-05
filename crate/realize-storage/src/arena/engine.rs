#![allow(dead_code)] // work in progress

use super::db::{ArenaDatabase, ArenaReadTransaction, ArenaWriteTransaction};
use super::index;
use super::types::{FailedJobTableEntry, LocalAvailability, RetryJob};
use crate::arena::arena_cache;
use crate::arena::blob;
use crate::arena::mark;
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

    /// Move file containing the given version into the cache.
    Unrealize(Path, Hash),
}

impl Job {
    /// Return the path the job is for.
    pub fn path(&self) -> &Path {
        match self {
            Job::Download(path, _) => path,
            Job::Realize(path, _, _) => path,
            Job::Unrealize(path, _) => path,
        }
    }

    /// Return the hash the job is for.
    pub fn hash(&self) -> &Hash {
        match self {
            Job::Download(_, h) => h,
            Job::Realize(_, h, _) => h,
            Job::Unrealize(_, h) => h,
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
        let last_counter = task::spawn_blocking(move || {
            let counter: u64;

            let txn = db.begin_write()?;
            {
                let dirty_counter_table = txn.dirty_counter_table()?;
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
        txn: &ArenaWriteTransaction,
        path: &Path,
    ) -> Result<(), StorageError> {
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
        dirty_paths: Arc<DirtyPaths>,
        arena_root: Inode,
        job_retry_strategy: impl (Fn(u32) -> Option<Duration>) + Sync + Send + 'static,
    ) -> Arc<Engine> {
        let (failed_job_tx, _) = watch::channel(());
        let (retry_jobs_missing_peers, _) = broadcast::channel(8);
        Arc::new(Self {
            arena,
            db,
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
    pub fn job_stream(self: &Arc<Engine>) -> ReceiverStream<(JobId, Job)> {
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
        if let Some(removed) = txn.dirty_log_table()?.remove(job_id.as_u64())? {
            txn.failed_job_table()?.remove(job_id.as_u64())?;
            txn.dirty_table()?.remove(removed.value())?;
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
    ///
    /// Return the time after which the job will be retried or None if
    /// the job won't be retried.
    fn job_failed(&self, job_id: JobId) -> Result<Option<UnixTime>, StorageError> {
        let txn = self.db.begin_write()?;
        let backoff_until = {
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

            match (self.job_retry_strategy)(failure_count) {
                None => {
                    dirty_log_table.remove(counter)?;
                    failed_job_table.remove(counter)?;
                    txn.dirty_table()?.remove(path.as_str())?;

                    None
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

                    Some(backoff_until)
                }
            }
        };
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
        {
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
        }
        txn.commit()?;

        Ok(())
    }

    async fn build_jobs(self: &Arc<Engine>, tx: mpsc::Sender<(JobId, Job)>) -> anyhow::Result<()> {
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
        start_counter: u64,
    ) -> Result<(Option<(JobId, Job)>, Option<u64>), StorageError> {
        let txn = self.db.begin_read()?;
        let dirty_log_table = txn.dirty_log_table()?;
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
    ) -> Result<Option<(JobId, Job)>, StorageError> {
        let this = Arc::clone(self);
        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            let dirty_log_table = txn.dirty_log_table()?;
            if let Some(v) = dirty_log_table.get(counter)? {
                drop(dirty_log_table);

                let path = Path::parse(v.value())?;
                return this.build_job(&txn, path, counter);
            }
            return Ok::<_, StorageError>(None);
        })
        .await?
    }

    /// Build a Job for the given path, if any.
    pub(crate) async fn job_for_path(
        self: &Arc<Self>,
        path: &Path,
    ) -> Result<Option<(JobId, Job)>, StorageError> {
        let this = Arc::clone(self);
        let path = path.clone();
        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            let dirty_table = txn.dirty_table()?;
            if let Some(v) = dirty_table.get(path.as_str())? {
                drop(dirty_table);

                let counter = v.value();
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
    ) -> Result<Option<(JobId, Job)>, StorageError> {
        match mark::get_mark(txn, &path)? {
            Mark::Watch => {
                if let (Ok(cached), Ok(Some(indexed))) = (
                    arena_cache::get_file_entry_for_path(txn, self.arena_root, &path),
                    index::get_file_entry(txn, &path),
                ) && cached.content.hash == indexed.hash
                {
                    return Ok(Some((
                        JobId(counter),
                        Job::Unrealize(path, cached.content.hash),
                    )));
                }
            }
            Mark::Keep => {
                if let Ok(cached) =
                    arena_cache::get_file_entry_for_path(txn, self.arena_root, &path)
                {
                    if let Ok(Some(indexed)) = index::get_file_entry(txn, &path)
                        && cached.content.hash == indexed.hash
                    {
                        return Ok(Some((
                            JobId(counter),
                            Job::Unrealize(path, cached.content.hash),
                        )));
                    }

                    if blob::local_availability(txn, &cached)? != LocalAvailability::Verified {
                        return Ok(Some((
                            JobId(counter),
                            Job::Download(path, cached.content.hash),
                        )));
                    }
                }
            }
            Mark::Own => {
                // TODO: treat is as Keep if there is no index.
                if let Ok(cached) =
                    arena_cache::get_file_entry_for_path(txn, self.arena_root, &path)
                {
                    let from_index = index::get_file_entry(txn, &path).ok().flatten();
                    if from_index.is_none() {
                        // File is missing from the index, move it there.
                        return Ok(Some((
                            JobId(counter),
                            Job::Realize(path, cached.content.hash, None),
                        )));
                    }
                    if let Some(indexed) = from_index
                        && indexed
                            .outdated_by
                            .as_ref()
                            .map(|h| *h == cached.content.hash)
                            .unwrap_or(false)
                    {
                        return Ok(Some((
                            JobId(counter),
                            Job::Realize(path, cached.content.hash, Some(indexed.hash)),
                        )));
                    }
                }
            }
        };

        Ok(None)
    }

    fn delete_range(&self, beg: u64, end: u64) -> Result<(), StorageError> {
        if !(beg < end) {
            return Ok(());
        }
        let txn = self.db.begin_write()?;
        {
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
        }
        txn.commit()?;
        Ok(())
    }

    /// Check whether this is a failed job.
    async fn is_failed(self: &Arc<Self>, job_id: JobId) -> Result<bool, StorageError> {
        let this = Arc::clone(self);
        let counter = job_id.as_u64();
        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            let failed_job_table = txn.failed_job_table()?;

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
        lower_bound: Option<UnixTime>,
    ) -> Result<Option<(UnixTime, Vec<u64>)>, StorageError> {
        let mut rx = self.failed_job_tx.subscribe();
        loop {
            let this = Arc::clone(self);
            let current = task::spawn_blocking(move || {
                let txn = this.db.begin_read()?;
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
                Ok::<_, StorageError>(if earliest_backoff < UnixTime::MAX {
                    Some((earliest_backoff, counters))
                } else {
                    None
                })
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
                Ok::<_, StorageError>(jobs)
            })
            .await??;
            if !jobs.is_empty() {
                return Ok(jobs);
            }
        }
    }
}

fn current_path_counter(
    txn: &ArenaWriteTransaction,
    path: &Path,
) -> Result<Option<u64>, StorageError> {
    let current = txn.dirty_table()?.get(path.as_str())?.map(|e| e.value());
    Ok(current)
}

fn last_counter(
    dirty_counter_table: &impl redb::ReadableTable<(), u64>,
) -> Result<u64, StorageError> {
    Ok(dirty_counter_table.get(())?.map(|v| v.value()).unwrap_or(0))
}

/// Check the dirty mark on a path.
pub(crate) fn is_dirty(txn: &ArenaReadTransaction, path: &Path) -> Result<bool, StorageError> {
    let dirty_table = txn.dirty_table()?;

    Ok(dirty_table.get(path.as_str())?.is_some())
}

/// Check whether a path is dirty and if it is return its dirty mark counter.
pub(crate) fn get_dirty(
    txn: &ArenaReadTransaction,
    path: &Path,
) -> Result<Option<u64>, StorageError> {
    let dirty_table = txn.dirty_table()?;

    let entry = dirty_table.get(path.as_str())?;

    Ok(entry.map(|e| e.value()))
}

/// Clear the dirty mark on a path.
///
/// This does nothing if the path has no dirty mark.
pub(crate) fn clear_dirty(txn: &ArenaWriteTransaction, path: &Path) -> Result<(), StorageError> {
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

/// Get a dirty path from the table for processing.
///
/// The returned path is removed from the dirty table when the
/// transaction is committed.
pub(crate) fn take_dirty(txn: &ArenaWriteTransaction) -> Result<Option<(Path, u64)>, StorageError> {
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
#[cfg(test)]
mod tests {
    use super::*;
    use crate::GlobalDatabase;
    use crate::InodeAllocator;
    use crate::Notification;
    use crate::arena::arena_cache::ArenaCache;
    use crate::arena::index::RealIndexBlocking;
    use crate::arena::mark::PathMarks;
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
            let db = ArenaDatabase::new(redb_utils::in_memory()?)?;
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
        index: RealIndexBlocking,
        pathmarks: PathMarks,
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

            let db = ArenaDatabase::new(redb_utils::in_memory()?)?;
            let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;

            let arena = Arena::from("myarena");
            let allocator =
                InodeAllocator::new(GlobalDatabase::new(redb_utils::in_memory()?)?, [arena])?;
            let acache = ArenaCache::new(
                arena,
                allocator,
                Arc::clone(&db),
                &tempdir.path().join("blobs"),
                Arc::clone(&dirty_paths),
            )?;
            let arena_root = acache.arena_root();
            let index = RealIndexBlocking::new(arena, Arc::clone(&db), Arc::clone(&dirty_paths))?;
            let pathmarks = PathMarks::new(Arc::clone(&db), arena_root, Arc::clone(&dirty_paths))?;
            let engine = Engine::new(
                arena,
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
                index,
                engine,
                pathmarks,
                arena_path: arena_path.to_path_buf(),
                _tempdir: tempdir,
            })
        }

        /// Add a file to the cache for testing
        fn add_file_to_cache(&self, path: &Path) -> anyhow::Result<()> {
            self.add_file_to_cache_with_version(path, test_hash())
        }

        fn add_file_to_cache_with_version(&self, path: &Path, hash: Hash) -> anyhow::Result<()> {
            self.update_cache(Notification::Add {
                arena: self.arena,
                index: 1,
                path: path.clone(),
                mtime: UnixTime::from_secs(1234567890),
                size: 4,
                hash,
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

        // A new version comes in that replaces the version in the
        // cache/index.
        fn replace_in_cache_and_index(
            &self,
            path: &Path,
            hash: Hash,
            old_hash: Hash,
        ) -> anyhow::Result<()> {
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

        fn add_file_to_index_with_version(&self, path: &Path, hash: Hash) -> anyhow::Result<()> {
            Ok(self
                .index
                .add_file(path, 100, UnixTime::from_secs(1234567889), hash)?)
        }

        fn update_cache(&self, notification: Notification) -> anyhow::Result<()> {
            let test_peer = Peer::from("other");
            self.acache.update(test_peer, notification)?;
            Ok(())
        }

        fn update_index(&self, notification: &Notification) -> anyhow::Result<()> {
            self.index.update(notification, &self.arena_path)?;

            Ok(())
        }

        fn lowest_counter(&self) -> anyhow::Result<Option<u64>> {
            let txn = self.db.begin_read()?;
            let dirty_log_table = txn.dirty_log_table()?;

            Ok(dirty_log_table.first()?.map(|(k, _)| k.value()))
        }

        fn mark_dirty(&self, path: &Path) -> anyhow::Result<()> {
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
        txn.dirty_table()?.insert("///", 1)?;
        txn.dirty_counter_table()?.insert((), 1)?;
        txn.dirty_log_table()?.insert(1, "///")?;
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
        assert_eq!(Some((JobId(1), Job::Download(barfile, test_hash()))), job);

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

        let goal_job = Some((JobId(1), Job::Download(barfile, test_hash())));
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
        assert_eq!(Some((JobId(2), Job::Download(barfile, test_hash()))), job);

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
        assert_eq!(Some((JobId(1), Job::Download(barfile, test_hash()))), job);

        Ok(())
    }

    #[tokio::test]
    async fn job_stream_file_to_keep_fully_downloaded() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foodir = Path::parse("foo")?;
        let barfile = Path::parse("foo/bar.txt")?;

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

        let job = next_with_timeout(&mut job_stream).await?;
        assert_eq!(Some((JobId(1), Job::Download(barfile, test_hash()))), job);

        Ok(())
    }

    #[tokio::test]
    async fn job_stream_file_to_keep_verified() -> anyhow::Result<()> {
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
        assert_eq!(Some((JobId(2), Job::Download(otherfile, test_hash()))), job);

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
        assert_eq!((JobId(3), Job::Download(otherfile, test_hash())), job);

        // The entries before the job that have been deleted.
        assert_eq!(Some(3), fixture.lowest_counter()?);

        Ok(())
    }

    #[tokio::test]
    async fn job_stream_file_to_own_not_in_index() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foobar = Path::parse("foo/bar")?;

        fixture.pathmarks.set_arena_mark(Mark::Own)?;
        fixture.add_file_to_cache(&foobar)?;

        let mut job_stream = fixture.engine.job_stream();

        let job = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!((JobId(1), Job::Realize(foobar, test_hash(), None)), job);

        Ok(())
    }

    #[tokio::test]
    async fn job_stream_file_to_own_in_index_with_same_version() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foobar = Path::parse("foo/bar")?;
        let otherfile = Path::parse("other")?;

        fixture.pathmarks.set_arena_mark(Mark::Own)?;
        fixture.add_file_to_cache_with_version(&foobar, Hash([1; 32]))?;
        fixture.add_file_to_index_with_version(&foobar, Hash([1; 32]))?;

        // Add something else for the stream to return, or it'll just
        // hang forever.
        fixture.pathmarks.set_mark(&otherfile, Mark::Keep)?;
        fixture.add_file_to_cache(&otherfile)?;

        let mut job_stream = fixture.engine.job_stream();

        let job = next_with_timeout(&mut job_stream).await?.unwrap();
        // 1 has been skipped, since the file is in the index
        assert_eq!((JobId(3), Job::Download(otherfile, test_hash())), job);

        Ok(())
    }

    #[tokio::test]
    async fn job_stream_file_to_own_in_index_with_different_version() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foobar = Path::parse("foo/bar")?;
        let otherfile = Path::parse("other")?;

        fixture.pathmarks.set_arena_mark(Mark::Own)?;
        fixture.add_file_to_cache_with_version(&foobar, Hash([1; 32]))?;
        fixture.add_file_to_index_with_version(&foobar, Hash([2; 32]))?;

        // Add something else for the stream to return, or it'll just
        // hang forever.
        fixture.pathmarks.set_mark(&otherfile, Mark::Keep)?;
        fixture.add_file_to_cache(&otherfile)?;

        let mut job_stream = fixture.engine.job_stream();

        let job = next_with_timeout(&mut job_stream).await?.unwrap();
        // 1 has been skipped, since the file is in the index
        assert_eq!((JobId(3), Job::Download(otherfile, test_hash())), job);

        Ok(())
    }

    #[tokio::test]
    async fn job_stream_file_to_own_in_index_with_outdated_version() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foobar = Path::parse("foo/bar")?;

        fixture.pathmarks.set_arena_mark(Mark::Own)?;

        // Cache and index have the same version
        fixture.add_file_to_cache_with_version(&foobar, Hash([1; 32]))?;
        fixture.add_file_to_index_with_version(&foobar, Hash([1; 32]))?;
        fixture.replace_in_cache_and_index(&foobar, Hash([2; 32]), Hash([1; 32]))?;

        let mut job_stream = fixture.engine.job_stream();

        let job = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(
            (
                JobId(3),
                Job::Realize(foobar, Hash([2; 32]), Some(Hash([1; 32])))
            ),
            job
        );

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

        let (job_id, _) = next_with_timeout(&mut job_stream).await?.unwrap();
        fixture.engine.job_finished(job_id, Ok(JobStatus::Done))?;

        // Any future stream won't return this job (even though the
        // job isn't really done, since the file wasn't downloaded; this isn't checked).
        let mut job_stream = fixture.engine.job_stream();
        assert_eq!(
            Some((JobId(2), Job::Download(otherfile, test_hash()))),
            next_with_timeout(&mut job_stream).await?
        );

        // The dirty log entry that correspond to the job has been deleted.
        assert_eq!(Some(2), fixture.lowest_counter()?);

        Ok(())
    }

    #[tokio::test]
    async fn job_abandoned() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foodir = Path::parse("foo")?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.pathmarks.set_mark(&foodir, Mark::Keep)?;
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
            Some((JobId(2), Job::Download(otherfile, test_hash()))),
            next_with_timeout(&mut job_stream).await?
        );

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
        let foodir = Path::parse("foo")?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.pathmarks.set_mark(&foodir, Mark::Keep)?;
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
        let foodir = Path::parse("foo")?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.pathmarks.set_mark(&foodir, Mark::Keep)?;
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
        let foodir = Path::parse("foo")?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.pathmarks.set_mark(&foodir, Mark::Keep)?;
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
        let foodir = Path::parse("foo")?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.pathmarks.set_mark(&foodir, Mark::Keep)?;
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
        let foodir = Path::parse("foo")?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.pathmarks.set_mark(&foodir, Mark::Keep)?;
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
        let foodir = Path::parse("foo")?;
        let barfile = Path::parse("foo/bar.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.pathmarks.set_mark(&foodir, Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        let same_job = fixture.engine.job_for_path(job.path()).await?;
        assert_eq!(Some((job_id, job)), same_job);

        Ok(())
    }

    #[tokio::test]
    async fn check_job_returns_updated() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foodir = Path::parse("foo")?;
        let barfile = Path::parse("foo/bar.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.pathmarks.set_mark(&foodir, Mark::Keep)?;
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
        let foodir = Path::parse("foo")?;
        let barfile = Path::parse("foo/bar.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.pathmarks.set_mark(&foodir, Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;

        let (_, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        let (inode, _) = fixture.acache.lookup_path(&barfile)?;
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
        let foodir = Path::parse("foo")?;
        let barfile = Path::parse("foo/bar.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        fixture.pathmarks.set_mark(&foodir, Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;

        let (_, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        fixture.remove_file_from_cache(&barfile)?;
        assert_eq!(None, fixture.engine.job_for_path(job.path()).await?);

        Ok(())
    }
}
