use super::arena_cache::ArenaCache;
use super::blob::BlobExt;
use super::db::{ArenaDatabase, ArenaReadTransaction};
use super::dirty::DirtyReadOperations;
use super::index::IndexReadOperations;
use super::mark::MarkExt;
use super::tree::{TreeExt, TreeLoc};
use super::types::LocalAvailability;
use crate::types::JobId;
use crate::{Inode, Mark, StorageError};
use realize_types::{Arena, Hash, Path, UnixTime};
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
}

impl Job {
    /// Return the path the job is for.
    pub fn path(&self) -> &Path {
        match self {
            Self::Download(path, _) => path,
        }
    }

    /// Return the hash the job is for.
    pub fn hash(&self) -> &Hash {
        match self {
            Self::Download(_, h) => h,
        }
    }
}

/// An extended representation of [Job] that includes jobs not exposed
/// outside of this crate.
#[derive(Eq, PartialEq, Debug, Clone)]
pub(crate) enum StorageJob {
    External(Job),

    /// Move file containing the given version into the cache.
    Unrealize(Inode, Hash),

    /// Realize the given file with `counter`, moving `Hash` from
    /// cache to the index, currently containing nothing or `Hash`.
    Realize(Inode, Hash, Option<Hash>),
}

impl StorageJob {
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

pub struct Engine {
    arena: Arena,
    db: Arc<ArenaDatabase>,
    cache: Arc<ArenaCache>,
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
        cache: Arc<ArenaCache>,

        job_retry_strategy: impl (Fn(u32) -> Option<Duration>) + Sync + Send + 'static,
    ) -> Arc<Engine> {
        let (failed_job_tx, _) = watch::channel(());
        let (retry_jobs_missing_peers, _) = broadcast::channel(8);
        Arc::new(Self {
            arena,
            db,
            cache,
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
        {
            let mut dirty = txn.write_dirty()?;
            dirty.mark_job_done(job_id)?;
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
            let mut dirty = txn.write_dirty()?;
            dirty.mark_job_failed(job_id, &self.job_retry_strategy)?
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
            let mut dirty = txn.write_dirty()?;
            dirty.mark_job_missing_peers(job_id)?;
        }
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
                let mut watch = self.db.dirty().subscribe();
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
        let dirty = txn.read_dirty()?;

        while let Some((path, counter)) = dirty.next_dirty(start_counter)? {
            if let Some(job) = self.build_job(&txn, path, counter)? {
                return Ok((Some(job), Some(counter)));
            }
            start_counter = counter + 1;
        }

        // Get the last counter for cleanup purposes
        let last_counter = dirty.get_last_counter(start_counter)?;
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
            let dirty = txn.read_dirty()?;
            if let Some(inode) = dirty.get_inode_for_counter(counter)? {
                return this.build_job(&txn, inode, counter);
            }
            return Ok::<_, StorageError>(None);
        })
        .await?
    }

    /// Build a Job for the given path, if any.
    pub(crate) async fn job_for_loc<'b, L: Into<TreeLoc<'b>>>(
        self: &Arc<Self>,
        loc: L,
    ) -> Result<Option<(JobId, StorageJob)>, StorageError> {
        let this = Arc::clone(self);
        let loc = loc.into().into_owned();
        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            let tree = txn.read_tree()?;
            let dirty = txn.read_dirty()?;
            if let Some(inode) = tree.resolve(loc)? {
                if let Some(counter) = dirty.get_counter(inode)? {
                    return this.build_job(&txn, inode, counter);
                }
            }
            return Ok::<_, StorageError>(None);
        })
        .await?
    }

    /// Build a job from a path, if possible.
    fn build_job(
        &self,
        txn: &ArenaReadTransaction,
        inode: Inode,
        counter: u64,
    ) -> Result<Option<(JobId, StorageJob)>, StorageError> {
        let tree = txn.read_tree()?;
        let marks = txn.read_marks()?;
        let index = txn.read_index()?;
        match marks.get(&tree, inode) {
            Err(_) => {
                return Ok(None);
            }
            Ok(Mark::Watch) => {
                if let (Ok(cached), Ok(Some(indexed))) = (
                    self.cache.get_file_entry_for_loc(txn, &tree, inode),
                    index.get_at_inode(inode),
                ) && cached.hash == indexed.hash
                {
                    return Ok(Some((
                        JobId(counter),
                        StorageJob::Unrealize(inode, cached.hash),
                    )));
                }
            }
            Ok(Mark::Keep) => {
                if let Ok(cached) = self.cache.get_file_entry_for_loc(txn, &tree, inode) {
                    if let Ok(Some(indexed)) = index.get_at_inode(inode)
                        && cached.hash == indexed.hash
                    {
                        return Ok(Some((
                            JobId(counter),
                            StorageJob::Unrealize(inode, cached.hash),
                        )));
                    }

                    let blobs = txn.read_blobs()?;
                    if blobs.local_availability(&tree, inode)? != LocalAvailability::Verified {
                        if let Ok(path) = tree.backtrack(inode) {
                            return Ok(Some((
                                JobId(counter),
                                StorageJob::External(Job::Download(path, cached.hash)),
                            )));
                        }
                    }
                }
            }
            Ok(Mark::Own) => {
                if let Ok(cached) = self.cache.get_file_entry_for_loc(txn, &tree, inode) {
                    // File is missing from the index or version in
                    // cache should overwrite the version in the
                    // inedx.
                    let indexed = index.get_at_inode(inode)?;
                    let should_move = match &indexed {
                        None => true,
                        Some(indexed) => {
                            indexed.hash != cached.hash && indexed.is_outdated_by(&cached.hash)
                        }
                    };
                    if should_move {
                        let blobs = txn.read_blobs()?;
                        if blobs.local_availability(&tree, inode)? == LocalAvailability::Verified {
                            return Ok(Some((
                                JobId(counter),
                                StorageJob::Realize(inode, cached.hash, indexed.map(|i| i.hash)),
                            )));
                        } else {
                            if let Ok(path) = tree.backtrack(inode) {
                                return Ok(Some((
                                    JobId(counter),
                                    StorageJob::External(Job::Download(path, cached.hash)),
                                )));
                            }
                        }
                    }
                }
            }
        };

        Ok(None)
    }

    fn delete_range(&self, beg: u64, end: u64) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut dirty = txn.write_dirty()?;
            dirty.delete_range(beg, end)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Check whether this is a failed job.
    async fn is_failed(self: &Arc<Self>, job_id: JobId) -> Result<bool, StorageError> {
        let this = Arc::clone(self);
        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            dirty.is_job_failed(job_id)
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
                let dirty = txn.read_dirty()?;
                dirty.get_earliest_backoff(lower_bound)
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
                let dirty = txn.read_dirty()?;
                dirty.get_jobs_waiting_for_peers()
            })
            .await??;
            if !jobs.is_empty() {
                return Ok(jobs);
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
    use crate::arena::index::RealIndex;
    use crate::arena::mark;
    use crate::arena::tree::TreeLoc;
    use crate::utils::redb_utils;
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use futures::StreamExt as _;
    use realize_types::{Arena, Peer, UnixTime};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt as _;
    use tokio::time;

    fn test_hash() -> Hash {
        Hash([1; 32])
    }

    /// Fixture for testing Engine.
    struct EngineFixture {
        arena: Arena,
        db: Arc<ArenaDatabase>,
        acache: Arc<ArenaCache>,
        index: Arc<dyn RealIndex>,
        engine: Arc<Engine>,
        _tempdir: TempDir,
    }
    impl EngineFixture {
        async fn setup() -> anyhow::Result<EngineFixture> {
            let _ = env_logger::try_init();

            let tempdir = TempDir::new()?;
            let arena_path = tempdir.child("arena");
            arena_path.create_dir_all()?;

            let arena = Arena::from("myarena");
            let blob_dir = tempdir.path().join("blobs");
            let db = ArenaDatabase::new(
                redb_utils::in_memory()?,
                arena,
                InodeAllocator::new(GlobalDatabase::new(redb_utils::in_memory()?)?, [arena])?,
                &blob_dir,
            )?;
            let acache = ArenaCache::new(arena, Arc::clone(&db), &blob_dir)?;
            let index = acache.as_index();
            let engine = Engine::new(arena, Arc::clone(&db), Arc::clone(&acache), |attempt| {
                if attempt < 3 {
                    Some(Duration::from_secs(attempt as u64 * 10))
                } else {
                    None
                }
            });

            Ok(Self {
                arena,
                db,
                acache,
                index,
                engine,
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

        fn lowest_counter(&self) -> anyhow::Result<Option<u64>> {
            let txn = self.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            Ok(dirty.next_dirty(0)?.map(|(_, c)| c))
        }

        fn mark_dirty<'b, L: Into<TreeLoc<'b>>>(&self, loc: L) -> anyhow::Result<()> {
            let txn = self.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                let tree = txn.read_tree()?;
                dirty.mark_dirty(tree.expect(loc)?, "test")?;
            }
            txn.commit()?;
            Ok(())
        }

        fn inode(&self, path: &Path) -> anyhow::Result<Inode> {
            let txn = self.db.begin_read()?;

            Ok(txn.read_tree()?.expect(path)?)
        }
    }

    async fn next_with_timeout<T>(stream: &mut ReceiverStream<T>) -> anyhow::Result<Option<T>> {
        let job = time::timeout(Duration::from_secs(3), stream.next()).await?;

        Ok(job)
    }

    #[tokio::test]
    async fn job_stream_new_file_to_keep() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let barfile = Path::parse("foo/bar.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        mark::set_arena_mark(&fixture.db, Mark::Keep)?;
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

        mark::set_arena_mark(&fixture.db, Mark::Keep)?;
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
        mark::set(&fixture.db, &foodir, Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;

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

        mark::set_arena_mark(&fixture.db, Mark::Keep)?;
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

        mark::set_arena_mark(&fixture.db, Mark::Keep)?;
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

        mark::set_arena_mark(&fixture.db, Mark::Keep)?;
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

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        // barfile has been skipped, because it is fully downloaded
        assert_eq!(
            job,
            StorageJob::External(Job::Download(otherfile, test_hash()))
        );

        // The entries before the job that have been deleted.
        assert_eq!(Some(job_id.as_u64()), fixture.lowest_counter()?);

        Ok(())
    }

    #[tokio::test]
    async fn job_stream_file_to_keep_already_removed() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;

        mark::set_arena_mark(&fixture.db, Mark::Keep)?;
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
    async fn download_then_realize_file_to_own_not_in_index() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foobar = Path::parse("foo/bar")?;

        mark::set_arena_mark(&fixture.db, Mark::Own)?;
        fixture.add_file_to_cache(&foobar)?;
        let inode = fixture.acache.expect(&foobar)?;

        let mut job_stream = fixture.engine.job_stream();

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(
            StorageJob::External(Job::Download(foobar, test_hash())),
            job,
        );
        {
            let mut blob = fixture.acache.open_file(inode)?;
            blob.write_all(b"test").await?;
            blob.mark_verified().await?;
        }
        fixture.engine.job_finished(job_id, Ok(JobStatus::Done))?;

        let (new_job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert!(new_job_id > job_id);
        assert_eq!(StorageJob::Realize(inode, test_hash(), None), job);

        Ok(())
    }

    #[tokio::test]
    async fn realize_verified_file_to_own_not_in_index() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foobar = Path::parse("foo/bar")?;

        mark::set_arena_mark(&fixture.db, Mark::Own)?;
        fixture.add_file_to_cache(&foobar)?;
        let inode = fixture.acache.expect(&foobar)?;
        {
            let mut blob = fixture.acache.open_file(inode)?;
            blob.write_all(b"test").await?;
            blob.mark_verified().await?;
        }

        let mut job_stream = fixture.engine.job_stream();

        let (_, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(StorageJob::Realize(inode, test_hash(), None), job);

        Ok(())
    }

    #[tokio::test]
    async fn ignore_file_to_own_in_index_with_same_version() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foobar = Path::parse("foo/bar")?;
        let otherfile = Path::parse("other")?;

        mark::set_arena_mark(&fixture.db, Mark::Own)?;
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
    async fn ignore_file_to_own_in_index_with_different_version() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foobar = Path::parse("foo/bar")?;
        let otherfile = Path::parse("other")?;

        mark::set_arena_mark(&fixture.db, Mark::Own)?;
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
    async fn realize_file_to_own_in_index_with_outdated_version() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foobar = Path::parse("foo/bar")?;

        mark::set_arena_mark(&fixture.db, Mark::Own)?;

        // Cache and index have the same version
        fixture.add_file_to_cache_with_version(&foobar, Hash([1; 32]))?;
        fixture.add_file_to_index_with_version(&foobar, Hash([1; 32]))?;
        fixture.replace_in_cache_and_index(&foobar, Hash([2; 32]), Hash([1; 32]))?;
        let inode = fixture.acache.expect(&foobar)?;
        {
            let mut blob = fixture.acache.open_file(inode)?;
            blob.write_all(b"test").await?;
            blob.mark_verified().await?;
        }

        let mut job_stream = fixture.engine.job_stream();

        let job = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(
            (
                JobId(4),
                StorageJob::Realize(fixture.inode(&foobar)?, Hash([2; 32]), Some(Hash([1; 32])))
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
        mark::set_arena_mark(&fixture.db, Mark::Keep)?;
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
        mark::set_arena_mark(&fixture.db, Mark::Keep)?;
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
        mark::set_arena_mark(&fixture.db, Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;

        let (job1_id, job1) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(
            StorageJob::External(Job::Download(barfile.clone(), test_hash())),
            job1
        );
        assert_eq!(1, job1_id.as_u64());
        fixture.mark_dirty(&barfile)?;

        let (job2_id, job2) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(
            StorageJob::External(Job::Download(barfile.clone(), test_hash())),
            job2
        );
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
        mark::set_arena_mark(&fixture.db, Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;
        fixture.add_file_to_cache(&otherfile)?;

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(
            StorageJob::External(Job::Download(barfile.clone(), test_hash())),
            job
        );
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
        assert_eq!(
            StorageJob::External(Job::Download(otherfile.clone(), test_hash())),
            job
        );

        // 2nd attempt
        let (job_id, job) = job_stream.next().await.unwrap();
        assert_eq!(
            StorageJob::External(Job::Download(barfile.clone(), test_hash())),
            job
        );
        fixture
            .engine
            .job_finished(job_id, Err(anyhow::anyhow!("fake")))?;

        // 3rd and last attempt
        let (job_id, job) = job_stream.next().await.unwrap();
        assert_eq!(
            StorageJob::External(Job::Download(barfile.clone(), test_hash())),
            job
        );
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
        mark::set_arena_mark(&fixture.db, Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;
        fixture.add_file_to_cache(&otherfile)?;

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(
            StorageJob::External(Job::Download(barfile.clone(), test_hash())),
            job
        );
        fixture.engine.job_missing_peers(job_id)?;

        // barfile is still there
        assert_eq!(Some(1), fixture.lowest_counter()?);

        let (_, job) = job_stream.next().await.unwrap();
        assert_eq!(
            StorageJob::External(Job::Download(otherfile.clone(), test_hash())),
            job
        );
        fixture.engine.retry_jobs_missing_peers();

        let (retry_job_id, retry_job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(
            StorageJob::External(Job::Download(barfile.clone(), test_hash())),
            retry_job
        );
        assert_eq!(job_id, retry_job_id);

        Ok(())
    }

    #[tokio::test]
    async fn new_stream_delays_failed_job() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        mark::set_arena_mark(&fixture.db, Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;
        fixture.add_file_to_cache(&otherfile)?;

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(
            StorageJob::External(Job::Download(barfile.clone(), test_hash())),
            job
        );
        fixture
            .engine
            .job_finished(job_id, Err(anyhow::anyhow!("fake")))?;

        // New stream starts with otherfile, because barfile is delayed.
        // This is what happens after a restart as well.
        let mut job_stream = fixture.engine.job_stream();

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(
            StorageJob::External(Job::Download(otherfile.clone(), test_hash())),
            job
        );
        fixture
            .engine
            .job_finished(job_id, Err(anyhow::anyhow!("fake")))?;

        tokio::time::pause();

        // 2nd attempt
        let (job_id, job) = job_stream.next().await.unwrap();
        assert_eq!(
            StorageJob::External(Job::Download(barfile.clone(), test_hash())),
            job
        );

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
        mark::set_arena_mark(&fixture.db, Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;
        fixture.add_file_to_cache(&otherfile)?;

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(
            StorageJob::External(Job::Download(barfile.clone(), test_hash())),
            job
        );

        fixture
            .engine
            .job_finished(job_id, Err(anyhow::anyhow!("fake")))?;
        assert_eq!(Some(1), fixture.lowest_counter()?);

        // Modifying the path gets rid of the failed job.
        fixture.mark_dirty(&barfile)?;
        assert_eq!(Some(2), fixture.lowest_counter()?);

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(
            StorageJob::External(Job::Download(otherfile.clone(), test_hash())),
            job
        );

        assert_eq!(2, job_id.as_u64());

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(
            StorageJob::External(Job::Download(barfile.clone(), test_hash())),
            job
        );

        assert_eq!(3, job_id.as_u64());

        Ok(())
    }

    #[tokio::test]
    async fn job_failed_after_path_modified() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let barfile = Path::parse("foo/bar.txt")?;
        let otherfile = Path::parse("foo/other.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        mark::set_arena_mark(&fixture.db, Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;
        fixture.add_file_to_cache(&otherfile)?;

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(
            StorageJob::External(Job::Download(barfile.clone(), test_hash())),
            job
        );

        fixture.mark_dirty(&barfile)?;
        fixture
            .engine
            .job_finished(job_id, Err(anyhow::anyhow!("fake")))?; // This does nothing

        assert_eq!(Some(2), fixture.lowest_counter()?);

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(
            StorageJob::External(Job::Download(otherfile.clone(), test_hash())),
            job
        );

        assert_eq!(2, job_id.as_u64());

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(
            StorageJob::External(Job::Download(barfile.clone(), test_hash())),
            job
        );

        assert_eq!(3, job_id.as_u64());

        Ok(())
    }

    #[tokio::test]
    async fn check_job_returns_same() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let barfile = Path::parse("foo/bar.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        mark::set_arena_mark(&fixture.db, Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        let same_job = fixture.engine.job_for_loc(&barfile).await?;
        assert_eq!(Some((job_id, job)), same_job);

        Ok(())
    }

    #[tokio::test]
    async fn check_job_returns_updated() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let barfile = Path::parse("foo/bar.txt")?;
        let mut job_stream = fixture.engine.job_stream();
        mark::set_arena_mark(&fixture.db, Mark::Keep)?;
        fixture.add_file_to_cache(&barfile)?;

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        fixture.mark_dirty(&barfile)?;
        let (new_job_id, new_job) = fixture.engine.job_for_loc(&barfile).await?.unwrap();
        assert_eq!(job, new_job);
        assert!(new_job_id > job_id);

        Ok(())
    }
}
