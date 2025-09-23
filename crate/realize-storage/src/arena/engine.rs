use super::blob::BlobReadOperations;
use super::cache::CacheReadOperations;
use super::db::{ArenaDatabase, ArenaReadTransaction};
use super::dirty::DirtyReadOperations;
use super::mark::MarkExt;
use super::tree::{TreeExt, TreeLoc};
use super::types::CacheStatus;
use crate::arena::tree::TreeReadOperations;
use crate::types::JobId;
use crate::{Mark, PathId, StorageError};
use realize_types::{Hash, Path, UnixTime};
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

    /// Move indexed file containing the given version into the cache.
    ///
    /// If the cache already has a newer version, the indexed file is
    /// just dropped.
    Unrealize {
        pathid: PathId,
        indexed_hash: Hash,
    },

    /// Realize the given file with `counter`, moving `Hash` from
    /// cache to the index, currently containing nothing or `Hash`.
    Realize {
        pathid: PathId,
        hash: Hash,
    },

    /// Move the blob to the protected queue.
    ProtectBlob(PathId),

    /// Move the blob to the unprotected queue.
    UnprotectBlob(PathId),
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
    db: Arc<ArenaDatabase>,
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
        db: Arc<ArenaDatabase>,
        job_retry_strategy: impl (Fn(u32) -> Option<Duration>) + Sync + Send + 'static,
    ) -> Arc<Engine> {
        let (failed_job_tx, _) = watch::channel(());
        let (retry_jobs_missing_peers, _) = broadcast::channel(8);
        Arc::new(Self {
            db,
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
                log::warn!("[{}] Failed to collect jobs: {err}", this.db.tag())
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
                log::info!("[{}] Job #{job_id} Done", self.db.tag());
                self.job_done(job_id)
            }
            Ok(JobStatus::Abandoned(reason)) => {
                log::info!("[{}] Job #{job_id} Abandoned ({reason})", self.db.tag());
                self.job_done(job_id)
            }
            Ok(JobStatus::Cancelled) => {
                log::debug!("[{}] Job #{job_id} Cancelled", self.db.tag());
                self.job_failed(job_id)?;
                Ok(())
            }
            Ok(JobStatus::NoPeers) => {
                log::info!(
                    "[{}] Job #{job_id} Peers offline; will retry after connection",
                    self.db.tag()
                );
                self.job_missing_peers(job_id)
            }
            Err(err) => match self.job_failed(job_id) {
                Ok(None) => {
                    log::warn!("[{}] Job #{job_id} failed; giving up: {err}", self.db.tag());

                    Ok(())
                }
                Ok(Some(backoff_time)) => {
                    log::debug!(
                        "[{}] Job #{job_id} failed; will retry after {backoff_time:?}, in {}s: {err}",
                        self.db.tag(),
                        backoff_time.duration_since(UnixTime::now()).as_secs()
                    );

                    Ok(())
                }
                Err(err) => {
                    log::warn!(
                        "[{}] Job #{job_id} Failed to reschedule; giving up: {err}",
                        self.db.tag()
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
        let mut watch = self.db.dirty().subscribe();
        let mut start_counter = 1;
        let mut retry_lower_bound = None;
        let mut retry_jobs_missing_peers = self.retry_jobs_missing_peers.subscribe();

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
            let tag = self.db.tag();
            if let Some(c) = last_counter {
                start_counter = c + 1;
            }
            if let Some((job_id, job)) = job {
                if self.is_failed(job_id).await? {
                    // Skip it for now; it'll be handled after a delay.
                    continue;
                }
                log::debug!("[{tag}] Job #{job_id} Pending {job:?}");
                if tx.send((job_id, job)).await.is_err() {
                    // Broken channel; normal end of this loop
                    return Ok(());
                }
            } else {
                // Wait for more dirty paths, then continue.
                // Now is also a good time to retry failed jobs whose backoff period has passed.
                let mut jobs_to_retry = vec![];
                tokio::select!(
                    _ = tx.closed() => {
                        return Ok(());
                    }

                    ret = watch.wait_for(|v| *v >= start_counter) => {
                        ret?;
                    }

                    Ok(Some((backoff_until, mut jobs))) = self.wait_until_next_backoff(retry_lower_bound) => {
                        jobs_to_retry.append(&mut jobs);

                        // From now on, only retry jobs whose backoff
                        // period ends after backoff_until. This
                        // avoids retrying the same failed jobs within
                        // a single stream, relying on newly written
                        // backoffs always being in the future.
                        retry_lower_bound = Some(backoff_until);
                    }

                    Ok(mut jobs) = self.jobs_to_retry_missing_peers(&mut retry_jobs_missing_peers) => {
                        jobs_to_retry.append(&mut jobs);
                    }
                );
                for counter in jobs_to_retry {
                    if let Some((job_id, job)) =
                        self.build_job_with_counter(counter).await.ok().flatten()
                    {
                        log::debug!("[{tag}] Retry Job #{job_id}: {job:?}");
                        if tx.send((job_id, job)).await.is_err() {
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
        let mut seen = None;
        while let Some((pathid, counter)) = dirty.next_dirty(start_counter)? {
            if let Some(job) = self.build_job(&txn, &txn.read_tree()?, pathid)? {
                return Ok((Some((JobId(counter), job)), Some(counter)));
            }
            seen = Some(counter);
            start_counter = counter + 1;
        }

        Ok((None, seen))
    }

    /// Lookup the path given a counter and build a job, if possible.
    async fn build_job_with_counter(
        self: &Arc<Self>,
        counter: u64,
    ) -> Result<Option<(JobId, StorageJob)>, StorageError> {
        let this = Arc::clone(self);
        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            if let Some(pathid) = dirty.get_pathid_for_counter(counter)? {
                match this.build_job(&txn, &txn.read_tree()?, pathid) {
                    Ok(None) => return Ok(None),
                    Ok(Some(job)) => {
                        return Ok(Some((JobId(counter), job)));
                    }
                    Err(err) => {
                        log::warn!(
                            "[{tag}] Job #{counter} failed to build job for pathid {pathid}: {err:?}",
                            tag = this.db.tag()
                        );
                        return Ok(None);
                    }
                }
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
            if let Some(pathid) = tree.resolve(loc)? {
                if let Some(counter) = dirty.get_counter(pathid)? {
                    return Ok(this
                        .build_job(&txn, &tree, pathid)?
                        .map(|job| (JobId(counter), job)));
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
        tree: &impl TreeReadOperations,
        pathid: PathId,
    ) -> Result<Option<StorageJob>, StorageError> {
        let cache = txn.read_cache()?;
        let blobs = txn.read_blobs()?;

        let mut want_protect_blob = false;
        let mut want_unprotect_blob = false;
        let mut want_download = false;
        let mut want_realize = false;
        let mut want_unrealize = false;
        match txn.read_marks()?.get(tree, pathid)? {
            Mark::Watch => {
                want_unprotect_blob = true;
                want_unrealize = true;
            }
            Mark::Keep => {
                want_protect_blob = true;
                want_download = true;
                want_unrealize = true;
            }
            Mark::Own => {
                want_protect_blob = true;
                want_download = true;
                want_realize = true;
            }
        };

        let mut should_protect_blob = false;
        let mut should_unprotect_blob = false;
        let mut should_download = None;
        let mut should_realize = None;
        let mut should_unrealize = None;

        if let Some(cached) = cache.file_at_pathid(pathid)? {
            let hash = cached.hash.clone();
            let is_local = cached.is_local();

            if want_unrealize {
                if is_local {
                    should_unrealize = Some(hash.clone());
                }
            } else if want_realize {
                if !is_local {
                    should_realize = Some(hash.clone());
                }
            }
            if !is_local {
                let blob = blobs.get_with_pathid(pathid)?;
                if let Some(blob) = &blob {
                    if blob.protected {
                        should_unprotect_blob = want_unprotect_blob;
                    } else {
                        should_protect_blob = want_protect_blob;
                    }
                }
                if want_download
                    && blob
                        .map(|b| b.cache_status())
                        .unwrap_or(CacheStatus::Missing)
                        != CacheStatus::Verified
                {
                    should_download = Some(hash);
                }
            }
        }
        if should_protect_blob {
            return Ok(Some(StorageJob::ProtectBlob(pathid)));
        }
        if should_unprotect_blob {
            return Ok(Some(StorageJob::UnprotectBlob(pathid)));
        }
        if let Some(hash) = should_download {
            if let Some(path) = tree.backtrack(pathid)? {
                return Ok(Some(StorageJob::External(Job::Download(path, hash))));
            }
        }
        if let Some(hash) = should_realize {
            return Ok(Some(StorageJob::Realize { pathid, hash }));
        }
        if let Some(index_hash) = should_unrealize {
            return Ok(Some(StorageJob::Unrealize {
                pathid,
                indexed_hash: index_hash,
            }));
        }

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
    use crate::Blob;
    use crate::GlobalDatabase;
    use crate::Notification;
    use crate::PathIdAllocator;
    use crate::arena::fs::ArenaFilesystem;
    use crate::arena::index;
    use crate::arena::mark;
    use crate::arena::tree::TreeLoc;
    use crate::utils::redb_utils;
    use assert_fs::TempDir;
    use futures::StreamExt as _;
    use realize_types::{Arena, Peer, UnixTime};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time;

    fn test_hash() -> Hash {
        Hash([1; 32])
    }

    /// Fixture for testing Engine.
    struct EngineFixture {
        arena: Arena,
        db: Arc<ArenaDatabase>,
        acache: Arc<ArenaFilesystem>,
        engine: Arc<Engine>,
        _tempdir: TempDir,
    }
    impl EngineFixture {
        async fn setup() -> anyhow::Result<EngineFixture> {
            let _ = env_logger::try_init();

            let tempdir = TempDir::new()?;
            let arena = Arena::from("myarena");
            let blob_dir = tempdir.path().join("blobs");
            std::fs::create_dir_all(&blob_dir)?;
            let datadir = tempdir.path().join("data");
            std::fs::create_dir_all(&datadir)?;

            let db = ArenaDatabase::new(
                redb_utils::in_memory()?,
                arena,
                PathIdAllocator::new(GlobalDatabase::new(redb_utils::in_memory()?)?, [arena])?,
                &blob_dir,
                &datadir,
            )?;
            let acache = ArenaFilesystem::new(arena, Arc::clone(&db))?;
            let engine = Engine::new(Arc::clone(&db), |attempt| {
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

        fn add_file_to_index_with_version<T>(&self, path: T, hash: Hash) -> anyhow::Result<()>
        where
            T: AsRef<Path>,
        {
            let path = path.as_ref();
            Ok(index::add_file(
                &self.db,
                path,
                100,
                UnixTime::from_secs(1234567889),
                hash,
            )?)
        }

        fn update_cache(&self, notification: Notification) -> anyhow::Result<()> {
            let test_peer = Peer::from("other");
            self.acache.update(test_peer, notification)?;
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

        fn pathid(&self, path: &Path) -> anyhow::Result<PathId> {
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
        {
            let mut blob = fixture.acache.file_content(&barfile)?.blob().unwrap();
            blob.update(0, b"te").await?;
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
        {
            let mut blob = fixture.acache.file_content(&barfile)?.blob().unwrap();
            blob.update(0, b"test").await?;
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
        {
            let mut blob = fixture.acache.file_content(&barfile)?.blob().unwrap();
            blob.update(0, b"test").await?;
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
    async fn download_then_realize() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foobar = Path::parse("foo/bar")?;

        mark::set_arena_mark(&fixture.db, Mark::Own)?;
        fixture.add_file_to_cache(&foobar)?;

        let mut job_stream = fixture.engine.job_stream();

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(
            StorageJob::External(Job::Download(foobar.clone(), test_hash())),
            job,
        );
        {
            let mut blob = fixture.acache.file_content(&foobar)?.blob().unwrap();
            blob.update(0, b"test").await?;
            blob.mark_verified().await?;
        }
        fixture.engine.job_finished(job_id, Ok(JobStatus::Done))?;

        let (new_job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert!(new_job_id > job_id);
        assert_eq!(
            StorageJob::Realize {
                pathid: fixture.pathid(&foobar)?,
                hash: test_hash(),
            },
            job
        );

        Ok(())
    }

    #[tokio::test]
    async fn protect_then_download_then_realize() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foobar = Path::parse("foo/bar")?;

        fixture.add_file_to_cache(&foobar)?;
        {
            let mut blob = fixture.acache.file_content(&foobar)?.blob().unwrap();
            blob.update(0, b"te").await?;
        }
        // blob is partially available, but unprotected, going from
        // Mark::Watch to Mark::Keep triggers the need to:
        //
        // 1. set it protected
        // 2. download the rest and verify it
        // 3. realize it
        //
        // This test makes sure that the chain of events is correct.

        mark::set(&fixture.db, &foobar, Mark::Own)?;

        let pathid = fixture.pathid(&foobar)?;
        let mut job_stream = fixture.engine.job_stream();
        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(StorageJob::ProtectBlob(pathid), job,);
        let txn = fixture.db.begin_write()?;
        {
            let tree = txn.read_tree()?;
            let mut dirty = txn.write_dirty()?;
            txn.write_blobs()?
                .set_protected(&tree, &mut dirty, pathid, true)?;
        }
        txn.commit()?;
        fixture.engine.job_finished(job_id, Ok(JobStatus::Done))?;

        let (job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert_eq!(
            StorageJob::External(Job::Download(foobar, test_hash())),
            job,
        );
        {
            let mut blob = Blob::open(&fixture.db, pathid)?;
            blob.update(0, b"test").await?;
            blob.mark_verified().await?;
        }
        fixture.engine.job_finished(job_id, Ok(JobStatus::Done))?;

        let (new_job_id, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        assert!(new_job_id > job_id);
        assert_eq!(
            StorageJob::Realize {
                pathid: pathid,
                hash: test_hash(),
            },
            job
        );

        Ok(())
    }

    #[tokio::test]
    async fn unrealize() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foobar = Path::parse("foo/bar")?;

        mark::set(&fixture.db, &foobar, Mark::Keep)?;
        fixture.add_file_to_index_with_version(&foobar, test_hash())?;
        fixture.add_file_to_cache_with_version(&foobar, test_hash())?;

        // File is in the index. Going from Mark::Keep to Mark::Watch
        // triggers unrealize.
        mark::set(&fixture.db, &foobar, Mark::Watch)?;

        let mut job_stream = fixture.engine.job_stream();

        let (_, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        let pathid = fixture.pathid(&foobar)?;
        assert_eq!(
            StorageJob::Unrealize {
                pathid: pathid,
                indexed_hash: test_hash()
            },
            job
        );

        Ok(())
    }

    #[tokio::test]
    async fn realize_verified_file_to_own_not_in_index() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foobar = Path::parse("foo/bar")?;

        mark::set_arena_mark(&fixture.db, Mark::Own)?;
        fixture.add_file_to_cache(&foobar)?;
        {
            let mut blob = fixture.acache.file_content(&foobar)?.blob().unwrap();
            blob.update(0, b"test").await?;
            blob.mark_verified().await?;
        }

        let mut job_stream = fixture.engine.job_stream();

        let (_, job) = next_with_timeout(&mut job_stream).await?.unwrap();
        let pathid = fixture.pathid(&foobar)?;
        assert_eq!(
            StorageJob::Realize {
                pathid: pathid,
                hash: test_hash(),
            },
            job
        );

        Ok(())
    }

    #[tokio::test]
    async fn ignore_file_to_own_in_index_with_same_version() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foobar = Path::parse("foo/bar")?;

        mark::set_arena_mark(&fixture.db, Mark::Own)?;
        fixture.add_file_to_cache_with_version(&foobar, Hash([1; 32]))?;
        fixture.add_file_to_index_with_version(&foobar, Hash([1; 32]))?;

        assert_eq!(None, fixture.engine.job_for_loc(&foobar).await?);

        Ok(())
    }

    #[tokio::test]
    async fn ignore_file_to_own_in_index_with_different_version() -> anyhow::Result<()> {
        let fixture = EngineFixture::setup().await?;
        let foobar = Path::parse("foo/bar")?;

        mark::set_arena_mark(&fixture.db, Mark::Own)?;
        fixture.add_file_to_cache_with_version(&foobar, Hash([1; 32]))?;
        fixture.add_file_to_index_with_version(&foobar, Hash([2; 32]))?;

        assert_eq!(None, fixture.engine.job_for_loc(&foobar).await?);

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
