#![allow(dead_code)] // work in progress

use crate::mark;
use crate::unreal::{arena_cache, blob};
use crate::{Inode, Mark, StorageError};
use realize_types::{ByteRanges, Hash, Path};
use redb::{Database, ReadTransaction, ReadableTable as _, TableDefinition, WriteTransaction};
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

    /// A job returned by a stream has been successfully executed.
    ///
    /// The dirty mark is cleared unless the path has been modified
    /// after the job was created.
    pub(crate) fn job_done(&self, job: &Job) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        let path = job.path();

        let current = txn
            .open_table(DIRTY_TABLE)?
            .get(path.as_str())?
            .map(|e| e.value());
        if current == Some(job.counter()) {
            clear_dirty(&txn, path)?;
        }
        txn.commit()?;

        Ok(())
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
    use super::*;
    use crate::Notification;
    use crate::mark::PathMarks;
    use crate::unreal::arena_cache::ArenaCache;
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
            let engine = Engine::new(Arc::clone(&db), Arc::clone(&dirty_paths), arena_root);

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
        assert_eq!(2, job1.counter());
        {
            let txn = fixture.db.begin_write()?;
            fixture.dirty_paths.mark_dirty(&txn, &barfile)?;
            txn.commit()?;
        }

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
}
