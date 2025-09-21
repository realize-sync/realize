use super::db::ArenaDatabase;
use super::engine::{Engine, StorageJob};
use crate::arena::blob::BlobExt;
use crate::arena::cache::{CacheExt, CacheReadOperations};
use crate::{JobId, JobStatus, PathId, StorageError};
use realize_types::Hash;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::task::{self, JoinHandle};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

pub(crate) struct StorageJobProcessor {
    db: Arc<ArenaDatabase>,
    engine: Arc<Engine>,
}

impl StorageJobProcessor {
    pub(crate) fn new(db: Arc<ArenaDatabase>, engine: Arc<Engine>) -> Arc<Self> {
        Arc::new(Self { db, engine })
    }

    pub fn spawn(self: Arc<Self>, shutdown: CancellationToken) -> JoinHandle<()> {
        tokio::spawn(async move { self.process_jobs(shutdown).await })
    }

    async fn process_jobs(self: &Arc<Self>, shutdown: CancellationToken) {
        let mut stream = self.engine.job_stream();

        while let Some((job_id, job)) = tokio::select!(
            _ = shutdown.cancelled() => {
                return
            }
            ret = stream.next() => ret
        ) {
            if let Err(err) = self.process_and_report(job_id, job).await {
                log::debug!(
                    "[{}] Job #{job_id} Failed to report result: {err}",
                    self.db.tag()
                )
            }
        }
    }

    async fn process_and_report(
        self: &Arc<Self>,
        job_id: JobId,
        job: StorageJob,
    ) -> anyhow::Result<()> {
        let this = Arc::clone(self);
        task::spawn_blocking(move || {
            if !matches!(job, StorageJob::External(_)) {
                log::info!("[{}] Job #{job_id} Starting {job:?}", this.db.tag());
            }
            if let Some(status) = this.process_job(job) {
                this.engine
                    .job_finished(job_id, status.map_err(|e| e.into()))?;
            }

            Ok::<(), anyhow::Error>(())
        })
        .await??;

        Ok(())
    }

    fn process_job(&self, job: StorageJob) -> Option<Result<JobStatus, StorageError>> {
        match job {
            StorageJob::External(_) => None,
            StorageJob::Unrealize {
                pathid,
                indexed_hash: hash,
            } => Some(self.unrealize(pathid, hash)),
            StorageJob::Realize {
                pathid,
                cached_hash,
                indexed_hash,
            } => Some(self.realize(pathid, cached_hash, indexed_hash)),
            StorageJob::ProtectBlob(pathid) => Some(self.set_protected(pathid, true)),
            StorageJob::UnprotectBlob(pathid) => Some(self.set_protected(pathid, false)),
        }
    }

    fn set_protected(&self, pathid: PathId, protected: bool) -> Result<JobStatus, StorageError> {
        let txn = self.db.begin_write()?;
        {
            let tree = txn.read_tree()?;
            let mut blobs = txn.write_blobs()?;
            let mut dirty = txn.write_dirty()?;
            blobs.set_protected(&tree, &mut dirty, pathid, protected)?;
        }
        txn.commit()?;
        Ok(JobStatus::Done)
    }

    /// Move a file from the filesystem to the cache.
    ///
    /// Gives up and returns [JobStatus::Abandoned] if the current
    /// versions in the cache or the current version in the index
    /// don't match `hash`.
    fn unrealize(&self, pathid: PathId, hash: Hash) -> Result<JobStatus, StorageError> {
        let realpath: PathBuf;
        let cachepath: Option<PathBuf>;
        let txn = self.db.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let mut cache = txn.write_cache()?;
            let indexed = match cache.index_entry_at_pathid(pathid)? {
                Some(v) => v,
                None => return Ok(JobStatus::Abandoned("not_in_index")),
            };
            if indexed.hash != hash {
                return Ok(JobStatus::Abandoned("indexed_version_mismatch"));
            }
            let mut blobs = txn.write_blobs()?;
            let mut history = txn.write_history()?;
            let mut dirty = txn.write_dirty()?;
            let marks = txn.read_marks()?;
            match cache.unrealize(
                &mut tree,
                &mut blobs,
                &mut history,
                &mut dirty,
                &marks,
                pathid,
            ) {
                Ok((s, d)) => {
                    realpath = s;
                    cachepath = d;
                }
                Err(StorageError::DatabaseOutdated(_)) => {
                    return Ok(JobStatus::Abandoned("file_version_mismatch"));
                }
                Err(StorageError::NoPeers) => {
                    return Ok(JobStatus::Abandoned("not_in_cache"));
                }
                Err(err) => return Err(err),
            }
        }
        txn.commit()?;

        let tag = self.db.tag();

        // Database changes are ready. Make the fs change.
        //
        // This is done *after* updating the database because if the
        // file is moved/deleted and the database is not updated it
        // would look like the user deleted the file, which then might
        // be propagated to other peers.
        if let Some(cachepath) = &cachepath {
            std::fs::rename(&realpath, cachepath)?;
            log::debug!("[{tag}] Renamed {realpath:?} to {cachepath:?}",);
        } else {
            std::fs::remove_file(&realpath)?;
            log::debug!("[{tag}] Deleted {realpath:?}");
        }

        log::info!("[{tag}] Unrealized pathid {pathid} {hash} from {realpath:?}",);

        return Ok(JobStatus::Done);
    }

    /// Move a file from the cache to the filesystem.
    ///
    /// The file must have been fully downloaded and verified or the
    /// move will fail.
    ///
    /// Give up and return false if the current versions in the cache
    /// don't match `cache_hash` and `index_hash`.
    ///
    /// A `index_hash` value of `None` means that the file must not
    /// exit. If it exists, realize gives up and returns false.
    fn realize(
        &self,
        pathid: PathId,
        cache_hash: Hash,
        index_hash: Option<Hash>,
    ) -> Result<JobStatus, StorageError> {
        let tag = self.db.tag();
        let source: PathBuf;
        let dest: PathBuf;
        let txn = self.db.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let mut cache = txn.write_cache()?;
            let cached = match cache.file_at_pathid(pathid)? {
                Some(e) => e,
                None => {
                    return Ok(JobStatus::Abandoned("cache_entry"));
                }
            };
            if cached.hash != cache_hash {
                return Ok(JobStatus::Abandoned("cache_version"));
            }

            // Make sure that indexed file corresponds to index_hash.
            let indexed_previously = cache.indexed(&tree, pathid)?;
            match index_hash {
                None => {
                    if indexed_previously.is_some() {
                        return Ok(JobStatus::Abandoned("indexed_previously"));
                    }
                }
                Some(index_hash) => match &indexed_previously {
                    None => return Ok(JobStatus::Abandoned("not_indexed")),
                    Some(e) => {
                        if e.hash != index_hash {
                            return Ok(JobStatus::Abandoned("indexed_version_mismatch"));
                        }
                    }
                },
            }

            let mut blobs = txn.write_blobs()?;
            let blobinfo = match blobs.get(&tree, pathid)? {
                Some(b) => b,
                None => return Ok(JobStatus::Abandoned("no_blob")),
            };
            if blobinfo.hash != cache_hash {
                return Ok(JobStatus::Abandoned("blob_version"));
            }
            if !blobinfo.verified {
                return Ok(JobStatus::Abandoned("not_verified"));
            }
            let mut history = txn.write_history()?;
            let mut dirty = txn.write_dirty()?;
            (source, dest) =
                cache.realize(&mut tree, &mut blobs, &mut dirty, &mut history, pathid)?;

            // Make sure that dest corresponds to indexed_previously
            // and that we're not overwriting something else.
            match indexed_previously {
                Some(indexed_previously) => {
                    if !indexed_previously.matches_file(&dest) {
                        return Ok(JobStatus::Abandoned("file_mismatch"));
                    }
                }
                None => {
                    if dest.exists() {
                        return Ok(JobStatus::Abandoned("unexpected_file"));
                    }
                }
            }

            // Set mtime on source (best effort)
            if let Some(time) = cached.mtime.as_system_time() {
                let f = File::open(&source)?;
                let _ = f.set_modified(time);
            }
        }
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::rename(&source, &dest)?;
        let ret = txn.commit();
        if !ret.is_ok() {
            // Commit didn't work; try to revert the file change
            std::fs::rename(&dest, &source)?;
        }
        ret?;
        log::info!("[{tag}] Realized pathid {pathid} {cache_hash} as {dest:?}");

        Ok(JobStatus::Done)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arena::cache::CacheExt;
    use crate::arena::history::HistoryReadOperations;
    use crate::arena::index;
    use crate::arena::tree::TreeExt;
    use crate::arena::types::HistoryTableEntry;
    use crate::arena::types::IndexedFile;
    use crate::utils::hash;
    use crate::{ArenaFilesystem, Blob, CacheStatus, Mark, Notification, PathId};
    use assert_fs::TempDir;
    use assert_fs::fixture::ChildPath;
    use assert_fs::prelude::*;
    use realize_types::{Arena, Path, Peer, UnixTime};
    use std::time::Duration;
    use tokio::io::AsyncReadExt;

    fn test_time() -> UnixTime {
        UnixTime::from_secs(1234567890)
    }

    struct Fixture {
        arena: Arena,
        db: Arc<ArenaDatabase>,
        cache: Arc<ArenaFilesystem>,
        root: ChildPath,
        processor: Arc<StorageJobProcessor>,
        _tempdir: TempDir,
    }

    impl Fixture {
        fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();
            let tempdir = TempDir::new()?;

            let arena = Arena::from("myarena");
            let blob_dir = tempdir.path().join("blobs");
            let root = tempdir.child("root");
            root.create_dir_all()?;
            let db = ArenaDatabase::for_testing_single_arena(arena, &blob_dir, &root)?;
            let cache = ArenaFilesystem::new(arena, Arc::clone(&db))?;

            let engine = Engine::new(Arc::clone(&db), |attempt| {
                if attempt < 3 {
                    Some(Duration::from_secs(1))
                } else {
                    None
                }
            });
            let processor = StorageJobProcessor::new(Arc::clone(&db), Arc::clone(&engine));

            let fixture = Self {
                arena,
                db,
                cache,
                root,
                processor,
                _tempdir: tempdir,
            };
            Ok(fixture)
        }

        /// Create a test file in the filesystem and wait for it to be
        /// added to the index.
        async fn create_indexed_file(&self, path_str: &str, content: &str) -> anyhow::Result<Hash> {
            let child = self.root.child(path_str);
            child.write_str(content)?;

            let path = Path::parse(path_str)?;
            let m = child.path().metadata()?;
            index::add_file(
                &self.db,
                &path,
                m.len(),
                UnixTime::mtime(&m),
                hash::digest(content),
            )?;
            let entry = {
                let this = &self;
                index::get_file(&this.db, &Path::parse(path_str)?)
            }?
            .expect("{path_str} indexed");

            Ok(entry.hash)
        }

        fn add_to_cache(&self, path_str: &str, hash: &Hash, size: u64) -> anyhow::Result<()> {
            self.cache.update(
                Peer::from("peer"),
                Notification::Add {
                    arena: self.arena,
                    index: 1,
                    path: Path::parse(path_str)?,
                    mtime: test_time(),
                    size,
                    hash: hash.clone(),
                },
            )?;

            Ok(())
        }

        fn replace_in_cache(
            &self,
            path_str: &str,
            old_hash: &Hash,
            new_hash: &Hash,
            size: u64,
        ) -> anyhow::Result<()> {
            self.cache.update(
                Peer::from("peer"),
                Notification::Replace {
                    arena: self.arena,
                    index: 1,
                    path: Path::parse(path_str)?,
                    mtime: test_time(),
                    size,
                    old_hash: old_hash.clone(),
                    hash: new_hash.clone(),
                },
            )?;

            Ok(())
        }

        fn set_mark(&self, path: &Path, mark: Mark) -> Result<(), StorageError> {
            let txn = self.db.begin_write()?;
            {
                let mut tree = txn.write_tree()?;
                let mut dirty = txn.write_dirty()?;
                txn.write_marks()?.set(&mut tree, &mut dirty, path, mark)?;
            }
            txn.commit()?;

            Ok(())
        }

        /// Check if a file exists in the filesystem
        fn file_exists(&self, path: &str) -> bool {
            self.root.join(path).exists()
        }

        fn find_in_index(&self, path_str: &str) -> Result<Option<IndexedFile>, StorageError> {
            index::get_file(&self.db, &Path::parse(path_str)?)
        }

        fn open_blob(&self, path_str: &str) -> anyhow::Result<Blob> {
            Ok(self
                .cache
                .file_content(Path::parse(path_str)?)?
                .blob()
                .unwrap())
        }

        async fn read_blob_content(&self, path_str: &str) -> anyhow::Result<String> {
            let mut blob = self.open_blob(path_str)?;
            let mut buf = String::new();
            blob.read_to_string(&mut buf).await?;

            Ok(buf)
        }

        fn pathid(&self, path: &Path) -> anyhow::Result<PathId> {
            let txn = self.db.begin_read()?;

            Ok(txn.read_tree()?.expect(path)?)
        }

        async fn unrealize(&self, path: Path, hash: Hash) -> anyhow::Result<JobStatus> {
            log::debug!("[{}] Unrealize \"{path}\" {hash}", self.processor.db.tag());
            let pathid = self.pathid(&path)?;
            let processor = Arc::clone(&self.processor);
            let tag = self.processor.db.tag();
            tokio::task::spawn_blocking(move || {
                let status = processor.unrealize(pathid, hash)?;

                log::debug!("[{tag}] -> {status:?}");
                Ok::<JobStatus, anyhow::Error>(status)
            })
            .await?
        }

        async fn realize(
            &self,
            path: Path,
            hash: Hash,
            index_hash: Option<Hash>,
        ) -> anyhow::Result<JobStatus> {
            let tag = self.processor.db.tag();
            log::debug!("[{tag}] Realize({path}, {hash})",);
            let pathid = self.pathid(&path)?;
            let processor = Arc::clone(&self.processor);
            tokio::task::spawn_blocking(move || {
                let status = processor.realize(pathid, hash, index_hash)?;

                log::debug!("[{tag}] -> {status:?}");
                Ok::<JobStatus, anyhow::Error>(status)
            })
            .await?
        }
    }

    #[tokio::test]
    async fn unrealize_success() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let hash = fixture
            .create_indexed_file("dir/test.txt", "foobar")
            .await?;
        fixture.add_to_cache("dir/test.txt", &hash, 6)?;
        let path = Path::parse("dir/test.txt")?;
        assert_eq!(JobStatus::Done, fixture.unrealize(path, hash).await?);
        assert!(fixture.find_in_index("dir/test.txt")?.is_none());
        assert!(!fixture.file_exists("dir/test.txt"));
        assert_eq!("foobar", fixture.read_blob_content("dir/test.txt").await?);

        Ok(())
    }

    #[tokio::test]
    async fn unrealize_not_in_cache() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let hash = fixture.create_indexed_file("test.txt", "foobar").await?;

        let path = Path::parse("test.txt")?;
        assert!(matches!(
            fixture.unrealize(path, hash).await?,
            JobStatus::Abandoned(_)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn unrealize_empty_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let hash = fixture.create_indexed_file("test.txt", "").await?;
        fixture.add_to_cache("test.txt", &hash, 0)?;

        let path = Path::parse("test.txt")?;
        assert_eq!(JobStatus::Done, fixture.unrealize(path, hash).await?);

        assert!(fixture.find_in_index("test.txt")?.is_none());
        assert!(!fixture.file_exists("test.txt"));
        assert_eq!("", fixture.read_blob_content("test.txt").await?);

        Ok(())
    }

    #[tokio::test]
    async fn unrealize_outdated() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let indexed_hash = fixture
            .create_indexed_file("dir/test.txt", "foobar")
            .await?;
        let cached_hash = hash::digest("cached");
        fixture.add_to_cache("dir/test.txt", &indexed_hash, 6)?;
        fixture.replace_in_cache("dir/test.txt", &indexed_hash, &cached_hash, 8)?;

        let path = Path::parse("dir/test.txt")?;
        assert_eq!(
            JobStatus::Done,
            fixture
                .unrealize(path.clone(), indexed_hash.clone())
                .await?
        );

        let txn = fixture.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        let history = txn.read_history()?;
        assert!(!cache.is_indexed(&tree, &path)?);
        assert!(!fixture.file_exists("dir/test.txt"));

        assert!(cache.metadata(&tree, &path)?.is_some());

        // The removal must be reported as a Drop, not Remove.
        let history_entry = history
            .history(history.last_history_index()?..)
            .map(|res| res.map(|(_, e)| e))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            vec![HistoryTableEntry::Drop(path.clone(), indexed_hash)],
            history_entry
        );

        Ok(())
    }

    #[tokio::test]
    async fn realize_new_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("dir/test.txt")?;
        fixture.set_mark(&path, Mark::Own)?;

        let hash = hash::digest("test");
        fixture.add_to_cache("dir/test.txt", &hash, 4)?;
        let pathid = fixture.pathid(&path)?;
        {
            let mut blob = fixture.cache.file_content(pathid)?.blob().unwrap();
            blob.update(0, b"test").await?;
            blob.mark_verified().await?;
        }

        assert_eq!(
            JobStatus::Done,
            fixture.realize(path.clone(), hash.clone(), None).await?
        );

        let realpath = path.within(&fixture.root);
        assert_eq!("test".to_string(), std::fs::read_to_string(&realpath)?);
        assert_eq!(test_time(), UnixTime::mtime(&realpath.metadata()?));

        let txn = fixture.db.begin_read()?;
        let cache = txn.read_cache()?;
        let tree = txn.read_tree()?;
        let blobs = txn.read_blobs()?;
        assert_eq!(CacheStatus::Missing, blobs.cache_status(&tree, pathid)?);
        let indexed = cache
            .index_entry_at_pathid(pathid)?
            .expect("must have been indexed");
        assert_eq!(hash, indexed.hash);
        assert_eq!(test_time(), indexed.mtime);

        Ok(())
    }

    #[tokio::test]
    async fn realize_replaces_outdated() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("test.txt")?;
        fixture.set_mark(&path, Mark::Own)?;
        let pathid = fixture.pathid(&path)?;

        let old_hash = hash::digest("old");
        let new_hash = hash::digest("new!");
        fixture.add_to_cache("test.txt", &old_hash, 3)?;
        fixture.create_indexed_file("test.txt", "old").await?;
        fixture.replace_in_cache("test.txt", &old_hash, &new_hash, 4)?;
        {
            let mut blob = fixture.cache.file_content(pathid)?.blob().unwrap();
            blob.update(0, b"new!").await?;
            blob.mark_verified().await?;
        }

        assert_eq!(
            JobStatus::Done,
            fixture
                .realize(path.clone(), new_hash.clone(), Some(old_hash.clone()))
                .await?
        );

        let realpath = path.within(&fixture.root);
        assert_eq!("new!".to_string(), std::fs::read_to_string(&realpath)?);
        assert_eq!(test_time(), UnixTime::mtime(&realpath.metadata()?));

        let txn = fixture.db.begin_read()?;
        let cache = txn.read_cache()?;
        let tree = txn.read_tree()?;
        let blobs = txn.read_blobs()?;
        assert_eq!(CacheStatus::Missing, blobs.cache_status(&tree, pathid)?);
        let indexed = cache
            .index_entry_at_pathid(pathid)?
            .expect("must have been indexed");
        assert_eq!(new_hash, indexed.hash);
        assert_eq!(test_time(), indexed.mtime);
        assert_eq!(None, indexed.outdated_by);

        Ok(())
    }

    #[tokio::test]
    async fn realize_skip_wrong_version() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("test.txt")?;
        fixture.set_mark(&path, Mark::Own)?;

        let hash = hash::digest("test");
        fixture.add_to_cache("test.txt", &hash, 4)?;
        let pathid = fixture.pathid(&path)?;
        {
            let mut blob = fixture.cache.file_content(pathid)?.blob().unwrap();
            blob.update(0, b"test").await?;
            blob.mark_verified().await?;
        }

        assert!(matches!(
            fixture
                .realize(path.clone(), hash::digest("something else"), None)
                .await?,
            JobStatus::Abandoned(_)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn realize_skip_not_verified() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("test.txt")?;
        fixture.set_mark(&path, Mark::Own)?;

        let hash = hash::digest("test");
        fixture.add_to_cache("test.txt", &hash, 4)?;
        let pathid = fixture.pathid(&path)?;
        {
            let mut blob = fixture.cache.file_content(pathid)?.blob().unwrap();
            blob.update(0, b"test").await?;
            blob.update_db().await?; // complete, but not verified
        }

        assert!(matches!(
            fixture
                .realize(path.clone(), hash::digest("something else"), None)
                .await?,
            JobStatus::Abandoned(_)
        ));

        Ok(())
    }
}
