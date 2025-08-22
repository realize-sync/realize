use super::db::ArenaDatabase;
use super::engine::{Engine, StorageJob};
use crate::arena::arena_cache::CacheReadOperations;
use crate::arena::index::{self, IndexReadOperations};
use crate::arena::tree::TreeExt;
use crate::{Inode, JobId, JobStatus, StorageError};
use realize_types::Hash;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::task::{self, JoinHandle};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

pub(crate) struct StorageJobProcessor {
    db: Arc<ArenaDatabase>,
    engine: Arc<Engine>,
    index_root: Option<PathBuf>,
}

impl StorageJobProcessor {
    pub(crate) fn new(
        db: Arc<ArenaDatabase>,
        engine: Arc<Engine>,
        index_root: Option<PathBuf>,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            engine,
            index_root,
        })
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
                    "[{}] Failed to report result of Job {job_id}: {err}",
                    self.db.arena()
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
            StorageJob::Unrealize(inode, hash) => Some(self.unrealize(inode, hash)),
            StorageJob::Realize(inode, hash, index_hash) => {
                Some(self.realize(inode, hash, index_hash))
            }
            StorageJob::ProtectBlob(inode) => Some(self.set_protected(inode, true)),
            StorageJob::UnprotectBlob(inode) => Some(self.set_protected(inode, false)),
        }
    }

    fn set_protected(&self, inode: Inode, protected: bool) -> Result<JobStatus, StorageError> {
        let txn = self.db.begin_write()?;
        let tree = txn.read_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        blobs.set_protected(&tree, &mut dirty, inode, protected)?;

        Ok(JobStatus::Done)
    }

    /// Move a file from the filesystem to the cache.
    ///
    /// Gives up and returns [JobStatus::Abandoned] if the current
    /// versions in the cache or the current version in the index
    /// don't match `hash`.
    fn unrealize(&self, inode: Inode, hash: Hash) -> Result<JobStatus, StorageError> {
        let root = match &self.index_root {
            Some(ret) => ret,
            None => return Err(StorageError::NoLocalStorage(self.db.arena())),
        };
        let path: realize_types::Path;
        let realpath: PathBuf;
        let cachepath: PathBuf;
        let txn = self.db.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let mut index = txn.write_index()?;
            path = match tree.backtrack(inode)? {
                Some(v) => v,
                None => return Ok(JobStatus::Abandoned("no_path")),
            };
            realpath = path.within(root);
            let indexed = match index.get_at_inode(inode)? {
                Some(v) => v,
                None => return Ok(JobStatus::Abandoned("not_in_index")),
            };
            if !indexed.matches_file(&realpath) {
                return Ok(JobStatus::Abandoned("file_mismatch"));
            }
            let cached = match txn.write_cache()?.get_at_inode(inode)? {
                Some(v) => v,
                None => return Ok(JobStatus::Abandoned("no_cache_entry")),
            };
            if cached.hash != hash {
                return Ok(JobStatus::Abandoned("cache_version_mismatch"));
            }
            let mut blobs = txn.write_blobs()?;
            let marks = txn.read_marks()?;
            cachepath = blobs.import(&mut tree, &marks, inode, &hash, &realpath.metadata()?)?;

            let mut history = txn.write_history()?;
            let mut dirty = txn.write_dirty()?;
            index.drop(&mut tree, &mut history, &mut dirty, inode)?;

            // We make a second check of the file mtime and size just
            // before renaming, in case it has changed.
            //
            // Even with that, it's *still* possible some handle is
            // open and is going to write on the file after the move,
            // a change that would eventually be lost when the file in
            // the cache is verified.
            if !indexed.matches_file(&realpath) {
                return Ok(JobStatus::Abandoned("file_mismatch(late)"));
            }

            // Database changes are ready. Make the fs change.
            std::fs::rename(&realpath, &cachepath)?;
            log::debug!("renamed {realpath:?} to {cachepath:?}");
        }
        let committed = txn.commit();
        if committed.is_err() {
            log::warn!("commit failed; revert {realpath:?}");
            // best effort revert of the fs change
            std::fs::rename(&cachepath, &realpath)?;
        }
        committed?;

        log::debug!(
            "Unrealized {realpath:?} {hash} into the cache as [{}]/{path}",
            self.db.arena()
        );

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
        inode: Inode,
        cache_hash: Hash,
        index_hash: Option<Hash>,
    ) -> Result<JobStatus, StorageError> {
        let arena = self.db.arena();
        let root = match &self.index_root {
            Some(ret) => ret,
            None => return Err(StorageError::NoLocalStorage(arena)),
        };
        let moved: bool;
        let txn = self.db.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let path = match tree.backtrack(inode)? {
                Some(path) => path,
                None => {
                    return Ok(JobStatus::Abandoned("no_path"));
                }
            };
            let realpath = index::indexed_file_path(
                &txn.read_index()?,
                &tree,
                &root,
                &path,
                index_hash.as_ref(),
            )?;
            let mut blobs = txn.write_blobs()?;
            if let Some(realpath) = realpath {
                moved = blobs.export(&mut tree, &path, &cache_hash, &realpath)?;
                if moved {
                    log::debug!("Realized [{arena}]/{path} {cache_hash} as {realpath:?}");
                }
            } else {
                return Ok(JobStatus::Abandoned("indexed_file_path"));
            }
        }
        if moved {
            txn.commit()?;
            Ok(JobStatus::Done)
        } else {
            Ok(JobStatus::Abandoned("blobs.export"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arena::index::IndexedFile;
    use crate::utils::hash;
    use crate::{ArenaCache, Blob, Inode, LocalAvailability, Mark, Notification};
    use assert_fs::TempDir;
    use assert_fs::fixture::ChildPath;
    use assert_fs::prelude::*;
    use realize_types::{Arena, Path, Peer, UnixTime};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct Fixture {
        arena: Arena,
        db: Arc<ArenaDatabase>,
        cache: Arc<ArenaCache>,
        root: ChildPath,
        processor: Arc<StorageJobProcessor>,
        _tempdir: TempDir,
    }

    impl Fixture {
        fn setup() -> anyhow::Result<Self> {
            Self::setup_internal(true)
        }

        fn setup_cache_only() -> anyhow::Result<Self> {
            Self::setup_internal(false)
        }

        /// Call setup or setup_cache_only
        fn setup_internal(with_root: bool) -> anyhow::Result<Self> {
            let _ = env_logger::try_init();
            let tempdir = TempDir::new()?;

            let arena = Arena::from("myarena");
            let blob_dir = tempdir.path().join("blobs");
            let db = ArenaDatabase::for_testing_single_arena(arena, &blob_dir)?;
            let root = tempdir.child("root");
            root.create_dir_all()?;
            let cache = ArenaCache::new(arena, Arc::clone(&db), &blob_dir)?;

            let engine = Engine::new(arena, Arc::clone(&db), |attempt| {
                if attempt < 3 {
                    Some(Duration::from_secs(1))
                } else {
                    None
                }
            });
            let processor = StorageJobProcessor::new(
                Arc::clone(&db),
                Arc::clone(&engine),
                if with_root {
                    Some(root.to_path_buf())
                } else {
                    None
                },
            );

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
                    mtime: UnixTime::from_secs(1234567890),
                    size,
                    hash: hash.clone(),
                },
                Some(&self.root),
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
                    mtime: UnixTime::from_secs(1234567890),
                    size,
                    old_hash: old_hash.clone(),
                    hash: new_hash.clone(),
                },
                Some(&self.root),
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

        fn find_in_cache(&self, path_str: &str) -> Result<Option<Inode>, StorageError> {
            match self.cache.expect(&Path::parse(path_str)?) {
                Ok(inode) => Ok(Some(inode)),
                Err(StorageError::NotFound) => Ok(None),
                Err(err) => Err(err),
            }
        }

        fn open_blob(&self, path_str: &str) -> anyhow::Result<Blob> {
            let inode = self.find_in_cache(path_str)?.expect("{path_str} in cache");

            Ok(self.cache.open_file(inode)?)
        }

        async fn read_blob_content(&self, path_str: &str) -> anyhow::Result<String> {
            let mut blob = self.open_blob(path_str)?;
            let mut buf = String::new();
            blob.read_to_string(&mut buf).await?;

            Ok(buf)
        }

        fn inode(&self, path: &Path) -> anyhow::Result<Inode> {
            let txn = self.db.begin_read()?;

            Ok(txn.read_tree()?.expect(path)?)
        }

        async fn unrealize(&self, path: Path, hash: Hash) -> anyhow::Result<JobStatus> {
            log::debug!("unrealize({path}, {hash})");
            let inode = self.inode(&path)?;
            let processor = Arc::clone(&self.processor);
            tokio::task::spawn_blocking(move || {
                let status = processor.unrealize(inode, hash)?;

                log::debug!("-> {status:?}");
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
            log::debug!("realize({path}, {hash})");
            let inode = self.inode(&path)?;
            let processor = Arc::clone(&self.processor);
            tokio::task::spawn_blocking(move || {
                let status = processor.realize(inode, hash, index_hash)?;

                log::debug!("-> {status:?}");
                Ok::<JobStatus, anyhow::Error>(status)
            })
            .await?
        }
    }

    #[tokio::test]
    async fn unrealize_success() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let hash = fixture.create_indexed_file("test.txt", "foobar").await?;
        fixture.add_to_cache("test.txt", &hash, 6)?;
        let path = Path::parse("test.txt")?;
        assert_eq!(JobStatus::Done, fixture.unrealize(path, hash).await?);
        assert!(fixture.find_in_index("test.txt")?.is_none());
        assert!(!fixture.file_exists("test.txt"));
        assert_eq!("foobar", fixture.read_blob_content("test.txt").await?);

        Ok(())
    }

    #[tokio::test]
    async fn unrealize_no_local_storage() -> anyhow::Result<()> {
        let fixture = Fixture::setup_cache_only()?;
        let hash = hash::digest("foobar");
        fixture.add_to_cache("test.txt", &hash, 6)?;
        let path = Path::parse("test.txt")?;
        assert!(matches!(
            fixture.processor.unrealize(fixture.inode(&path)?, hash),
                Err(StorageError::NoLocalStorage(a)) if a == fixture.arena));

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
    async fn unrealize_wrong_hash_in_cache() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let hash = fixture.create_indexed_file("test.txt", "foobar").await?;
        fixture.add_to_cache("test.txt", &hash::digest("something else"), 6)?;

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
    async fn realize_new_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("test.txt")?;
        fixture.set_mark(&path, Mark::Own)?;

        let hash = hash::digest("test");
        fixture.add_to_cache("test.txt", &hash, 4)?;
        let inode = fixture.inode(&path)?;
        {
            let mut blob = fixture.cache.open_file(inode)?;
            blob.write_all(b"test").await?;
            blob.mark_verified().await?;
        }

        assert_eq!(
            JobStatus::Done,
            fixture.realize(path.clone(), hash.clone(), None).await?
        );

        assert_eq!(
            "test".to_string(),
            std::fs::read_to_string(path.within(&fixture.root))?
        );
        assert_eq!(
            LocalAvailability::Missing,
            fixture.cache.local_availability(inode)?
        );

        Ok(())
    }

    #[tokio::test]
    async fn realize_replaces_outdated() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("test.txt")?;
        fixture.set_mark(&path, Mark::Own)?;
        let inode = fixture.inode(&path)?;

        let old_hash = hash::digest("old");
        let new_hash = hash::digest("new!");
        fixture.add_to_cache("test.txt", &old_hash, 3)?;
        fixture.create_indexed_file("test.txt", "old").await?;
        fixture.replace_in_cache("test.txt", &old_hash, &new_hash, 4)?;
        {
            let mut blob = fixture.cache.open_file(inode)?;
            blob.write_all(b"new!").await?;
            blob.mark_verified().await?;
        }

        assert_eq!(
            JobStatus::Done,
            fixture
                .realize(path.clone(), new_hash.clone(), Some(old_hash.clone()))
                .await?
        );

        assert_eq!(
            "new!".to_string(),
            std::fs::read_to_string(path.within(&fixture.root))?
        );
        assert_eq!(
            LocalAvailability::Missing,
            fixture.cache.local_availability(inode)?
        );

        Ok(())
    }

    #[tokio::test]
    async fn realize_skip_wrong_version() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("test.txt")?;
        fixture.set_mark(&path, Mark::Own)?;

        let hash = hash::digest("test");
        fixture.add_to_cache("test.txt", &hash, 4)?;
        let inode = fixture.inode(&path)?;
        {
            let mut blob = fixture.cache.open_file(inode)?;
            blob.write_all(b"test").await?;
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
        let inode = fixture.inode(&path)?;
        {
            let mut blob = fixture.cache.open_file(inode)?;
            blob.write_all(b"test").await?;
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
