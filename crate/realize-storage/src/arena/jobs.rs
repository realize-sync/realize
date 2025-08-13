use super::arena_cache::ArenaCache;
use super::db::ArenaDatabase;
use super::engine::{Engine, StorageJob};
use super::index::RealIndex;
use crate::{JobId, JobStatus, StorageError};
use realize_types::{Hash, Path};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::task::{self, JoinHandle};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

pub(crate) struct StorageJobProcessor {
    db: Arc<ArenaDatabase>,
    engine: Arc<Engine>,
    cache: Arc<ArenaCache>,
    index: Option<(Arc<dyn RealIndex>, PathBuf)>,
}

impl StorageJobProcessor {
    pub(crate) fn new(
        db: Arc<ArenaDatabase>,
        engine: Arc<Engine>,
        cache: Arc<ArenaCache>,
        index: Option<(Arc<dyn RealIndex>, PathBuf)>,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            engine,
            cache,
            index,
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
                    self.cache.arena()
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
            StorageJob::Unrealize(path, hash) => Some(self.unrealize(path, hash)),
        }
    }

    /// Move a file from the filesystem to the cache.
    ///
    /// Gives up and returns [JobStatus::Abandoned] if the current
    /// versions in the cache or the current version in the index
    /// don't match `hash`.
    fn unrealize(&self, path: Path, hash: Hash) -> Result<JobStatus, StorageError> {
        let (index, root) = match &self.index {
            Some(ret) => ret,
            None => return Err(StorageError::NoLocalStorage(self.cache.arena())),
        };
        let txn = self.db.begin_write()?;
        let realpath = match index.get_indexed_file_txn(&txn, root, &path, Some(&hash))? {
            Some(ret) => ret,
            None => {
                return Ok(JobStatus::Abandoned("get_indexed_file"));
            }
        };
        let cachepath =
            match self
                .cache
                .move_into_blob_if_matches(&txn, &path, &hash, &realpath.metadata()?)?
            {
                Some(ret) => ret,
                None => {
                    return Ok(JobStatus::Abandoned("move_into_blob"));
                }
            };

        // drop_file_if_matches makes a second check of
        // the file mtime and size just before renaming,
        // in case it has changed.

        // Even with that, it's *still* possible some
        // handle is open and is going to write on the
        // file after the move, a change that would
        // eventually be lost when the file in the cache
        // is verified.
        if !index.drop_file_if_matches(&txn, root, &path, &hash)? {
            return Ok(JobStatus::Abandoned("drop_file_if_matches"));
        }

        // Database changes are ready. Make the fs change.
        std::fs::rename(&realpath, &cachepath)?;
        log::debug!("renamed {realpath:?} to {cachepath:?}");
        let committed = txn.commit();
        if committed.is_err() {
            log::warn!("commit failed; revert {realpath:?}");
            // best effort revert of the fs change
            std::fs::rename(&cachepath, &realpath)?;
        }
        committed?;

        log::debug!(
            "Unrealized {realpath:?} {hash} into the cache as [{}]/{path}",
            index.arena()
        );

        return Ok(JobStatus::Done);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::arena::engine::DirtyPaths;
    use crate::arena::types::IndexedFileTableEntry;
    use crate::utils::{hash, redb_utils};
    use crate::{Blob, GlobalDatabase, Inode, InodeAllocator, Notification};
    use assert_fs::TempDir;
    use assert_fs::fixture::ChildPath;
    use assert_fs::prelude::*;
    use realize_types::{Arena, Path, Peer, UnixTime};
    use tokio::io::AsyncReadExt;

    struct Fixture {
        arena: Arena,
        cache: Arc<ArenaCache>,
        index: Arc<dyn RealIndex>,
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
            let arena = Arena::from("myarena");
            let tempdir = TempDir::new()?;

            let globaldb = GlobalDatabase::new(redb_utils::in_memory()?)?;
            let allocator = InodeAllocator::new(globaldb, [arena])?;
            let arena = Arena::from("myarena");
            let db = ArenaDatabase::new(redb_utils::in_memory()?)?;
            let dirty_paths = DirtyPaths::new_blocking(Arc::clone(&db))?;
            let root = tempdir.child("root");
            root.create_dir_all()?;
            let cache = ArenaCache::new(
                arena,
                allocator,
                Arc::clone(&db),
                &tempdir.path().join("blobs"),
                Arc::clone(&dirty_paths),
            )?;

            let engine = Engine::new(
                arena,
                Arc::clone(&db),
                if with_root {
                    Some(cache.as_index())
                } else {
                    None
                },
                Arc::clone(&cache),
                cache.clone(),
                Arc::clone(&dirty_paths),
                cache.arena_root(),
                |attempt| {
                    if attempt < 3 {
                        Some(Duration::from_secs(1))
                    } else {
                        None
                    }
                },
            );
            let index = cache.as_index();
            let processor = StorageJobProcessor::new(
                Arc::clone(&db),
                Arc::clone(&engine),
                Arc::clone(&cache),
                if with_root {
                    Some((cache.as_index(), root.to_path_buf()))
                } else {
                    None
                },
            );

            let fixture = Self {
                arena,
                cache,
                index,
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
            self.index
                .add_file(&path, m.len(), UnixTime::mtime(&m), hash::digest(content))?;
            let entry = self.find_in_index(path_str)?.expect("{path_str} indexed");

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
                None,
            )?;

            Ok(())
        }

        /// Check if a file exists in the filesystem
        fn file_exists(&self, path: &str) -> bool {
            self.root.join(path).exists()
        }

        fn find_in_index(
            &self,
            path_str: &str,
        ) -> Result<Option<IndexedFileTableEntry>, StorageError> {
            self.index.get_file(&Path::parse(path_str)?)
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

        async fn unrealize(&self, path: Path, hash: Hash) -> anyhow::Result<JobStatus> {
            log::debug!("unrealize({path}, {hash})");
            let processor = Arc::clone(&self.processor);
            tokio::task::spawn_blocking(move || {
                let status = processor.unrealize(path, hash)?;

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
                fixture.processor.unrealize(path, hash),
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

    // #[tokio::test]
    // async fn unrealize_empty_file() -> anyhow::Result<()> {
    //     let fixture = Fixture::setup()?;

    //     let hash = fixture.create_indexed_file("test.txt", "").await?;
    //     fixture.add_to_cache("test.txt", &hash, 0)?;

    //     let path = Path::parse("test.txt")?;
    //     assert_eq!(JobStatus::Done, fixture.unrealize(path, hash).await?);

    //     assert!(fixture.find_in_index("test.txt")?.is_none());
    //     assert!(!fixture.file_exists("test.txt"));
    //     assert_eq!("", fixture.read_blob_content("test.txt").await?);

    //     Ok(())
    // }
}
