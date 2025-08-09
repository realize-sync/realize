use crate::InodeAllocator;
use crate::StorageError;
use crate::config;
use crate::utils::redb_utils;
use arena_cache::ArenaCache;
use db::ArenaDatabase;
use engine::{DirtyPaths, Engine};
use index::RealIndexAsync;
use mark::PathMarks;
use realize_types::{Arena, Hash};
use std::time::Duration;
use std::{path::PathBuf, sync::Arc};
use tokio::task;
use watcher::RealWatcher;

pub mod arena_cache;
pub mod blob;
pub mod db;
pub mod engine;
pub mod hasher;
pub mod index;
pub mod indexed_store;
pub mod mark;
pub mod notifier;
pub mod types;
pub mod watcher;

/// Gives access to arena-specific stores and functions.
pub(crate) struct ArenaStorage {
    pub(crate) arena: Arena,
    pub(crate) db: Arc<ArenaDatabase>,
    pub(crate) cache: Arc<ArenaCache>,
    pub(crate) engine: Arc<Engine>,
    pub(crate) indexed: Option<IndexedArenaStorage>,
}

/// Indexed (FS-based) local storage.
pub(crate) struct IndexedArenaStorage {
    pub(crate) root: PathBuf,
    pub(crate) index: RealIndexAsync,
    _watcher: RealWatcher,
}

impl ArenaStorage {
    pub(crate) async fn from_config(
        arena: Arena,
        arena_config: &config::ArenaConfig,
        exclude: &Vec<&std::path::Path>,
        allocator: &Arc<InodeAllocator>,
    ) -> anyhow::Result<Self> {
        let db = ArenaDatabase::new(redb_utils::open(&arena_config.db).await?)?;
        let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;
        let arena_cache = ArenaCache::new(
            arena,
            Arc::clone(allocator),
            Arc::clone(&db),
            &arena_config.blob_dir,
            Arc::clone(&dirty_paths),
        )?;
        let indexed = match arena_config.root.as_ref() {
            None => None,
            Some(root) => {
                let index = RealIndexAsync::new(arena_cache.as_index());
                let exclude = exclude
                    .iter()
                    .filter_map(|p| realize_types::Path::from_real_path_in(p, root))
                    .collect::<Vec<_>>();
                log::debug!("Watch {root:?}, excluding {exclude:?}");
                let watcher = RealWatcher::builder(root, index.clone())
                    .with_initial_scan()
                    .exclude_all(exclude.iter())
                    .debounce(Duration::from_secs(arena_config.debounce_secs.unwrap_or(3)))
                    .max_parallel_hashers(arena_config.max_parallel_hashers.unwrap_or(4))
                    .spawn()
                    .await?;

                Some(IndexedArenaStorage {
                    root: root.to_path_buf(),
                    index,
                    _watcher: watcher,
                })
            }
        };
        let arena_root = arena_cache.arena_root();
        let engine = Engine::new(
            arena,
            Arc::clone(&db),
            indexed.as_ref().map(|indexed| indexed.index.blocking()),
            arena_cache.clone() as Arc<dyn PathMarks>,
            Arc::clone(&dirty_paths),
            arena_root,
            job_retry_strategy,
        );

        Ok(ArenaStorage {
            arena,
            db,
            cache: Arc::clone(&arena_cache),
            engine,
            indexed,
        })
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
    pub(crate) async fn realize(
        &self,
        path: &realize_types::Path,
        cache_hash: &Hash,
        index_hash: Option<&Hash>,
    ) -> Result<bool, StorageError> {
        let indexed = match &self.indexed {
            None => return Err(StorageError::NoLocalStorage(self.arena)),
            Some(indexed) => indexed,
        };

        let cache = self.cache.clone();
        let root = indexed.root.clone();
        let path = path.clone();
        let cache_hash = cache_hash.clone();
        let index_hash = index_hash.cloned();
        let db = Arc::clone(&self.db);
        let index = indexed.index.blocking();
        let done = task::spawn_blocking(move || {
            let txn = db.begin_write()?;
            if let Some(realpath) =
                index.get_indexed_file_txn(&txn, &root, &path, index_hash.as_ref())?
            {
                if cache.move_blob_if_matches(&txn, &path, &cache_hash, &realpath)? {
                    txn.commit()?;
                    return Ok(true);
                }
            }

            Ok::<bool, StorageError>(false)
        })
        .await??;

        Ok(done)
    }

    /// Move a file from the filesystem to the cache.
    ///
    /// Gives up and returns false if the current versions in the
    /// cache or the current version in the index don't match `hash`.
    pub(crate) async fn unrealize(
        &self,
        path: &realize_types::Path,
        hash: &Hash,
    ) -> Result<bool, StorageError> {
        let indexed = match &self.indexed {
            None => return Err(StorageError::NoLocalStorage(self.arena)),
            Some(indexed) => indexed,
        };

        let arena = self.arena;
        let cache = self.cache.clone();
        let index = indexed.index.blocking();
        let root = indexed.root.clone();
        let path = path.clone();
        let hash = hash.clone();
        let db = Arc::clone(&self.db);
        let done = task::spawn_blocking(move || {
            let txn = db.begin_write()?;
            if let Some(realpath) = index.get_indexed_file_txn(&txn, &root, &path, Some(&hash))? {
                if let Some(cachepath) =
                    cache.move_into_blob_if_matches(&txn, &path, &hash, &realpath.metadata()?)?
                {
                    // drop_file_if_matches makes a second check of
                    // the file mtime and size just before renaming,
                    // in case it has changed.

                    // Even with that, it's *still* possible some
                    // handle is open and is going to write on the
                    // file after the move, a change that would
                    // eventually be lost when the file in the cache
                    // is verified.
                    if index.drop_file_if_matches(&txn, &root, &path, &hash)? {
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
                            arena
                        );

                        return Ok(true);
                    }
                }
            }

            Ok::<bool, StorageError>(false)
        })
        .await??;

        Ok(done)
    }
}

/// Minimum wait time after a failed job.
const JOB_RETRY_BASE_SECS: u64 = 15;

/// Max is less than one day, so we retry at different time of day.
const MAX_JOB_RETRY_SECS: u64 = 18 * 3600;

/// Exponential backoff, starting with [JOB_RETRY_TIME_BASE] with a
/// max of [MAX_JOB_RETRY_DURATION].
///
/// TODO: make that configurable in ArenaConfig.
fn job_retry_strategy(attempt: u32) -> Option<Duration> {
    if attempt == 0 {
        return Some(Duration::ZERO);
    }

    if let Some(secs) = 2u32
        .checked_pow(attempt - 1)
        .map(|pow| (pow as u64).checked_mul(JOB_RETRY_BASE_SECS))
        .flatten()
    {
        if secs < MAX_JOB_RETRY_SECS {
            return Some(Duration::from_secs(secs));
        }
    }

    Some(Duration::from_secs(MAX_JOB_RETRY_SECS))
}

#[cfg(test)]
mod tests {
    use super::index::RealIndex;
    use super::types::IndexedFileTableEntry;
    use super::*;
    use crate::config::ArenaConfig;
    use crate::utils::hash;
    use crate::{Blob, GlobalDatabase, Inode, Notification};
    use assert_fs::TempDir;
    use assert_fs::fixture::ChildPath;
    use assert_fs::prelude::*;
    use realize_types::{Path, Peer, UnixTime};
    use tokio::io::AsyncReadExt;

    struct Fixture {
        arena: Arena,
        storage: ArenaStorage,
        root: ChildPath,
        _tempdir: TempDir,
    }

    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            Self::setup_internal(true).await
        }

        async fn setup_cache_only() -> anyhow::Result<Self> {
            Self::setup_internal(false).await
        }

        /// Call setup or setup_cache_only
        async fn setup_internal(with_root: bool) -> anyhow::Result<Self> {
            let _ = env_logger::try_init();
            let arena = Arena::from("myarena");
            let tempdir = TempDir::new()?;
            let globaldb = GlobalDatabase::new(redb_utils::in_memory()?)?;
            let allocator = InodeAllocator::new(globaldb, [arena])?;

            let root = tempdir.child("root");
            root.create_dir_all()?;
            let config = ArenaConfig {
                db: tempdir.child("arena.db").to_path_buf(),
                blob_dir: tempdir.child("blobs").to_path_buf(),
                root: if with_root {
                    Some(root.to_path_buf())
                } else {
                    None
                },
                debounce_secs: Some(0),
                max_parallel_hashers: Some(0),
            };
            let storage = ArenaStorage::from_config(arena, &config, &vec![], &allocator).await?;

            Ok(Self {
                arena,
                storage,
                root,
                _tempdir: tempdir,
            })
        }

        fn index(&self) -> Arc<dyn RealIndex> {
            self.storage
                .indexed
                .as_ref()
                .expect("indexed")
                .index
                .blocking()
        }

        /// Create a test file in the filesystem and wait for it to be
        /// added to the index.
        async fn create_indexed_file(&self, path_str: &str, content: &str) -> anyhow::Result<Hash> {
            let mut watch = self.index().watch_history();
            self.root.child(path_str).write_str(content)?;
            watch.changed().await?;

            let entry = self.find_in_index(path_str)?.expect("{path_str} indexed");

            Ok(entry.hash)
        }

        fn add_to_cache(&self, path_str: &str, hash: &Hash, size: u64) -> anyhow::Result<()> {
            self.storage.cache.update(
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
            self.index().get_file(&Path::parse(path_str)?)
        }

        fn find_in_cache(&self, path_str: &str) -> Result<Option<Inode>, StorageError> {
            match self.storage.cache.lookup_path(&Path::parse(path_str)?) {
                Ok((inode, _)) => Ok(Some(inode)),
                Err(StorageError::NotFound) => Ok(None),
                Err(err) => Err(err),
            }
        }

        fn open_blob(&self, path_str: &str) -> anyhow::Result<Blob> {
            let inode = self.find_in_cache(path_str)?.expect("{path_str} in cache");

            Ok(self.storage.cache.open_file(inode)?)
        }

        async fn read_blob_content(&self, path_str: &str) -> anyhow::Result<String> {
            let mut blob = self.open_blob(path_str)?;
            let mut buf = String::new();
            blob.read_to_string(&mut buf).await?;

            Ok(buf)
        }
    }

    #[tokio::test]
    async fn unrealize_success() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let hash = fixture.create_indexed_file("test.txt", "foobar").await?;
        fixture.add_to_cache("test.txt", &hash, 6)?;

        let path = Path::parse("test.txt")?;
        assert!(fixture.storage.unrealize(&path, &hash).await?);

        assert!(fixture.find_in_index("test.txt")?.is_none());
        assert!(!fixture.file_exists("test.txt"));
        assert_eq!("foobar", fixture.read_blob_content("test.txt").await?);

        Ok(())
    }

    #[tokio::test]
    async fn unrealize_no_local_storage() -> anyhow::Result<()> {
        let fixture = Fixture::setup_cache_only().await?;

        let hash = hash::digest("foobar");
        fixture.add_to_cache("test.txt", &hash, 6)?;

        let path = Path::parse("test.txt")?;
        assert!(matches!(
                fixture.storage.unrealize(&path, &hash).await,
                Err(StorageError::NoLocalStorage(a)) if a == fixture.arena));

        Ok(())
    }

    #[tokio::test]
    async fn unrealize_not_in_cache() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let hash = fixture.create_indexed_file("test.txt", "foobar").await?;

        let path = Path::parse("test.txt")?;
        assert!(!fixture.storage.unrealize(&path, &hash).await?);

        Ok(())
    }

    #[tokio::test]
    async fn unrealize_wrong_hash_in_cache() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let hash = fixture.create_indexed_file("test.txt", "foobar").await?;
        fixture.add_to_cache("test.txt", &hash::digest("something else"), 6)?;

        let path = Path::parse("test.txt")?;
        assert!(!fixture.storage.unrealize(&path, &hash).await?);

        Ok(())
    }

    #[tokio::test]
    async fn unrealize_empty_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let hash = fixture.create_indexed_file("test.txt", "").await?;
        fixture.add_to_cache("test.txt", &hash, 0)?;

        let path = Path::parse("test.txt")?;
        assert!(fixture.storage.unrealize(&path, &hash).await?);

        assert!(fixture.find_in_index("test.txt")?.is_none());
        assert!(!fixture.file_exists("test.txt"));
        assert_eq!("", fixture.read_blob_content("test.txt").await?);

        Ok(())
    }

    #[test]
    fn hardcoded_retry_strategy() -> anyhow::Result<()> {
        assert_eq!(Some(Duration::ZERO), job_retry_strategy(0));
        assert_eq!(Some(Duration::from_secs(15)), job_retry_strategy(1));
        assert_eq!(Some(Duration::from_secs(30)), job_retry_strategy(2));
        assert_eq!(Some(Duration::from_secs(60)), job_retry_strategy(3));
        assert_eq!(Some(Duration::from_secs(120)), job_retry_strategy(4));
        assert_eq!(
            Some(Duration::from_secs(18 * 60 * 60)), // 18h
            job_retry_strategy(20)
        );
        assert_eq!(
            Some(Duration::from_secs(18 * 60 * 60)), // 18h
            job_retry_strategy(9999)                 // overflow
        );

        Ok(())
    }
}
