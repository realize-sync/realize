use realize_types::Arena;
use std::collections::HashMap;
use std::path::PathBuf;

/// Storage configuration.
#[derive(Clone, serde::Deserialize, serde::Serialize, Debug)]
pub struct StorageConfig {
    pub arenas: HashMap<Arena, ArenaConfig>,
    pub cache: CacheConfig,
}

impl StorageConfig {
    pub fn new<P>(cache_db: P) -> Self
    where
        P: AsRef<std::path::Path>,
    {
        StorageConfig {
            arenas: HashMap::new(),
            cache: CacheConfig {
                db: cache_db.as_ref().to_path_buf(),
            },
        }
    }
}

/// For the global cache (no blob_dir)
#[derive(Clone, serde::Deserialize, serde::Serialize, Debug)]
pub struct CacheConfig {
    /// Path to the cache database.
    pub db: PathBuf,
}

impl CacheConfig {
    pub fn new<P>(db: P) -> Self
    where
        P: AsRef<std::path::Path>,
    {
        Self {
            db: db.as_ref().to_path_buf(),
        }
    }
}

#[derive(Clone, serde::Deserialize, serde::Serialize, Debug, Default)]
pub struct ArenaConfig {
    /// Optional local path to the directory where files for that arena are stored.
    /// If specified, an indexer will be created for this arena.
    pub root: Option<PathBuf>,
    /// Path to the database that contains both index and cache data (required for arena cache).
    pub db: PathBuf,
    /// Path to the directory where blob files are stored (required for arena cache).
    pub blob_dir: PathBuf,

    /// Maximum number of hashers running in parallel.
    ///
    /// Hashing is CPU intensive, so hashing several large files in
    /// parallel can become a problem. It's a good idea to limit
    /// parallelism to a fraction of the available cores.
    pub max_parallel_hashers: Option<usize>,

    /// Set debounce delay for hashing files. This allows some time for
    /// operations in progress to finish.
    pub debounce_secs: Option<u64>,

    /// Limits how much disk space will be used to store local copies
    /// of remote data.
    ///
    /// Note that it might not be possible to enforce this limitation:
    /// if the size of files to keep goes above that limit, those
    /// files are kept anyways.
    pub disk_usage: Option<DiskUsageLimits>,
}

impl ArenaConfig {
    pub fn new<P1, P2, P3>(root: P1, db: P2, blob_dir: P3) -> Self
    where
        P1: AsRef<std::path::Path>,
        P2: AsRef<std::path::Path>,
        P3: AsRef<std::path::Path>,
    {
        Self {
            root: Some(root.as_ref().to_path_buf()),
            db: db.as_ref().to_path_buf(),
            blob_dir: blob_dir.as_ref().to_path_buf(),
            ..Default::default()
        }
    }

    /// Configure an arena without local root folder.
    pub fn rootless<P1, P2>(db: P1, blob_dir: P2) -> Self
    where
        P1: AsRef<std::path::Path>,
        P2: AsRef<std::path::Path>,
    {
        Self {
            db: db.as_ref().to_path_buf(),
            blob_dir: blob_dir.as_ref().to_path_buf(),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct DiskUsageLimits {
    /// Try to use at most that many bytes or percent of disk.
    ///
    /// The cache can temporarily go above that value.
    pub max: BytesOrPercent,

    /// Reduce disk usage to keep at keep that many bytes or percent
    /// of the disk free on the disk.
    ///
    /// This is applied after the `max` value.
    pub leave: Option<BytesOrPercent>,
}

impl DiskUsageLimits {
    pub fn max_bytes(v: u64) -> DiskUsageLimits {
        Self {
            max: BytesOrPercent::Bytes(v),
            leave: None,
        }
    }
    pub fn max_percent(v: u32) -> DiskUsageLimits {
        Self {
            max: BytesOrPercent::Percent(v),
            leave: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum BytesOrPercent {
    Percent(u32),
    Bytes(u64),
}
