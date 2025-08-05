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
    pub fn new(cache_db: PathBuf) -> Self {
        StorageConfig {
            arenas: HashMap::new(),
            cache: CacheConfig { db: cache_db },
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
    pub fn new(db: PathBuf) -> Self {
        Self { db }
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
}

impl ArenaConfig {
    pub fn new(root: PathBuf, db: PathBuf, blob_dir: PathBuf) -> Self {
        Self {
            root: Some(root),
            db,
            blob_dir,
            ..Default::default()
        }
    }

    /// Configure an arena without local root folder.
    pub fn rootless(db: PathBuf, blob_dir: PathBuf) -> Self {
        Self {
            db,
            blob_dir,
            ..Default::default()
        }
    }
}
