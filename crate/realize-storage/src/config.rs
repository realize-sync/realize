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

#[derive(Clone, serde::Deserialize, serde::Serialize, Debug)]
pub struct ArenaConfig {
    /// Optional local path to the directory where files for that arena are stored.
    /// If specified, an indexer will be created for this arena.
    pub root: Option<PathBuf>,
    /// Path to the database that contains both index and cache data (required for arena cache).
    pub db: PathBuf,
    /// Path to the directory where blob files are stored (required for arena cache).
    pub blob_dir: PathBuf,
}

impl ArenaConfig {
    pub fn new(root: PathBuf, db: PathBuf, blob_dir: PathBuf) -> Self {
        Self {
            root: Some(root),
            db,
            blob_dir,
        }
    }

    /// Configure an arena without local root folder.
    pub fn rootless(db: PathBuf, blob_dir: PathBuf) -> Self {
        Self {
            root: None,
            db,
            blob_dir,
        }
    }
}
