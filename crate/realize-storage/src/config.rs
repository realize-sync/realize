use realize_types::Arena;
use std::collections::HashMap;
use std::path::PathBuf;

use crate::mark::Mark;

/// Storage configuration.
#[derive(Clone, serde::Deserialize, serde::Serialize, Debug)]
pub struct StorageConfig {
    pub arenas: HashMap<Arena, ArenaConfig>,
    pub cache: Option<CacheConfig>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageConfig {
    pub fn new() -> Self {
        StorageConfig {
            arenas: HashMap::new(),
            cache: None,
        }
    }
}

/// For the global cache (no blob_dir)
#[derive(Clone, serde::Deserialize, serde::Serialize, Debug)]
pub struct CacheConfig {
    /// Path to the cache database.
    pub db: PathBuf,
}

#[derive(Clone, serde::Deserialize, serde::Serialize, Debug)]
pub struct ArenaConfig {
    /// Local path to the directory where files for that arena are stored.
    pub path: PathBuf,
    /// Optional path to the database that contains both index and cache data.
    pub db: Option<PathBuf>,
    /// Optional path to the directory where blob files are stored (required for cache functionality).
    pub blob_dir: Option<PathBuf>,
    /// The default mark for files and directories in this arena.
    #[serde(default)]
    pub mark: Mark,
}
