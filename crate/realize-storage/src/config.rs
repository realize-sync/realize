use realize_types::Arena;
use std::collections::HashMap;
use std::path::PathBuf;

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

/// For per-arena cache (blob_dir required)
#[derive(Clone, serde::Deserialize, serde::Serialize, Debug)]
pub struct ArenaCacheConfig {
    /// Path to the cache database.
    pub db: PathBuf,
    /// Path to the directory where blob files are stored (required)
    pub blob_dir: PathBuf,
}

#[derive(Clone, serde::Deserialize, serde::Serialize, Debug)]
pub struct ArenaConfig {
    /// Local path to the directory where files for that arena are stored.
    pub path: PathBuf,
    pub index: Option<IndexConfig>,
    /// Optional path to the cache database for this arena.
    pub cache: Option<ArenaCacheConfig>,
}

#[derive(Clone, serde::Deserialize, serde::Serialize, Debug)]
pub struct IndexConfig {
    pub db: PathBuf,
}
