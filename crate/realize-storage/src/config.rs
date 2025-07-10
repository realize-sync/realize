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

/// Define an Arena available locally.
///
/// An arena is identified by [realize_types::Arena].
#[derive(Clone, serde::Deserialize, serde::Serialize, Debug)]
pub struct ArenaConfig {
    /// Local path to the directory where files for that arena are
    /// stored.
    ///
    /// That directory must be writable by the current user.
    pub path: PathBuf,

    /// Configures the local index.
    pub index: Option<IndexConfig>,
}

#[derive(Clone, serde::Deserialize, serde::Serialize, Debug)]
pub struct CacheConfig {
    /// Path to the cache database.
    pub db: PathBuf,
}

#[derive(Clone, serde::Deserialize, serde::Serialize, Debug)]
pub struct IndexConfig {
    /// Path to the index database.
    pub db: PathBuf,
}
