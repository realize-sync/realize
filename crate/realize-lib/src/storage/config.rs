use std::{collections::HashMap, path::PathBuf};

use crate::model::Arena;

/// Storage configuration.
#[derive(Clone, serde::Deserialize, Debug)]
pub struct StorageConfig {
    pub arenas: HashMap<Arena, ArenaConfig>,
    pub cache: Option<CacheConfig>,
}

/// Define an Arena available locally.
///
/// An arena is identified by [crate::model::Arena].
#[derive(Clone, serde::Deserialize, Debug)]
pub struct ArenaConfig {
    /// Local path to the directory where files for that arena are
    /// stored.
    ///
    /// That directory must be writable by the current user.
    pub path: PathBuf,
}

#[derive(Clone, serde::Deserialize, Debug)]
pub struct CacheConfig {
    /// Path to the cache database.
    pub db: PathBuf,
}
