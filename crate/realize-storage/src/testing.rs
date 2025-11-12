use crate::config::WatcherConfig;

use super::Storage;
use super::config::{CacheConfig, HumanDuration, StorageConfig};
use realize_types::Arena;
use std::sync::Arc;
use std::time::Duration;

/// Build a storage with a cache and indexes for the given arenas.
///
/// The database and arena roots are put into the provided directory.
/// Use [arena_root] to get the root path of a specific arena.
pub async fn storage<T, P>(dir: P, arenas: T) -> anyhow::Result<Arc<Storage>>
where
    T: IntoIterator<Item = Arena>,
    P: AsRef<std::path::Path>,
{
    let dir = dir.as_ref();
    let config = config(dir);
    std::fs::create_dir_all(dir)?;
    let storage = Storage::from_config(&config).await?;
    for arena in arenas {
        let datadir = arena_root(dir, arena);
        std::fs::create_dir_all(&datadir)?;
        storage.create_arena(arena, &datadir).await?;
    }
    Ok(storage)
}

/// Create a test configuration with the given arenas.
///
/// The database and arena roots are put into the provided directory.
/// Use [arena_root] to get the root path of a specific arena.
pub fn config<P>(dir: P) -> StorageConfig
where
    P: AsRef<std::path::Path>,
{
    StorageConfig {
        arenas: vec![],
        cache: CacheConfig {
            db: dir.as_ref().join("cache.db"),
        },
        watcher: WatcherConfig {
            // Disabled in tests
            debounce: Some(HumanDuration(Duration::ZERO)),
            max_parallel_hashers: Some(0),
        },
    }
}

/// Returns a directory in the given dir to store the files of the
/// given arena.
///
/// This is used by [storage].
pub fn arena_root<P>(root: P, arena: Arena) -> std::path::PathBuf
where
    P: AsRef<std::path::Path>,
{
    let root = root.as_ref();
    root.join(arena.as_str()).join("data")
}
