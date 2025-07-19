use crate::utils::redb_utils;

use super::config::{ArenaConfig, CacheConfig, StorageConfig};
use super::{Storage, UnrealCacheAsync};
use realize_types::Arena;
use std::sync::Arc;
use tokio::fs;

pub async fn in_memory_cache<T>(arenas: T) -> anyhow::Result<UnrealCacheAsync>
where
    T: IntoIterator<Item = Arena> + Send + 'static,
{
    let mut arena_dbs = vec![];
    for arena in arenas.into_iter() {
        arena_dbs.push((
            arena,
            redb_utils::in_memory()?,
            std::path::PathBuf::from("/dev/null"),
        ));
    }
    let cache = UnrealCacheAsync::with_db(redb_utils::in_memory()?, arena_dbs).await?;

    Ok(cache)
}

/// Build a storage with a cache and indexes for the given arenas.
///
/// The database and arena roots are put into the provided directory.
/// Use [arena_root] to get the root path of a specific arena.
pub async fn storage<T>(dir: &std::path::Path, arenas: T) -> anyhow::Result<Arc<Storage>>
where
    T: IntoIterator<Item = Arena>,
{
    let config = StorageConfig {
        arenas: arenas
            .into_iter()
            .map(|arena| {
                let arena_dir = arena_root(dir, arena);
                (
                    arena,
                    ArenaConfig::new(
                        arena_dir.clone(),
                        arena_dir.join(".arena.db"),
                        arena_dir.join(".arena.blobs"),
                    ),
                )
            })
            .collect(),
        cache: CacheConfig {
            db: dir.join("cache.db"),
        },
    };

    for arena_config in config.arenas.values() {
        if let Some(root) = &arena_config.root {
            fs::create_dir_all(root).await?;
        }
        fs::create_dir_all(&arena_config.blob_dir).await?;
    }

    Storage::from_config(&config).await
}

/// Returns a directory in the given dir to store the files of the
/// given arena.
///
/// This is used by [storage].
pub fn arena_root(root: &std::path::Path, arena: Arena) -> std::path::PathBuf {
    root.join(arena.as_str())
}
