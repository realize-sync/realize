use crate::InodeAllocator;
use crate::config::{self, HumanDuration};
use crate::utils::redb_utils;
use anyhow::Context;
use arena_cache::ArenaCache;
use db::ArenaDatabase;
use engine::Engine;
use index::RealIndexAsync;
use realize_types::Arena;
use std::time::Duration;
use std::{path::PathBuf, sync::Arc};
use tokio_util::sync::CancellationToken;
use tokio_util::sync::DropGuard;
use watcher::RealWatcher;

pub mod arena_cache;
pub mod blob;
mod cleaner;
pub mod db;
mod dirty;
pub mod engine;
pub mod hasher;
mod history;
pub mod index;
pub mod indexed_store;
mod jobs;
pub mod mark;
pub mod notifier;
mod peer;
mod tree;
pub mod types;
pub mod watcher;

/// Gives access to arena-specific stores and functions.
pub(crate) struct ArenaStorage {
    pub(crate) db: Arc<ArenaDatabase>,
    pub(crate) cache: Arc<ArenaCache>,
    pub(crate) engine: Arc<Engine>,
    pub(crate) indexed: Option<IndexedArenaStorage>,
    _drop_guard: DropGuard,
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
        let shutdown = CancellationToken::new();
        let db = create_db(arena, arena_config, allocator)
            .await
            .with_context(|| format!("database {:?}", arena_config.db))?;
        let arena_cache = ArenaCache::new(arena, Arc::clone(&db), &arena_config.blob_dir)?;
        let indexed = match arena_config.root.as_ref() {
            None => {
                log::info!("[{arena}] Arena setup for caching");

                None
            }
            Some(root) => {
                log::info!("[{arena}] Arena setup with root {root:?}");

                let index = RealIndexAsync::new(Arc::clone(&db));
                let exclude = exclude
                    .iter()
                    .filter_map(|p| realize_types::Path::from_real_path_in(p, root))
                    .collect::<Vec<_>>();
                log::info!(
                    "[{arena}] Watching {root:?}{}",
                    exclude
                        .iter()
                        .map(|p| format!(" -\"{p}\""))
                        .collect::<Vec<_>>()
                        .join(",")
                );
                let watcher = RealWatcher::builder(root, index.clone())
                    .with_initial_scan()
                    .exclude_all(exclude.iter())
                    .debounce(
                        arena_config
                            .debounce
                            .clone()
                            .unwrap_or(HumanDuration(Duration::from_secs(3)))
                            .into(),
                    )
                    .max_parallel_hashers(arena_config.max_parallel_hashers.unwrap_or(4))
                    .spawn()
                    .await
                    .with_context(|| format!("{root:?}"))?;
                if let Some(limits) = &arena_config.disk_usage {
                    tokio::spawn({
                        let db = Arc::clone(&db);
                        let shutdown = shutdown.clone();
                        let limits = limits.clone();

                        async move { cleaner::run_loop(db, limits, shutdown).await }
                    });
                }
                tokio::spawn({
                    let db = Arc::clone(&db);
                    let shutdown = shutdown.clone();
                    async move {
                        blob::mark_accessed_loop(db, Duration::from_millis(500), shutdown).await
                    }
                });

                Some(IndexedArenaStorage {
                    root: root.to_path_buf(),
                    index,
                    _watcher: watcher,
                })
            }
        };
        let engine = Engine::new(arena, Arc::clone(&db), job_retry_strategy);

        jobs::StorageJobProcessor::new(
            Arc::clone(&db),
            Arc::clone(&engine),
            indexed.as_ref().map(|indexed| indexed.root.to_path_buf()),
        )
        .spawn(shutdown.clone());

        Ok(ArenaStorage {
            db,
            cache: Arc::clone(&arena_cache),
            engine,
            indexed,
            _drop_guard: shutdown.drop_guard(),
        })
    }
}

async fn create_db(
    arena: Arena,
    arena_config: &config::ArenaConfig,
    allocator: &Arc<InodeAllocator>,
) -> anyhow::Result<Arc<ArenaDatabase>> {
    let db = ArenaDatabase::new(
        redb_utils::open(&arena_config.db).await?,
        arena,
        Arc::clone(allocator),
        &arena_config.blob_dir,
    )?;
    Ok(db)
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
    use super::*;

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
