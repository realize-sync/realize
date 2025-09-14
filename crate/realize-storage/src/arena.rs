use crate::PathIdAllocator;
use crate::config::{self, HumanDuration};
use crate::utils::redb_utils;
use anyhow::Context;
use db::ArenaDatabase;
use engine::Engine;
use fs::ArenaFilesystem;
use realize_types::Arena;
use std::time::Duration;
use std::{path::PathBuf, sync::Arc};
use tokio_util::sync::CancellationToken;
use tokio_util::sync::DropGuard;
use watcher::RealWatcher;

pub mod blob;
pub mod cache;
mod cleaner;
pub mod db;
mod dirty;
pub mod engine;
pub mod fs;
mod history;
pub mod index;
pub mod indexed_store;
mod jobs;
pub mod mark;
pub mod notifier;
mod peer;
mod tree;
pub mod types;
mod update;
pub mod watcher;

/// Gives access to arena-specific stores and functions.
pub(crate) struct ArenaStorage {
    pub(crate) db: Arc<ArenaDatabase>,
    pub(crate) fs: Arc<ArenaFilesystem>,
    pub(crate) engine: Arc<Engine>,
    pub(crate) datadir: PathBuf,
    _watcher: RealWatcher,
    _drop_guard: DropGuard,
}

impl ArenaStorage {
    pub(crate) async fn from_config(
        arena: Arena,
        arena_config: &config::ArenaConfig,
        exclude: &Vec<&std::path::Path>,
        allocator: &Arc<PathIdAllocator>,
    ) -> anyhow::Result<Self> {
        let shutdown = CancellationToken::new();
        let dbpath = arena_config.workdir.join("arena.db");
        let datadir = &arena_config.datadir;
        log::debug!("[{arena}] Arena setup with database {dbpath:?} and datadir {datadir:?}");
        let db = ArenaDatabase::new(
            redb_utils::open(&dbpath).await?,
            arena,
            Arc::clone(allocator),
            &arena_config.workdir.join("blobs"),
        )
        .with_context(|| format!("[{arena}] Arena database in {dbpath:?}",))?;

        let arena_fs = ArenaFilesystem::new(arena, Arc::clone(&db), datadir)?;
        let exclude = exclude
            .iter()
            .filter_map(|p| realize_types::Path::from_real_path_in(p, &datadir))
            .collect::<Vec<_>>();
        log::info!(
            "[{arena}] Watching {datadir:?}{}",
            exclude
                .iter()
                .map(|p| format!(" -\"{p}\""))
                .collect::<Vec<_>>()
                .join(",")
        );
        let watcher = RealWatcher::builder(datadir, Arc::clone(&db))
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
            .with_context(|| format!("{datadir:?}"))?;
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
            async move { blob::mark_accessed_loop(db, Duration::from_millis(500), shutdown).await }
        });

        let engine = Engine::new(arena, Arc::clone(&db), job_retry_strategy);

        jobs::StorageJobProcessor::new(Arc::clone(&db), Arc::clone(&engine), datadir.clone())
            .spawn(shutdown.clone());

        Ok(ArenaStorage {
            db,
            fs: Arc::clone(&arena_fs),
            engine,
            datadir: datadir.clone(),
            _watcher: watcher,
            _drop_guard: shutdown.drop_guard(),
        })
    }
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
