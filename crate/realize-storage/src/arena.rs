use crate::config::{self, HumanDuration};
use anyhow::Context;
use db::ArenaDatabase;
use engine::Engine;
use std::sync::Arc;
use std::time::Duration;
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
mod settings;
mod tree;
pub mod types;
mod update;
pub mod watcher;
mod xattr;

/// Gives access to arena-specific stores and functions.
pub(crate) struct ArenaStorage {
    pub(crate) db: Arc<ArenaDatabase>,
    pub(crate) engine: Arc<Engine>,
    _watcher: RealWatcher,
    _drop_guard: DropGuard,
}

impl ArenaStorage {
    pub(crate) async fn with_db(
        db: Arc<ArenaDatabase>,
        watcher_config: &config::WatcherConfig,
    ) -> anyhow::Result<Self> {
        let shutdown = CancellationToken::new();
        let tag = db.tag();
        let datadir = db.cache().datadir();
        log::info!("[{tag}] Watching {datadir:?}");

        let watcher = RealWatcher::builder(Arc::clone(&db))
            .with_initial_scan()
            .debounce(
                watcher_config
                    .debounce
                    .clone()
                    .unwrap_or(HumanDuration(Duration::from_secs(3)))
                    .into(),
            )
            .max_parallel_hashers(watcher_config.max_parallel_hashers.unwrap_or(4))
            .spawn()
            .await
            .with_context(|| format!("{datadir:?}"))?;
        tokio::spawn({
            let db = Arc::clone(&db);
            let shutdown = shutdown.clone();

            async move { cleaner::run_loop(db, shutdown).await }
        });
        tokio::spawn({
            let db = Arc::clone(&db);
            let shutdown = shutdown.clone();
            async move { blob::mark_accessed_loop(db, Duration::from_millis(500), shutdown).await }
        });

        let engine = Engine::new(Arc::clone(&db), job_retry_strategy);

        jobs::StorageJobProcessor::new(Arc::clone(&db), Arc::clone(&engine))
            .spawn(shutdown.clone());

        Ok(ArenaStorage {
            db,
            engine,
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
