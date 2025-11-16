use crate::StorageError;
use crate::config::{self, HumanDuration};
use db::ArenaDatabase;
use engine::Engine;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_util::sync::DropGuard;
use tokio_util::task::TaskTracker;

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
    shutdown: CancellationToken,
    tasks: TaskTracker,

    /// Shutdown tasks if ArenaStorage is dropped. This is meant as an
    /// extra safety. [ArenaStorage] should normally be shutdown
    /// cleanly with [ArenaStorage::shutdown].
    _drop_guard: DropGuard,
}

impl ArenaStorage {
    pub(crate) async fn spawn(
        db: &Arc<ArenaDatabase>,
        watcher_config: &config::WatcherConfig,
    ) -> Result<Self, StorageError> {
        let db = Arc::clone(db);
        let shutdown = CancellationToken::new();
        let drop_guard = shutdown.clone().drop_guard();
        let tasks = TaskTracker::new();
        let tag = db.tag();
        let datadir = db.cache().datadir();
        log::info!("[{tag}] Watching {datadir:?}");

        watcher::builder(&db)
            .with_initial_scan()
            .debounce(
                watcher_config
                    .debounce
                    .clone()
                    .unwrap_or(HumanDuration(Duration::from_secs(3)))
                    .into(),
            )
            .max_parallel_hashers(watcher_config.max_parallel_hashers.unwrap_or(4))
            .spawn(shutdown.clone(), tasks.clone())
            .await?;
        tasks.spawn({
            let db = Arc::clone(&db);
            let shutdown = shutdown.clone();

            async move { cleaner::run_loop(db, shutdown).await }
        });
        tasks.spawn({
            let db = Arc::clone(&db);
            let shutdown = shutdown.clone();
            async move { blob::mark_accessed_loop(db, Duration::from_millis(500), shutdown).await }
        });

        let engine = Engine::new(Arc::clone(&db), job_retry_strategy);
        tasks.spawn({
            let processor =
                jobs::StorageJobProcessor::new(Arc::clone(&db), Arc::clone(&engine), tasks.clone());
            let shutdown = shutdown.clone();
            async move { processor.process_jobs(shutdown).await }
        });

        Ok(ArenaStorage {
            db,
            engine,
            tasks,
            shutdown,
            _drop_guard: drop_guard,
        })
    }

    /// Shutdown any tasks working on the arena.
    ///
    /// This function doesn't wait for the shutdown to actually
    /// happen. Call [ArenaStorage::closed] for that.
    #[allow(dead_code)]
    pub(crate) fn shutdown(&self) {
        self.shutdown.cancel();
        self.tasks.close();
    }

    /// Wait for all tasks working on the arena to be shut down.
    #[allow(dead_code)]
    pub(crate) async fn closed(&self) {
        self.tasks.wait().await
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
    use assert_fs::TempDir;
    use realize_types::Arena;

    use crate::config::WatcherConfig;

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

    #[tokio::test]
    async fn shutdown() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        let tempdir = TempDir::new()?;
        let arena = Arena::from("myarena");
        let db = db::ArenaDatabase::for_testing(arena, tempdir.path())?;
        assert_eq!(1, Arc::strong_count(&db));

        let storage = ArenaStorage::spawn(&db, &WatcherConfig::default()).await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        storage.shutdown();

        // make sure that all spawned tasks end after a shutdown
        tokio::time::timeout(Duration::from_secs(15), storage.closed()).await?;
        drop(storage);

        // make sure there isn't some leftover background task holding
        // on to the database.
        assert_eq!(1, Arc::strong_count(&db));
        assert!(Arc::try_unwrap(db).is_ok());
        Ok(())
    }
}
