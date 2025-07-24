#![allow(dead_code)] // work in progress

use super::{download::download, progress::ByteCountProgress};
use crate::rpc::Household;
use futures::StreamExt;
use realize_storage::{Job, JobStatus, Storage};
use realize_types::Arena;
use std::sync::Arc;
use tarpc::tokio_util::sync::CancellationToken;
use tokio::{sync::broadcast, task::JoinHandle};

/// Notifications broadcast by [Churten].
#[derive(Debug, Clone, PartialEq)]
pub enum ChurtenNotification {
    /// Report the general job state.
    Update {
        arena: Arena,
        job: Arc<Job>,
        progress: JobProgress,
    },

    /// Report bytecount update, such as for a copy or a download job.
    ///
    /// Not all jobs emit such updates.
    UpdateByteCount {
        arena: Arena,
        job: Arc<Job>,

        /// Current number of bytes.
        ///
        /// The first such update normally, but not necessarily has
        /// current_bytes set to 0.
        ///
        /// This value normally but not necessarily increases.
        current_bytes: u64,

        /// Total (expected) number of bytes.
        ///
        /// This value is normally, but not necessarily stable.
        total_bytes: u64,
    },
}

impl ChurtenNotification {
    pub(crate) fn arena(&self) -> Arena {
        match self {
            ChurtenNotification::Update { arena, .. } => *arena,
            ChurtenNotification::UpdateByteCount { arena, .. } => *arena,
        }
    }
    pub(crate) fn job(&self) -> &Arc<Job> {
        match self {
            ChurtenNotification::Update { job, .. } => job,
            ChurtenNotification::UpdateByteCount { job, .. } => job,
        }
    }
}

/// Job progress reported by [ChurtenNotification]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobProgress {
    /// A job exists, but isn't running yet.
    Pending,

    /// The job is running.
    Running,

    /// The job was completed successfully.
    Done,

    /// The job was abandoned, likely because it is outdated.
    Abandoned,

    /// The job was cancelled by a call to [Churten::shutdown].
    Cancelled,

    /// The job failed. It may be retried.
    ///
    /// The string is an error description.
    Failed(String),
}

/// Bring the local store and peers closer together.
///
/// Maintains a background job that checks whatever needs to be done
/// and does it. Call [Churten::subscribe] to be notified of what
/// happens on that job.
struct Churten {
    storage: Arc<Storage>,
    handler: JobHandler,
    task: Option<(JoinHandle<()>, CancellationToken)>,
    tx: broadcast::Sender<ChurtenNotification>,
}

impl Churten {
    pub(crate) fn new(storage: Arc<Storage>, household: Household) -> Self {
        let handler = JobHandler::new(Arc::clone(&storage), household);
        let (tx, _) = broadcast::channel(16);

        Self {
            storage,
            handler,
            task: None,
            tx,
        }
    }

    /// Subscribe to [ChurtenNotification]s.
    pub fn subscribe(&self) -> broadcast::Receiver<ChurtenNotification> {
        self.tx.subscribe()
    }

    /// Check whether the background is running.
    pub fn is_running(&self) -> bool {
        self.task
            .as_ref()
            .map(|(handle, _)| !handle.is_finished())
            .unwrap_or(false)
    }

    /// Start the background jobs, unless they're already running.
    pub fn start(&mut self) {
        if self.task.is_some() {
            return;
        }
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn({
            let shutdown = shutdown.clone();
            let storage = Arc::clone(&self.storage);
            let handler = self.handler.clone();
            let tx = self.tx.clone();

            async move { background_job(&storage, &handler, tx, shutdown).await }
        });
        self.task = Some((handle, shutdown));
    }

    /// Shutdown the background jobs, but don't wait for them to finish.
    ///
    /// Does nothing if the jobs aren't running.
    pub fn shutdown(&mut self) {
        if let Some((_, shutdown)) = self.task.take() {
            shutdown.cancel();
        }
    }
}

/// Number of background jobs to run in parallel.
const PARALLEL_JOB_COUNT: usize = 4;

/// Process jobs from the job stream and report their result to [Storage].
///
/// Processing ends if the stream ends or if cancelled using the token.
async fn background_job(
    storage: &Arc<Storage>,
    handler: &JobHandler,
    tx: broadcast::Sender<ChurtenNotification>,
    shutdown: CancellationToken,
) {
    let mut result_stream = storage
        .job_stream()
        .map(|(arena, job)| {
            let job = Arc::new(job);
            let _ = tx.send(ChurtenNotification::Update {
                arena,
                job: Arc::clone(&job),
                progress: JobProgress::Pending,
            });

            (arena, job)
        })
        .map(|(arena, job)| run_job(handler, arena, job, &tx, shutdown.clone()))
        .buffer_unordered(PARALLEL_JOB_COUNT);
    while let Some((arena, job, status)) = tokio::select!(
        result = result_stream.next() => {
            result
        }
        _ = shutdown.cancelled() => { None })
    {
        let _ = tx.send(ChurtenNotification::Update {
            arena,
            job: Arc::clone(&job),
            progress: match &status {
                Ok(JobStatus::Done) => JobProgress::Done,
                Ok(JobStatus::Abandoned) => JobProgress::Abandoned,
                Err(err) => {
                    if shutdown.is_cancelled() {
                        JobProgress::Cancelled
                    } else {
                        JobProgress::Failed(err.to_string())
                    }
                }
            },
        });
        if let Err(err) = storage.job_finished(arena, &job, status) {
            // We don't want to interrupt job processing, even in this case.
            log::warn!("[{arena}] failed to report status of {job:?}: {err}");
        }
    }
}

async fn run_job(
    handler: &JobHandler,
    arena: Arena,
    job: Arc<Job>,
    tx: &broadcast::Sender<ChurtenNotification>,
    shutdown: CancellationToken,
) -> (Arena, Arc<Job>, anyhow::Result<JobStatus>) {
    log::debug!("[{arena}] STARTING: {job:?}");
    let _ = tx.send(ChurtenNotification::Update {
        arena,
        job: job.clone(),
        progress: JobProgress::Running,
    });
    let result = handler.run(arena, &job, tx.clone(), shutdown).await;

    (arena, job, result)
}

#[derive(Clone)]
struct JobHandler {
    storage: Arc<Storage>,
    household: Household,
}

impl JobHandler {
    fn new(storage: Arc<Storage>, household: Household) -> Self {
        Self { storage, household }
    }

    async fn run(
        &self,
        arena: Arena,
        job: &Arc<Job>,
        tx: broadcast::Sender<ChurtenNotification>,
        shutdown: CancellationToken,
    ) -> anyhow::Result<JobStatus> {
        let mut progress = TxByteCountProgress {
            tx,
            arena,
            job: Arc::clone(job),
        };
        match &**job {
            Job::Download(path, _, hash) => {
                download(
                    &self.storage,
                    &self.household,
                    arena,
                    path,
                    hash,
                    &mut progress,
                    shutdown,
                )
                .await
            }
        }
    }
}

struct TxByteCountProgress {
    tx: broadcast::Sender<ChurtenNotification>,
    arena: Arena,
    job: Arc<Job>,
}

impl ByteCountProgress for TxByteCountProgress {
    fn update(&mut self, current_bytes: u64, total_bytes: u64) {
        let _ = self.tx.send(ChurtenNotification::UpdateByteCount {
            arena: self.arena,
            job: Arc::clone(&self.job),
            current_bytes,
            total_bytes,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::testing::{self, HouseholdFixture};
    use realize_storage::Mark;
    use realize_storage::utils::hash::digest;
    use tokio::io::AsyncReadExt;

    struct Fixture {
        inner: HouseholdFixture,
    }

    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();
            let household_fixture = HouseholdFixture::setup().await?;

            Ok(Self {
                inner: household_fixture,
            })
        }
    }

    #[tokio::test]
    async fn churten_downloads_file() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                let arena = HouseholdFixture::test_arena();
                let storage = fixture.inner.storage(a)?;
                testing::connect(&household_a, b).await?;

                let mut churten = Churten::new(Arc::clone(&storage), household_a.clone());
                let mut rx = churten.subscribe();
                churten.start();

                storage.set_arena_mark(arena, Mark::Keep).await?;
                let foo = fixture.inner.write_file(b, "foo", "this is foo").await?;
                let hash = digest("this is foo");
                let job = Arc::new(Job::Download(foo, 1, hash));
                assert_eq!(
                    ChurtenNotification::Update {
                        arena,
                        job: job.clone(),
                        progress: JobProgress::Pending,
                    },
                    rx.recv().await?
                );

                assert_eq!(
                    ChurtenNotification::Update {
                        arena,
                        job: job.clone(),
                        progress: JobProgress::Running,
                    },
                    rx.recv().await?
                );

                assert_eq!(
                    ChurtenNotification::UpdateByteCount {
                        arena,
                        job: job.clone(),
                        current_bytes: 0,
                        total_bytes: 11,
                    },
                    rx.recv().await?
                );

                assert_eq!(
                    ChurtenNotification::UpdateByteCount {
                        arena,
                        job: job.clone(),
                        current_bytes: 11,
                        total_bytes: 11,
                    },
                    rx.recv().await?
                );

                assert_eq!(
                    ChurtenNotification::Update {
                        arena,
                        job: job.clone(),
                        progress: JobProgress::Done,
                    },
                    rx.recv().await?
                );

                let mut blob = fixture.inner.open_file(a, "foo").await?;
                let mut buf = String::new();
                blob.read_to_string(&mut buf).await?;
                assert_eq!("this is foo", buf);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    // TODO: Make Churten testable and add more tests
}
