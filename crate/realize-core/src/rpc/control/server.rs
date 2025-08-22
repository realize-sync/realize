#![allow(dead_code)] // work in progress

use super::control_capnp;
use super::control_capnp::churten::{
    self, IsRunningParams, IsRunningResults, RecentJobsParams, RecentJobsResults, ShutdownParams,
    ShutdownResults, StartParams, StartResults, SubscribeParams, SubscribeResults,
};
use super::control_capnp::control::{
    self, ChurtenParams, ChurtenResults, GetMarkParams, GetMarkResults, SetArenaMarkParams,
    SetArenaMarkResults, SetMarkParams, SetMarkResults,
};
use super::convert;
use crate::consensus::churten::{Churten, JobHandler};
use capnp::capability::Promise;
use realize_storage::{Mark, Storage, StorageError};
use realize_types::{Arena, Hash, Path};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct ControlServer<H: JobHandler + 'static> {
    storage: Arc<Storage>,
    churten: Rc<RefCell<Churten<H>>>,
}

impl<H: JobHandler + 'static> ControlServer<H> {
    pub(crate) fn new(storage: Arc<Storage>, churten: Churten<H>) -> ControlServer<H> {
        Self {
            storage,
            churten: Rc::new(RefCell::new(churten)),
        }
    }

    pub(crate) fn into_client(self) -> control::Client {
        capnp_rpc::new_client(self)
    }
}

impl<H: JobHandler + 'static> control::Server for ControlServer<H> {
    fn churten(
        &mut self,
        _: ChurtenParams,
        mut results: ChurtenResults,
    ) -> Promise<(), capnp::Error> {
        results
            .get()
            .set_churten(capnp_rpc::new_client(ChurtenServer {
                churten: self.churten.clone(),
            }));

        Promise::ok(())
    }

    fn set_mark(&mut self, params: SetMarkParams, _: SetMarkResults) -> Promise<(), capnp::Error> {
        let storage = Arc::clone(&self.storage);
        Promise::from_future(async move {
            let req = params.get()?.get_req()?;
            let arena = parse_arena(req.get_arena()?)?;
            let path = parse_path(req.get_path()?)?;
            let mark = parse_mark(req.get_mark()?);

            storage
                .set_mark(arena, &path, mark)
                .await
                .map_err(from_storage_err)?;

            Ok(())
        })
    }

    fn set_arena_mark(
        &mut self,
        params: SetArenaMarkParams,
        _: SetArenaMarkResults,
    ) -> Promise<(), capnp::Error> {
        let storage = Arc::clone(&self.storage);
        Promise::from_future(async move {
            let req = params.get()?.get_req()?;
            let arena = parse_arena(req.get_arena()?)?;
            let mark = parse_mark(req.get_mark()?);

            storage
                .set_arena_mark(arena, mark)
                .await
                .map_err(from_storage_err)?;

            Ok(())
        })
    }

    fn get_mark(
        &mut self,
        params: GetMarkParams,
        mut results: GetMarkResults,
    ) -> Promise<(), capnp::Error> {
        let storage = Arc::clone(&self.storage);
        Promise::from_future(async move {
            let req = params.get()?.get_req()?;
            let arena = parse_arena(req.get_arena()?)?;
            let path = parse_path(req.get_path()?)?;

            let mark = storage
                .get_mark(arena, &path)
                .await
                .map_err(from_storage_err)?;

            let mut res = results.get().init_res();
            res.set_mark(mark_to_capnp(mark));
            Ok(())
        })
    }
}

#[derive(Clone)]
struct ChurtenServer<H: JobHandler + 'static> {
    churten: Rc<RefCell<Churten<H>>>,
}

impl<H: JobHandler + 'static> churten::Server for ChurtenServer<H> {
    fn subscribe(
        &mut self,
        params: SubscribeParams,
        _: SubscribeResults,
    ) -> Promise<(), capnp::Error> {
        let mut rx = self.churten.borrow().subscribe();
        let churten = self.churten.clone();

        Promise::from_future(async move {
            let subscriber = params.get()?.get_subscriber()?;

            // First send an initial set of jobs.
            send_active_jobs(&churten, &subscriber).await?;

            // Forward notifications from tx to the subscriber in the
            // background.
            //
            // This task remains as long as sending to the subscriber
            // succeeds and the channel hasn't been closed.
            tokio::task::spawn_local(async move {
                use tokio::sync::broadcast::error::RecvError;

                loop {
                    match rx.recv().await {
                        Ok(notification) => {
                            let mut request = subscriber.notify_request();
                            convert::fill_notification(
                                notification,
                                request.get().init_notification(),
                            );
                            if request.send().await.is_err() {
                                return;
                            }
                        }
                        Err(RecvError::Closed) => {
                            return;
                        }
                        Err(RecvError::Lagged(_)) => {
                            // Relieve some pressure on the channel by
                            // dropping events in the queue. The client
                            // rely on the set of active jobs to catch up.
                            rx.resubscribe();
                            if send_active_jobs(&churten, &subscriber).await.is_err() {
                                return;
                            }
                        }
                    };
                }
            });

            Ok::<_, capnp::Error>(())
        })
    }

    fn start(&mut self, _: StartParams, _: StartResults) -> Promise<(), capnp::Error> {
        self.churten.borrow_mut().start();

        Promise::ok(())
    }

    fn shutdown(&mut self, _: ShutdownParams, _: ShutdownResults) -> Promise<(), capnp::Error> {
        self.churten.borrow_mut().shutdown();

        Promise::ok(())
    }

    fn is_running(
        &mut self,
        _: IsRunningParams,
        mut results: IsRunningResults,
    ) -> Promise<(), capnp::Error> {
        results
            .get()
            .set_running(self.churten.borrow().is_running());

        Promise::ok(())
    }

    fn recent_jobs(
        &mut self,
        _: RecentJobsParams,
        mut results: RecentJobsResults,
    ) -> Promise<(), capnp::Error> {
        let churten = self.churten.clone();
        Promise::from_future(async move {
            let recent_jobs = churten.borrow().recent_jobs().await;

            let mut job_list = results.get().init_res(recent_jobs.len() as u32);
            for (i, job_info) in recent_jobs.into_iter().enumerate() {
                convert::fill_job_info(&job_info, job_list.reborrow().get(i as u32));
            }

            Ok(())
        })
    }
}

async fn send_active_jobs<H: JobHandler + 'static>(
    churten: &Rc<RefCell<Churten<H>>>,
    subscriber: &control_capnp::churten::subscriber::Client,
) -> Result<(), capnp::Error> {
    let jobs = churten.borrow().active_jobs().await;

    let mut request = subscriber.reset_request();
    let mut builder = request.get().init_jobs(jobs.len() as u32);
    for (i, job_info) in jobs.into_iter().enumerate() {
        convert::fill_job_info(&job_info, builder.reborrow().get(i as u32));
    }
    request.send().await?;

    Ok(())
}

/// Straightforward conversion from storage error to capnp error
/// that's just good enough to get started.
fn from_storage_err(err: StorageError) -> capnp::Error {
    capnp::Error::failed(format!("{err:?}"))
}

// These capnp parse_ functions are duplicates of these found in household.rs.
//
// TODO: consolidate these somewhere

fn parse_arena(reader: capnp::text::Reader<'_>) -> Result<Arena, capnp::Error> {
    Ok(Arena::from(reader.to_str()?))
}

fn parse_path(reader: capnp::text::Reader<'_>) -> Result<Path, capnp::Error> {
    Path::parse(reader.to_str()?).map_err(|e| capnp::Error::failed(e.to_string()))
}

fn parse_hash(hash: &[u8]) -> Result<Hash, capnp::Error> {
    let hash: [u8; 32] = hash
        .try_into()
        .map_err(|_| capnp::Error::failed("invalid hash".to_string()))?;

    Ok(Hash(hash))
}

fn parse_mark(mark: control_capnp::Mark) -> Mark {
    match mark {
        control_capnp::Mark::Own => Mark::Own,
        control_capnp::Mark::Watch => Mark::Watch,
        control_capnp::Mark::Keep => Mark::Keep,
    }
}

fn mark_to_capnp(mark: Mark) -> control_capnp::Mark {
    match mark {
        Mark::Own => control_capnp::Mark::Own,
        Mark::Watch => control_capnp::Mark::Watch,
        Mark::Keep => control_capnp::Mark::Keep,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::churten::{JobHandler, JobHandlerImpl};
    use crate::consensus::jobs::JobError;
    use crate::consensus::progress::{ByteCountProgress, TxByteCountProgress};
    use crate::consensus::types::{ChurtenNotification, JobAction, JobProgress};
    use crate::rpc::Household;
    use crate::rpc::control::client::{self, ChurtenUpdates, TxChurtenSubscriber};
    use crate::rpc::testing::HouseholdFixture;
    use assert_fs::TempDir;
    use realize_network::unixsocket;
    use realize_storage::{Job, JobId, JobStatus, Mark, Notification};
    use realize_types::{Peer, UnixTime};
    use std::path::PathBuf;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::task::LocalSet;
    use tokio_util::sync::CancellationToken;

    struct Fixture {
        inner: HouseholdFixture,
        tempdir: TempDir,
        shutdown: CancellationToken,
    }

    /// A fake JobHandler for testing different job outcomes through RPC
    #[derive(Clone)]
    struct FakeJobHandler {
        result_fn: Arc<dyn Fn() -> Result<JobStatus, JobError> + Send + Sync>,
        should_send_progress: bool,
    }

    impl FakeJobHandler {
        fn new<F>(result_fn: F) -> Self
        where
            F: Fn() -> Result<JobStatus, JobError> + Send + Sync + 'static,
        {
            Self {
                result_fn: Arc::new(result_fn),
                should_send_progress: false,
            }
        }

        fn with_progress(mut self, should_send: bool) -> Self {
            self.should_send_progress = should_send;
            self
        }
    }

    impl JobHandler for FakeJobHandler {
        async fn run(
            &self,
            _arena: Arena,
            _job: &Arc<realize_storage::Job>,
            progress: &mut TxByteCountProgress,
            shutdown: CancellationToken,
        ) -> Result<JobStatus, JobError> {
            if shutdown.is_cancelled() {
                return Ok(JobStatus::Cancelled);
            }

            // Send progress updates if requested
            if self.should_send_progress {
                progress.update_action(JobAction::Download);
                progress.update(50, 100);
                progress.update(100, 100);
            }

            // Return the configured result
            (self.result_fn)()
        }
    }

    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();
            let household_fixture = HouseholdFixture::setup().await?;
            let tempdir = TempDir::new()?;
            let shutdown = CancellationToken::new();

            Ok(Self {
                inner: household_fixture,
                tempdir,
                shutdown,
            })
        }

        async fn bind_server<H: JobHandler + 'static>(
            &self,
            local: &LocalSet,
            peer: Peer,
            household: Household,
            handler: H,
        ) -> anyhow::Result<PathBuf> {
            let storage = self.inner.storage(peer)?;
            let churten = Churten::with_handler(Arc::clone(storage), household, handler);
            let server = ControlServer::new(Arc::clone(storage), churten);

            let sockpath = self
                .tempdir
                .path()
                .join("realize/control.socket")
                .to_path_buf();
            unixsocket::bind(
                &local,
                &sockpath,
                move || server.clone().into_client().client,
                self.shutdown.clone(),
            )
            .await?;

            Ok(sockpath)
        }
    }

    #[tokio::test]
    async fn set_mark() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let arena = HouseholdFixture::test_arena();
        let peer = HouseholdFixture::a();
        let local = LocalSet::new();
        let household = fixture.inner.create_household(&local, peer)?;
        let storage = fixture.inner.storage(peer)?;
        let sockpath = fixture
            .bind_server(
                &local,
                peer,
                household.clone(),
                JobHandlerImpl::new(Arc::clone(storage), household.clone()),
            )
            .await?;
        let foo = Path::parse("foo")?;

        local
            .run_until(async move {
                let control: control::Client = unixsocket::connect(&sockpath).await?;

                let mut request = control.set_mark_request();
                let mut req = request.get().init_req();
                req.set_arena(arena.as_str());
                req.set_path("foo");
                req.set_mark(control_capnp::Mark::Keep);
                request.send().promise.await?;

                assert_eq!(Mark::Keep, storage.get_mark(arena, &foo).await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn set_arena_mark() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let arena = HouseholdFixture::test_arena();
        let peer = HouseholdFixture::a();
        let local = LocalSet::new();
        let household = fixture.inner.create_household(&local, peer)?;
        let storage = fixture.inner.storage(peer)?;
        let sockpath = fixture
            .bind_server(
                &local,
                peer,
                household.clone(),
                JobHandlerImpl::new(Arc::clone(storage), household.clone()),
            )
            .await?;
        let foo = Path::parse("foo")?;

        local
            .run_until(async move {
                let control: control::Client = unixsocket::connect(&sockpath).await?;

                let mut request = control.set_arena_mark_request();
                let mut req = request.get().init_req();
                req.set_arena(arena.as_str());
                req.set_mark(control_capnp::Mark::Keep);
                request.send().promise.await?;

                assert_eq!(Mark::Keep, storage.get_mark(arena, &foo).await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn get_mark() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let arena = HouseholdFixture::test_arena();
        let peer = HouseholdFixture::a();
        let local = LocalSet::new();
        let household = fixture.inner.create_household(&local, peer)?;
        let storage = fixture.inner.storage(peer)?;
        let sockpath = fixture
            .bind_server(
                &local,
                peer,
                household.clone(),
                JobHandlerImpl::new(Arc::clone(storage), household.clone()),
            )
            .await?;
        let foo = Path::parse("foo")?;

        local
            .run_until(async move {
                let control: control::Client = unixsocket::connect(&sockpath).await?;

                let mut request = control.get_mark_request();
                let mut req = request.get().init_req();
                req.set_arena(arena.as_str());
                req.set_path("foo");
                let result = request.send().promise.await?;
                assert_eq!(
                    control_capnp::Mark::Watch,
                    result.get()?.get_res()?.get_mark()?
                );

                storage.set_mark(arena, &foo, Mark::Keep).await?;

                let mut request = control.get_mark_request();
                let mut req = request.get().init_req();
                req.set_arena(arena.as_str());
                req.set_path("foo");
                let result = request.send().promise.await?;
                assert_eq!(
                    control_capnp::Mark::Keep,
                    result.get()?.get_res()?.get_mark()?
                );

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn churten_rpc_job_succeeds() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let arena = HouseholdFixture::test_arena();
        let peer = HouseholdFixture::a();
        let local = LocalSet::new();
        let storage = Arc::clone(fixture.inner.storage(peer)?);
        let household = fixture.inner.create_household(&local, peer)?;

        // Create a fake job handler that succeeds
        let handler = FakeJobHandler::new(|| Ok(JobStatus::Done)).with_progress(true);
        let sockpath = fixture
            .bind_server(&local, peer, household, handler)
            .await?;

        local
            .run_until(async move {
                let control = client::connect(&sockpath).await?;
                let churten = client::get_churten(&control).await?;

                // Start churten
                churten.start_request().send().promise.await?;

                // Check if it's running
                let is_running_result = churten.is_running_request().send().promise.await?;
                assert!(is_running_result.get()?.get_running());

                // Set up a subscription
                let (tx, mut rx) = tokio::sync::mpsc::channel::<ChurtenUpdates>(10);

                let mut subscribe_request = churten.subscribe_request();
                subscribe_request
                    .get()
                    .set_subscriber(TxChurtenSubscriber::new(tx).as_client());
                subscribe_request.send().promise.await?;

                // Wait for the initial set of jobs (empty)
                assert_eq!(
                    ChurtenUpdates::Reset(vec![]),
                    tokio::time::timeout(Duration::from_secs(3), rx.recv())
                        .await?
                        .unwrap()
                );

                // Create a job by setting up a file to download
                storage.set_arena_mark(arena, Mark::Keep).await?;
                let foo = Path::parse("foo")?;
                let hash = Hash([1; 32]);
                fixture
                    .inner
                    .cache(peer)?
                    .update(
                        Peer::from("other"),
                        Notification::Add {
                            arena,
                            index: 1,
                            path: foo.clone(),
                            mtime: UnixTime::from_secs(1234567890),
                            size: 100,
                            hash: hash.clone(),
                        },
                    )
                    .await?;

                // Verify the notifications
                assert_eq!(
                    ChurtenUpdates::Notify(ChurtenNotification::New {
                        arena,
                        job_id: JobId(1),
                        job: Arc::new(Job::Download(foo.clone(), hash.clone()))
                    }),
                    tokio::time::timeout(Duration::from_secs(3), rx.recv())
                        .await?
                        .unwrap()
                );

                assert_eq!(
                    ChurtenUpdates::Notify(ChurtenNotification::Start {
                        arena,
                        job_id: JobId(1),
                    }),
                    tokio::time::timeout(Duration::from_secs(3), rx.recv())
                        .await?
                        .unwrap()
                );

                // Expect progress sent by FakeJobHandler
                assert_eq!(
                    ChurtenUpdates::Notify(ChurtenNotification::UpdateAction {
                        arena,
                        job_id: JobId(1),
                        action: JobAction::Download,
                        index: 2,
                    }),
                    tokio::time::timeout(Duration::from_secs(3), rx.recv())
                        .await?
                        .unwrap()
                );

                assert_eq!(
                    ChurtenUpdates::Notify(ChurtenNotification::UpdateByteCount {
                        arena,
                        job_id: JobId(1),
                        current_bytes: 50,
                        total_bytes: 100,
                        index: 3,
                    }),
                    tokio::time::timeout(Duration::from_secs(3), rx.recv())
                        .await?
                        .unwrap()
                );

                assert_eq!(
                    ChurtenUpdates::Notify(ChurtenNotification::UpdateByteCount {
                        arena,
                        job_id: JobId(1),
                        current_bytes: 100,
                        total_bytes: 100,
                        index: 4,
                    }),
                    tokio::time::timeout(Duration::from_secs(3), rx.recv())
                        .await?
                        .unwrap()
                );

                // Expect the job to succeed
                assert_eq!(
                    ChurtenUpdates::Notify(ChurtenNotification::Finish {
                        arena,
                        job_id: JobId(1),
                        progress: JobProgress::Done,
                    }),
                    tokio::time::timeout(Duration::from_secs(3), rx.recv())
                        .await?
                        .unwrap()
                );

                // Shutdown churten
                churten.shutdown_request().send().promise.await?;

                // Make sure that churten is no longer running
                let is_running_result = churten.is_running_request().send().promise.await?;
                assert!(!is_running_result.get()?.get_running());

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn recent_jobs() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let arena = HouseholdFixture::test_arena();
        let peer = HouseholdFixture::a();
        let local = LocalSet::new();
        let storage = Arc::clone(fixture.inner.storage(peer)?);
        let household = fixture.inner.create_household(&local, peer)?;

        // Create a fake job handler that succeeds
        let handler = FakeJobHandler::new(|| Ok(JobStatus::Done));
        let sockpath = fixture
            .bind_server(&local, peer, household, handler)
            .await?;

        local
            .run_until(async move {
                let control: control::Client = unixsocket::connect(&sockpath).await?;
                let churten = control
                    .churten_request()
                    .send()
                    .promise
                    .await?
                    .get()?
                    .get_churten()?;

                // Start churten
                churten.start_request().send().promise.await?;
                let (tx, mut rx) = mpsc::channel(10);
                let mut subscribe_request = churten.subscribe_request();
                subscribe_request
                    .get()
                    .set_subscriber(TxChurtenSubscriber::new(tx).as_client());
                subscribe_request.send().promise.await?;

                // Create a job by setting up a file to download
                storage.set_arena_mark(arena, Mark::Keep).await?;
                fixture
                    .inner
                    .cache(peer)?
                    .update(
                        Peer::from("other"),
                        Notification::Add {
                            arena,
                            index: 1,
                            path: Path::parse("foo")?,
                            mtime: UnixTime::from_secs(1234567890),
                            size: 100,
                            hash: Hash([1; 32]),
                        },
                    )
                    .await?;

                // Wait for it to be processed
                while let Some(n) = tokio::time::timeout(Duration::from_secs(3), rx.recv()).await? {
                    match n {
                        ChurtenUpdates::Notify(ChurtenNotification::Finish { .. }) => {
                            break;
                        }
                        _ => {}
                    }
                }

                // Get recent jobs
                let recent_jobs_result = churten.recent_jobs_request().send().promise.await?;
                let jobs = recent_jobs_result.get()?.get_res()?;

                // Should have at least one job
                assert!(jobs.len() > 0);

                // Check the first job
                let job = jobs.get(0);
                assert_eq!(job.get_arena()?, arena.as_str());
                assert_eq!(job.get_id(), 1);
                assert_eq!(
                    job.get_progress()?.get_type()?,
                    control_capnp::job_progress::Type::Done
                );
                assert_eq!(job.get_action()?, control_capnp::JobAction::None);

                // Check the job details
                let job_info = job.get_job()?;
                assert_eq!(job_info.get_path()?, "foo");

                // Shutdown churten
                churten.shutdown_request().send().promise.await?;

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }
}
