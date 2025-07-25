#![allow(dead_code)] // work in progress

use super::control_capnp::churten::{
    self, IsRunningParams, IsRunningResults, ShutdownParams, ShutdownResults, StartParams,
    StartResults, SubscribeParams, SubscribeResults,
};
use super::control_capnp::control::{
    self, ChurtenParams, ChurtenResults, GetMarkParams, GetMarkResults, SetArenaMarkParams,
    SetArenaMarkResults, SetMarkParams, SetMarkResults,
};
use super::control_capnp::{self, churten_notification};
use crate::consensus::churten::{Churten, ChurtenNotification, JobHandler, JobProgress};
use capnp::capability::Promise;
use capnp_rpc::pry;
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
        let subscriber = pry!(pry!(params.get()).get_subscriber());

        // Forward notifications from tx to the subscriber
        tokio::task::spawn_local(async move {
            while let Ok(notification) = rx.recv().await {
                let mut request = subscriber.notify_request();
                fill_notification(notification, request.get().init_notification());
                // Ignore errors as the client might have disconnected
                let _ = request.send().await;
            }
        });

        Promise::ok(())
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

fn fill_notification(source: ChurtenNotification, mut dest: churten_notification::Builder<'_>) {
    let arena = source.arena();
    let job = source.job();
    let path = job.path().as_str().to_owned();
    let hash = match &**job {
        realize_storage::Job::Download(_, _, hash) => hash.0.clone(),
    };

    dest.set_arena(&arena.as_str());
    // Set job fields using reborrow
    let mut job_builder = dest.reborrow().init_job();
    job_builder.set_path(&path);
    let mut download = job_builder.init_download();
    download.set_hash(&hash);
    // Set notification type
    match &source {
        ChurtenNotification::Update { progress, .. } => {
            let mut update = dest.reborrow().init_update();
            match progress {
                JobProgress::Pending => {
                    update.set_progress(control_capnp::JobProgress::Pending);
                }
                JobProgress::Running => {
                    update.set_progress(control_capnp::JobProgress::Running);
                }
                JobProgress::Done => {
                    update.set_progress(control_capnp::JobProgress::Done);
                }
                JobProgress::Abandoned => {
                    update.set_progress(control_capnp::JobProgress::Abandoned);
                }
                JobProgress::Cancelled => {
                    update.set_progress(control_capnp::JobProgress::Cancelled);
                }
                JobProgress::Failed(msg) => {
                    update.set_progress(control_capnp::JobProgress::Failed);
                    update.set_message(&msg);
                }
            }
        }
        ChurtenNotification::UpdateByteCount {
            current_bytes,
            total_bytes,
            ..
        } => {
            let mut update_byte_count = dest.reborrow().init_update_byte_count();
            update_byte_count.set_current_bytes(*current_bytes);
            update_byte_count.set_total_bytes(*total_bytes);
        }
    }
}
#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use super::*;
    use crate::consensus::churten::{ChurtenNotification, JobHandler, JobHandlerImpl};
    use crate::rpc::testing::HouseholdFixture;
    use assert_fs::TempDir;
    use realize_network::unixsocket;
    use realize_storage::{JobStatus, Mark, Notification};
    use realize_types::{Peer, UnixTime};
    use tarpc::tokio_util::sync::CancellationToken;
    use tokio::sync::broadcast;
    use tokio::task::LocalSet;

    struct Fixture {
        inner: HouseholdFixture,
        tempdir: TempDir,
        shutdown: CancellationToken,
    }

    /// A fake JobHandler for testing different job outcomes through RPC
    #[derive(Clone)]
    struct FakeJobHandler {
        result_fn: Arc<dyn Fn() -> anyhow::Result<JobStatus> + Send + Sync>,
        should_send_progress: bool,
        should_cancel: bool,
    }

    impl FakeJobHandler {
        fn new<F>(result_fn: F) -> Self
        where
            F: Fn() -> anyhow::Result<JobStatus> + Send + Sync + 'static,
        {
            Self {
                result_fn: Arc::new(result_fn),
                should_send_progress: false,
                should_cancel: false,
            }
        }

        fn with_progress(mut self, should_send: bool) -> Self {
            self.should_send_progress = should_send;
            self
        }

        fn with_cancel(mut self, should_cancel: bool) -> Self {
            self.should_cancel = should_cancel;
            self
        }
    }

    impl JobHandler for FakeJobHandler {
        async fn run(
            &self,
            arena: Arena,
            job: &Arc<realize_storage::Job>,
            tx: broadcast::Sender<ChurtenNotification>,
            shutdown: CancellationToken,
        ) -> anyhow::Result<JobStatus> {
            // Simulate some work time
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

            if self.should_cancel {
                shutdown.cancel();
            }
            if shutdown.is_cancelled() {
                shutdown.cancel();
                anyhow::bail!("cancelled");
            }

            // Send progress updates if requested
            if self.should_send_progress {
                let _ = tx.send(ChurtenNotification::UpdateByteCount {
                    arena,
                    job: Arc::clone(job),
                    current_bytes: 50,
                    total_bytes: 100,
                });
                tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
                let _ = tx.send(ChurtenNotification::UpdateByteCount {
                    arena,
                    job: Arc::clone(job),
                    current_bytes: 100,
                    total_bytes: 100,
                });
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
            handler: H,
        ) -> anyhow::Result<PathBuf> {
            let storage = self.inner.storage(peer)?;
            let churten = Churten::with_handler(Arc::clone(storage), handler);
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
                JobHandlerImpl::new(Arc::clone(storage), household.clone()),
            )
            .await?;

        local
            .run_until(async move {
                let control = unixsocket::connect::<control::Client>(&sockpath).await?;

                let mut request = control.set_mark_request();
                let mut req = request.get().init_req();
                req.set_arena(arena.as_str());
                req.set_path("foo");
                req.set_mark(control_capnp::Mark::Keep);
                request.send().promise.await?;

                assert_eq!(
                    Mark::Keep,
                    storage.get_mark(arena, &Path::parse("foo")?).await?
                );

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
                JobHandlerImpl::new(Arc::clone(storage), household.clone()),
            )
            .await?;

        local
            .run_until(async move {
                let control = unixsocket::connect::<control::Client>(&sockpath).await?;

                let mut request = control.set_arena_mark_request();
                let mut req = request.get().init_req();
                req.set_arena(arena.as_str());
                req.set_mark(control_capnp::Mark::Keep);
                request.send().promise.await?;

                assert_eq!(
                    Mark::Keep,
                    storage.get_mark(arena, &Path::parse("foo")?).await?
                );

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
                JobHandlerImpl::new(Arc::clone(storage), household.clone()),
            )
            .await?;

        local
            .run_until(async move {
                let control = unixsocket::connect::<control::Client>(&sockpath).await?;

                let mut request = control.get_mark_request();
                let mut req = request.get().init_req();
                req.set_arena(arena.as_str());
                req.set_path("foo");
                let result = request.send().promise.await?;
                assert_eq!(
                    control_capnp::Mark::Watch,
                    result.get()?.get_res()?.get_mark()?
                );

                storage
                    .set_mark(arena, &Path::parse("foo")?, Mark::Keep)
                    .await?;

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

        // Create a fake job handler that succeeds
        let handler = FakeJobHandler::new(|| Ok(JobStatus::Done)).with_progress(true);
        let sockpath = fixture.bind_server(&local, peer, handler).await?;

        local
            .run_until(async move {
                let control = unixsocket::connect::<control::Client>(&sockpath).await?;
                let churten = control
                    .churten_request()
                    .send()
                    .promise
                    .await?
                    .get()?
                    .get_churten()?;

                // Start churten
                churten.start_request().send().promise.await?;

                // Check if it's running
                let is_running_result = churten.is_running_request().send().promise.await?;
                assert!(is_running_result.get()?.get_running());

                // Set up a subscription
                let (tx, mut rx) = tokio::sync::mpsc::channel::<
                    control_capnp::churten::subscriber::NotifyParams,
                >(10);
                let subscriber = capnp_rpc::new_client(TestSubscriber { tx });

                let mut subscribe_request = churten.subscribe_request();
                subscribe_request.get().set_subscriber(subscriber);
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

                // Verify the first notification is Pending
                let first = tokio::time::timeout(Duration::from_secs(3), rx.recv())
                    .await?
                    .unwrap();
                let notification = first.get()?.get_notification()?;
                assert_eq!(notification.get_arena()?, arena.as_str());
                assert_eq!(notification.get_job()?.get_path()?, "foo");
                assert!(matches!(
                    notification.which(),
                    Ok(control_capnp::churten_notification::Update(_))
                ));

                // Verify the second notification is Running
                let second = tokio::time::timeout(Duration::from_secs(3), rx.recv())
                    .await?
                    .unwrap();
                let notification = second.get()?.get_notification()?;
                match notification.which()? {
                    control_capnp::churten_notification::Update(update) => {
                        let update = update?;
                        assert_eq!(control_capnp::JobProgress::Running, update.get_progress()?)
                    }
                    _ => panic!("Expected an Update, got {notification:?}"),
                }

                // Verify progress updates
                let third = tokio::time::timeout(Duration::from_secs(3), rx.recv())
                    .await?
                    .unwrap();
                let notification = third.get()?.get_notification()?;
                if let Ok(control_capnp::churten_notification::UpdateByteCount(update)) =
                    notification.which()
                {
                    let update = update.expect("UpdateByteCount present");
                    assert_eq!(update.get_current_bytes(), 50);
                    assert_eq!(update.get_total_bytes(), 100);
                } else {
                    panic!("Expected UpdateByteCount notification");
                }

                let fourth = tokio::time::timeout(Duration::from_secs(3), rx.recv())
                    .await?
                    .unwrap();
                let notification = fourth.get()?.get_notification()?;
                if let Ok(control_capnp::churten_notification::UpdateByteCount(update)) =
                    notification.which()
                {
                    let update = update.expect("UpdateByteCount present");
                    assert_eq!(update.get_current_bytes(), 100);
                    assert_eq!(update.get_total_bytes(), 100);
                } else {
                    panic!("Expected UpdateByteCount notification");
                }

                // Verify the last notification is Done
                let last = tokio::time::timeout(Duration::from_secs(3), rx.recv())
                    .await?
                    .unwrap();
                let notification = last.get()?.get_notification()?;
                if let Ok(control_capnp::churten_notification::Update(update)) =
                    notification.which()
                {
                    let update = update.expect("Update present");
                    assert_eq!(
                        update.get_progress().expect("progress present"),
                        control_capnp::JobProgress::Done
                    );
                } else {
                    panic!("Expected Update notification with Done progress");
                }

                // Shutdown churten
                churten.shutdown_request().send().promise.await?;

                // Check if it's no longer running
                let is_running_result = churten.is_running_request().send().promise.await?;
                assert!(!is_running_result.get()?.get_running());

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    // Test subscriber that captures notifications
    struct TestSubscriber {
        tx: tokio::sync::mpsc::Sender<control_capnp::churten::subscriber::NotifyParams>,
    }

    impl control_capnp::churten::subscriber::Server for TestSubscriber {
        fn notify(
            &mut self,
            params: control_capnp::churten::subscriber::NotifyParams,
        ) -> Promise<(), capnp::Error> {
            let tx = self.tx.clone();
            Promise::from_future(async move {
                let _ = tx.send(params).await;
                Ok(())
            })
        }
    }
}
