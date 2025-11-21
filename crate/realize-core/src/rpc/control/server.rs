#![allow(dead_code)] // work in progress

use super::control_capnp;
use super::control_capnp::churten::{
    self, IsRunningParams, IsRunningResults, RecentJobsParams, RecentJobsResults, ShutdownParams,
    ShutdownResults, StartParams, StartResults, SubscribeParams, SubscribeResults,
};
use super::control_capnp::control::{
    self, ChurtenParams, ChurtenResults, CreateArenaParams, CreateArenaResults, DisconnectParams,
    DisconnectResults, GetAttrParams, GetAttrResults, GetMarkParams, GetMarkResults,
    KeepConnectedParams, KeepConnectedResults, ListAttrParams, ListAttrResults, ListPeersParams,
    ListPeersResults, RemoveArenaParams, RemoveArenaResults, SetAttrParams, SetAttrResults,
    SetMarkParams, SetMarkResults,
};
use super::convert;
use crate::consensus::churten::{Churten, JobHandler};
use crate::rpc::{Household, household::ConnectionStatus};
use capnp::capability::Promise;
use realize_storage::{Mark, SanityCheck, Storage, StorageError};
use realize_types::{Arena, Hash, Path, Peer};
use std::cell::RefCell;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::rc::Rc;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct ControlServer<H: JobHandler + 'static> {
    storage: Arc<Storage>,
    churten: Rc<RefCell<Churten<H>>>,
    household: Arc<Household>,
}

impl<H: JobHandler + 'static> ControlServer<H> {
    pub(crate) fn new(
        storage: Arc<Storage>,
        churten: Churten<H>,
        household: Arc<Household>,
    ) -> ControlServer<H> {
        Self {
            storage,
            churten: Rc::new(RefCell::new(churten)),
            household,
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
            let path_str = req.get_path()?;
            let mark = parse_mark(req.get_mark()?);

            if path_str.is_empty() {
                // Set arena mark when no path is provided
                storage
                    .set_arena_mark(arena, mark)
                    .await
                    .map_err(from_storage_err)?;
            } else {
                // Set path-specific mark
                let path = parse_path(path_str)?;
                storage
                    .set_mark(arena, &path, mark)
                    .await
                    .map_err(from_storage_err)?;
            }

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
            let path_str = req.get_path()?;

            let mark = if path_str.is_empty() {
                // Get arena mark when no path is provided
                storage
                    .get_arena_mark(arena)
                    .await
                    .map_err(from_storage_err)?
            } else {
                // Get path-specific mark
                let path = parse_path(path_str)?;
                storage
                    .get_mark(arena, &path)
                    .await
                    .map_err(from_storage_err)?
            };

            let mut res = results.get().init_res();
            res.set_mark(mark_to_capnp(mark));
            Ok(())
        })
    }

    fn list_peers(
        &mut self,
        _: ListPeersParams,
        mut results: ListPeersResults,
    ) -> Promise<(), capnp::Error> {
        let household = Arc::clone(&self.household);
        Promise::from_future(async move {
            let connection_info = household
                .query_peers()
                .await
                .map_err(|e| capnp::Error::failed(e.to_string()))?;

            let mut peer_list = results.get().init_res(connection_info.len() as u32);
            for (i, (peer, info)) in connection_info.into_iter().enumerate() {
                let mut peer_info = peer_list.reborrow().get(i as u32);
                peer_info.set_peer(peer.as_str());
                peer_info.set_connected(matches!(info.connection, ConnectionStatus::Connected));
                peer_info.set_keep_connected(info.keep_connected);
            }

            Ok(())
        })
    }

    fn keep_connected(
        &mut self,
        params: KeepConnectedParams,
        _: KeepConnectedResults,
    ) -> Promise<(), capnp::Error> {
        let household = Arc::clone(&self.household);
        Promise::from_future(async move {
            let peer_str = params.get()?.get_peer()?;
            let peer = parse_peer(peer_str)?;

            household
                .keep_peer_connected(peer)
                .map_err(|e| capnp::Error::failed(e.to_string()))?;

            Ok(())
        })
    }

    fn disconnect(
        &mut self,
        params: DisconnectParams,
        _: DisconnectResults,
    ) -> Promise<(), capnp::Error> {
        let household = Arc::clone(&self.household);
        Promise::from_future(async move {
            let peer_str = params.get()?.get_peer()?;
            let peer = parse_peer(peer_str)?;

            household
                .disconnect_peer(peer)
                .map_err(|e| capnp::Error::failed(e.to_string()))?;

            Ok(())
        })
    }

    fn create_arena(
        &mut self,
        params: CreateArenaParams,
        mut results: CreateArenaResults,
    ) -> Promise<(), capnp::Error> {
        let storage = Arc::clone(&self.storage);
        Promise::from_future(async move {
            let req = params.get()?.get_req()?;
            let arena = parse_arena(req.get_arena()?)?;
            let dir = std::path::Path::new(&OsStr::from_bytes(req.get_dir()?)).to_path_buf();

            let result =
                tokio::task::spawn_local(async move { storage.create_arena(arena, &dir).await })
                    .await
                    .map_err(|e| capnp::Error::failed(e.to_string()))?;

            match result {
                Ok(()) => {
                    results.get().init_res().init_ok();
                }
                Err(StorageError::SanityCheckFailed(_, path, check)) => {
                    fill_issue(
                        &path,
                        check,
                        results.get().init_res().init_err().init_issue(),
                    );
                }
                Err(err) => return Err(from_storage_err(err)),
            }
            Ok(())
        })
    }

    fn remove_arena(
        &mut self,
        params: RemoveArenaParams,
        mut results: RemoveArenaResults,
    ) -> Promise<(), capnp::Error> {
        let storage = Arc::clone(&self.storage);
        Promise::from_future(async move {
            let req = params.get()?.get_req()?;
            let arena = parse_arena(req.get_arena()?)?;

            let paths = tokio::task::spawn_local(async move { storage.remove_arena(arena).await })
                .await
                .map_err(|e| capnp::Error::failed(e.to_string()))?
                .map_err(|e| capnp::Error::failed(e.to_string()))?;

            let mut res = results.get().init_res().init_ok();
            if let Some((dir, workdir)) = paths {
                res.set_dir(dir.as_os_str().as_bytes());
                res.set_workdir(workdir.as_os_str().as_bytes());
            }

            Ok(())
        })
    }

    fn list_attr(
        &mut self,
        params: ListAttrParams,
        mut results: ListAttrResults,
    ) -> Promise<(), capnp::Error> {
        let storage = Arc::clone(&self.storage);
        Promise::from_future(async move {
            let req = params.get()?.get_req()?;
            let arena = parse_arena(req.get_arena()?)?;
            let path = parse_path(req.get_path()?)?;
            let res = results.get().init_res();
            match storage.cache().list_xattrs((arena, path)).await {
                Ok(attrs) => {
                    let res = res.init_ok();
                    let mut list = res.init_attrs(attrs.len() as u32);
                    for (i, attr) in attrs.into_iter().enumerate() {
                        if let Some(stripped) = attr.strip_prefix("realize.") {
                            list.set(i as u32, stripped);
                        } else {
                            list.set(i as u32, attr);
                        }
                    }
                }
                Err(e) => fill_attr_error(e, res.init_err())?,
            }
            Ok(())
        })
    }

    fn get_attr(
        &mut self,
        params: GetAttrParams,
        mut results: GetAttrResults,
    ) -> Promise<(), capnp::Error> {
        let storage = Arc::clone(&self.storage);
        Promise::from_future(async move {
            let req = params.get()?.get_req()?;
            let arena = parse_arena(req.get_arena()?)?;
            let path = parse_path(req.get_path()?)?;
            let attr = format!("realize.{}", req.get_attr()?.to_str()?);

            let res = results.get().init_res();
            match storage.cache().get_xattr((arena, path), &attr).await {
                Ok(v) => res.init_ok().set_value(&v),
                Err(e) => fill_attr_error(e, res.init_err())?,
            }
            Ok(())
        })
    }

    fn set_attr(
        &mut self,
        params: SetAttrParams,
        mut results: SetAttrResults,
    ) -> Promise<(), capnp::Error> {
        let storage = Arc::clone(&self.storage);
        Promise::from_future(async move {
            let req = params.get()?.get_req()?;
            let arena = parse_arena(req.get_arena()?)?;
            let path = parse_path(req.get_path()?)?;
            let attr = format!("realize.{}", req.get_attr()?.to_str()?);
            let value = req.get_value()?;

            let res = results.get().init_res();
            match storage
                .cache()
                .set_xattr((arena, path), &attr, value.to_str()?.into())
                .await
            {
                Ok(()) => {
                    res.init_ok();
                }
                Err(e) => fill_attr_error(e, res.init_err())?,
            }

            Ok(())
        })
    }
}

fn fill_attr_error(
    err: StorageError,
    mut res: control_capnp::attr_error::Builder<'_>,
) -> Result<(), capnp::Error> {
    match err {
        StorageError::NoSuchAttribute => {
            res.set_no_such_attribute(());
        }
        StorageError::InvalidAttributeValue => {
            res.set_invalid_attribute_value(());
        }
        StorageError::UnknownArena(_) => {
            res.set_unknown_arena(());
        }
        StorageError::NotFound => {
            res.set_path_not_found(());
        }
        _ => {
            return Err(from_storage_err(err));
        }
    }

    Ok(())
}

fn fill_issue(
    path: &std::path::Path,
    check: SanityCheck,
    mut dest: control_capnp::sanity_check_issue::Builder<'_>,
) {
    dest.set_path(path.as_os_str().as_bytes());
    dest.set_check(match check {
        SanityCheck::Exists => control_capnp::sanity_check_issue::Check::Exists,
        SanityCheck::ReadableDir => control_capnp::sanity_check_issue::Check::ReadableDir,
        SanityCheck::WritableDir => control_capnp::sanity_check_issue::Check::WritableDir,
        SanityCheck::SameDevice => control_capnp::sanity_check_issue::Check::SameDevice,
    });
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

fn parse_peer(reader: capnp::text::Reader<'_>) -> Result<Peer, capnp::Error> {
    Ok(Peer::from(reader.to_str()?))
}

fn parse_mark(mark: control_capnp::Mark) -> Mark {
    match mark {
        control_capnp::Mark::Own => Mark::Own,
        control_capnp::Mark::Watch => Mark::Watch,
        control_capnp::Mark::Default => Mark::Default,
        control_capnp::Mark::Keep => Mark::Keep,
    }
}

fn mark_to_capnp(mark: Mark) -> control_capnp::Mark {
    match mark {
        Mark::Own => control_capnp::Mark::Own,
        Mark::Watch => control_capnp::Mark::Watch,
        Mark::Default => control_capnp::Mark::Default,
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
    use crate::rpc::control::client::{self, ChurtenUpdates, TxChurtenSubscriber};
    use crate::rpc::testing::{self, HouseholdFixture};
    use crate::rpc::{Household, PeerStatus, result_capnp};
    use assert_fs::TempDir;
    use assert_fs::prelude::PathChild;
    use realize_network::unixsocket;
    use realize_storage::{Job, JobId, JobStatus, Mark, Notification};
    use realize_types::{Peer, UnixTime};
    use std::collections::BTreeSet;
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
            _job_id: JobId,
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
            household: Arc<Household>,
            handler: H,
        ) -> anyhow::Result<PathBuf> {
            let storage = self.inner.storage(peer)?;
            let churten = Churten::with_handler(Arc::clone(storage), household.clone(), handler);
            let server = ControlServer::new(Arc::clone(storage), churten, household);

            let sockpath = self
                .tempdir
                .path()
                .join("realize/control.socket")
                .to_path_buf();
            unixsocket::bind(
                &local,
                &sockpath,
                0o077,
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

                let mut request = control.set_mark_request();
                let mut req = request.get().init_req();
                req.set_arena(arena.as_str());
                req.set_path(""); // Empty path means arena mark
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
                    control_capnp::Mark::Default,
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
    async fn get_arena_mark() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let arena = HouseholdFixture::test_arena();
        let peer = HouseholdFixture::a();
        let local = LocalSet::new();
        let household = fixture.inner.create_household(&local, peer)?;
        let sockpath = fixture
            .bind_server(
                &local,
                peer,
                household.clone(),
                JobHandlerImpl::new(Arc::clone(fixture.inner.storage(peer)?), household.clone()),
            )
            .await?;

        local
            .run_until(async move {
                let control: control::Client = unixsocket::connect(&sockpath).await?;

                // First, set an arena mark
                let mut request = control.set_mark_request();
                let mut req = request.get().init_req();
                req.set_arena(arena.as_str());
                req.set_path(""); // Empty path means arena mark
                req.set_mark(control_capnp::Mark::Keep);
                request.send().promise.await?;

                // Now get the arena mark
                let mut request = control.get_mark_request();
                let mut req = request.get().init_req();
                req.set_arena(arena.as_str());
                req.set_path(""); // Empty path means arena mark
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
                // Set arena mark using the RPC method
                let mut request = control.set_mark_request();
                let mut req = request.get().init_req();
                req.set_arena(arena.as_str());
                req.set_path(""); // Empty path means arena mark
                req.set_mark(control_capnp::Mark::Keep);
                request.send().promise.await?;
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
                // Set arena mark using the RPC method
                let mut request = control.set_mark_request();
                let mut req = request.get().init_req();
                req.set_arena(arena.as_str());
                req.set_path(""); // Empty path means arena mark
                req.set_mark(control_capnp::Mark::Keep);
                request.send().promise.await?;
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

    #[tokio::test]
    async fn list_peers() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                let local = LocalSet::new();

                testing::connect(&household_a, b).await?;

                let handler = FakeJobHandler::new(|| Ok(JobStatus::Done));
                let sockpath = fixture
                    .bind_server(&local, a, household_a.clone(), handler)
                    .await?;

                local
                    .run_until(async move {
                        let control: control::Client = unixsocket::connect(&sockpath).await?;

                        // Initially, should have peers B and C (from fixture setup)
                        let list_peers_result = control.list_peers_request().send().promise.await?;
                        let peers = list_peers_result.get()?.get_res()?;
                        assert_eq!(peers.len(), 2);

                        // Find peer B in the list
                        let mut b_info = None;
                        for i in 0..peers.len() {
                            let peer_info = peers.get(i);
                            if peer_info.get_peer()? == b.as_str() {
                                b_info = Some(peer_info);
                                break;
                            }
                        }
                        let b_info = b_info.expect("Peer B should be in the list");

                        // Initially peer B should be connected (due to interconnected setup)
                        assert!(b_info.get_connected());
                        assert!(b_info.get_keep_connected());

                        // Disconnect from peer B
                        let mut status_a = household_a.peer_status();
                        household_a.disconnect_peer(b)?;
                        assert_eq!(
                            PeerStatus::Disconnected(b),
                            tokio::time::timeout(Duration::from_secs(3), status_a.recv())
                                .await
                                .unwrap()?
                        );

                        // Now list peers again
                        let list_peers_result = control.list_peers_request().send().promise.await?;
                        let peers = list_peers_result.get()?.get_res()?;

                        // Should still have 2 peers
                        assert_eq!(peers.len(), 2);

                        // Find peer B again and check it's now disconnected
                        let mut b_info = None;
                        for i in 0..peers.len() {
                            let peer_info = peers.get(i);
                            if peer_info.get_peer()? == b.as_str() {
                                b_info = Some(peer_info);
                                break;
                            }
                        }
                        let b_info = b_info.expect("Peer B should be in the list");
                        assert!(!b_info.get_connected());
                        assert!(!b_info.get_keep_connected());

                        // Reconnect to peer B
                        household_a.keep_peer_connected(b)?;
                        assert_eq!(
                            PeerStatus::Connected(b),
                            tokio::time::timeout(Duration::from_secs(3), status_a.recv())
                                .await
                                .unwrap()?
                        );

                        // List peers again
                        let list_peers_result = control.list_peers_request().send().promise.await?;
                        let peers = list_peers_result.get()?.get_res()?;

                        // Should still have 2 peers
                        assert_eq!(peers.len(), 2);

                        // Find peer B again and check it's now connected
                        let mut b_info = None;
                        for i in 0..peers.len() {
                            let peer_info = peers.get(i);
                            if peer_info.get_peer()? == b.as_str() {
                                b_info = Some(peer_info);
                                break;
                            }
                        }
                        let b_info = b_info.expect("Peer B should be in the list");
                        assert!(b_info.get_connected());
                        assert!(b_info.get_keep_connected());
                        Ok::<(), anyhow::Error>(())
                    })
                    .await?;

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn keep_connected() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                let local = LocalSet::new();

                testing::connect(&household_a, b).await?;

                let handler = FakeJobHandler::new(|| Ok(JobStatus::Done));
                let sockpath = fixture
                    .bind_server(&local, a, household_a.clone(), handler)
                    .await?;
                local
                    .run_until(async move {
                        let control: control::Client = unixsocket::connect(&sockpath).await?;

                        // Initially, should have peers B and C (from fixture setup)
                        let list_peers_result = control.list_peers_request().send().promise.await?;
                        let peers = list_peers_result.get()?.get_res()?;
                        assert_eq!(peers.len(), 2);

                        // Find peer B in the list
                        let mut b_info = None;
                        for i in 0..peers.len() {
                            let peer_info = peers.get(i);
                            if peer_info.get_peer()? == b.as_str() {
                                b_info = Some(peer_info);
                                break;
                            }
                        }
                        let b_info = b_info.expect("Peer B should be in the list");

                        // Initially peer B should be connected (due to interconnected setup)
                        assert!(b_info.get_connected());
                        assert!(b_info.get_keep_connected());

                        // Disconnect from peer B first
                        let mut status_a = household_a.peer_status();
                        household_a.disconnect_peer(b)?;
                        assert_eq!(
                            PeerStatus::Disconnected(b),
                            tokio::time::timeout(Duration::from_secs(3), status_a.recv())
                                .await
                                .unwrap()?
                        );

                        // Verify peer is now disconnected
                        let list_peers_result = control.list_peers_request().send().promise.await?;
                        let peers = list_peers_result.get()?.get_res()?;
                        assert_eq!(peers.len(), 2);

                        // Find peer B again and check it's now disconnected
                        let mut b_info = None;
                        for i in 0..peers.len() {
                            let peer_info = peers.get(i);
                            if peer_info.get_peer()? == b.as_str() {
                                b_info = Some(peer_info);
                                break;
                            }
                        }
                        let b_info = b_info.expect("Peer B should be in the list");
                        assert!(!b_info.get_connected());
                        assert!(!b_info.get_keep_connected());

                        // Use keep_connected RPC method
                        let mut request = control.keep_connected_request();
                        request.get().set_peer(b.as_str());
                        request.send().promise.await?;

                        // Wait a bit for connection to establish
                        assert_eq!(
                            PeerStatus::Connected(b),
                            tokio::time::timeout(Duration::from_secs(3), status_a.recv())
                                .await
                                .unwrap()?
                        );

                        // Verify peer is now connected
                        let list_peers_result = control.list_peers_request().send().promise.await?;
                        let peers = list_peers_result.get()?.get_res()?;
                        assert_eq!(peers.len(), 2);

                        // Find peer B again and check it's now connected
                        let mut b_info = None;
                        for i in 0..peers.len() {
                            let peer_info = peers.get(i);
                            if peer_info.get_peer()? == b.as_str() {
                                b_info = Some(peer_info);
                                break;
                            }
                        }
                        let b_info = b_info.expect("Peer B should be in the list");
                        assert!(b_info.get_connected());
                        assert!(b_info.get_keep_connected());
                        Ok::<(), anyhow::Error>(())
                    })
                    .await?;

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn disconnect() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                let local = LocalSet::new();

                testing::connect(&household_a, b).await?;

                // Create a fake job handler
                let handler = FakeJobHandler::new(|| Ok(JobStatus::Done));
                let sockpath = fixture
                    .bind_server(&local, a, household_a.clone(), handler)
                    .await?;

                local
                    .run_until(async move {
                        let control: control::Client = unixsocket::connect(&sockpath).await?;

                        // Initially peer B should be connected (due to interconnected setup)
                        let list_peers_result = control.list_peers_request().send().promise.await?;
                        let peers = list_peers_result.get()?.get_res()?;
                        assert_eq!(peers.len(), 2);

                        // Find peer B and check it's connected
                        let mut b_info = None;
                        for i in 0..peers.len() {
                            let peer_info = peers.get(i);
                            if peer_info.get_peer()? == b.as_str() {
                                b_info = Some(peer_info);
                                break;
                            }
                        }
                        let b_info = b_info.expect("Peer B should be in the list");
                        assert!(b_info.get_connected());
                        assert!(b_info.get_keep_connected());

                        let mut status_a = household_a.peer_status();

                        // Use disconnect RPC method
                        let mut request = control.disconnect_request();
                        request.get().set_peer(b.as_str());
                        request.send().promise.await?;

                        assert_eq!(
                            PeerStatus::Disconnected(b),
                            tokio::time::timeout(Duration::from_secs(3), status_a.recv())
                                .await
                                .unwrap()?
                        );

                        // Verify peer is now disconnected
                        let list_peers_result = control.list_peers_request().send().promise.await?;
                        let peers = list_peers_result.get()?.get_res()?;
                        assert_eq!(peers.len(), 2);

                        // Find peer B again and check it's now disconnected
                        let mut b_info = None;
                        for i in 0..peers.len() {
                            let peer_info = peers.get(i);
                            if peer_info.get_peer()? == b.as_str() {
                                b_info = Some(peer_info);
                                break;
                            }
                        }
                        let b_info = b_info.expect("Peer B should be in the list");
                        assert!(!b_info.get_connected());
                        assert!(!b_info.get_keep_connected());

                        Ok::<(), anyhow::Error>(())
                    })
                    .await?;
                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_arena() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
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
        local
            .run_until(async move {
                let control: control::Client = unixsocket::connect(&sockpath).await?;

                let newarena = TempDir::new()?;
                let mut request = control.create_arena_request();
                let mut req = request.get().init_req();
                req.set_arena("newarena");
                req.set_dir(newarena.path().as_os_str().as_bytes());
                let result = request.send().promise.await?;
                assert!(matches!(
                    result.get()?.get_res()?.which()?,
                    result_capnp::result::Which::Ok(_)
                ));

                assert_unordered::assert_eq_unordered!(
                    BTreeSet::from([HouseholdFixture::test_arena(), Arena::from("newarena")]),
                    storage.arenas()
                );

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_arena_dir_not_found() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
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
        local
            .run_until(async move {
                let control: control::Client = unixsocket::connect(&sockpath).await?;

                let tempdir = TempDir::new()?;
                let newarena = tempdir.child("doesnotexist");
                let mut request = control.create_arena_request();
                let mut req = request.get().init_req();
                req.set_arena("newarena");
                req.set_dir(newarena.path().as_os_str().as_bytes());
                let result = request.send().promise.await?;

                // T response mentions the appropriate error.
                match result.get()?.get_res()?.which()? {
                    result_capnp::result::Which::Ok(_) => panic!("Unexpected success response"),
                    result_capnp::result::Which::Err(err) => {
                        let err = err?;
                        let issue = err.get_issue()?;
                        assert_eq!(
                            control_capnp::sanity_check_issue::Check::Exists,
                            issue.get_check()?
                        );
                        assert_eq!(
                            newarena.path(),
                            std::path::Path::new(&OsStr::from_bytes(issue.get_path()?))
                        );
                    }
                }

                // No new arena was added.
                assert_eq!(
                    BTreeSet::from([HouseholdFixture::test_arena()]),
                    storage.arenas()
                );

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn remove_arena() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let peer = HouseholdFixture::a();
        let local = LocalSet::new();
        let datadir = fixture.inner.arena_root(peer).to_path_buf();
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
        local
            .run_until(async move {
                let control: control::Client = unixsocket::connect(&sockpath).await?;

                let arena = HouseholdFixture::test_arena();
                let mut request = control.remove_arena_request();
                let mut req = request.get().init_req();
                req.set_arena(arena.as_str());
                let result = request.send().promise.await?;
                match result.get()?.get_res()?.which()? {
                    result_capnp::result::Which::Ok(res) => {
                        let res = res?;
                        assert_eq!(
                            datadir.as_path(),
                            std::path::Path::new(&OsStr::from_bytes(res.get_dir()?))
                        );
                        assert_eq!(
                            datadir.join(".realize"),
                            std::path::Path::new(&OsStr::from_bytes(res.get_workdir()?))
                        );
                    }
                    _ => panic!("Unexpected error from remove_arena"),
                };
                assert!(storage.arenas().is_empty());

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn list_attr() -> anyhow::Result<()> {
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

        local
            .run_until(async move {
                let control: control::Client = unixsocket::connect(&sockpath).await?;

                let mut request = control.list_attr_request();
                let mut req = request.get().init_req();
                req.set_arena(arena.as_str());
                req.set_path("");
                let result = request.send().promise.await?;

                let res = result.get()?.get_res()?;
                let attrs = match res.which()? {
                    result_capnp::result::Which::Ok(ok) => ok?.get_attrs()?,
                    result_capnp::result::Which::Err(e) => panic!("RPC failed: {:?}", e),
                };

                // We expect "quota.max" and "quota.leave" for root, stripped of "realize."
                let mut attr_vec: Vec<String> = Vec::new();
                for attr in attrs.iter() {
                    attr_vec.push(attr?.to_str()?.to_string());
                }
                attr_vec.sort();

                assert!(attr_vec.contains(&"quota.max".to_string()));
                assert!(attr_vec.contains(&"quota.leave".to_string()));

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn get_attr() -> anyhow::Result<()> {
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

        local
            .run_until(async move {
                let control: control::Client = unixsocket::connect(&sockpath).await?;

                // Set a mark first
                storage.set_arena_mark(arena, Mark::Keep).await?;

                let mut request = control.get_attr_request();
                let mut req = request.get().init_req();
                req.set_arena(arena.as_str());
                req.set_path("");
                req.set_attr("mark");
                let result = request.send().promise.await?;

                let res = result.get()?.get_res()?;
                match res.which()? {
                    result_capnp::result::Which::Ok(ok) => {
                        assert_eq!("keep", ok?.get_value()?.to_str()?);
                    }
                    result_capnp::result::Which::Err(e) => panic!("RPC failed: {:?}", e),
                }

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn set_attr() -> anyhow::Result<()> {
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

        local
            .run_until(async move {
                let control: control::Client = unixsocket::connect(&sockpath).await?;

                let mut request = control.set_attr_request();
                let mut req = request.get().init_req();
                req.set_arena(arena.as_str());
                req.set_path("");
                req.set_attr("mark");
                req.set_value("own");
                let result = request.send().promise.await?;

                let res = result.get()?.get_res()?;
                match res.which()? {
                    result_capnp::result::Which::Ok(_) => {}
                    result_capnp::result::Which::Err(e) => panic!("RPC failed: {:?}", e),
                }

                assert_eq!(Mark::Own, storage.get_arena_mark(arena).await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn attr_errors() -> anyhow::Result<()> {
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

        local
            .run_until(async move {
                let control: control::Client = unixsocket::connect(&sockpath).await?;

                // Get unknown attribute
                let mut request = control.get_attr_request();
                let mut req = request.get().init_req();
                req.set_arena(arena.as_str());
                req.set_path("");
                req.set_attr("nonexistent");
                let result = request.send().promise.await?;

                let res = result.get()?.get_res()?;
                match res.which()? {
                    result_capnp::result::Which::Ok(_) => panic!("Expected error"),
                    result_capnp::result::Which::Err(e) => match e?.which()? {
                        control_capnp::attr_error::Which::NoSuchAttribute(_) => {}
                        _ => panic!("Wrong error type"),
                    },
                }

                // Set invalid value
                let mut request = control.set_attr_request();
                let mut req = request.get().init_req();
                req.set_arena(arena.as_str());
                req.set_path("");
                req.set_attr("mark");
                req.set_value("invalid_mark");
                let result = request.send().promise.await?;

                let res = result.get()?.get_res()?;
                match res.which()? {
                    result_capnp::result::Which::Ok(_) => panic!("Expected error"),
                    result_capnp::result::Which::Err(e) => match e?.which()? {
                        control_capnp::attr_error::Which::InvalidAttributeValue(_) => {}
                        _ => panic!("Wrong error type"),
                    },
                }

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }
}
