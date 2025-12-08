use super::convert;
use super::rate_limit;
use crate::rpc::store_capnp::store::{self, SubscriptionsParams, SubscriptionsResults};
use crate::rpc::store_capnp::subscriber::{self, NotifyParams, NotifyResults};
use crate::rpc::store_capnp::subscriptions::{self, SubscribeParams, SubscribeResults};
use async_speed_limit::Limiter;
use capnp::capability::Promise;
use realize_storage::Notification;
use realize_storage::{Progress, Storage, StorageError};
use realize_types::Arena;
use realize_types::Peer;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use std::rc::Rc;

/// Subscribe to notifications from the given client and use it to
/// update the cache.
pub(crate) async fn subscribe_self(
    storage: &Arc<Storage>,
    peer: Peer,
    store: store::Client,
    token: CancellationToken,
) -> anyhow::Result<()> {
    let mut request = store.subscriptions_request();
    request
        .get()
        .set_subscriber(SubscriberServer::new(peer, Arc::clone(storage)).into_client());
    let subscriptions = request.send().promise.await?.get()?.get_subscriptions()?;

    let cache = storage.cache().clone();
    let mut arenas = storage.watch_arenas();
    let mut registered = arenas.borrow_and_update().clone();
    for arena in registered.iter() {
        track_arena(peer, &cache, &subscriptions, *arena).await?;
    }

    // Track new arenas as long as the connection holds.
    tokio::task::spawn_local(async move {
        loop {
            tokio::select! {
                _ = token.cancelled() => return,
                ret = arenas.changed() => {
                    if !ret.is_ok() {
                        return;
                    }
                    let update = arenas.borrow_and_update().clone();
                    for arena in update {
                        if registered.contains(&arena) {
                            continue;
                        }
                        match track_arena(peer, &cache, &subscriptions, arena).await {
                            Ok(_) => {
                                registered.insert(arena);
                            }
                            Err(err) => {
                                if err.kind == capnp::ErrorKind::Disconnected {
                                    return;
                                }
                                log::error!("[{arena}]@{peer} failed to subscribe to arena: {err:?}")
                            }
                        }
                    }
                }
            }
        }
    });

    Ok(())
}

async fn track_arena(
    peer: Peer,
    cache: &Arc<realize_storage::Filesystem>,
    subscriptions: &subscriptions::Client,
    arena: Arena,
) -> Result<(), capnp::Error> {
    let mut request = subscriptions.subscribe_request();
    let mut request_builder = request.get().init_req();
    request_builder.set_arena(arena.as_str());
    if let Some(progress) = cache
        .peer_progress(peer, arena)
        .await
        .map_err(convert::storage_to_capnp_err)?
    {
        let mut builder = request_builder.init_progress();
        builder.set_last_seen(progress.last_seen);
        convert::fill_uuid(builder.init_uuid(), &progress.uuid);
    }
    request.send().promise.await?;
    log::debug!("{arena}@{peer} Tracking");
    Ok(())
}

/// Implement capnp interface Subscriber, defined in
/// `capnp/peer.capnp`.
#[derive(Clone)]
struct SubscriberServer {
    peer: Peer,
    storage: Arc<Storage>,
}

impl SubscriberServer {
    fn new(peer: Peer, storage: Arc<Storage>) -> Self {
        Self { peer, storage }
    }

    fn into_client(self) -> subscriber::Client {
        capnp_rpc::new_client(self)
    }
}

impl subscriber::Server for SubscriberServer {
    fn notify(
        self: Rc<Self>,
        params: NotifyParams,
        _: NotifyResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        Promise::from_future(do_notify(Arc::clone(&self.storage), self.peer, params))
    }
}

async fn do_notify(
    storage: Arc<Storage>,
    peer: Peer,
    params: NotifyParams,
) -> Result<(), capnp::Error> {
    let mut notifications = vec![];
    for n in params.get()?.get_notifications()?.iter() {
        notifications.push(convert::parse_notification(n)?);
    }

    tokio::spawn(async move {
        for notification in notifications {
            storage.update(peer, notification).await?;
        }

        Ok::<(), StorageError>(())
    })
    .await
    .map_err(|e| capnp::Error::failed(e.to_string()))?
    .map_err(|e| capnp::Error::failed(e.to_string()))?;

    Ok(())
}

pub(crate) async fn do_subscriptions(
    peer: Peer,
    storage: Arc<Storage>,
    limiter: Option<Limiter>,
    params: SubscriptionsParams,
    mut results: SubscriptionsResults,
) -> Result<(), capnp::Error> {
    let subscriber = params.get()?.get_subscriber()?;

    let (tx, mut rx) = mpsc::channel(100);
    let (sub_tx, mut sub_rx) = mpsc::channel(1);

    results
        .get()
        .set_subscriptions(Subscriptions::new(sub_tx).into_client());
    tokio::task::spawn_local(async move {
        let mut registered = HashSet::new();
        let mut required = HashMap::new();
        async fn subscribe_missing(
            registered: &mut HashSet<Arena>,
            required: &mut HashMap<Arena, Option<Progress>>,
            storage: &Arc<Storage>,
            tx: &mpsc::Sender<Notification>,
            peer: Peer,
        ) {
            let arenas = required.keys().map(|a| *a).collect::<Vec<_>>();
            for arena in arenas {
                if registered.contains(&arena) {
                    continue;
                }
                let progress = required.get(&arena).cloned().flatten();
                match storage.subscribe(arena, tx.clone(), progress).await {
                    Ok(_) => {
                        registered.insert(arena);
                        required.remove(&arena);
                        log::debug!("[{arena}]@{peer} will send notifications to peer");
                    }
                    Err(StorageError::UnknownArena(_)) => {
                        log::debug!("[{arena}]@{peer} request arena doesn't exist, skipping");
                    }
                    Err(err) => {
                        log::error!("[{arena}] subscription for {peer} failed {err}");
                    }
                }
            }
        }
        let mut notifications = Vec::new();
        let mut arenas = storage.watch_arenas();
        loop {
            tokio::select! {
                Some((arena, progress)) = sub_rx.recv() => {
                    required.insert(arena, progress);
                    subscribe_missing(&mut registered, &mut required, &storage, &tx, peer).await;
                },
                _ = arenas.changed() => {
                    arenas.borrow_and_update();

                    // retry subscribing to requested arenas, as new
                    // arenas might have been added since last time
                    subscribe_missing(&mut registered, &mut required, &storage, &tx, peer).await;
                }
                count = rx.recv_many(&mut notifications, 25) => {
                    if count == 0 {
                        // Channel has been closed
                        return;
                    }

                    let mut request = subscriber.notify_request();
                    let mut builder = request.get().init_notifications(notifications.len() as u32);
                    for (i, n) in std::mem::take(&mut notifications).into_iter().enumerate() {
                        log::trace!("[{}@{peer}] Notify: {n:?}", n.arena());
                        convert::fill_notification(&n, builder.reborrow().get(i as u32));
                    }

                    rate_limit::apply(&limiter, request.get().total_size()).await;
                    if let Err(err) = request.send().promise.await
                    && err.kind == capnp::ErrorKind::Disconnected
                    {
                        return;
                    }
                }
            }
        }
    });

    Ok(())
}

/// Track another peer's arena subscriptions.
struct Subscriptions {
    sub_tx: mpsc::Sender<(Arena, Option<Progress>)>,
}

impl Subscriptions {
    fn new(sub_tx: mpsc::Sender<(Arena, Option<Progress>)>) -> Self {
        Self { sub_tx }
    }

    fn into_client(self) -> subscriptions::Client {
        capnp_rpc::new_client(self)
    }
}

impl subscriptions::Server for Subscriptions {
    fn subscribe(
        self: Rc<Self>,
        params: SubscribeParams,
        _: SubscribeResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let sub_tx = self.sub_tx.clone();
        Promise::from_future(async move {
            let req = params.get()?.get_req()?;

            let arena = convert::parse_arena(req.get_arena()?)?;
            let progress = if req.has_progress() {
                let progress = req.get_progress()?;
                Some(Progress::new(
                    convert::parse_uuid(progress.get_uuid()?),
                    progress.get_last_seen(),
                ))
            } else {
                None
            };
            if let Err(err) = sub_tx.send((arena, progress)).await {
                return Err(capnp::Error::disconnected(err.to_string()));
            }

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::Arc,
        time::{Duration, Instant},
    };

    use crate::rpc::{
        store_capnp::{
            self,
            store::{SubscriptionsParams, SubscriptionsResults},
        },
        testing::HouseholdFixture,
    };
    use assert_fs::{
        TempDir,
        prelude::{FileWriteStr, PathChild, PathCreateDir},
    };
    use capnp::capability::Promise;
    use realize_storage::{Storage, utils::hash};
    use realize_types::{Arena, Peer};
    use tokio::{fs, task::LocalSet};
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn household_subscribes() -> anyhow::Result<()> {
        let mut fixture = HouseholdFixture::setup().await?;
        fixture
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |_, _| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                // A file created in B's arena should eventually become
                // available in cache A.
                let b_dir = fixture.arena_root(b);
                fs::write(&b_dir.join("bar.txt"), b"test").await?;

                fixture
                    .wait_for_file_in_cache(a, "bar.txt", &hash::digest(b"test"))
                    .await?;

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    fn mkdir(tempdir: &TempDir, path: &str) -> PathBuf {
        let child = tempdir.child(path);
        child.create_dir_all().expect("mkdir {path}");
        return child.to_path_buf();
    }

    async fn file_eventually_created(
        storage: &Arc<Storage>,
        arena: Arena,
        path: &str,
    ) -> anyhow::Result<bool> {
        let cache = storage.cache();
        let path = realize_types::Path::parse(path)?;
        let timeout = Instant::now() + Duration::from_secs(15);

        while !cache.metadata((arena, &path)).await.is_ok() && Instant::now() < timeout {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Ok(cache.metadata((arena, &path)).await.is_ok())
    }

    #[tokio::test]
    async fn subscribe_to_new_arenas() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        let tempdir = TempDir::new()?;
        let arena1 = Arena::from("arena1");
        let arena2 = Arena::from("arena2");
        let arena3 = Arena::from("arena3");
        let peer1 = Peer::from("peer1");
        let storage1 =
            realize_storage::testing::storage(mkdir(&tempdir, "storage1"), [arena1, arena2])
                .await?;
        let peer2 = Peer::from("peer2");
        let storage2 =
            realize_storage::testing::storage(mkdir(&tempdir, "storage2"), [arena1, arena3])
                .await?;

        // - arena1 exists in storage1 and storage2
        // - arena2 exists in storage1, not yet in storage2.
        //   This tests arenas added first in the sender, then in the receiver.
        // - arena3 exists in storage2, not yet in storage1.
        //   This tests arenas added first in the receiver, then in the sender.

        // create files in all arenas and all storages
        for arena in [arena1, arena2, arena3] {
            tempdir
                .child(&format!("storage1/{arena}/from_storage1"))
                .write_str("test")?;
        }

        struct FakeStore {
            peer: Peer,
            storage: Arc<Storage>,
        }
        impl store_capnp::store::Server for FakeStore {
            fn subscriptions(
                self: std::rc::Rc<Self>,
                params: SubscriptionsParams,
                results: SubscriptionsResults,
            ) -> Promise<(), capnp::Error> {
                let peer = self.peer;
                let storage = Arc::clone(&self.storage);
                Promise::from_future(async move {
                    super::do_subscriptions(peer, storage, None, params, results).await
                })
            }
        }

        let local = LocalSet::new();
        local
            .run_until(async move {
                // peer1 notifies peer2 about changes
                let fake_store: store_capnp::store::Client = capnp_rpc::new_client(FakeStore {
                    peer: peer1,
                    storage: Arc::clone(&storage1),
                });
                let token = CancellationToken::new();
                super::subscribe_self(&storage2, peer2, fake_store, token.clone()).await?;

                // syncing arena1 works right away, since arena1 exists on both already
                assert!(file_eventually_created(&storage2, arena1, "from_storage1").await?);

                // create arena2 in storage2 and make sure it's synced
                storage2
                    .create_arena(arena2, &mkdir(&tempdir, "storage2/arena2"))
                    .await
                    .unwrap();
                assert!(file_eventually_created(&storage2, arena2, "from_storage1").await?);

                // create arena3 in storage1 and make sure it's synced
                storage1
                    .create_arena(arena3, &mkdir(&tempdir, "storage1/arena3"))
                    .await
                    .unwrap();
                assert!(file_eventually_created(&storage2, arena3, "from_storage1").await?);

                token.cancel();

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }
}
