use super::convert;
use super::rate_limit;
use crate::rpc::store_capnp::store::{self, SubscriptionsParams, SubscriptionsResults};
use crate::rpc::store_capnp::subscriber::{self, NotifyParams, NotifyResults};
use crate::rpc::store_capnp::subscriptions::{self, SubscribeParams, SubscribeResults};
use async_speed_limit::Limiter;
use capnp::capability::Promise;
use realize_storage::Notification;
use realize_storage::{Progress, Storage, StorageError};
use realize_types::Peer;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Subscribe to notifications from the given client and use it to
/// update the cache.
pub(crate) async fn subscribe_self(
    storage: &Arc<Storage>,
    peer: Peer,
    store: store::Client,
) -> anyhow::Result<()> {
    let cache = storage.cache();
    let request = store.arenas_request();
    let reply = request.send().promise.await?;
    let arenas = reply.get()?.get_arenas()?;
    let peer_arenas = convert::parse_arena_set(arenas)?;
    log::debug!(
        "@{peer} Arenas: {}",
        peer_arenas
            .iter()
            .map(|a| a.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );

    let goal_arenas = cache
        .arenas()
        .filter(|a| peer_arenas.contains(a))
        .map(|a| a.clone())
        .collect::<Vec<_>>();
    if goal_arenas.is_empty() {
        log::debug!(
            "@{peer} No common arenas. peer: {:?} vs local: {:?}",
            peer_arenas,
            cache.arenas().collect::<Vec<_>>(),
        );

        return Ok(());
    }
    for arena in &goal_arenas {
        log::debug!("[{arena}]@{peer} Tracking")
    }
    let mut progress = tokio::spawn({
        let goal_arenas = goal_arenas.clone();
        let cache = cache.clone();
        async move {
            let mut map = HashMap::new();
            for arena in goal_arenas {
                if let Some(progress) = cache.peer_progress(peer, arena).await? {
                    map.insert(arena, progress);
                }
            }

            Ok::<_, anyhow::Error>(map)
        }
    })
    .await??;

    let mut request = store.subscriptions_request();
    request
        .get()
        .set_subscriber(SubscriberServer::new(peer, Arc::clone(storage)).into_client());
    let subscriptions = request.send().promise.await?.get()?.get_subscriptions()?;

    for arena in goal_arenas {
        let mut request = subscriptions.subscribe_request();
        let mut request_builder = request.get().init_req();
        request_builder.set_arena(arena.as_str());
        if let Some(progress) = progress.remove(&arena) {
            let mut builder = request_builder.init_progress();
            builder.set_last_seen(progress.last_seen);
            convert::fill_uuid(builder.init_uuid(), &progress.uuid);
        }

        request.send().promise.await?;
    }

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
        &mut self,
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

    results
        .get()
        .set_subscriptions(Subscriptions::new(tx, Arc::clone(&storage)).into_client());
    tokio::task::spawn_local(async move {
        let mut notifications = Vec::new();
        loop {
            let count = rx.recv_many(&mut notifications, 25).await;
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
    });

    Ok(())
}

/// Track another peer's arena subscriptions.
struct Subscriptions {
    storage: Arc<Storage>,
    tx: mpsc::Sender<Notification>,
}

impl Subscriptions {
    fn new(tx: mpsc::Sender<Notification>, storage: Arc<Storage>) -> Self {
        Self { tx, storage }
    }

    fn into_client(self) -> subscriptions::Client {
        capnp_rpc::new_client(self)
    }
}

impl subscriptions::Server for Subscriptions {
    fn subscribe(
        &mut self,
        params: SubscribeParams,
        _: SubscribeResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let tx = self.tx.clone();
        let storage = Arc::clone(&self.storage);
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
            storage
                .subscribe(arena, tx, progress)
                .await
                .map_err(convert::storage_to_capnp_err)?;

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::rpc::testing::HouseholdFixture;
    use realize_storage::utils::hash;
    use tokio::fs;

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
}
