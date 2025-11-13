use super::convert;
use super::rate_limit;
use crate::rpc::result_capnp;
use crate::rpc::store_capnp::store::{self, SubscribeParams, SubscribeResults};
use crate::rpc::store_capnp::subscriber::{self, NotifyParams, NotifyResults};
use async_speed_limit::Limiter;
use capnp::capability::Promise;
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

    let subscriber = SubscriberServer::new(peer, Arc::clone(storage)).into_client();
    for arena in goal_arenas {
        let mut request = store.subscribe_request();
        let mut request_builder = request.get().init_req();
        request_builder.set_arena(arena.as_str());
        request_builder.set_subscriber(subscriber.clone());
        if let Some(progress) = progress.remove(&arena) {
            let mut builder = request_builder.init_progress();
            builder.set_last_seen(progress.last_seen);
            convert::fill_uuid(builder.init_uuid(), &progress.uuid);
        }

        let reply = request.send().promise.await?;
        let result = reply.get()?.get_result()?;

        if let result_capnp::result::Err(err) = result.which()? {
            return Err(anyhow::anyhow!(err?.get_message()?.to_string()?));
        }
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

pub(crate) async fn do_subscribe(
    peer: Peer,
    storage: Arc<Storage>,
    limiter: Option<Limiter>,
    params: SubscribeParams,
    mut results: SubscribeResults,
) -> Result<(), capnp::Error> {
    let req = params.get()?.get_req()?;
    let arena = convert::parse_arena(req.get_arena()?)?;

    let result = results.get().init_result();
    let progress = if req.has_progress() {
        let progress = req.get_progress()?;
        Some(Progress::new(
            convert::parse_uuid(progress.get_uuid()?),
            progress.get_last_seen(),
        ))
    } else {
        None
    };

    let subscriber = req.get_subscriber()?;

    let (tx, mut rx) = mpsc::channel(100);

    if let Err(err) = tokio::spawn({
        let storage = storage.clone();
        async move {
            storage.subscribe(arena, tx, progress).await?;

            Ok::<(), anyhow::Error>(())
        }
    })
    .await
    {
        result.init_err().set_message(err.to_string());
        return Ok(());
    }

    log::debug!("[{arena}]@{peer} Will report local changes to peer",);
    tokio::task::spawn_local(async move {
        let mut notifications = Vec::new();
        loop {
            let count = rx.recv_many(&mut notifications, 25).await;
            if count == 0 {
                // Channel has been closed
                return;
            }
            log::trace!("[{arena}@{peer}] Notify: {notifications:?}");

            let mut request = subscriber.notify_request();
            let mut builder = request.get().init_notifications(notifications.len() as u32);
            for (i, n) in std::mem::take(&mut notifications).into_iter().enumerate() {
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

    result.init_ok();

    Ok(())
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
