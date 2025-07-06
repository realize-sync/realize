use super::peer_capnp::connected_peer;
use super::result_capnp;
use super::store_capnp::notification;
use super::store_capnp::store::{
    self, ArenasParams, ArenasResults, SubscribeParams, SubscribeResults,
};
use super::store_capnp::subscriber::{self, NotifyParams, NotifyResults};
use crate::model::{Arena, Hash, Path, Peer, UnixTime};
use crate::network::rate_limit::RateLimitedStream;
use crate::network::{Networking, Server};
use crate::storage::{Notification, Progress, Storage, UnrealCacheAsync, UnrealError};
use capnp::capability::Promise;
use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::twoparty::VatNetwork;
use capnp_rpc::{RpcSystem, VatNetwork as _};
use futures::AsyncReadExt;
use futures::io::{BufReader, BufWriter};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::runtime;
use tokio::sync::{broadcast, mpsc};
use tokio::task::{JoinHandle, LocalSet};
use tokio_retry::strategy::ExponentialBackoff;
use tokio_util::compat::TokioAsyncReadCompatExt;
use uuid::Uuid;

/// Identifies Cap'n Proto ConnectedPeer connections.
const TAG: &[u8; 4] = b"PEER";

/// Connection status of a peer, broadcast by [Household].
#[derive(Clone, PartialEq, Debug)]
pub enum PeerStatus {
    Connected(Peer),
    Disconnected(Peer),
}

/// A set of peers and their connections.
///
/// Cap'n Proto connections are handled or their own thread. This
/// object serves as a communication channel between that thread and
/// the rest of the application.
///
/// To listen to incoming connections, call [Household::register].
#[derive(Clone)]
pub struct Household {
    tx: mpsc::UnboundedSender<HouseholdConnection>,
    broadcast_tx: broadcast::Sender<PeerStatus>,
}

impl Household {
    /// Spawn a new RPC thread and return the Household instance that
    /// manages it.
    pub fn spawn(
        networking: Networking,
        storage: Arc<Storage>,
    ) -> anyhow::Result<(Self, thread::JoinHandle<()>)> {
        let (tx, rx) = mpsc::unbounded_channel();
        let (broadcast_tx, _) = broadcast::channel(128);
        let handle = spawn_rpc_thread(networking, storage, rx, broadcast_tx.clone())?;

        Ok((Self { tx, broadcast_tx }, handle))
    }

    /// Report peer status changes through the given receiver.
    pub fn peer_status(&self) -> broadcast::Receiver<PeerStatus> {
        self.broadcast_tx.subscribe()
    }

    /// Keep a client connection up to all peers for which an address is known.
    pub fn keep_connected(&self) -> anyhow::Result<()> {
        self.tx.send(HouseholdConnection::KeepConnected)?;

        Ok(())
    }

    /// Register peer connections to the given server.
    ///
    /// With this call, the server answers to PEER calls as Cap'n Proto
    /// PeerConnection, defined in `capnp/peer.capnp`.
    pub fn register(&self, server: &mut Server) {
        let tx = self.tx.clone();
        server.register_raw(TAG, move |peer, stream, _, shutdown_rx| {
            // TODO: support shutdown_rx
            let _ = tx.send(HouseholdConnection::Incoming {
                peer,
                stream: Box::new(stream),
                shutdown_rx,
            });
        })
    }
}

/// Messages used to communicate with [CapnpRpcThread].
enum HouseholdConnection {
    /// Send incoming (server) TCP connections to the capnp threads to
    /// be handled there.
    Incoming {
        peer: Peer,
        stream: Box<tokio_rustls::server::TlsStream<RateLimitedStream<TcpStream>>>,
        shutdown_rx: broadcast::Receiver<()>,
    },
    KeepConnected,
}

/// Spawns a single thread that handles all Cap'n Proto RPC connections.
///
/// To communicate with the thread, send [HousehholdConnection]s to the channel.
fn spawn_rpc_thread(
    networking: Networking,
    storage: Arc<Storage>,
    mut rx: mpsc::UnboundedReceiver<HouseholdConnection>,
    broadcast_tx: broadcast::Sender<PeerStatus>,
) -> anyhow::Result<thread::JoinHandle<()>> {
    let main_rt = runtime::Handle::current();
    let rt = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    Ok(thread::Builder::new()
        .name("capnprpc".into())
        .spawn(move || {
            let ctx = AppContext::new(networking, storage, broadcast_tx, main_rt);
            let local = LocalSet::new();

            local.spawn_local(async move {
                while let Some(conn) = rx.recv().await {
                    match conn {
                        HouseholdConnection::Incoming {
                            peer,
                            stream,
                            shutdown_rx,
                        } => ctx.accept(peer, stream, shutdown_rx),
                        HouseholdConnection::KeepConnected => ctx.keep_connected(),
                    }
                }
            });

            rt.block_on(local);
        })?)
}

#[derive(Default)]
struct TrackedPeerConnections {
    tracked_client: Option<store::Client>,
    tracker: Option<JoinHandle<()>>,
}

struct AppContext {
    networking: Networking,
    storage: Arc<Storage>,
    broadcast_tx: broadcast::Sender<PeerStatus>,
    main_rt: runtime::Handle,
    connections: RefCell<HashMap<Peer, TrackedPeerConnections>>,
}

impl AppContext {
    fn new(
        networking: Networking,
        storage: Arc<Storage>,
        broadcast_tx: broadcast::Sender<PeerStatus>,
        main_rt: runtime::Handle,
    ) -> Rc<Self> {
        Rc::new(Self {
            networking,
            storage,
            broadcast_tx,
            main_rt,
            connections: RefCell::new(HashMap::new()),
        })
    }

    fn accept(
        self: &Rc<Self>,
        peer: Peer,
        stream: Box<tokio_rustls::server::TlsStream<RateLimitedStream<TcpStream>>>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) {
        let client = ConnectedPeerServer::new(peer.clone(), Rc::clone(self)).into_connected_peer();
        tokio::task::spawn_local(async move {
            let (r, w) = TokioAsyncReadCompatExt::compat(stream).split();
            let mut net = Box::new(VatNetwork::new(
                BufReader::new(r),
                BufWriter::new(w),
                Side::Server,
                Default::default(),
            ));
            let until_shutdown = net.drive_until_shutdown();
            let system = RpcSystem::new(net, Some(client.clone().client));
            let disconnector = system.get_disconnector();
            tokio::task::spawn_local(system);

            tokio::select!(
                _ = shutdown_rx.recv() => {
                    let _ = disconnector.await;
                },
                res = until_shutdown => {
                    if let Err(err) = res {
                        log::debug!("RPC connection from {peer} failed: {err}")
                    }
                }
            );
        });
    }

    fn keep_connected(self: &Rc<Self>) {
        for peer in self.networking.connectable_peers() {
            let mut borrow = self.connections.borrow_mut();
            let conn = borrow.entry(peer.clone()).or_default();
            let has_usable_tracker = conn
                .tracker
                .as_ref()
                .map(|t| !t.is_finished())
                .unwrap_or(false);
            if has_usable_tracker {
                continue;
            }

            let this = Rc::clone(self);
            let peer = peer.clone();
            conn.tracker = Some(tokio::task::spawn_local(async move {
                this.track_peer(&peer).await
            }));
        }
    }

    async fn track_peer(self: &Rc<Self>, peer: &Peer) {
        let retry_strategy =
            ExponentialBackoff::from_millis(500).max_delay(Duration::from_secs(5 * 60));
        let mut current_backoff: Option<ExponentialBackoff> = None;
        loop {
            match &mut current_backoff {
                Some(backoff) => match backoff.next() {
                    None => {
                        log::warn!("Giving up connecting to {peer}");
                        return;
                    }
                    Some(delay) => {
                        tokio::time::sleep(delay).await;
                    }
                },
                None => {
                    // Execute immediately, and install backoff for next time.
                    current_backoff = Some(retry_strategy.clone());
                }
            }
            let stream = match self.networking.connect_raw(peer, TAG, None).await {
                Ok(stream) => stream,
                Err(err) => {
                    log::debug!("Failed to connect to {peer}: {err}; Will retry.");
                    continue;
                }
            };

            let (r, w) = TokioAsyncReadCompatExt::compat(stream).split();
            let mut net = Box::new(VatNetwork::new(
                BufReader::new(r),
                BufWriter::new(w),
                Side::Client,
                Default::default(),
            ));
            let until_shutdown = net.drive_until_shutdown();
            let mut system = RpcSystem::new(net, None);
            let mut client: connected_peer::Client = system.bootstrap(Side::Server);
            let disconnector = system.get_disconnector();
            scopeguard::defer!({
                tokio::task::spawn_local(disconnector);
            });
            tokio::task::spawn_local(system);

            let mut store = match get_connected_peer_store(&mut client).await {
                Ok(store) => store,
                Err(err) => {
                    log::debug!("Failed to get store from {peer}: {err}; Will retry.");
                    continue;
                }
            };
            self.set_tracked_client(peer, Some(store.clone()));
            let _ = self.broadcast_tx.send(PeerStatus::Connected(peer.clone()));
            scopeguard::defer!({
                self.set_tracked_client(peer, None);
                let _ = self
                    .broadcast_tx
                    .send(PeerStatus::Disconnected(peer.clone()));
            });

            //this.register_self(&client);
            if let Err(err) = self.subscribe_self(peer, &mut store).await {
                log::debug!("Failed to subscribe to {peer}: {err}");
            }

            // We're fully connected. Reset the backoff delay for next time.
            current_backoff = None;

            if let Err(err) = until_shutdown.await {
                log::debug!("Connection to {peer} was shutdown: {err}; Will reconnect.")
            }
        }
    }

    async fn subscribe_self(
        self: &Rc<Self>,
        peer: &Peer,
        client: &mut store::Client,
    ) -> anyhow::Result<()> {
        let cache = match self.storage.cache() {
            None => {
                return Ok(());
            }
            Some(c) => c,
        };

        let request = client.arenas_request();
        let reply = request.send().promise.await?;
        let arenas = reply.get()?.get_arenas()?;
        let peer_arenas = parse_arena_set(arenas)?;

        let goal_arenas = cache
            .arenas()
            .filter(|a| peer_arenas.contains(*a))
            .map(|a| a.clone())
            .collect::<Vec<_>>();
        if goal_arenas.is_empty() {
            log::debug!(
                "Not subscribing to {peer}: no common arena. {:?} vs {:?}",
                peer_arenas,
                cache.arenas().collect::<Vec<_>>(),
            );

            return Ok(());
        }
        log::debug!(
            "Subscribe to {} on {peer}",
            goal_arenas
                .iter()
                .map(|a| a.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        let mut progress = self
            .main_rt
            .spawn({
                let peer = peer.clone();
                let goal_arenas = goal_arenas.clone();
                let cache = cache.clone();
                async move {
                    let mut map = HashMap::new();
                    for arena in goal_arenas {
                        if let Some(progress) = cache.peer_progress(&peer, &arena).await? {
                            map.insert(arena, progress);
                        }
                    }

                    Ok::<_, anyhow::Error>(map)
                }
            })
            .await??;

        for arena in goal_arenas {
            let mut request = client.subscribe_request();
            let mut request_builder = request.get().init_req();
            request_builder.set_arena(arena.as_str());
            request_builder.set_subscriber(
                ConnectedPeerServer::new(peer.clone(), self.clone()).into_subscriber(),
            );
            if let Some(progress) = progress.remove(&arena) {
                let mut builder = request_builder.init_progress();
                builder.set_last_seen(progress.last_seen);
                fill_uuid(builder.init_uuid(), &progress.uuid);
            }

            let reply = request.send().promise.await?;
            let result = reply.get()?.get_result()?;

            if let result_capnp::result::Err(err) = result.which()? {
                return Err(anyhow::anyhow!(err?.get_message()?.to_string()?));
            }
        }

        Ok(())
    }

    /// Associate the given client with the peer.
    fn set_tracked_client(self: &Rc<Self>, peer: &Peer, client: Option<store::Client>) {
        self.connections
            .borrow_mut()
            .entry(peer.clone())
            .or_default()
            .tracked_client = client;
    }
}

async fn get_connected_peer_store(
    client: &mut connected_peer::Client,
) -> anyhow::Result<store::Client> {
    let request = client.store_request();
    let reply = request.send().promise.await?;
    let store = reply.get()?.get_store()?;

    Ok(store)
}

fn parse_arena(reader: capnp::text::Reader<'_>) -> Result<Arena, capnp::Error> {
    Ok(Arena::from(reader.to_str()?))
}

fn parse_arena_set(arenas: capnp::text_list::Reader<'_>) -> Result<HashSet<Arena>, capnp::Error> {
    let mut set = HashSet::new();
    for arena in arenas.iter() {
        set.insert(parse_arena(arena?)?);
    }
    Ok(set)
}

fn parse_uuid(reader: super::store_capnp::uuid::Reader<'_>) -> Uuid {
    Uuid::from_u64_pair(reader.get_hi(), reader.get_lo())
}

fn parse_mtime(reader: super::store_capnp::time::Reader<'_>) -> UnixTime {
    UnixTime::new(reader.get_secs(), reader.get_nsecs())
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

/// Implement capnp interface ConnectedPeer, defined in
/// `capnp/peer.capnp`.
#[derive(Clone)]
struct ConnectedPeerServer {
    peer: Peer,
    ctx: Rc<AppContext>,
}

impl ConnectedPeerServer {
    fn new(peer: Peer, ctx: Rc<AppContext>) -> Self {
        Self { peer, ctx }
    }

    fn into_connected_peer(self) -> connected_peer::Client {
        capnp_rpc::new_client(self)
    }

    fn into_store(self) -> store::Client {
        capnp_rpc::new_client(self)
    }

    fn into_subscriber(self) -> subscriber::Client {
        capnp_rpc::new_client(self)
    }
}

impl connected_peer::Server for ConnectedPeerServer {
    fn store(
        &mut self,
        _params: connected_peer::StoreParams,
        mut results: connected_peer::StoreResults,
    ) -> Promise<(), capnp::Error> {
        results.get().set_store(self.clone().into_store());

        Promise::ok(())
    }
}

impl store::Server for ConnectedPeerServer {
    fn arenas(&mut self, _: ArenasParams, mut results: ArenasResults) -> Promise<(), capnp::Error> {
        let arenas = self
            .ctx
            .storage
            .indexed_arenas()
            .map(|a| a.clone())
            .collect::<Vec<_>>();
        let mut list = results.get().init_arenas(arenas.len() as u32);
        for (i, arena) in arenas.into_iter().enumerate() {
            list.set(i as u32, arena.as_str());
        }

        Promise::ok(())
    }

    fn subscribe(
        &mut self,
        params: SubscribeParams,
        results: SubscribeResults,
    ) -> Promise<(), capnp::Error> {
        Promise::from_future(do_subscribe(
            self.peer.clone(),
            Rc::clone(&self.ctx),
            params,
            results,
        ))
    }
}

impl subscriber::Server for ConnectedPeerServer {
    fn notify(
        &mut self,
        params: NotifyParams,
        _: NotifyResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        if let Some(cache) = self.ctx.storage.cache() {
            Promise::from_future(do_notify(
                cache.clone(),
                self.ctx.main_rt.clone(),
                self.peer.clone(),
                params,
            ))
        } else {
            Promise::ok(())
        }
    }
}

async fn do_notify(
    cache: UnrealCacheAsync,
    main_rt: runtime::Handle,
    peer: Peer,
    params: NotifyParams,
) -> Result<(), capnp::Error> {
    let mut notifications = vec![];
    for n in params.get()?.get_notifications()?.iter() {
        notifications.push(match n.which()? {
            notification::Which::Add(add) => {
                let add = add?;

                Notification::Add {
                    arena: parse_arena(add.get_arena()?)?,
                    index: add.get_index(),
                    path: parse_path(add.get_path()?)?,
                    size: add.get_size(),
                    mtime: parse_mtime(add.get_mtime()?),
                    hash: parse_hash(add.get_hash()?)?,
                }
            }
            notification::Which::Replace(replace) => {
                let replace = replace?;

                Notification::Replace {
                    arena: parse_arena(replace.get_arena()?)?,
                    index: replace.get_index(),
                    path: parse_path(replace.get_path()?)?,
                    mtime: parse_mtime(replace.get_mtime()?),
                    size: replace.get_size(),
                    hash: parse_hash(replace.get_hash()?)?,
                    old_hash: parse_hash(replace.get_old_hash()?)?,
                }
            }
            notification::Which::Remove(remove) => {
                let remove = remove?;

                Notification::Remove {
                    arena: parse_arena(remove.get_arena()?)?,
                    index: remove.get_index(),
                    path: parse_path(remove.get_path()?)?,
                    old_hash: parse_hash(remove.get_old_hash()?)?,
                }
            }
            notification::Which::CatchupStart(start) => {
                Notification::CatchupStart(parse_arena(start?.get_arena()?)?)
            }
            notification::Which::Catchup(catchup) => {
                let catchup = catchup?;

                Notification::Catchup {
                    arena: parse_arena(catchup.get_arena()?)?,
                    path: parse_path(catchup.get_path()?)?,
                    size: catchup.get_size(),
                    mtime: parse_mtime(catchup.get_mtime()?),
                    hash: parse_hash(catchup.get_hash()?)?,
                }
            }
            notification::Which::CatchupComplete(complete) => {
                let complete = complete?;

                Notification::CatchupComplete {
                    arena: parse_arena(complete.get_arena()?)?,
                    index: complete.get_index(),
                }
            }
            notification::Which::Connected(connected) => {
                let connected = connected?;

                Notification::Connected {
                    arena: parse_arena(connected.get_arena()?)?,
                    uuid: parse_uuid(connected.get_uuid()?),
                }
            }
        });
    }

    main_rt
        .spawn(async move {
            for notification in notifications {
                cache.update(&peer, notification).await?;
            }

            Ok::<(), UnrealError>(())
        })
        .await
        .map_err(|e| capnp::Error::failed(e.to_string()))?
        .map_err(|e| capnp::Error::failed(e.to_string()))?;

    Ok(())
}

async fn do_subscribe(
    peer: Peer,
    ctx: Rc<AppContext>,
    params: SubscribeParams,
    mut results: SubscribeResults,
) -> Result<(), capnp::Error> {
    let req = params.get()?.get_req()?;
    let arena = parse_arena(req.get_arena()?)?;

    let result = results.get().init_result();
    let progress = if req.has_progress() {
        let progress = req.get_progress()?;
        Some(Progress::new(
            parse_uuid(progress.get_uuid()?),
            progress.get_last_seen(),
        ))
    } else {
        None
    };

    let subscriber = req.get_subscriber()?;

    let (tx, mut rx) = mpsc::channel(100);

    let storage = ctx.storage.clone();
    if let Err(err) = ctx
        .main_rt
        .spawn({
            let arena = arena.clone();
            async move {
                storage.subscribe(&arena, tx, progress).await?;

                Ok::<(), anyhow::Error>(())
            }
        })
        .await
    {
        result.init_err().set_message(err.to_string());
        return Ok(());
    }

    log::debug!("{peer} subscribed to notifications from {arena}");
    tokio::task::spawn_local(async move {
        let mut notifications = Vec::new();
        loop {
            let count = rx.recv_many(&mut notifications, 25).await;
            if count == 0 {
                // Channel has been closed
                return;
            }
            log::debug!("notify {peer}: {notifications:?}");
            if let Err(err) = send_notifications(notifications.as_slice(), &subscriber).await {
                if err.kind == capnp::ErrorKind::Disconnected {
                    return;
                }
            }
            notifications.clear();
        }
    });

    result.init_ok();

    Ok(())
}

async fn send_notifications(
    notifications: &[Notification],
    client: &subscriber::Client,
) -> Result<(), capnp::Error> {
    let mut request = client.notify_request();
    let mut builder = request.get().init_notifications(notifications.len() as u32);
    for (i, notif) in notifications.iter().enumerate() {
        let notif_builder = builder.reborrow().get(i as u32);
        match notif {
            Notification::Add {
                arena,
                index,
                path,
                size,
                mtime,
                hash,
            } => fill_add(
                notif_builder.init_add(),
                arena,
                *index,
                path,
                *size,
                mtime,
                hash,
            ),

            Notification::Replace {
                arena,
                index,
                path,
                size,
                mtime,
                hash,
                old_hash,
            } => fill_replace(
                notif_builder.init_replace(),
                arena,
                *index,
                path,
                *size,
                mtime,
                hash,
                old_hash,
            ),

            Notification::Remove {
                arena,
                index,
                path,
                old_hash,
            } => fill_remove(notif_builder.init_remove(), arena, *index, path, old_hash),

            Notification::Catchup {
                arena,
                path,
                size,
                mtime,
                hash,
            } => fill_catchup(
                notif_builder.init_catchup(),
                arena,
                path,
                *size,
                mtime,
                hash,
            ),

            Notification::CatchupStart(arena) => {
                notif_builder.init_catchup_start().set_arena(arena.as_str())
            }

            Notification::CatchupComplete { arena, index } => {
                let mut builder = notif_builder.init_catchup_complete();
                builder.set_arena(arena.as_str());
                builder.set_index(*index);
            }

            Notification::Connected { arena, uuid } => {
                let mut builder = notif_builder.init_connected();
                builder.set_arena(arena.as_str());
                fill_uuid(builder.init_uuid(), &uuid);
            }
        }
    }
    let _ = request.send().promise.await?;

    Ok(())
}

fn fill_uuid(mut builder: super::store_capnp::uuid::Builder<'_>, uuid: &Uuid) {
    let (hi, lo) = uuid.as_u64_pair();
    builder.set_hi(hi);
    builder.set_lo(lo);
}

fn fill_add(
    mut builder: super::store_capnp::add::Builder<'_>,
    arena: &Arena,
    index: u64,
    path: &crate::model::Path,
    size: u64,
    mtime: &crate::model::UnixTime,
    hash: &crate::model::Hash,
) {
    builder.set_arena(arena.as_str());
    builder.set_index(index);
    builder.set_path(path.as_str());
    builder.set_size(size);
    builder.set_hash(&hash.0);
    fill_time(builder.init_mtime(), mtime);
}

fn fill_replace(
    mut builder: super::store_capnp::replace::Builder<'_>,
    arena: &Arena,
    index: u64,
    path: &crate::model::Path,
    size: u64,
    mtime: &crate::model::UnixTime,
    hash: &crate::model::Hash,
    old_hash: &crate::model::Hash,
) {
    builder.set_arena(arena.as_str());
    builder.set_index(index);
    builder.set_path(path.as_str());
    builder.set_size(size);
    builder.set_hash(&hash.0);
    builder.set_old_hash(&old_hash.0);
    fill_time(builder.init_mtime(), mtime);
}

fn fill_remove(
    mut builder: super::store_capnp::remove::Builder<'_>,
    arena: &Arena,
    index: u64,
    path: &crate::model::Path,
    old_hash: &crate::model::Hash,
) {
    builder.set_arena(arena.as_str());
    builder.set_index(index);
    builder.set_path(path.as_str());
    builder.set_old_hash(&old_hash.0);
}

fn fill_catchup(
    mut builder: super::store_capnp::catchup::Builder<'_>,
    arena: &Arena,
    path: &crate::model::Path,
    size: u64,
    mtime: &crate::model::UnixTime,
    hash: &crate::model::Hash,
) {
    builder.set_arena(arena.as_str());
    builder.set_path(path.as_str());
    builder.set_size(size);
    builder.set_hash(&hash.0);
    fill_time(builder.init_mtime(), mtime);
}

fn fill_time(
    mut mtime_builder: super::store_capnp::time::Builder<'_>,
    mtime: &crate::model::UnixTime,
) {
    mtime_builder.set_secs(mtime.as_secs());
    mtime_builder.set_nsecs(mtime.subsec_nanos());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Arena;
    use crate::network::hostport::HostPort;
    use crate::network::testing::TestingPeers;
    use crate::storage;
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::fs;
    use tokio::task::LocalSet;
    use tokio_retry::strategy::FixedInterval;

    fn a() -> Peer {
        TestingPeers::a()
    }
    fn b() -> Peer {
        TestingPeers::b()
    }
    fn c() -> Peer {
        TestingPeers::c()
    }

    fn test_arena() -> Arena {
        Arena::from("myarena")
    }

    struct Fixture {
        peers: TestingPeers,
        peer_storage: HashMap<Peer, Arc<Storage>>,
        tempdir: TempDir,
    }
    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();

            let tempdir = TempDir::new()?;
            let mut peer_storage = HashMap::new();
            for peer in [a(), b(), c()] {
                let s =
                    storage::testing::storage(tempdir.child(peer.as_str()).path(), [test_arena()])
                        .await?;
                peer_storage.insert(peer, s);
            }
            Ok(Self {
                peers: TestingPeers::new()?,
                peer_storage,
                tempdir,
            })
        }

        fn arena_root(&self, peer: &Peer) -> PathBuf {
            storage::testing::arena_root(self.tempdir.child(peer.as_str()).path(), &test_arena())
        }

        fn household(&self, peer: &Peer) -> anyhow::Result<(Household, thread::JoinHandle<()>)> {
            let storage = self
                .peer_storage
                .get(peer)
                .ok_or_else(|| anyhow::anyhow!("unknown peer: {peer}"))?;

            Household::spawn(self.peers.networking(peer)?, storage.clone())
        }

        async fn run_server(
            &mut self,
            peer: &Peer,
            household: &Household,
        ) -> anyhow::Result<Arc<Server>> {
            let mut server = Server::new(self.peers.networking(peer)?);
            household.register(&mut server);

            let server = Arc::new(server);

            let configured = self.peers.hostport(peer).await;
            let addr = server
                .listen(configured.unwrap_or(&HostPort::localhost(0)))
                .await?;
            if configured.is_none() {
                self.peers.set_addr(peer, addr);
            }

            Ok(server)
        }
    }

    async fn connect(
        networking: Networking,
        peer: &Peer,
    ) -> anyhow::Result<connected_peer::Client> {
        let stream = networking.connect_raw(peer, TAG, None).await?;
        let (r, w) = TokioAsyncReadCompatExt::compat(stream).split();
        let net = Box::new(VatNetwork::new(
            BufReader::new(r),
            BufWriter::new(w),
            Side::Client,
            Default::default(),
        ));
        let mut system = RpcSystem::new(net, None);
        let client: connected_peer::Client = system.bootstrap(Side::Server);
        tokio::task::spawn_local(system);

        Ok(client)
    }

    #[tokio::test]
    async fn household_listens() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;

        let a = &a();
        let (household, _) = fixture.household(a)?;
        let _server = fixture.run_server(a, &household).await?;

        let b_networking = fixture.peers.networking(&b())?;
        LocalSet::new()
            .run_until(async move {
                let client = connect(b_networking, a).await?;

                let request = client.store_request();
                let reply = request.send().promise.await?;
                let store = reply.get()?.get_store()?;

                let request = store.arenas_request();
                let reply = request.send().promise.await?;
                let arenas = reply.get()?.get_arenas()?;
                assert_eq!(1, arenas.len());
                assert_eq!(test_arena().as_str(), arenas.get(0)?.to_str()?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn capnprpc_thread_shutdown() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;

        let a = &a();
        let (household, thread_join) = fixture.household(a)?;
        let server = fixture.run_server(a, &household).await?;

        let b_networking = fixture.peers.networking(&b())?;
        LocalSet::new()
            .run_until(async move {
                let client = connect(b_networking, a).await?;
                let request = client.store_request();
                let reply = request.send().promise.await?;
                assert!(reply.get()?.has_store());

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        // The thread shuts down once the channel is unused.
        server.shutdown().await?;
        drop(server);
        drop(household);
        assert!(thread_join.join().is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn household_connects() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;

        let a = &a();
        fixture.peers.pick_port(a)?;

        let b = &b();
        fixture.peers.pick_port(b)?;

        let (household_a, _) = fixture.household(a)?;
        let _server_a = fixture.run_server(a, &household_a).await?;

        let (household_b, _) = fixture.household(b)?;
        let _server_b = fixture.run_server(b, &household_b).await?;

        let mut status_a = household_a.peer_status();
        let mut status_b = household_b.peer_status();

        household_a.keep_connected()?;
        assert_eq!(PeerStatus::Connected(b.clone()), status_a.recv().await?);

        household_b.keep_connected()?;
        assert_eq!(PeerStatus::Connected(a.clone()), status_b.recv().await?);

        Ok(())
    }

    #[tokio::test]
    async fn household_reconnects() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;

        let a = &a();
        fixture.peers.pick_port(a)?;

        let b = &b();
        fixture.peers.pick_port(b)?;

        let (household_a, _) = fixture.household(a)?;
        let _server_a = fixture.run_server(a, &household_a).await?;

        let (household_b, _) = fixture.household(b)?;
        let server_b = fixture.run_server(b, &household_b).await?;

        let mut status_a = household_a.peer_status();
        household_a.keep_connected()?;

        assert_eq!(PeerStatus::Connected(b.clone()), status_a.recv().await?);
        server_b.shutdown().await?;
        assert_eq!(PeerStatus::Disconnected(b.clone()), status_a.recv().await?);

        let _server_b = fixture.run_server(b, &household_b).await?;
        assert_eq!(PeerStatus::Connected(b.clone()), status_a.recv().await?);

        Ok(())
    }

    #[tokio::test]
    async fn household_subscribes() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;

        let a = &a();
        fixture.peers.pick_port(a)?;

        let b = &b();
        fixture.peers.pick_port(b)?;

        let (household_a, _) = fixture.household(a)?;
        let _server_a = fixture.run_server(a, &household_a).await?;

        let (household_b, _) = fixture.household(b)?;
        let _server_b = fixture.run_server(b, &household_b).await?;
        household_a.keep_connected()?;

        // A file created in B's arena should eventually become
        // available in cache A.
        let b_dir = fixture.arena_root(&b);
        fs::write(&b_dir.join("bar.txt"), b"test").await?;

        let mut retry = FixedInterval::new(Duration::from_millis(50)).take(100);
        let a_cache = &fixture.peer_storage.get(a).unwrap().cache().unwrap();
        let arena_inode = a_cache.arena_root(&test_arena())?;
        while a_cache.lookup(arena_inode, "bar.txt").await.is_err() {
            if let Some(delay) = retry.next() {
                tokio::time::sleep(delay).await;
            } else {
                panic!("bar.txt was never added to the cache");
            }
        }

        Ok(())
    }
}
