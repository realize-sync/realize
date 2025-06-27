use super::peer_capnp::connected_peer;
use super::result_capnp;
use super::store_capnp::notification;
use super::store_capnp::store::{
    self, ArenasParams, ArenasResults, SubscribeParams, SubscribeResults,
};
use super::store_capnp::subscriber::{self, NotifyParams, NotifyResults};
use crate::model::{Arena, Path, Peer, UnixTime};
use crate::network::rate_limit::RateLimitedStream;
use crate::network::{Networking, Server};
use crate::storage::real::{Notification, StoreSubscribe};
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

/// Subset of [RealStore] Household requires.
type StoreType = Arc<dyn StoreSubscribe + Sync + Send>;

impl Household {
    /// Spawn a new RPC thread and return the Household instance that
    /// manages it.
    pub fn spawn(
        networking: Networking,
        store: StoreType,
        notification_tx: Option<mpsc::Sender<(Peer, Notification)>>,
    ) -> anyhow::Result<(Self, thread::JoinHandle<()>)> {
        let (tx, rx) = mpsc::unbounded_channel();
        let (broadcast_tx, _) = broadcast::channel(128);
        let handle =
            spawn_rpc_thread(networking, store, rx, notification_tx, broadcast_tx.clone())?;

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
                stream,
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
        stream: tokio_rustls::server::TlsStream<RateLimitedStream<TcpStream>>,
        shutdown_rx: broadcast::Receiver<()>,
    },
    KeepConnected,
}

/// Spawns a single thread that handles all Cap'n Proto RPC connections.
///
/// To communicate with the thread, send [HousehholdConnection]s to the channel.
fn spawn_rpc_thread(
    networking: Networking,
    store: StoreType,
    mut rx: mpsc::UnboundedReceiver<HouseholdConnection>,
    notification_tx: Option<mpsc::Sender<(Peer, Notification)>>,
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
            let ctx = AppContext::new(networking, store, notification_tx, broadcast_tx, main_rt);
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
    store: StoreType,
    broadcast_tx: broadcast::Sender<PeerStatus>,
    notification_tx: Option<mpsc::Sender<(Peer, Notification)>>,
    main_rt: runtime::Handle,
    connections: RefCell<HashMap<Peer, TrackedPeerConnections>>,
}

impl AppContext {
    fn new(
        networking: Networking,
        store: StoreType,
        notification_tx: Option<mpsc::Sender<(Peer, Notification)>>,
        broadcast_tx: broadcast::Sender<PeerStatus>,
        main_rt: runtime::Handle,
    ) -> Rc<Self> {
        Rc::new(Self {
            networking,
            store,
            notification_tx,
            broadcast_tx,
            main_rt,
            connections: RefCell::new(HashMap::new()),
        })
    }

    fn accept(
        self: &Rc<Self>,
        peer: Peer,
        stream: tokio_rustls::server::TlsStream<RateLimitedStream<TcpStream>>,
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
        if self.notification_tx.is_none() {
            return Ok(());
        }

        let request = client.arenas_request();
        let reply = request.send().promise.await?;
        let arenas = reply.get()?.get_arenas()?;
        let peer_arenas = parse_arena_set(arenas)?;

        let mut goal_arenas = self.store.arenas();
        goal_arenas.retain(|a| peer_arenas.contains(a));
        if goal_arenas.is_empty() {
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

        let mut request = client.subscribe_request();
        let mut request_builder = request.get().init_req();
        let mut arenas_builder = request_builder
            .reborrow()
            .init_arenas(goal_arenas.len() as u32);
        for (i, arena) in goal_arenas.into_iter().enumerate() {
            arenas_builder.set(i as u32, arena.as_str());
        }
        request_builder
            .set_subscriber(ConnectedPeerServer::new(peer.clone(), self.clone()).into_subscriber());

        let reply = request.send().promise.await?;
        let result = reply.get()?.get_result()?;

        if let result_capnp::result::Err(err) = result.which()? {
            return Err(anyhow::anyhow!(err?.get_message()?.to_string()?));
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

fn parse_mtime(reader: super::store_capnp::time::Reader<'_>) -> UnixTime {
    UnixTime::new(reader.get_secs(), reader.get_nsecs())
}

fn parse_path(reader: capnp::text::Reader<'_>) -> Result<Path, capnp::Error> {
    Path::parse(reader.to_str()?).map_err(|e| capnp::Error::failed(e.to_string()))
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
        let arenas = self.ctx.store.arenas();
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
        Promise::from_future(do_subscribe(Rc::clone(&self.ctx), params, results))
    }
}

impl subscriber::Server for ConnectedPeerServer {
    fn notify(
        &mut self,
        params: NotifyParams,
        _: NotifyResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        if let Some(tx) = &self.ctx.notification_tx {
            Promise::from_future(do_notify(tx.clone(), self.peer.clone(), params))
        } else {
            Promise::ok(())
        }
    }
}

async fn do_notify(
    notification_tx: mpsc::Sender<(Peer, Notification)>,
    peer: Peer,
    params: NotifyParams,
) -> Result<(), capnp::Error> {
    for n in params.get()?.get_notifications()?.iter() {
        let to_send = match n.which()? {
            notification::Which::Link(link) => {
                let link = link?;

                Notification::Link {
                    arena: parse_arena(link.get_arena()?)?,
                    path: parse_path(link.get_path()?)?,
                    size: link.get_size(),
                    mtime: parse_mtime(link.get_mtime()?),
                }
            }
            notification::Which::Unlink(unlink) => {
                let unlink = unlink?;

                Notification::Unlink {
                    arena: parse_arena(unlink.get_arena()?)?,
                    path: parse_path(unlink.get_path()?)?,
                    mtime: parse_mtime(unlink.get_mtime()?),
                }
            }
            notification::Which::CatchingUp(catching_up) => Notification::CatchingUp {
                arena: parse_arena(catching_up?.get_arena()?)?,
            },
            notification::Which::Catchup(catchup) => {
                let catchup = catchup?;

                Notification::Catchup {
                    arena: parse_arena(catchup.get_arena()?)?,
                    path: parse_path(catchup.get_path()?)?,
                    size: catchup.get_size(),
                    mtime: parse_mtime(catchup.get_mtime()?),
                }
            }
            notification::Which::Ready(ready) => Notification::Ready {
                arena: parse_arena(ready?.get_arena()?)?,
            },
        };
        let _ = notification_tx.send((peer.clone(), to_send)).await;
    }

    Ok(())
}

async fn do_subscribe(
    ctx: Rc<AppContext>,
    params: SubscribeParams,
    mut results: SubscribeResults,
) -> Result<(), capnp::Error> {
    let req = params.get()?.get_req()?;
    let result = results.get().init_result();
    let arenas = parse_arena_set(req.get_arenas()?)?;
    if arenas.is_empty() {
        result.init_ok();
        return Ok(());
    }

    let subscriber = req.get_subscriber()?;

    let (tx, mut rx) = mpsc::channel(100);
    let store = Arc::clone(&ctx.store);
    if let Err(err) = ctx
        .main_rt
        .spawn(async move {
            for arena in arenas {
                store.subscribe(arena, tx.clone(), true)?;
            }

            Ok::<(), anyhow::Error>(())
        })
        .await
    {
        result.init_err().set_message(err.to_string());
        return Ok(());
    }

    tokio::task::spawn_local(async move {
        let mut notifications = Vec::new();
        loop {
            let count = rx.recv_many(&mut notifications, 25).await;
            if count == 0 {
                // Channel has been closed
                return;
            }
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
            Notification::Link {
                arena,
                path,
                size,
                mtime,
            } => fill_link(notif_builder.init_link(), arena, path, *size, mtime),

            Notification::Catchup {
                arena,
                path,
                size,
                mtime,
            } => fill_link(notif_builder.init_catchup(), arena, path, *size, mtime),

            Notification::Unlink { arena, path, mtime } => {
                fill_unlink(notif_builder.init_unlink(), arena, path, mtime)
            }

            Notification::CatchingUp { arena } => {
                notif_builder.init_catching_up().set_arena(arena.as_str())
            }

            Notification::Ready { arena } => notif_builder.init_ready().set_arena(arena.as_str()),
        }
    }
    let _ = request.send().promise.await?;

    Ok(())
}

fn fill_link(
    mut builder: super::store_capnp::link::Builder<'_>,
    arena: &Arena,
    path: &crate::model::Path,
    size: u64,
    mtime: &crate::model::UnixTime,
) {
    builder.set_arena(arena.as_str());
    builder.set_path(path.as_str());
    builder.set_size(size);
    fill_time(builder.init_mtime(), mtime);
}

fn fill_unlink(
    mut builder: super::store_capnp::unlink::Builder<'_>,
    arena: &Arena,
    path: &crate::model::Path,
    mtime: &crate::model::UnixTime,
) {
    builder.set_arena(arena.as_str());
    builder.set_path(path.as_str());
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
    use std::sync::Arc;

    use tokio::{task::LocalSet, time::timeout};

    use crate::{
        model::Arena,
        network::{hostport::HostPort, testing::TestingPeers},
        storage::testing::FakeStoreSubscribe,
    };

    use super::*;

    struct Fixture {
        arena: Arena,
        peers: TestingPeers,
        peer_a: Peer,
        #[allow(dead_code)]
        peer_b: Peer,
        #[allow(dead_code)]
        peer_c: Peer,
        stores: HashMap<Peer, Arc<FakeStoreSubscribe>>,
    }
    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();

            let arena = Arena::from("test");

            let mut stores = HashMap::new();
            for peer in [TestingPeers::a(), TestingPeers::b(), TestingPeers::c()] {
                stores.insert(peer, FakeStoreSubscribe::new(vec![arena.clone()]));
            }
            Ok(Self {
                arena,
                stores,
                peers: TestingPeers::new()?,
                peer_a: TestingPeers::a(),
                peer_b: TestingPeers::b(),
                peer_c: TestingPeers::c(),
            })
        }

        fn household(
            &self,
            peer: &Peer,
            notification_tx: Option<mpsc::Sender<(Peer, Notification)>>,
        ) -> anyhow::Result<(Household, thread::JoinHandle<()>)> {
            let store: Arc<dyn StoreSubscribe + Send + Sync> = self
                .stores
                .get(peer)
                .ok_or(anyhow::anyhow!("No store defined for {peer}"))?
                .clone();
            Household::spawn(self.peers.networking(peer)?, store, notification_tx)
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

        let peer = &fixture.peer_a.clone();
        let (household, _) = fixture.household(peer, None)?;
        let _server = fixture.run_server(peer, &household).await?;

        let networking = fixture.peers.networking(&fixture.peer_b)?;
        LocalSet::new()
            .run_until(async move {
                let client = connect(networking, peer).await?;

                let request = client.store_request();
                let reply = request.send().promise.await?;
                let store = reply.get()?.get_store()?;

                let request = store.arenas_request();
                let reply = request.send().promise.await?;
                let arenas = reply.get()?.get_arenas()?;
                assert_eq!(1, arenas.len());
                assert_eq!(fixture.arena.as_str(), arenas.get(0)?.to_str()?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn capnprpc_thread_shutdown() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;

        let peer = &fixture.peer_a.clone();
        let (household, thread_join) = fixture.household(peer, None)?;
        let server = fixture.run_server(peer, &household).await?;

        let networking = fixture.peers.networking(&fixture.peer_b)?;
        LocalSet::new()
            .run_until(async move {
                let client = connect(networking, peer).await?;
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

        let a = &fixture.peer_a.clone();
        fixture.peers.pick_port(a)?;

        let b = &fixture.peer_b.clone();
        fixture.peers.pick_port(b)?;

        let (household_a, _) = fixture.household(a, None)?;
        let _server_a = fixture.run_server(a, &household_a).await?;

        let (household_b, _) = fixture.household(b, None)?;
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

        let a = &fixture.peer_a.clone();
        fixture.peers.pick_port(a)?;

        let b = &fixture.peer_b.clone();
        fixture.peers.pick_port(b)?;

        let (household_a, _) = fixture.household(a, None)?;
        let _server_a = fixture.run_server(a, &household_a).await?;

        let (household_b, _) = fixture.household(b, None)?;
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

        let a = &fixture.peer_a.clone();
        fixture.peers.pick_port(a)?;

        let b = &fixture.peer_b.clone();
        fixture.peers.pick_port(b)?;

        let (tx, mut rx) = mpsc::channel(10);
        let (household_a, _) = fixture.household(a, Some(tx))?;
        let _server_a = fixture.run_server(a, &household_a).await?;

        let (household_b, _) = fixture.household(b, None)?;
        let _server_b = fixture.run_server(b, &household_b).await?;
        household_a.keep_connected()?;

        let arena = &fixture.arena;

        let store = fixture.stores.get(b).unwrap();
        store
            .send(Notification::CatchingUp {
                arena: arena.clone(),
            })
            .await?;
        store
            .send(Notification::Ready {
                arena: arena.clone(),
            })
            .await?;
        store
            .send(Notification::Link {
                arena: arena.clone(),
                path: Path::parse("a/b/test.txt")?,
                size: 4,
                mtime: UnixTime::new(1234567890, 111),
            })
            .await?;

        let b = TestingPeers::b();
        assert_eq!(
            Some((
                b.clone(),
                Notification::CatchingUp {
                    arena: arena.clone()
                }
            )),
            timeout(Duration::from_secs(5), rx.recv()).await?,
        );
        assert_eq!(
            Some((
                b.clone(),
                Notification::Ready {
                    arena: arena.clone()
                }
            )),
            timeout(Duration::from_secs(5), rx.recv()).await?,
        );

        assert_eq!(
            Some((
                b.clone(),
                Notification::Link {
                    arena: arena.clone(),
                    path: Path::parse("a/b/test.txt")?,
                    size: 4,
                    mtime: UnixTime::new(1234567890, 111),
                }
            )),
            timeout(Duration::from_secs(5), rx.recv()).await?,
        );

        Ok(())
    }
}
