use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::twoparty::VatNetwork;
use capnp_rpc::{RpcSystem, VatNetwork as _};
use futures::AsyncReadExt;
use futures::io::{BufReader, BufWriter};
use std::cell::RefCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, LocalSet};
use tokio_retry::strategy::ExponentialBackoff;
use tokio_util::compat::TokioAsyncReadCompatExt;
use tokio_util::sync::CancellationToken;

use realize_types::Peer;

use super::rate_limit::RateLimitedStream;
use crate::{Networking, Server};

/// Connection status of a peer, broadcast by [ConnectionManager].
#[derive(Clone, PartialEq, Debug)]
pub enum PeerStatus {
    Connected(Peer),
    Disconnected(Peer),
}

/// Messages used to communicate with capnp on the main thread.
enum ConnectionMessage {
    /// Send incoming (server) TCP connections to the capnp threads to
    /// be handled there.
    Incoming {
        peer: Peer,
        stream: Box<tokio_rustls::server::TlsStream<RateLimitedStream<TcpStream>>>,
        shutdown_rx: broadcast::Receiver<()>,
    },
    /// Connect to all peers that have an address and attempt to keep
    /// the connection up.
    KeepAllConnected,

    /// Disconnect for all peers. This disables KeepConnected.
    DisconnectAll,

    /// Attempt to keep the connection up for a single peer.
    ///
    /// Does nothing unless an address is known for the peer.
    KeepPeerConnected(Peer),

    /// Disconnects the peer and/or stop trying to connect to it.
    DisconnectPeer(Peer),
}

#[allow(async_fn_in_trait)]
pub trait ConnectionHandler<T, C>: Send
where
    T: ConnectionTracker<C>,
{
    fn tag(&self) -> &'static [u8; 4];
    async fn create_tracker(self) -> Rc<T>;
}

#[allow(async_fn_in_trait)]
pub trait ConnectionTracker<C> {
    fn server(&self, peer: Peer) -> capnp::capability::Client;
    async fn register(&self, peer: Peer, client: C) -> anyhow::Result<()>;
    fn unregister(&self, peer: Peer);
}

pub struct ConnectionManager {
    tag: &'static [u8; 4],
    tx: mpsc::UnboundedSender<ConnectionMessage>,
    broadcast_tx: broadcast::Sender<PeerStatus>,
}

impl ConnectionManager {
    /// Build a new ConnectionManager instance that runs on the
    /// given [LocalSet].
    ///
    /// The [LocalSet] must later be run to run the tasks spawned on
    /// it by the connection manager. This is done by calling
    /// [LocalSet::run_until] or awaiting the local set itself.
    pub fn spawn<T, C>(
        local: &LocalSet,
        networking: Networking,
        handler: impl ConnectionHandler<T, C> + 'static,
    ) -> anyhow::Result<Self>
    where
        T: ConnectionTracker<C> + 'static,
        C: capnp::capability::FromClientHook + Clone + 'static,
    {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (broadcast_tx, _) = broadcast::channel(128);
        let tag = handler.tag();

        local.spawn_local({
            let broadcast_tx = broadcast_tx.clone();

            async move {
                let tracker = handler.create_tracker().await;
                let ctx = AppContext::new(networking, tag, tracker, broadcast_tx);
                while let Some(conn) = rx.recv().await {
                    match conn {
                        ConnectionMessage::Incoming {
                            peer,
                            stream,
                            shutdown_rx,
                        } => ctx.accept(peer, stream, shutdown_rx),
                        ConnectionMessage::KeepAllConnected => ctx.keep_all_connected(),
                        ConnectionMessage::DisconnectAll => ctx.disconnect_all(),
                        ConnectionMessage::KeepPeerConnected(peer) => {
                            if ctx.networking.is_connectable(peer) {
                                ctx.keep_peer_connected(peer);
                            }
                        }
                        ConnectionMessage::DisconnectPeer(peer) => ctx.disconnect_peer(peer),
                    }
                }
            }
        });

        Ok(Self {
            tx,
            broadcast_tx,
            tag,
        })
    }

    /// Report peer status changes through the given receiver.
    pub fn peer_status(&self) -> broadcast::Receiver<PeerStatus> {
        self.broadcast_tx.subscribe()
    }

    /// Keep a client connection up to all peers for which an address is known.
    pub fn keep_all_connected(&self) -> anyhow::Result<()> {
        self.tx.send(ConnectionMessage::KeepAllConnected)?;

        Ok(())
    }

    /// Disconnect from all servers. Stop trying to connect to other peers.
    pub fn disconnect_all(&self) -> anyhow::Result<()> {
        self.tx.send(ConnectionMessage::DisconnectAll)?;

        Ok(())
    }

    /// Keep a client connection up to the given peer.
    ///
    /// Has no effect if no address is known for the peer
    pub fn keep_peer_connected(&self, peer: Peer) -> anyhow::Result<()> {
        self.tx.send(ConnectionMessage::KeepPeerConnected(peer))?;

        Ok(())
    }

    /// Disconnect from all servers. Stop trying to connect to other peers.
    pub fn disconnect_peer(&self, peer: Peer) -> anyhow::Result<()> {
        self.tx.send(ConnectionMessage::DisconnectPeer(peer))?;

        Ok(())
    }

    /// Register peer connections to the given server.
    ///
    /// With this call, the server answers to PEER calls as Cap'n Proto
    /// PeerConnection, defined in `capnp/peer.capnp`.
    pub fn register(&self, server: &mut Server) {
        let tx = self.tx.clone();
        server.register_raw(self.tag, move |peer, stream, _, shutdown_rx| {
            // TODO: support shutdown_rx
            let _ = tx.send(ConnectionMessage::Incoming {
                peer,
                stream: Box::new(stream),
                shutdown_rx,
            });
        })
    }
}

struct AppContext<T, C>
where
    T: ConnectionTracker<C>,
{
    networking: Networking,
    tracker: Rc<T>,
    tag: &'static [u8; 4],
    broadcast_tx: broadcast::Sender<PeerStatus>,
    connections: RefCell<HashMap<Peer, TrackedPeerConnections>>,
    _phantom1: PhantomData<C>,
}

struct TrackedPeerConnections {
    tracker: Option<JoinHandle<()>>,
    cancel: CancellationToken,
}

impl TrackedPeerConnections {
    pub fn new() -> Self {
        TrackedPeerConnections {
            tracker: None,
            cancel: CancellationToken::new(),
        }
    }
}
impl<T, C> AppContext<T, C>
where
    T: ConnectionTracker<C> + 'static,
    C: capnp::capability::FromClientHook + Clone + 'static,
{
    fn new(
        networking: Networking,
        tag: &'static [u8; 4],
        tracker: Rc<T>,
        broadcast_tx: broadcast::Sender<PeerStatus>,
    ) -> Rc<Self> {
        Rc::new(Self {
            networking,
            tag,
            tracker,
            broadcast_tx,
            connections: RefCell::new(HashMap::new()),
            _phantom1: PhantomData,
        })
    }

    fn accept(
        self: &Rc<Self>,
        peer: Peer,
        stream: Box<tokio_rustls::server::TlsStream<RateLimitedStream<TcpStream>>>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) {
        let this = Rc::clone(self);
        tokio::task::spawn_local(async move {
            let (r, w) = TokioAsyncReadCompatExt::compat(stream).split();
            let mut net = Box::new(VatNetwork::new(
                BufReader::new(r),
                BufWriter::new(w),
                Side::Server,
                Default::default(),
            ));
            let until_shutdown = net.drive_until_shutdown();
            let system = RpcSystem::new(net, Some(this.tracker.server(peer)));
            let disconnector = system.get_disconnector();
            tokio::task::spawn_local(system);

            tokio::select!(
                _ = shutdown_rx.recv() => {
                    let _ = disconnector.await;
                },
                res = until_shutdown => {
                    if let Err(err) = res {
                        if err.kind != capnp::ErrorKind::Disconnected {
                            log::debug!("RPC connection from {peer} failed: {err}")
                        }
                    }
                }
            );
        });
    }

    fn keep_all_connected(self: &Rc<Self>) {
        for peer in self.networking.connectable_peers() {
            self.keep_peer_connected(peer);
        }
    }

    fn keep_peer_connected(self: &Rc<Self>, peer: Peer) {
        let mut borrow = self.connections.borrow_mut();
        let conn = borrow
            .entry(peer)
            .or_insert_with(TrackedPeerConnections::new);
        if conn.cancel.is_cancelled() {
            conn.tracker = None;
            conn.cancel = CancellationToken::new();
        } else {
            let has_usable_tracker = conn
                .tracker
                .as_ref()
                .map(|t| !t.is_finished())
                .unwrap_or(false);
            if has_usable_tracker {
                // Already running
                return;
            }
        }

        let this = Rc::clone(self);
        let cancel = conn.cancel.clone();
        conn.tracker = Some(tokio::task::spawn_local(async move {
            this.track_peer(peer, cancel).await
        }));
    }

    fn disconnect_peer(self: &Rc<Self>, peer: Peer) {
        let borrow = self.connections.borrow();
        if let Some(conn) = borrow.get(&peer) {
            conn.cancel.cancel();
        }
    }

    fn disconnect_all(self: &Rc<Self>) {
        let borrow = self.connections.borrow();
        for conn in borrow.values() {
            conn.cancel.cancel();
        }
    }

    async fn track_peer(self: &Rc<Self>, peer: Peer, cancel: CancellationToken) {
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
                        tokio::select!(
                        _ = cancel.cancelled() => { return },
                        _ = tokio::time::sleep(delay) => {});
                    }
                },
                None => {
                    // Execute immediately, and install backoff for next time.
                    current_backoff = Some(retry_strategy.clone());
                }
            }
            let stream = tokio::select!(
                _ = cancel.cancelled() => {
                    return;
                }
                connected = self.networking.connect_raw(peer, self.tag, None) =>  match connected {
                    Ok(stream) => {
                        log::debug!("Connected to {peer}.");

                        stream
                    },
                    Err(err) => {
                        log::debug!("Failed to connect to {peer}: {err}; Will retry.");
                        continue;
                    }
                }
            );

            let (r, w) = TokioAsyncReadCompatExt::compat(stream).split();
            let mut net = Box::new(VatNetwork::new(
                BufReader::new(r),
                BufWriter::new(w),
                Side::Client,
                Default::default(),
            ));
            let until_shutdown = net.drive_until_shutdown();
            let mut system = RpcSystem::new(net, None);
            let client: C = system.bootstrap(Side::Server);
            let disconnector = system.get_disconnector();
            scopeguard::defer!({
                tokio::task::spawn_local(disconnector);
            });
            tokio::task::spawn_local(system);

            scopeguard::defer!({
                self.tracker.unregister(peer);
            });
            if let Err(err) = self.tracker.register(peer, client).await {
                log::debug!("Registration on {peer} failed: {err}");
                continue;
            }

            let _ = self.broadcast_tx.send(PeerStatus::Connected(peer));
            scopeguard::defer!({
                let _ = self.broadcast_tx.send(PeerStatus::Disconnected(peer));
            });

            // We're fully connected. Reset the backoff delay for next time.
            current_backoff = None;

            tokio::select!(
                _ = cancel.cancelled() => {
                    return;
                }
                res = until_shutdown => if let Err(err) = res {
                    log::debug!("Connection to {peer} was shutdown: {err}; Will reconnect.")
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hostport::HostPort;
    use crate::testing::TestingPeers;
    use crate::testing::hello_capnp::hello;
    use crate::testing::hello_capnp::hello::HelloParams;
    use crate::testing::hello_capnp::hello::HelloResults;
    use capnp::capability::Promise;
    use capnp_rpc::RpcSystem;
    use capnp_rpc::pry;
    use capnp_rpc::rpc_twoparty_capnp::Side;
    use capnp_rpc::twoparty::VatNetwork;
    use futures::AsyncReadExt;
    use futures::io::BufReader;
    use futures::io::BufWriter;
    use std::collections::HashSet;
    use std::sync::Arc;
    use tokio::sync::oneshot;
    use tokio::task::LocalSet;
    use tokio_util::compat::TokioAsyncReadCompatExt;

    fn a() -> Peer {
        TestingPeers::a()
    }
    fn b() -> Peer {
        TestingPeers::b()
    }
    fn c() -> Peer {
        TestingPeers::c()
    }

    struct HelloConnectionHandler {
        peer: Peer,
        rx: mpsc::Receiver<oneshot::Sender<HashSet<Peer>>>,
    }
    impl HelloConnectionHandler {
        fn new(peer: Peer, rx: mpsc::Receiver<oneshot::Sender<HashSet<Peer>>>) -> Self {
            Self { peer, rx }
        }
    }
    impl ConnectionHandler<HelloConnectionTracker, hello::Client> for HelloConnectionHandler {
        fn tag(&self) -> &'static [u8; 4] {
            b"HELO"
        }

        async fn create_tracker(self) -> Rc<HelloConnectionTracker> {
            let HelloConnectionHandler { peer, mut rx } = self;
            let tracker = Rc::new(HelloConnectionTracker {
                peer,
                registered: RefCell::new(HashSet::new()),
            });

            tokio::task::spawn_local({
                let tracker = Rc::clone(&tracker);

                async move {
                    while let Some(oneshot_sender) = rx.recv().await {
                        let _ = oneshot_sender.send(tracker.registered.borrow().clone());
                    }
                }
            });

            tracker
        }
    }

    struct HelloConnectionTracker {
        peer: Peer,
        registered: RefCell<HashSet<Peer>>,
    }

    impl ConnectionTracker<hello::Client> for HelloConnectionTracker {
        fn server(&self, peer: Peer) -> capnp::capability::Client {
            let c: hello::Client = capnp_rpc::new_client(HelloServer {
                this_peer: self.peer,
                other_peer: peer,
            });

            c.client
        }

        async fn register(&self, peer: Peer, client: hello::Client) -> anyhow::Result<()> {
            let mut request = client.hello_request();
            request.get().set_name(self.peer.as_str());

            let reply = request.send().promise.await?;
            let greetings = reply.get()?.get_result()?.to_string()?;
            assert_eq!(
                format!("Hello {0} -- From {peer} to {0}", self.peer),
                greetings
            );

            self.registered.borrow_mut().insert(peer);
            Ok(())
        }

        fn unregister(&self, peer: Peer) {
            self.registered.borrow_mut().remove(&peer);
        }
    }

    struct HelloServer {
        this_peer: Peer,
        other_peer: Peer,
    }

    impl hello::Server for HelloServer {
        fn hello(
            &mut self,
            params: HelloParams,
            mut results: HelloResults,
        ) -> Promise<(), capnp::Error> {
            let name = pry!(pry!(pry!(params.get()).get_name()).to_str());
            results.get().set_result(format!(
                "Hello {name} -- From {} to {}",
                self.this_peer, self.other_peer,
            ));

            Promise::ok(())
        }
    }

    struct Fixture {
        peers: TestingPeers,
        peer_tx: HashMap<Peer, mpsc::Sender<oneshot::Sender<HashSet<Peer>>>>,
    }
    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();

            let peers = TestingPeers::new()?;
            Ok(Self {
                peers,
                peer_tx: HashMap::new(),
            })
        }

        fn manager(&mut self, local: &LocalSet, peer: Peer) -> anyhow::Result<ConnectionManager> {
            let (tx, rx) = mpsc::channel(10);
            let handler = HelloConnectionHandler::new(peer, rx);
            self.peer_tx.insert(peer, tx);
            ConnectionManager::spawn(local, self.peers.networking(peer)?, handler)
        }

        async fn get_registered(&self, peer: Peer) -> anyhow::Result<HashSet<Peer>> {
            let (tx, rx) = oneshot::channel();
            self.peer_tx
                .get(&peer)
                .expect("tx for {peer}")
                .send(tx)
                .await?;

            Ok(rx.await?)
        }

        async fn launch_server(
            &mut self,
            peer: Peer,
            manager: &ConnectionManager,
        ) -> anyhow::Result<Arc<Server>> {
            let mut server = Server::new(self.peers.networking(peer)?);
            manager.register(&mut server);

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

    async fn connect(networking: Networking, peer: Peer) -> anyhow::Result<hello::Client> {
        let stream = networking.connect_raw(peer, b"HELO", None).await?;
        let (r, w) = TokioAsyncReadCompatExt::compat(stream).split();
        let net = Box::new(VatNetwork::new(
            BufReader::new(r),
            BufWriter::new(w),
            Side::Client,
            Default::default(),
        ));
        let mut system = RpcSystem::new(net, None);
        let client: hello::Client = system.bootstrap(Side::Server);
        tokio::task::spawn_local(system);

        Ok(client)
    }

    #[tokio::test]
    async fn manager_listens() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let local = LocalSet::new();

        let a = a();
        let manager = fixture.manager(&local, a)?;
        let _server = fixture.launch_server(a, &manager).await?;
        let b_networking = fixture.peers.networking(b())?;
        local
            .run_until(async move {
                let client = connect(b_networking, a).await?;

                let mut request = client.hello_request();
                request.get().set_name("Bee");
                let reply = request.send().promise.await?;
                let greetings = reply.get()?.get_result()?.to_str()?;
                assert_eq!("Hello Bee -- From a to b", greetings);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn manager_connects() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let local = LocalSet::new();

        let a = a();
        fixture.peers.pick_port(a)?;

        let b = b();
        fixture.peers.pick_port(b)?;

        let manager_a = fixture.manager(&local, a)?;
        let _server_a = fixture.launch_server(a, &manager_a).await?;

        let manager_b = fixture.manager(&local, b)?;
        let _server_b = fixture.launch_server(b, &manager_b).await?;

        local
            .run_until(async move {
                let mut status_a = manager_a.peer_status();
                let mut status_b = manager_b.peer_status();

                manager_a.keep_all_connected()?;
                assert_eq!(PeerStatus::Connected(b), status_a.recv().await?);

                manager_b.keep_all_connected()?;
                assert_eq!(PeerStatus::Connected(a), status_b.recv().await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn manager_reconnects() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let local = LocalSet::new();

        let a = a();
        fixture.peers.pick_port(a)?;

        let b = b();
        fixture.peers.pick_port(b)?;

        let manager_a = fixture.manager(&local, a)?;
        let _server_a = fixture.launch_server(a, &manager_a).await?;

        let manager_b = fixture.manager(&local, b)?;
        let server_b = fixture.launch_server(b, &manager_b).await?;

        let mut status_a = manager_a.peer_status();
        manager_a.keep_all_connected()?;

        local
            .run_until(async move {
                assert_eq!(PeerStatus::Connected(b), status_a.recv().await?);
                server_b.shutdown().await?;
                assert_eq!(PeerStatus::Disconnected(b), status_a.recv().await?);

                let _server_b = fixture.launch_server(b, &manager_b).await?;
                assert_eq!(PeerStatus::Connected(b), status_a.recv().await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn manager_connects_and_disconnects() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let local = LocalSet::new();

        let a = a();
        let b = b();
        fixture.peers.pick_port(b)?;

        let manager_a = fixture.manager(&local, a)?;

        let manager_b = fixture.manager(&local, b)?;
        let _server_b = fixture.launch_server(b, &manager_b).await?;

        local
            .run_until(async move {
                let mut status_a = manager_a.peer_status();

                manager_a.keep_all_connected()?;
                assert_eq!(PeerStatus::Connected(b), status_a.recv().await?);

                manager_a.disconnect_all()?;
                assert_eq!(PeerStatus::Disconnected(b), status_a.recv().await?);

                manager_a.keep_all_connected()?;
                assert_eq!(PeerStatus::Connected(b), status_a.recv().await?);

                manager_a.disconnect_all()?;
                assert_eq!(PeerStatus::Disconnected(b), status_a.recv().await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn manager_connects_and_disconnects_peer() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let local = LocalSet::new();

        let a = a();
        let b = b();
        fixture.peers.pick_port(b)?;

        let c = c();
        fixture.peers.pick_port(c)?;

        let manager_a = fixture.manager(&local, a)?;

        let manager_b = fixture.manager(&local, b)?;
        let _server_b = fixture.launch_server(b, &manager_b).await?;

        let manager_c = fixture.manager(&local, c)?;
        let _server_c = fixture.launch_server(c, &manager_c).await?;

        local
            .run_until(async move {
                let mut status_a = manager_a.peer_status();

                manager_a.keep_peer_connected(b)?;
                assert_eq!(PeerStatus::Connected(b), status_a.recv().await?);

                manager_a.keep_peer_connected(c)?;
                assert_eq!(PeerStatus::Connected(c), status_a.recv().await?);

                manager_a.disconnect_peer(c)?;
                assert_eq!(PeerStatus::Disconnected(c), status_a.recv().await?);

                manager_a.keep_peer_connected(c)?;
                assert_eq!(PeerStatus::Connected(c), status_a.recv().await?);

                manager_a.disconnect_peer(b)?;
                assert_eq!(PeerStatus::Disconnected(b), status_a.recv().await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn manager_registers_and_unregisters() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let local = LocalSet::new();

        let a = a();
        let b = b();
        fixture.peers.pick_port(b)?;

        let manager_a = fixture.manager(&local, a)?;

        let manager_b = fixture.manager(&local, b)?;
        let _server_b = fixture.launch_server(b, &manager_b).await?;

        local
            .run_until(async move {
                let mut status_a = manager_a.peer_status();

                assert_eq!(HashSet::new(), fixture.get_registered(a).await?);

                manager_a.keep_all_connected()?;
                assert_eq!(PeerStatus::Connected(b), status_a.recv().await?);

                assert_eq!(HashSet::from([b]), fixture.get_registered(a).await?);

                manager_a.disconnect_all()?;
                assert_eq!(PeerStatus::Disconnected(b), status_a.recv().await?);

                assert_eq!(HashSet::from([]), fixture.get_registered(a).await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }
}
