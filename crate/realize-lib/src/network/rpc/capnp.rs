use super::peer_capnp::connected_peer::{self, RegisterParams, RegisterResults};
use super::store_capnp::store::{
    self, ArenasParams, ArenasResults, ReadParams, ReadResults, SubscribeParams, SubscribeResults,
};
use crate::model::Peer;
use crate::network::rate_limit::RateLimitedStream;
use crate::network::{Networking, Server};
use crate::storage::real::RealStore;
use capnp::capability::Promise;
use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::twoparty::VatNetwork;
use capnp_rpc::RpcSystem;
use futures::io::{BufReader, BufWriter};
use futures::AsyncReadExt;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
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

impl Household {
    /// Spawn an new RPC thread and return th Household instance that
    /// manages it.
    pub fn spawn(
        networking: Networking,
        store: RealStore,
    ) -> anyhow::Result<(Self, thread::JoinHandle<()>)> {
        let (tx, connect_rx) = mpsc::unbounded_channel();
        let (broadcast_tx, _) = broadcast::channel(128);
        let handle = spawn_rpc_thread(networking, store, connect_rx, broadcast_tx.clone())?;

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
        server.register_raw(TAG, move |peer, stream, _, _| {
            // TODO: support shutdown_rx
            let _ = tx.send(HouseholdConnection::Incoming { peer, stream });
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
    },
    KeepConnected,
}

/// Spawns a single thread that handles all Cap'n Proto RPC connections.
///
/// To communicate with the thread, send [HousehholdConnection]s to the channel.
fn spawn_rpc_thread(
    networking: Networking,
    store: RealStore,
    mut rx: mpsc::UnboundedReceiver<HouseholdConnection>,
    broadcast_tx: broadcast::Sender<PeerStatus>,
) -> anyhow::Result<thread::JoinHandle<()>> {
    //let main_rt = runtime::Handle::current();
    let rt = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    Ok(thread::Builder::new()
        .name("capnprpc".into())
        .spawn(move || {
            let ctx = AppContext::new(networking, store, broadcast_tx);
            let local = LocalSet::new();

            local.spawn_local(async move {
                while let Some(conn) = rx.recv().await {
                    match conn {
                        HouseholdConnection::Incoming {
                            peer,
                            stream,
                            .. // TODO: handle shutdown
                        } => ctx.accept(peer, stream),
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
    tracker: Option<JoinHandle<anyhow::Result<()>>>,
}

struct AppContext {
    networking: Networking,
    store: RealStore,
    broadcast_tx: broadcast::Sender<PeerStatus>,
    connections: RefCell<HashMap<Peer, TrackedPeerConnections>>,
}

impl AppContext {
    fn new(
        networking: Networking,
        store: RealStore,
        broadcast_tx: broadcast::Sender<PeerStatus>,
    ) -> Rc<Self> {
        Rc::new(Self {
            networking,
            store,
            broadcast_tx,
            connections: RefCell::new(HashMap::new()),
        })
    }

    fn accept(
        self: &Rc<Self>,
        peer: Peer,
        stream: tokio_rustls::server::TlsStream<RateLimitedStream<TcpStream>>,
    ) {
        let client = ConnectedPeerServer::new(peer.clone(), Rc::clone(self)).into_connected_peer();
        tokio::task::spawn_local(async move {
            let (r, w) = TokioAsyncReadCompatExt::compat(stream).split();
            let net = Box::new(VatNetwork::new(
                BufReader::new(r),
                BufWriter::new(w),
                Side::Server,
                Default::default(),
            ));
            let system = RpcSystem::new(net, Some(client.clone().client));
            if let Err(err) = system.await {
                log::debug!("RPC System from {peer} failed: {err}")
            }
        });
    }

    fn keep_connected(self: &Rc<Self>) {
        for peer in self.networking.connectable_peers() {
            let mut borrow = self.connections.borrow_mut();
            let conn = borrow
                .entry(peer.clone())
                .or_insert_with(TrackedPeerConnections::default);
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

    fn register_peer(self: &Rc<Self>, _peer: &Peer, _store: store::Client) {
        todo!();
    }

    async fn track_peer(self: &Rc<Self>, peer: &Peer) -> anyhow::Result<()> {
        loop {
            let stream = self
                .networking
                .connect_with_retries_raw(
                    peer,
                    TAG,
                    None,
                    ExponentialBackoff::from_millis(500).max_delay(Duration::from_secs(5 * 60)),
                    None,
                )
                .await?;
            let (r, w) = TokioAsyncReadCompatExt::compat(stream).split();
            let net = Box::new(VatNetwork::new(
                BufReader::new(r),
                BufWriter::new(w),
                Side::Client,
                Default::default(),
            ));
            let mut system = RpcSystem::new(net, None);
            let mut client: connected_peer::Client = system.bootstrap(Side::Server);
            let setup = tokio::task::spawn_local({
                let this = Rc::clone(&self);
                let peer = peer.clone();
                async move {
                    let store = get_connected_peer_store(&mut client).await?;
                    this.set_tracked_client(&peer, Some(store.clone()));

                    let _ = this.broadcast_tx.send(PeerStatus::Connected(peer.clone()));

                    //this.register_self(&client);
                    //this.subscribe_self(&peer, &mut store).await?;

                    Ok::<_, anyhow::Error>(())
                }
            });

            let ret = system.await;
            let setup_ret = setup.await; // wait to avoid race condition with set_tracked_client
            self.set_tracked_client(peer, None);
            let _ = self
                .broadcast_tx
                .send(PeerStatus::Disconnected(peer.clone()));

            if let Err(err) = ret {
                log::debug!("Disconnected from {peer}: {err}. Will reconnect.");
            }
            if let Err(err) = setup_ret {
                log::debug!("Error during setup for {peer}: {err}.");
            }
        }
    }

    fn set_tracked_client(self: &Rc<Self>, peer: &Peer, client: Option<store::Client>) {
        self.connections
            .borrow_mut()
            .entry(peer.clone())
            .or_insert_with(TrackedPeerConnections::default)
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

    fn register(
        &mut self,
        params: RegisterParams,
        _: RegisterResults,
    ) -> Promise<(), capnp::Error> {
        self.ctx.register_peer(
            &self.peer,
            capnp_rpc::pry!(capnp_rpc::pry!(params.get()).get_store()),
        );

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

    fn read(&mut self, _: ReadParams, _: ReadResults) -> Promise<(), capnp::Error> {
        Promise::err(capnp::Error::unimplemented(
            "method store::Server::read not implemented".to_string(),
        ))
    }

    fn subscribe(
        &mut self,
        _: SubscribeParams,
        _: SubscribeResults,
    ) -> Promise<(), ::capnp::Error> {
        Promise::err(capnp::Error::unimplemented(
            "method store::Server::subscribe not implemented".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_fs::{
        prelude::{PathChild as _, PathCreateDir as _},
        TempDir,
    };
    use tokio::task::LocalSet;

    use crate::{
        model::Arena,
        network::{hostport::HostPort, testing::TestingPeers},
    };

    use super::*;

    struct Fixture {
        tempdir: TempDir,
        arena: Arena,
        peers: TestingPeers,
        peer_a: Peer,
        #[allow(dead_code)]
        peer_b: Peer,
        #[allow(dead_code)]
        peer_c: Peer,
    }
    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();

            let tempdir = TempDir::new()?;
            let arena = Arena::from("test");

            Ok(Self {
                tempdir,
                arena,
                peers: TestingPeers::new()?,
                peer_a: TestingPeers::a(),
                peer_b: TestingPeers::b(),
                peer_c: TestingPeers::c(),
            })
        }

        fn store(&self, peer: &Peer) -> anyhow::Result<RealStore> {
            let dir = self.tempdir.child(peer.as_str());
            dir.create_dir_all()?;

            Ok(RealStore::single(&self.arena, &dir.path()))
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
        let (household, _) =
            Household::spawn(fixture.peers.networking(peer)?, fixture.store(peer)?)?;
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
        let (household, thread_join) =
            Household::spawn(fixture.peers.networking(peer)?, fixture.store(peer)?)?;
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
        fixture.peers.pick_port(&a)?;

        let b = &fixture.peer_b.clone();
        fixture.peers.pick_port(&b)?;

        let (household_a, _) = Household::spawn(fixture.peers.networking(a)?, fixture.store(a)?)?;
        let _server_a = fixture.run_server(a, &household_a).await?;

        let (household_b, _) = Household::spawn(fixture.peers.networking(b)?, fixture.store(b)?)?;
        let _server_b = fixture.run_server(b, &household_b).await?;

        let mut status_a = household_a.peer_status();
        let mut status_b = household_b.peer_status();

        household_a.keep_connected()?;
        assert_eq!(PeerStatus::Connected(b.clone()), status_a.recv().await?);

        household_b.keep_connected()?;
        assert_eq!(PeerStatus::Connected(a.clone()), status_b.recv().await?);

        Ok(())
    }

    // TODO: test reconnections
}
