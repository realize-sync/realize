use super::peer_capnp::connected_peer::{self, RegisterParams, RegisterResults};
use super::store_capnp::store::{
    self, ArenasParams, ArenasResults, ReadParams, ReadResults, SubscribeParams, SubscribeResults,
};
use crate::model::Peer;
use crate::network::rate_limit::RateLimitedStream;
use crate::network::Server;
use crate::storage::real::RealStore;
use capnp::capability::Promise;
use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::twoparty::VatNetwork;
use capnp_rpc::RpcSystem;
use futures::io::{BufReader, BufWriter};
use futures::AsyncReadExt;
use std::thread;
use tokio::net::TcpStream;
use tokio::runtime;
use tokio::sync::mpsc;
use tokio::task::LocalSet;
use tokio_util::compat::TokioAsyncReadCompatExt;

/// Identifies Cap'n Proto ConnectedPeer connections.
const TAG: &[u8; 4] = b"PEER";

/// A set of peers and their connections.
///
/// Cap'n Proto connections are handled or their own thread. This
/// object serves as a communication channel between that thread and
/// the rest of the application.
///
/// To listen to incoming connections, call [Household::register].
#[derive(Clone)]
pub struct Household {
    connect_tx: mpsc::UnboundedSender<HouseholdConnection>,
}

impl Household {
    /// Spawn an new RPC thread and return th Household instance that
    /// manages it.
    pub fn spawn(store: RealStore) -> anyhow::Result<(Self, thread::JoinHandle<()>)> {
        let (connect_tx, connect_rx) = mpsc::unbounded_channel();
        let handle = CapnpRpcThread::new(store).spawn(connect_rx)?;

        Ok((Self { connect_tx }, handle))
    }

    /// Register peer connections to the given server.
    ///
    /// With this call, the server answers to PEER calls as Cap'n Proto
    /// PeerConnection, defined in `capnp/peer.capnp`.
    pub fn register(&self, server: &mut Server) {
        let tx = self.connect_tx.clone();
        server.register_raw(TAG, move |peer, stream, _, _| {
            // TODO: support shutdown_rx
            let _ = tx.send(HouseholdConnection::Incoming { peer, stream });
        })
    }
}

/// Messages used to communicate with [CapnpThread].
enum HouseholdConnection {
    /// Send incoming (server) TCP connections to the capnp threads to
    /// be handled there.
    Incoming {
        peer: Peer,
        stream: tokio_rustls::server::TlsStream<RateLimitedStream<TcpStream>>,
    },
}

/// Spawns a single thread that handles all Cap'n Proto RPC connections.
///
/// To communicate with the thread, send [HousehholdConnection]s to the channel.
struct CapnpRpcThread {
    store: RealStore,
}

impl CapnpRpcThread {
    /// Create a new [CapnpThread], but don't spawn it yet.
    fn new(store: RealStore) -> Self {
        Self { store }
    }

    fn connected_peer(&self, peer: Peer) -> connected_peer::Client {
        capnp_rpc::new_client(ConnectedPeerServer::new(peer, self.store.clone()))
    }

    fn spawn(
        self,
        mut rx: mpsc::UnboundedReceiver<HouseholdConnection>,
    ) -> anyhow::Result<thread::JoinHandle<()>> {
        //let main_rt = Runtime::handle();
        let rt = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        Ok(thread::Builder::new()
            .name("capnprpc".into())
            .spawn(move || {
                let local = LocalSet::new();

                local.spawn_local(async move {
                    while let Some(conn) = rx.recv().await {
                        match conn {
                        HouseholdConnection::Incoming {
                            peer,
                            stream,
                            .. // TODO: handle shutdown
                        } => {
                            let client = self.connected_peer(peer.clone());
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
                    }
                    }
                    log::debug!("finished ---");
                });

                rt.block_on(local);
            })?)
    }
}

/// Implement capnp interface ConnectedPeer, defined in
/// `capnp/peer.capnp`.
#[derive(Clone)]
struct ConnectedPeerServer {
    _peer: Peer,
    store: RealStore,
}

impl ConnectedPeerServer {
    fn new(peer: Peer, store: RealStore) -> Self {
        Self { _peer: peer, store }
    }
}

impl connected_peer::Server for ConnectedPeerServer {
    fn store(
        &mut self,
        _params: connected_peer::StoreParams,
        mut results: connected_peer::StoreResults,
    ) -> Promise<(), capnp::Error> {
        results.get().set_store(capnp_rpc::new_client(self.clone()));

        Promise::ok(())
    }

    fn register(&mut self, _: RegisterParams, _: RegisterResults) -> Promise<(), capnp::Error> {
        Promise::err(capnp::Error::unimplemented(
            "method connected_peer::Server::register not implemented".to_string(),
        ))
    }
}

impl store::Server for ConnectedPeerServer {
    fn arenas(&mut self, _: ArenasParams, mut results: ArenasResults) -> Promise<(), capnp::Error> {
        let arenas = self.store.arenas();
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

    fn subscribe(&mut self, _: SubscribeParams, _: SubscribeResults) -> Promise<(), capnp::Error> {
        Promise::err(capnp::Error::unimplemented(
            "method store::Server::subscribe not implemented".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert_fs::TempDir;
    use tokio::task::LocalSet;

    use crate::{
        model::Arena,
        network::{self, hostport::HostPort, security},
    };

    use super::*;

    struct Fixture {
        _tempdir: TempDir,
        arena: Arena,
        store: RealStore,
    }
    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();

            let tempdir = TempDir::new()?;
            let arena = Arena::from("test");
            let store = RealStore::new(vec![(arena.clone(), tempdir.path().to_path_buf())]);

            Ok(Self {
                _tempdir: tempdir,
                arena,
                store,
            })
        }
    }

    fn test_server(household: &Household) -> anyhow::Result<Arc<Server>> {
        let mut server = Server::new(network::testing::server_networking()?);
        household.register(&mut server);

        Ok(Arc::new(server))
    }

    async fn connect(addr: SocketAddr) -> anyhow::Result<connected_peer::Client> {
        let networking = network::testing::client_networking(addr)?;
        let stream = networking
            .connect_raw(&security::testing::server_peer(), TAG, None)
            .await?;
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
        let fixture = Fixture::setup().await?;

        let (household, _) = Household::spawn(fixture.store.clone())?;
        let server = test_server(&household)?;
        let addr = server.listen(&HostPort::localhost(0)).await?;

        LocalSet::new()
            .run_until(async move {
                let client = connect(addr).await?;

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
        let fixture = Fixture::setup().await?;

        let (household, thread_join) = Household::spawn(fixture.store.clone())?;
        let server = test_server(&household)?;
        let addr = server.listen(&HostPort::localhost(0)).await?;

        LocalSet::new()
            .run_until(async move {
                let client = connect(addr).await?;
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
}
