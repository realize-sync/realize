use realize_types::Peer;
use anyhow::Context as _;
use async_speed_limit::Limiter;
use async_speed_limit::clock::StandardClock;
use config::PeerConfig;
use futures::prelude::*;
use hostport::HostPort;
use rate_limit::RateLimitedStream;
use rustls::pki_types::ServerName;
use security::{PeerVerifier, RawPublicKeyResolver};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path;
use std::sync::Arc;
use tarpc::tokio_serde::formats::Bincode;
use tarpc::tokio_util::codec::length_delimited::LengthDelimitedCodec;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_rustls::{TlsAcceptor, TlsConnector};

pub mod config;
pub mod hostport;
mod metrics;
pub(crate) mod rate_limit;
pub(crate) mod reconnect;
pub mod security;

pub mod capnp;
#[cfg(any(test, feature = "testing"))]
pub mod testing;

#[derive(Clone)]
pub struct Networking {
    addresses: Arc<HashMap<Peer, String>>,
    verifier: Arc<PeerVerifier>,
    acceptor: Arc<TlsAcceptor>,
    connector: Arc<TlsConnector>,
}

impl Networking {
    pub fn from_config(
        peers: &HashMap<Peer, PeerConfig>,
        privkey: &path::Path,
    ) -> anyhow::Result<Self> {
        let verifier = PeerVerifier::from_config(peers)?;
        let resolver = RawPublicKeyResolver::from_private_key_file(privkey)?;
        Ok(Self::new(
            peers
                .iter()
                .flat_map(|(p, c)| c.address.as_ref().map(|addr| (p, addr.as_ref()))),
            resolver,
            verifier,
        ))
    }

    pub fn new<'a, T>(
        addresses: T,
        resolver: Arc<RawPublicKeyResolver>,
        verifier: Arc<PeerVerifier>,
    ) -> Self
    where
        T: IntoIterator<Item = (&'a Peer, &'a str)>,
    {
        Self {
            addresses: Arc::new(
                addresses
                    .into_iter()
                    .map(|(p, addr)| (p.clone(), addr.to_string()))
                    .collect(),
            ),
            verifier: Arc::clone(&verifier),
            acceptor: Arc::new(security::make_tls_acceptor(
                Arc::clone(&verifier),
                Arc::clone(&resolver),
            )),
            connector: Arc::new(security::make_tls_connector(verifier, resolver)),
        }
    }

    /// Set of peers that have a known address.
    pub fn connectable_peers(&self) -> impl Iterator<Item = &Peer> {
        self.addresses.keys()
    }

    pub(crate) async fn connect<Req, Resp>(
        &self,
        peer: &Peer,
        tag: &[u8; 4],
        limiter: Option<Limiter>,
    ) -> Result<
        tarpc::serde_transport::Transport<
            tokio_rustls::client::TlsStream<RateLimitedStream<TcpStream>>,
            Req,
            Resp,
            Bincode<Req, Resp>,
        >,
        anyhow::Error,
    >
    where
        Req: for<'de> serde::Deserialize<'de>,
        Resp: serde::Serialize,
    {
        let tls_stream = self.connect_raw(peer, tag, limiter).await?;

        let transport = tarpc::serde_transport::new(
            LengthDelimitedCodec::builder().new_framed(tls_stream),
            Bincode::default(),
        );

        Ok(transport)
    }

    pub(crate) async fn connect_raw(
        &self,
        peer: &Peer,
        tag: &[u8; 4],
        limiter: Option<Limiter>,
    ) -> anyhow::Result<tokio_rustls::client::TlsStream<RateLimitedStream<TcpStream>>> {
        let addr = self
            .addresses
            .get(peer)
            .ok_or(anyhow::anyhow!("no address known for {peer}"))?;
        let addr = HostPort::parse(addr)
            .await
            .with_context(|| format!("address for {peer} is invalid"))?;
        let domain = ServerName::try_from(addr.host().to_string())?;
        let stream = TcpStream::connect(addr.addr()).await?;
        stream.set_nodelay(true)?;

        let stream = RateLimitedStream::new(
            stream,
            limiter.unwrap_or_else(|| Limiter::<StandardClock>::new(f64::INFINITY)),
        );
        let mut tls_stream = self.connector.connect(domain, stream).await?;
        tls_stream.write_all(tag).await?;

        Ok(tls_stream)
    }

    async fn accept(
        &self,
        stream: TcpStream,
        limiter: Limiter,
    ) -> Result<
        (
            Peer,
            [u8; 4],
            tokio_rustls::server::TlsStream<RateLimitedStream<TcpStream>>,
        ),
        anyhow::Error,
    > {
        let peer_addr = stream.peer_addr()?;
        let stream = RateLimitedStream::new(stream, limiter);
        let mut tls_stream = self.acceptor.accept(stream).await?;
        // Guards against bugs in the auth code; worth dying
        let peer = self
            .verifier
            .connection_peer_id(&tls_stream)
            .expect("Peer must be known at this point")
            .clone();
        log::info!("Accepted peer {peer} from {peer_addr}");
        let mut tag = [0u8; 4];
        tls_stream.read_exact(&mut tag).await?;

        Ok((peer, tag, tls_stream))
    }
}

/// A server that that listens on a port.
///
/// Which services are served depends on what's registered to the
/// server.
pub struct Server {
    networking: Networking,
    handlers: HashMap<
        &'static [u8; 4],
        Box<
            dyn Fn(
                    Peer,
                    tokio_rustls::server::TlsStream<RateLimitedStream<TcpStream>>,
                    Limiter,
                    broadcast::Receiver<()>,
                ) + Send
                + Sync,
        >,
    >,
    shutdown_tx: broadcast::Sender<()>,
}

impl Server {
    /// Create a new server, with the given networking configuration.
    pub fn new(networking: Networking) -> Self {
        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);
        Self {
            networking,
            handlers: HashMap::new(),
            shutdown_tx,
        }
    }

    /// Register a handler for a TARPC service.
    pub(crate) fn register_server<T, S, F>(&mut self, tag: &'static [u8; 4], handler: T)
    where
        T: Fn(
                &Peer,
                Limiter,
                tarpc::tokio_util::codec::Framed<
                    tokio_rustls::server::TlsStream<RateLimitedStream<TcpStream>>,
                    LengthDelimitedCodec,
                >,
            ) -> S
            + Send
            + Sync
            + 'static,
        S: Stream<Item = F> + Send + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        self.register_raw(
            tag,
            move |peer: Peer,
                  stream,
                  limiter: Limiter,
                  mut shutdown_rx: broadcast::Receiver<()>| {
                let framed = LengthDelimitedCodec::builder().new_framed(stream);
                let stream = handler(&peer, limiter, framed);

                tokio::spawn(async move {
                    let mut stream = Box::pin(stream);
                    loop {
                        tokio::select!(
                            _ = shutdown_rx.recv() => {
                                log::debug!("Shutting connection to {peer}");
                                break;
                            }
                            next = stream.next() => {
                                match next {
                                    None => {
                                        break;
                                    },
                                    Some(fut) => {
                                        tokio::spawn(metrics::track_in_flight_request(fut));
                                    }
                                }
                            }
                        )
                    }
                });
            },
        );
    }

    /// Register a handler for the given tagged stream.
    pub(crate) fn register_raw(
        &mut self,
        tag: &'static [u8; 4],
        handler: impl Fn(
            Peer,
            tokio_rustls::server::TlsStream<RateLimitedStream<TcpStream>>,
            Limiter,
            broadcast::Receiver<()>,
        ) + Send
        + Sync
        + 'static,
    ) {
        self.handlers.insert(tag, Box::new(handler));
    }

    /// Shutdown any listener and wait for the shutdown to happen.
    ///
    /// The listener is also shutdown when the server instance is
    /// dropped.
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.shutdown_tx.send(())?;
        self.shutdown_tx.closed().await;

        Ok(())
    }

    /// Listen or the given address.
    ///
    /// The server spawns a task for listening in the background. The
    /// task runs as long as there is at least one server instance
    /// available through the Arc, or until [Server::shutdown] is
    /// called.
    pub async fn listen(self: &Arc<Self>, hostport: &HostPort) -> anyhow::Result<SocketAddr> {
        let listener = TcpListener::bind(hostport.addr()).await?;
        log::info!(
            "Listening for RPC connections on {:?}",
            listener
                .local_addr()
                .map(|addr| addr.to_string())
                .unwrap_or_else(|err| err.to_string())
        );

        let addr = listener.local_addr()?;
        let mut shutdown_listener_rx = self.shutdown_tx.subscribe();
        let weak_self = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                tokio::select!(
                    _ = shutdown_listener_rx.recv() => {
                        log::debug!("Shutting down listener");
                        return;
                    }
                    Ok((stream, peer)) = listener.accept() => {
                        match weak_self.upgrade() {
                            Some(strong_self) => {
                                match strong_self.accept(stream).await {
                                    Ok(_) => {}
                                    Err(err) => log::debug!("{peer}: connection rejected: {err}"),
                                };
                            }
                            None => {
                                // Server has been dropped; Shutdown listener.
                                return;
                            }

                        }
                    }
                );
            }
        });

        Ok(addr)
    }

    async fn accept(&self, stream: TcpStream) -> anyhow::Result<()> {
        stream.set_nodelay(true)?;

        let shutdown_rx = self.shutdown_tx.subscribe();
        let limiter = Limiter::<StandardClock>::new(f64::INFINITY);
        let (peer, tag, mut tls_stream) = self.networking.accept(stream, limiter.clone()).await?;
        if let Some(handler) = self.handlers.get(&tag) {
            (*handler)(peer, tls_stream, limiter, shutdown_rx);
        } else {
            log::info!("Got unsupported RPC service tag '{tag:?}' from {peer}");
            // We log this separately from generic errors, because
            // bad tags might highlight peer versioning issues.

            let _ = tls_stream.shutdown().await;
            return Err(anyhow::anyhow!("Unknown RPC service tag"));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use realize_types::Peer;
    use crate::network;
    use crate::network::testing;
    use tarpc::context;
    use tarpc::server::Channel as _;

    struct Fixture {
        resolver: Arc<RawPublicKeyResolver>,
        verifier: Arc<PeerVerifier>,
        server: Option<Arc<Server>>,
        port: u16,
    }

    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();

            let resolver = RawPublicKeyResolver::from_private_key(testing::server_private_key())?;
            let verifier = verifier_both();

            Ok(Self {
                resolver,
                verifier,
                server: None,
                port: 0,
            })
        }

        async fn start_server(&mut self) -> anyhow::Result<SocketAddr> {
            let networking = Networking::new(
                vec![],
                Arc::clone(&self.resolver),
                Arc::clone(&self.verifier),
            );
            let mut server = Server::new(networking);
            register_ping(&mut server);
            let server = Arc::new(server);
            let addr = Arc::clone(&server)
                .listen(&HostPort::localhost(self.port))
                .await?;

            self.server = Some(server);

            Ok(addr)
        }

        fn client_networking(&self, peer: &Peer, addr: SocketAddr) -> anyhow::Result<Networking> {
            self.client_networking_with_verifier(peer, addr, Arc::clone(&self.verifier))
        }

        fn client_networking_with_verifier(
            &self,
            peer: &Peer,
            addr: SocketAddr,
            verifier: Arc<PeerVerifier>,
        ) -> anyhow::Result<Networking> {
            Ok(Networking::new(
                vec![(peer, addr.to_string().as_ref())],
                RawPublicKeyResolver::from_private_key(testing::client_private_key())?,
                verifier,
            ))
        }
    }

    // Helper to create a PeerVerifier with only the server key
    fn verifier_server_only() -> Arc<PeerVerifier> {
        let mut verifier = PeerVerifier::new();
        verifier.add_peer(&Peer::from("server"), testing::server_public_key());
        Arc::new(verifier)
    }

    // Helper to create a PeerVerifier with only the client key
    fn verifier_client_only() -> Arc<PeerVerifier> {
        let mut verifier = PeerVerifier::new();
        verifier.add_peer(&Peer::from("client"), testing::client_public_key());
        Arc::new(verifier)
    }

    // Helper to create a PeerVerifier with both keys
    fn verifier_both() -> Arc<PeerVerifier> {
        let mut verifier = PeerVerifier::new();
        verifier.add_peer(&Peer::from("client"), testing::client_public_key());
        verifier.add_peer(&Peer::from("server"), testing::server_public_key());
        Arc::new(verifier)
    }

    #[tokio::test]
    async fn tarpc_tcp_connect() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let addr = fixture.start_server().await?;

        let peer = Peer::from("server");
        let networking = fixture.client_networking(&peer, addr)?;
        let client = connect_ping(&networking, &peer).await?;
        assert_eq!("pong", client.ping(context::current()).await?);

        Ok(())
    }

    #[tokio::test]
    async fn tarpc_tcp_reject_bad_tag() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let addr = fixture.start_server().await?;

        let connector = security::make_tls_connector(
            verifier_both(),
            RawPublicKeyResolver::from_private_key(testing::client_private_key())?,
        );
        let stream = TcpStream::connect(addr).await?;
        let domain = ServerName::try_from("localhost")?;
        let mut tls_stream = connector.connect(domain, stream).await?;
        tls_stream.write_all(b"BADD").await?;
        tls_stream.flush().await?;

        // Stream must disconnect right after reading the bad tag.
        let mut buf = [0u8; 12];
        assert_eq!(tls_stream.read(&mut buf).await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn client_not_in_server_verifier_fails() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture.verifier = verifier_server_only();
        let addr = fixture.start_server().await?;

        let peer = Peer::from("server");
        let networking = fixture.client_networking_with_verifier(&peer, addr, verifier_both())?;
        let client_result = connect_ping(&networking, &peer).await;
        let failed = match client_result {
            Ok(client) => client.ping(context::current()).await.is_err(),
            // If handshake fails, that's also acceptable
            Err(_) => true,
        };
        assert!(failed);

        Ok(())
    }

    #[tokio::test]
    async fn server_not_in_client_verifier_fails() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let addr = fixture.start_server().await?;

        let peer = Peer::from("server");
        let networking =
            fixture.client_networking_with_verifier(&peer, addr, verifier_client_only())?;
        let client_result = connect_ping(&networking, &peer).await;
        let failed = match client_result {
            Ok(client) => client.ping(context::current()).await.is_err(),
            // If handshake fails, that's also acceptable
            Err(_) => true,
        };
        assert!(failed);

        Ok(())
    }

    #[tokio::test]
    async fn server_shutdown() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let addr = fixture.start_server().await?;

        let peer = Peer::from("server");
        let networking = fixture.client_networking(&peer, addr)?;

        connect_ping(&networking, &peer).await?;

        fixture.server.expect("server").shutdown().await?;

        // After shutdown, connection fails
        let ret = connect_ping(&networking, &peer).await;

        assert_eq!(
            Some(std::io::ErrorKind::ConnectionRefused),
            network::testing::io_error_kind(ret.err())
        );

        Ok(())
    }

    #[tokio::test]
    async fn server_shutdown_when_dropped() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let addr = fixture.start_server().await?;

        let peer = Peer::from("server");
        let networking = fixture.client_networking(&peer, addr)?;

        connect_ping(&networking, &peer).await?;

        fixture.server.take();

        // Allow tasks to notice.
        tokio::task::yield_now().await;

        // Connection fails from now on. The exact error isn't known,
        // as the server could be in different steps of the shutdown
        // when the client attempts to connect.
        let ret = connect_ping(&networking, &peer).await;
        assert!(ret.is_err());

        Ok(())
    }

    /// A dummy test service, for testing client and server framework.
    #[tarpc::service]
    trait PingService {
        async fn ping() -> String;
    }
    const PING_TAG: &[u8; 4] = b"PING";

    #[derive(Clone)]
    struct PingServer {}

    impl PingService for PingServer {
        async fn ping(self, _ctx: context::Context) -> String {
            "pong".to_string()
        }
    }

    /// Register a [PingService] to the given server.
    fn register_ping(server: &mut Server) {
        server.register_server(PING_TAG, move |_, _, framed| {
            let transport = tarpc::serde_transport::new(framed, Bincode::default());
            let server = PingServer {};
            let serve_fn = PingServer::serve(server.clone());

            tarpc::server::BaseChannel::with_defaults(transport).execute(serve_fn)
        })
    }

    /// Connect to the given peer as [PingService].
    async fn connect_ping(
        networking: &Networking,
        peer: &Peer,
    ) -> anyhow::Result<PingServiceClient> {
        let transport = networking.connect(peer, PING_TAG, None).await?;

        Ok(PingServiceClient::new(tarpc::client::Config::default(), transport).spawn())
    }
}
