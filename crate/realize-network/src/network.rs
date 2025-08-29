use crate::config::PeerConfig;
use crate::hostport::HostPort;
use crate::security::{PeerVerifier, RawPublicKeyResolver};
use anyhow::Context as _;
use realize_types::Peer;
use rustls::pki_types::ServerName;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path;
use std::sync::Arc;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_rustls::{TlsAcceptor, TlsConnector};

#[derive(Clone)]
pub struct Networking {
    peer_setup: Arc<HashMap<Peer, PeerSetup>>,
    verifier: Arc<PeerVerifier>,
    acceptor: Arc<TlsAcceptor>,
    connector: Arc<TlsConnector>,
}

/// Information about peers
#[derive(Default, Debug, PartialEq, Eq, Clone)]
pub struct PeerSetup {
    pub address: Option<String>,
    pub batch_rate_limit: Option<u64>,
}
impl PeerSetup {
    fn from_config(config: &PeerConfig) -> Self {
        Self {
            address: config.address.clone(),
            batch_rate_limit: config.batch_rate_limit.as_ref().map(|bv| bv.0),
        }
    }
}

impl Networking {
    pub fn from_config(
        peers: &HashMap<Peer, PeerConfig>,
        privkey: &path::Path,
    ) -> anyhow::Result<Self> {
        let verifier = PeerVerifier::from_config(peers)?;
        let resolver = RawPublicKeyResolver::from_private_key_file(privkey)?;
        Ok(Self::new(
            peers.iter().map(|(p, c)| (*p, PeerSetup::from_config(c))),
            resolver,
            verifier,
        ))
    }

    pub fn new<'a, T>(
        peer_setup: T,
        resolver: Arc<RawPublicKeyResolver>,
        verifier: Arc<PeerVerifier>,
    ) -> Self
    where
        T: IntoIterator<Item = (Peer, PeerSetup)>,
    {
        Self {
            peer_setup: Arc::new(peer_setup.into_iter().collect()),
            verifier: Arc::clone(&verifier),
            acceptor: Arc::new(crate::security::make_tls_acceptor(
                Arc::clone(&verifier),
                Arc::clone(&resolver),
            )),
            connector: Arc::new(crate::security::make_tls_connector(verifier, resolver)),
        }
    }

    /// [PeerSetup] of all configured peers.
    pub fn peer_setup(&self) -> impl Iterator<Item = (Peer, &PeerSetup)> {
        self.peer_setup.iter().map(|(p, s)| (*p, s))
    }

    /// All known peers.
    pub fn peers(&self) -> impl Iterator<Item = Peer> {
        self.verifier.peers()
    }

    /// Set of peers that have a known address.
    pub fn connectable_peers(&self) -> impl Iterator<Item = Peer> {
        self.peer_setup
            .iter()
            .flat_map(|(p, s)| s.address.as_ref().map(|_| *p))
    }

    /// Check whether the peer is connectable.
    pub fn is_connectable(&self, peer: Peer) -> bool {
        self.peer_setup
            .get(&peer)
            .map(|s| s.address.is_some())
            .unwrap_or(false)
    }

    /// Return the batch rate limit configured for the peer, if any.
    pub fn batch_rate_limit(&self, peer: Peer) -> Option<u64> {
        self.peer_setup
            .get(&peer)
            .map(|s| s.batch_rate_limit)
            .flatten()
    }

    pub(crate) async fn connect_raw(
        &self,
        peer: Peer,
        tag: &[u8; 4],
    ) -> anyhow::Result<tokio_rustls::client::TlsStream<TcpStream>> {
        let addr = self
            .peer_setup
            .get(&peer)
            .map(|s| s.address.as_ref())
            .flatten()
            .ok_or(anyhow::anyhow!("no address known for {peer}"))?;
        let addr = HostPort::parse(addr)
            .await
            .with_context(|| format!("address for {peer} is invalid"))?;
        let domain = ServerName::try_from(addr.host().to_string())?;
        let stream = TcpStream::connect(addr.addr()).await?;
        stream.set_nodelay(true)?;

        let mut tls_stream = self.connector.connect(domain, stream).await?;
        tls_stream.write_all(tag).await?;

        Ok(tls_stream)
    }

    async fn accept(
        &self,
        stream: TcpStream,
    ) -> Result<(Peer, [u8; 4], tokio_rustls::server::TlsStream<TcpStream>), anyhow::Error> {
        let peer_addr = stream.peer_addr()?;
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
            dyn Fn(Peer, tokio_rustls::server::TlsStream<TcpStream>, broadcast::Receiver<()>)
                + Send
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

    /// Register a handler for the given tagged stream.
    pub(crate) fn register_raw(
        &mut self,
        tag: &'static [u8; 4],
        handler: impl Fn(Peer, tokio_rustls::server::TlsStream<TcpStream>, broadcast::Receiver<()>)
        + Send
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
            "Listening on {addr}",
            addr = listener.local_addr()?.to_string()
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
                                    Err(err) => log::debug!("@{peer} Connection rejected: {err}"),
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
        let (peer, tag, mut tls_stream) = self.networking.accept(stream).await?;
        if let Some(handler) = self.handlers.get(&tag) {
            (*handler)(peer, tls_stream, shutdown_rx);
        } else {
            log::info!("@peer Unknown RPC service tag '{tag:?}'; Shutting down.");
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
    use crate::testing;
    use realize_types::Peer;

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

        fn client_networking(&self, peer: Peer, addr: SocketAddr) -> anyhow::Result<Networking> {
            self.client_networking_with_verifier(peer, addr, Arc::clone(&self.verifier))
        }

        fn client_networking_with_verifier(
            &self,
            peer: Peer,
            addr: SocketAddr,
            verifier: Arc<PeerVerifier>,
        ) -> anyhow::Result<Networking> {
            Ok(Networking::new(
                [(
                    peer,
                    PeerSetup {
                        address: Some(addr.to_string()),
                        ..Default::default()
                    },
                )],
                RawPublicKeyResolver::from_private_key(testing::client_private_key())?,
                verifier,
            ))
        }
    }

    // Helper to create a PeerVerifier with only the server key
    fn verifier_server_only() -> Arc<PeerVerifier> {
        let mut verifier = PeerVerifier::new();
        verifier.add_peer(Peer::from("server"), testing::server_public_key());
        Arc::new(verifier)
    }

    // Helper to create a PeerVerifier with only the client key
    fn verifier_client_only() -> Arc<PeerVerifier> {
        let mut verifier = PeerVerifier::new();
        verifier.add_peer(Peer::from("client"), testing::client_public_key());
        Arc::new(verifier)
    }

    // Helper to create a PeerVerifier with both keys
    fn verifier_both() -> Arc<PeerVerifier> {
        let mut verifier = PeerVerifier::new();
        verifier.add_peer(Peer::from("client"), testing::client_public_key());
        verifier.add_peer(Peer::from("server"), testing::server_public_key());
        Arc::new(verifier)
    }

    #[tokio::test]
    async fn connect() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let addr = fixture.start_server().await?;

        let peer = Peer::from("server");
        let networking = fixture.client_networking(peer, addr)?;
        assert_eq!("ping!", connect_ping(&networking, peer).await?);

        Ok(())
    }

    #[tokio::test]
    async fn reject_bad_tag() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let addr = fixture.start_server().await?;

        let connector = crate::security::make_tls_connector(
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
        let networking = fixture.client_networking_with_verifier(peer, addr, verifier_both())?;
        assert!(connect_ping(&networking, peer).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn server_not_in_client_verifier_fails() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let addr = fixture.start_server().await?;

        let peer = Peer::from("server");
        let networking =
            fixture.client_networking_with_verifier(peer, addr, verifier_client_only())?;
        assert!(connect_ping(&networking, peer).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn server_shutdown() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let addr = fixture.start_server().await?;

        let peer = Peer::from("server");
        let networking = fixture.client_networking(peer, addr)?;

        connect_ping(&networking, peer).await?;

        fixture.server.expect("server").shutdown().await?;

        // After shutdown, connection fails
        let ret = connect_ping(&networking, peer).await;

        assert_eq!(
            Some(std::io::ErrorKind::ConnectionRefused),
            testing::io_error_kind(ret.err())
        );

        Ok(())
    }

    #[tokio::test]
    async fn server_shutdown_when_dropped() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let addr = fixture.start_server().await?;

        let peer = Peer::from("server");
        let networking = fixture.client_networking(peer, addr)?;

        connect_ping(&networking, peer).await?;

        fixture.server.take();

        // Allow tasks to notice.
        tokio::task::yield_now().await;

        // Connection fails from now on. The exact error isn't known,
        // as the server could be in different steps of the shutdown
        // when the client attempts to connect.
        let ret = connect_ping(&networking, peer).await;
        assert!(ret.is_err());

        Ok(())
    }

    const PING_TAG: &[u8; 4] = b"PING";

    /// Register a [PingService] to the given server.
    fn register_ping(server: &mut Server) {
        server.register_raw(PING_TAG, |_, mut stream, _| {
            tokio::spawn(async move {
                stream.write("ping!".as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            });
        });
    }

    /// Connect to the given peer as [PingService].
    async fn connect_ping(networking: &Networking, peer: Peer) -> anyhow::Result<String> {
        let mut transport = networking.connect_raw(peer, PING_TAG).await?;
        let mut str = String::new();
        transport.read_to_string(&mut str).await?;

        Ok(str)
    }
}
