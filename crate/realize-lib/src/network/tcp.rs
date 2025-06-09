//! TCP transport for Realize - Symmetric File Syncer
//!
//! This module provides secure, rate-limited, and reconnecting TCP transport for the RealizeService RPC interface.
//! It handles TLS setup, peer authentication, DNS resolution, and connection retry logic.

use anyhow::Context as _;
use async_speed_limit::clock::StandardClock;
use async_speed_limit::Limiter;
use futures::prelude::*;
use rustls::pki_types::ServerName;
use tarpc::client::stub::Stub;
use tarpc::client::RpcError;
use tarpc::context;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::broadcast;
use tokio::sync::Mutex;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::TlsConnector;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use tarpc::serde_transport as transport;
use tarpc::tokio_serde::formats::Bincode;
use tarpc::tokio_util::codec::length_delimited::LengthDelimitedCodec;

use crate::model::Peer;
use crate::network::rate_limit::RateLimitedStream;
use crate::network::reconnect::Connect;
use crate::network::reconnect::Reconnect;
use crate::network::rpc::realize;
use crate::network::rpc::realize::metrics;
use crate::network::rpc::realize::metrics::MetricsRealizeClient;
use crate::network::rpc::realize::Config;
use crate::network::rpc::realize::RealizeServiceClient;
use crate::network::rpc::realize::RealizeServiceRequest;
use crate::network::rpc::realize::RealizeServiceResponse;
use crate::network::security;
use crate::network::security::PeerVerifier;

use super::config::PeerConfig;
use super::hostport::HostPort;
use super::security::RawPublicKeyResolver;

pub type TcpRealizeServiceClient = RealizeServiceClient<TcpStub>;

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
        let verifier = PeerVerifier::from_config(&peers)?;
        let resolver = RawPublicKeyResolver::from_private_key_file(privkey)?;
        Ok(Self::new(
            peers.iter().flat_map(|(p, c)| {
                if let Some(addr) = &c.address {
                    Some((p, addr.as_ref()))
                } else {
                    None
                }
            }),
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

    pub(crate) async fn connect<Req, Resp>(
        &self,
        peer: &Peer,
        tag: &[u8; 4],
        limiter: Option<Limiter>,
    ) -> Result<
        transport::Transport<
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
        let addr = self
            .addresses
            .get(&peer)
            .ok_or(anyhow::anyhow!("no address known for {peer}"))?;
        let addr = HostPort::parse(addr)
            .await
            .with_context(|| format!("address for {peer} is invalid"))?;
        let domain = ServerName::try_from(addr.host().to_string())?;
        let stream = TcpStream::connect(addr.addr()).await?;
        let stream = RateLimitedStream::new(
            stream,
            limiter.unwrap_or_else(|| Limiter::<StandardClock>::new(f64::INFINITY)),
        );
        let mut tls_stream = self.connector.connect(domain, stream).await?;
        tls_stream.write_all(tag).await?;

        let transport = transport::new(
            LengthDelimitedCodec::builder().new_framed(tls_stream),
            Bincode::default(),
        );

        Ok(transport)
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
        log::info!("Accepted peer {} from {}", peer, peer_addr);
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
                    tarpc::tokio_util::codec::Framed<
                        tokio_rustls::server::TlsStream<RateLimitedStream<TcpStream>>,
                        LengthDelimitedCodec,
                    >,
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

    /// Register a handler for the given tag.
    ///
    /// If registering a TARPC service listener, use
    /// [Server::register_server] instead. This method is mostly
    /// useful for registering TARPC clients.
    pub(crate) fn register(
        &mut self,
        tag: &'static [u8; 4],
        handler: impl Fn(
                Peer,
                tarpc::tokio_util::codec::Framed<
                    tokio_rustls::server::TlsStream<RateLimitedStream<TcpStream>>,
                    LengthDelimitedCodec,
                >,
                Limiter,
                broadcast::Receiver<()>,
            ) + Send
            + Sync
            + 'static,
    ) {
        self.handlers.insert(tag, Box::new(handler));
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
        self.register(
            tag,
            move |peer: Peer,
                  framed,
                  limiter: Limiter,
                  mut shutdown_rx: broadcast::Receiver<()>| {
                let stream = handler(&peer, limiter, framed);

                tokio::spawn(async move {
                    let mut stream = Box::pin(stream);
                    loop {
                        tokio::select!(
                            _ = shutdown_rx.recv() => {
                                log::debug!("Shutting connection to {}", peer);
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
    pub async fn listen(self: Arc<Self>, hostport: &HostPort) -> anyhow::Result<SocketAddr> {
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
        let weak_self = Arc::downgrade(&self);
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
                                    Err(err) => log::debug!("{}: connection rejected: {}", peer, err),
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
        let shutdown_rx = self.shutdown_tx.subscribe();
        let limiter = Limiter::<StandardClock>::new(f64::INFINITY);
        let (peer, tag, mut tls_stream) = self.networking.accept(stream, limiter.clone()).await?;
        if let Some(handler) = self.handlers.get(&tag) {
            let framed = LengthDelimitedCodec::builder().new_framed(tls_stream);

            (*handler)(peer, framed, limiter, shutdown_rx);
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

#[derive(Clone, Default)]
pub struct ClientOptions {
    pub limiter: Option<Limiter<StandardClock>>,
    pub connection_events: Option<tokio::sync::watch::Sender<ClientConnectionState>>,
}

/// What happened to a client connection.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum ClientConnectionState {
    /// Initial state
    NotConnected,
    /// The client is attempting to connect.
    Connecting,
    /// The client is connected.
    Connected,
}

/// Create a [RealizeServiceClient] connected to the given TCP address.
pub async fn connect_client(
    networking: &Networking,
    peer: &Peer,
    options: ClientOptions,
) -> anyhow::Result<TcpRealizeServiceClient> {
    let stub = TcpStub::new(networking, peer, options).await?;
    Ok(RealizeServiceClient::from(stub))
}

#[derive(Clone)]
struct TcpConnect {
    networking: Networking,
    peer: Peer,
    options: ClientOptions,
    config: Arc<Mutex<Option<Config>>>,
}

impl Connect<tarpc::client::Channel<Arc<RealizeServiceRequest>, RealizeServiceResponse>>
    for TcpConnect
{
    async fn try_connect(
        &self,
    ) -> anyhow::Result<tarpc::client::Channel<Arc<RealizeServiceRequest>, RealizeServiceResponse>>
    {
        if let Some(tx) = &self.options.connection_events {
            let _ = tx.send(ClientConnectionState::Connecting);
        }
        let transport = self
            .networking
            .connect(&self.peer, realize::TAG, self.options.limiter.clone())
            .await?;
        let channel = tarpc::client::new(Default::default(), transport).spawn();

        // Recover stored configuration
        if let Some(config) = self.config.lock().await.as_ref() {
            let req = Arc::new(RealizeServiceRequest::Configure {
                config: config.clone(),
            });
            channel.call(context::current(), req).await?;
        }

        if let Some(tx) = &self.options.connection_events {
            let _ = tx.send(ClientConnectionState::Connected);
        }
        Ok(channel)
    }
}

/// A type that encapsulates and hides the actual stub type.
///
/// Most usage should be through the alias [TcpRealizeServiceClient].
pub struct TcpStub {
    inner: MetricsRealizeClient<
        Reconnect<
            tarpc::client::Channel<Arc<RealizeServiceRequest>, RealizeServiceResponse>,
            TcpConnect,
            ExponentialBackoff,
        >,
    >,
    config: Arc<Mutex<Option<Config>>>,
}

impl TcpStub {
    async fn new(
        networking: &Networking,
        peer: &Peer,
        options: ClientOptions,
    ) -> anyhow::Result<Self> {
        let retry_strategy =
            ExponentialBackoff::from_millis(500).max_delay(Duration::from_secs(5 * 60));

        let config = Arc::new(Mutex::new(None));

        if let Some(tx) = &options.connection_events {
            let _ = tx.send(ClientConnectionState::NotConnected);
        }
        let connect = TcpConnect {
            networking: networking.clone(),
            peer: peer.clone(),
            options,
            config: Arc::clone(&config),
        };

        let reconnect: Reconnect<
            tarpc::client::Channel<Arc<RealizeServiceRequest>, RealizeServiceResponse>,
            TcpConnect,
            ExponentialBackoff,
        > = Reconnect::new(retry_strategy, connect, Some(Duration::from_secs(5 * 60))).await?;
        let stub = MetricsRealizeClient::new(reconnect);
        Ok(Self {
            inner: stub,
            config,
        })
    }
}

impl Stub for TcpStub {
    type Req = RealizeServiceRequest;
    type Resp = RealizeServiceResponse;

    async fn call(
        &self,
        ctx: context::Context,
        request: RealizeServiceRequest,
    ) -> std::result::Result<RealizeServiceResponse, RpcError> {
        let res = self.inner.call(ctx, request).await;

        // Store configuration to recover it on reconnection.
        if let Ok(RealizeServiceResponse::Configure(Ok(config))) = &res {
            *(self.config.lock().await) = if *config == Config::default() {
                None
            } else {
                Some(config.clone())
            };
        }

        res
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;
    use crate::model::{Arena, Peer};
    use crate::network;
    use crate::network::rpc::realize::{Config, Options};
    use crate::network::security::testing;
    use crate::storage::real::LocalStorage;
    use crate::utils::async_utils::AbortOnDrop;
    use assert_fs::TempDir;
    use tarpc::context;

    struct Fixture {
        storage: LocalStorage,
        resolver: Arc<RawPublicKeyResolver>,
        verifier: Arc<PeerVerifier>,
        server: Option<Arc<Server>>,
        _tempdir: TempDir,
    }

    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();

            let tempdir = TempDir::new()?;
            let storage = LocalStorage::single(&Arena::from("testdir"), tempdir.path());
            let resolver = RawPublicKeyResolver::from_private_key(testing::server_private_key())?;
            let verifier = verifier_both();

            Ok(Self {
                storage,
                resolver,
                verifier,
                server: None,
                _tempdir: tempdir,
            })
        }

        async fn start_server(&mut self) -> anyhow::Result<SocketAddr> {
            let networking = Networking::new(
                vec![],
                Arc::clone(&self.resolver),
                Arc::clone(&self.verifier),
            );
            let mut server = Server::new(networking);
            realize::server::register(&mut server, self.storage.clone());
            let server = Arc::new(server);
            let addr = Arc::clone(&server).listen(&HostPort::localhost(0)).await?;

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

        let options = ClientOptions::default();
        let peer = Peer::from("server");
        let networking = fixture.client_networking(&peer, addr)?;
        let client = connect_client(&networking, &peer, options).await?;
        let list = client
            .list(
                context::current(),
                Arena::from("testdir"),
                Options::default(),
            )
            .await??;
        assert!(list.is_empty());
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

        let options = ClientOptions::default();
        let peer = Peer::from("server");
        let networking = fixture.client_networking_with_verifier(&peer, addr, verifier_both())?;
        let client_result = connect_client(&networking, &peer, options).await;
        let failed = match client_result {
            Ok(client) => client
                .list(
                    context::current(),
                    Arena::from("testdir"),
                    Options::default(),
                )
                .await
                .is_err(),
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

        let options = ClientOptions::default();
        let peer = Peer::from("server");
        let networking =
            fixture.client_networking_with_verifier(&peer, addr, verifier_client_only())?;
        let client_result = connect_client(&networking, &peer, options).await;
        let failed = match client_result {
            Ok(client) => client
                .list(
                    context::current(),
                    Arena::from("testdir"),
                    Options::default(),
                )
                .await
                .is_err(),
            // If handshake fails, that's also acceptable
            Err(_) => true,
        };
        assert!(failed);

        Ok(())
    }

    #[tokio::test]
    async fn configure_tcp_returns_limit() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let addr = fixture.start_server().await?;

        let options = ClientOptions::default();
        let peer = Peer::from("server");
        let networking = fixture.client_networking(&peer, addr)?;
        let client = connect_client(&networking, &peer, options).await?;
        let limit = 12345u64;
        let config = client
            .configure(
                context::current(),
                Config {
                    write_limit: Some(limit),
                },
            )
            .await?;
        assert_eq!(config.unwrap().write_limit, Some(limit));
        let config = client
            .configure(context::current(), Config { write_limit: None })
            .await?;
        assert_eq!(config.unwrap().write_limit, Some(limit));
        Ok(())
    }

    #[tokio::test]
    async fn configure_tcp_per_connection_limit() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let addr = fixture.start_server().await?;

        let peer = Peer::from("server");
        let networking = fixture.client_networking(&peer, addr)?;
        let client1 = connect_client(&networking, &peer, ClientOptions::default()).await?;
        let client2 = connect_client(&networking, &peer, ClientOptions::default()).await?;
        let limit = 54321u64;
        let config1 = client1
            .configure(
                context::current(),
                Config {
                    write_limit: Some(limit),
                },
            )
            .await?;
        assert_eq!(config1.unwrap().write_limit, Some(limit));
        let config2 = client2
            .configure(context::current(), Config { write_limit: None })
            .await?;
        assert_eq!(config2.unwrap().write_limit, None);
        Ok(())
    }

    #[tokio::test]
    async fn tcp_reconnect() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let addr = fixture.start_server().await?;

        let connection_count = Arc::new(AtomicU32::new(0));
        let (shutdown, _) = tokio::sync::broadcast::channel(1);
        let (proxy_addr, _proxy_handle) =
            proxy_tcp(&addr, shutdown.clone(), connection_count.clone()).await?;

        let peer = Peer::from("server");
        let networking = fixture.client_networking(&peer, proxy_addr)?;
        let client = connect_client(&networking, &peer, ClientOptions::default()).await?;

        client
            .list(
                context::current(),
                Arena::from("testdir"),
                Options::default(),
            )
            .await??;
        shutdown.send(())?;
        client
            .list(
                context::current(),
                Arena::from("testdir"),
                Options::default(),
            )
            .await??;
        assert_eq!(connection_count.load(Ordering::Relaxed), 2);

        Ok(())
    }

    #[tokio::test]
    async fn tcp_reconnect_keeps_config() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let addr = fixture.start_server().await?;

        let connection_count = Arc::new(AtomicU32::new(0));
        let (shutdown, _) = tokio::sync::broadcast::channel(1);
        let (proxy_addr, _proxy_handle) =
            proxy_tcp(&addr, shutdown.clone(), connection_count.clone()).await?;

        let peer = Peer::from("server");
        let networking = fixture.client_networking(&peer, proxy_addr)?;
        let client = connect_client(&networking, &peer, ClientOptions::default()).await?;

        let config = Config {
            write_limit: Some(1024),
        };
        assert_eq!(
            config,
            client
                .configure(context::current(), config.clone())
                .await??
        );
        assert_eq!(
            config,
            client
                .configure(context::current(), Config::default())
                .await??
        );

        shutdown.send(())?;
        assert_eq!(
            config,
            client
                .configure(context::current(), Config::default())
                .await??
        );
        assert_eq!(connection_count.load(Ordering::Relaxed), 2);

        Ok(())
    }

    #[tokio::test]
    async fn tcp_reconnect_events() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let addr = fixture.start_server().await?;

        let (shutdown, _) = tokio::sync::broadcast::channel(1);
        let (proxy_addr, _proxy_handle) =
            proxy_tcp(&addr, shutdown.clone(), Arc::new(AtomicU32::new(0))).await?;

        let (conn_tx, mut conn_rx) =
            tokio::sync::watch::channel(ClientConnectionState::NotConnected);
        let history = tokio::spawn(async move {
            let mut history = vec![];
            loop {
                history.push(*conn_rx.borrow_and_update());
                if conn_rx.changed().await.is_err() {
                    return history;
                }
            }
        });

        let peer = Peer::from("server");
        let networking = fixture.client_networking(&peer, proxy_addr)?;
        let options = ClientOptions {
            connection_events: Some(conn_tx),
            ..ClientOptions::default()
        };
        let client = connect_client(&networking, &peer, options).await?;

        client
            .list(
                context::current(),
                Arena::from("testdir"),
                Options::default(),
            )
            .await??;
        shutdown.send(())?;
        client
            .list(
                context::current(),
                Arena::from("testdir"),
                Options::default(),
            )
            .await??;
        drop(client); // also closes conn_tx

        let history = history.await?;
        assert_eq!(
            vec![
                ClientConnectionState::Connecting,
                ClientConnectionState::Connected,
                ClientConnectionState::Connecting,
                ClientConnectionState::Connected,
            ],
            history
        );

        Ok(())
    }

    #[tokio::test]
    async fn server_shutdown() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let addr = fixture.start_server().await?;

        let peer = Peer::from("server");
        let networking = fixture.client_networking(&peer, addr)?;

        connect_client(&networking, &peer, ClientOptions::default()).await?;

        fixture.server.expect("server").shutdown().await?;

        // After shutdown, connection fails
        let ret = connect_client(&networking, &peer, ClientOptions::default()).await;

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

        connect_client(&networking, &peer, ClientOptions::default()).await?;

        fixture.server.take();

        // Allow tasks to notice.
        tokio::task::yield_now().await;

        // Connection fails from now on. The exact error isn't known,
        // as the server could be in different steps of the shutdown
        // when the client attempts to connect.
        let ret = connect_client(&networking, &peer, ClientOptions::default()).await;
        assert!(ret.is_err());

        Ok(())
    }

    /// Create a listener that proxies data to [server_addr].
    ///
    /// There can be only one connection at a time. Sending a message
    /// to [shutdown] shuts down that connection.
    ///
    /// This proxy is geared towards simplicity, not efficiency or
    /// correctness. It ignores errors. This is barely acceptable for
    /// a simple test.
    async fn proxy_tcp(
        server_addr: &SocketAddr,
        shutdown: tokio::sync::broadcast::Sender<()>,
        connection_count: Arc<AtomicU32>,
    ) -> anyhow::Result<(SocketAddr, AbortOnDrop<()>)> {
        let listener = TcpListener::bind("localhost:0").await?;

        let addr = listener.local_addr()?;
        let server_addr = *server_addr;

        let handle = AbortOnDrop::new(tokio::spawn(async move {
            while let Ok((mut inbound, _)) = listener.accept().await {
                if let Ok(mut outbound) = TcpStream::connect(server_addr).await {
                    connection_count.fetch_add(1, Ordering::Relaxed);

                    let mut shutdown = shutdown.subscribe();
                    let mut buf_in_to_out = [0u8; 8 * 1024];
                    let mut buf_out_to_in = [0u8; 8 * 1024];
                    loop {
                        tokio::select! {
                            res = inbound.read(&mut buf_in_to_out) => {
                                if let Ok(n) = res {
                                    if n > 0 && outbound.write_all(&buf_in_to_out[0..n]).await.is_ok() {
                                        continue
                                    }
                                }
                                break;
                            },
                            res = outbound.read(&mut buf_out_to_in) => {
                                if let Ok(n) = res {
                                    if n > 0 && inbound.write_all(&buf_out_to_in[0..n]).await.is_ok() {
                                        continue;
                                    }
                                }
                                break;
                            },
                            _ = shutdown.recv() => {
                                break;
                            }
                        }
                    }
                    let _ = tokio::join!(inbound.shutdown(), outbound.shutdown());
                }
            }
        }));

        Ok((addr, handle))
    }
}
