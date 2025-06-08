//! TCP transport for Realize - Symmetric File Syncer
//!
//! This module provides secure, rate-limited, and reconnecting TCP transport for the RealizeService RPC interface.
//! It handles TLS setup, peer authentication, DNS resolution, and connection retry logic.

use anyhow::Context as _;
use async_speed_limit::clock::StandardClock;
use async_speed_limit::Limiter;
use futures::prelude::*;
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::PrivateKeyDer;
use rustls::pki_types::ServerName;
use rustls::sign::SigningKey;
use tarpc::client::stub::Stub;
use tarpc::client::RpcError;
use tarpc::context;
use tarpc::context::Context;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_retry::strategy::ExponentialBackoff;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use tarpc::serde_transport as transport;
use tarpc::server::{BaseChannel, Channel};
use tarpc::tokio_serde::formats::Bincode;
use tarpc::tokio_util::codec::length_delimited::LengthDelimitedCodec;

use crate::model::Arena;
use crate::model::Peer;
use crate::network::rate_limit::RateLimitedStream;
use crate::network::reconnect::Connect;
use crate::network::reconnect::Reconnect;
use crate::network::rpc;
use crate::network::rpc::history::{self, HistoryServiceClient};
use crate::network::rpc::realize;
use crate::network::rpc::realize::metrics;
use crate::network::rpc::realize::metrics::MetricsRealizeClient;
use crate::network::rpc::realize::metrics::MetricsRealizeServer;
use crate::network::rpc::realize::server::RealizeServer;
use crate::network::rpc::realize::Config;
use crate::network::rpc::realize::RealizeServiceRequest;
use crate::network::rpc::realize::RealizeServiceResponse;
use crate::network::rpc::realize::{RealizeService, RealizeServiceClient};
use crate::network::security;
use crate::network::security::PeerVerifier;
use crate::storage::real::LocalStorage;
use crate::storage::real::Notification;
use crate::utils::async_utils::AbortOnDrop;

use super::config::PeerConfig;
use super::hostport::HostPort;

pub type TcpRealizeServiceClient = RealizeServiceClient<TcpStub>;

#[derive(Clone)]
pub struct Networking {
    addresses: Arc<HashMap<Peer, String>>,
    privkey: Arc<dyn SigningKey>,
    verifier: Arc<PeerVerifier>,
}

impl Networking {
    pub fn from_config(
        peers: &HashMap<Peer, PeerConfig>,
        privkey: &path::Path,
    ) -> anyhow::Result<Self> {
        let verifier = PeerVerifier::from_config(&peers)?;
        let privkey =
            security::default_provider()
                .key_provider
                .load_private_key(PrivateKeyDer::from_pem_file(privkey).with_context(|| {
                    format!("not a PEM-encoded private key: {}", privkey.display())
                })?)
                .with_context(|| format!("invalid private key in {}", privkey.display()))?;

        Ok(Self::new(
            peers.iter().flat_map(|(p, c)| {
                if let Some(addr) = &c.address {
                    Some((p, addr.as_ref()))
                } else {
                    None
                }
            }),
            privkey,
            verifier,
        ))
    }

    pub fn new<'a, T>(
        addresses: T,
        privkey: Arc<dyn SigningKey>,
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
            privkey,
            verifier,
        }
    }

    async fn connect<Req, Resp>(
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
        let connector =
            security::make_tls_connector(Arc::clone(&self.verifier), Arc::clone(&self.privkey))?;

        let stream = TcpStream::connect(addr.addr()).await?;
        let stream = RateLimitedStream::new(
            stream,
            limiter.unwrap_or_else(|| Limiter::<StandardClock>::new(f64::INFINITY)),
        );
        let mut tls_stream = connector.connect(domain, stream).await?;
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
        let acceptor =
            security::make_tls_acceptor(Arc::clone(&self.verifier), Arc::clone(&self.privkey))?;
        let mut tls_stream = acceptor.accept(stream).await?;
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
    pub fn new(networking: Networking) -> Self {
        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);
        Self {
            networking,
            handlers: HashMap::new(),
            shutdown_tx,
        }
    }

    fn register(
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

    fn register_server<T, S, F>(&mut self, tag: &'static [u8; 4], handler: T)
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

    pub fn shutdown(&self) -> anyhow::Result<()> {
        self.shutdown_tx.send(())?;

        Ok(())
    }

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
        tokio::spawn(async move {
            loop {
                tokio::select!(
                    _ = shutdown_listener_rx.recv() => {
                        log::debug!("Shutting down listener");
                        return;
                    }
                    Ok((stream, peer)) = listener.accept() => {
                        match self.accept(stream).await {
                            Ok(_) => {}
                            Err(err) => log::debug!("{}: connection rejected: {}", peer, err),
                        };
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

/// Start the server, listening on the given address.
///
/// Returns (address, shutdown, handle)
///
/// To stop the server cleanly, send a message to the shutdown channel
/// then wait for that channel to be closed. Dropping the shutdown
/// channel also eventually shuts down the server.
pub async fn start_server(
    hostport: &HostPort,
    storage: LocalStorage,
    verifier: Arc<PeerVerifier>,
    privkey: Arc<dyn SigningKey>,
) -> anyhow::Result<(SocketAddr, tokio::sync::broadcast::Sender<()>)> {
    let networking = Networking::new(vec![], privkey, verifier);
    let mut server = Server::new(networking);

    let realize_storage = storage.clone();
    let history_storage = storage.clone();
    server.register_server(rpc::realize::TAG, move |_peer, limiter, framed| {
        realize_requests(realize_storage.clone(), limiter, framed)
    });
    server.register(
        rpc::history::TAG,
        move |peer: Peer, framed, _limiter: Limiter, shutdown_rx| {
            let transport = transport::new(framed, Bincode::default());
            let channel = tarpc::client::new(Default::default(), transport).spawn();
            let client = HistoryServiceClient::from(channel);

            let peer = peer.clone();
            let storage = history_storage.clone();
            tokio::spawn(async move {
                if let Err(err) = history::client::collect(client, storage, shutdown_rx).await {
                    log::debug!("{}: history collection failed: {}", peer, err);
                }
            });
        },
    );

    let server = Arc::new(server);
    let addr = Arc::clone(&server).listen(hostport).await?;

    Ok((addr, server.shutdown_tx.clone()))
}

fn realize_requests(
    storage: LocalStorage,
    limiter: Limiter,
    framed: tarpc::tokio_util::codec::Framed<
        tokio_rustls::server::TlsStream<RateLimitedStream<TcpStream>>,
        LengthDelimitedCodec,
    >,
) -> impl Stream<Item = impl Future<Output = ()>> {
    let transport = transport::new(framed, Bincode::default());
    let server = RealizeServer::new_limited(storage, limiter);
    let serve_fn = MetricsRealizeServer::new(RealizeServer::serve(server.clone()));
    let stream = BaseChannel::with_defaults(transport).execute(serve_fn);
    stream
}

/// Forward history for the given peer to a mpsc channel.
///
/// Connect to the peer, then ask it to send history for the given
/// arenas.
///
/// When this function returns successfully, the connection to the
/// peer has succeeded and the peer has declared itself ready to
/// report any file changes. This might take a while.
///
/// After a successful connection, the forwarder runs until either the
/// channel is closed or the connection is lost. Check the
/// [JoinHandle].
pub async fn forward_peer_history<T>(
    networking: &Networking,
    ctx: Context,
    peer: &Peer,
    arenas: T,
    tx: mpsc::Sender<(Peer, Notification)>,
) -> anyhow::Result<(Vec<Arena>, JoinHandle<anyhow::Result<()>>)>
where
    T: IntoIterator<Item = Arena>,
{
    let transport = networking.connect(&peer, history::TAG, None).await?;

    let (ready_tx, ready_rx) = oneshot::channel();
    let peer = peer.clone();
    let arenas = arenas.into_iter().collect();
    let handle = AbortOnDrop::new(tokio::spawn(history::server::run_forwarder(
        peer,
        arenas,
        BaseChannel::with_defaults(transport),
        tx,
        Some(ready_tx),
    )));

    // Wait for the peer to declare itself ready before returning.
    // This might take a while.
    //
    // If the context deadline is reached, abort the forwarder and
    // return a deadline error.
    match timeout(ctx.deadline.duration_since(Instant::now()), ready_rx).await? {
        Err(oneshot::error::RecvError { .. }) => {
            // The forwarder must have already died if the oneshot was
            // killed before anything was sent. Attempt to recover the original error.
            handle.join().await??;

            // The following should never be reached:
            anyhow::bail!("Connection failed");
        }
        Ok(watched) => {
            return Ok((watched, handle.as_handle()));
        }
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
    use crate::model::{self, Arena, Peer};
    use crate::network::rpc::realize::{Config, Options};
    use crate::network::security::testing;
    use crate::utils::async_utils::AbortOnDrop;
    use assert_fs::prelude::{FileWriteStr as _, PathChild as _};
    use assert_fs::TempDir;
    use rustls::pki_types::PrivateKeyDer;
    use tarpc::context;
    use tokio::time::timeout;

    // Helper to setup a test server and return (server_addr, server_handle, crypto, temp_dir)
    async fn setup_test_server(
        verifier: Arc<PeerVerifier>,
    ) -> anyhow::Result<(SocketAddr, tokio::sync::broadcast::Sender<()>, TempDir)> {
        let temp = TempDir::new()?;
        let dirs = LocalStorage::single(&Arena::from("testdir"), temp.path());
        let server_privkey = load_private_key(testing::server_private_key())?;
        let (addr, shutdown) = start_server(
            &HostPort::parse("127.0.0.1:0").await?,
            dirs,
            verifier,
            server_privkey,
        )
        .await?;

        Ok((addr, shutdown, temp))
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

    fn load_private_key(
        private_key: PrivateKeyDer<'static>,
    ) -> anyhow::Result<Arc<dyn SigningKey>> {
        Ok(security::default_provider()
            .key_provider
            .load_private_key(private_key)?)
    }

    #[tokio::test]
    async fn tarpc_tcp_connect() -> anyhow::Result<()> {
        let verifier = verifier_both();
        let (addr, _shutdown, _temp) = setup_test_server(Arc::clone(&verifier)).await?;
        let client_privkey = load_private_key(testing::client_private_key())?;
        let options = ClientOptions::default();
        let peer = Peer::from("server");
        let networking = Networking::new(
            vec![(&peer, addr.to_string().as_ref())],
            client_privkey,
            verifier,
        );
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
        let verifier = verifier_both();
        let (addr, _shutdown, _temp) = setup_test_server(Arc::clone(&verifier)).await?;
        let client_privkey = load_private_key(testing::client_private_key())?;

        let connector = security::make_tls_connector(verifier_both(), client_privkey)?;
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
        let verifier = verifier_server_only();
        let (addr, _shutdown, _temp) = setup_test_server(Arc::clone(&verifier)).await.unwrap();
        let client_privkey = load_private_key(testing::client_private_key())?;
        let options = ClientOptions::default();
        let peer = Peer::from("server");
        let networking = Networking::new(
            vec![(&peer, addr.to_string().as_ref())],
            client_privkey,
            verifier,
        );
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
        let verifier = verifier_client_only();
        let (addr, _shutdown, _temp) = setup_test_server(Arc::clone(&verifier)).await.unwrap();
        let client_privkey = load_private_key(testing::client_private_key())?;
        let options = ClientOptions::default();
        let peer = Peer::from("server");
        let networking = Networking::new(
            vec![(&peer, addr.to_string().as_ref())],
            client_privkey,
            verifier,
        );
        let result = connect_client(&networking, &peer, options).await;
        assert!(
            result.is_err(),
            "Expected error when server is not in client verifier"
        );

        Ok(())
    }

    #[tokio::test]
    async fn configure_tcp_returns_limit() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let dirs = LocalStorage::single(&Arena::from("testdir"), temp.path());
        let verifier = verifier_both();
        let server_privkey = load_private_key(testing::server_private_key())?;
        let (addr, _handle) = start_server(
            &HostPort::parse("127.0.0.1:0").await?,
            dirs.clone(),
            verifier.clone(),
            server_privkey,
        )
        .await?;
        let client_privkey = load_private_key(testing::client_private_key())?;
        let options = ClientOptions::default();
        let peer = Peer::from("server");
        let networking = Networking::new(
            vec![(&peer, addr.to_string().as_ref())],
            client_privkey,
            verifier,
        );
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
        let temp = TempDir::new()?;
        let dirs = LocalStorage::single(&Arena::from("testdir"), temp.path());
        let verifier = verifier_both();
        let server_privkey = load_private_key(testing::server_private_key())?;
        let (addr, _handle) = start_server(
            &HostPort::parse("127.0.0.1:0").await?,
            dirs.clone(),
            verifier.clone(),
            server_privkey,
        )
        .await?;
        let client_privkey1 = load_private_key(testing::client_private_key())?;
        let client_privkey2 = load_private_key(testing::client_private_key())?;
        let peer = Peer::from("server");
        let client1 = connect_client(
            &Networking::new(
                vec![(&peer, addr.to_string().as_ref())],
                client_privkey1,
                Arc::clone(&verifier),
            ),
            &peer,
            ClientOptions::default(),
        )
        .await?;
        let client2 = connect_client(
            &Networking::new(
                vec![(&peer, addr.to_string().as_ref())],
                client_privkey2,
                Arc::clone(&verifier),
            ),
            &peer,
            ClientOptions::default(),
        )
        .await?;
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
        let verifier = verifier_both();
        let (addr, _shutdown, _temp) = setup_test_server(Arc::clone(&verifier)).await?;

        let connection_count = Arc::new(AtomicU32::new(0));
        let (shutdown, _) = tokio::sync::broadcast::channel(1);
        let (proxy_addr, _proxy_handle) =
            proxy_tcp(&addr, shutdown.clone(), connection_count.clone()).await?;

        let client_privkey = load_private_key(testing::client_private_key())?;
        let peer = Peer::from("server");
        let networking = Networking::new(
            vec![(&peer, proxy_addr.to_string().as_ref())],
            client_privkey,
            verifier,
        );
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
        let verifier = verifier_both();
        let (addr, _shutdown, _temp) = setup_test_server(Arc::clone(&verifier)).await?;

        let connection_count = Arc::new(AtomicU32::new(0));
        let (shutdown, _) = tokio::sync::broadcast::channel(1);
        let (proxy_addr, _proxy_handle) =
            proxy_tcp(&addr, shutdown.clone(), connection_count.clone()).await?;

        let client_privkey = load_private_key(testing::client_private_key())?;
        let peer = Peer::from("server");
        let networking = Networking::new(
            vec![(&peer, proxy_addr.to_string().as_ref())],
            client_privkey,
            verifier,
        );
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
        let verifier = verifier_both();
        let (addr, _shutdown, _temp) = setup_test_server(Arc::clone(&verifier)).await?;

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
        let networking = Networking::new(
            vec![(&peer, proxy_addr.to_string().as_ref())],
            load_private_key(testing::client_private_key())?,
            verifier_both(),
        );
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

    #[tokio::test]
    async fn server_shutdown() -> anyhow::Result<()> {
        let verifier = verifier_both();
        let (addr, shutdown, _temp) = setup_test_server(Arc::clone(&verifier)).await?;

        let privkey = load_private_key(testing::client_private_key())?;
        let peer = Peer::from("server");
        let networking =
            Networking::new(vec![(&peer, addr.to_string().as_ref())], privkey, verifier);

        connect_client(&networking, &peer, ClientOptions::default()).await?;

        shutdown.send(())?;
        shutdown.closed().await;

        // After shutdown, connection fails
        let ret = connect_client(&networking, &peer, ClientOptions::default()).await;

        assert_eq!(
            Some(std::io::ErrorKind::ConnectionRefused),
            io_error_kind(ret.err())
        );

        Ok(())
    }

    #[tokio::test]
    async fn forward_history() -> anyhow::Result<()> {
        let _ = env_logger::try_init();

        let verifier = verifier_both();
        let (addr, _shutdown, tempdir) = setup_test_server(Arc::clone(&verifier)).await?;
        let privkey = load_private_key(testing::client_private_key())?;
        let server = Peer::from("server");
        let net = Networking::new(
            vec![(&server, addr.to_string().as_ref())],
            privkey,
            verifier,
        );
        let arena = Arena::from("testdir");
        let (tx, mut rx) = mpsc::channel(10);
        let (watched, handle) =
            forward_peer_history(&net, context::current(), &server, vec![arena.clone()], tx)
                .await?;

        assert_eq!(vec![arena.clone()], watched);

        // The forwarder is ready; a new file will generate a notification.

        let foobar = tempdir.child("foobar.txt");
        foobar.write_str("test")?;
        let foobar_mtime = foobar.metadata()?.modified()?;

        let (peer, notif) = timeout(Duration::from_secs(1), rx.recv()).await?.unwrap();
        assert_eq!(Peer::from("server"), peer);

        assert_eq!(
            Notification::Link {
                arena,
                path: model::Path::parse("foobar.txt")?,
                size: 4,
                mtime: foobar_mtime
            },
            notif
        );

        // Dropping rx stops the forwarder.
        drop(rx);
        timeout(Duration::from_secs(1), handle).await???;

        Ok(())
    }

    #[tokio::test]
    async fn forward_history_loses_connection() -> anyhow::Result<()> {
        let _ = env_logger::try_init();

        let verifier = verifier_both();
        let (addr, shutdown, _tempdir) = setup_test_server(Arc::clone(&verifier)).await?;
        let privkey = load_private_key(testing::client_private_key())?;
        let server = Peer::from("server");
        let net = Networking::new(
            vec![(&server, addr.to_string().as_ref())],
            privkey,
            verifier,
        );

        let arena = Arena::from("testdir");
        let (tx, _rx) = mpsc::channel(10);
        let (watched, handle) =
            forward_peer_history(&net, context::current(), &server, vec![arena.clone()], tx)
                .await?;

        assert_eq!(vec![arena.clone()], watched);

        shutdown.send(())?;

        // The server shutting down stops the forwarder
        timeout(Duration::from_secs(1), handle).await???;

        Ok(())
    }

    #[tokio::test]
    async fn forward_history_fails_to_connect() -> anyhow::Result<()> {
        let _ = env_logger::try_init();

        let verifier = verifier_both();
        let privkey = load_private_key(testing::client_private_key())?;
        let server = Peer::from("server");
        let net = Networking::new(vec![(&server, "localhost:0")], privkey, verifier);
        let arena = Arena::from("testdir");
        let (tx, rx) = mpsc::channel(10);
        let ret =
            forward_peer_history(&net, context::current(), &server, vec![arena.clone()], tx).await;

        assert_eq!(
            Some(std::io::ErrorKind::ConnectionRefused),
            io_error_kind(ret.err())
        );

        assert!(rx.is_closed());

        Ok(())
    }

    /// Extract I/O error kind from an anyhow error, if possible.
    fn io_error_kind(err: Option<anyhow::Error>) -> Option<std::io::ErrorKind> {
        Some(err?.downcast_ref::<std::io::Error>()?.kind())
    }
}
