//! TCP transport for Realize - Symmetric File Syncer
//!
//! This module provides secure, rate-limited, and reconnecting TCP transport for the RealizeService RPC interface.
//! It handles TLS setup, peer authentication, DNS resolution, and connection retry logic.

use async_speed_limit::clock::StandardClock;
use async_speed_limit::Limiter;
use futures::prelude::*;
use rustls::pki_types::ServerName;
use rustls::sign::SigningKey;
use tarpc::client::stub::Stub;
use tarpc::client::RpcError;
use tarpc::context;
use tarpc::context::Context;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_rustls::TlsConnector;

use std::net::SocketAddr;
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

use super::hostport::HostPort;

pub type TcpRealizeServiceClient = RealizeServiceClient<TcpStub>;

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
    let (shutdown_tx, mut shutdown_listener_rx) = tokio::sync::broadcast::channel(1);
    let acceptor = security::make_tls_acceptor(Arc::clone(&verifier), privkey)?;
    let listener = TcpListener::bind(hostport.addr()).await?;
    log::info!(
        "Listening for RPC connections on {:?}",
        listener
            .local_addr()
            .map(|addr| addr.to_string())
            .unwrap_or_else(|err| err.to_string())
    );

    let addr = listener.local_addr()?;

    let shutdown_accept_tx = shutdown_tx.downgrade();
    let accept = async move |stream: TcpStream| -> anyhow::Result<()> {
        let mut shutdown_rx = shutdown_accept_tx
            .upgrade()
            .map(|s| s.subscribe())
            .ok_or(anyhow::anyhow!("Shutdown initiated"))?;

        let peer_addr = stream.peer_addr()?;
        let limiter = Limiter::<StandardClock>::new(f64::INFINITY);
        let mut tls_stream = acceptor
            .accept(RateLimitedStream::new(stream, limiter.clone()))
            .await?;
        // Guards against bugs in the auth code; worth dying
        let peer = verifier
            .connection_peer_id(&tls_stream)
            .expect("Peer must be known at this point")
            .clone();
        log::info!("Accepted peer {} from {}", peer, peer_addr);

        let mut tag = [0u8; 4];
        tls_stream.read_exact(&mut tag).await?;
        match &tag {
            rpc::realize::TAG => {
                let framed = LengthDelimitedCodec::builder().new_framed(tls_stream);
                let server = RealizeServer::new_limited(storage.clone(), limiter.clone());
                tokio::spawn(async move {
                    let mut stream = Box::pin(
                        BaseChannel::with_defaults(transport::new(framed, Bincode::default()))
                            .execute(MetricsRealizeServer::new(RealizeServer::serve(
                                server.clone(),
                            ))),
                    );
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
            }
            rpc::history::TAG => {
                let framed = LengthDelimitedCodec::builder().new_framed(tls_stream);
                let transport = transport::new(framed, Bincode::default());
                let channel = tarpc::client::new(Default::default(), transport).spawn();
                let client = HistoryServiceClient::from(channel);

                let peer = peer.clone();
                let storage = storage.clone();
                tokio::spawn(async move {
                    if let Err(err) = history::client::collect(client, storage, shutdown_rx).await {
                        log::debug!("{}: history collection failed: {}", peer, err);
                    }
                });
            }
            _ => {
                log::info!("Got unsupported RPC service tag '{tag:?}' from {peer}");
                // We log this separately from generic errors, because
                // bad tags might highlight peer versioning issues.

                let _ = tls_stream.shutdown().await;
                return Err(anyhow::anyhow!("Unknown RPC service tag"));
            }
        }

        Ok(())
    };

    tokio::spawn(async move {
        loop {
            tokio::select!(
                _ = shutdown_listener_rx.recv() => {
                    log::debug!("Shutting down listener");
                    return;
                }
                Ok((stream, peer)) = listener.accept() => {
                    match accept(stream).await {
                        Ok(_) => {}
                        Err(err) => log::debug!("{}: connection rejected: {}", peer, err),
                    };
                }
            );
        }
    });

    Ok((addr, shutdown_tx))
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
pub async fn forward_peer_history(
    ctx: Context,
    peer: Peer,
    addr: HostPort,
    verifier: Arc<PeerVerifier>,
    privkey: Arc<dyn SigningKey>,
    arenas: Vec<Arena>,
    tx: mpsc::Sender<(Peer, Notification)>,
) -> anyhow::Result<(Vec<Arena>, JoinHandle<anyhow::Result<()>>)> {
    let connector = security::make_tls_connector(verifier, privkey)?;
    let stream = TcpStream::connect(addr.addr()).await?;
    let mut tls_stream = connector
        .connect(ServerName::try_from(addr.host().to_string())?, stream)
        .await?;
    tls_stream.write_all(history::TAG).await?;

    let transport = transport::new(
        LengthDelimitedCodec::builder().new_framed(tls_stream),
        Bincode::default(),
    );

    let (ready_tx, ready_rx) = oneshot::channel();
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
    pub domain: Option<String>,
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
    hostport: &HostPort,
    verifier: Arc<PeerVerifier>,
    privkey: Arc<dyn SigningKey>,
    options: ClientOptions,
) -> anyhow::Result<TcpRealizeServiceClient> {
    let connector = security::make_tls_connector(verifier, privkey)?;
    let stub = TcpStub::new(hostport, connector, options).await?;
    Ok(RealizeServiceClient::from(stub))
}

#[derive(Clone)]
struct TcpConnect {
    hostport: HostPort,
    connector: TlsConnector,
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
        let stream = TcpStream::connect(self.hostport.addr()).await?;
        let domain = ServerName::try_from(
            self.options
                .clone()
                .domain
                .unwrap_or_else(|| self.hostport.host().to_string()),
        )?;
        let limiter = self
            .options
            .limiter
            .clone()
            .unwrap_or_else(|| Limiter::<StandardClock>::new(f64::INFINITY));
        let mut tls_stream = self
            .connector
            .connect(domain, RateLimitedStream::new(stream, limiter.clone()))
            .await?;
        tls_stream.write_all(realize::TAG).await?;

        let codec_builder = LengthDelimitedCodec::builder();
        let transport = transport::new(codec_builder.new_framed(tls_stream), Bincode::default());
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
        hostport: &HostPort,
        connector: TlsConnector,
        options: ClientOptions,
    ) -> anyhow::Result<Self> {
        let retry_strategy =
            ExponentialBackoff::from_millis(500).max_delay(Duration::from_secs(5 * 60));

        let config = Arc::new(Mutex::new(None));

        if let Some(tx) = &options.connection_events {
            let _ = tx.send(ClientConnectionState::NotConnected);
        }
        let connect = TcpConnect {
            hostport: hostport.clone(),
            connector,
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
        let client = connect_client(
            &HostPort::from(addr),
            verifier_both(),
            client_privkey,
            ClientOptions::default(),
        )
        .await?;
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
        let client_result = connect_client(
            &HostPort::from(addr),
            Arc::clone(&verifier),
            client_privkey,
            ClientOptions::default(),
        )
        .await;
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
        let result = connect_client(
            &HostPort::from(addr),
            Arc::clone(&verifier),
            client_privkey,
            ClientOptions::default(),
        )
        .await;
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
        let client = connect_client(
            &HostPort::from(addr),
            verifier.clone(),
            client_privkey,
            ClientOptions::default(),
        )
        .await?;
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
        let client1 = connect_client(
            &HostPort::from(addr),
            Arc::clone(&verifier),
            client_privkey1,
            ClientOptions::default(),
        )
        .await?;
        let client2 = connect_client(
            &HostPort::from(addr),
            Arc::clone(&verifier),
            client_privkey2,
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
        let client = connect_client(
            &HostPort::from(proxy_addr),
            verifier_both(),
            client_privkey,
            ClientOptions::default(),
        )
        .await?;

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
        let client = connect_client(
            &HostPort::from(proxy_addr),
            verifier_both(),
            client_privkey,
            ClientOptions::default(),
        )
        .await?;

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

        let client = connect_client(
            &HostPort::from(proxy_addr),
            verifier_both(),
            load_private_key(testing::client_private_key())?,
            ClientOptions {
                connection_events: Some(conn_tx),
                ..ClientOptions::default()
            },
        )
        .await?;

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

        // Before shutdown, connection succeeds.
        let client_privkey = load_private_key(testing::client_private_key())?;
        connect_client(
            &HostPort::from(addr),
            Arc::clone(&verifier_both()),
            Arc::clone(&client_privkey),
            ClientOptions::default(),
        )
        .await?;

        shutdown.send(())?;
        shutdown.closed().await;

        // After shutdown, connection fails
        let ret = connect_client(
            &HostPort::from(addr),
            verifier_both(),
            client_privkey,
            ClientOptions::default(),
        )
        .await;

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

        let arena = Arena::from("testdir");
        let (tx, mut rx) = mpsc::channel(10);
        let (watched, handle) = forward_peer_history(
            context::current(),
            Peer::from("server"),
            addr.into(),
            verifier,
            privkey,
            vec![arena.clone()],
            tx,
        )
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

        let arena = Arena::from("testdir");
        let (tx, _rx) = mpsc::channel(10);
        let (watched, handle) = forward_peer_history(
            context::current(),
            Peer::from("server"),
            addr.into(),
            verifier,
            privkey,
            vec![arena.clone()],
            tx,
        )
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
        let arena = Arena::from("testdir");
        let (tx, rx) = mpsc::channel(10);
        let ret = forward_peer_history(
            context::current(),
            Peer::from("server"),
            HostPort::parse("localhost:0").await?,
            verifier,
            privkey,
            vec![arena.clone()],
            tx,
        )
        .await;

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
