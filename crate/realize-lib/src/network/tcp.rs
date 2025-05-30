//! TCP transport for Realize - Symmetric File Syncer
//!
//! This module provides secure, rate-limited, and reconnecting TCP transport for the RealizeService RPC interface.
//! It handles TLS setup, peer authentication, DNS resolution, and connection retry logic.

use anyhow::Context as _;
use async_speed_limit::clock::StandardClock;
use async_speed_limit::Limiter;
use futures::prelude::*;
use rustls::pki_types::ServerName;
use rustls::sign::SigningKey;
use tarpc::client::stub::Stub;
use tarpc::client::RpcError;
use tarpc::context;
use tokio::sync::Mutex;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_rustls::TlsConnector;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use tarpc::serde_transport as transport;
use tarpc::server::{BaseChannel, Channel};
use tarpc::tokio_serde::formats::Bincode;
use tarpc::tokio_util::codec::length_delimited::LengthDelimitedCodec;

use crate::client::reconnect::Connect;
use crate::client::reconnect::Reconnect;
use crate::metrics;
use crate::metrics::MetricsRealizeClient;
use crate::metrics::MetricsRealizeServer;
use crate::network::rate_limit::RateLimitedStream;
use crate::network::services::realize::Config;
use crate::network::services::realize::RealizeServiceRequest;
use crate::network::services::realize::RealizeServiceResponse;
use crate::network::services::realize::{RealizeService, RealizeServiceClient};
use crate::server::RealizeServer;
use crate::network::security;
use crate::network::security::PeerVerifier;
use crate::utils::async_utils::AbortOnDrop;

use crate::server::DirectoryMap;

use std::fmt;
use std::net::IpAddr;

pub type TcpRealizeServiceClient = RealizeServiceClient<TcpStub>;

/// HostPort represents a resolved host:port pair, with DNS resolution.
#[derive(Clone, PartialEq, Eq, Debug, Hash)]
pub struct HostPort {
    host: String,
    port: u16,
    addr: SocketAddr,
}

impl HostPort {
    /// Parse a host:port string, resolve DNS, and return a HostPort.
    pub async fn parse(s: &str) -> anyhow::Result<Self> {
        // Handle [::1]:port for IPv6
        let (host, port) = if let Some(idx) = s.rfind(':') {
            let (host, port_str) = s.split_at(idx);
            let port = port_str[1..]
                .parse::<u16>()
                .map_err(|_| anyhow::anyhow!("Invalid port in address: {s}"))?;
            let host = if host.starts_with('[') && host.ends_with(']') {
                &host[1..host.len() - 1]
            } else {
                host
            };
            (host.to_string(), port)
        } else {
            return Err(anyhow::anyhow!("Missing port in address: {s}"));
        };
        // DNS resolution
        let mut addrs = tokio::net::lookup_host(s)
            .await
            .with_context(|| format!("DNS lookup failed for {s}"))?;
        let addr = addrs
            .next()
            .ok_or_else(|| anyhow::anyhow!("DNS lookup failed for {s}"))?;
        Ok(HostPort { host, port, addr })
    }
    pub fn host(&self) -> &str {
        &self.host
    }
    pub fn port(&self) -> u16 {
        self.port
    }
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl fmt::Display for HostPort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl From<SocketAddr> for HostPort {
    fn from(addr: SocketAddr) -> Self {
        let host = match addr.ip() {
            IpAddr::V4(ip) => ip.to_string(),
            IpAddr::V6(ip) => ip.to_string(),
        };
        let port = addr.port();
        HostPort { host, port, addr }
    }
}

/// Start the server, listening on the given address.
pub async fn start_server(
    hostport: &HostPort,
    dirs: DirectoryMap,
    verifier: Arc<PeerVerifier>,
    privkey: Arc<dyn SigningKey>,
) -> anyhow::Result<(SocketAddr, AbortOnDrop<()>)> {
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

    let accept = async move |stream: TcpStream| -> anyhow::Result<()> {
        let peer_addr = stream.peer_addr()?;
        let limiter = Limiter::<StandardClock>::new(f64::INFINITY);
        let tls_stream = acceptor
            .accept(RateLimitedStream::new(stream, limiter.clone()))
            .await?;

        log::info!(
            "Accepted peer {} from {}",
            verifier
                .connection_peer_id(&tls_stream)
                .expect("Peer MUST be known"), // Guards against bugs in the auth code; worth dying
            peer_addr
        );
        let framed = LengthDelimitedCodec::builder().new_framed(tls_stream);
        let server = RealizeServer::new_limited(dirs.clone(), limiter.clone());
        tokio::spawn(
            BaseChannel::with_defaults(transport::new(framed, Bincode::default()))
                .execute(MetricsRealizeServer::new(RealizeServer::serve(
                    server.clone(),
                )))
                .for_each(async move |fut| {
                    tokio::spawn(metrics::track_in_flight_request(fut));
                }),
        );

        Ok(())
    };

    let handle = AbortOnDrop::new(tokio::spawn(async move {
        loop {
            if let Ok((stream, peer)) = listener.accept().await {
                match accept(stream).await {
                    Ok(_) => {}
                    Err(err) => log::debug!("{}: connection rejected: {}", peer, err),
                };
            }
        }
    }));

    Ok((addr, handle))
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
        let stream = self
            .connector
            .connect(domain, RateLimitedStream::new(stream, limiter.clone()))
            .await?;

        let codec_builder = LengthDelimitedCodec::builder();
        let transport = transport::new(codec_builder.new_framed(stream), Bincode::default());
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
    use crate::network::services::realize::{Config, DirectoryId, Options};
    use crate::utils::async_utils::AbortOnDrop;
    use assert_fs::TempDir;
    use rustls::pki_types::PrivateKeyDer;
    use tarpc::context;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    // Helper to setup a test server and return (server_addr, server_handle, crypto, temp_dir)
    async fn setup_test_server(
        verifier: Arc<PeerVerifier>,
    ) -> anyhow::Result<(SocketAddr, AbortOnDrop<()>, TempDir)> {
        let temp = TempDir::new()?;
        let dirs = DirectoryMap::for_dir(&DirectoryId::from("testdir"), temp.path());
        let server_privkey =
            load_private_key(crate::network::security::testing::server_private_key())?;
        let (addr, server_handle) = start_server(
            &HostPort::parse("127.0.0.1:0").await?,
            dirs,
            verifier,
            server_privkey,
        )
        .await?;

        Ok((addr, server_handle, temp))
    }

    // Helper to create a PeerVerifier with only the server key
    fn verifier_server_only() -> Arc<PeerVerifier> {
        let crypto = Arc::new(security::default_provider());
        let mut verifier = PeerVerifier::new(&crypto);
        verifier.add_peer(crate::network::security::testing::server_public_key());
        Arc::new(verifier)
    }

    // Helper to create a PeerVerifier with only the client key
    fn verifier_client_only() -> Arc<PeerVerifier> {
        let crypto = Arc::new(security::default_provider());
        let mut verifier = PeerVerifier::new(&crypto);
        verifier.add_peer(crate::network::security::testing::client_public_key());
        Arc::new(verifier)
    }

    // Helper to create a PeerVerifier with both keys
    fn verifier_both() -> Arc<PeerVerifier> {
        let crypto = Arc::new(security::default_provider());
        let mut verifier = PeerVerifier::new(&crypto);
        verifier.add_peer(crate::network::security::testing::client_public_key());
        verifier.add_peer(crate::network::security::testing::server_public_key());
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
        let (addr, _server_handle, _temp) = setup_test_server(Arc::clone(&verifier)).await?;
        let client_privkey =
            load_private_key(crate::network::security::testing::client_private_key())?;
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
                DirectoryId::from("testdir"),
                Options::default(),
            )
            .await??;
        assert!(list.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn client_not_in_server_verifier_fails() -> anyhow::Result<()> {
        let verifier = verifier_server_only();
        let (addr, _server_handle, _temp) = setup_test_server(Arc::clone(&verifier)).await.unwrap();
        let client_privkey =
            load_private_key(crate::network::security::testing::client_private_key())?;
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
                    DirectoryId::from("testdir"),
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
        let (addr, _server_handle, _temp) = setup_test_server(Arc::clone(&verifier)).await.unwrap();
        let client_privkey =
            load_private_key(crate::network::security::testing::client_private_key())?;
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
        let dirs = DirectoryMap::for_dir(&DirectoryId::from("testdir"), temp.path());
        let verifier = verifier_both();
        let server_privkey =
            load_private_key(crate::network::security::testing::server_private_key())?;
        let (addr, _handle) = start_server(
            &HostPort::parse("127.0.0.1:0").await?,
            dirs.clone(),
            verifier.clone(),
            server_privkey,
        )
        .await?;
        let client_privkey =
            load_private_key(crate::network::security::testing::client_private_key())?;
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
        let dirs = DirectoryMap::for_dir(&DirectoryId::from("testdir"), temp.path());
        let verifier = verifier_both();
        let server_privkey =
            load_private_key(crate::network::security::testing::server_private_key())?;
        let (addr, _handle) = start_server(
            &HostPort::parse("127.0.0.1:0").await?,
            dirs.clone(),
            verifier.clone(),
            server_privkey,
        )
        .await?;
        let client_privkey1 =
            load_private_key(crate::network::security::testing::client_private_key())?;
        let client_privkey2 =
            load_private_key(crate::network::security::testing::client_private_key())?;
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
        let (addr, _server_handle, _temp) = setup_test_server(Arc::clone(&verifier)).await?;

        let connection_count = Arc::new(AtomicU32::new(0));
        let (shutdown, _) = tokio::sync::broadcast::channel(1);
        let (proxy_addr, _proxy_handle) =
            proxy_tcp(&addr, shutdown.clone(), connection_count.clone()).await?;

        let client_privkey =
            load_private_key(crate::network::security::testing::client_private_key())?;
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
                DirectoryId::from("testdir"),
                Options::default(),
            )
            .await??;
        shutdown.send(())?;
        client
            .list(
                context::current(),
                DirectoryId::from("testdir"),
                Options::default(),
            )
            .await??;
        assert_eq!(connection_count.load(Ordering::Relaxed), 2);

        Ok(())
    }

    #[tokio::test]
    async fn tcp_reconnect_keeps_config() -> anyhow::Result<()> {
        let verifier = verifier_both();
        let (addr, _server_handle, _temp) = setup_test_server(Arc::clone(&verifier)).await?;

        let connection_count = Arc::new(AtomicU32::new(0));
        let (shutdown, _) = tokio::sync::broadcast::channel(1);
        let (proxy_addr, _proxy_handle) =
            proxy_tcp(&addr, shutdown.clone(), connection_count.clone()).await?;

        let client_privkey =
            load_private_key(crate::network::security::testing::client_private_key())?;
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
        let (addr, _server_handle, _temp) = setup_test_server(Arc::clone(&verifier)).await?;

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
            load_private_key(crate::network::security::testing::client_private_key())?,
            ClientOptions {
                connection_events: Some(conn_tx),
                ..ClientOptions::default()
            },
        )
        .await?;

        client
            .list(
                context::current(),
                DirectoryId::from("testdir"),
                Options::default(),
            )
            .await??;
        shutdown.send(())?;
        client
            .list(
                context::current(),
                DirectoryId::from("testdir"),
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
    async fn test_parse_ipv4() {
        let hp = HostPort::parse("127.0.0.1:8000").await.unwrap();
        assert_eq!(hp.host(), "127.0.0.1");
        assert_eq!(hp.port(), 8000);
        assert_eq!(hp.addr().port(), 8000);
    }

    #[tokio::test]
    async fn test_parse_ipv6() {
        let hp = HostPort::parse("[::1]:8000").await.unwrap();
        assert_eq!(hp.host(), "::1");
        assert_eq!(hp.port(), 8000);
        assert_eq!(hp.addr().port(), 8000);
    }

    #[tokio::test]
    async fn test_parse_hostname() {
        let hp = HostPort::parse("localhost:1234").await.unwrap();
        assert_eq!(hp.host(), "localhost");
        assert_eq!(hp.port(), 1234);
        assert_eq!(hp.addr().port(), 1234);

        let hp = HostPort::parse("www.google.com:1234").await.unwrap();
        assert_eq!(hp.host(), "www.google.com");
        assert_eq!(hp.port(), 1234);
        assert_eq!(hp.addr().port(), 1234);
    }

    #[tokio::test]
    async fn test_parse_invalid() {
        assert!(HostPort::parse("myhost").await.is_err());
        assert!(HostPort::parse("doesnotexist:1000").await.is_err());
    }

    #[tokio::test]
    async fn test_from_socketaddr() {
        let hp1 = HostPort::parse("127.0.0.1:8000").await.unwrap();
        let hp2 = HostPort::from(hp1.addr());
        assert_eq!(hp1, hp2);
    }

    #[tokio::test]
    async fn test_display() {
        let hp = HostPort::parse("127.0.0.1:8000").await.unwrap();
        assert_eq!(format!("{}", hp), "127.0.0.1:8000");
    }
}
