use async_speed_limit::Limiter;
use async_speed_limit::clock::StandardClock;
use futures::prelude::*;
use rustls::pki_types::ServerName;
use rustls::sign::SigningKey;
use tarpc::client::RpcError;
use tarpc::client::stub::Stub;
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
use crate::model::service::Config;
use crate::model::service::RealizeServiceRequest;
use crate::model::service::RealizeServiceResponse;
use crate::model::service::{RealizeService, RealizeServiceClient};
use crate::server::RealizeServer;
use crate::transport::security;
use crate::transport::security::PeerVerifier;
use crate::utils::async_utils::AbortOnDrop;

use crate::server::DirectoryMap;

use super::rate_limit::RateLimitedStream;

pub type TcpRealizeServiceClient = RealizeServiceClient<TcpStub>;

/// Lookup an address as a string.
pub async fn lookup_addr(addr: &str) -> anyhow::Result<SocketAddr> {
    tokio::net::lookup_host(addr)
        .await?
        .next()
        .ok_or(anyhow::anyhow!("DNS lookup failed for {addr}"))
}

/// Start the server, listening on the given address.
pub async fn start_server<T>(
    addr: T,
    dirs: DirectoryMap,
    verifier: Arc<PeerVerifier>,
    privkey: Arc<dyn SigningKey>,
) -> anyhow::Result<(SocketAddr, AbortOnDrop<()>)>
where
    T: tokio::net::ToSocketAddrs,
{
    let acceptor = security::make_tls_acceptor(Arc::clone(&verifier), privkey)?;
    let listener = TcpListener::bind(&addr).await?;
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

#[derive(Clone, Debug, Default)]
pub struct ClientOptions {
    pub domain: Option<String>,
    pub limiter: Option<Limiter<StandardClock>>,
}

/// Create a [RealizeServiceClient] connected to the given TCP address.
pub async fn connect_client(
    server_addr: &SocketAddr,
    verifier: Arc<PeerVerifier>,
    privkey: Arc<dyn SigningKey>,
    options: ClientOptions,
) -> anyhow::Result<TcpRealizeServiceClient> {
    let connector = security::make_tls_connector(verifier, privkey)?;
    let stub = TcpStub::new(server_addr, connector, options).await?;
    Ok(RealizeServiceClient::from(stub))
}

#[derive(Clone)]
struct TcpConnect {
    server_addr: SocketAddr,
    connector: TlsConnector,
    options: ClientOptions,
    config: Arc<Mutex<Option<Config>>>,
}

impl Connect<tarpc::client::Channel<Arc<RealizeServiceRequest>, RealizeServiceResponse>>
    for TcpConnect
{
    async fn run(
        &self,
    ) -> anyhow::Result<tarpc::client::Channel<Arc<RealizeServiceRequest>, RealizeServiceResponse>>
    {
        let stream = TcpStream::connect(&self.server_addr).await?;
        let domain = ServerName::try_from(
            self.options
                .clone()
                .domain
                .unwrap_or_else(|| "localhost".to_string()),
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
        server_addr: &SocketAddr,
        connector: TlsConnector,
        options: ClientOptions,
    ) -> anyhow::Result<Self> {
        let retry_strategy = ExponentialBackoff::from_millis(500)
            .factor(2)
            .max_delay(Duration::from_secs(5 * 60));

        let config = Arc::new(Mutex::new(None));
        let connect = TcpConnect {
            server_addr: server_addr.clone(),
            connector,
            options,
            config: Arc::clone(&config),
        };

        let reconnect: Reconnect<
            tarpc::client::Channel<Arc<RealizeServiceRequest>, RealizeServiceResponse>,
            TcpConnect,
            ExponentialBackoff,
        > = Reconnect::new(retry_strategy, connect, Some(Duration::from_secs(60))).await?;
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
    use crate::model::service::{Config, DirectoryId, Options};
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
            load_private_key(crate::transport::security::testing::server_private_key())?;
        let (addr, server_handle) =
            start_server("localhost:0", dirs, verifier, server_privkey).await?;

        Ok((addr, server_handle, temp))
    }

    // Helper to create a PeerVerifier with only the server key
    fn verifier_server_only() -> Arc<PeerVerifier> {
        let crypto = Arc::new(security::default_provider());
        let mut verifier = PeerVerifier::new(&crypto);
        verifier.add_peer(crate::transport::security::testing::server_public_key());
        Arc::new(verifier)
    }

    // Helper to create a PeerVerifier with only the client key
    fn verifier_client_only() -> Arc<PeerVerifier> {
        let crypto = Arc::new(security::default_provider());
        let mut verifier = PeerVerifier::new(&crypto);
        verifier.add_peer(crate::transport::security::testing::client_public_key());
        Arc::new(verifier)
    }

    // Helper to create a PeerVerifier with both keys
    fn verifier_both() -> Arc<PeerVerifier> {
        let crypto = Arc::new(security::default_provider());
        let mut verifier = PeerVerifier::new(&crypto);
        verifier.add_peer(crate::transport::security::testing::client_public_key());
        verifier.add_peer(crate::transport::security::testing::server_public_key());
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
            load_private_key(crate::transport::security::testing::client_private_key())?;
        let client = connect_client(
            &addr,
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
        assert_eq!(list.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn client_not_in_server_verifier_fails() -> anyhow::Result<()> {
        let verifier = verifier_server_only();
        let (addr, _server_handle, _temp) = setup_test_server(Arc::clone(&verifier)).await.unwrap();
        let client_privkey =
            load_private_key(crate::transport::security::testing::client_private_key())?;
        let client_result = connect_client(
            &addr,
            Arc::clone(&verifier),
            client_privkey,
            ClientOptions::default(),
        )
        .await;
        match client_result {
            Ok(client) => {
                let list_result = client
                    .list(
                        context::current(),
                        DirectoryId::from("testdir"),
                        Options::default(),
                    )
                    .await;
                assert!(
                    list_result.is_err(),
                    "Expected error when client is not in server verifier on first RPC"
                );
            }
            Err(_) => {
                // If handshake fails, that's also acceptable
                assert!(true);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn server_not_in_client_verifier_fails() -> anyhow::Result<()> {
        let verifier = verifier_client_only();
        let (addr, _server_handle, _temp) = setup_test_server(Arc::clone(&verifier)).await.unwrap();
        let client_privkey =
            load_private_key(crate::transport::security::testing::client_private_key())?;
        let result = connect_client(
            &addr,
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
            load_private_key(crate::transport::security::testing::server_private_key())?;
        let (addr, server_handle) = start_server(
            "localhost:0",
            dirs.clone(),
            verifier.clone(),
            server_privkey,
        )
        .await?;
        let client_privkey =
            load_private_key(crate::transport::security::testing::client_private_key())?;
        let client = connect_client(
            &addr,
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
        server_handle.abort();
        Ok(())
    }

    #[tokio::test]
    async fn configure_tcp_per_connection_limit() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let dirs = DirectoryMap::for_dir(&DirectoryId::from("testdir"), temp.path());
        let verifier = verifier_both();
        let server_privkey =
            load_private_key(crate::transport::security::testing::server_private_key())?;
        let (addr, server_handle) = start_server(
            "localhost:0",
            dirs.clone(),
            verifier.clone(),
            server_privkey,
        )
        .await?;
        let client_privkey1 =
            load_private_key(crate::transport::security::testing::client_private_key())?;
        let client_privkey2 =
            load_private_key(crate::transport::security::testing::client_private_key())?;
        let client1 = connect_client(
            &addr,
            Arc::clone(&verifier),
            client_privkey1,
            ClientOptions::default(),
        )
        .await?;
        let client2 = connect_client(
            &addr,
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
        server_handle.abort();
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
            load_private_key(crate::transport::security::testing::client_private_key())?;
        let client = connect_client(
            &proxy_addr,
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
            load_private_key(crate::transport::security::testing::client_private_key())?;
        let client = connect_client(
            &proxy_addr,
            verifier_both(),
            client_privkey,
            ClientOptions::default(),
        )
        .await?;

        let mut config = Config::default();
        config.write_limit = Some(1024);
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
        let server_addr = server_addr.clone();

        let handle = AbortOnDrop::new(tokio::spawn(async move {
            while let Ok((mut inbound, _)) = listener.accept().await {
                if let Ok(mut outbound) = TcpStream::connect(server_addr.clone()).await {
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
