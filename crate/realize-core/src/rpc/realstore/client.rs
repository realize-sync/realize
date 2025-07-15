use crate::rpc::realstore;
use crate::rpc::realstore::metrics::MetricsRealizeClient;
use crate::rpc::realstore::{
    Config, RealStoreServiceClient, RealStoreServiceRequest, RealStoreServiceResponse,
};
use async_speed_limit::Limiter;
use async_speed_limit::clock::StandardClock;
use realize_network::Networking;
use realize_network::reconnect::{Connect, Reconnect};
use realize_types::Peer;
use std::sync::Arc;
use std::time::Duration;
use tarpc::client::RpcError;
use tarpc::client::stub::Stub;
use tarpc::context;
use tokio::sync::Mutex;
use tokio_retry::strategy::ExponentialBackoff;

pub type RealStoreClient = RealStoreServiceClient<RealizeStub>;

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

/// Create a [RealStoreServiceClient] connected to the given TCP address.
pub async fn connect(
    networking: &Networking,
    peer: Peer,
    options: ClientOptions,
) -> anyhow::Result<RealStoreClient> {
    let stub = RealizeStub::new(networking, peer, options).await?;
    Ok(RealStoreServiceClient::from(stub))
}

#[derive(Clone)]
struct TcpConnect {
    networking: Networking,
    peer: Peer,
    options: ClientOptions,
    config: Arc<Mutex<Option<Config>>>,
}

impl Connect<tarpc::client::Channel<Arc<RealStoreServiceRequest>, RealStoreServiceResponse>>
    for TcpConnect
{
    async fn try_connect(
        &self,
    ) -> anyhow::Result<
        tarpc::client::Channel<Arc<RealStoreServiceRequest>, RealStoreServiceResponse>,
    > {
        if let Some(tx) = &self.options.connection_events {
            let _ = tx.send(ClientConnectionState::Connecting);
        }
        let transport = self
            .networking
            .connect(self.peer, realstore::TAG, self.options.limiter.clone())
            .await?;
        let channel = tarpc::client::new(Default::default(), transport).spawn();

        // Recover stored configuration
        if let Some(config) = self.config.lock().await.as_ref() {
            let req = Arc::new(RealStoreServiceRequest::Configure {
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
/// Most usage should be through the alias [TcpRealStoreServiceClient].
pub struct RealizeStub {
    inner: MetricsRealizeClient<
        Reconnect<
            tarpc::client::Channel<Arc<RealStoreServiceRequest>, RealStoreServiceResponse>,
            TcpConnect,
            ExponentialBackoff,
        >,
    >,
    config: Arc<Mutex<Option<Config>>>,
}

impl RealizeStub {
    async fn new(
        networking: &Networking,
        peer: Peer,
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
            peer,
            options,
            config: Arc::clone(&config),
        };

        let reconnect: Reconnect<
            tarpc::client::Channel<Arc<RealStoreServiceRequest>, RealStoreServiceResponse>,
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

impl Stub for RealizeStub {
    type Req = RealStoreServiceRequest;
    type Resp = RealStoreServiceResponse;

    async fn call(
        &self,
        ctx: context::Context,
        request: RealStoreServiceRequest,
    ) -> std::result::Result<RealStoreServiceResponse, RpcError> {
        let res = self.inner.call(ctx, request).await;

        // Store configuration to recover it on reconnection.
        if let Ok(RealStoreServiceResponse::Configure(Ok(config))) = &res {
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
    use super::*;
    use crate::rpc::realstore::Config;
    use crate::utils::async_utils::AbortOnDrop;
    use assert_fs::TempDir;
    use realize_network::hostport::HostPort;
    use realize_network::security::{PeerVerifier, RawPublicKeyResolver};
    use realize_network::{Server, testing};
    use realize_storage::{RealStore, RealStoreOptions};
    use realize_types::{Arena, Peer};
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tarpc::context;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::{TcpListener, TcpStream};

    struct Fixture {
        storage: RealStore,
        resolver: Arc<RawPublicKeyResolver>,
        verifier: Arc<PeerVerifier>,
        server: Option<Arc<Server>>,
        _tempdir: TempDir,
    }

    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();

            let tempdir = TempDir::new()?;
            let storage = RealStore::single(Arena::from("testdir"), tempdir.path());
            let resolver = RawPublicKeyResolver::from_private_key(testing::server_private_key())?;
            let mut verifier = PeerVerifier::new();
            verifier.add_peer(Peer::from("client"), testing::client_public_key());
            verifier.add_peer(Peer::from("server"), testing::server_public_key());
            let verifier = Arc::new(verifier);

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
            realstore::server::register(&mut server, self.storage.clone());
            let server = Arc::new(server);
            let addr = Arc::clone(&server).listen(&HostPort::localhost(0)).await?;

            self.server = Some(server);

            Ok(addr)
        }

        fn client_networking(&self, peer: Peer, addr: SocketAddr) -> anyhow::Result<Networking> {
            let addr_str = addr.to_string();
            Ok(Networking::new(
                vec![(peer, addr_str.leak() as &'static str)],
                RawPublicKeyResolver::from_private_key(testing::client_private_key())?,
                Arc::clone(&self.verifier),
            ))
        }
    }

    #[tokio::test]
    async fn simpleconnect_client() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let addr = fixture.start_server().await?;

        let options = ClientOptions::default();
        let peer = Peer::from("server");
        let networking = fixture.client_networking(peer, addr)?;
        let client = connect(&networking, peer, options).await?;
        let list = client
            .list(
                context::current(),
                Arena::from("testdir"),
                RealStoreOptions::default(),
            )
            .await??;
        assert!(list.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn configure_tcp_returns_limit() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let addr = fixture.start_server().await?;

        let options = ClientOptions::default();
        let peer = Peer::from("server");
        let networking = fixture.client_networking(peer, addr)?;
        let client = connect(&networking, peer, options).await?;
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
        let networking = fixture.client_networking(peer, addr)?;
        let client1 = connect(&networking, peer, ClientOptions::default()).await?;
        let client2 = connect(&networking, peer, ClientOptions::default()).await?;
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
    async fn reconnect() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let addr = fixture.start_server().await?;

        let connection_count = Arc::new(AtomicU32::new(0));
        let (shutdown, _) = tokio::sync::broadcast::channel(1);
        let (proxy_addr, _proxy_handle) =
            proxy_tcp(&addr, shutdown.clone(), connection_count.clone()).await?;

        let peer = Peer::from("server");
        let networking = fixture.client_networking(peer, proxy_addr)?;
        let client = connect(&networking, peer, ClientOptions::default()).await?;

        client
            .list(
                context::current(),
                Arena::from("testdir"),
                RealStoreOptions::default(),
            )
            .await??;
        shutdown.send(())?;
        client
            .list(
                context::current(),
                Arena::from("testdir"),
                RealStoreOptions::default(),
            )
            .await??;
        assert_eq!(connection_count.load(Ordering::Relaxed), 2);

        Ok(())
    }

    #[tokio::test]
    async fn reconnect_keeps_config() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let addr = fixture.start_server().await?;

        let connection_count = Arc::new(AtomicU32::new(0));
        let (shutdown, _) = tokio::sync::broadcast::channel(1);
        let (proxy_addr, _proxy_handle) =
            proxy_tcp(&addr, shutdown.clone(), connection_count.clone()).await?;

        let peer = Peer::from("server");
        let networking = fixture.client_networking(peer, proxy_addr)?;
        let client = connect(&networking, peer, ClientOptions::default()).await?;

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
    async fn reconnect_events() -> anyhow::Result<()> {
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
        let networking = fixture.client_networking(peer, proxy_addr)?;
        let options = ClientOptions {
            connection_events: Some(conn_tx),
            ..ClientOptions::default()
        };
        let client = connect(&networking, peer, options).await?;

        client
            .list(
                context::current(),
                Arena::from("testdir"),
                RealStoreOptions::default(),
            )
            .await??;
        shutdown.send(())?;
        client
            .list(
                context::current(),
                Arena::from("testdir"),
                RealStoreOptions::default(),
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
}
