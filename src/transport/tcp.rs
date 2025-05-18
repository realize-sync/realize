use async_speed_limit::clock::Clock;
use async_speed_limit::clock::StandardClock;
use async_speed_limit::limiter::Consume;
use async_speed_limit::Limiter;
use futures::prelude::*;
use rustls::pki_types::ServerName;
use rustls::sign::SigningKey;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use tarpc::serde_transport as transport;
use tarpc::server::{BaseChannel, Channel};
use tarpc::tokio_serde::formats::Bincode;
use tarpc::tokio_util::codec::length_delimited::LengthDelimitedCodec;

use crate::client::WithDeadline;
use crate::metrics;
use crate::metrics::MetricsRealizeClient;
use crate::metrics::MetricsRealizeServer;
use crate::model::service::RealizeServiceRequest;
use crate::model::service::RealizeServiceResponse;
use crate::model::service::{RealizeService, RealizeServiceClient};
use crate::server::RealizeServer;
use crate::transport::security;
use crate::transport::security::PeerVerifier;
use crate::utils::async_utils::AbortOnDrop;

use crate::server::DirectoryMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};

pub type TcpRealizeServiceClient = RealizeServiceClient<
    WithDeadline<
        MetricsRealizeClient<tarpc::client::Channel<RealizeServiceRequest, RealizeServiceResponse>>,
    >,
>;

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
    let server = RunningServer::bind(addr, dirs, verifier, privkey).await?;
    let addr = server.local_addr()?;
    let handle = AbortOnDrop::new(server.spawn());

    Ok((addr, handle))
}

#[derive(Clone, Debug, Default)]
pub struct ClientOptions {
    pub domain: Option<String>,
    pub limiter: Option<Limiter<StandardClock>>,
}

/// Create a [RealizeServiceClient] connected to the given TCP address.
pub async fn connect_client<T>(
    server_addr: T,
    verifier: Arc<PeerVerifier>,
    privkey: Arc<dyn SigningKey>,
    options: ClientOptions,
) -> anyhow::Result<TcpRealizeServiceClient>
where
    T: tokio::net::ToSocketAddrs,
{
    let connector = security::make_tls_connector(verifier, privkey)?;

    let stream = TcpStream::connect(server_addr).await?;
    let domain = ServerName::try_from(options.domain.unwrap_or_else(|| "localhost".to_string()))?;
    let limiter = options
        .limiter
        .unwrap_or_else(|| Limiter::<StandardClock>::new(f64::INFINITY));
    let stream = connector
        .connect(domain, RateLimitedStream::new(stream, limiter.clone()))
        .await?;

    let codec_builder = LengthDelimitedCodec::builder();
    let transport = transport::new(codec_builder.new_framed(stream), Bincode::default());

    let client = tarpc::client::new(Default::default(), transport).spawn();
    let client = RealizeServiceClient::from(WithDeadline::new(
        MetricsRealizeClient::new(client),
        Duration::from_secs(60),
    ));

    Ok(client)
}

/// A TCP server bound to an address.
struct RunningServer {
    acceptor: TlsAcceptor,
    listener: TcpListener,
    dirs: DirectoryMap,
    verifier: Arc<PeerVerifier>,
}

impl RunningServer {
    /// Bind to the address, but don't process client requests yet.
    async fn bind<T>(
        addr: T,
        dirs: DirectoryMap,
        verifier: Arc<PeerVerifier>,
        privkey: Arc<dyn SigningKey>,
    ) -> anyhow::Result<Self>
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
        Ok(Self {
            acceptor,
            listener,
            dirs,
            verifier,
        })
    }

    /// Return the address the server is listening to.
    ///
    /// This is useful when the port given to [start_server] is 0, which means that the OS should choose.
    fn local_addr(&self) -> anyhow::Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    /// Run the server; listen to client connections.
    fn spawn(self) -> JoinHandle<()> {
        let accept = async move |stream: TcpStream| -> anyhow::Result<()> {
            let peer_addr = stream.peer_addr()?;
            let limiter = Limiter::<StandardClock>::new(f64::INFINITY);
            let tls_stream = self
                .acceptor
                .accept(RateLimitedStream::new(stream, limiter.clone()))
                .await?;

            log::info!(
                "Accepted peer {} from {}",
                self.verifier
                    .connection_peer_id(&tls_stream)
                    .expect("Peer MUST be known"), // Guards against bugs in the auth code; worth dying
                peer_addr
            );
            let framed = LengthDelimitedCodec::builder().new_framed(tls_stream);
            let server = RealizeServer::new_limited(self.dirs.clone(), limiter.clone());
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

        tokio::spawn(async move {
            loop {
                if let Ok((stream, peer)) = self.listener.accept().await {
                    match accept(stream).await {
                        Ok(_) => {}
                        Err(err) => log::debug!("{}: connection rejected: {}", peer, err),
                    };
                }
            }
        })
    }
}

/// Apply the given bps limit on writes to this stream.
pub struct RateLimitedStream<S, C = StandardClock>
where
    C: Clock,
{
    inner: S,
    limiter: Limiter<C>,
    waiter: Option<Consume<C, ()>>,
}

impl<S, C> RateLimitedStream<S, C>
where
    C: Clock,
{
    pub fn new(inner: S, limiter: Limiter<C>) -> Self {
        Self {
            inner,
            limiter,
            waiter: None,
        }
    }
}

impl<S, C> AsyncRead for RateLimitedStream<S, C>
where
    S: AsyncRead + Unpin,
    C: Clock,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S, C> AsyncWrite for RateLimitedStream<S, C>
where
    S: AsyncWrite + Unpin,
    C: Clock,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if let Some(waiter) = &mut self.waiter {
            let res = Pin::new(waiter).poll(cx);
            if res.is_pending() {
                return Poll::Pending;
            }
            self.waiter = None;
        }

        let res = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(len)) = &res {
            if *len > 0 {
                self.waiter = Some(self.limiter.consume(*len));
            }
        }

        res
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    // Note: poll_write_vectored disabled even if the underlying
    // stream supports it; writes go through poll_write
}

impl<S: Unpin, C: Clock> Unpin for RateLimitedStream<S, C> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::service::{Config, DirectoryId, Options};
    use crate::utils::async_utils::AbortOnDrop;
    use assert_fs::TempDir;
    use async_speed_limit::clock::{ManualClock, Nanoseconds};
    use rustls::pki_types::PrivateKeyDer;
    use tarpc::context;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Helper to setup a test server and return (server_addr, server_handle, crypto, temp_dir)
    async fn setup_test_server(
        verifier: Arc<PeerVerifier>,
    ) -> anyhow::Result<(SocketAddr, AbortOnDrop<()>, TempDir)> {
        let temp = TempDir::new()?;
        let dirs = DirectoryMap::for_dir(&DirectoryId::from("testdir"), temp.path());
        let server_privkey =
            load_private_key(crate::transport::security::testing::server_private_key())?;
        let server = RunningServer::bind("localhost:0", dirs, verifier, server_privkey).await?;
        let addr = server.local_addr()?;
        let server_handle = AbortOnDrop::new(server.spawn());
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
    async fn test_tarpc_rpc_tcp_tls() -> anyhow::Result<()> {
        let verifier = verifier_both();
        let (addr, _server_handle, _temp) = setup_test_server(Arc::clone(&verifier)).await?;
        let client_privkey =
            load_private_key(crate::transport::security::testing::client_private_key())?;
        let client = connect_client(
            addr,
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
    async fn test_client_not_in_server_verifier_fails() -> anyhow::Result<()> {
        let verifier = verifier_server_only();
        let (addr, _server_handle, _temp) = setup_test_server(Arc::clone(&verifier)).await.unwrap();
        let client_privkey =
            load_private_key(crate::transport::security::testing::client_private_key())?;
        let client_result = connect_client(
            addr,
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
    async fn test_server_not_in_client_verifier_fails() -> anyhow::Result<()> {
        let verifier = verifier_client_only();
        let (addr, _server_handle, _temp) = setup_test_server(Arc::clone(&verifier)).await.unwrap();
        let client_privkey =
            load_private_key(crate::transport::security::testing::client_private_key())?;
        let result = connect_client(
            addr,
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
    async fn test_rate_limited_stream() -> anyhow::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let mut a = TcpStream::connect(listener.local_addr()?).await?;
        let (b, _) = listener.accept().await?;
        let limiter = Limiter::<ManualClock>::new(3.0); // 3 bps

        let limiter_for_reader = limiter.clone();
        let reader = tokio::spawn(async move {
            let mut buf = vec![0; 6];

            let c = a.read(&mut buf).await?;
            assert_eq!(c, 3);
            assert_eq!(buf, b"123\0\0\0");

            // Advance time to let through more bytes.
            tokio::task::yield_now().await;
            limiter_for_reader
                .clock()
                .set_time(Nanoseconds(1_000_000_000));
            tokio::task::yield_now().await;

            let c = a.read(&mut buf).await?;
            assert_eq!(c, 3);
            assert_eq!(buf, b"456\0\0\0");

            // Advance time to let through more bytes.
            tokio::task::yield_now().await;
            limiter_for_reader
                .clock()
                .set_time(Nanoseconds(2_000_000_000));
            tokio::task::yield_now().await;

            let c = a.read(&mut buf).await?;
            assert_eq!(c, 3);
            assert_eq!(buf, b"789\0\0\0");

            Ok::<(), anyhow::Error>(())
        });

        let mut limited_b = RateLimitedStream::new(b, limiter.clone());
        let writer = tokio::spawn(async move {
            limited_b.write_all(b"123").await?;
            // First write goes through immediately.

            limited_b.write_all(b"456").await?;
            // Second write had to wait for the clock to advance.
            assert_eq!(limited_b.limiter.clock().now(), Nanoseconds(1_000_000_000));

            limited_b.write_all(b"789").await?;
            // Third write had to wait for the clock to advance a 2nd time.
            assert_eq!(limited_b.limiter.clock().now(), Nanoseconds(2_000_000_000));

            limited_b.shutdown().await?;

            Ok::<(), anyhow::Error>(())
        });

        reader.await??;
        writer.await??;

        assert_eq!(limiter.total_bytes_consumed(), 9);
        assert_eq!(limiter.clock().now(), Nanoseconds(2_000_000_000));

        Ok(())
    }

    #[tokio::test]
    async fn test_configure_tcp_returns_limit() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let dirs = DirectoryMap::for_dir(&DirectoryId::from("testdir"), temp.path());
        let verifier = verifier_both();
        let server_privkey =
            load_private_key(crate::transport::security::testing::server_private_key())?;
        let server = RunningServer::bind(
            "localhost:0",
            dirs.clone(),
            verifier.clone(),
            server_privkey,
        )
        .await?;
        let addr = server.local_addr()?;
        let server_handle = AbortOnDrop::new(server.spawn());
        let client_privkey =
            load_private_key(crate::transport::security::testing::client_private_key())?;
        let client = connect_client(
            addr,
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
    async fn test_configure_tcp_per_connection_limit() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let dirs = DirectoryMap::for_dir(&DirectoryId::from("testdir"), temp.path());
        let verifier = verifier_both();
        let server_privkey =
            load_private_key(crate::transport::security::testing::server_private_key())?;
        let server = RunningServer::bind(
            "localhost:0",
            dirs.clone(),
            verifier.clone(),
            server_privkey,
        )
        .await?;
        let addr = server.local_addr()?;
        let server_handle = AbortOnDrop::new(server.spawn());
        let client_privkey1 =
            load_private_key(crate::transport::security::testing::client_private_key())?;
        let client_privkey2 =
            load_private_key(crate::transport::security::testing::client_private_key())?;
        let client1 = connect_client(
            addr,
            Arc::clone(&verifier),
            client_privkey1,
            ClientOptions::default(),
        )
        .await?;
        let client2 = connect_client(
            addr,
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
}
