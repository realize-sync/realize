use futures::prelude::*;
use rustls::pki_types::ServerName;
use rustls::sign::SigningKey;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use tarpc::serde_transport as transport;
use tarpc::server::{BaseChannel, Channel};
use tarpc::tokio_serde::formats::Bincode;
use tarpc::tokio_util::codec::length_delimited::LengthDelimitedCodec;

use crate::client::DeadlineSetter;
use crate::model::service::RealizeServiceRequest;
use crate::model::service::RealizeServiceResponse;
use crate::model::service::{RealizeService, RealizeServiceClient};
use crate::server::RealizeServer;
use crate::transport::security;
use crate::transport::security::PeerVerifier;
use crate::utils::async_utils::AbortOnDrop;

/// Start the server, listening on the given address.
pub async fn start_server<T>(
    addr: T,
    server: RealizeServer,
    verifier: Arc<PeerVerifier>,
    privkey: Arc<dyn SigningKey>,
) -> anyhow::Result<(SocketAddr, AbortOnDrop<()>)>
where
    T: tokio::net::ToSocketAddrs,
{
    let server = RunningServer::bind(addr, server, verifier, privkey).await?;
    let addr = server.local_addr()?;
    let handle = AbortOnDrop::new(server.spawn());

    Ok((addr, handle))
}

/// Create a [RealizeServiceClient] connected to the given TCP address.
pub async fn connect_client<T>(
    server_name: &str,
    server_addr: T,
    verifier: Arc<PeerVerifier>,
    privkey: Arc<dyn SigningKey>,
) -> anyhow::Result<
    RealizeServiceClient<
        DeadlineSetter<tarpc::client::Channel<RealizeServiceRequest, RealizeServiceResponse>>,
    >,
>
where
    T: tokio::net::ToSocketAddrs,
{
    let connector = security::make_tls_connector(verifier, privkey)?;

    let stream = TcpStream::connect(server_addr).await?;
    let domain = ServerName::try_from(server_name.to_string())?;
    let stream = connector.connect(domain, stream).await?;

    let codec_builder = LengthDelimitedCodec::builder();
    let transport = transport::new(codec_builder.new_framed(stream), Bincode::default());

    let client = tarpc::client::new(Default::default(), transport).spawn();
    let client = RealizeServiceClient::from(DeadlineSetter::new(client));

    Ok(client)
}

/// A TCP server bound to an address.
struct RunningServer {
    acceptor: TlsAcceptor,
    listener: TcpListener,
    server: RealizeServer,
    verifier: Arc<PeerVerifier>,
}

impl RunningServer {
    /// Bind to the address, but don't process client requests yet.
    async fn bind<T>(
        addr: T,
        server: RealizeServer,
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
            server,
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
            let tls_stream = self.acceptor.accept(stream).await?;

            log::info!(
                "Accepted peer {} from {}",
                self.verifier
                    .connection_peer_id(&tls_stream)
                    .expect("Peer MUST be known"), // Guards against bugs in the auth code; worth dying
                peer_addr
            );
            let framed = LengthDelimitedCodec::builder().new_framed(tls_stream);

            tokio::spawn(
                BaseChannel::with_defaults(transport::new(framed, Bincode::default()))
                    .execute(RealizeServer::serve(self.server.clone()))
                    .for_each(async move |fut| {
                        tokio::spawn(fut);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::service::{DirectoryId, Options};
    use crate::server::Directory;
    use crate::utils::async_utils::AbortOnDrop;
    use assert_fs::TempDir;
    use rustls::pki_types::PrivateKeyDer;
    use tarpc::context;

    // Helper to setup a test server and return (server_addr, server_handle, crypto, temp_dir)
    async fn setup_test_server(
        verifier: Arc<PeerVerifier>,
    ) -> anyhow::Result<(SocketAddr, AbortOnDrop<()>, TempDir)> {
        let temp = TempDir::new()?;
        let server_impl = RealizeServer::new(vec![Directory::new(
            &DirectoryId::from("testdir"),
            temp.path(),
        )]);
        let server_privkey =
            load_private_key(crate::transport::security::testing::server_private_key())?;
        let server = RunningServer::bind(
            "localhost:0",
            server_impl,
            Arc::clone(&verifier),
            server_privkey,
        )
        .await?;
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
        let client = connect_client("localhost", addr, verifier_both(), client_privkey).await?;
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
        let client_result =
            connect_client("localhost", addr, Arc::clone(&verifier), client_privkey).await;
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
        let result = connect_client("localhost", addr, Arc::clone(&verifier), client_privkey).await;
        assert!(
            result.is_err(),
            "Expected error when server is not in client verifier"
        );

        Ok(())
    }
}
