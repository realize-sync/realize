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

use crate::model::service::{RealizeService, RealizeServiceClient};
use crate::server::RealizeServer;
use crate::transport::security;
use crate::transport::security::PeerVerifier;

/// Start the server, listening on the given address.
pub async fn start_server<T>(
    addr: T,
    server: RealizeServer,
    verifier: Arc<PeerVerifier>,
    privkey: Arc<dyn SigningKey>,
) -> anyhow::Result<JoinHandle<()>>
where
    T: tokio::net::ToSocketAddrs,
{
    Ok(RunningServer::bind(addr, server, verifier, privkey)
        .await?
        .spawn())
}

/// A TCP server bound to an address.
pub struct RunningServer {
    acceptor: TlsAcceptor,
    listener: TcpListener,
    server: RealizeServer,
}

impl RunningServer {
    /// Bind to the address, but don't process client requests yet.
    pub async fn bind<T>(
        addr: T,
        server: RealizeServer,
        verifier: Arc<PeerVerifier>,
        privkey: Arc<dyn SigningKey>,
    ) -> anyhow::Result<Self>
    where
        T: tokio::net::ToSocketAddrs,
    {
        let acceptor = security::make_tls_acceptor(verifier, privkey)?;
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
        })
    }

    /// Return the address the server is listening to.
    ///
    /// This is useful when the port given to [start_server] is 0, which means that the OS should choose.
    pub fn local_addr(&self) -> anyhow::Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    /// Run the server; listen to client connections.
    pub fn spawn(self) -> JoinHandle<()> {
        let accept = async move |stream: TcpStream| -> anyhow::Result<()> {
            let tls_stream = self.acceptor.accept(stream).await?;
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
                        Err(err) => log::error!("{}: connection failed: {}", peer, err),
                    };
                }
            }
        })
    }
}

/// Listen to the given TCP address for connections for
/// [RealizeService].
pub async fn spawn_server<T>(
    addr: T,
    server: RealizeServer,
    verifier: Arc<PeerVerifier>,
    privkey: Arc<dyn SigningKey>,
) -> anyhow::Result<JoinHandle<()>>
where
    T: tokio::net::ToSocketAddrs + std::fmt::Debug,
{
    let acceptor = security::make_tls_acceptor(verifier, privkey)?;
    let listener = TcpListener::bind(&addr).await?;

    log::info!("Listening for RPC connections on {:?}", addr);
    let accept = async move |stream: TcpStream| -> anyhow::Result<()> {
        let tls_stream = acceptor.accept(stream).await?;
        let framed = LengthDelimitedCodec::builder().new_framed(tls_stream);

        tokio::spawn(
            BaseChannel::with_defaults(transport::new(framed, Bincode::default()))
                .execute(RealizeServer::serve(server.clone()))
                .for_each(async move |fut| {
                    tokio::spawn(fut);
                }),
        );

        Ok(())
    };

    Ok(tokio::spawn(async move {
        loop {
            if let Ok((stream, peer)) = listener.accept().await {
                match accept(stream).await {
                    Ok(_) => {}
                    Err(err) => log::error!("{}: connection failed: {}", peer, err),
                };
            }
        }
    }))
}

/// Create a [RealizeServiceClient] connected to the given TCP address.
pub async fn connect_client<T>(
    server_name: &str,
    server_addr: T,
    verifier: Arc<PeerVerifier>,
    privkey: Arc<dyn SigningKey>,
) -> anyhow::Result<RealizeServiceClient>
where
    T: tokio::net::ToSocketAddrs,
{
    let connector = security::make_tls_connector(verifier, privkey)?;

    let stream = TcpStream::connect(server_addr).await?;
    let domain = ServerName::try_from(server_name.to_string())?;
    let stream = connector.connect(domain, stream).await?;

    let codec_builder = LengthDelimitedCodec::builder();
    let transport = transport::new(codec_builder.new_framed(stream), Bincode::default());

    Ok(RealizeServiceClient::new(Default::default(), transport).spawn())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::service::DirectoryId;
    use crate::server::Directory;
    use crate::testing::AbortJoinHandleOnDrop;
    use assert_fs::TempDir;
    use tarpc::context;

    #[tokio::test]
    async fn test_tarpc_rpc_tcp_tls() -> anyhow::Result<()> {
        // do this in main
        env_logger::init();
        let crypto = Arc::new(rustls::crypto::aws_lc_rs::default_provider());

        let mut verifier = PeerVerifier::new(&crypto);
        verifier.add_peer(&security::testing::client_public_key());
        verifier.add_peer(&security::testing::server_public_key());
        let verifier = Arc::new(verifier);

        let temp = TempDir::new()?;
        let server_impl = RealizeServer::new(vec![Directory::new(
            &DirectoryId::from("testdir"),
            temp.path(),
        )]);

        let server_privkey = crypto
            .key_provider
            .load_private_key(security::testing::server_private_key())?;
        let server = RunningServer::bind(
            "localhost:0",
            server_impl,
            Arc::clone(&verifier),
            server_privkey,
        )
        .await?;

        // Binding to localhost:0 lets the OS choose the port. We need
        // to know what it is for the client.
        let addr = server.local_addr()?;
        let _server_handle = AbortJoinHandleOnDrop::new(server.spawn());

        let client_privkey = crypto
            .key_provider
            .load_private_key(security::testing::client_private_key())?;

        let client =
            connect_client("localhost", addr, Arc::clone(&verifier), client_privkey).await?;

        let list = client
            .list(context::current(), DirectoryId::from("testdir"))
            .await??;
        assert_eq!(list.len(), 0);

        Ok(())
    }
}
