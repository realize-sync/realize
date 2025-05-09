use futures::prelude::*;
use rustls::pki_types::ServerName;
use rustls::sign::SigningKey;
use tokio::task::JoinHandle;

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

/// Listen to the given TCP address for connections for
/// [RealizeService].
pub async fn spawn_server<T>(
    server: RealizeServer,
    addr: T,
    verifier: Arc<PeerVerifier>,
    privkey: Arc<dyn SigningKey>,
) -> anyhow::Result<JoinHandle<()>>
where
    T: tokio::net::ToSocketAddrs + std::fmt::Debug,
{
    let acceptor = security::make_tls_acceptor(verifier, privkey)?;
    let listener = TcpListener::bind(&addr).await?;
    let codec_builder = LengthDelimitedCodec::builder();

    log::info!("Listening for RPC connections on {:?}", addr);
    let accept = async move |stream: TcpStream| -> anyhow::Result<()> {
        let tls_stream = acceptor.accept(stream).await?;
        let framed = codec_builder.new_framed(tls_stream);

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
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;
    use crate::model::service::DirectoryId;
    use crate::server::Directory;
    use assert_fs::TempDir;
    use tarpc::context;

    #[tokio::test]
    async fn test_tarpc_rpc_tcp_tls() -> anyhow::Result<()> {
        // do this in main
        env_logger::init();
        let crypto = Arc::new(rustls::crypto::aws_lc_rs::default_provider());

        let addr = (IpAddr::V4(Ipv4Addr::LOCALHOST), 32119);
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
        spawn_server(server_impl, addr, Arc::clone(&verifier), server_privkey).await?;

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
