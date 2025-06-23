use super::peer_capnp::connected_peer;
use capnp::capability::Promise;
use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::twoparty::VatNetwork;
use capnp_rpc::{pry, RpcSystem};
use futures::io::{BufReader, BufWriter};
use futures::AsyncReadExt;
use std::net::SocketAddr;
use tokio::task::LocalSet;
use tokio_util::compat::TokioAsyncReadCompatExt;

pub struct ConnectedPeer;

impl ConnectedPeer {
    pub async fn connect(addr: SocketAddr) -> Result<connected_peer::Client, anyhow::Error> {
        let stream = tokio::net::TcpStream::connect(&addr).await?;
        stream.set_nodelay(true)?;
        let (r, w) = TokioAsyncReadCompatExt::compat(stream).split();
        let net = Box::new(VatNetwork::new(
            BufReader::new(r),
            BufWriter::new(w),
            Side::Client,
            Default::default(),
        ));
        let mut system = RpcSystem::new(net, None);
        let client: connected_peer::Client = system.bootstrap(Side::Server);

        tokio::task::spawn_local(system);

        Ok(client)
    }

    pub async fn listen(self, addr: SocketAddr) -> Result<SocketAddr, anyhow::Error> {
        let client: connected_peer::Client = capnp_rpc::new_client(self);
        let listener = tokio::net::TcpListener::bind(&addr).await?;
        let addr = listener.local_addr()?;
        tokio::task::spawn_local(async move {
            loop {
                match listener.accept().await {
                    Err(err) => log::debug!("Connection failed: {err}"),
                    Ok((stream, _)) => {
                        if stream.set_nodelay(true).is_ok() {
                            let (r, w) = TokioAsyncReadCompatExt::compat(stream).split();
                            let net = Box::new(VatNetwork::new(
                                BufReader::new(r),
                                BufWriter::new(w),
                                Side::Server,
                                Default::default(),
                            ));
                            let system = RpcSystem::new(net, Some(client.clone().client));
                            tokio::task::spawn_local(system);
                        }
                    }
                }
            }
        });

        Ok(addr)
    }
}

impl connected_peer::Server for ConnectedPeer {
    fn ping(
        &mut self,
        params: connected_peer::PingParams,
        mut results: connected_peer::PingResults,
    ) -> Promise<(), ::capnp::Error> {
        let text = pry!(pry!(pry!(params.get()).get_text()).to_str());
        let message = format!("{text} PING");
        results.get().set_response(message);

        Promise::ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::network::hostport::HostPort;

    use super::*;

    #[tokio::test]
    async fn ping() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        LocalSet::new()
            .run_until(async move {
                let addr = ConnectedPeer.listen(HostPort::localhost(0).addr()).await?;
                let client = ConnectedPeer::connect(addr).await?;
                let mut request = client.ping_request();
                request.get().set_text("foo");

                let reply = request.send().promise.await?;
                assert_eq!("foo PING", reply.get()?.get_response()?.to_str()?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }
}
