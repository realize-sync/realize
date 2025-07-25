use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::twoparty;
use capnp_rpc::{RpcSystem, VatNetwork};
use futures::AsyncReadExt;
use futures::io::{BufReader, BufWriter};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use tokio::fs;
use tokio::task::{self, LocalSet};
use tokio_util::compat::TokioAsyncReadCompatExt;
use tokio_util::sync::CancellationToken;

/// Listen to a UNIX domain socket bound to the given path.
///
/// Serves Cap'n Proto clients created by `new_client` on the socket.
///
/// If `path` parent directory doesn't exist, it is created with a
/// UNIX permission of 700 (u=rwx).
pub async fn bind(
    local: &LocalSet,
    path: &Path,
    new_client: impl Fn() -> capnp::capability::Client + 'static,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        if !fs::metadata(parent).await.is_ok() {
            fs::create_dir_all(parent).await?;
            let mut permissions = fs::metadata(parent).await?.permissions();
            permissions.set_mode(0o700);
            fs::set_permissions(parent, permissions).await?;
        }
    }
    let listener = tokio::net::UnixListener::bind(path)?;
    local.spawn_local(async move {
        loop {
            let res = tokio::select!(
            res = listener.accept() => {
                res
            },
            _ = shutdown.cancelled() => {
                return;
            });

            let client = new_client();
            let shutdown = shutdown.clone();
            task::spawn_local(async move {
                let stream = match res {
                    Ok((stream, _)) => stream,
                    Err(err) => {
                        log::debug!("Local socket error: {err}");
                        return;
                    }
                };
                let (r, w) = stream.compat().split();
                let mut net = twoparty::VatNetwork::new(
                    BufReader::new(r),
                    BufWriter::new(w),
                    Side::Server,
                    Default::default(),
                );

                let until_shutdown = net.drive_until_shutdown();
                let system = RpcSystem::new(Box::new(net), Some(client));

                let disconnector = system.get_disconnector();
                task::spawn_local(system);

                // Disconnect if the cancellation token is cancelled.
                tokio::select!(
                    _ = shutdown.cancelled() => {
                        let _ = disconnector.await;
                    },
                    res = until_shutdown => {
                        if let Err(err) = res {
                            log::debug!("Local socket connection failed: {err}")
                        }
                    }
                );
            });
        }
    });

    Ok(())
}

/// Connect to a UNIX domain socket bound to the given path and return
/// the client.
///
/// This must be called from an environment where
/// [tokio::task::spawn_local] is available, such as one setup by
/// [tokio::task::LocalSet].
pub async fn connect<C>(path: &Path) -> anyhow::Result<C>
where
    C: capnp::capability::FromClientHook + Clone + 'static,
{
    let stream = tokio::net::UnixStream::connect(path).await?;
    let (r, w) = stream.compat().split();
    let net = Box::new(twoparty::VatNetwork::new(
        BufReader::new(r),
        BufWriter::new(w),
        Side::Client,
        Default::default(),
    ));
    let mut rpc_system = RpcSystem::new(net, None);
    let client: C = rpc_system.bootstrap(Side::Server);

    task::spawn_local(rpc_system);

    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::hello_capnp::hello;
    use crate::testing::hello_capnp::hello::HelloParams;
    use crate::testing::hello_capnp::hello::HelloResults;
    use assert_fs::TempDir;
    use capnp::capability::Promise;
    use capnp_rpc::pry;
    use tokio::task::LocalSet;

    struct HelloServer;

    impl hello::Server for HelloServer {
        fn hello(
            &mut self,
            params: HelloParams,
            mut results: HelloResults,
        ) -> Promise<(), capnp::Error> {
            let name = pry!(pry!(pry!(params.get()).get_name()).to_str());
            results.get().set_result(format!("Hello, {name}, I'm Bar"));

            Promise::ok(())
        }
    }

    #[tokio::test]
    async fn client_server() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;
        let path = tempdir.path().join("unix_socket");

        let local = LocalSet::new();
        let server_fn = || capnp_rpc::new_client::<hello::Client, _>(HelloServer).client;
        let shutdown = CancellationToken::new();

        bind(&local, &path, server_fn, shutdown.clone()).await?;

        local
            .run_until(async move {
                let client = connect::<hello::Client>(&path).await?;

                let mut request = client.hello_request();
                request.get().set_name("Foo");

                let reply = request.send().promise.await?;
                let greetings = reply.get()?.get_result()?.to_string()?;
                assert_eq!("Hello, Foo, I'm Bar", greetings.as_str());

                shutdown.cancel();
                task::yield_now().await;

                // Server has shut down; new requests will fail.
                let request = client.hello_request();
                assert!(request.send().promise.await.is_err());

                Ok::<_, anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn bind_creates_parent() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;
        let path = tempdir.path().join("parent/unix_socket");

        let local = LocalSet::new();
        let server_fn = || capnp_rpc::new_client::<hello::Client, _>(HelloServer).client;
        let shutdown = CancellationToken::new();

        bind(&local, &path, server_fn, shutdown.clone()).await?;

        let parent = tempdir.path().join("parent");
        assert!(parent.exists());
        assert_eq!(0o700, parent.metadata()?.permissions().mode() & 0o777);

        Ok(())
    }
}
