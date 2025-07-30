use realize_network::unixsocket;

use super::control_capnp;

/// Connect to a running daemon through the given socket path.
///
/// Panics if run outside of a [LocalSet].
pub async fn connect(
    socket_path: &std::path::Path,
) -> anyhow::Result<control_capnp::control::Client> {
    unixsocket::connect::<control_capnp::control::Client>(socket_path).await
}

/// Get a Churten client from a Control client
pub async fn get_churten(
    c: &control_capnp::control::Client,
) -> Result<control_capnp::churten::Client, capnp::Error> {
    c.churten_request()
        .send()
        .promise
        .await?
        .get()?
        .get_churten()
}
