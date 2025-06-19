use std::net::SocketAddr;

use super::{security, Networking};

/// Extract I/O error kind from an anyhow error, if possible.
pub fn io_error_kind(err: Option<anyhow::Error>) -> Option<std::io::ErrorKind> {
    Some(err?.downcast_ref::<std::io::Error>()?.kind())
}

pub fn server_networking() -> anyhow::Result<Networking> {
    Ok(Networking::new(
        vec![],
        security::testing::server_resolver()?,
        security::testing::client_server_verifier(),
    ))
}

pub fn client_networking(addr: SocketAddr) -> anyhow::Result<Networking> {
    Ok(Networking::new(
        vec![(&security::testing::server_peer(), addr.to_string().as_ref())],
        security::testing::client_resolver()?,
        security::testing::client_server_verifier(),
    ))
}
