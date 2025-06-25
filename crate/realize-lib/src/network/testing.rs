use rustls::pki_types::{pem::PemObject as _, PrivateKeyDer, SubjectPublicKeyInfoDer};
use std::net::SocketAddr;
use std::sync::Arc;

use super::security::{PeerVerifier, RawPublicKeyResolver};
use super::Networking;
use crate::model::Peer;

pub fn client_peer() -> Peer {
    Peer::from("client")
}

/// Standard server peer for testing.
pub fn server_peer() -> Peer {
    Peer::from("server")
}

/// Verifier that knows public and client peers.
pub fn client_server_verifier() -> Arc<PeerVerifier> {
    let mut verifier = PeerVerifier::new();
    verifier.add_peer(&client_peer(), client_public_key());
    verifier.add_peer(&server_peer(), server_public_key());

    Arc::new(verifier)
}

/// Resolver for server peer.
pub fn server_resolver() -> anyhow::Result<Arc<RawPublicKeyResolver>> {
    let resolver = RawPublicKeyResolver::from_private_key(server_private_key())?;

    Ok(resolver)
}

/// Resolver for client peer.
pub fn client_resolver() -> anyhow::Result<Arc<RawPublicKeyResolver>> {
    let resolver = RawPublicKeyResolver::from_private_key(client_private_key())?;

    Ok(resolver)
}

/// Public key for test clients.
///
/// Generated from [client_private_key] with:
///   openssl pkey -in peer.key -pubout -out -
pub(crate) fn client_public_key() -> SubjectPublicKeyInfoDer<'static> {
    SubjectPublicKeyInfoDer::from_pem_slice(
        br#"
-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEA/CMSGfePPViYEUoHMNTrywE+mwTmB0poO0A1ATNIJGo=
-----END PUBLIC KEY-----
"#,
    )
    .expect("Invalid test client public key")
}

/// Public key for test servers.
///
/// Generated from [server_private_key] with:
///   openssl pkey -in peer.key -pubout -out -
pub(crate) fn server_public_key() -> SubjectPublicKeyInfoDer<'static> {
    SubjectPublicKeyInfoDer::from_pem_slice(
        br#"
-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAIckr1J3xrgglc6pseuCDWDAupSMzA1TJyitkgJi/SPg=
-----END PUBLIC KEY-----
"#,
    )
    .expect("Invalid test server public key")
}

/// Private key for test servers.
///
/// Generated with:
///  openssl genpkey -algorithm ed25519 -out -
pub(crate) fn server_private_key() -> PrivateKeyDer<'static> {
    PrivateKeyDer::from_pem_slice(
        br#"
-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIBde8kxon54UvlyvkcwIUf7mFR4SkBJGsstNAn2HzK1Y
-----END PRIVATE KEY-----
"#,
    )
    .expect("Invalid server private key")
}

/// Private key for test clients.
///
/// Generated with:
///  openssl genpkey -algorithm ed25519 -out -
pub(crate) fn client_private_key() -> PrivateKeyDer<'static> {
    PrivateKeyDer::from_pem_slice(
        br#"
-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIPaGEL0B7EAMQb5anN+DTH0vZ/qI90AQpbwYuklDABpV
-----END PRIVATE KEY-----
"#,
    )
    .expect("Invalid client private key")
}

/// Some other private key.
///
/// Generated with:
///  openssl genpkey -algorithm ed25519 -out -
pub(crate) fn other_private_key() -> PrivateKeyDer<'static> {
    PrivateKeyDer::from_pem_slice(
        br#"
-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIKQaAyXiyQU0mslObJQDBav1po/d5m4OmBvJR1l5iLC5
-----END PRIVATE KEY-----
"#,
    )
    .expect("Invalid client private key")
}

/// Extract I/O error kind from an anyhow error, if possible.
pub fn io_error_kind(err: Option<anyhow::Error>) -> Option<std::io::ErrorKind> {
    Some(err?.downcast_ref::<std::io::Error>()?.kind())
}

pub fn server_networking() -> anyhow::Result<Networking> {
    Ok(Networking::new(
        vec![],
        server_resolver()?,
        client_server_verifier(),
    ))
}

pub fn client_networking(addr: SocketAddr) -> anyhow::Result<Networking> {
    Ok(Networking::new(
        vec![(&server_peer(), addr.to_string().as_ref())],
        client_resolver()?,
        client_server_verifier(),
    ))
}
