use rustls::pki_types::{PrivateKeyDer, SubjectPublicKeyInfoDer, pem::PemObject as _};

/// Public key for test clients.
///
/// Generated from [client_private_key] with:
///   openssl pkey -in peer.key -pubout -out -
pub fn client_public_key() -> SubjectPublicKeyInfoDer<'static> {
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
pub fn server_public_key() -> SubjectPublicKeyInfoDer<'static> {
    SubjectPublicKeyInfoDer::from_pem_slice(
        br#"
-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAIckr1J3xrgglc6pseuCDWDAupSMzA1TJyitkgJi/SPg=
-----END PUBLIC KEY-----
"#,
    )
    .expect("Invalid test server public key")
}

/// Some other public key.
///
/// Generated from [other_private_key] with:
///   openssl pkey -in peer.key -pubout -out -
pub fn other_public_key() -> SubjectPublicKeyInfoDer<'static> {
    SubjectPublicKeyInfoDer::from_pem_slice(
        br#"
-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAJ7KIbhdPS2ESzYMeQXoqHJv8Vdmi+pJlkFChY8K+IVg=
-----END PUBLIC KEY-----
"#,
    )
    .expect("Invalid test server public key")
}

/// Private key for test servers.
///
/// Generated with:
///  openssl genpkey -algorithm ed25519 -out -
pub fn server_private_key() -> PrivateKeyDer<'static> {
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
pub fn client_private_key() -> PrivateKeyDer<'static> {
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
pub fn other_private_key() -> PrivateKeyDer<'static> {
    PrivateKeyDer::from_pem_slice(
        br#"
-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIKQaAyXiyQU0mslObJQDBav1po/d5m4OmBvJR1l5iLC5
-----END PRIVATE KEY-----
"#,
    )
    .expect("Invalid client private key")
}
