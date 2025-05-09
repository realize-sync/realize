use rustls::pki_types::{pem::PemObject as _, PrivateKeyDer, SubjectPublicKeyInfoDer};

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
