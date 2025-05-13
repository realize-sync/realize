#[cfg(test)]
pub mod testing;

use base64::Engine as _;
use rustls::client::danger::ServerCertVerifier;
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::sign::{CertifiedKey, SigningKey};
use rustls::version::TLS13;
use rustls::{ClientConfig, Error, ServerConfig};
use sha2::Digest as _;
use std::collections::HashSet;

use std::sync::Arc;
use tokio_rustls::rustls::{self, server::danger::ClientCertVerifier};
use tokio_rustls::{TlsAcceptor, TlsConnector};

pub use rustls::crypto::aws_lc_rs::default_provider;

/// Create a TlsAcceptor (server-side) for the given peers and private key.
pub fn make_tls_acceptor(
    verifier: Arc<PeerVerifier>,
    privkey: Arc<dyn SigningKey>,
) -> Result<TlsAcceptor, anyhow::Error> {
    let config = ServerConfig::builder_with_protocol_versions(&[&TLS13])
        .with_client_cert_verifier(verifier)
        .with_cert_resolver(RawPublicKeyResolver::create(privkey)?);
    let acceptor = TlsAcceptor::from(Arc::new(config));
    Ok(acceptor)
}

/// Create af TlsConnector (client-side) for the given peer and private key.
pub fn make_tls_connector(
    verifier: Arc<PeerVerifier>,
    privkey: Arc<dyn SigningKey>,
) -> Result<TlsConnector, anyhow::Error> {
    let config = ClientConfig::builder_with_protocol_versions(&[&TLS13])
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_cert_resolver(RawPublicKeyResolver::create(privkey)?);
    let connector = TlsConnector::from(Arc::new(config));
    Ok(connector)
}

/// Check client and server certificates.
///
/// Client and server connections are only accepted for known peers.
/// Add public keys to the verifier before using it.
#[derive(Debug)]
pub struct PeerVerifier {
    allowed_peers: HashSet<String>,
    algos: WebPkiSupportedAlgorithms,
}

impl PeerVerifier {
    /// Create a new, empty verifier.
    pub fn new(crypto: &Arc<CryptoProvider>) -> Self {
        Self {
            allowed_peers: HashSet::new(),
            algos: crypto.signature_verification_algorithms,
        }
    }
}

impl PeerVerifier {
    /// Accept connections to the peer with the given public key.
    ///
    /// The SPKI must be the public part of a ED25519 key.
    pub fn add_peer(&mut self, spki: &rustls::pki_types::SubjectPublicKeyInfoDer) {
        self.allowed_peers.insert(hash_spki(spki));
    }

    fn accept_peer(&self, spki: &rustls::pki_types::SubjectPublicKeyInfoDer) -> bool {
        self.allowed_peers.contains(&hash_spki(spki))
    }

    fn verify_peer(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
    ) -> Result<(), rustls::Error> {
        let end_entity_as_spki =
            rustls::pki_types::SubjectPublicKeyInfoDer::from(end_entity.as_ref());
        if !self.accept_peer(&end_entity_as_spki) {
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ));
        }
        // TODO: store the peer id whose key this is and use it for logging.

        Ok(())
    }

    fn verify_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature_with_raw_key(
            message,
            &rustls::pki_types::SubjectPublicKeyInfoDer::from(cert.as_ref()),
            dss,
            &self.algos,
        )
    }

    // Only support Ed25519.
    //
    // More are available in self.algos, but we want to limit the set
    // of schemes for simplicity.
    fn verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![rustls::SignatureScheme::ED25519]
    }
}

impl ClientCertVerifier for PeerVerifier {
    fn client_auth_mandatory(&self) -> bool {
        true
    }

    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::server::danger::ClientCertVerified, rustls::Error> {
        self.verify_peer(end_entity)?;

        Ok(rustls::server::danger::ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::PeerIncompatible(
            rustls::PeerIncompatible::Tls12NotOffered,
        ))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.verify_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.verify_schemes()
    }

    fn offer_client_auth(&self) -> bool {
        true
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}

impl ServerCertVerifier for PeerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        _: &[rustls::pki_types::CertificateDer<'_>],
        _: &rustls::pki_types::ServerName<'_>,
        _: &[u8],
        _: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        self.verify_peer(end_entity)?;

        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::PeerIncompatible(
            rustls::PeerIncompatible::Tls12NotOffered,
        ))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.verify_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.verify_schemes()
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}

fn hash_spki(spki: &rustls::pki_types::SubjectPublicKeyInfoDer) -> String {
    base64::prelude::BASE64_STANDARD.encode(sha2::Sha256::digest(spki))
}

#[derive(Debug)]
struct RawPublicKeyResolver {
    certified_key: Arc<CertifiedKey>,
}

impl RawPublicKeyResolver {
    fn create(privkey: Arc<dyn rustls::sign::SigningKey>) -> anyhow::Result<Arc<Self>> {
        let pubkey = privkey
            .public_key()
            .ok_or(Error::InconsistentKeys(rustls::InconsistentKeys::Unknown))?;
        let pubkey_as_cert = rustls::pki_types::CertificateDer::from(pubkey.to_vec());
        let certified_key = Arc::new(CertifiedKey::new(vec![pubkey_as_cert], privkey));

        Ok(RawPublicKeyResolver::create_internal(certified_key))
    }

    fn create_internal(certified_key: Arc<CertifiedKey>) -> Arc<Self> {
        Arc::new(Self { certified_key })
    }
}

impl rustls::client::ResolvesClientCert for RawPublicKeyResolver {
    fn resolve(
        &self,
        _: &[&[u8]],
        _: &[rustls::SignatureScheme],
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        Some(Arc::clone(&self.certified_key))
    }

    fn only_raw_public_keys(&self) -> bool {
        true
    }

    fn has_certs(&self) -> bool {
        true
    }
}

impl rustls::server::ResolvesServerCert for RawPublicKeyResolver {
    fn resolve(&self, _: rustls::server::ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(Arc::clone(&self.certified_key))
    }

    fn only_raw_public_keys(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::default_provider;
    use super::*;
    use crate::transport::security::testing;
    use crate::utils::async_utils::AbortOnDrop;
    use rustls::pki_types::PrivateKeyDer;
    use rustls::pki_types::pem::PemObject as _;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn test_tls_acceptor_connector() -> anyhow::Result<()> {
        test_connect(complete_verifier()).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_tls_unknown_client_peer() -> anyhow::Result<()> {
        let crypto = Arc::new(default_provider());
        let mut verifier = PeerVerifier::new(&crypto);
        // client public key missing from verifier
        verifier.add_peer(&testing::server_public_key());

        assert!(test_connect(verifier).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_tls_unknown_server_peer() -> anyhow::Result<()> {
        let crypto = Arc::new(default_provider());
        let mut verifier = PeerVerifier::new(&crypto);
        verifier.add_peer(&testing::client_public_key());
        // server public key missing from verifier

        assert!(test_connect(verifier).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_tls_reject_client_with_bad_private_key() -> anyhow::Result<()> {
        let crypto = Arc::new(default_provider());
        let mut verifier = PeerVerifier::new(&crypto);
        verifier.add_peer(&testing::client_public_key());
        verifier.add_peer(&testing::server_public_key());

        let verifier = Arc::new(verifier);

        let server_priv = load_signing_key(testing::server_private_key())?;
        let acceptor = make_tls_acceptor(Arc::clone(&verifier), server_priv)?;

        // Misconfigured connector; public and private keys don't match.  (Uses
        // private functions in super)
        let other_priv = load_signing_key(testing::other_private_key())?;
        let bad_connector = {
            let pubkey = testing::client_public_key();
            let pubkey_as_cert = rustls::pki_types::CertificateDer::from(pubkey.to_vec());
            let certified_key = Arc::new(CertifiedKey::new(vec![pubkey_as_cert], other_priv));

            let config = ClientConfig::builder_with_protocol_versions(&[&TLS13])
                .dangerous()
                .with_custom_certificate_verifier(verifier.clone())
                .with_client_cert_resolver(RawPublicKeyResolver::create_internal(certified_key));

            TlsConnector::from(Arc::new(config))
        };

        assert!(test_connect_1(acceptor, bad_connector).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_tls_reject_client_without_cert() -> anyhow::Result<()> {
        let crypto = Arc::new(default_provider());
        let mut verifier = PeerVerifier::new(&crypto);
        verifier.add_peer(&testing::client_public_key());
        verifier.add_peer(&testing::server_public_key());

        let verifier = Arc::new(verifier);

        let server_priv = load_signing_key(testing::server_private_key())?;
        let acceptor = make_tls_acceptor(Arc::clone(&verifier), server_priv)?;

        // Misconfigured connector; no client auth.
        let bad_connector = {
            let config = ClientConfig::builder_with_protocol_versions(&[&TLS13])
                .dangerous()
                .with_custom_certificate_verifier(verifier.clone())
                .with_no_client_auth();

            TlsConnector::from(Arc::new(config))
        };

        assert!(test_connect_1(acceptor, bad_connector).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_tls_reject_server_with_bad_private_key() -> anyhow::Result<()> {
        let crypto = Arc::new(default_provider());
        let mut verifier = PeerVerifier::new(&crypto);
        verifier.add_peer(&testing::client_public_key());
        verifier.add_peer(&testing::server_public_key());

        let verifier = Arc::new(verifier);

        // Misconfigured acceptor; public and private keys don't
        // match. (Uses private functions in super)
        let other_priv = load_signing_key(testing::other_private_key())?;
        let bad_acceptor = {
            let pubkey = testing::client_public_key();
            let pubkey_as_cert = rustls::pki_types::CertificateDer::from(pubkey.to_vec());
            let certified_key = Arc::new(CertifiedKey::new(vec![pubkey_as_cert], other_priv));

            let config = ServerConfig::builder_with_protocol_versions(&[&TLS13])
                .with_client_cert_verifier(verifier.clone())
                .with_cert_resolver(RawPublicKeyResolver::create_internal(certified_key));

            TlsAcceptor::from(Arc::new(config))
        };

        let client_priv = load_signing_key(testing::client_private_key())?;
        let connector = make_tls_connector(Arc::clone(&verifier), client_priv)?;

        assert!(test_connect_1(bad_acceptor, connector).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_tls_bad_private_key_algo() -> anyhow::Result<()> {
        let verifier = Arc::new(complete_verifier());

        let server_priv = load_signing_key(testing::server_private_key())?;
        let acceptor = make_tls_acceptor(verifier.clone(), server_priv)?;

        // Wrong key algorithm
        let ecdsa_client_priv = load_signing_key(ecdsa_private_key())?;
        let connector = make_tls_connector(verifier.clone(), ecdsa_client_priv)?;

        assert!(test_connect_1(acceptor, connector).await.is_err());

        Ok(())
    }

    async fn test_connect(verifier: PeerVerifier) -> anyhow::Result<()> {
        let verifier = Arc::new(verifier);

        let server_priv = load_signing_key(testing::server_private_key())?;
        let acceptor = make_tls_acceptor(Arc::clone(&verifier), server_priv)?;

        let client_priv = load_signing_key(testing::client_private_key())?;
        let connector = make_tls_connector(Arc::clone(&verifier), client_priv)?;

        test_connect_1(acceptor, connector).await
    }

    async fn test_connect_1(acceptor: TlsAcceptor, connector: TlsConnector) -> anyhow::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server: AbortOnDrop<anyhow::Result<()>> = AbortOnDrop::new(tokio::spawn(async move {
            let (tcp, _) = listener.accept().await?;

            let mut tls = acceptor.accept(tcp).await?;
            tls.write_all(b"foobar").await?;
            tls.shutdown().await?;

            Ok(())
        }));

        let tcp = TcpStream::connect(addr).await?;
        let domain = rustls::pki_types::ServerName::try_from("localhost")?;
        let mut tls = connector.connect(domain, tcp).await?;
        let mut buf = vec![0u8; 6];
        tls.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"foobar");

        server.as_handle().await??;
        Ok(())
    }

    fn load_signing_key(
        key: rustls::pki_types::PrivateKeyDer<'static>,
    ) -> Result<Arc<dyn SigningKey>, rustls::Error> {
        super::default_provider().key_provider.load_private_key(key)
    }

    fn complete_verifier() -> PeerVerifier {
        let crypto = Arc::new(default_provider());
        let mut verifier = PeerVerifier::new(&crypto);
        verifier.add_peer(&testing::client_public_key());
        verifier.add_peer(&testing::server_public_key());

        verifier
    }

    // Helper to load an ECDSA private key (wrong algorithm)
    //
    // Generated with:
    //  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:secp521r1 -out -
    fn ecdsa_private_key() -> PrivateKeyDer<'static> {
        PrivateKeyDer::from_pem_slice(
            br#"
-----BEGIN PRIVATE KEY-----
MIHuAgEAMBAGByqGSM49AgEGBSuBBAAjBIHWMIHTAgEBBEIBdmUEGjRjKtFVKy4x
8W8K2LqM1sYMOmR/j9F5hVer8c1HJpMKayEvIKPOXdz4zkCYzOIObeYQYViKwYVM
mS4Q8IShgYkDgYYABAFRqTnrP2pV1WUT3Lh2equo3FHynH7NHLal4POdPMjOBoJY
18l5B8EFR8GDaVCZOInmjAljmojgHGzE4mtaJxlIdQABnDgmYcZVCeF3z6L9Xqxi
nuvOs8dx4/JaO4c3f+8m4U2FW+j1o5jei5GbZIgVaOWZICbVJj3vtW0JTzgipnhm
KQ==
-----END PRIVATE KEY-----
"#,
        )
        .expect("Invalid ECDSA private key")
    }
}
