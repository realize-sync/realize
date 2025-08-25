use super::config::PeerConfig;
use anyhow::Context as _;
use realize_types::Peer;
use rustls::client::Resumption;
use rustls::client::danger::ServerCertVerifier;
use rustls::crypto::WebPkiSupportedAlgorithms;
pub use rustls::crypto::aws_lc_rs::default_provider;
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{PrivateKeyDer, SubjectPublicKeyInfoDer};
use rustls::sign::CertifiedKey;
use rustls::version::TLS13;
use rustls::{ClientConfig, Error, ServerConfig};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use tokio_rustls::rustls::server::danger::ClientCertVerifier;
use tokio_rustls::rustls::{self};
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// Create a TlsAcceptor (server-side) for the given peers and private key.
pub(crate) fn make_tls_acceptor(
    verifier: Arc<PeerVerifier>,
    resolver: Arc<RawPublicKeyResolver>,
) -> TlsAcceptor {
    let config = ServerConfig::builder_with_protocol_versions(&[&TLS13])
        .with_client_cert_verifier(verifier)
        .with_cert_resolver(resolver);

    TlsAcceptor::from(Arc::new(config))
}

/// Create af TlsConnector (client-side) for the given peer and private key.
pub(crate) fn make_tls_connector(
    verifier: Arc<PeerVerifier>,
    resolver: Arc<RawPublicKeyResolver>,
) -> TlsConnector {
    let mut config = ClientConfig::builder_with_protocol_versions(&[&TLS13])
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_cert_resolver(resolver);
    config.resumption = Resumption::disabled();
    config.enable_sni = false;

    TlsConnector::from(Arc::new(config))
}

/// Check client and server certificates.
///
/// Client and server connections are only accepted for known peers.
/// Add public keys to the verifier before using it.
#[derive(Debug)]
pub struct PeerVerifier {
    allowed_peers: BTreeMap<Vec<u8>, Peer>,
    algos: WebPkiSupportedAlgorithms,
}

impl Default for PeerVerifier {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerVerifier {
    /// Create a new, empty verifier
    pub fn new() -> Self {
        {
            let crypto = Arc::new(default_provider());
            Self {
                allowed_peers: BTreeMap::new(),
                algos: crypto.signature_verification_algorithms,
            }
        }
    }

    /// Return the set of peers this verifier may accept.
    pub fn peers(&self) -> impl Iterator<Item = Peer> {
        self.allowed_peers.values().map(|p| *p)
    }

    /// Create a verifier and fill it from [PeerConfig] instances.
    pub fn from_config(peers: &HashMap<Peer, PeerConfig>) -> anyhow::Result<Arc<Self>> {
        let mut verifier = Self::new();
        for (peer, config) in peers {
            let spki = SubjectPublicKeyInfoDer::from_pem_slice(config.pubkey.as_bytes())
                .with_context(|| format!("Failed to parse public key for peer {peer}"))?;
            verifier.add_peer(*peer, spki);
        }

        Ok(Arc::new(verifier))
    }

    /// Accept connections to the peer with the given public key and ID.
    ///
    /// The ID meant to identify the peer in logs.
    ///
    /// The SPKI must be the public part of a ED25519 key.
    pub fn add_peer(&mut self, peer: Peer, spki: rustls::pki_types::SubjectPublicKeyInfoDer) {
        self.allowed_peers.insert(spki.to_vec(), peer);
    }

    /// Check whether the given spki is authorized.
    fn accept_peer(&self, spki: &rustls::pki_types::SubjectPublicKeyInfoDer) -> bool {
        self.allowed_peers.contains_key(spki.as_ref())
    }

    /// Return the ID of the stream's peer.
    pub(crate) fn connection_peer_id<T>(
        &self,
        stream: &tokio_rustls::server::TlsStream<T>,
    ) -> Option<Peer> {
        let (_, conn) = stream.get_ref();
        if let Some(cert) = conn.peer_certificates() {
            if !cert.is_empty() {
                return self.allowed_peers.get(cert[0].as_ref()).copied();
            }
        }

        None
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

/// A type that holds the identity of the current peer.
#[derive(Debug)]
pub struct RawPublicKeyResolver {
    certified_key: Arc<CertifiedKey>,
}

impl RawPublicKeyResolver {
    /// Load the private key from the given pat and use it for the resolver.
    pub fn from_private_key_file<T: AsRef<Path>>(path: T) -> anyhow::Result<Arc<Self>> {
        let path = path.as_ref();
        let private_key = PrivateKeyDer::from_pem_file(path)
            .with_context(|| format!("invalid private key file {}", path.display()))?;
        let resolver = Self::from_private_key(private_key)
            .with_context(|| format!("invalid private key in file {}", path.display()))?;

        Ok(resolver)
    }

    /// Load the private key and use it for the resolver.
    pub fn from_private_key(
        private_key: PrivateKeyDer<'static>,
    ) -> Result<Arc<Self>, rustls::Error> {
        Self::new(
            default_provider()
                .key_provider
                .load_private_key(private_key)?,
        )
    }

    /// Create a resolver from a loaded signing key.
    pub fn new(privkey: Arc<dyn rustls::sign::SigningKey>) -> Result<Arc<Self>, rustls::Error> {
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
    use super::*;
    use crate::testing;
    use rustls::pki_types::PrivateKeyDer;
    use rustls::pki_types::pem::PemObject as _;
    use rustls::sign::SigningKey;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn tls_acceptor_connector() -> anyhow::Result<()> {
        test_connect(complete_verifier()).await?;

        Ok(())
    }

    #[tokio::test]
    async fn tls_unknown_client_peer() -> anyhow::Result<()> {
        let mut verifier = PeerVerifier::new();
        // client public key missing from verifier
        verifier.add_peer(Peer::from("server"), testing::server_public_key());

        assert!(test_connect(verifier).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn tls_unknown_server_peer() -> anyhow::Result<()> {
        let mut verifier = PeerVerifier::new();
        verifier.add_peer(Peer::from("client"), testing::client_public_key());
        // server public key missing from verifier

        assert!(test_connect(verifier).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn tls_reject_client_with_bad_private_key() -> anyhow::Result<()> {
        let mut verifier = PeerVerifier::new();
        verifier.add_peer(Peer::from("client"), testing::client_public_key());
        verifier.add_peer(Peer::from("server"), testing::server_public_key());

        let verifier = Arc::new(verifier);

        let server_priv = load_signing_key(testing::server_private_key())?;
        let acceptor = make_tls_acceptor(
            Arc::clone(&verifier),
            RawPublicKeyResolver::new(server_priv)?,
        );

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
    async fn tls_reject_client_without_cert() -> anyhow::Result<()> {
        let mut verifier = PeerVerifier::new();
        verifier.add_peer(Peer::from("client"), testing::client_public_key());
        verifier.add_peer(Peer::from("server"), testing::server_public_key());

        let verifier = Arc::new(verifier);

        let server_priv = load_signing_key(testing::server_private_key())?;
        let acceptor = make_tls_acceptor(
            Arc::clone(&verifier),
            RawPublicKeyResolver::new(server_priv)?,
        );

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
    async fn tls_reject_server_with_bad_private_key() -> anyhow::Result<()> {
        let mut verifier = PeerVerifier::new();
        verifier.add_peer(Peer::from("client"), testing::client_public_key());
        verifier.add_peer(Peer::from("server"), testing::server_public_key());

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
        let connector = make_tls_connector(
            Arc::clone(&verifier),
            RawPublicKeyResolver::new(client_priv)?,
        );

        assert!(test_connect_1(bad_acceptor, connector).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn tls_bad_private_key_algo() -> anyhow::Result<()> {
        let verifier = Arc::new(complete_verifier());

        let server_priv = load_signing_key(testing::server_private_key())?;
        let acceptor = make_tls_acceptor(verifier.clone(), RawPublicKeyResolver::new(server_priv)?);

        // Wrong key algorithm
        let ecdsa_client_priv = load_signing_key(ecdsa_private_key())?;
        let connector = make_tls_connector(
            verifier.clone(),
            RawPublicKeyResolver::new(ecdsa_client_priv)?,
        );

        assert!(test_connect_1(acceptor, connector).await.is_err());

        Ok(())
    }

    async fn test_connect(verifier: PeerVerifier) -> anyhow::Result<()> {
        let verifier = Arc::new(verifier);

        let server_priv = load_signing_key(testing::server_private_key())?;
        let acceptor = make_tls_acceptor(
            Arc::clone(&verifier),
            RawPublicKeyResolver::new(server_priv)?,
        );

        let client_priv = load_signing_key(testing::client_private_key())?;
        let connector = make_tls_connector(
            Arc::clone(&verifier),
            RawPublicKeyResolver::new(client_priv)?,
        );

        test_connect_1(acceptor, connector).await
    }

    async fn test_connect_1(acceptor: TlsAcceptor, connector: TlsConnector) -> anyhow::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let handle: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await?;

            let mut tls = acceptor.accept(tcp).await?;
            tls.write_all(b"foobar").await?;
            tls.shutdown().await?;

            Ok(())
        });

        let tcp = TcpStream::connect(addr).await?;
        let domain = rustls::pki_types::ServerName::try_from("localhost")?;
        let mut tls = connector.connect(domain, tcp).await?;
        let mut buf = vec![0u8; 6];
        tls.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"foobar");

        handle.await??;
        Ok(())
    }

    fn load_signing_key(
        key: rustls::pki_types::PrivateKeyDer<'static>,
    ) -> Result<Arc<dyn SigningKey>, rustls::Error> {
        super::default_provider().key_provider.load_private_key(key)
    }

    fn complete_verifier() -> PeerVerifier {
        let mut verifier = PeerVerifier::new();
        verifier.add_peer(Peer::from("client"), testing::client_public_key());
        verifier.add_peer(Peer::from("server"), testing::server_public_key());

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
