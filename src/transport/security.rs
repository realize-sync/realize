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

        Ok(Arc::new(Self { certified_key }))
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
