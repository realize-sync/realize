use crate::network::PeerSetup;

use super::config::PeerConfig;
use super::hostport::HostPort;
use super::security::{PeerVerifier, RawPublicKeyResolver};
use realize_types::Peer;
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{PrivateKeyDer, SubjectPublicKeyInfoDer};
use std::collections::HashMap;
use std::net::SocketAddr;

/// Public key for test client, in PEM format.
pub const CLIENT_PUBLIC_KEY_PEM: &[u8] = br#"
-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEA/CMSGfePPViYEUoHMNTrywE+mwTmB0poO0A1ATNIJGo=
-----END PUBLIC KEY-----
"#;

/// Public key for test servers, in PEM format.
const SERVER_PUBLIC_KEY_PEM: &[u8] = br#"
-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAIckr1J3xrgglc6pseuCDWDAupSMzA1TJyitkgJi/SPg=
-----END PUBLIC KEY-----
"#;

/// Another test public key, in PEM format.
const OTHER_PUBLIC_KEY_PEM: &[u8] = br#"
-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAJ7KIbhdPS2ESzYMeQXoqHJv8Vdmi+pJlkFChY8K+IVg=
-----END PUBLIC KEY-----
"#;

/// Public key for test clients.
///
/// Generated from [client_private_key] with:
///   openssl pkey -in peer.key -pubout -out -
pub fn client_public_key() -> SubjectPublicKeyInfoDer<'static> {
    SubjectPublicKeyInfoDer::from_pem_slice(CLIENT_PUBLIC_KEY_PEM)
        .expect("Invalid test client public key")
}

/// Public key for test servers.
///
/// Generated from [server_private_key] with:
///   openssl pkey -in peer.key -pubout -out -
pub fn server_public_key() -> SubjectPublicKeyInfoDer<'static> {
    SubjectPublicKeyInfoDer::from_pem_slice(SERVER_PUBLIC_KEY_PEM)
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

/// Extract I/O error kind from an anyhow error, if possible.
pub fn io_error_kind(err: Option<anyhow::Error>) -> Option<std::io::ErrorKind> {
    Some(err?.downcast_ref::<std::io::Error>()?.kind())
}

/// Helper for building [Networking] for multiple peers that can
/// communicate with each other.
pub struct TestingPeers {
    peer_setup: HashMap<Peer, PeerSetup>,
    peers: HashMap<Peer, PeerConfig>,
    private_keys: HashMap<Peer, PrivateKeyDer<'static>>,
}

#[allow(dead_code)]
impl TestingPeers {
    pub fn a() -> Peer {
        Peer::from("a")
    }
    pub fn b() -> Peer {
        Peer::from("b")
    }
    pub fn c() -> Peer {
        Peer::from("c")
    }

    /// Build an empty instance.
    pub fn empty() -> Self {
        Self {
            peer_setup: HashMap::new(),
            peers: HashMap::new(),
            private_keys: HashMap::new(),
        }
    }

    /// Build an instance with the three peers, [TestingPeers::a],
    /// [TestingPeers::b] and [TestingPeers::c], pre-configured.
    pub fn new() -> anyhow::Result<Self> {
        let mut peers = Self::empty();
        peers.add(
            TestingPeers::a(),
            CLIENT_PUBLIC_KEY_PEM,
            client_private_key(),
        )?;
        peers.add(
            TestingPeers::b(),
            SERVER_PUBLIC_KEY_PEM,
            server_private_key(),
        )?;
        peers.add(TestingPeers::c(), OTHER_PUBLIC_KEY_PEM, other_private_key())?;

        Ok(peers)
    }

    /// Add a peer.
    pub fn add(
        &mut self,
        peer: Peer,
        pubkey: &[u8],
        private_key: PrivateKeyDer<'static>,
    ) -> anyhow::Result<()> {
        self.peers.insert(
            peer,
            PeerConfig {
                peer,
                pubkey: String::from_utf8(pubkey.to_vec())?,
                address: None,
                batch_rate_limit: None,
            },
        );
        self.private_keys.insert(peer, private_key);

        Ok(())
    }

    /// Choose an address for the given peer, store it and return it.
    pub fn pick_port(&mut self, peer: Peer) -> anyhow::Result<HostPort> {
        let port = portpicker::pick_unused_port().ok_or(anyhow::anyhow!("No free port"))?;
        let hostport = HostPort::localhost(port);
        self.set_addr(peer, hostport.addr());
        Ok(hostport)
    }

    /// Set address of the given peer.
    pub fn set_addr(&mut self, peer: Peer, addr: SocketAddr) {
        self.set_hostport(peer, HostPort::from(addr))
    }

    pub fn set_hostport(&mut self, peer: Peer, hostport: HostPort) {
        self.peer_setup.entry(peer).or_default().address = Some(hostport.to_string());
    }

    pub fn set_batch_rate_limit(&mut self, peer: Peer, bytes_per_second: u64) {
        self.peer_setup.entry(peer).or_default().batch_rate_limit = Some(bytes_per_second);
    }

    /// Get the address configured for peer.
    pub async fn hostport(&self, peer: Peer) -> Option<HostPort> {
        if let Some(address) = self
            .peer_setup
            .get(&peer)
            .map(|s| s.address.as_deref())
            .flatten()
        {
            return HostPort::parse(address).await.ok();
        }

        None
    }

    /// Build a [Networking] instance for the given peer.
    ///
    /// Other peer public keys and addreses will be available.
    pub fn networking(&self, peer: Peer) -> anyhow::Result<crate::Networking> {
        let others: Vec<PeerConfig> = self
            .peers
            .values()
            .filter(|config| config.peer != peer)
            .cloned()
            .collect();

        let verifier = PeerVerifier::from_config(&others)?;
        let resolver = RawPublicKeyResolver::from_private_key(
            self.private_keys
                .get(&peer)
                .ok_or(anyhow::anyhow!("No private key for {peer}"))?
                .clone_key(),
        )?;

        Ok(crate::Networking::new(
            self.peer_setup.clone(),
            resolver,
            verifier,
        ))
    }
}

#[allow(dead_code)]
#[allow(unknown_lints)]
#[allow(clippy::uninlined_format_args)]
#[allow(clippy::extra_unused_type_parameters)]
pub mod hello_capnp {
    include!(concat!(env!("OUT_DIR"), "/testing/hello_capnp.rs"));
}
