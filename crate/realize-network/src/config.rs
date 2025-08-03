use realize_types::Peer;
use std::collections::HashMap;

#[derive(Clone, serde::Deserialize, serde::Serialize, Debug)]
pub struct NetworkConfig {
    pub peers: HashMap<Peer, PeerConfig>,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl NetworkConfig {
    pub fn new() -> Self {
        NetworkConfig {
            peers: HashMap::new(),
        }
    }
}

/// Define a peer.
///
/// A peer is identified by [realize_types::Peer].
#[derive(Clone, serde::Deserialize, serde::Serialize, Debug, Default)]
pub struct PeerConfig {
    /// Address of the peer, if available.
    ///
    /// Not all peers can be connected to. Peers that can should have
    /// their address listed here.
    pub address: Option<String>,

    /// Specify the peer's public key that'll be used to identify
    /// the peer during connection.
    ///
    /// Must be a PEM-encoded ED25519 public key.
    pub pubkey: String,

    /// Rate limit that applies to batch operations.
    ///
    /// Interactive operations are not limited.
    pub batch_rate_limit: Option<u64>,
}
