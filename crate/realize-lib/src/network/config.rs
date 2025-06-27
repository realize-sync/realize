use std::collections::HashMap;

use crate::model::Peer;

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
/// A peer is identified by [crate::model::Peer].
#[derive(Clone, serde::Deserialize, serde::Serialize, Debug)]
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
}
