use realize_types::Peer;
use std::collections::HashMap;

#[derive(Clone, serde::Deserialize, serde::Serialize, Debug, PartialEq, Eq)]
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
#[derive(Clone, serde::Deserialize, serde::Serialize, Debug, Default, PartialEq, Eq)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_network_config() {
        let toml_str = r#"
            [peers."peer1"]
            address = "192.168.1.100:8080"
            pubkey = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...\n-----END PUBLIC KEY-----"
            batch_rate_limit = 1000

            [peers."peer2"]
            pubkey = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...\n-----END PUBLIC KEY-----"

            [peers."peer3"]
            address = "10.0.0.1:9000"
            pubkey = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...\n-----END PUBLIC KEY-----"
            batch_rate_limit = 500
        "#;

        let config: NetworkConfig = toml::from_str(toml_str).unwrap();

        assert_eq!(config, NetworkConfig {
            peers: HashMap::from([(
            Peer::from("peer1"),
            PeerConfig {
                address: Some("192.168.1.100:8080".to_string()),
                pubkey: "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...\n-----END PUBLIC KEY-----".to_string(),
                batch_rate_limit: Some(1000),
            },
        ),(
            Peer::from("peer2"),
            PeerConfig {
                address: None,
                pubkey: "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...\n-----END PUBLIC KEY-----".to_string(),
                batch_rate_limit: None,
            },
        ),(
            Peer::from("peer3"),
            PeerConfig {
                address: Some("10.0.0.1:9000".to_string()),
                pubkey: "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...\n-----END PUBLIC KEY-----".to_string(),
                batch_rate_limit: Some(500),
            },
        )])});
    }
}
