use realize_network::config::NetworkConfig;
use realize_storage::config::StorageConfig;
use std::path::PathBuf;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
pub struct Config {
    #[serde(flatten)]
    pub network: NetworkConfig,
    #[serde(flatten)]
    pub storage: StorageConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

impl Config {
    pub fn new() -> Self {
        Self {
            network: NetworkConfig::new(),
            storage: StorageConfig {
                arenas: Vec::new(),
                cache: realize_storage::config::CacheConfig {
                    db: PathBuf::from("cache.db"), // Default for backward compatibility
                },
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use realize_storage::config::HumanDuration;
    use realize_types::{Arena, Peer};

    #[test]
    fn parse_config() {
        let toml_str = r#"
            [[peer]]
            name = "peer1"
            address = "192.168.1.100:8080"
            pubkey = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...\n-----END PUBLIC KEY-----"
            batch_rate_limit = "512K"

            [cache]
            db = "/path/to/cache.db"

            [[arena]]
            name = "arena1"
            root = "/path/to/arena1"
            db = "/path/to/arena1.db"
            blob_dir = "/path/to/arena1/blobs"
            max_parallel_hashers = 4
            debounce = "500ms"
            disk_usage = { max = "50%", leave = "1G" }
        "#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(
            config,
            Config {
                network: realize_network::config::NetworkConfig {
                    peers: vec![
                        realize_network::config::PeerConfig {
                            peer: Peer::from("peer1"),
                            address: Some("192.168.1.100:8080".to_string()),
                            pubkey: "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...\n-----END PUBLIC KEY-----".to_string(),
                            batch_rate_limit: Some(realize_network::config::ByteValue(512*1024)),
                        },
                    ],
                },
                storage: realize_storage::config::StorageConfig {
                    arenas: vec![
                        realize_storage::config::ArenaConfig {
                            arena: Arena::from("arena1"),
                            root: Some(PathBuf::from("/path/to/arena1")),
                            db: PathBuf::from("/path/to/arena1.db"),
                            blob_dir: PathBuf::from("/path/to/arena1/blobs"),
                            max_parallel_hashers: Some(4),
                            debounce: Some(HumanDuration::from_millis(500)),
                            disk_usage: Some(realize_storage::config::DiskUsageLimits {
                                max: realize_storage::config::BytesOrPercent::Percent(50),
                                leave: Some(realize_storage::config::BytesOrPercent::Bytes(1024 * 1024 * 1024)),
                            }),
                        },
                    ],
                    cache: realize_storage::config::CacheConfig {
                        db: PathBuf::from("/path/to/cache.db"),
                    },
                },
            });
    }
}
