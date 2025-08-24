use realize_network::config::NetworkConfig;
use realize_storage::config::StorageConfig;
use std::collections::HashMap;
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
                arenas: HashMap::new(),
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
    use realize_types::{Arena, Peer};

    #[test]
    fn parse_config() {
        let toml_str = r#"
            [peers."peer1"]
            address = "192.168.1.100:8080"
            pubkey = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...\n-----END PUBLIC KEY-----"
            batch_rate_limit = 1000

            [cache]
            db = "/path/to/cache.db"

            [arenas."arena1"]
            root = "/path/to/arena1"
            db = "/path/to/arena1.db"
            blob_dir = "/path/to/arena1/blobs"
            max_parallel_hashers = 4
            debounce_secs = 5
            disk_usage = { max = "50%", leave = "1G" }
        "#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(
            config,
            Config {
                network: realize_network::config::NetworkConfig {
                    peers: HashMap::from([
                        (
                            Peer::from("peer1"),
                            realize_network::config::PeerConfig {
                                address: Some("192.168.1.100:8080".to_string()),
                                pubkey: "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...\n-----END PUBLIC KEY-----".to_string(),
                                batch_rate_limit: Some(realize_network::config::ByteValue(1000)),
                            },
                        )]),
                },
                storage: realize_storage::config::StorageConfig {
                    arenas: HashMap::from([(
                        Arena::from("arena1"),
                        realize_storage::config::ArenaConfig {
                            root: Some(PathBuf::from("/path/to/arena1")),
                            db: PathBuf::from("/path/to/arena1.db"),
                            blob_dir: PathBuf::from("/path/to/arena1/blobs"),
                            max_parallel_hashers: Some(4),
                            debounce_secs: Some(5),
                            disk_usage: Some(realize_storage::config::DiskUsageLimits {
                                max: realize_storage::config::BytesOrPercent::Percent(50),
                                leave: Some(realize_storage::config::BytesOrPercent::Bytes(1024 * 1024 * 1024)),
                            }),
                        },
                    )]),
                    cache: realize_storage::config::CacheConfig {
                        db: PathBuf::from("/path/to/cache.db"),
                    },
                },
            });
    }
}
