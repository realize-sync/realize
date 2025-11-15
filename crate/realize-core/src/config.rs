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
                watcher: realize_storage::config::WatcherConfig::default(),
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
    use realize_types::Peer;

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

            [watcher]
            max_parallel_hashers = 4
            debounce = "500ms"
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
                    cache: realize_storage::config::CacheConfig {
                        db: PathBuf::from("/path/to/cache.db"),
                    },
                    watcher: realize_storage::config::WatcherConfig {
                        max_parallel_hashers: Some(4),
                        debounce: Some(HumanDuration::from_millis(500)),
                    }
                },
            });
    }

    #[test]
    fn parse_minimal_config() {
        let toml_str = r#"
            [cache]
            db = "/path/to/cache.db"
        "#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(
            config,
            Config {
                network: realize_network::config::NetworkConfig { peers: vec![] },
                storage: realize_storage::config::StorageConfig {
                    cache: realize_storage::config::CacheConfig {
                        db: PathBuf::from("/path/to/cache.db"),
                    },
                    watcher: realize_storage::config::WatcherConfig::default()
                },
            }
        );
    }
}
