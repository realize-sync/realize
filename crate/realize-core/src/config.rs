use realize_network::config::NetworkConfig;
use realize_storage::config::StorageConfig;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
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
