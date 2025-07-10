use realize_network::config::NetworkConfig;
use crate::storage::config::StorageConfig;

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
            storage: StorageConfig::new(),
        }
    }
}
