use std::{
    fs,
    path::{Path, PathBuf},
};

#[derive(serde::Deserialize)]
pub struct Config {
    pub nfs: Option<NfsConfig>,
}

#[derive(serde::Deserialize)]
pub struct NfsConfig {
    pub mountpoint: PathBuf,
    pub port: u16,
    pub flock: PathBuf,
}

impl Config {
    pub fn path() -> PathBuf {
        let manifest_dir = &Path::new(env!("CARGO_MANIFEST_DIR"));

        manifest_dir.join("Test-local.toml")
    }
    pub fn read() -> anyhow::Result<Config> {
        let config_path = Config::path();

        if !config_path.exists() {
            anyhow::bail!(
                "Local test configuration missing at {}",
                config_path.display()
            );
        }

        Ok(toml::from_str(&fs::read_to_string(config_path)?)?)
    }
}
