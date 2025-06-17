use std::{path::PathBuf, process::Stdio};

use nix::fcntl::FlockArg;
use realize_lib::{
    network::hostport::HostPort, storage::unreal::UnrealCacheAsync, utils::async_utils::AbortOnDrop,
};
use tokio::process::Command;

use crate::common::config::Config;

pub struct Fixture {
    pub mountpoint: PathBuf,
    _export: AbortOnDrop<std::io::Result<()>>,
}
impl Fixture {
    pub async fn setup(cache: UnrealCacheAsync) -> anyhow::Result<Fixture> {
        let _ = env_logger::try_init();
        let config = Config::read()?;
        let nfs_config = config.nfs.ok_or_else(|| {
            anyhow::anyhow!("NFS should be configure in {}", Config::path().display())
        })?;

        log::debug!("Obtaining lock {}...", nfs_config.flock.display());
        if let Some(p) = nfs_config.flock.parent() {
            if !p.exists() {
                std::fs::create_dir_all(p)?;
            }
        }
        let _lock = nix::fcntl::Flock::lock(
            std::fs::File::create(&nfs_config.flock)?,
            FlockArg::LockExclusive,
        )
        .map_err(|(_, errno)| {
            anyhow::anyhow!(
                "flock failed on {}: errno {}",
                nfs_config.flock.display(),
                errno
            )
        })?;
        log::debug!("Obtained lock {}", nfs_config.flock.display());

        log::debug!("Exporting UnrealFs on localhost:{}", nfs_config.port);
        let export = AbortOnDrop::new(
            realize_fs::nfs::export(cache, HostPort::localhost(nfs_config.port).addr()).await?,
        );

        if nfs_config.mountpoint.exists() {
            log::debug!("Unmounting {}", nfs_config.mountpoint.display());
            let _ = Command::new("umount")
                .stdin(Stdio::null())
                .arg(&nfs_config.mountpoint)
                .stderr(Stdio::null())
                .status()
                .await?;
        } else {
            log::debug!("Create mountpoint {}", nfs_config.mountpoint.display());
            std::fs::create_dir_all(&nfs_config.mountpoint)?;
        }
        log::debug!("Mounting on {}", nfs_config.mountpoint.display());
        let status = Command::new("mount")
            .stdin(Stdio::null())
            .arg(&nfs_config.mountpoint)
            .status()
            .await?;
        if !status.success() {
            anyhow::bail!("mount failed with status code {status}");
        }

        log::debug!("UnrealFs mounted on {}", nfs_config.mountpoint.display());

        Ok(Fixture {
            mountpoint: nfs_config.mountpoint,
            _export: export,
        })
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let mountpoint = &self.mountpoint;
        log::debug!("Unmounting {}", mountpoint.display());

        let _ = std::process::Command::new("umount")
            .stdin(Stdio::null())
            .arg(mountpoint)
            .spawn();
    }
}
