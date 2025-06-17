use std::process::Stdio;

use assert_fs::TempDir;
use config::Config;
use nix::fcntl::FlockArg;
use realize_lib::{model::Arena, network::hostport::HostPort};
use tokio::process::Command;

mod config;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[test_tag::tag(nfs)]
async fn smoke() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(std::io::stderr)
        .init();

    let tmpdir = TempDir::new()?;
    let mut cache =
        realize_lib::storage::unreal::UnrealCacheBlocking::open(&tmpdir.path().join("unreal.db"))?;
    let arena = Arena::from("test");
    cache.add_arena(&arena)?;
    let cache = cache.into_async();

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

    log::debug!("Exporting on localhost:{}", nfs_config.port);
    realize_fs::nfs::export(cache, HostPort::localhost(nfs_config.port).addr()).await?;

    if nfs_config.mountpoint.exists() {
        log::debug!("Unmounting {}", nfs_config.mountpoint.display());
        let _ = Command::new("umount")
            .stdin(Stdio::null())
            .arg(&nfs_config.mountpoint)
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
        anyhow::bail!("mount fail with status code {status}");
    }

    log::debug!(
        "Mounted on {}; reading dir",
        nfs_config.mountpoint.display()
    );
    // let _ = nix::mount::umount(&nfs_config.mountpoint);
    // nix::mount::mount(
    //     None::<&std::ffi::OsStr>,
    //     &nfs_config.mountpoint,
    //     None::<&std::ffi::OsStr>,
    //     nix::mount::MsFlags::empty(),
    //     None::<&std::ffi::OsStr>,
    // )
    // .with_context(|| format!("mount failed for {}", nfs_config.mountpoint.display()))?;

    let mut dir_content = tokio::fs::read_dir(&nfs_config.mountpoint).await?;
    assert_eq!(
        "test",
        dir_content
            .next_entry()
            .await?
            .ok_or(anyhow::anyhow!("Expected directory entry"))?
            .file_name()
            .to_string_lossy()
            .to_string()
    );
    assert!(dir_content.next_entry().await?.is_none());

    let _ = Command::new("umount")
        .stdin(Stdio::null())
        .arg("--force")
        .arg(&&nfs_config.mountpoint)
        .status();

    Ok(())
}
