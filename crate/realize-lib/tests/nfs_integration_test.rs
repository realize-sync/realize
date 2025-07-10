use assert_fs::prelude::*;
use common::mountpoint;
use realize_lib::model::{Hash, Path, UnixTime};
use realize_lib::storage::Notification;
use realize_lib::utils::hash;
use std::os::unix::fs::PermissionsExt as _;

mod common;

#[tokio::test]
#[test_tag::tag(nfsmount)]
#[cfg_attr(not(target_os = "linux"), ignore)]
async fn export_arena() -> anyhow::Result<()> {
    let local = tokio::task::LocalSet::new();
    let fixture = mountpoint::Fixture::setup(&local).await?;
    local
        .run_until(async move {
            let mut dir_content = tokio::fs::read_dir(&fixture.mountpoint).await?;
            assert_eq!(
                fixture.arena.as_str(),
                dir_content
                    .next_entry()
                    .await?
                    .ok_or(anyhow::anyhow!("Expected directory entry"))?
                    .file_name()
                    .to_string_lossy()
                    .to_string()
            );
            assert!(dir_content.next_entry().await?.is_none());

            Ok::<_, anyhow::Error>(())
        })
        .await?;

    Ok(())
}

#[tokio::test]
#[test_tag::tag(nfsmount)]
#[cfg_attr(not(target_os = "linux"), ignore)]
async fn export_files() -> anyhow::Result<()> {
    let local = tokio::task::LocalSet::new();
    let fixture = mountpoint::Fixture::setup(&local).await?;
    local
        .run_until(async move {
            let cache = &fixture.cache;
            let peer = realize_lib::model::Peer::from("peer");
            let arena = &fixture.arena;
            let test_mtime = UnixTime::from_secs(1234567890);
            cache
                .update(
                    &peer,
                    Notification::Add {
                        arena: arena.clone(),
                        index: 0,
                        path: Path::parse("a/b/c.txt")?,
                        mtime: test_mtime.clone(),
                        size: 1024,
                        hash: Hash([1u8; 32]),
                    },
                )
                .await?;
            cache
                .update(
                    &peer,
                    Notification::Add {
                        arena: arena.clone(),
                        index: 0,
                        path: Path::parse("a/b/d.txt")?,
                        mtime: test_mtime.clone(),
                        size: 2048,
                        hash: Hash([2u8; 32]),
                    },
                )
                .await?;

            let mut entries = vec![];
            let mut dir_content = tokio::fs::read_dir(&fixture.mountpoint.join("test/a/b")).await?;
            while let Some(entry) = dir_content.next_entry().await? {
                entries.push(entry);
            }
            entries.sort_by_key(|a| a.file_name());
            assert_eq!(2, entries.len());

            let c = &entries[0];
            let d = &entries[1];

            assert_eq!("c.txt", c.file_name().to_string_lossy());
            let metadata = c.metadata().await?;
            assert!(metadata.file_type().is_file());
            assert_eq!(0o0440, metadata.permissions().mode() & 0o7777);
            assert_eq!(test_mtime, UnixTime::mtime(&metadata));

            assert_eq!("d.txt", d.file_name().to_string_lossy());
            let metadata = d.metadata().await?;
            assert!(metadata.file_type().is_file());
            assert_eq!(0o0440, metadata.permissions().mode() & 0o7777);
            assert_eq!(test_mtime, UnixTime::mtime(&metadata));

            Ok::<_, anyhow::Error>(())
        })
        .await?;

    Ok(())
}

#[tokio::test]
#[test_tag::tag(nfsmount)]
#[cfg_attr(not(target_os = "linux"), ignore)]
async fn read_file() -> anyhow::Result<()> {
    let local = tokio::task::LocalSet::new();
    let fixture = mountpoint::Fixture::setup(&local).await?;
    local
        .run_until(async move {
            let cache = &fixture.cache;
            let peer = realize_lib::model::Peer::from("server");
            let arena = &fixture.arena;
            let path = Path::parse("testfile.txt")?;
            let testfile = fixture.tempdir.child("testfile.txt");
            testfile.write_str("hello, world")?;
            let m = tokio::fs::metadata(&testfile.path()).await?;
            cache
                .update(
                    &peer,
                    Notification::Add {
                        arena: arena.clone(),
                        index: 0,
                        path: path.clone(),
                        mtime: UnixTime::mtime(&m),
                        size: m.len(),
                        hash: hash::digest("hello, world"),
                    },
                )
                .await?;

            assert_eq!(
                "hello, world",
                tokio::fs::read_to_string(&fixture.mountpoint.join("test/testfile.txt")).await?
            );

            Ok::<_, anyhow::Error>(())
        })
        .await?;

    Ok(())
}
