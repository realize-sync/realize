use std::time::{Duration, SystemTime};

use assert_fs::prelude::FileWriteStr as _;
use assert_fs::prelude::PathChild as _;
use common::mountpoint;
use realize_lib::model::Path;
use std::os::unix::fs::PermissionsExt as _;

mod common;

#[tokio::test]
#[test_tag::tag(nfs)]
async fn export_arena() -> anyhow::Result<()> {
    let fixture = mountpoint::Fixture::setup().await?;

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

    Ok(())
}

#[tokio::test]
#[test_tag::tag(nfs)]
async fn export_linked_files() -> anyhow::Result<()> {
    let fixture = mountpoint::Fixture::setup().await?;
    let cache = &fixture.cache;
    let peer = realize_lib::model::Peer::from("peer");
    let arena = &fixture.arena;
    let test_mtime = SystemTime::UNIX_EPOCH
        .checked_add(Duration::from_secs(1234567890))
        .unwrap();
    cache
        .link(&peer, arena, &Path::parse("a/b/c.txt")?, 1024, test_mtime)
        .await?;
    cache
        .link(&peer, arena, &Path::parse("a/b/d.txt")?, 2048, test_mtime)
        .await?;

    let mut entries = vec![];
    let mut dir_content = tokio::fs::read_dir(&fixture.mountpoint.join("test/a/b")).await?;
    while let Some(entry) = dir_content.next_entry().await? {
        entries.push(entry);
    }
    entries.sort_by(|a, b| a.file_name().cmp(&b.file_name()));
    assert_eq!(2, entries.len());

    let c = &entries[0];
    let d = &entries[1];

    assert_eq!("c.txt", c.file_name().to_string_lossy());
    let metadata = c.metadata().await?;
    assert!(metadata.file_type().is_file());
    assert_eq!(0o0440, metadata.permissions().mode() & 0o7777);
    assert_eq!(test_mtime, metadata.modified().unwrap());

    assert_eq!("d.txt", d.file_name().to_string_lossy());
    let metadata = d.metadata().await?;
    assert!(metadata.file_type().is_file());
    assert_eq!(0o0440, metadata.permissions().mode() & 0o7777);
    assert_eq!(test_mtime, metadata.modified().unwrap());

    Ok(())
}

#[tokio::test]
#[test_tag::tag(nfs)]
async fn read_file() -> anyhow::Result<()> {
    let fixture = mountpoint::Fixture::setup().await?;
    let cache = &fixture.cache;
    let peer = realize_lib::model::Peer::from("server");
    let arena = &fixture.arena;
    let path = Path::parse("testfile.txt")?;
    let testfile = fixture.tempdir.child("testfile.txt");
    testfile.write_str("hello, world")?;
    let m = tokio::fs::metadata(&testfile.path()).await?;
    cache
        .link(&peer, arena, &path, m.len(), m.modified()?)
        .await?;

    assert_eq!(
        "hello, world",
        tokio::fs::read_to_string(&fixture.mountpoint.join("test/testfile.txt")).await?
    );

    Ok(())
}
