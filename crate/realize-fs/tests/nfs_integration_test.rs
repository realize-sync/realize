use std::time::{Duration, SystemTime};

use common::mountpoint;
use realize_lib::{
    model::{Arena, Path},
    storage::unreal::UnrealCacheBlocking,
};
use std::os::unix::fs::PermissionsExt as _;

mod common;

#[tokio::test]
#[test_tag::tag(nfs)]
async fn export_arena() -> anyhow::Result<()> {
    let mut cache = in_memory_cache()?;
    let arena = Arena::from("test");
    cache.add_arena(&arena)?;
    let cache = cache.into_async();

    let fixture = mountpoint::Fixture::setup(cache).await?;

    let mut dir_content = tokio::fs::read_dir(&fixture.mountpoint).await?;
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

    Ok(())
}

#[tokio::test]
#[test_tag::tag(nfs)]
async fn export_linked_files() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let mut cache = in_memory_cache()?;
    let arena = Arena::from("test");
    cache.add_arena(&arena)?;
    let cache = cache.into_async();
    let peer = realize_lib::model::Peer::from("peer");
    let test_mtime = SystemTime::UNIX_EPOCH
        .checked_add(Duration::from_secs(1234567890))
        .unwrap();
    cache
        .link(&peer, &arena, &Path::parse("a/b/c.txt")?, 1024, test_mtime)
        .await?;
    cache
        .link(&peer, &arena, &Path::parse("a/b/d.txt")?, 2048, test_mtime)
        .await?;

    let fixture = mountpoint::Fixture::setup(cache).await?;

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

fn in_memory_cache() -> anyhow::Result<UnrealCacheBlocking> {
    let cache = realize_lib::storage::unreal::UnrealCacheBlocking::new(
        redb::Builder::new().create_with_backend(redb::backends::InMemoryBackend::new())?,
    )?;

    Ok(cache)
}
