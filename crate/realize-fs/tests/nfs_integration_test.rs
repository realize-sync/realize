use common::mountpoint;
use realize_lib::model::Arena;

mod common;

#[tokio::test]
#[test_tag::tag(nfs)]
async fn export_arena() -> anyhow::Result<()> {
    let mut cache = realize_lib::storage::unreal::UnrealCacheBlocking::new(
        redb::Builder::new().create_with_backend(redb::backends::InMemoryBackend::new())?,
    )?;
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
