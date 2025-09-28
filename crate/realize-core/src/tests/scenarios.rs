use crate::consensus::churten::Churten;
use crate::rpc::testing::{self, HouseholdFixture};
use realize_storage::config::DiskUsageLimits;
use realize_storage::utils::hash;
use realize_storage::{CacheStatus, FileRealm, Mark, Version};
use realize_types::Path;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

#[tokio::test]
async fn file_drop() -> anyhow::Result<()> {
    let a = HouseholdFixture::a();
    let b = HouseholdFixture::b();
    let arena = HouseholdFixture::test_arena();
    let mut builder = HouseholdFixture::builder();
    let config = builder.config_mut(a);
    let arena_config = config.arena_config_mut(arena).unwrap();
    arena_config.disk_usage = Some(DiskUsageLimits::max_bytes(0));

    let mut fixture = builder.setup().await?;
    fixture
        .with_two_peers()
        .await?
        .interconnected()
        .run(async |_household_a, household_b| {
            let storage_a = fixture.storage(a)?;
            storage_a.set_arena_mark(arena, Mark::Watch).await?;
            let storage_b = fixture.storage(b)?;
            storage_b.set_arena_mark(arena, Mark::Own).await?;

            // write a file to a; it'll get downloaded by b then a's
            // copy will be deleted.
            let mut churten = Churten::new(Arc::clone(&storage_b), household_b.clone());
            churten.start();

            let content = b"foo!".repeat(2 * 1024 * 1024 / 4); // 2M
            let hash = hash::digest(&content);

            let root_a = fixture.arena_root(a);
            let foo = Path::parse("foo")?;
            let foo_a = foo.within(&root_a);
            std::fs::write(&foo_a, content.as_slice())?;

            // foo must be gone from A
            let limit = Instant::now() + Duration::from_secs(10);
            while foo_a.exists() && Instant::now() < limit {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert!(!foo_a.exists());

            // foo must be available in b as a file
            let root_b = fixture.arena_root(b);
            let foo_b = foo.within(&root_b);
            assert!(foo_b.exists());

            assert_eq!(hash, hash::digest(std::fs::read_to_string(foo_b)?));

            // foo must be available in A, in the cache, but the local
            // copy that was moved into the cache must eventually be
            // deleted, because max disk usage is 0.
            let cache_a = storage_a.cache();
            while cache_a.file_realm((arena, &foo)).await?
                != FileRealm::Remote(CacheStatus::Missing)
                && Instant::now() < limit
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert_eq!(
                FileRealm::Remote(CacheStatus::Missing),
                cache_a.file_realm((arena, &foo)).await?
            );

            Ok::<(), anyhow::Error>(())
        })
        .await?;

    Ok(())
}

#[tokio::test]
async fn link_to_own() -> anyhow::Result<()> {
    let a = HouseholdFixture::a();
    let b = HouseholdFixture::b();
    let arena = HouseholdFixture::test_arena();
    let mut builder = HouseholdFixture::builder();
    let config = builder.config_mut(a);
    let arena_config = config.arena_config_mut(arena).unwrap();
    arena_config.disk_usage = Some(DiskUsageLimits::max_bytes(0));

    let mut fixture = builder.setup().await?;
    fixture
        .with_two_peers()
        .await?
        .run(async |household_a, household_b| {
            testing::connect(&household_a, b).await?;
            let work = Path::parse("work")?;
            let store = Path::parse("store")?;
            let storage_a = fixture.storage(a)?;
            storage_a.set_mark(arena, &work, Mark::Own).await?;
            storage_a.set_mark(arena, &store, Mark::Watch).await?;
            let storage_b = fixture.storage(b)?;
            storage_b.set_mark(arena, &work, Mark::Watch).await?;
            storage_b.set_mark(arena, &store, Mark::Own).await?;

            let content = b"foo!".repeat(2 * 1024 * 1024 / 4); // 2M
            let hash = hash::digest(&content);

            // A: create work/foo (on the filesystem)
            let root_a = fixture.arena_root(a);
            let work_foo_in_a = Path::parse("work/foo")?.within(&root_a);
            std::fs::create_dir(work_foo_in_a.parent().unwrap())?;
            std::fs::write(&work_foo_in_a, content.as_slice())?;

            // B: branch work/foo -> store/foo (on the filesystem)
            fixture.wait_for_file_in_cache(b, "work/foo", &hash).await?;
            let cache_b = fixture.cache(b)?;
            let (store_pathid, _) = cache_b.mkdir((cache_b.arena_root(arena)?, "store")).await?;
            cache_b
                .branch((arena, &Path::parse("work/foo")?), (store_pathid, "foo"))
                .await?;

            let mut churten = Churten::new(Arc::clone(&storage_b), household_b.clone());
            churten.start();

            let store_foo = Path::parse("store/foo")?;
            // B should download and realize store/foo since it's marked owned
            let root_b = fixture.arena_root(b);
            let store_foo_in_b = store_foo.within(&root_b);
            let deadline = Instant::now() + Duration::from_secs(10);
            while !store_foo_in_b.exists() && Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert!(store_foo_in_b.exists());
            while cache_b.file_realm((arena, &store_foo)).await? != FileRealm::Local
                && Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            while cache_b
                .file_metadata((arena, &store_foo))
                .await?
                .version
                .indexed_hash()
                .is_none()
                && Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            assert_eq!(
                Version::Indexed(hash.clone()),
                cache_b.file_metadata((arena, &store_foo)).await?.version
            );
            assert_eq!(hash, hash::digest(std::fs::read_to_string(store_foo_in_b)?));

            // Once B has reported that it has store/foo, A should
            // unrealize store/foo, since it's marked watched. This
            // deletes B's local copy of store/foo.
            let store_foo_in_a = Path::parse("store/foo")?.within(&root_a);
            while store_foo_in_a.exists() && Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert!(!store_foo_in_a.exists());
            assert!(work_foo_in_a.exists());

            Ok::<(), anyhow::Error>(())
        })
        .await?;

    Ok(())
}
