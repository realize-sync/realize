use crate::consensus::churten::Churten;
use crate::rpc::testing::HouseholdFixture;
use realize_storage::config::DiskUsageLimits;
use realize_storage::utils::hash;
use realize_storage::{LocalAvailability, Mark};
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
            let limit = Instant::now() + Duration::from_secs(5);
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
            while cache_a.local_availability((arena, &foo)).await? != LocalAvailability::Missing
                && Instant::now() < limit
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert_eq!(
                LocalAvailability::Missing,
                cache_a.local_availability((arena, &foo)).await?
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
        .interconnected()
        .run(async |_household_a, household_b| {
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

            let root_a = fixture.arena_root(a);
            let work_foo_in_a = Path::parse("work/foo")?.within(&root_a);
            std::fs::create_dir(work_foo_in_a.parent().unwrap())?;
            std::fs::write(&work_foo_in_a, content.as_slice())?;

            fixture.wait_for_file_in_cache(b, "work/foo", &hash).await?;
            let cache_b = fixture.cache(b)?;
            let (store_pathid, _) = cache_b.mkdir((cache_b.arena_root(arena)?, "store")).await?;
            cache_b
                .branch((arena, &Path::parse("work/foo")?), (store_pathid, "foo"))
                .await?;

            let mut churten = Churten::new(Arc::clone(&storage_b), household_b.clone());
            churten.start();

            let root_b = fixture.arena_root(b);
            let store_foo_in_b = Path::parse("store/foo")?.within(&root_b);
            let deadline = Instant::now() + Duration::from_secs(3);
            while !store_foo_in_b.exists() && Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert!(store_foo_in_b.exists());
            assert_eq!(hash, hash::digest(std::fs::read_to_string(store_foo_in_b)?));

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
