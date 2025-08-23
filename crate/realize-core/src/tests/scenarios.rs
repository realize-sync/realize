use std::time::Instant;
use std::{time::Duration};
use std::sync::Arc;
use realize_storage::config::DiskUsageLimits;
use realize_storage::utils::hash;
use realize_storage::{LocalAvailability, Mark};
use realize_types::Path;
use crate::rpc::testing::HouseholdFixture;
use crate::consensus::churten::Churten;

#[tokio::test]
async fn file_drop() -> anyhow::Result<()> {
    let a = HouseholdFixture::a();
    let b = HouseholdFixture::b();
    let arena = HouseholdFixture::test_arena();
    let mut builder = HouseholdFixture::builder();
    builder.config_mut(a).arenas.get_mut(&arena).unwrap().disk_usage = Some(DiskUsageLimits::max_bytes(0));

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
            let mut churten = Churten::new(
                Arc::clone(&storage_b),
                household_b.clone(),
            );
            churten.start();

            let content = b"foo!".repeat(2*1024*1024/4); // 2M
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
            let foo_inode = cache_a.expect(arena, &foo).await?;
            while cache_a.local_availability(foo_inode).await? != LocalAvailability::Missing && Instant::now() < limit {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert_eq!(LocalAvailability::Missing, cache_a.local_availability(foo_inode).await?);

            Ok::<(), anyhow::Error>(())
        })
        .await?;

    Ok(())
}
