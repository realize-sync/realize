use std::time::Instant;
use std::{time::Duration};
use std::sync::Arc;
use realize_storage::Mark;
use realize_types::Path;
use tokio::io::AsyncReadExt;
use crate::rpc::testing::HouseholdFixture;
use crate::consensus::churten::Churten;

struct Fixture {
    inner: HouseholdFixture,
}

impl Fixture {
    async fn setup() -> anyhow::Result<Self> {
        let _ = env_logger::try_init();
        let household_fixture = HouseholdFixture::setup().await?;

        Ok(Self {
            inner: household_fixture,
        })
    }
}


#[tokio::test]
async fn file_drop() -> anyhow::Result<()> {
    let mut fixture = Fixture::setup().await?;
    fixture
        .inner
        .with_two_peers()
        .await?
        .interconnected()
        .run(async |_household_a, household_b| {
            let a = HouseholdFixture::a();
            let b = HouseholdFixture::b();
            let arena = HouseholdFixture::test_arena();
            let storage_a = fixture.inner.storage(a)?;
            storage_a.set_arena_mark(arena, Mark::Watch).await?;
            let storage_b = fixture.inner.storage(b)?;
            storage_b.set_arena_mark(arena, Mark::Own).await?;

            // write a file to a; it'll get downloaded by b then a's
            // copy will be deleted.
            let mut churten = Churten::new(
                Arc::clone(&storage_b),
                household_b.clone(),
            );
            churten.start();

            let root_a = fixture.inner.arena_root(a);
            let foo = Path::parse("foo")?;
            let foo_a = foo.within(&root_a);
            std::fs::write(&foo_a, "this is foo")?;

            let limit = Instant::now() + Duration::from_secs(5);
            while foo_a.exists() && Instant::now() < limit {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert!(!foo_a.exists());

            let root_b = fixture.inner.arena_root(b);
            let foo_b = foo.within(&root_b);
            assert!(foo_b.exists());

            assert_eq!("this is foo", std::fs::read_to_string(foo_b)?);

            let mut blob = fixture.inner.open_file(a, "foo").await?;
            let mut buf = String::new();
            blob.read_to_string(&mut buf).await?;
            assert_eq!("this is foo".to_string(), buf);

            Ok::<(), anyhow::Error>(())
        })
        .await?;

    Ok(())
}
