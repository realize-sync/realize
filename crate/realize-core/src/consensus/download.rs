use crate::rpc::Household;
use futures::StreamExt;
use realize_storage::{JobStatus, Storage, StorageError};
use realize_types::{Arena, ByteRanges, Hash, Path, Peer};
use std::{io::SeekFrom, sync::Arc};
use tarpc::tokio_util::sync::CancellationToken;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

/// Make a local copy of a specific version of a remote file.
///
/// Executes a [realize_storage::Job::Download]
pub(crate) async fn download(
    storage: &Arc<Storage>,
    household: &Household,
    arena: Arena,
    path: &Path,
    hash: &Hash,
    shutdown: CancellationToken,
) -> anyhow::Result<JobStatus> {
    let inode = match storage.cache().lookup_path(arena, path).await {
        Err(StorageError::NotFound) => {
            return Ok(JobStatus::Abandoned);
        }
        Err(err) => {
            return Err(err.into());
        }
        Ok((inode, _)) => inode,
    };
    let peers = storage.cache().file_availability(inode).await?.peers;
    if peers.is_empty() {
        return Ok(JobStatus::Abandoned);
    }

    let mut blob = storage.cache().open_file(inode).await?;
    if *blob.hash() != *hash {
        return Ok(JobStatus::Abandoned);
    }
    let res = write_to_blob(household, arena, path, peers, &mut blob, shutdown).await;
    // Update the database even in the case of errors, to keep
    // whatever we could write before the error happened.
    blob.update_db().await?;

    // TODO: add data verification

    res.map(|_| JobStatus::Done)
}

/// Read file data from `peers` into `blob`.
async fn write_to_blob(
    household: &Household,
    arena: Arena,
    path: &Path,
    peers: Vec<Peer>,
    blob: &mut realize_storage::Blob,
    shutdown: CancellationToken,
) -> Result<(), anyhow::Error> {
    let missing = ByteRanges::single(0, blob.size()).subtraction(blob.local_availability());
    if missing.is_empty() {
        return Ok(());
    }

    for range in missing {
        let mut stream = household.read(
            peers.clone(),
            arena,
            path.clone(),
            range.start,
            Some(range.bytecount()),
        )?;
        blob.seek(SeekFrom::Start(range.start)).await?;
        while let Some(chunk) = tokio::select!(
            res = stream.next() => {res}
            _ = shutdown.cancelled() => {
                anyhow::bail!("cancelled");
            }
        ) {
            let chunk = chunk?;
            blob.write_all(&chunk).await?;
        }
    }
    // TODO: track bytes written and call update_db at regular
    // intervals, so we don't lose too much data in case the process
    // is interrupted.

    Ok(())
}

#[cfg(test)]
mod tests {
    use realize_storage::{Blob, utils::hash::digest};
    use tokio::{fs, io::AsyncReadExt};

    use crate::rpc::testing::{self, HouseholdFixture};

    use super::*;

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

        async fn write_file(
            &self,
            peer: Peer,
            path_str: &str,
            content: &str,
        ) -> anyhow::Result<Path> {
            let root = self.inner.arena_root(peer);
            let path = Path::parse(path_str)?;
            let realpath = path.within(&root);
            fs::write(realpath, content).await?;

            Ok(path)
        }

        async fn open_file(&self, peer: Peer, path_str: &str) -> anyhow::Result<Blob> {
            let cache = self.inner.cache(peer)?;
            let (inode, _) = cache
                .lookup_path(HouseholdFixture::test_arena(), &Path::parse(path_str)?)
                .await?;

            Ok(cache.open_file(inode).await?)
        }
    }

    #[tokio::test]
    async fn download_from_b() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                testing::connect(&household_a, b).await?;

                let path = fixture.write_file(b, "foobar", "foo then bar").await?;
                fixture.inner.wait_for_file_in_cache(a, "foobar").await?;

                assert_eq!(
                    JobStatus::Done,
                    download(
                        fixture.inner.storage(a)?,
                        &household_a,
                        HouseholdFixture::test_arena(),
                        &path,
                        &digest("foo then bar"),
                        CancellationToken::new(),
                    )
                    .await?,
                );

                let mut blob = fixture.open_file(a, "foobar").await?;
                assert_eq!(ByteRanges::single(0, 12), *blob.local_availability());

                let mut buf = String::new();
                blob.read_to_string(&mut buf).await?;
                assert_eq!("foo then bar", buf);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn download_from_b_partial() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                testing::connect(&household_a, b).await?;

                let path = fixture
                    .write_file(b, "foobar", "baa, baa, black sheep")
                    .await?;
                fixture.inner.wait_for_file_in_cache(a, "foobar").await?;

                {
                    let mut blob = fixture.open_file(a, "foobar").await?;
                    blob.write(b"raa, raa").await?;
                    blob.seek(SeekFrom::End(-5)).await?;
                    blob.write(b"moose").await?;
                    blob.update_db().await?;
                }
                assert_eq!(
                    JobStatus::Done,
                    download(
                        fixture.inner.storage(a)?,
                        &household_a,
                        HouseholdFixture::test_arena(),
                        &path,
                        &digest("baa, baa, black sheep"),
                        CancellationToken::new(),
                    )
                    .await?,
                );

                let mut blob = fixture.open_file(a, "foobar").await?;
                assert_eq!(ByteRanges::single(0, 21), *blob.local_availability());

                let mut buf = String::new();
                blob.read_to_string(&mut buf).await?;

                // The local data was merged with the remote data,
                // resulting in something not quite correct, but
                // that's what's expected from download at this point.
                assert_eq!("raa, raa, black moose", buf);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn download_from_b_disjoint_ranges() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                testing::connect(&household_a, b).await?;

                let path = fixture
                    .write_file(b, "foobar", "baa, baa, black sheep")
                    .await?;
                fixture.inner.wait_for_file_in_cache(a, "foobar").await?;

                {
                    let mut blob = fixture.open_file(a, "foobar").await?;
                    blob.seek(SeekFrom::Start(10)).await?;
                    blob.write(b"green").await?;
                    blob.update_db().await?;
                    // There are two holes to fill, one at the
                    // beginning and one at the end.
                }
                assert_eq!(
                    JobStatus::Done,
                    download(
                        fixture.inner.storage(a)?,
                        &household_a,
                        HouseholdFixture::test_arena(),
                        &path,
                        &digest("baa, baa, black sheep"),
                        CancellationToken::new(),
                    )
                    .await?,
                );

                let mut blob = fixture.open_file(a, "foobar").await?;
                assert_eq!(ByteRanges::single(0, 21), *blob.local_availability());

                let mut buf = String::new();
                blob.read_to_string(&mut buf).await?;

                // The local data was merged with the remote data,
                // resulting in something not quite correct, but
                // that's what's expected from download at this point.
                assert_eq!("baa, baa, green sheep", buf);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn abandon_download_if_hash_mismatch() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                testing::connect(&household_a, b).await?;

                let path = fixture.write_file(b, "foobar", "foobar").await?;
                fixture.inner.wait_for_file_in_cache(a, "foobar").await?;

                assert_eq!(
                    JobStatus::Abandoned,
                    download(
                        fixture.inner.storage(a)?,
                        &household_a,
                        HouseholdFixture::test_arena(),
                        &path,
                        &digest("barfoo"), // mismatch
                        CancellationToken::new(),
                    )
                    .await?,
                );

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn abandon_download_if_not_found() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                testing::connect(&household_a, b).await?;

                assert_eq!(
                    JobStatus::Abandoned,
                    download(
                        fixture.inner.storage(a)?,
                        &household_a,
                        HouseholdFixture::test_arena(),
                        &Path::parse("doesnotexist")?,
                        &digest(""),
                        CancellationToken::new(),
                    )
                    .await?,
                );

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn cancelled() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                testing::connect(&household_a, b).await?;

                let path = fixture.write_file(b, "foobar", "foobar").await?;
                fixture.inner.wait_for_file_in_cache(a, "foobar").await?;

                let cancelled_token = CancellationToken::new();
                cancelled_token.cancel();

                let res = download(
                    fixture.inner.storage(a)?,
                    &household_a,
                    HouseholdFixture::test_arena(),
                    &path,
                    &digest("foobar"),
                    cancelled_token,
                )
                .await;
                assert_eq!(
                    Some("cancelled".to_string()),
                    res.err().map(|err| err.to_string())
                );

                let blob = fixture.open_file(a, "foobar").await?;
                assert!(blob.local_availability().is_empty());

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }
}
