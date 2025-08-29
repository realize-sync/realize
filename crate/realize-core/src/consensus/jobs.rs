use super::progress::ByteCountProgress;
use super::types::JobAction;
use crate::rpc::HouseholdOperationError;
use crate::rpc::{ExecutionMode, Household};
use fast_rsync::ApplyError;
use futures::StreamExt;
use realize_storage::{Inode, JobStatus, LocalAvailability, Storage, StorageError};
use realize_types::{Arena, ByteRanges, Hash, Path, Peer, Signature};
use std::io::SeekFrom;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

/// Maximum byterange to sync with rsync. This is also the worst-case
/// size of the Delta to send back, so must be something that fits
/// reasonably well into one message without slowing everything down.
const RSYNC_BLOCK_SIZE: usize = 32 * 1024; // 32K

/// Interval at which to update the database during downloads, in
/// bytes.
const UPDATE_DB_INTERVAL_BYTES: u64 = 4 * 1024 * 1024; // 4M

#[derive(Debug, thiserror::Error)]
pub(crate) enum JobError {
    #[error("{0}")]
    Storage(#[from] StorageError),

    #[error("{0}")]
    Household(#[from] HouseholdOperationError),

    #[error("I/O {0}")]
    Io(#[from] std::io::Error),

    #[error("Rsync error")]
    Rsync(#[from] ApplyError),

    #[error("Hashes inconsistent after repair")]
    InconsistentHash,
}

/// Make a local copy of a specific version of a remote file.
///
/// Executes a [realize_storage::Job::Download]
pub(crate) async fn download(
    storage: &Arc<Storage>,
    household: &Arc<Household>,
    arena: Arena,
    path: &Path,
    hash: &Hash,
    progress: &mut impl ByteCountProgress,
    shutdown: CancellationToken,
) -> Result<JobStatus, JobError> {
    let cache = storage.cache();
    let inode = match cache.expect(arena, path).await {
        Err(StorageError::NotFound) => {
            return Ok(JobStatus::Abandoned("not in cache"));
        }
        Err(err) => {
            return Err(err.into());
        }
        Ok(inode) => inode,
    };
    match cache.local_availability(inode).await {
        Err(StorageError::NotFound) => {
            return Ok(JobStatus::Abandoned("not in cache"));
        }
        Err(err) => {
            return Err(err.into());
        }
        Ok(LocalAvailability::Verified) => {
            return Ok(JobStatus::Done);
        }

        Ok(LocalAvailability::Complete) => {
            return verify(
                storage, household, arena, path, inode, hash, progress, shutdown,
            )
            .await;
        }

        Ok(LocalAvailability::Missing) | Ok(LocalAvailability::Partial(_, _)) => {
            let peers = storage.cache().file_availability(inode).await?.peers;
            if peers.is_empty() {
                return Ok(JobStatus::Abandoned("no peer has it"));
            }

            let mut blob = storage.cache().open_file(inode).await?;
            if *blob.hash() != *hash {
                return Ok(JobStatus::Abandoned("Blob.hash is wrong"));
            }

            let res = write_to_blob(
                household,
                arena,
                path,
                peers,
                &mut blob,
                progress,
                shutdown.clone(),
            )
            .await;
            // Update the database even in the case of errors, to keep
            // whatever we could write before the error happened.
            blob.update_db().await?;

            match res? {
                JobStatus::Done => {}
                other => {
                    return Ok(other);
                }
            }

            drop(blob); // Make sure the file is closed before verifying

            return verify(
                storage, household, arena, path, inode, hash, progress, shutdown,
            )
            .await;
        }
    }
}

/// Read file data from `peers` into `blob`.
async fn write_to_blob(
    household: &Arc<Household>,
    arena: Arena,
    path: &Path,
    peers: Vec<Peer>,
    blob: &mut realize_storage::Blob,
    progress: &mut impl ByteCountProgress,
    shutdown: CancellationToken,
) -> Result<JobStatus, JobError> {
    let missing = ByteRanges::single(0, blob.size()).subtraction(blob.local_availability());
    if missing.is_empty() {
        return Ok(JobStatus::Done);
    }
    let total_bytes = missing.bytecount();
    let mut current_bytes: u64 = 0;
    let mut last_update_bytes = 0;
    progress.update_action(JobAction::Download);
    progress.update(0, total_bytes);

    for range in missing {
        let mut stream = household.read(
            peers.clone(),
            ExecutionMode::Batch,
            arena,
            path.clone(),
            range.start,
            Some(range.bytecount()),
        )?;

        while let Some(chunk) = tokio::select!(
            res = stream.next() => {res}
            _ = shutdown.cancelled() => {
                return Ok(JobStatus::Cancelled);
            }
        ) {
            let (chunk_offset, chunk) = chunk?;
            if chunk_offset != blob.offset() {
                blob.seek(SeekFrom::Start(chunk_offset)).await?;
            }
            blob.write_all(&chunk).await?;

            current_bytes += chunk.len() as u64;
            progress.update(current_bytes, total_bytes);

            if current_bytes - last_update_bytes >= UPDATE_DB_INTERVAL_BYTES {
                let _ = blob.update_db().await;
                last_update_bytes = current_bytes;
            }
        }
    }

    Ok(JobStatus::Done)
}

/// Check blob content against hash, repair it if necessary.
///
/// This call will fail if the file is incomplete. Call download()
/// first if necessary.
pub(crate) async fn verify(
    storage: &Arc<Storage>,
    household: &Arc<Household>,
    arena: Arena,
    path: &Path,
    inode: Inode,
    hash: &Hash,
    progress: &mut impl ByteCountProgress,
    shutdown: CancellationToken,
) -> Result<JobStatus, JobError> {
    let mut blob = storage.cache().open_file(inode).await?;
    if *blob.hash() != *hash {
        return Ok(JobStatus::Abandoned("Blob.hash mismatch"));
    }

    progress.update_action(JobAction::Verify);
    let content_hash = tokio::select!(
    res = blob.compute_hash() => { res? },
    _ = shutdown.cancelled() => {
        return Ok(JobStatus::Cancelled);
    });
    if content_hash == *hash {
        blob.mark_verified().await?;
        log::debug!("[{arena}] Verified \"{path}\" against {hash}");
        return Ok(JobStatus::Done);
    }
    log::debug!(
        "[{arena}] Inconsistent hash for \"{path}\"; Repairing. Expected {hash}, got {content_hash}."
    );

    // repair
    progress.update_action(JobAction::Repair);
    let peers = storage.cache().file_availability(inode).await?.peers;
    if peers.is_empty() {
        return Ok(JobStatus::Abandoned("not available anywhere"));
    }
    let opts = fast_rsync::SignatureOptions {
        block_size: 4 * 1024 as u32,
        crypto_hash_size: 8,
    };

    let size = blob.size();
    progress.update(0, size);
    let mut buf = vec![0; RSYNC_BLOCK_SIZE];
    let mut fixed_buf = Vec::with_capacity(RSYNC_BLOCK_SIZE);
    for range in ByteRanges::single(0, size).chunked(RSYNC_BLOCK_SIZE as u64) {
        let range_len = range.bytecount() as usize;
        let limited_buf = &mut buf[0..range_len];
        blob.seek(SeekFrom::Start(range.start)).await?;
        blob.read_exact(limited_buf).await?;
        assert_eq!(range_len, limited_buf.len());

        let sig = Signature(fast_rsync::Signature::calculate(limited_buf, opts).into_serialized());
        let delta = tokio::select!(
            res = household.rsync(peers.clone(), ExecutionMode::Batch, arena, path, &range, sig) => {res?},
            _ = shutdown.cancelled() => {
                return Ok(JobStatus::Cancelled);
            }
        );
        fixed_buf.clear();
        fast_rsync::apply_limited(limited_buf, delta.0.as_slice(), &mut fixed_buf, range_len)?;
        assert_eq!(range_len, fixed_buf.len());
        blob.seek(SeekFrom::Start(range.start)).await?;
        blob.write_all(fixed_buf.as_slice()).await?;

        progress.update(range.end, size);
    }
    blob.flush_and_sync().await?;

    progress.update_action(JobAction::Verify);
    let content_hash = tokio::select!(
    res = blob.compute_hash() => { res? },
    _ = shutdown.cancelled() => {
        return Ok(JobStatus::Cancelled);
    });
    if content_hash != *hash {
        log::debug!(
            "[{arena}] Inconsistent hash after repair for \"{path}\"; Giving up. Expected {hash}, got {content_hash}"
        );
        return Err(JobError::InconsistentHash);
    }
    blob.mark_verified().await?;
    log::debug!("[{arena}] Fixed and verified \"{path}\" {hash}");

    Ok(JobStatus::Done)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::progress::testing::{NoOpByteCountProgress, SimpleByteCountProgress};
    use crate::rpc::testing::{self, HouseholdFixture};
    use rand::rngs::SmallRng;
    use rand::{RngCore, SeedableRng};
    use realize_storage::Blob;
    use realize_storage::utils::hash;
    use tokio::fs::File;
    use tokio::io::{AsyncWrite, BufReader, BufWriter};
    use tokio::{fs, io::AsyncReadExt};

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
        ) -> anyhow::Result<(Path, Hash)> {
            let root = self.inner.arena_root(peer);
            let path = Path::parse(path_str)?;
            let realpath = path.within(&root);
            fs::write(realpath, content).await?;

            Ok((path, hash::digest(content)))
        }

        async fn write_large_file(
            &self,
            peer: Peer,
            path_str: &str,
            seed: u64,
            size_kb: u64,
        ) -> anyhow::Result<(Path, Hash)> {
            let root = self.inner.arena_root(peer);
            let path = Path::parse(path_str)?;
            let realpath = path.within(&root);

            let mut file = File::create(realpath).await?;
            let hash = write_random_data(&mut file, seed, size_kb).await?;

            Ok((path, hash))
        }

        async fn hash_blob(&self, file: Blob) -> anyhow::Result<Hash> {
            let mut reader = BufReader::new(file);
            let mut hasher = hash::running();
            let mut buf = vec![0; 8 * 1024];
            loop {
                let n = reader.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[0..n])
            }

            Ok(hasher.finalize())
        }

        async fn open_file(&self, peer: Peer, path_str: &str) -> anyhow::Result<Blob> {
            let cache = self.inner.cache(peer)?;
            let inode = cache
                .expect(HouseholdFixture::test_arena(), &Path::parse(path_str)?)
                .await?;

            Ok(cache.open_file(inode).await?)
        }

        async fn set_blob_content(
            &self,
            peer: Peer,
            path_str: &str,
            content: &str,
        ) -> anyhow::Result<()> {
            let mut blob = self.inner.open_file(peer, path_str).await?;
            blob.write_all(content.as_bytes()).await?;
            blob.update_db().await?;

            Ok(())
        }

        async fn get_blob_content_as_string(
            &self,
            peer: Peer,
            path_str: &str,
        ) -> anyhow::Result<String> {
            let mut blob = self.inner.open_file(peer, path_str).await?;
            let mut buf = String::new();
            blob.read_to_string(&mut buf).await?;

            Ok(buf)
        }

        async fn local_availability(
            &self,
            peer: Peer,
            path_str: &str,
        ) -> anyhow::Result<LocalAvailability> {
            let cache = self.inner.cache(peer)?;
            let inode = cache
                .expect(HouseholdFixture::test_arena(), &Path::parse(path_str)?)
                .await?;

            Ok(cache.local_availability(inode).await?)
        }
    }

    async fn write_random_data(
        out: impl AsyncWrite,
        seed: u64,
        size_kb: u64,
    ) -> Result<Hash, anyhow::Error> {
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut out = std::pin::pin!(BufWriter::new(out));
        let mut hasher = hash::running();
        let mut bytes = [0; 1024];
        for _ in 0..size_kb {
            rng.fill_bytes(&mut bytes);
            out.write_all(&bytes).await?;
            hasher.update(&bytes);
        }
        out.flush().await?;
        let hash = hasher.finalize();
        Ok(hash)
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

                let (path, hash) = fixture.write_file(b, "foobar", "foo then bar").await?;
                fixture
                    .inner
                    .wait_for_file_in_cache(a, "foobar", &hash)
                    .await?;

                assert_eq!(
                    JobStatus::Done,
                    download(
                        fixture.inner.storage(a)?,
                        &household_a,
                        HouseholdFixture::test_arena(),
                        &path,
                        &hash,
                        &mut NoOpByteCountProgress,
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
    async fn download_zero_length_file() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                testing::connect(&household_a, b).await?;

                let (path, hash) = fixture.write_file(b, "foobar", "").await?;
                fixture
                    .inner
                    .wait_for_file_in_cache(a, "foobar", &hash)
                    .await?;

                let mut progress = SimpleByteCountProgress::new();
                assert_eq!(
                    JobStatus::Done,
                    download(
                        fixture.inner.storage(a)?,
                        &household_a,
                        HouseholdFixture::test_arena(),
                        &path,
                        &hash::digest(""),
                        &mut progress,
                        CancellationToken::new(),
                    )
                    .await?,
                );

                let blob = fixture.open_file(a, "foobar").await?;
                assert!(blob.local_availability().is_empty());

                assert_eq!(
                    LocalAvailability::Verified,
                    fixture.local_availability(a, "foobar").await?
                );

                assert_eq!("", fixture.get_blob_content_as_string(a, "foobar").await?);

                assert_eq!(vec![JobAction::Verify], progress.actions);

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

                let (path, hash) = fixture
                    .write_file(b, "foobar", "baa, baa, black sheep")
                    .await?;
                fixture
                    .inner
                    .wait_for_file_in_cache(a, "foobar", &hash)
                    .await?;

                {
                    let mut blob = fixture.open_file(a, "foobar").await?;
                    blob.write(b"baa, baa").await?;
                    blob.seek(SeekFrom::End(-5)).await?;
                    blob.write(b"sheep").await?;
                    blob.update_db().await?;
                }
                assert_eq!(
                    JobStatus::Done,
                    download(
                        fixture.inner.storage(a)?,
                        &household_a,
                        HouseholdFixture::test_arena(),
                        &path,
                        &hash::digest("baa, baa, black sheep"),
                        &mut NoOpByteCountProgress,
                        CancellationToken::new(),
                    )
                    .await?,
                );

                let mut blob = fixture.open_file(a, "foobar").await?;
                assert_eq!(ByteRanges::single(0, 21), *blob.local_availability());

                let mut buf = String::new();
                blob.read_to_string(&mut buf).await?;

                assert_eq!("baa, baa, black sheep", buf);

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

                let (path, hash) = fixture
                    .write_file(b, "foobar", "baa, baa, black sheep")
                    .await?;
                fixture
                    .inner
                    .wait_for_file_in_cache(a, "foobar", &hash)
                    .await?;

                {
                    let mut blob = fixture.open_file(a, "foobar").await?;
                    blob.seek(SeekFrom::Start(10)).await?;
                    blob.write(b"black").await?;
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
                        &hash::digest("baa, baa, black sheep"),
                        &mut NoOpByteCountProgress,
                        CancellationToken::new(),
                    )
                    .await?,
                );

                let mut blob = fixture.open_file(a, "foobar").await?;
                assert_eq!(ByteRanges::single(0, 21), *blob.local_availability());

                let mut buf = String::new();
                blob.read_to_string(&mut buf).await?;

                assert_eq!("baa, baa, black sheep", buf);

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

                let (path, hash) = fixture.write_file(b, "foobar", "foobar").await?;
                fixture
                    .inner
                    .wait_for_file_in_cache(a, "foobar", &hash)
                    .await?;

                assert!(matches!(
                    download(
                        fixture.inner.storage(a)?,
                        &household_a,
                        HouseholdFixture::test_arena(),
                        &path,
                        &hash::digest("barfoo"), // mismatch
                        &mut NoOpByteCountProgress,
                        CancellationToken::new(),
                    )
                    .await?,
                    JobStatus::Abandoned(_),
                ));

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

                assert!(matches!(
                    download(
                        fixture.inner.storage(a)?,
                        &household_a,
                        HouseholdFixture::test_arena(),
                        &Path::parse("doesnotexist")?,
                        &hash::digest(""),
                        &mut NoOpByteCountProgress,
                        CancellationToken::new(),
                    )
                    .await?,
                    JobStatus::Abandoned(_)
                ));

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

                let (path, hash) = fixture.write_file(b, "foobar", "foobar").await?;
                fixture
                    .inner
                    .wait_for_file_in_cache(a, "foobar", &hash)
                    .await?;

                let cancelled_token = CancellationToken::new();
                cancelled_token.cancel();

                assert_eq!(
                    JobStatus::Cancelled,
                    download(
                        fixture.inner.storage(a)?,
                        &household_a,
                        HouseholdFixture::test_arena(),
                        &path,
                        &hash,
                        &mut NoOpByteCountProgress,
                        cancelled_token,
                    )
                    .await?
                );

                let blob = fixture.open_file(a, "foobar").await?;
                assert!(blob.local_availability().is_empty());

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn download_progress_no_action_when_already_complete() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                testing::connect(&household_a, b).await?;

                let (path, hash) = fixture
                    .write_file(b, "foobar", "baa, baa, black sheep")
                    .await?;
                fixture
                    .inner
                    .wait_for_file_in_cache(a, "foobar", &hash)
                    .await?;

                // Write the complete file content
                {
                    let mut blob = fixture.open_file(a, "foobar").await?;
                    blob.write(b"baa, baa, black sheep").await?;
                    blob.update_db().await?;
                }

                let mut progress = SimpleByteCountProgress::new();
                assert_eq!(
                    JobStatus::Done,
                    download(
                        fixture.inner.storage(a)?,
                        &household_a,
                        HouseholdFixture::test_arena(),
                        &path,
                        &hash,
                        &mut progress,
                        CancellationToken::new(),
                    )
                    .await?,
                );

                assert_eq!(vec![JobAction::Verify], progress.actions);
                assert_eq!(0, progress.current_bytes);
                assert_eq!(0, progress.total_bytes);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn download_progress() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                testing::connect(&household_a, b).await?;

                let (path, hash) = fixture
                    .write_file(b, "foobar", "baa, baa, black sheep")
                    .await?;
                fixture
                    .inner
                    .wait_for_file_in_cache(a, "foobar", &hash)
                    .await?;

                {
                    let mut blob = fixture.open_file(a, "foobar").await?;
                    blob.write(b"baa").await?;
                    blob.update_db().await?;
                }
                let mut progress = SimpleByteCountProgress::new();
                assert_eq!(
                    JobStatus::Done,
                    download(
                        fixture.inner.storage(a)?,
                        &household_a,
                        HouseholdFixture::test_arena(),
                        &path,
                        &hash,
                        &mut progress,
                        CancellationToken::new(),
                    )
                    .await?,
                );
                // 18 = "baa, baa, black sheep".len() - 3 already written
                assert_eq!(18, progress.current_bytes);
                assert_eq!(18, progress.total_bytes);

                // Verify that update_action was called
                assert_eq!(
                    vec![JobAction::Download, JobAction::Verify],
                    progress.actions
                );

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn fix_invalid_content() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let arena = HouseholdFixture::test_arena();
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                testing::connect(&household_a, b).await?;

                let (foobar, hash) = fixture.inner.write_file(b, "foobar", "foo & bar").await?;
                fixture
                    .inner
                    .wait_for_file_in_cache(a, "foobar", &hash)
                    .await?;
                fixture.set_blob_content(a, "foobar", "barbatruc").await?;

                let mut progress = SimpleByteCountProgress::new();
                assert_eq!(
                    JobStatus::Done,
                    download(
                        fixture.inner.storage(a)?,
                        &household_a,
                        arena,
                        &foobar,
                        &hash,
                        &mut progress,
                        CancellationToken::new(),
                    )
                    .await?
                );

                assert_eq!(
                    LocalAvailability::Verified,
                    fixture.local_availability(a, "foobar").await?
                );

                assert_eq!(
                    "foo & bar".to_string(),
                    fixture.get_blob_content_as_string(a, "foobar").await?
                );

                assert_eq!(
                    vec![JobAction::Verify, JobAction::Repair, JobAction::Verify],
                    progress.actions
                );

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn complete_and_fix_partial_content() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let arena = HouseholdFixture::test_arena();
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                testing::connect(&household_a, b).await?;

                let (foobar, hash) = fixture.inner.write_file(b, "foobar", "foo & bar").await?;
                fixture
                    .inner
                    .wait_for_file_in_cache(a, "foobar", &hash)
                    .await?;
                fixture.set_blob_content(a, "foobar", "boo &").await?;

                let mut progress = SimpleByteCountProgress::new();
                assert_eq!(
                    JobStatus::Done,
                    download(
                        fixture.inner.storage(a)?,
                        &household_a,
                        arena,
                        &foobar,
                        &hash,
                        &mut progress,
                        CancellationToken::new(),
                    )
                    .await?
                );

                assert_eq!(
                    LocalAvailability::Verified,
                    fixture.local_availability(a, "foobar").await?
                );

                assert_eq!(
                    "foo & bar".to_string(),
                    fixture.get_blob_content_as_string(a, "foobar").await?
                );

                assert_eq!(
                    vec![
                        JobAction::Download,
                        JobAction::Verify,
                        JobAction::Repair,
                        JobAction::Verify
                    ],
                    progress.actions
                );

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn download_large_file() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                testing::connect(&household_a, b).await?;

                let (path, hash) = fixture.write_large_file(b, "large", 433, 1024).await?;
                fixture
                    .inner
                    .wait_for_file_in_cache(a, "large", &hash)
                    .await?;

                assert_eq!(
                    JobStatus::Done,
                    download(
                        fixture.inner.storage(a)?,
                        &household_a,
                        HouseholdFixture::test_arena(),
                        &path,
                        &hash,
                        &mut NoOpByteCountProgress,
                        CancellationToken::new(),
                    )
                    .await?,
                );

                assert_eq!(
                    LocalAvailability::Verified,
                    fixture.local_availability(a, "large").await?
                );

                let blob = fixture.open_file(a, "large").await?;
                assert_eq!(hash, *blob.hash());
                assert_eq!(hash, fixture.hash_blob(blob).await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn repair_large_file() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                testing::connect(&household_a, b).await?;

                let (path, hash) = fixture.write_large_file(b, "large", 415, 1024).await?;
                fixture
                    .inner
                    .wait_for_file_in_cache(a, "large", &hash)
                    .await?;

                // Fill the blob for 'large' with data. Download will
                // have to repair it to get to the correct hash.
                let mut blob = fixture.open_file(a, "large").await?;
                // Same data as in the file (generated from the same seed)
                let hash2 = write_random_data(&mut blob, 415, 1024).await?;
                assert_eq!(hash, hash2); // same seed,same data
                blob.seek(SeekFrom::Start(3 * 1024)).await?;
                // 16k of bad data.
                write_random_data(&mut blob, 512, 16).await?;
                blob.update_db().await?;

                let mut progress = SimpleByteCountProgress::new();
                assert_eq!(
                    JobStatus::Done,
                    download(
                        fixture.inner.storage(a)?,
                        &household_a,
                        HouseholdFixture::test_arena(),
                        &path,
                        &hash,
                        &mut progress,
                        CancellationToken::new(),
                    )
                    .await?,
                );

                // file was repaired
                assert_eq!(
                    vec![JobAction::Verify, JobAction::Repair, JobAction::Verify],
                    progress.actions
                );

                // repair was successful
                assert_eq!(
                    LocalAvailability::Verified,
                    fixture.local_availability(a, "large").await?
                );

                // check the content again
                let blob = fixture.open_file(a, "large").await?;
                assert_eq!(hash, *blob.hash());
                assert_eq!(hash, fixture.hash_blob(blob).await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }
}
