//#![allow(dead_code)] // work in progress

use super::blob::DiskUsage;
use super::db::ArenaDatabase;
use crate::StorageError;
use crate::arena::blob::BlobReadOperations;
use crate::config::{BytesOrPercent, DiskUsageLimits};
use std::cmp::min;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Enforce these limits on the given database.
pub(crate) async fn run_loop(
    db: Arc<ArenaDatabase>,
    limits: DiskUsageLimits,
    shutdown: CancellationToken,
) {
    let mut rx = db.blobs().watch_disk_usage();

    loop {
        let usage = rx.borrow_and_update().clone();
        if let Err(err) = check_limits_async(&db, &limits, usage).await {
            log::warn!(
                "[{}] Failed to enforce disk usage limits: {err:?}",
                db.arena()
            )
        }
        tokio::select!(
        _ = shutdown.cancelled() => { return; }
        ret = rx.changed() => {
            match ret {
                Err(_) => return,
                Ok(_) => continue,
            }
        });
    }
}

async fn check_limits_async(
    db: &Arc<ArenaDatabase>,
    limits: &DiskUsageLimits,
    usage: DiskUsage,
) -> Result<(), StorageError> {
    tokio::task::spawn_blocking({
        let db = db.clone();
        let limits = limits.clone();
        move || {
            let (byte_disk_total, byte_disk_free) = db.blobs().disk_space()?;
            check_limits(&db, &limits, &usage, byte_disk_total, byte_disk_free)
        }
    })
    .await?
}

/// Apply `limits` as necessary by calling
/// [super::blob::WritableOpenBlob::cleanup] as necessary when disk
/// usage has changed.
fn check_limits(
    db: &Arc<ArenaDatabase>,
    limits: &DiskUsageLimits,
    usage: &DiskUsage,
    byte_disk_total: u64,
    byte_disk_free: u64,
) -> Result<(), StorageError> {
    let arena = db.arena();
    let mut byte_limit = limits.max.as_bytes(byte_disk_total);

    // If leave is specified, reduce effective_limit as needed to
    // leave that much disk free.
    if let Some(leave) = &limits.leave {
        let byte_leave = leave.as_bytes(byte_disk_total);
        if byte_leave > byte_disk_free {
            let diff = byte_leave.saturating_sub(byte_disk_free);
            byte_limit = min(byte_limit, usage.total.saturating_sub(diff));
        }
    }

    let tolerance = std::cmp::max(
        1024 * 1024,      // 1MB
        byte_limit / 100, // 1%
    );

    if usage.total >= byte_limit + tolerance {
        // Calculate target for cleanup. The target should be
        // effective_limit - tolerance, but we need to account for
        // non-evictable usage.
        let evictable_target = byte_limit
            .saturating_sub(tolerance)
            .saturating_sub(usage.non_evictable());

        if evictable_target < usage.evictable {
            log::debug!(
                "[{arena}] Will cleanup {} evictable bytes out of {} to target {evictable_target} bytes",
                usage.evictable,
                usage.total,
            );

            // Perform cleanup
            let txn = db.begin_write()?;
            {
                let mut blobs = txn.write_blobs()?;
                let mut tree = txn.write_tree()?;
                blobs.cleanup(&mut tree, evictable_target)?;

                log::info!(
                    "[{arena}] Usage went from {usage:?} to {:?} after cleanup",
                    blobs.disk_usage()?
                );
            };
            txn.commit()?;
        }
    }

    Ok(())
}

/// Calculate the actual limit in bytes based on BytesOrPercent
impl BytesOrPercent {
    fn as_bytes(&self, total_space: u64) -> u64 {
        match self {
            BytesOrPercent::Bytes(bytes) => *bytes,
            BytesOrPercent::Percent(percent) => total_space * (*percent as u64) / 100,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arena::blob::{BlobExt, BlobReadOperations};
    use crate::arena::db::{ArenaReadTransaction, ArenaWriteTransaction};
    use crate::utils::hash;
    use crate::{Blob, Mark};
    use assert_fs::prelude::PathCreateDir;
    use assert_fs::{TempDir, fixture::PathChild};
    use realize_types::{Arena, Path};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    const MB: u64 = 1024 * 1024;
    const GB: u64 = 1024 * MB;
    const TEST_BYTE_TOTAL: u64 = 100 * GB;
    const TEST_BYTE_FREE: u64 = 50 * GB;

    fn test_arena() -> Arena {
        Arena::from("test_arena")
    }

    struct Fixture {
        db: Arc<ArenaDatabase>,
        blob_dir: assert_fs::fixture::ChildPath,
        _tempdir: TempDir,
    }

    impl Fixture {
        fn setup() -> anyhow::Result<Fixture> {
            let _ = env_logger::try_init();
            let arena = test_arena();
            let tempdir = TempDir::new()?;
            let blob_dir = tempdir.child(format!("{arena}/blobs"));
            blob_dir.create_dir_all()?;
            let datadir = tempdir.child(format!("{arena}/data"));
            datadir.create_dir_all()?;
            let db =
                ArenaDatabase::for_testing_single_arena(arena, blob_dir.path(), datadir.path())?;

            Ok(Self {
                db,
                blob_dir,
                _tempdir: tempdir,
            })
        }

        fn begin_read(&self) -> anyhow::Result<ArenaReadTransaction<'_>> {
            Ok(self.db.begin_read()?)
        }

        fn begin_write(&self) -> anyhow::Result<ArenaWriteTransaction<'_>> {
            Ok(self.db.begin_write()?)
        }

        fn create_blob_with_data<'b, L: Into<crate::arena::tree::TreeLoc<'b>>>(
            &self,
            loc: L,
            test_data: String,
        ) -> anyhow::Result<crate::arena::blob::BlobInfo> {
            let hash = hash::digest(&test_data);

            let txn = self.begin_write()?;
            let info: crate::arena::blob::BlobInfo;
            {
                let marks = txn.read_marks()?;
                let mut blobs = txn.write_blobs()?;
                let mut tree = txn.write_tree()?;

                info = blobs.create(&mut tree, &marks, loc, &hash, test_data.len() as u64)?;

                // Write data to the blob file
                let blob_path = self.blob_dir.child(info.pathid.hex());
                std::fs::write(blob_path.path(), &test_data)?;
                blobs.extend_local_availability(
                    &tree,
                    info.pathid,
                    &hash,
                    &realize_types::ByteRanges::single(0, test_data.len() as u64),
                )?;
            }
            txn.commit()?;

            Ok(info)
        }

        fn get_disk_usage(&self) -> anyhow::Result<DiskUsage> {
            let txn = self.begin_read()?;
            let blobs = txn.read_blobs()?;
            Ok(blobs.disk_usage()?)
        }
    }

    #[test]
    fn test_check_limits_no_cleanup_needed() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        // Create a small blob that won't exceed limits
        fixture.create_blob_with_data(Path::parse("small.txt")?, "small data".to_string())?;

        let usage = fixture.get_disk_usage()?;
        let limits = DiskUsageLimits::max_bytes(1 * GB);

        // Should not trigger cleanup
        check_limits(
            &fixture.db,
            &limits,
            &usage,
            TEST_BYTE_TOTAL,
            TEST_BYTE_FREE,
        )?;

        // Verify blob still exists
        let txn = fixture.begin_read()?;
        let blobs = txn.read_blobs()?;
        let tree = txn.read_tree()?;
        assert!(blobs.get(&tree, Path::parse("small.txt")?)?.is_some());

        Ok(())
    }

    #[test]
    fn test_check_limits_cleanup_triggered() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        // Create multiple blobs to exceed the limit
        for i in 0..5 {
            let path = Path::parse(format!("blob{}.txt", i))?;
            // Use a static string that's large enough to trigger cleanup
            let data = "x".repeat(1 * MB as usize);
            fixture.create_blob_with_data(&path, data)?;
        }

        let usage = fixture.get_disk_usage()?;

        let limits = DiskUsageLimits::max_bytes(2 * MB);

        // Should trigger cleanup
        check_limits(
            &fixture.db,
            &limits,
            &usage,
            TEST_BYTE_TOTAL,
            TEST_BYTE_FREE,
        )?;

        // Verify some blobs were cleaned up
        let txn = fixture.begin_read()?;
        let blobs = txn.read_blobs()?;
        let tree = txn.read_tree()?;

        let remaining_blobs = (0..5)
            .filter_map(|i| {
                let path = Path::parse(format!("blob{}.txt", i)).ok()?;
                blobs.get(&tree, &path).ok()?
            })
            .count();

        // Should have fewer blobs after cleanup
        assert!(remaining_blobs < 5);

        Ok(())
    }

    #[test]
    fn test_check_limits_percentage_based() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        // Create a blob
        fixture.create_blob_with_data(Path::parse("test.txt")?, "test data".to_string())?;

        let usage = fixture.get_disk_usage()?;
        let limits = DiskUsageLimits::max_percent(50);

        // Should not fail
        check_limits(
            &fixture.db,
            &limits,
            &usage,
            TEST_BYTE_TOTAL,
            TEST_BYTE_FREE,
        )?;

        Ok(())
    }

    #[test]
    fn test_check_limits_protected_blobs_preserved() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        // Create protected and unprotected blobs
        let protected_path = Path::parse("protected.txt")?;
        let unprotected_path = Path::parse("unprotected.txt")?;

        // Use smaller data to avoid lifetime issues
        fixture.create_blob_with_data(
            &protected_path,
            "protected data that is long enough to be meaningful".to_string(),
        )?;
        fixture.create_blob_with_data(
            &unprotected_path,
            "unprotected data that is long enough to be meaningful".to_string(),
        )?;

        // Mark protected blob as protected
        let txn = fixture.begin_write()?;
        {
            let mut marks = txn.write_marks()?;
            let mut tree = txn.write_tree()?;
            let mut dirty = txn.write_dirty()?;
            marks.set(&mut tree, &mut dirty, &protected_path, Mark::Keep)?;

            let mut blobs = txn.write_blobs()?;
            blobs.set_protected(&tree, &mut dirty, &protected_path, true)?;
        }
        txn.commit()?;

        let usage = fixture.get_disk_usage()?;
        let limits = DiskUsageLimits::max_bytes(1 * MB);

        // Should trigger cleanup
        check_limits(
            &fixture.db,
            &limits,
            &usage,
            TEST_BYTE_TOTAL,
            TEST_BYTE_FREE,
        )?;

        // Verify protected blob still exists, unprotected may be gone
        let txn = fixture.begin_read()?;
        let blobs = txn.read_blobs()?;
        let tree = txn.read_tree()?;

        // Protected blob should still exist
        assert!(blobs.get(&tree, &protected_path)?.is_some());

        Ok(())
    }

    #[test]
    fn test_calculate_limit_bytes() -> anyhow::Result<()> {
        assert_eq!(BytesOrPercent::Bytes(0).as_bytes(100 * GB), 0);
        assert_eq!(BytesOrPercent::Bytes(1 * MB).as_bytes(100 * GB), 1 * MB);
        assert_eq!(BytesOrPercent::Bytes(2 * MB).as_bytes(100 * GB), 2 * MB);
        Ok(())
    }

    #[test]
    fn test_calculate_limit_percent() -> anyhow::Result<()> {
        assert_eq!(BytesOrPercent::Percent(0).as_bytes(100 * GB), 0);
        assert_eq!(BytesOrPercent::Percent(50).as_bytes(100 * GB), 50 * GB);
        assert_eq!(BytesOrPercent::Percent(25).as_bytes(100 * GB), 25 * GB);
        assert_eq!(BytesOrPercent::Percent(110).as_bytes(100 * GB), 110 * GB);
        Ok(())
    }

    #[test]
    fn test_check_limits_with_leave_parameter_in_effect() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let path = Path::parse("large.txt")?;

        fixture.create_blob_with_data(&path, "x".repeat(5 * MB as usize))?;

        let usage = fixture.get_disk_usage()?;

        // Set a limit that would normally allow the blob, but with a
        // leave parameter that might require keeping more free space
        let limits = DiskUsageLimits {
            max: BytesOrPercent::Bytes(10 * MB),
            leave: Some(BytesOrPercent::Bytes(55 * GB)), // More than available free space
        };

        // there's enough free space for now
        check_limits(&fixture.db, &limits, &usage, TEST_BYTE_TOTAL, 60 * GB)?;

        // blob is still available
        {
            let txn = fixture.begin_read()?;
            let blobs = txn.read_blobs()?;
            let tree = txn.read_tree()?;
            assert!(blobs.get(&tree, &path)?.is_some());
        }

        // not enough free space anymore
        check_limits(&fixture.db, &limits, &usage, TEST_BYTE_TOTAL, 45 * GB)?;

        // blob was cleaned up
        {
            let txn = fixture.begin_read()?;
            let blobs = txn.read_blobs()?;
            let tree = txn.read_tree()?;
            assert_eq!(0, blobs.disk_usage()?.evictable);
            assert!(blobs.get(&tree, &path)?.is_none());
        }

        Ok(())
    }

    #[test]
    fn test_check_limits_with_leave_percentage() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        // Create a blob
        fixture.create_blob_with_data(Path::parse("test.txt")?, "x".repeat(1 * MB as usize))?;

        let usage = fixture.get_disk_usage()?;

        // Set a limit with leave as percentage
        let limits = DiskUsageLimits {
            max: BytesOrPercent::Bytes(10 * MB),
            leave: Some(BytesOrPercent::Percent(60)), // Leave 60% free (60GB out of 100GB)
        };

        // Should trigger cleanup because we need to leave 60GB free but only have 50GB
        check_limits(
            &fixture.db,
            &limits,
            &usage,
            TEST_BYTE_TOTAL,
            TEST_BYTE_FREE, // 50GB free, but we need to leave 60GB
        )?;

        // Verify blob was cleaned up
        let txn = fixture.begin_read()?;
        let blobs = txn.read_blobs()?;
        let tree = txn.read_tree()?;
        assert!(blobs.get(&tree, Path::parse("test.txt")?)?.is_none());

        Ok(())
    }

    #[test]
    fn test_check_limits_tolerance_calculation() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        // Create a blob that's just under the tolerance threshold
        fixture.create_blob_with_data(Path::parse("test.txt")?, "x".repeat(1 * MB as usize))?;

        let usage = fixture.get_disk_usage()?;

        // Set a limit that's just above current usage but within tolerance
        let limits = DiskUsageLimits::max_bytes(usage.total + 500 * 1024); // Just above usage

        // Should not trigger cleanup because it's within tolerance
        check_limits(
            &fixture.db,
            &limits,
            &usage,
            TEST_BYTE_TOTAL,
            TEST_BYTE_FREE,
        )?;

        // Verify blob still exists
        let txn = fixture.begin_read()?;
        let blobs = txn.read_blobs()?;
        let tree = txn.read_tree()?;
        assert!(blobs.get(&tree, Path::parse("test.txt")?)?.is_some());

        Ok(())
    }

    #[test]
    fn test_check_limits_all_protected_blobs() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        // Create only protected blobs
        let protected_path = Path::parse("protected.txt")?;
        fixture.create_blob_with_data(&protected_path, "x".repeat(5 * MB as usize))?;

        // Mark as protected
        let txn = fixture.begin_write()?;
        {
            let mut marks = txn.write_marks()?;
            let mut tree = txn.write_tree()?;
            let mut dirty = txn.write_dirty()?;
            marks.set(&mut tree, &mut dirty, &protected_path, Mark::Keep)?;

            let mut blobs = txn.write_blobs()?;
            blobs.set_protected(&tree, &mut dirty, &protected_path, true)?;
        }
        txn.commit()?;

        let usage = fixture.get_disk_usage()?;
        let limits = DiskUsageLimits::max_bytes(1 * MB);

        // Should not trigger cleanup because all blobs are protected (non-evictable)
        check_limits(
            &fixture.db,
            &limits,
            &usage,
            TEST_BYTE_TOTAL,
            TEST_BYTE_FREE,
        )?;

        // Verify protected blob still exists
        let txn = fixture.begin_read()?;
        let blobs = txn.read_blobs()?;
        let tree = txn.read_tree()?;
        assert!(blobs.get(&tree, &protected_path)?.is_some());

        Ok(())
    }

    #[test]
    fn test_check_limits_edge_case_zero_limits() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        // Create a blob
        fixture.create_blob_with_data(Path::parse("test.txt")?, "x".repeat(2 * MB as usize))?;

        let usage = fixture.get_disk_usage()?;

        let limits = DiskUsageLimits::max_bytes(0);

        // Should trigger cleanup to get under the limit
        check_limits(
            &fixture.db,
            &limits,
            &usage,
            TEST_BYTE_TOTAL,
            TEST_BYTE_FREE,
        )?;

        // Verify blob was cleaned up
        let txn = fixture.begin_read()?;
        let blobs = txn.read_blobs()?;
        let tree = txn.read_tree()?;
        assert!(blobs.get(&tree, Path::parse("test.txt")?)?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup_after_blobs_dropped() -> anyhow::Result<()> {
        let path1 = Path::parse("1")?;
        let path2 = Path::parse("2")?;
        let fixture = Fixture::setup()?;

        fixture.create_blob_with_data(&path1, "x".repeat(2 * MB as usize))?;
        fixture.create_blob_with_data(&path2, "y".repeat(2 * MB as usize))?;

        let blob1 = Blob::open(&fixture.db, &path1)?;
        let blob2 = Blob::open(&fixture.db, &path2)?;

        let limits = DiskUsageLimits::max_bytes(0);
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn({
            let db = Arc::clone(&fixture.db);
            let shutdown = shutdown.clone();
            async move { run_loop(db, limits, shutdown).await }
        });

        // make sure the blobs still exist
        let blob1_2 = Blob::open(&fixture.db, &path1);
        let blob2_2 = Blob::open(&fixture.db, &path2);

        let blobs_exist = || {
            let txn = fixture.db.begin_read()?;
            let tree = txn.read_tree()?;
            let blobs = txn.read_blobs()?;
            Ok::<(bool, bool), StorageError>((
                blobs.get(&tree, &path1)?.is_some(),
                blobs.get(&tree, &path2)?.is_some(),
            ))
        };
        tokio::task::yield_now().await;

        assert_eq!((true, true), blobs_exist()?);

        drop(blob1);
        drop(blob2);
        drop(blob1_2);
        drop(blob2_2);

        // Now that all blobs have been dropped, cleanup should run
        // and delete them.
        let limit = Instant::now() + Duration::from_secs(3);
        while blobs_exist()? != (false, false) && Instant::now() < limit {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert_eq!((false, false), blobs_exist()?);

        shutdown.cancel();
        handle.await?;
        Ok(())
    }
}
