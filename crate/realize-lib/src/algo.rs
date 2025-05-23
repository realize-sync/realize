//! Move algorithm for Realize - Symmetric File Syncer
//!
//! Implements the unoptimized algorithm to move files from source (A) to destination (B)
//! using the RealizeService trait. See spec/design.md for details.

use crate::model::service::{
    DirectoryId, Hash, Options, RealizeError, RealizeServiceClient, RealizeServiceRequest,
    RealizeServiceResponse, SyncedFile,
};
use futures::future;
use futures::future::Either;
use futures::stream::StreamExt as _;
use prometheus::{IntCounter, IntCounterVec, register_int_counter, register_int_counter_vec};
use std::cmp::min;
use std::sync::Arc;
use std::{collections::HashMap, path::Path};
use tarpc::client::stub::Stub;
use thiserror::Error;
use tokio::sync::Semaphore;
const CHUNK_SIZE: u64 = 4 * 1024 * 1024;
const PARALLEL_FILE_COUNT: usize = 8;
const HASH_FILE_CHUNK: u64 = 256 * 1024 * 1024; // 256M
const PARALLEL_FILE_HASH: usize = 4;

lazy_static::lazy_static! {
    pub static ref METRIC_START_COUNT: IntCounter =
        register_int_counter!("realize_move_start_count", "Number of times move_files() was called").unwrap();
    pub static ref METRIC_END_COUNT: IntCounter =
        register_int_counter!("realize_move_end_count", "Number of times move_files() finished").unwrap();
    pub static ref METRIC_FILE_START_COUNT: IntCounter =
        register_int_counter!("realize_move_file_start_count", "Number of files started by move_files").unwrap();
    pub static ref METRIC_FILE_END_COUNT: IntCounterVec =
        register_int_counter_vec!(
            "realize_move_file_end_count",
            "Number of files synced (status is Ok or Inconsistent)",
            &["status"]
        ).unwrap();
    pub static ref METRIC_READ_BYTES: IntCounterVec =
        register_int_counter_vec!(
            "realize_move_read_bytes",
            "Number of bytes read, with method label (read or diff)",
            &["method"]
        ).unwrap();
    pub static ref METRIC_WRITE_BYTES: IntCounterVec =
        register_int_counter_vec!(
            "realize_move_write_bytes",
            "Number of bytes written, with method label (send or apply_patch)",
            &["method"]
        ).unwrap();
    pub static ref METRIC_RANGE_READ_BYTES: IntCounterVec =
        register_int_counter_vec!(
            "realize_move_range_read_bytes",
            "Number of bytes read (range), with method label (read or diff)",
            &["method"]
        ).unwrap();
    pub static ref METRIC_RANGE_WRITE_BYTES: IntCounterVec =
        register_int_counter_vec!(
            "realize_move_range_write_bytes",
            "Number of bytes written (range), with method label (send or apply_patch)",
            &["method"]
        ).unwrap();
}

// Progress instance passed to move_files()
pub trait Progress: Sync + Send {
    /// Once the number of files to move is known, report it
    fn set_length(&mut self, total_files: usize, total_bytes: u64);

    /// Process a file
    fn for_file(&self, path: &std::path::Path, bytes: u64) -> Box<dyn FileProgress>;
}

pub trait FileProgress: Sync + Send {
    /// Checking the hash at the beginning or end
    fn verifying(&mut self);
    /// Comparing and fixing data using the rsync algorithm.
    fn rsyncing(&mut self);
    /// Transferring data
    fn copying(&mut self);
    /// Increment byte count for the file and overall byte count by this amount.
    fn inc(&mut self, bytecount: u64);
    /// File was moved, finished and deleted on the source. File is done, increment file count by 1.
    fn success(&mut self);
    /// Moving the file failed. File is done, increment file count by 1.
    fn error(&mut self, err: &MoveFileError);
}

/// Empty implementation of Progress trait
pub struct NoProgress;

impl Progress for NoProgress {
    fn set_length(&mut self, _total_files: usize, _total_bytes: u64) {}
    fn for_file(&self, _path: &std::path::Path, _bytes: u64) -> Box<dyn FileProgress> {
        Box::new(NoFileProgress)
    }
}

pub(crate) struct NoFileProgress;

impl FileProgress for NoFileProgress {
    fn verifying(&mut self) {}
    fn rsyncing(&mut self) {}
    fn copying(&mut self) {}
    fn inc(&mut self, _bytecount: u64) {}
    fn success(&mut self) {}
    fn error(&mut self, _err: &MoveFileError) {}
}

/// Options used for RPC calls on the source.
fn src_options() -> Options {
    Options {
        ignore_partial: true,
    }
}

/// Options used for RPC calls on the destination.
fn dst_options() -> Options {
    Options {
        ignore_partial: false,
    }
}

/// Moves files from source to destination using the RealizeService interface.
/// After a successful move and hash match, deletes the file from the source.
/// Returns (success_count, error_count, interrupted_count).
pub async fn move_files<T, U, P>(
    ctx: tarpc::context::Context,
    src: &RealizeServiceClient<T>,
    dst: &RealizeServiceClient<U>,
    dir_id: DirectoryId,
    progress: &mut P,
) -> Result<(usize, usize, usize), MoveFileError>
where
    T: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
    U: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
    P: Progress,
{
    METRIC_START_COUNT.inc();
    // 1. List files on src and dst in parallel
    let (src_files, dst_files) = future::join(
        src.list(ctx, dir_id.clone(), src_options()),
        dst.list(ctx, dir_id.clone(), dst_options()),
    )
    .await;
    let src_files = src_files??;
    let dst_files = dst_files??;

    // 2. Build lookup for dst
    let dst_map: HashMap<_, _> = dst_files
        .into_iter()
        .map(|f| (f.path.to_path_buf(), f))
        .collect();
    let total_files = src_files.len();
    let total_bytes = src_files.iter().map(|f| f.size).sum();
    progress.set_length(total_files, total_bytes);
    let copy_sem = Arc::new(Semaphore::new(1));
    let results = futures::stream::iter(src_files.into_iter())
        .map(|file| {
            let mut file_progress = progress.for_file(&file.path, file.size);
            let dst_file = dst_map.get(&file.path);
            let dir_id = dir_id.clone();
            let copy_sem = copy_sem.clone();
            async move {
                let result = move_file(
                    ctx,
                    src,
                    &file,
                    dst,
                    dst_file,
                    &dir_id,
                    copy_sem.clone(),
                    &mut *file_progress,
                )
                .await;
                match result {
                    Ok(_) => {
                        file_progress.success();
                        (1, 0, 0)
                    }
                    Err(MoveFileError::Rpc(tarpc::client::RpcError::DeadlineExceeded)) => {
                        file_progress.error(&MoveFileError::Rpc(
                            tarpc::client::RpcError::DeadlineExceeded,
                        ));
                        (0, 0, 1)
                    }
                    Err(err) => {
                        file_progress.error(&err);
                        (0, 1, 0)
                    }
                }
            }
        })
        .buffer_unordered(PARALLEL_FILE_COUNT)
        .collect::<Vec<(usize, usize, usize)>>()
        .await;
    let (success_count, error_count, interrupted_count) = results
        .into_iter()
        .fold((0, 0, 0), |(s, e, i), (s1, e1, i1)| {
            (s + s1, e + e1, i + i1)
        });
    METRIC_END_COUNT.inc();
    Ok((success_count, error_count, interrupted_count))
}

async fn move_file<T, U>(
    ctx: tarpc::context::Context,
    src: &RealizeServiceClient<T>,
    src_file: &SyncedFile,
    dst: &RealizeServiceClient<U>,
    dst_file: Option<&SyncedFile>,
    dir_id: &DirectoryId,
    copy_sem: Arc<Semaphore>,
    progress: &mut dyn FileProgress,
) -> Result<(), MoveFileError>
where
    T: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
    U: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
{
    METRIC_FILE_START_COUNT.inc();
    let path = src_file.path.as_path();
    let src_size = src_file.size;
    let dst_size = dst_file.map(|f| f.size).unwrap_or(0);

    progress.verifying();

    // Important: Compute source hash only once, though it might be
    // used twice; it's very slow to compute on large files.
    let mut src_hash = Either::Left(hash_file(
        ctx,
        src,
        dir_id,
        path,
        src_size,
        HASH_FILE_CHUNK,
        src_options(),
    ));
    if src_size == dst_size {
        let hash = match src_hash {
            Either::Left(fut) => fut.await?,
            Either::Right(hash) => hash,
        };
        if check_hashes_and_delete_with_src_hash(ctx, &hash, src_size, src, dst, dir_id, path)
            .await?
        {
            return Ok(());
        }
        src_hash = Either::Right(hash);
    }

    log::info!(
        "{}:{:?} transferring (src size: {}, dst size: {})",
        dir_id,
        path,
        src_size,
        dst_size
    );

    // Chunked Transfer
    let mut offset: u64 = 0;
    let mut rsyncing = true;
    let mut _copy_lock = None;
    progress.rsyncing();
    while offset < src_size {
        let mut end = offset + CHUNK_SIZE;
        if end > src_file.size {
            end = src_file.size;
        }
        let range = (offset, end);

        if dst_size <= offset {
            if rsyncing {
                progress.copying();
                rsyncing = false;
                _copy_lock = Some(copy_sem.acquire().await.unwrap());
                log::debug!("Copy semaphore locked for {}", src_file.path.display());
            }
            // Send data
            log::debug!("{}:{:?} sending range {:?}", dir_id, path, range);
            let data = src
                .read(
                    ctx,
                    dir_id.clone(),
                    path.to_path_buf(),
                    range,
                    src_options(),
                )
                .await??;
            METRIC_READ_BYTES
                .with_label_values(&["read"])
                .inc_by(data.len() as u64);
            METRIC_RANGE_READ_BYTES
                .with_label_values(&["read"])
                .inc_by(range.1 - range.0);
            dst.send(
                ctx,
                dir_id.clone(),
                path.to_path_buf(),
                range,
                src_file.size,
                data.clone(),
                dst_options(),
            )
            .await??;
            METRIC_WRITE_BYTES
                .with_label_values(&["send"])
                .inc_by(data.len() as u64);
            METRIC_RANGE_WRITE_BYTES
                .with_label_values(&["send"])
                .inc_by(range.1 - range.0);
            progress.inc(end - offset);
        } else {
            // Compute diff and apply
            log::debug!("{}:{:?} rsync on range {:?}", dir_id, path, range);
            let sig = dst
                .calculate_signature(
                    ctx,
                    dir_id.clone(),
                    path.to_path_buf(),
                    range,
                    dst_options(),
                )
                .await??;
            let delta = src
                .diff(
                    ctx,
                    dir_id.clone(),
                    path.to_path_buf(),
                    range,
                    sig,
                    src_options(),
                )
                .await??;
            METRIC_READ_BYTES
                .with_label_values(&["diff"])
                .inc_by(delta.0.len() as u64);
            METRIC_RANGE_READ_BYTES
                .with_label_values(&["diff"])
                .inc_by(range.1 - range.0);
            dst.apply_delta(
                ctx,
                dir_id.clone(),
                path.to_path_buf(),
                range,
                src_file.size,
                delta.clone(),
                dst_options(),
            )
            .await??;
            progress.inc(end - offset);
        }
        offset = end;
    }

    progress.verifying();
    if let Some(_) = _copy_lock {
        log::debug!("Copy semaphore unlocked for {}", src_file.path.display());
    }
    drop(_copy_lock);

    let hash = match src_hash {
        Either::Left(hash) => hash.await?,
        Either::Right(hash) => hash,
    };
    if !check_hashes_and_delete_with_src_hash(ctx, &hash, src_size, src, dst, dir_id, path).await? {
        return Err(MoveFileError::Realize(RealizeError::Sync(
            path.to_path_buf(),
            "Data still inconsistent after sync".to_string(),
        )));
    }

    Ok(())
}

/// Check hashes and, if they match, finish the dest file and delete the source.
///
/// Return true if the hashes matched, false otherwise.
async fn check_hashes_and_delete_with_src_hash<T, U>(
    ctx: tarpc::context::Context,
    src_hash: &Vec<Hash>,
    file_size: u64,
    src: &RealizeServiceClient<T>,
    dst: &RealizeServiceClient<U>,
    dir_id: &DirectoryId,
    path: &Path,
) -> Result<bool, MoveFileError>
where
    T: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
    U: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
{
    let dst_hash = hash_file(
        ctx,
        dst,
        dir_id,
        path,
        file_size,
        HASH_FILE_CHUNK,
        dst_options(),
    )
    .await?;
    if *src_hash != dst_hash {
        log::info!("{}:{:?} inconsistent hashes", dir_id, path);
        METRIC_FILE_END_COUNT
            .with_label_values(&["Inconsistent"])
            .inc();
        return Ok(false);
    }
    log::info!(
        "{}:{:?} OK (hash match); finishing and deleting",
        dir_id,
        path
    );
    // Hashes match, finish and delete
    dst.finish(ctx, dir_id.clone(), path.to_path_buf(), dst_options())
        .await??;
    src.delete(ctx, dir_id.clone(), path.to_path_buf(), src_options())
        .await??;

    METRIC_FILE_END_COUNT.with_label_values(&["Ok"]).inc();
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::service::DirectoryId;
    use crate::server::{self, DirectoryMap};
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use assert_unordered::assert_eq_unordered;
    use sha2::Digest as _;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::Mutex;
    use walkdir::WalkDir;

    struct MockFileProgress {
        log: Arc<Mutex<Vec<String>>>,
        id: String,
    }

    impl FileProgress for MockFileProgress {
        fn verifying(&mut self) {
            self.log
                .lock()
                .unwrap()
                .push(format!("{}:verifying", self.id));
        }
        fn rsyncing(&mut self) {
            self.log
                .lock()
                .unwrap()
                .push(format!("{}:rsyncing", self.id));
        }
        fn copying(&mut self) {
            self.log
                .lock()
                .unwrap()
                .push(format!("{}:copying", self.id));
        }
        fn inc(&mut self, bytecount: u64) {
            self.log
                .lock()
                .unwrap()
                .push(format!("{}:inc:{}", self.id, bytecount));
        }
        fn success(&mut self) {
            self.log
                .lock()
                .unwrap()
                .push(format!("{}:success", self.id));
        }
        fn error(&mut self, _err: &MoveFileError) {
            self.log.lock().unwrap().push(format!("{}:error", self.id));
        }
    }

    struct MockProgress {
        log: Arc<Mutex<Vec<String>>>,
    }

    impl Progress for MockProgress {
        fn set_length(&mut self, total_files: usize, total_bytes: u64) {
            self.log
                .lock()
                .unwrap()
                .push(format!("set_length:{}:{}", total_files, total_bytes));
        }
        fn for_file(&self, path: &std::path::Path, bytes: u64) -> Box<dyn FileProgress> {
            let id = path.to_string_lossy().to_string();
            self.log
                .lock()
                .unwrap()
                .push(format!("for_file:{}:{}", id, bytes));
            Box::new(MockFileProgress {
                log: Arc::clone(&self.log),
                id,
            })
        }
    }

    #[tokio::test]
    async fn progress_trait_is_called() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        let src_temp = TempDir::new()?;
        let dst_temp = TempDir::new()?;
        src_temp.child("foo").write_str("abc")?;
        let src_dir = Arc::new(crate::server::Directory::new(
            &DirectoryId::from("testdir"),
            src_temp.path(),
        ));
        let dst_dir = Arc::new(crate::server::Directory::new(
            &DirectoryId::from("testdir"),
            dst_temp.path(),
        ));
        let src_server =
            server::create_inprocess_client(DirectoryMap::for_dir(src_dir.id(), src_dir.path()));
        let dst_server =
            server::create_inprocess_client(DirectoryMap::for_dir(dst_dir.id(), dst_dir.path()));
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut progress = MockProgress {
            log: Arc::clone(&log),
        };
        let (_, _, _) = move_files(
            tarpc::context::current(),
            &src_server,
            &dst_server,
            DirectoryId::from("testdir"),
            &mut progress,
        )
        .await?;
        let log = log.lock().unwrap();
        // Check that set_length, for_file, and at least one FileProgress method were called
        assert!(log.iter().any(|l| l.starts_with("set_length:")));
        assert!(log.iter().any(|l| l.starts_with("for_file:")));
        assert!(log.iter().any(|l| l.contains(":verifying")));
        assert!(
            log.iter().any(|l| l.contains(":success")) || log.iter().any(|l| l.contains(":error"))
        );
        // Existing tests continue to use NoProgress
        Ok(())
    }

    #[tokio::test]
    async fn move_some_files() -> anyhow::Result<()> {
        let _ = env_logger::try_init();

        // Setup source directory with files
        let src_temp = TempDir::new()?;
        let src_dir = Arc::new(crate::server::Directory::new(
            &DirectoryId::from("testdir"),
            src_temp.path(),
        ));
        let src_server =
            server::create_inprocess_client(DirectoryMap::for_dir(src_dir.id(), src_dir.path()));

        // Setup destination directory (empty)
        let dst_temp = TempDir::new()?;
        let dst_dir = Arc::new(crate::server::Directory::new(
            &DirectoryId::from("testdir"),
            dst_temp.path(),
        ));
        let dst_server =
            server::create_inprocess_client(DirectoryMap::for_dir(dst_dir.id(), dst_dir.path()));

        // Pre-populate destination with a file of the same length as source (should trigger rsync optimization)
        src_temp.child("same_length").write_str("hello")?;
        dst_temp.child("same_length").write_str("xxxxx")?; // same length as "hello"

        // Corrupted dst file in partial state
        src_temp.child("longer").write_str("world")?;
        dst_temp.child(".longer.part").write_str("corrupt")?;

        // Corrupted dst file in final state
        dst_temp.child("corrupt_final").write_str("corruptfinal")?;
        src_temp.child("corrupt_final").write_str("bazgood")?;

        // Partially copied dst file (shorter than src)
        dst_temp.child("partial").write_str("par")?;
        src_temp.child("partial").write_str("partialcopy")?;

        println!("test_move_files: all files set up");
        let (success, error, _interrupted) = move_files(
            tarpc::context::current(),
            &src_server,
            &dst_server,
            DirectoryId::from("testdir"),
            &mut NoProgress,
        )
        .await?;
        assert_eq!(error, 0, "No errors expected");
        assert_eq!(success, 4, "All files should be moved");
        // Check that files are present in destination and not in source
        assert_eq_unordered!(snapshot_dir(src_temp.path())?, vec![]);
        assert_eq_unordered!(
            snapshot_dir(dst_temp.path())?,
            vec![
                (PathBuf::from("same_length"), "hello".to_string()),
                (PathBuf::from("longer"), "world".to_string()),
                (PathBuf::from("corrupt_final"), "bazgood".to_string()),
                (PathBuf::from("partial"), "partialcopy".to_string()),
            ]
        );

        Ok(())
    }

    #[tokio::test]
    #[test_tag::tag(slow)]
    async fn move_files_chunked() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        const FILE_SIZE: usize = (1.25 * CHUNK_SIZE as f32) as usize;
        let chunk = vec![0xAB; FILE_SIZE];
        let chunk2 = vec![0xCD; FILE_SIZE];
        let chunk3 = vec![0xEF; FILE_SIZE];
        let chunk4 = vec![0x12; FILE_SIZE];
        let chunk5 = vec![0x34; FILE_SIZE];

        let src_temp = TempDir::new()?;
        let src_dir = Arc::new(crate::server::Directory::new(
            &DirectoryId::from("testdir"),
            src_temp.path(),
        ));
        let src_server =
            server::create_inprocess_client(DirectoryMap::for_dir(src_dir.id(), src_dir.path()));

        let dst_temp = TempDir::new()?;
        let dst_dir = Arc::new(crate::server::Directory::new(
            &DirectoryId::from("testdir"),
            dst_temp.path(),
        ));
        let dst_server =
            server::create_inprocess_client(DirectoryMap::for_dir(dst_dir.id(), dst_dir.path()));

        // Case 1: source > CHUNK_SIZE, destination empty
        src_temp.child("large_empty").write_binary(&chunk)?;
        // Case 2: source > CHUNK_SIZE, destination same size, but content is different
        src_temp.child("large_diff").write_binary(&chunk)?;
        dst_temp.child("large_diff").write_binary(&chunk2)?;
        // Case 3: source > CHUNK_SIZE, destination truncated, shorter than CHUNK_SIZE
        src_temp.child("large_trunc_short").write_binary(&chunk)?;
        dst_temp
            .child("large_trunc_short")
            .write_binary(&chunk3[..(0.5 * CHUNK_SIZE as f32) as usize])?;
        // Case 4: source > CHUNK_SIZE, destination truncated, longer than CHUNK_SIZE
        src_temp.child("large_trunc_long").write_binary(&chunk)?;
        dst_temp
            .child("large_trunc_long")
            .write_binary(&chunk4[..(1.25 * CHUNK_SIZE as f32) as usize])?;
        // Case 5: source > CHUNK_SIZE, destination same content, with garbage at the end
        src_temp.child("large_garbage").write_binary(&chunk)?;
        let mut garbage = chunk.clone();
        garbage.extend_from_slice(&chunk5[..1024 * 1024]); // 1MB garbage
        dst_temp.child("large_garbage").write_binary(&garbage)?;

        let (success, error, _interrupted) = move_files(
            tarpc::context::current(),
            &src_server,
            &dst_server,
            DirectoryId::from("testdir"),
            &mut NoProgress,
        )
        .await?;
        assert_eq!(error, 0, "No errors expected");
        assert_eq!(success, 5, "All files should be moved");
        // Check that files are present in destination and not in source
        assert_eq_unordered!(snapshot_dir(src_temp.path())?, vec![]);
        let expected = vec![
            (PathBuf::from("large_empty"), chunk.clone()),
            (PathBuf::from("large_diff"), chunk.clone()),
            (PathBuf::from("large_trunc_short"), chunk.clone()),
            (PathBuf::from("large_trunc_long"), chunk.clone()),
            (PathBuf::from("large_garbage"), chunk.clone()),
        ];
        let actual = snapshot_dir_bin(dst_temp.path())?;
        for (path, data) in expected {
            let found = actual
                .iter()
                .find(|(p, _)| *p == path)
                .unwrap_or_else(|| panic!("missing {path:?}"));
            assert_eq!(&found.1, &data, "content mismatch for {path:?}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn move_files_partial_error() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        let src_temp = TempDir::new()?;
        let src_dir = Arc::new(crate::server::Directory::new(
            &DirectoryId::from("testdir"),
            src_temp.path(),
        ));
        let src_server =
            server::create_inprocess_client(DirectoryMap::for_dir(src_dir.id(), src_dir.path()));
        let dst_temp = TempDir::new()?;
        let dst_dir = Arc::new(crate::server::Directory::new(
            &DirectoryId::from("testdir"),
            dst_temp.path(),
        ));
        let dst_server =
            server::create_inprocess_client(DirectoryMap::for_dir(dst_dir.id(), dst_dir.path()));
        // Good file
        src_temp.child("good").write_str("ok")?;
        // Unreadable file
        let unreadable = src_temp.child("bad");
        unreadable.write_str("fail")?;
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(unreadable.path(), fs::Permissions::from_mode(0o000))?;
        let (success, error, _interrupted) = move_files(
            tarpc::context::current(),
            &src_server,
            &dst_server,
            DirectoryId::from("testdir"),
            &mut NoProgress,
        )
        .await?;
        assert_eq!(success, 1, "One file should succeed");
        assert_eq!(error, 1, "One file should fail");
        // Restore permissions for cleanup
        fs::set_permissions(unreadable.path(), fs::Permissions::from_mode(0o644))?;
        Ok(())
    }

    #[tokio::test]
    async fn move_files_ignores_partial_in_src() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        let src_temp = TempDir::new()?;
        let dst_temp = TempDir::new()?;
        let src_dir = Arc::new(crate::server::Directory::new(
            &DirectoryId::from("testdir"),
            src_temp.path(),
        ));
        let dst_dir = Arc::new(crate::server::Directory::new(
            &DirectoryId::from("testdir"),
            dst_temp.path(),
        ));
        let src_server =
            server::create_inprocess_client(DirectoryMap::for_dir(src_dir.id(), src_dir.path()));
        let dst_server =
            server::create_inprocess_client(DirectoryMap::for_dir(dst_dir.id(), dst_dir.path()));
        // Create a final file and a partial file in src
        src_temp.child("final.txt").write_str("finaldata")?;
        src_temp
            .child(".partial.txt.part")
            .write_str("partialdata")?;
        // Run move_files
        let (success, error, _interrupted) = move_files(
            tarpc::context::current(),
            &src_server,
            &dst_server,
            DirectoryId::from("testdir"),
            &mut NoProgress,
        )
        .await?;
        // Only the final file should be moved
        assert_eq!(success, 1, "Only one file should be moved");
        assert_eq!(error, 0, "No errors expected");
        // Check that only final.txt is present in dst
        let files = WalkDir::new(dst_temp.path())
            .into_iter()
            .flatten()
            .filter(|e| e.path().is_file())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        assert_eq!(files, vec!["final.txt"]);
        Ok(())
    }

    #[tokio::test]
    async fn hash_file_non_chunked() -> anyhow::Result<()> {
        let _ = env_logger::try_init();

        let temp = TempDir::new()?;
        let file = temp.child("somefile");
        let content = b"baa, baa, black sheep";
        file.write_binary(content)?;

        let dir_id = DirectoryId::from("dir");
        let server = server::create_inprocess_client(DirectoryMap::for_dir(&dir_id, temp.path()));
        let hashvec = hash_file(
            tarpc::context::current(),
            &server,
            &dir_id,
            &PathBuf::from("somefile"),
            content.len() as u64,
            HASH_FILE_CHUNK,
            Options::default(),
        )
        .await?;
        assert_eq!(hashvec, vec![Hash(sha2::Sha256::digest(content).into())]);

        Ok(())
    }

    #[tokio::test]
    async fn hash_file_chunked() -> anyhow::Result<()> {
        let _ = env_logger::try_init();

        let temp = TempDir::new()?;
        let file = temp.child("somefile");
        let content = b"baa, baa, black sheep";
        file.write_binary(content)?;

        let dir_id = DirectoryId::from("dir");
        let server = server::create_inprocess_client(DirectoryMap::for_dir(&dir_id, temp.path()));
        let hashvec = hash_file(
            tarpc::context::current(),
            &server,
            &dir_id,
            &PathBuf::from("somefile"),
            content.len() as u64,
            4,
            Options::default(),
        )
        .await?;
        assert_eq!(
            hashvec,
            vec![
                Hash(sha2::Sha256::digest(b"baa,").into()),
                Hash(sha2::Sha256::digest(b" baa").into()),
                Hash(sha2::Sha256::digest(b", bl").into()),
                Hash(sha2::Sha256::digest(b"ack ").into()),
                Hash(sha2::Sha256::digest(b"shee").into()),
                Hash(sha2::Sha256::digest(b"p").into()),
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn hash_file_wrong_size() -> anyhow::Result<()> {
        let _ = env_logger::try_init();

        let temp = TempDir::new()?;
        let file = temp.child("somefile");
        let content = b"baa, baa, black sheep";
        file.write_binary(content)?;

        let dir_id = DirectoryId::from("dir");
        let server = server::create_inprocess_client(DirectoryMap::for_dir(&dir_id, temp.path()));
        let hashvec = hash_file(
            tarpc::context::current(),
            &server,
            &dir_id,
            &PathBuf::from("somefile"),
            content.len() as u64 * 2,
            12,
            Options::default(),
        )
        .await?;
        assert_eq!(hashvec.len(), 4);
        assert_eq!(hashvec[1], Hash::zero());

        Ok(())
    }

    /// Return the set of files in [dir] and their content.
    fn snapshot_dir(dir: &Path) -> anyhow::Result<Vec<(PathBuf, String)>> {
        let mut result = vec![];
        for entry in WalkDir::new(dir).into_iter().flatten() {
            if !entry.path().is_file() {
                continue;
            }
            let relpath = pathdiff::diff_paths(entry.path(), dir);
            if let Some(relpath) = relpath {
                let content = std::fs::read_to_string(entry.path())?;
                result.push((relpath, content));
            }
        }

        Ok(result)
    }

    /// Return the set of files in [dir] and their content (binary).
    fn snapshot_dir_bin(dir: &Path) -> anyhow::Result<Vec<(PathBuf, Vec<u8>)>> {
        let mut result = vec![];
        for entry in WalkDir::new(dir).into_iter().flatten() {
            if !entry.path().is_file() {
                continue;
            }
            let relpath = pathdiff::diff_paths(entry.path(), dir);
            if let Some(relpath) = relpath {
                let content = std::fs::read(entry.path())?;
                result.push((relpath, content));
            }
        }
        Ok(result)
    }
}

/// Hash file in chunks and return the result.
pub(crate) async fn hash_file<T>(
    ctx: tarpc::context::Context,
    client: &RealizeServiceClient<T>,
    dir_id: &DirectoryId,
    relative_path: &Path,
    file_size: u64,
    chunk_size: u64,
    options: Options,
) -> Result<Vec<Hash>, MoveFileError>
where
    T: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
{
    let mut offset = 0;
    let mut ranges = vec![];
    while offset < file_size {
        let end = min(file_size, offset + chunk_size);
        ranges.push((offset, end));
        offset = end;
    }

    let results = futures::stream::iter(ranges.into_iter())
        .map(|range| {
            client.hash(
                ctx,
                dir_id.clone(),
                relative_path.to_path_buf(),
                range,
                options,
            )
        })
        .buffered(PARALLEL_FILE_HASH)
        .collect::<Vec<_>>()
        .await;

    let mut all_hashes = vec![];
    for res in results.into_iter() {
        all_hashes.push(res??);
    }

    Ok(all_hashes)
}

/// Errors returned by [move_files]
///
/// Error messages are kept short, to avoid repetition when printed
/// with anyhow {:#}.
#[derive(Debug, Error)]
pub enum MoveFileError {
    #[error("RPC error: {0}")]
    Rpc(#[from] tarpc::client::RpcError),
    #[error("Remote Error: {0}")]
    Realize(#[from] RealizeError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
