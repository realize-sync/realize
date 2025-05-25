//! Move algorithm for Realize - Symmetric File Syncer
//!
//! Implements the unoptimized algorithm to move files from source (A) to destination (B)
//! using the RealizeService trait. See spec/design.md for details.

use crate::model::byterange::{ByteRange, ByteRanges};
use crate::model::service::{
    DirectoryId, Options, RangedHash, RealizeError, RealizeServiceClient, RealizeServiceRequest,
    RealizeServiceResponse, SyncedFile,
};
use futures::FutureExt;
use futures::future;
use futures::future::Either;
use futures::stream::StreamExt as _;
use prometheus::{IntCounter, IntCounterVec, register_int_counter, register_int_counter_vec};
use std::sync::Arc;
use std::{collections::HashMap, path::Path};
use tarpc::client::stub::Stub;
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::Sender;

pub mod hash;

const CHUNK_SIZE: u64 = 4 * 1024 * 1024;
const PARALLEL_FILE_COUNT: usize = 4;
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
    pub static ref METRIC_APPLY_DELTA_FALLBACK_COUNT: IntCounter =
        register_int_counter!(
            "realize_apply_delta_fallback_count",
            "Number of times copy had to be used as fallback to apply_delta",
        ).unwrap();
    pub static ref METRIC_APPLY_DELTA_FALLBACK_BYTES: IntCounter =
        register_int_counter!(
            "realize_apply_delta_fallback_bytes",
            "Bytes for which copy had to be used as fallback to apply_delta",
        ).unwrap();
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

/// Event enum for channel-based progress reporting.
#[derive(Debug, Clone)]
pub enum ProgressEvent {
    /// Indicates the start of moving a directory, with total files and bytes.
    MovingDir {
        dir_id: DirectoryId,
        total_files: usize,
        total_bytes: u64,
    },
    /// Indicates a file is being processed (start), with its path and size.
    MovingFile {
        dir_id: DirectoryId,
        path: std::path::PathBuf,
        bytes: u64,

        /// Bytes already available, to be r-synced.
        available: u64,
    },
    /// File is being verified (hash check).
    VerifyingFile {
        dir_id: DirectoryId,
        path: std::path::PathBuf,
    },
    /// File is being rsynced (diff/patch).
    RsyncingFile {
        dir_id: DirectoryId,
        path: std::path::PathBuf,
    },
    /// File is being copied (data transfer).
    CopyingFile {
        dir_id: DirectoryId,
        path: std::path::PathBuf,
    },
    /// Increment byte count for a file and overall progress.
    IncrementByteCount {
        dir_id: DirectoryId,
        path: std::path::PathBuf,
        bytecount: u64,
    },
    /// File was moved successfully.
    FileSuccess {
        dir_id: DirectoryId,
        path: std::path::PathBuf,
    },
    /// Moving the file failed.
    FileError {
        dir_id: DirectoryId,
        path: std::path::PathBuf,
        error: String,
    },
}

/// Moves files from source to destination using the RealizeService interface, sending progress events to a channel.
pub async fn move_files<T, U>(
    ctx: tarpc::context::Context,
    src: &RealizeServiceClient<T>,
    dst: &RealizeServiceClient<U>,
    dir_id: DirectoryId,
    progress_tx: Option<Sender<ProgressEvent>>,
) -> Result<(usize, usize, usize), MoveFileError>
where
    T: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
    U: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
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
    if let Some(tx) = &progress_tx {
        let _ = tx
            .send(ProgressEvent::MovingDir {
                dir_id: dir_id.clone(),
                total_files,
                total_bytes,
            })
            .await;
    }
    let copy_sem = Arc::new(Semaphore::new(1));
    let results = futures::stream::iter(src_files.into_iter())
        .map(|file| {
            let dst_file = dst_map.get(&file.path);
            let dir_id = dir_id.clone();
            let copy_sem = copy_sem.clone();
            let tx = progress_tx.clone();
            let file_path = file.path.clone();

            async move {
                let tx = tx.clone();
                if let Some(tx) = &tx {
                    let _ = tx
                        .send(ProgressEvent::MovingFile {
                            dir_id: dir_id.clone(),
                            path: file.path.clone(),
                            bytes: file.size,
                            available: dst_file.map_or(0, |f| f.size),
                        })
                        .await;
                }

                let result = move_file(
                    ctx,
                    src,
                    &file,
                    dst,
                    dst_file,
                    &dir_id,
                    copy_sem.clone(),
                    tx.clone(),
                )
                .await;
                match result {
                    Ok(_) => {
                        if let Some(tx) = &tx {
                            let _ = tx
                                .send(ProgressEvent::FileSuccess {
                                    dir_id: dir_id.clone(),
                                    path: file_path.clone(),
                                })
                                .await;
                        }

                        (1, 0, 0)
                    }
                    Err(MoveFileError::Rpc(tarpc::client::RpcError::DeadlineExceeded)) => {
                        log::debug!("{}/{}: Deadline exceeded", dir_id, file_path.display());
                        if let Some(tx) = &tx {
                            let _ = tx
                                .send(ProgressEvent::FileError {
                                    dir_id: dir_id.clone(),
                                    path: file_path.clone(),
                                    error: "Deadline exceeded".to_string(),
                                })
                                .await;
                        }

                        (0, 0, 1)
                    }
                    Err(ref err) => {
                        log::debug!("{}/{}: {}", dir_id, file_path.display(), err);
                        if let Some(tx) = &tx {
                            let _ = tx
                                .send(ProgressEvent::FileError {
                                    dir_id: dir_id.clone(),
                                    path: file_path.clone(),
                                    error: format!("{}", err),
                                })
                                .await;
                        }
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
    progress_tx: Option<Sender<ProgressEvent>>,
) -> Result<(), MoveFileError>
where
    T: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
    U: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
{
    METRIC_FILE_START_COUNT.inc();
    let path = src_file.path.as_path();
    let src_size = src_file.size;
    let dst_size = dst_file.map(|f| f.size).unwrap_or(0);
    let dir_id = dir_id.clone();

    // Check the file if it already exists
    let mut src_hash = Either::Left(hash_file(
        ctx,
        src,
        &dir_id,
        path,
        src_size,
        HASH_FILE_CHUNK,
        src_options(),
    ));
    let mut correct = ByteRanges::new();
    if dst_size > HASH_FILE_CHUNK {
        report_verifying(&progress_tx, &dir_id, path).await;
        let hash = match src_hash {
            Either::Left(fut) => fut.await?,
            Either::Right(hash) => hash,
        };
        match check_hashes_and_delete(ctx, &hash, src_size, src, dst, &dir_id, path).await? {
            HashCheck::Match => {
                return Ok(());
            }
            HashCheck::Mismatch {
                partial_match: matches,
                ..
            } => {
                correct = matches;
            }
        }
        src_hash = Either::Right(hash);
    }
    // Chunked Transfer
    let ranges = ByteRanges::single(0, src_size);
    let mut copy_ranges = ranges.intersection(&ByteRanges::single(dst_size, src_size));
    let rsync_ranges = ranges
        .intersection(&ByteRanges::single(0, dst_size))
        .subtraction(&correct);
    if !correct.is_empty() {
        for range in correct.into_iter() {
            report_range_progress(&progress_tx, &dir_id, path, &range).await;
        }
    }

    // 1. Check existing data (rsyncing)
    if !rsync_ranges.is_empty() {
        report_rsyncing(&progress_tx, &dir_id, path).await;
        log::debug!(
            "{}/{} overall: {}, rsync: {}, (later) copy: {}",
            dir_id,
            path.display(),
            ranges,
            rsync_ranges,
            copy_ranges
        );

        for range in rsync_ranges.chunked(CHUNK_SIZE) {
            let sig = dst
                .calculate_signature(
                    ctx,
                    dir_id.clone(),
                    path.to_path_buf(),
                    range.clone(),
                    dst_options(),
                )
                .await??;
            let (delta, hash) = src
                .diff(
                    ctx,
                    dir_id.clone(),
                    path.to_path_buf(),
                    range.clone(),
                    sig,
                    src_options(),
                )
                .await??;
            METRIC_READ_BYTES
                .with_label_values(&["diff"])
                .inc_by(delta.0.len() as u64);
            METRIC_RANGE_READ_BYTES
                .with_label_values(&["diff"])
                .inc_by(range.bytecount());
            let delta_len = delta.0.len();
            match dst
                .apply_delta(
                    ctx,
                    dir_id.clone(),
                    path.to_path_buf(),
                    range.clone(),
                    src_file.size,
                    delta,
                    hash,
                    dst_options(),
                )
                .await?
            {
                Ok(_) => {
                    METRIC_WRITE_BYTES
                        .with_label_values(&["apply_delta"])
                        .inc_by(delta_len as u64);
                    METRIC_RANGE_WRITE_BYTES
                        .with_label_values(&["apply_delta"])
                        .inc_by(range.bytecount());
                    report_range_progress(&progress_tx, &dir_id, path, &range).await;
                }
                Err(RealizeError::HashMismatch) => {
                    copy_ranges.add(&range);
                    log::error!(
                        "{}/{}:{} hash mismatch after apply_delta, will copy",
                        dir_id,
                        path.display(),
                        range,
                    );
                    METRIC_APPLY_DELTA_FALLBACK_COUNT.inc();
                    METRIC_APPLY_DELTA_FALLBACK_BYTES.inc_by(range.bytecount());
                }
                Err(err) => {
                    return Err(MoveFileError::from(err));
                }
            };
        }
    }

    // 2. Copy missyng data
    if !copy_ranges.is_empty() {
        report_copying(&progress_tx, &dir_id, path).await;
        log::debug!(
            "{}/{} overall: {}, copy: {}",
            dir_id,
            path.display(),
            ranges,
            copy_ranges
        );

        let _lock = copy_sem.acquire().await;
        for range in copy_ranges.chunked(CHUNK_SIZE) {
            let data = src
                .read(
                    ctx,
                    dir_id.clone(),
                    path.to_path_buf(),
                    range.clone(),
                    src_options(),
                )
                .await??;
            METRIC_READ_BYTES
                .with_label_values(&["read"])
                .inc_by(data.len() as u64);
            METRIC_RANGE_READ_BYTES
                .with_label_values(&["read"])
                .inc_by(range.bytecount());
            let data_len = data.len();
            dst.send(
                ctx,
                dir_id.clone(),
                path.to_path_buf(),
                range.clone(),
                src_file.size,
                data,
                dst_options(),
            )
            .await??;
            METRIC_WRITE_BYTES
                .with_label_values(&["send"])
                .inc_by(data_len as u64);
            METRIC_RANGE_WRITE_BYTES
                .with_label_values(&["send"])
                .inc_by(range.bytecount());
            report_range_progress(&progress_tx, &dir_id, path, &range).await;
        }
    }

    // 3. Check full file and delete
    report_verifying(&progress_tx, &dir_id, path).await;
    let hash = match src_hash {
        Either::Left(hash) => hash.await?,
        Either::Right(hash) => hash,
    };
    match check_hashes_and_delete(ctx, &hash, src_size, src, dst, &dir_id, path).await? {
        HashCheck::Mismatch { .. } => Err(MoveFileError::FailedToSync),
        HashCheck::Match => Ok(()),
    }
}

async fn report_copying(
    progress_tx: &Option<Sender<ProgressEvent>>,
    dir_id: &DirectoryId,
    path: &Path,
) {
    if let Some(tx) = progress_tx {
        let _ = tx
            .send(ProgressEvent::CopyingFile {
                dir_id: dir_id.clone(),
                path: path.to_path_buf(),
            })
            .await;
    }
}

async fn report_rsyncing(
    progress_tx: &Option<Sender<ProgressEvent>>,
    dir_id: &DirectoryId,
    path: &Path,
) {
    if let Some(tx) = progress_tx {
        let _ = tx
            .send(ProgressEvent::RsyncingFile {
                dir_id: dir_id.clone(),
                path: path.to_path_buf(),
            })
            .await;
    }
}

async fn report_verifying(
    progress_tx: &Option<Sender<ProgressEvent>>,
    dir_id: &DirectoryId,
    path: &Path,
) {
    if let Some(tx) = progress_tx {
        let _ = tx
            .send(ProgressEvent::VerifyingFile {
                dir_id: dir_id.clone(),
                path: path.to_path_buf(),
            })
            .await;
    }
}

async fn report_range_progress(
    progress_tx: &Option<Sender<ProgressEvent>>,
    dir_id: &DirectoryId,
    path: &Path,
    range: &ByteRange,
) {
    if let Some(tx) = progress_tx {
        let _ = tx
            .send(ProgressEvent::IncrementByteCount {
                dir_id: dir_id.clone(),
                path: path.to_path_buf(),
                bytecount: range.bytecount(),
            })
            .await;
    }
}

/// Hash file in chunks and return the result.
pub(crate) async fn hash_file<T>(
    ctx: tarpc::context::Context,
    client: &RealizeServiceClient<T>,
    dir_id: &DirectoryId,
    relative_path: &std::path::Path,
    file_size: u64,
    chunk_size: u64,
    options: Options,
) -> Result<RangedHash, MoveFileError>
where
    T: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
{
    let results =
        futures::stream::iter(ByteRange::new(0, file_size).chunked(chunk_size).into_iter())
            .map(|range| {
                client
                    .hash(
                        ctx,
                        dir_id.clone(),
                        relative_path.to_path_buf(),
                        range.clone(),
                        options,
                    )
                    .map(move |res| res.map(|h| (range.clone(), h)))
            })
            .buffer_unordered(PARALLEL_FILE_HASH)
            .collect::<Vec<_>>()
            .await;

    let mut ranged = RangedHash::new();
    for res in results.into_iter() {
        let (range, hash_res) = res?;
        ranged.add(range, hash_res?);
    }
    Ok(ranged)
}

/// Return value for [check_hashes_and_delete]
enum HashCheck {
    /// The hashes matched.
    Match,

    /// The hashes didn't match.
    ///
    /// If some hash range matched, it is reported as partial match.
    Mismatch { partial_match: ByteRanges },
}

/// Check hashes and, if they match, finish the dest file and delete the source.
///
/// Return true if the hashes matched, false otherwise.
async fn check_hashes_and_delete<T, U>(
    ctx: tarpc::context::Context,
    src_hash: &RangedHash,
    file_size: u64,
    src: &RealizeServiceClient<T>,
    dst: &RealizeServiceClient<U>,
    dir_id: &DirectoryId,
    path: &std::path::Path,
) -> Result<HashCheck, MoveFileError>
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
    let is_complete_src = src_hash.is_complete(file_size);
    let is_complete_dst = dst_hash.is_complete(file_size);
    let (matches, mismatches) = src_hash.diff(&dst_hash);
    if !mismatches.is_empty() || !is_complete_src || !is_complete_dst {
        log::warn!(
            "{}:{:?} inconsistent hashes\nsrc: {}\ndst: {}\nmatches: {}\nmismatches: {}",
            dir_id,
            path,
            src_hash,
            dst_hash,
            matches,
            mismatches
        );
        METRIC_FILE_END_COUNT
            .with_label_values(&["Inconsistent"])
            .inc();
        return Ok(HashCheck::Mismatch {
            partial_match: matches,
        });
    }
    log::debug!("{}/{} MOVED", dir_id, path.display());
    // Hashes match, finish and delete
    dst.finish(ctx, dir_id.clone(), path.to_path_buf(), dst_options())
        .await??;
    src.delete(ctx, dir_id.clone(), path.to_path_buf(), src_options())
        .await??;

    METRIC_FILE_END_COUNT.with_label_values(&["Ok"]).inc();
    Ok(HashCheck::Match)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::service::DirectoryId;
    use crate::model::service::Hash;
    use crate::server::{self, DirectoryMap};
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use assert_unordered::assert_eq_unordered;
    use std::path::PathBuf;
    use std::sync::Arc;
    use walkdir::WalkDir;

    #[tokio::test]
    async fn events_are_sent() -> anyhow::Result<()> {
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

        let (tx, mut rx) = tokio::sync::mpsc::channel(32);
        let (success, error, _) = move_files(
            tarpc::context::current(),
            &src_server,
            &dst_server,
            DirectoryId::from("testdir"),
            Some(tx),
        )
        .await?;

        assert_eq!(success, 1);
        assert_eq!(error, 0);

        // Collect all events
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }

        // Verify essential events were sent
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ProgressEvent::MovingDir { .. })),
            "MovingDir event not found"
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ProgressEvent::MovingFile { .. })),
            "MovingFile event not found"
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ProgressEvent::VerifyingFile { .. })),
            "VerifyingFile event not found"
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ProgressEvent::FileSuccess { .. })),
            "FileSuccess event not found"
        );

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
            None,
        )
        .await?;
        assert_eq!((success, error), (4, 0));
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

        let mut ctx = tarpc::context::current();
        ctx.deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);

        let (success, error, _interrupted) = move_files(
            ctx,
            &src_server,
            &dst_server,
            DirectoryId::from("testdir"),
            None,
        )
        .await?;
        assert_eq!(
            (5, 0),
            (success, error),
            "should be: 5 files moved, 0 errors"
        );
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
            None,
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
            None,
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
        let ranged = hash_file(
            tarpc::context::current(),
            &server,
            &dir_id,
            &PathBuf::from("somefile"),
            content.len() as u64,
            HASH_FILE_CHUNK,
            Options::default(),
        )
        .await?;
        assert_eq!(
            RangedHash::single(
                ByteRange {
                    start: 0,
                    end: content.len() as u64
                },
                hash::digest(content)
            ),
            ranged
        );
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
        let ranged = hash_file(
            tarpc::context::current(),
            &server,
            &dir_id,
            &PathBuf::from("somefile"),
            content.len() as u64,
            4,
            Options::default(),
        )
        .await?;
        let mut expected = RangedHash::new();
        expected.add(ByteRange { start: 0, end: 4 }, hash::digest(b"baa,"));
        expected.add(ByteRange { start: 4, end: 8 }, hash::digest(b" baa"));
        expected.add(ByteRange { start: 8, end: 12 }, hash::digest(b", bl"));
        expected.add(ByteRange { start: 12, end: 16 }, hash::digest(b"ack "));
        expected.add(ByteRange { start: 16, end: 20 }, hash::digest(b"shee"));
        expected.add(ByteRange { start: 20, end: 21 }, hash::digest(b"p"));

        assert_eq!(ranged, expected);

        Ok(())
    }

    #[tokio::test]
    async fn hash_file_wrong_size() -> anyhow::Result<()> {
        let _ = env_logger::try_init();

        let temp = TempDir::new()?;
        let file = temp.child("somefile");
        let content = b"foobar";
        file.write_binary(content)?;

        let dir_id = DirectoryId::from("dir");
        let server = server::create_inprocess_client(DirectoryMap::for_dir(&dir_id, temp.path()));
        let ranged = hash_file(
            tarpc::context::current(),
            &server,
            &dir_id,
            &PathBuf::from("somefile"),
            8,
            4,
            Options::default(),
        )
        .await?;
        let mut expected = RangedHash::new();
        expected.add(ByteRange { start: 0, end: 4 }, hash::digest(b"foob"));
        expected.add(ByteRange { start: 4, end: 8 }, Hash::zero());
        assert_eq!(ranged, expected);

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
    #[error("Data still inconsistent after sync")]
    FailedToSync,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
