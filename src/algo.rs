//! Move algorithm for Realize - Symmetric File Syncer
//!
//! Implements the unoptimized algorithm to move files from source (A) to destination (B)
//! using the RealizeService trait. See spec/design.md for details.

use crate::model::service::{
    DirectoryId, RealizeError, RealizeServiceClient, RealizeServiceRequest, RealizeServiceResponse,
    SyncedFile,
};
use futures::future::{join, join_all};
use std::{collections::HashMap, path::Path};
use tarpc::{client::stub::Stub, context};
use thiserror::Error;

const CHUNK_SIZE: u64 = 8 * 1024 * 1024; // 8MB

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
    /// Transferring data
    fn moving(&mut self);
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

pub struct NoFileProgress;

impl FileProgress for NoFileProgress {
    fn verifying(&mut self) {}
    fn moving(&mut self) {}
    fn inc(&mut self, _bytecount: u64) {}
    fn success(&mut self) {}
    fn error(&mut self, _err: &MoveFileError) {}
}

/// Moves files from source to destination using the RealizeService interface.
/// After a successful move and hash match, deletes the file from the source.
/// Returns (success_count, error_count).
pub async fn move_files<T, U, P>(
    src: &RealizeServiceClient<T>,
    dst: &RealizeServiceClient<U>,
    dir_id: DirectoryId,
    progress: &mut P,
) -> Result<(usize, usize), MoveFileError>
where
    T: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
    U: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
    P: Progress,
{
    // 1. List files on src and dst in parallel
    let (src_files, dst_files) = join(
        src.list(context::current(), dir_id.clone()),
        dst.list(context::current(), dir_id.clone()),
    )
    .await;
    let src_files = src_files??;
    let dst_files = dst_files??;

    // 2. Build lookup for dst
    let dst_map: HashMap<_, _> = dst_files
        .into_iter()
        .map(|f| (f.path.to_path_buf(), f))
        .collect();
    let mut success_count = 0;
    let mut error_count = 0;
    let total_files = src_files.len();
    let total_bytes = src_files.iter().map(|f| f.size).sum();
    progress.set_length(total_files, total_bytes);
    let mut handles = vec![];
    for file in src_files.iter() {
        let mut file_progress = progress.for_file(&file.path, file.size);
        let dst_file = dst_map.get(&file.path);
        let dir_id = dir_id.clone();
        let fut = async move {
            let result = move_file(src, file, dst, dst_file, &dir_id, &mut *file_progress).await;
            match result {
                Ok(_) => {
                    file_progress.success();

                    true
                }
                Err(err) => {
                    file_progress.error(&err);

                    false
                }
            }
        };
        handles.push(fut);
    }
    for res in join_all(handles).await {
        if res {
            success_count += 1;
        } else {
            error_count += 1;
        }
    }
    Ok((success_count, error_count))
}

async fn move_file<T, U>(
    src: &RealizeServiceClient<T>,
    src_file: &SyncedFile,
    dst: &RealizeServiceClient<U>,
    dst_file: Option<&SyncedFile>,
    dir_id: &DirectoryId,
    progress: &mut dyn FileProgress,
) -> Result<(), MoveFileError>
where
    T: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
    U: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
{
    let path = src_file.path.as_path();
    let src_size = src_file.size;
    let dst_size = dst_file.map(|f| f.size).unwrap_or(0);

    progress.verifying();
    if src_size == dst_size && check_hashes_and_delete(src, dst, dir_id, &path).await? {
        return Ok(());
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
    while offset < src_size {
        let mut end = offset + CHUNK_SIZE;
        if end > src_file.size {
            end = src_file.size;
        }
        let range = (offset, end);
        progress.moving();
        if dst_size <= offset {
            // Send data
            log::debug!("{}:{:?} sending range {:?}", dir_id, path, range);
            let data = src
                .read(
                    context::current(),
                    dir_id.clone(),
                    path.to_path_buf(),
                    range,
                )
                .await??;
            dst.send(
                context::current(),
                dir_id.clone(),
                path.to_path_buf(),
                range,
                src_file.size,
                data.clone(),
            )
            .await??;
            progress.inc((end - offset) as u64);
        } else {
            // Compute diff and apply
            log::debug!("{}:{:?} rsync on range {:?}", dir_id, path, range);
            let sig = dst
                .calculate_signature(
                    context::current(),
                    dir_id.clone(),
                    path.to_path_buf(),
                    range,
                )
                .await??;
            let delta = src
                .diff(
                    context::current(),
                    dir_id.clone(),
                    path.to_path_buf(),
                    range,
                    sig,
                )
                .await??;
            dst.apply_delta(
                context::current(),
                dir_id.clone(),
                path.to_path_buf(),
                range,
                src_file.size,
                delta,
            )
            .await??;
            progress.inc((end - offset) as u64);
        }
        offset = end;
    }

    progress.verifying();
    if !check_hashes_and_delete(src, dst, dir_id, &path).await? {
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
async fn check_hashes_and_delete<T, U>(
    src: &RealizeServiceClient<T>,
    dst: &RealizeServiceClient<U>,
    dir_id: &DirectoryId,
    path: &Path,
) -> Result<bool, MoveFileError>
where
    T: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
    U: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
{
    let (src_hash, dst_hash) = tokio::join!(
        src.hash(context::current(), dir_id.clone(), path.to_path_buf()),
        dst.hash(context::current(), dir_id.clone(), path.to_path_buf()),
    );
    let src_hash = src_hash.map_err(MoveFileError::from)??;
    let dst_hash = dst_hash.map_err(MoveFileError::from)??;
    if src_hash != dst_hash {
        log::info!("{}:{:?} inconsistent hashes", dir_id, path);
        return Ok(false);
    }
    log::info!(
        "{}:{:?} OK (hash match); finishing and deleting",
        dir_id,
        path
    );
    // Hashes match, finish and delete
    dst.finish(context::current(), dir_id.clone(), path.to_path_buf())
        .await
        .map_err(MoveFileError::from)??;
    src.delete(context::current(), dir_id.clone(), path.to_path_buf())
        .await
        .map_err(MoveFileError::from)??;
    return Ok(true);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::service::DirectoryId;
    use crate::server::RealizeServer;
    use assert_fs::prelude::*;
    use assert_fs::TempDir;
    use assert_unordered::assert_eq_unordered;
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
        fn moving(&mut self) {
            self.log.lock().unwrap().push(format!("{}:moving", self.id));
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
    async fn test_progress_trait_is_called() -> anyhow::Result<()> {
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
        let src_server = RealizeServer::for_dir(src_dir.id(), src_dir.path()).as_inprocess_client();
        let dst_server = RealizeServer::for_dir(dst_dir.id(), dst_dir.path()).as_inprocess_client();
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut progress = MockProgress {
            log: Arc::clone(&log),
        };
        let (_success, _error) = move_files(
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
    async fn test_move_files() -> anyhow::Result<()> {
        let _ = env_logger::try_init();

        // Setup source directory with files
        let src_temp = TempDir::new()?;
        let src_dir = Arc::new(crate::server::Directory::new(
            &DirectoryId::from("testdir"),
            src_temp.path(),
        ));
        let src_server = RealizeServer::for_dir(src_dir.id(), src_dir.path()).as_inprocess_client();

        // Setup destination directory (empty)
        let dst_temp = TempDir::new()?;
        let dst_dir = Arc::new(crate::server::Directory::new(
            &DirectoryId::from("testdir"),
            dst_temp.path(),
        ));
        let dst_server = RealizeServer::for_dir(dst_dir.id(), dst_dir.path()).as_inprocess_client();

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
        let (success, error) = move_files(
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
    async fn test_move_files_chunked() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        const FILE_SIZE: usize = (1.25 * CHUNK_SIZE as f32) as usize; // 10MB
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
        let src_server = RealizeServer::for_dir(src_dir.id(), src_dir.path()).as_inprocess_client();

        let dst_temp = TempDir::new()?;
        let dst_dir = Arc::new(crate::server::Directory::new(
            &DirectoryId::from("testdir"),
            dst_temp.path(),
        ));
        let dst_server = RealizeServer::for_dir(dst_dir.id(), dst_dir.path()).as_inprocess_client();

        // Case 1: source > 8M, destination empty
        src_temp.child("large_empty").write_binary(&chunk)?;
        // Case 2: source > 8M, destination same size, but content is different
        src_temp.child("large_diff").write_binary(&chunk)?;
        dst_temp.child("large_diff").write_binary(&chunk2)?;
        // Case 3: source > 8M, destination truncated, shorter than 8M
        src_temp.child("large_trunc_short").write_binary(&chunk)?;
        dst_temp
            .child("large_trunc_short")
            .write_binary(&chunk3[..4 * 1024 * 1024])?;
        // Case 4: source > 8M, destination truncated, longer than 8M
        src_temp.child("large_trunc_long").write_binary(&chunk)?;
        dst_temp
            .child("large_trunc_long")
            .write_binary(&chunk4[..9 * 1024 * 1024])?;
        // Case 5: source > 8M, destination same content, with garbage at the end
        src_temp.child("large_garbage").write_binary(&chunk)?;
        let mut garbage = chunk.clone();
        garbage.extend_from_slice(&chunk5[..1024 * 1024]); // 1MB garbage
        dst_temp.child("large_garbage").write_binary(&garbage)?;

        let (success, error) = move_files(
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
                .expect(&format!("missing {path:?}"));
            assert_eq!(&found.1, &data, "content mismatch for {path:?}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_move_files_partial_error() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        let src_temp = TempDir::new()?;
        let src_dir = Arc::new(crate::server::Directory::new(
            &DirectoryId::from("testdir"),
            src_temp.path(),
        ));
        let src_server = RealizeServer::for_dir(src_dir.id(), src_dir.path()).as_inprocess_client();
        let dst_temp = TempDir::new()?;
        let dst_dir = Arc::new(crate::server::Directory::new(
            &DirectoryId::from("testdir"),
            dst_temp.path(),
        ));
        let dst_server = RealizeServer::for_dir(dst_dir.id(), dst_dir.path()).as_inprocess_client();
        // Good file
        src_temp.child("good").write_str("ok")?;
        // Unreadable file
        let unreadable = src_temp.child("bad");
        unreadable.write_str("fail")?;
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(unreadable.path(), fs::Permissions::from_mode(0o000))?;
        let (success, error) = move_files(
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
#[derive(Debug, Error)]
pub enum MoveFileError {
    #[error("RPC error: {0}")]
    Rpc(#[from] tarpc::client::RpcError),
    #[error("Error: {0}")]
    Realize(#[from] RealizeError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
