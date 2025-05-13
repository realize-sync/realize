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

const CHUNK_SIZE: u64 = 8 * 1024 * 1024; // 8MB

/// Moves files from source to destination using the RealizeService interface.
/// After a successful move and hash match, deletes the file from the source.
/// Returns (success_count, error_count).
pub async fn move_files<T, U>(
    src: &RealizeServiceClient<T>,
    dst: &RealizeServiceClient<U>,
    dir_id: DirectoryId,
) -> anyhow::Result<(usize, usize)>
where
    T: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
    U: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
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
    let mut handles = vec![];
    for file in src_files.iter() {
        let fut = move_file(src, file, dst, dst_map.get(&file.path), &dir_id);
        handles.push((file.path.clone(), fut));
    }
    let results = join_all(
        handles
            .into_iter()
            .map(|(path, fut)| async move { (path, fut.await) }),
    )
    .await;
    for (path, res) in results {
        match res {
            Ok(_) => success_count += 1,
            Err(e) => {
                log::error!("{}:{:?} failed to move: {}", dir_id, path, e);
                error_count += 1;
            }
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
) -> anyhow::Result<()>
where
    T: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
    U: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
{
    let path = src_file.path.as_path();
    let src_size = src_file.size;
    let dst_size = dst_file.map(|f| f.size).unwrap_or(0);

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
                data,
            )
            .await??;
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
        }
        offset = end;
    }

    if !check_hashes_and_delete(src, dst, dir_id, &path).await? {
        return Err(RealizeError::Sync(
            path.to_path_buf(),
            "Data still inconsistent after sync".to_string(),
        )
        .into());
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
) -> Result<bool, anyhow::Error>
where
    T: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
    U: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
{
    let (src_hash, dst_hash) = tokio::join!(
        src.hash(context::current(), dir_id.clone(), path.to_path_buf()),
        dst.hash(context::current(), dir_id.clone(), path.to_path_buf()),
    );
    let src_hash = src_hash??;
    let dst_hash = dst_hash??;
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
        .await??;
    src.delete(context::current(), dir_id.clone(), path.to_path_buf())
        .await??;
    return Ok(true);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::service::DirectoryId;
    use crate::server::RealizeServer;
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use assert_unordered::assert_eq_unordered;
    use std::path::PathBuf;
    use std::sync::Arc;
    use walkdir::WalkDir;

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
        let (success, error) =
            move_files(&src_server, &dst_server, DirectoryId::from("testdir")).await?;
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

        let (success, error) =
            move_files(&src_server, &dst_server, DirectoryId::from("testdir")).await?;
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
        let (success, error) =
            move_files(&src_server, &dst_server, DirectoryId::from("testdir")).await?;
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
