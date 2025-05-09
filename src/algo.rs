//! Move algorithm for Realize - Symmetric File Syncer
//!
//! Implements the unoptimized algorithm to move files from source (A) to destination (B)
//! using the RealizeService trait. See spec/design.md for details.

use crate::model::service::{DirectoryId, RealizeService, Result, SyncedFile, SyncedFileState};
use futures::future::{join, join_all};
use tarpc::context::Context;
use tokio::task;

const CHUNK_SIZE: u64 = 1024 * 1024; // 1MB

/// Moves files from source to destination using the RealizeService interface.
/// After a successful move and hash match, deletes the file from the source.
pub async fn move_files<S, D>(ctx: Context, src: &S, dst: &D, dir_id: DirectoryId) -> Result<()>
where
    S: RealizeService + Clone + Send + Sync + 'static,
    D: RealizeService + Clone + Send + Sync + 'static,
{
    // 1. List files on src and dst in parallel
    let (src_files, dst_files) = join(
        src.clone().list(ctx, dir_id.clone()),
        dst.clone().list(ctx, dir_id.clone()),
    )
    .await;
    let src_files = src_files?;
    let dst_files = dst_files?;

    // 2. Build lookup for dst
    use std::collections::HashMap;
    let dst_map: HashMap<_, _> = dst_files.into_iter().map(|f| (f.path, f.state)).collect();

    // 3. For all files in src, use iterator and run move_file concurrently with join_all
    let files_to_move: Vec<_> = src_files
        .iter()
        .filter(|file| {
            matches!(
                dst_map.get(&file.path),
                None | Some(SyncedFileState::Partial)
            )
        })
        .collect();

    let futures = files_to_move
        .iter()
        .map(|file| move_file(file, src, dst, &ctx, &dir_id));
    join_all(futures)
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    Ok(())
}

async fn move_file<S, D>(
    file: &SyncedFile,
    src: &S,
    dst: &D,
    ctx: &Context,
    dir_id: &DirectoryId,
) -> Result<()>
where
    S: RealizeService + Clone + Send + Sync + 'static,
    D: RealizeService + Clone + Send + Sync + 'static,
{
    let file_path = file.path.clone();
    let dir_id_clone = dir_id.clone();
    let ctx_clone = *ctx;
    // Start src_hash and dst_hash in parallel
    let src_hash_fut = src
        .clone()
        .hash(ctx_clone, dir_id_clone.clone(), file_path.clone());

    // File transfer
    let mut offset: u64 = 0;
    while offset < file.size {
        let mut end = offset + CHUNK_SIZE;
        if end > file.size {
            end = file.size;
        }
        let range = (offset, end);
        let data = src
            .clone()
            .read(*ctx, dir_id.clone(), file.path.clone(), range)
            .await?;
        dst.clone()
            .send(
                *ctx,
                dir_id.clone(),
                file.path.clone(),
                range,
                file.size,
                data,
            )
            .await?;
        offset = end;
    }

    // Await both hashes in parallel
    let dst_hash_fut = dst
        .clone()
        .hash(ctx_clone, dir_id_clone.clone(), file_path.clone());
    let (src_hash, dst_hash) = tokio::join!(src_hash_fut, dst_hash_fut);
    let src_hash = src_hash?;
    let dst_hash = dst_hash?;
    if src_hash == dst_hash {
        // Only finish and delete if hashes match
        dst.clone()
            .finish(*ctx, dir_id.clone(), file.path.clone())
            .await?;
        src.clone()
            .delete(*ctx, dir_id.clone(), file.path.clone())
            .await?;
    }
    // If hashes differ, leave file on both sides (future: handle with rsync/delta)
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::service::DirectoryId;
    use crate::server::RealizeServer;
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use assert_unordered::assert_eq_unordered;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_move_files() -> anyhow::Result<()> {
        // Setup source directory with files
        let src_temp = TempDir::new()?;
        src_temp.child("foo.txt").write_str("hello")?;
        src_temp.child("bar.txt").write_str("world")?;
        let src_dir = Arc::new(crate::server::Directory::new(
            &DirectoryId::from("testdir"),
            src_temp.path(),
        ));
        let src_server = RealizeServer::for_dir(src_dir.id(), src_dir.path());

        // Setup destination directory (empty)
        let dst_temp = TempDir::new()?;
        let dst_dir = Arc::new(crate::server::Directory::new(
            &DirectoryId::from("testdir"),
            dst_temp.path(),
        ));
        let dst_server = RealizeServer::for_dir(dst_dir.id(), dst_dir.path());

        move_files(
            tarpc::context::Context::current(),
            &src_server,
            &dst_server,
            DirectoryId::from("testdir"),
        )
        .await?;

        // Check that files are present in destination and not in source
        let files = dst_server
            .clone()
            .list(
                tarpc::context::Context::current(),
                DirectoryId::from("testdir"),
            )
            .await?;
        let file_names: Vec<_> = files
            .iter()
            .map(|f| f.path.file_name().unwrap().to_str().unwrap().to_string())
            .collect();
        assert_eq_unordered!(
            file_names,
            vec!["foo.txt".to_string(), "bar.txt".to_string()]
        );
        // Source should be empty
        let src_files = src_server
            .clone()
            .list(
                tarpc::context::Context::current(),
                DirectoryId::from("testdir"),
            )
            .await?;
        assert!(src_files.is_empty());
        Ok(())
    }
}
