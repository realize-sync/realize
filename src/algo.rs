//! Move algorithm for Realize - Symmetric File Syncer
//!
//! Implements the unoptimized algorithm to move files from source (A) to destination (B)
//! using the RealizeService trait. See spec/design.md for details.

use crate::model::service::{DirectoryId, RealizeError, RealizeServiceClient, SyncedFile};
use futures::future::{join, join_all};
use std::{collections::HashMap, path::Path};
use tarpc::context::Context;

const CHUNK_SIZE: u64 = 8 * 1024 * 1024; // 8MB

/// Moves files from source to destination using the RealizeService interface.
/// After a successful move and hash match, deletes the file from the source.
pub async fn move_files(
    ctx: Context,
    src: &RealizeServiceClient,
    dst: &RealizeServiceClient,
    dir_id: DirectoryId,
) -> anyhow::Result<()> {
    // 1. List files on src and dst in parallel
    let (src_files, dst_files) =
        join(src.list(ctx, dir_id.clone()), dst.list(ctx, dir_id.clone())).await;
    let src_files = src_files??;
    let dst_files = dst_files??;

    // 2. Build lookup for dst

    let dst_map: HashMap<_, _> = dst_files
        .into_iter()
        .map(|f| (f.path.to_path_buf(), f))
        .collect();
    let futures = src_files
        .iter()
        .map(|file| move_file(src, file, dst, dst_map.get(&file.path), &ctx, &dir_id));
    join_all(futures)
        .await
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(())
}

async fn move_file(
    src: &RealizeServiceClient,
    src_file: &SyncedFile,
    dst: &RealizeServiceClient,
    dst_file: Option<&SyncedFile>,
    ctx: &Context,
    dir_id: &DirectoryId,
) -> anyhow::Result<()> {
    let path = src_file.path.as_path();
    let src_size = src_file.size;
    let dst_size = dst_file.map(|f| f.size).unwrap_or(0);

    if src_size == dst_size && check_hashes_and_delete(src, dst, ctx, dir_id, &path).await? {
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
                .read(*ctx, dir_id.clone(), path.to_path_buf(), range)
                .await??;
            dst.send(
                *ctx,
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
                .calculate_signature(*ctx, dir_id.clone(), path.to_path_buf(), range)
                .await??;
            let delta = src
                .diff(*ctx, dir_id.clone(), path.to_path_buf(), range, sig)
                .await??;
            dst.apply_delta(
                *ctx,
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

    if !check_hashes_and_delete(src, dst, ctx, dir_id, &path).await? {
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
async fn check_hashes_and_delete(
    src: &RealizeServiceClient,
    dst: &RealizeServiceClient,
    ctx: &Context,
    dir_id: &DirectoryId,
    path: &Path,
) -> Result<bool, anyhow::Error> {
    let (src_hash, dst_hash) = tokio::join!(
        src.hash(*ctx, dir_id.clone(), path.to_path_buf()),
        dst.hash(*ctx, dir_id.clone(), path.to_path_buf()),
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
    dst.finish(*ctx, dir_id.clone(), path.to_path_buf())
        .await??;
    src.delete(*ctx, dir_id.clone(), path.to_path_buf())
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
    use std::sync::Arc;

    #[tokio::test]
    async fn test_move_files() -> anyhow::Result<()> {
        let _ = env_logger::try_init();

        // Setup source directory with files
        let src_temp = TempDir::new()?;
        src_temp.child("foo.txt").write_str("hello")?;
        src_temp.child("bar.txt").write_str("world")?;
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
        dst_temp.child("foo.txt").write_str("xxxxx")?; // same length as "hello"

        // Corrupted dst file in partial state
        dst_temp.child(".bar.txt.part").write_str("corrupt")?;

        // Corrupted dst file in final state
        dst_temp.child("baz.txt").write_str("corruptfinal")?;
        src_temp.child("baz.txt").write_str("bazgood")?;

        // Partially copied dst file (shorter than src)
        dst_temp.child("quux.txt").write_str("par")?;
        src_temp.child("quux.txt").write_str("partialcopy")?;

        // Dst file too long
        dst_temp.child("foobar.txt").write_str("foobarfoobar")?;
        src_temp.child("foobar.txt").write_str("foobar")?;

        // Interrupted file (partial file, correct prefix)
        dst_temp
            .child(".interrupted.txt.part")
            .write_str("interr")?;
        src_temp
            .child("interrupted.txt")
            .write_str("interruptedfull")?;

        println!("test_move_files: all files set up");
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
            .await??;
        let file_names: Vec<_> = files
            .iter()
            .map(|f| f.path.file_name().unwrap().to_str().unwrap().to_string())
            .collect();
        assert_eq_unordered!(
            file_names,
            vec![
                "foo.txt".to_string(),
                "bar.txt".to_string(),
                "baz.txt".to_string(),
                "foobar.txt".to_string(),
                "quux.txt".to_string(),
                "interrupted.txt".to_string()
            ]
        );
        // Source should be empty
        let src_files = src_server
            .clone()
            .list(
                tarpc::context::Context::current(),
                DirectoryId::from("testdir"),
            )
            .await??;
        if !src_files.is_empty() {
            println!("Remaining files in source: {:?}", src_files);
        }
        assert!(src_files.is_empty());
        // Check that all files have the correct content in destination
        let foo_content = std::fs::read_to_string(dst_temp.child("foo.txt"))?;
        let bar_content = std::fs::read_to_string(dst_temp.child("bar.txt"))?;
        let baz_content = std::fs::read_to_string(dst_temp.child("baz.txt"))?;
        let foobar_content = std::fs::read_to_string(dst_temp.child("foobar.txt"))?;
        let quux_content = std::fs::read_to_string(dst_temp.child("quux.txt"))?;
        let interrupted_content = std::fs::read_to_string(dst_temp.child("interrupted.txt"))?;
        assert_eq!(foo_content, "hello");
        assert_eq!(bar_content, "world");
        assert_eq!(baz_content, "bazgood");
        assert_eq!(foobar_content, "foobar");
        assert_eq!(quux_content, "partialcopy");
        assert_eq!(interrupted_content, "interruptedfull");
        Ok(())
    }
}
