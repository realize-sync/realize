//! Move algorithm for Realize - Symmetric File Syncer
//!
//! Implements the unoptimized algorithm to move files from source (A) to destination (B)
//! using the RealizeService trait. See spec/design.md for details.

use crate::model::service::{DirectoryId, RealizeService, Result, SyncedFile, SyncedFileState};
use std::path::PathBuf;
use tarpc::context::Context;

const CHUNK_SIZE: u64 = 1024 * 1024; // 1MB

/// Copies files from source to destination using the RealizeService interface.
///
/// # Arguments
/// * `ctx` - tarpc context to pass to service methods
/// * `src` - Source implementing RealizeService
/// * `dst` - Destination implementing RealizeService
/// * `dir_id` - DirectoryId to copy
pub async fn copy_files<S, D>(ctx: Context, src: &S, dst: &D, dir_id: DirectoryId) -> Result<()>
where
    S: RealizeService + Clone + Send + Sync + 'static,
    D: RealizeService + Clone + Send + Sync + 'static,
{
    // 1. List files on src and dst
    let src_files = src.clone().list(ctx.clone(), dir_id.clone()).await?;
    let dst_files = dst.clone().list(ctx.clone(), dir_id.clone()).await?;

    // 2. Build lookup for dst
    use std::collections::HashMap;
    let mut dst_map: HashMap<PathBuf, SyncedFileState> = HashMap::new();
    for f in dst_files {
        dst_map.insert(f.path.clone(), f.state);
    }

    // 3. For all files in src
    for file in src_files {
        match dst_map.get(&file.path) {
            None | Some(SyncedFileState::Partial) => {
                // Not in B or partial in B: copy/overwrite
                copy_file(&file, src, dst, &ctx, &dir_id).await?;
            }
            Some(SyncedFileState::Final) => {
                // Already present and final, skip
            }
        }
    }
    Ok(())
}

async fn copy_file<S, D>(
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
    let mut offset: u64 = 0;
    while offset < file.size {
        let mut end = offset + CHUNK_SIZE;
        if end > file.size {
            end = file.size;
        }
        let range = (offset, end);
        let data = src
            .clone()
            .read(ctx.clone(), dir_id.clone(), file.path.clone(), range)
            .await?;
        dst.clone()
            .send(
                ctx.clone(),
                dir_id.clone(),
                file.path.clone(),
                range,
                file.size,
                data,
            )
            .await?;
        offset = end;
    }
    dst.clone()
        .finish(ctx.clone(), dir_id.clone(), file.path.clone())
        .await?;

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
    async fn test_copy_files() -> anyhow::Result<()> {
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

        copy_files(
            tarpc::context::Context::current(),
            &src_server,
            &dst_server,
            DirectoryId::from("testdir"),
        )
        .await?;

        // Check that files are present in destination
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
        Ok(())
    }
}
