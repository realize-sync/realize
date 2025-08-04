use std::path::PathBuf;

use crate::StorageError;
use crate::utils::hash::{self};
use futures::TryStreamExt as _;
use realize_types::{self, Hash, Path, UnixTime};
use tokio::fs::{self, File};
use tokio::io::AsyncRead;
use tokio::sync::{broadcast, mpsc};
use tokio_util::io::ReaderStream;
use tokio_util::task::JoinMap;

use super::index::RealIndexAsync;

#[derive(Debug)]
enum Verdict {
    /// File was deleted or is not accessible anymore.
    Gone,

    /// The file is still being modified; disregard this result.
    Inconsistent,

    /// Hashed file content, modification time and size.
    FileContent(Hash, UnixTime, u64),
}

/// A type that puts new file into the index or remove deleted files.
pub(crate) struct Hasher {
    tx: mpsc::Sender<(PathBuf, Path)>,
}

impl Hasher {
    pub(crate) fn spawn(index: RealIndexAsync, shutdown_rx: broadcast::Receiver<()>) -> Self {
        let (tx, rx) = mpsc::channel(16);

        tokio::spawn(hasher_loop(index, rx, shutdown_rx));

        Self { tx }
    }

    /// Check the given `realpath`.
    ///
    /// If it exists, hash it and add it to the index with that ash.
    /// If it doesn't exist, remove it from the index.
    ///
    /// This handles conflicting updates; calling check_path on a path
    /// currently being processes cancels that processing so we only
    /// look at the latest state.
    pub(crate) async fn check_path(
        &self,
        realpath: PathBuf,
        path: realize_types::Path,
    ) -> anyhow::Result<()> {
        Ok(self.tx.send((realpath, path)).await?)
    }
}

async fn hasher_loop(
    index: RealIndexAsync,
    mut rx: mpsc::Receiver<(PathBuf, Path)>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let mut map: JoinMap<Path, Result<Verdict, std::io::Error>> = JoinMap::new();
    let arena = index.arena();
    loop {
        tokio::select!(
            _ = shutdown_rx.recv() =>{
                return;
            }
            e = rx.recv() => {
                match e {
                    None => {
                        return;
                    }
                    Some((realpath, path)) => {
                        // Any previous run of do_hash currently
                        // running for this path is cancelled by this
                        // call; we only want to hash the very latest
                        // version.
                        map.spawn(path.clone(), do_hash(realpath));
                    }
                }
            }
            Some((path, verdict)) = map.join_next() => {
                match verdict {
                   Ok(Ok(verdict)) => {
                       log::debug!("[{arena}] {path}: hasher:  {verdict:?}");
                       if let Err(err) = apply_verdict(&index, &path, verdict).await {
                           log::debug!("[{arena}] error updating index for {path}: {err}")
                       }
                   }
                    Ok(Err(err)) =>{
                        log::debug!("[{arena}] {path}: hasher:  {err}");
                    }
                    Err(_) => {}
                }
            }
        );
    }
}

async fn apply_verdict(
    index: &RealIndexAsync,
    path: &Path,
    verdict: Verdict,
) -> Result<(), StorageError> {
    match verdict {
        Verdict::Inconsistent => {
            // Ignore this result. Another one will follow later on.
            Ok(())
        }
        Verdict::Gone => index.remove_file_or_dir(path).await,
        Verdict::FileContent(hash, mtime, size) => index.add_file(path, size, &mtime, hash).await,
    }
}

async fn do_hash(realpath: PathBuf) -> Result<Verdict, std::io::Error> {
    // TODO: optionally sleep, to debounce.

    let (start_mtime, start_size) = match fs::symlink_metadata(&realpath).await {
        Err(_) => {
            return Ok(Verdict::Gone);
        }
        Ok(m) => (UnixTime::mtime(&m), m.len()),
    };

    // TODO: limit the number of parallel hashes running at a time
    // with a semaphore. This is important for large files.
    let hash = match File::open(&realpath).await {
        Err(_) => {
            return Ok(Verdict::Gone);
        }
        Ok(f) => hash_file(f).await?,
    };

    let end_m = fs::symlink_metadata(&realpath).await?;
    if end_m.len() != start_size || UnixTime::mtime(&end_m) != start_mtime {
        return Ok(Verdict::Inconsistent);
    }

    Ok(Verdict::FileContent(hash, start_mtime, start_size))
}

pub(crate) async fn hash_file<R: AsyncRead>(f: R) -> Result<Hash, std::io::Error> {
    let mut hasher = hash::running();
    ReaderStream::with_capacity(f, 8 * 1024)
        .try_for_each(|chunk| {
            hasher.update(chunk);

            std::future::ready(Ok(()))
        })
        .await?;
    let hash = hasher.finalize();
    Ok(hash)
}
