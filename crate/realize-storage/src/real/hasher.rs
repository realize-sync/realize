use std::path::PathBuf;

use crate::utils::hash;
use futures::TryStreamExt as _;
use realize_types::{self, Hash, UnixTime};
use tokio::fs::File;
use tokio::sync::mpsc;
use tokio_util::io::ReaderStream;

pub struct HashResult {
    pub hash: Hash,

    /// Modification time taken at the time the file was open, for verification.
    pub mtime: UnixTime,

    /// Size taken at the time the file was open, for verification.
    pub size: u64,
}

pub struct Hasher {
    tx: mpsc::Sender<(realize_types::Path, std::io::Result<HashResult>)>,
}

impl Hasher {
    pub fn new(tx: mpsc::Sender<(realize_types::Path, std::io::Result<HashResult>)>) -> Self {
        Self { tx }
    }

    pub fn request_hash(&self, realpath: PathBuf, path: realize_types::Path) {
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let _ = tx.send((path.clone(), do_hash(realpath).await)).await;
        });
    }
}

async fn do_hash(realpath: PathBuf) -> std::io::Result<HashResult> {
    let f = File::open(&realpath).await?;

    // Take the metadata on the open file, to be sure that that's what we hashed.
    let m = f.metadata().await?;
    let mtime = UnixTime::mtime(&m);
    let size = m.len();

    let mut hasher = hash::running();
    ReaderStream::with_capacity(f, 64 * 1024)
        .try_for_each(|chunk| {
            hasher.update(chunk);

            std::future::ready(Ok(()))
        })
        .await?;

    let hash = hasher.finalize();

    Ok(HashResult { hash, mtime, size })
}
