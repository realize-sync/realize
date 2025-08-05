use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::StorageError;
use crate::utils::hash::{self};
use futures::TryStreamExt as _;
use realize_types::{self, Hash, Path, UnixTime};
use tokio::fs::{self, File};
use tokio::io::AsyncRead;
use tokio::sync::{Semaphore, broadcast, mpsc};
use tokio_util::io::ReaderStream;
use tokio_util::task::JoinMap;

use super::index::RealIndexAsync;

#[derive(Debug)]
enum Verdict {
    /// File was deleted or is not accessible anymore.
    Gone,

    /// Hashed file content, modification time and size.
    FileContent(Hash, UnixTime, u64),
}

#[derive(Clone, Default, Debug)]
pub(crate) struct HasherOptions {
    pub debounce: Duration,
    pub max_parallelism: usize,
}

/// A type that puts new file into the index or remove deleted files.
pub(crate) struct Hasher {
    tx: mpsc::Sender<Path>,
}

impl Hasher {
    pub(crate) fn spawn(
        root: PathBuf,
        index: RealIndexAsync,
        shutdown_rx: broadcast::Receiver<()>,
        options: &HasherOptions,
    ) -> Self {
        log::debug!(
            "[{}] Starting hasher with options {options:?}",
            index.arena()
        );

        let (tx, rx) = mpsc::channel(16);
        let sem = if options.max_parallelism > 0 {
            Some(Arc::new(Semaphore::new(options.max_parallelism)))
        } else {
            None
        };

        tokio::spawn(hasher_loop(
            root,
            index,
            rx,
            shutdown_rx,
            sem,
            options.debounce,
        ));

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
    pub(crate) async fn check_path(&self, path: realize_types::Path) -> anyhow::Result<()> {
        Ok(self.tx.send(path).await?)
    }
}

async fn hasher_loop(
    root: PathBuf,
    index: RealIndexAsync,
    mut rx: mpsc::Receiver<Path>,
    mut shutdown_rx: broadcast::Receiver<()>,
    sem: Option<Arc<Semaphore>>,
    debounce: Duration,
) {
    let root = root;
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
                    Some(path) => {
                        // Any previous run of do_hash currently
                        // running for this path is cancelled by this
                        // call; we only want to hash the very latest
                        // version.
                        map.spawn(path.clone(), do_hash(path.within(&root), sem.clone(), debounce));
                    }
                }
            }
            Some((path, verdict)) = map.join_next() => {
                match verdict {
                   Ok(Ok(verdict)) => {
                       log::debug!("[{arena}] {path}: hasher:  {verdict:?}");
                       if let Err(err) = apply_verdict(&root, &index, &path, verdict).await {
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
    root: &std::path::Path,
    index: &RealIndexAsync,
    path: &Path,
    verdict: Verdict,
) -> Result<(), StorageError> {
    match verdict {
        Verdict::Gone => index.remove_file_or_dir(path).await,
        Verdict::FileContent(hash, mtime, size) => {
            if !index
                .add_file_if_matches(path, size, &mtime, hash, &path.within(root))
                .await?
            {
                log::debug!("Mimsatch; skipped adding {path}");
            }

            Ok(())
        }
    }
}

async fn do_hash(
    realpath: PathBuf,
    sem: Option<Arc<Semaphore>>,
    debounce: Duration,
) -> Result<Verdict, std::io::Error> {
    if !debounce.is_zero() {
        tokio::time::sleep(debounce).await;
    }

    let (start_mtime, start_size) = match fs::symlink_metadata(&realpath).await {
        Err(_) => {
            return Ok(Verdict::Gone);
        }
        Ok(m) => (UnixTime::mtime(&m), m.len()),
    };

    if start_size == 0 {
        // No need to bother reading the file
        return Ok(Verdict::FileContent(hash::empty(), start_mtime, start_size));
    }

    let _permit = if let Some(sem) = &sem {
        Some(sem.acquire().await.unwrap())
    } else {
        None
    };

    let hash = match File::open(&realpath).await {
        Err(_) => {
            return Ok(Verdict::Gone);
        }
        Ok(f) => hash_file(f).await?,
    };

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

#[cfg(test)]
mod tests {
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use realize_types::Arena;

    use crate::DirtyPaths;
    use crate::arena::db::ArenaDatabase;
    use crate::arena::index::RealIndexBlocking;
    use crate::utils::redb_utils;

    use super::*;

    struct Fixture {
        index: RealIndexAsync,
        tempdir: TempDir,
        shutdown_tx: broadcast::Sender<()>,
        options: HasherOptions,
    }

    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();

            let _ = env_logger::try_init();
            let arena = Arena::from("myarena");
            let db = ArenaDatabase::new(redb_utils::in_memory()?)?;
            let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;
            let index = RealIndexBlocking::new(arena, db, dirty_paths)?.into_async();
            let tempdir = TempDir::new()?;
            let (shutdown_tx, _) = broadcast::channel(1);
            let options = HasherOptions::default();

            Ok(Self {
                index,
                tempdir,
                shutdown_tx,
                options,
            })
        }

        fn hasher(&self) -> Hasher {
            Hasher::spawn(
                self.tempdir.to_path_buf(),
                self.index.clone(),
                self.shutdown_tx.subscribe(),
                &self.options,
            )
        }
    }

    #[tokio::test]
    async fn hash_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let hasher = fixture.hasher();

        let foo = fixture.tempdir.child("foo");
        foo.write_str("some content")?;

        let mut history_rx = fixture.index.watch_history();
        hasher.check_path(Path::parse("foo")?).await?;
        history_rx.changed().await?;

        let entry = fixture.index.get_file(&Path::parse("foo")?).await?.unwrap();
        assert_eq!(hash::digest("some content"), entry.hash);
        assert_eq!(12, entry.size);
        assert_eq!(UnixTime::mtime(&foo.path().metadata()?), entry.mtime);

        Ok(())
    }

    #[tokio::test]
    async fn hash_empty_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let hasher = fixture.hasher();

        let foo = fixture.tempdir.child("foo");
        foo.write_str("")?;

        let mut history_rx = fixture.index.watch_history();
        hasher.check_path(Path::parse("foo")?).await?;
        history_rx.changed().await?;

        let entry = fixture.index.get_file(&Path::parse("foo")?).await?.unwrap();
        assert_eq!(hash::empty(), entry.hash);
        assert_eq!(0, entry.size);
        assert_eq!(UnixTime::mtime(&foo.path().metadata()?), entry.mtime);

        Ok(())
    }

    #[tokio::test]
    async fn delete_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let hasher = fixture.hasher();

        let foo = Path::parse("foo")?;
        fixture
            .index
            .add_file(
                &foo,
                3,
                &UnixTime::from_secs(1234567890),
                hash::digest("foo"),
            )
            .await?;

        let mut history_rx = fixture.index.watch_history();
        hasher.check_path(foo.clone()).await?;
        history_rx.changed().await?;

        assert!(!fixture.index.has_file(&foo).await?);

        Ok(())
    }

    #[tokio::test]
    async fn finally_hash_file() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture.options.debounce = Duration::from_secs(30);

        let hasher = fixture.hasher();
        let foo = fixture.tempdir.child("foo");
        let path = Path::parse("foo")?;

        foo.write_str("content1")?;
        hasher.check_path(path.clone()).await?;
        foo.write_str("content2")?;
        hasher.check_path(path.clone()).await?;
        foo.write_str("content3")?;
        hasher.check_path(path.clone()).await?;

        let mut history_rx = fixture.index.watch_history();
        tokio::time::pause(); // hasher stops sleeping

        history_rx.changed().await?;
        let history_index = *history_rx.borrow_and_update();

        // The version that is finally hashed must be the last one.
        let entry = fixture.index.get_file(&Path::parse("foo")?).await?.unwrap();
        assert_eq!(hash::digest("content3"), entry.hash);

        // and this is the first entry that was written (intermediate values were not hashed).g
        assert_eq!(1, history_index);

        Ok(())
    }

    #[tokio::test]
    async fn finally_delete_file() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture.options.debounce = Duration::from_secs(30);

        let hasher = fixture.hasher();
        let foo = fixture.tempdir.child("foo");
        let realpath = foo.to_path_buf();
        let path = Path::parse("foo")?;

        fixture
            .index
            .add_file(
                &path,
                3,
                &UnixTime::from_secs(1234567890),
                hash::digest("foo"),
            )
            .await?;

        foo.write_str("content1")?;
        hasher.check_path(path.clone()).await?;
        foo.write_str("content2")?;
        hasher.check_path(path.clone()).await?;
        fs::remove_file(&realpath).await?;
        hasher.check_path(path.clone()).await?;

        let mut history_rx = fixture.index.watch_history();
        tokio::time::pause(); // hasher stops sleeping

        history_rx.changed().await?;
        let history_index = *history_rx.borrow_and_update();

        assert!(!fixture.index.has_file(&path).await?);

        // 1 entry for add_file and one for delete; intermediate
        // changes were just not recorded;
        assert_eq!(2, history_index);

        Ok(())
    }
}
