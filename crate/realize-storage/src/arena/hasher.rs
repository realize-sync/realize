use super::db::ArenaDatabase;
use super::index;
use crate::StorageError;
use crate::utils::hash::{self};
use futures::TryStreamExt as _;
use realize_types::{self, Arena, Hash, Path, UnixTime};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::AsyncRead;
use tokio::sync::{Semaphore, broadcast, mpsc};
use tokio_util::io::ReaderStream;
use tokio_util::task::JoinMap;

#[derive(Debug)]
enum Request {
    HashFileVersion(Path, UnixTime, u64),
    RemoveFileVersion(Path),
}

#[derive(Clone, Default, Debug)]
pub(crate) struct HasherOptions {
    pub debounce: Duration,
    pub max_parallelism: usize,
}

/// A type that puts new file into the index or remove deleted files.
pub(crate) struct Hasher {
    arena: Arena,
    tx: mpsc::Sender<Request>,
}

impl Hasher {
    pub(crate) fn spawn(
        root: PathBuf,
        db: Arc<ArenaDatabase>,
        shutdown_rx: broadcast::Receiver<()>,
        options: &HasherOptions,
    ) -> Self {
        log::debug!("[{}] Setup with {options:?}", db.arena());

        let (tx, rx) = mpsc::channel(16);
        let sem = if options.max_parallelism > 0 {
            Some(Arc::new(Semaphore::new(options.max_parallelism)))
        } else {
            None
        };

        let arena = db.arena();
        tokio::spawn(hasher_loop(
            root,
            db,
            rx,
            shutdown_rx,
            sem,
            options.debounce,
        ));

        Self { arena, tx }
    }

    /// Hash content of the given path.
    ///
    /// This handles conflicting updates; calling check_path on a path
    /// currently being processes cancels that processing so we only
    /// look at the latest state.
    pub(crate) async fn hash_content(
        &self,
        path: &realize_types::Path,
        mtime: UnixTime,
        size: u64,
    ) -> anyhow::Result<()> {
        log::info!("[{}] Hash requested for \"{path}\"", self.arena);
        Ok(self
            .tx
            .send(Request::HashFileVersion(path.clone(), mtime.clone(), size))
            .await?)
    }

    /// Remove the given path from the index.
    ///
    /// This debounces, in case the path is written again.
    pub(crate) async fn remove_content(&self, path: &realize_types::Path) -> anyhow::Result<()> {
        Ok(self
            .tx
            .send(Request::RemoveFileVersion(path.clone()))
            .await?)
    }
}

async fn hasher_loop(
    root: PathBuf,
    db: Arc<ArenaDatabase>,
    mut rx: mpsc::Receiver<Request>,
    mut shutdown_rx: broadcast::Receiver<()>,
    sem: Option<Arc<Semaphore>>,
    debounce: Duration,
) {
    let root = root;
    let mut map: JoinMap<Path, Result<(), StorageError>> = JoinMap::new();
    let arena = db.arena();

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
                    Some(Request::HashFileVersion(path, mtime, size)) => {
                        // Any previous run of do_hash currently
                        // running for this path is cancelled by this
                        // call; we only want to hash the very latest
                        // version.
                        map.spawn(path.clone(), add_version(db.clone(), path, root.clone(), mtime, size, sem.clone(), debounce));
                    }
                    Some(Request::RemoveFileVersion(path)) => {
                        map.spawn(path.clone(), remove_version(db.clone(), path, root.clone(), debounce));
                    }
                }
            }
            Some((path, result)) = map.join_next() => {
                if let Ok(Err(err)) = result {
                    log::debug!("[{arena}] Error for \"{path}\": {err}");
                }
            }
        );
    }
}

async fn add_version(
    db: Arc<ArenaDatabase>,
    path: Path,
    root: PathBuf,
    mtime: UnixTime,
    size: u64,
    sem: Option<Arc<Semaphore>>,
    debounce: Duration,
) -> Result<(), StorageError> {
    if !debounce.is_zero() {
        tokio::time::sleep(debounce).await;
    }

    let hash = if size > 0 {
        let _permit = if let Some(sem) = &sem {
            Some(sem.acquire().await.unwrap())
        } else {
            None
        };

        hash_file(File::open(path.within(&root)).await?).await?
    } else {
        hash::empty()
    };
    let arena = db.arena();
    log::info!("[{arena}] Hashed: \"{path}\" {hash} size={size}");
    if !index::add_file_if_matches_async(&db, &root, &path, size, mtime, hash).await? {
        log::debug!("[{arena}] Mismatch; Skipped adding \"{path}\"",);
    }
    Ok(())
}

async fn remove_version(
    db: Arc<ArenaDatabase>,
    path: Path,
    root: PathBuf,
    debounce: Duration,
) -> Result<(), StorageError> {
    if !debounce.is_zero() {
        tokio::time::sleep(debounce).await;
    }

    let arena = db.arena();
    log::info!("[{arena}] Remove: \"{path}\"");
    if !index::remove_file_if_missing_async(&db, &root, &path).await? {
        log::debug!(
            "[{arena}] Mismatch; Skipped removing \"{path}\"",
            arena = db.arena()
        );
    }

    Ok(())
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
    use crate::arena::db::ArenaDatabase;
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use realize_types::Arena;

    use super::*;

    struct Fixture {
        db: Arc<ArenaDatabase>,
        tempdir: TempDir,
        shutdown_tx: broadcast::Sender<()>,
        options: HasherOptions,
    }

    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();
            let tempdir = TempDir::new()?;
            let arena = Arena::from("myarena");
            let db = ArenaDatabase::for_testing_single_arena(arena, &tempdir.path().join("blobs"))?;

            let (shutdown_tx, _) = broadcast::channel(1);
            let options = HasherOptions::default();

            Ok(Self {
                db,
                tempdir,
                shutdown_tx,
                options,
            })
        }

        fn hasher(&self) -> Hasher {
            Hasher::spawn(
                self.tempdir.to_path_buf(),
                Arc::clone(&self.db),
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
        let mtime = UnixTime::mtime(&foo.path().metadata()?);

        let mut history_rx = fixture.db.history().watch();
        hasher.hash_content(&Path::parse("foo")?, mtime, 12).await?;
        tokio::time::timeout(Duration::from_secs(3), history_rx.changed()).await??;

        let entry = index::get_file_async(&fixture.db, &Path::parse("foo")?)
            .await?
            .unwrap();
        assert_eq!(hash::digest("some content"), entry.hash);
        assert_eq!(12, entry.size);
        assert_eq!(mtime, entry.mtime);

        Ok(())
    }

    #[tokio::test]
    async fn hash_empty_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let hasher = fixture.hasher();

        let foo = fixture.tempdir.child("foo");
        foo.write_str("")?;
        let mtime = UnixTime::mtime(&foo.path().metadata()?);
        let mut history_rx = fixture.db.history().watch();
        hasher.hash_content(&Path::parse("foo")?, mtime, 0).await?;
        tokio::time::timeout(Duration::from_secs(3), history_rx.changed()).await??;

        let entry = index::get_file_async(&fixture.db, &Path::parse("foo")?)
            .await?
            .unwrap();
        assert_eq!(hash::empty(), entry.hash);
        assert_eq!(0, entry.size);
        assert_eq!(mtime, entry.mtime);

        Ok(())
    }

    #[tokio::test]
    async fn finally_hash_file() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture.options.debounce = Duration::from_secs(30);

        let hasher = fixture.hasher();
        let foo = fixture.tempdir.child("foo");
        let path = Path::parse("foo")?;

        foo.write_str("one")?;
        hasher
            .hash_content(&path, UnixTime::mtime(&foo.path().metadata()?), 3)
            .await?;
        foo.write_str("two!")?;
        hasher
            .hash_content(&path, UnixTime::mtime(&foo.path().metadata()?), 4)
            .await?;

        foo.write_str("three")?;
        tokio::time::pause(); // new sleep calls returns immediately
        hasher
            .hash_content(&path, UnixTime::mtime(&foo.path().metadata()?), 5)
            .await?;

        let mut history_rx = fixture.db.history().watch();
        history_rx.changed().await?; // timeout not available when paused
        let history_index = *history_rx.borrow_and_update();

        // The version that is finally hashed must be the last one.
        let entry = index::get_file_async(&fixture.db, &Path::parse("foo")?)
            .await?
            .unwrap();
        assert_eq!(hash::digest("three"), entry.hash);

        // and this is the first entry that was written (intermediate values were not hashed).g
        assert_eq!(1, history_index);

        Ok(())
    }

    #[tokio::test]
    async fn remove_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let hasher = fixture.hasher();

        index::add_file_async(
            &fixture.db,
            &Path::parse("foo")?,
            3,
            UnixTime::from_secs(1234567890),
            hash::digest("foo"),
        )
        .await?;

        let mut history_rx = fixture.db.history().watch();
        hasher.remove_content(&Path::parse("foo")?).await?;
        tokio::time::timeout(Duration::from_secs(3), history_rx.changed()).await??;

        assert!(!index::has_file_async(&fixture.db, &Path::parse("foo")?).await?);

        Ok(())
    }

    #[tokio::test]
    async fn hash_file_that_reappeares() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture.options.debounce = Duration::from_secs(30);

        let hasher = fixture.hasher();
        let foo = fixture.tempdir.child("foo");
        let path = Path::parse("foo")?;
        index::add_file_async(
            &fixture.db,
            &path,
            3,
            UnixTime::from_secs(1234567890),
            hash::digest("foo"),
        )
        .await?;

        let mut history_rx = fixture.db.history().watch();
        hasher.remove_content(&Path::parse("foo")?).await?;

        foo.write_str("new!")?;
        tokio::time::pause(); // new sleep calls returns immediately
        hasher
            .hash_content(&path, UnixTime::mtime(&foo.path().metadata()?), 4)
            .await?;

        history_rx.changed().await?; // cannot use timeout when time paused
        let history_index = *history_rx.borrow_and_update();

        let entry = index::get_file_async(&fixture.db, &Path::parse("foo")?)
            .await?
            .unwrap();
        assert_eq!(hash::digest("new!"), entry.hash);

        // one entry for add, one for replace; the removal was skipped
        assert_eq!(2, history_index);

        Ok(())
    }
}
