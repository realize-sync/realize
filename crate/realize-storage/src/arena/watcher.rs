use super::db::ArenaDatabase;
use super::index;
use crate::StorageError;
use crate::arena::blob::WritableOpenBlob;
use crate::arena::cache::{CacheReadOperations, WritableOpenCache};
use crate::arena::db::Tag;
use crate::arena::dirty::WritableOpenDirty;
use crate::arena::history::WritableOpenHistory;
use crate::arena::tree::{TreeExt, TreeReadOperations, WritableOpenTree};
use crate::arena::types::CacheEntryStatus;
use crate::utils::debouncer::DebouncerMap;
use crate::utils::{fs_utils, hash};
use notify::event::{CreateKind, MetadataKind, ModifyKind};
use notify::{Event, EventKind, RecommendedWatcher, Watcher as _};
use realize_types::{self, Path, UnixTime};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::{self, File};
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::task;

/// Watch an arena directory and update its index.
///
/// This is created with `RealWatcher::builder()`
pub struct RealWatcher {
    shutdown_tx: broadcast::Sender<()>,
}

/// Builder for creating a RealWatcher with convenient configuration options.
pub struct RealWatcherBuilder {
    db: Arc<ArenaDatabase>,
    exclude: Vec<realize_types::Path>,
    initial_scan: bool,
    debounce: Duration,
    max_parallelism: usize,
}

impl RealWatcherBuilder {
    /// Create a new builder for watching the given root directory with the specified database.
    pub fn new(db: Arc<ArenaDatabase>) -> Self {
        Self {
            db,
            exclude: Vec::new(),
            initial_scan: false,
            debounce: Duration::ZERO,
            max_parallelism: 0,
        }
    }

    /// Look at existing files at startup, to catch up to any missed changes.
    pub fn with_initial_scan(mut self) -> Self {
        self.initial_scan = true;

        self
    }

    /// Set debounce delay for processing files. This allows some time
    /// for operations in progress to finish.
    pub fn debounce(mut self, duration: Duration) -> Self {
        self.debounce = duration;

        self
    }

    /// Maximum number of debouncers running in parallel.
    ///
    /// Hashing is CPU intensive, so hashing several large files in
    /// parallel can become a problem. It's a good idea to limit
    /// parallelism to a fraction of the available cores.
    ///
    /// Set it to 0 to not limit parallelism. This is the default.
    pub fn max_parallel_hashers(mut self, n: usize) -> Self {
        self.max_parallelism = n;

        self
    }

    /// Add multiple paths to exclude from watching.
    pub fn exclude_all<'a>(mut self, paths: impl Iterator<Item = &'a realize_types::Path>) -> Self {
        for path in paths {
            self.exclude.push(path.clone());
        }

        self
    }

    /// Spawn the watcher with the current configuration.
    ///
    /// To stop the background work cleanly, call [RealWatcher::shutdown].
    ///
    /// Background work is also stopped at some point after the instance is dropped.
    pub async fn spawn(self) -> anyhow::Result<RealWatcher> {
        RealWatcher::spawn(
            self.exclude,
            Arc::clone(&self.db),
            self.initial_scan,
            self.debounce,
            self.max_parallelism,
        )
        .await
    }
}

impl RealWatcher {
    /// Create a builder for configuring and spawning a RealWatcher.
    pub fn builder(db: Arc<ArenaDatabase>) -> RealWatcherBuilder {
        RealWatcherBuilder::new(db)
    }

    async fn spawn(
        exclude: Vec<realize_types::Path>,
        db: Arc<ArenaDatabase>,
        initial_scan: bool,
        debounce: Duration,
        max_parallelism: usize,
    ) -> anyhow::Result<Self> {
        let root = fs::canonicalize(db.cache().datadir()).await?;
        let tag = db.tag();

        let (watch_tx, watch_rx) = mpsc::channel(100);

        let watcher = {
            let root = root.clone();
            let watch_tx = watch_tx.clone();
            tokio::task::spawn_blocking(move || {
                let mut watcher = notify::recommended_watcher({
                    let root = root.clone();
                    move |ev: Result<Event, notify::Error>| {
                        if let Ok(ev) = ev {
                            log::trace!("[{tag}] Notify event: {ev:?}");
                            if ev.flag() == Some(notify::event::Flag::Rescan) {
                                let _ = watch_tx.blocking_send(FsEvent::Scan(Path::root()));
                            }
                            for ev in FsEvent::from_notify(&root, ev) {
                                let _ = watch_tx.blocking_send(ev);
                            }
                        }
                    }
                })?;
                watcher.configure(notify::Config::default().with_follow_symlinks(false))?;
                watcher.watch(&root, notify::RecursiveMode::Recursive)?;

                Ok::<RecommendedWatcher, notify::Error>(watcher)
            })
            .await??
        };

        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        let worker = Arc::new(RealWatcherWorker {
            db: db.clone(),
            exclude: Arc::new(exclude),
        });

        task::spawn({
            let watch_tx = watch_tx.clone();
            async move {
                let _watcher = watcher;

                worker
                    .event_loop(debounce, max_parallelism, watch_tx, watch_rx, shutdown_rx)
                    .await;
            }
        });

        if initial_scan {
            watch_tx.send(FsEvent::Scan(Path::root())).await?;
        }

        Ok(Self { shutdown_tx })
    }

    /// Shutdown background tasks and wait for them to be finished.
    #[allow(dead_code)]
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        let _ = self.shutdown_tx.send(());
        self.shutdown_tx.closed().await;

        Ok(())
    }
}

/// Filesystem event that the watcher finds relevant.
///
/// Events can be either translated from notification events or
/// generated when scanning.
#[derive(Debug)]
enum FsEvent {
    /// Notify reported that a scan may be needed
    Scan(Path),

    /// Notify or scanning reported some changes to the given file or
    /// dir.
    NeedsUpdate(Path),

    /// Request indexing of the given path.
    Index(Path, UnixTime, u64),
}

impl FsEvent {
    /// Create a [FsEvent] from a [notify::Event]
    fn from_notify(root: &std::path::Path, ev: Event) -> Vec<Self> {
        let needs_update = match ev.kind {
            EventKind::Modify(ModifyKind::Name(_))
            | EventKind::Remove(_)
            | EventKind::Create(CreateKind::Folder)
            | EventKind::Create(CreateKind::File)
            | EventKind::Modify(ModifyKind::Metadata(
                MetadataKind::Permissions | MetadataKind::Ownership | MetadataKind::Any,
            )) => true,

            #[cfg(target_os = "linux")]
            EventKind::Access(notify::event::AccessKind::Close(
                notify::event::AccessMode::Write,
            )) => true,
            #[cfg(target_os = "macos")]
            EventKind::Modify(ModifyKind::Data(_)) => true,
            _ => false,
        };
        if needs_update {
            ev.paths
                .into_iter()
                .flat_map(|p| Path::from_real_path_in(&p, root).ok())
                .map(|p| FsEvent::NeedsUpdate(p))
                .collect::<Vec<_>>()
        } else {
            vec![]
        }
    }
}

struct RealWatcherWorker {
    db: Arc<ArenaDatabase>,

    /// Paths that should be excluded from the index. These may be
    /// files or directories. For directories, the whole directory
    /// content is excluded.
    exclude: Arc<Vec<realize_types::Path>>,
}

impl RealWatcherWorker {
    /// Listen to notifications from the filesystem or when scanning files.
    async fn event_loop(
        &self,
        debounce: Duration,
        max_parallelism: usize,
        watch_tx: mpsc::Sender<FsEvent>,
        mut watch_rx: mpsc::Receiver<FsEvent>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) {
        let tag = self.db.tag();
        let mut barrier = self.db.cache().watcher_barrier();
        let mut debouncer = DebouncerMap::new(debounce, max_parallelism);
        loop {
            tokio::select!(
                _ = shutdown_rx.recv() => {
                    break;
                }
                ev = watch_rx.recv() => {
                    match ev {
                        None =>{ break; }
                        Some(ev) =>{
                            log::debug!("[{tag}] {ev:?}");
                            barrier.allow().await;
                            if let Err(err) = self.handle_event(&ev, &watch_tx, &mut debouncer).await {
                                log::warn!("[{tag}] Handling of {ev:?} failed: {err}");
                            }
                        }
                    }
                }
                Some((path, Err(err))) = debouncer.join_next() => {
                    log::debug!("[{tag}] Indexing failed for \"{path}\": {err:?}");
                }
            );
        }
    }

    async fn handle_event(
        &self,
        ev: &FsEvent,
        watch_tx: &mpsc::Sender<FsEvent>,
        debouncer: &mut DebouncerMap<Path, Result<(), StorageError>>,
    ) -> anyhow::Result<()> {
        match ev {
            FsEvent::Scan(path) => {
                self.process_path(watch_tx, path, true).await?;
            }
            FsEvent::NeedsUpdate(path) => {
                self.process_path(watch_tx, path, false).await?;
            }
            FsEvent::Index(path, mtime, size) => {
                if path.matches_any(&self.exclude) {
                    return Ok(());
                }
                debouncer.spawn_limited(path.clone(), *size > 0, {
                    let db = self.db.clone();
                    let path = path.clone();
                    let size = *size;
                    let mtime = *mtime;
                    async move {
                        let tag = db.tag();
                        let hash = if size == 0 {
                            hash::empty()
                        } else {
                            let realpath = path.within(db.cache().datadir());
                            hash::hash_file(File::open(realpath).await?).await?
                        };
                        log::info!("[{tag}] Hashed: \"{path}\" {mtime:?} {hash} size={size}");
                        if !index::add_file_if_matches_async(&db, &path, size, mtime, hash).await? {
                            log::debug!("[{tag}] Mismatch; Skipped adding \"{path}\" {mtime:?}",);
                        }
                        Ok(())
                    }
                });
            }
        }

        Ok(())
    }

    async fn process_path(
        &self,
        watch_tx: &mpsc::Sender<FsEvent>,
        path: &Path,
        scan: bool,
    ) -> Result<(), anyhow::Error> {
        let db = self.db.clone();
        let watch_tx = watch_tx.clone();
        let exclude = self.exclude.clone();
        let path = path.clone();
        tokio::task::spawn_blocking(move || {
            let mut needs_writing = false;
            {
                let txn = db.begin_read()?;
                let tree = txn.read_tree()?;
                let cache = txn.read_cache()?;
                if !process_path_read(db.tag(), &tree, &cache, &path, scan, &exclude, &watch_tx)? {
                    needs_writing = true;
                }
            }
            if needs_writing {
                let txn = db.begin_write()?;
                {
                    let mut tree = txn.write_tree()?;
                    let mut cache = txn.write_cache()?;
                    let mut blobs = txn.write_blobs()?;
                    let mut dirty = txn.write_dirty()?;
                    let mut history = txn.write_history()?;
                    process_path_write(
                        &mut tree,
                        &mut cache,
                        &mut blobs,
                        &mut history,
                        &mut dirty,
                        &db,
                        &path,
                        scan,
                        &exclude,
                        &watch_tx,
                    )?;
                }
                txn.commit()?;
            }
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(())
    }
}

/// Status of a filesystem node, to be compared with [CacheEntryStatus].
#[derive(Debug)]
enum FsNodeStatus {
    /// Node doesn't exist.
    Missing,

    /// Node exists and is a directory.
    Dir,

    /// Node exists and is a readable, regular file, with the given
    /// mtime and size.
    ReadableRegularFile(UnixTime, u64),

    /// Node exists and is neither a directory nor a readable regular file.
    OtherFile,
}

impl FsNodeStatus {
    /// Build a [FsNodeStatus] from the given metadata.
    ///
    /// This call may make blocking FS calls to check whether a
    /// regular file is readable.
    fn new_blocking(realpath: &std::path::Path, m: Option<&std::fs::Metadata>) -> FsNodeStatus {
        match m {
            None => FsNodeStatus::Missing,
            Some(m) => {
                if m.is_dir() {
                    return FsNodeStatus::Dir;
                }
                if m.is_file() && std::fs::File::open(realpath).is_ok() {
                    return FsNodeStatus::ReadableRegularFile(UnixTime::mtime(&m), m.len());
                }
                FsNodeStatus::OtherFile
            }
        }
    }
}

#[derive(Debug)]
enum WatcherAction {
    /// Remove file or dir, recursively, from the database
    Remove,
    /// Create dir in the database
    CreateDir,
    /// Compare the content of the directories in cache and on the fs and
    /// add or remove cache entries as necessary.
    ProcessDirContent,
    /// Trigger indexing of a local file, with the given mtime and size.
    Index(UnixTime, u64),
    /// Store previously indexed entry into the cache as local
    /// modification.
    Unindex,
    /// Store entry into the cache as local modification.
    Preindex,
}

/// Decide on a set of [WatcherAction] to execute for a path given a
/// filesystem and cache status.
fn choose_actions(
    path: &Path,
    fs_status: &FsNodeStatus,
    cache_status: &CacheEntryStatus,
    exclude: &Vec<Path>,
) -> Vec<WatcherAction> {
    match (fs_status, cache_status) {
        (FsNodeStatus::Dir, CacheEntryStatus::Dir { local: true, .. }) => {
            vec![WatcherAction::ProcessDirContent]
        }
        (FsNodeStatus::Dir, CacheEntryStatus::Dir { local: false, .. }) => {
            vec![WatcherAction::CreateDir, WatcherAction::ProcessDirContent]
        }
        (FsNodeStatus::Dir, CacheEntryStatus::Preindexed) => vec![
            WatcherAction::Remove,
            WatcherAction::CreateDir,
            WatcherAction::ProcessDirContent,
        ],
        (FsNodeStatus::Dir, CacheEntryStatus::Indexed { .. }) => vec![
            WatcherAction::Remove,
            WatcherAction::CreateDir,
            WatcherAction::ProcessDirContent,
        ],
        (FsNodeStatus::Dir, CacheEntryStatus::Missing) => {
            vec![WatcherAction::CreateDir, WatcherAction::ProcessDirContent]
        }
        (FsNodeStatus::Dir, CacheEntryStatus::Remote) => {
            vec![WatcherAction::CreateDir, WatcherAction::ProcessDirContent]
        }
        (FsNodeStatus::Missing, CacheEntryStatus::Missing) => vec![],
        (FsNodeStatus::Missing, CacheEntryStatus::Remote) => vec![],
        (FsNodeStatus::Missing, CacheEntryStatus::Dir { local: true, .. }) => {
            vec![WatcherAction::Remove]
        }
        (FsNodeStatus::Missing, CacheEntryStatus::Dir { local: false, .. }) => vec![],
        (FsNodeStatus::Missing, CacheEntryStatus::Preindexed) => vec![WatcherAction::Remove],
        (FsNodeStatus::Missing, CacheEntryStatus::Indexed { .. }) => vec![WatcherAction::Remove],
        (FsNodeStatus::OtherFile, CacheEntryStatus::Dir { .. }) => {
            vec![WatcherAction::Remove, WatcherAction::Preindex]
        }
        (FsNodeStatus::OtherFile, CacheEntryStatus::Indexed { .. }) => vec![WatcherAction::Unindex],
        (FsNodeStatus::OtherFile, CacheEntryStatus::Preindexed) => vec![],
        (FsNodeStatus::OtherFile, CacheEntryStatus::Missing) => vec![WatcherAction::Preindex],
        (FsNodeStatus::OtherFile, CacheEntryStatus::Remote) => vec![WatcherAction::Preindex],
        (
            FsNodeStatus::ReadableRegularFile(mtime_a, size_a),
            CacheEntryStatus::Indexed {
                mtime: mtime_b,
                size: size_b,
            },
        ) => {
            if path.matches_any(exclude) {
                vec![WatcherAction::Unindex]
            } else if mtime_a == mtime_b && size_a == size_b {
                vec![]
            } else {
                // reindexing is needed
                vec![WatcherAction::Index(*mtime_a, *size_a)]
            }
        }
        (FsNodeStatus::ReadableRegularFile(mtime, size), CacheEntryStatus::Preindexed) => {
            if path.matches_any(exclude) {
                vec![]
            } else {
                vec![WatcherAction::Index(*mtime, *size)]
            }
        }
        (
            FsNodeStatus::ReadableRegularFile(mtime, size),
            CacheEntryStatus::Missing | CacheEntryStatus::Remote,
        ) => vec![WatcherAction::Preindex, WatcherAction::Index(*mtime, *size)],
        (FsNodeStatus::ReadableRegularFile(mtime, size), CacheEntryStatus::Dir { .. }) => {
            let mut actions = vec![WatcherAction::Remove, WatcherAction::Preindex];
            if !path.matches_any(exclude) {
                actions.push(WatcherAction::Index(*mtime, *size));
            }

            actions
        }
    }
}

/// Process a path within a read transaction, if possible.
///
/// There's very little that can be done from within a read transaction; this call serves as
/// a filter as, in most case, there's nothing to do.
///
/// Return false if it turns out that a write transaction is
/// necessary.
fn process_path_read(
    tag: Tag,
    tree: &impl TreeReadOperations,
    cache: &impl CacheReadOperations,
    path: &Path,
    scan: bool,
    exclude: &Vec<Path>,
    tx: &mpsc::Sender<FsEvent>,
) -> anyhow::Result<bool> {
    let metadata = fs_utils::metadata_no_symlink_blocking(cache.datadir(), &path).ok();
    let realpath = path.within(cache.datadir());
    let fs_status = FsNodeStatus::new_blocking(&realpath, metadata.as_ref());
    let cache_status = cache.entry_status(tree, path)?;
    let actions = choose_actions(path, &fs_status, &cache_status, exclude);
    if !actions.is_empty() {
        log::debug!("[{tag}](r) {path} ({fs_status:?}, {cache_status:?}) -> actions {actions:?}");
    }

    for action in actions {
        match action {
            WatcherAction::ProcessDirContent => {
                process_dir_content(tag, tree, cache, path, scan, tx)?;
            }
            _ => {
                log::debug!("[{tag}](r) {path} switch to write txn for {action:?}");
                return Ok(false);
            }
        }
    }

    Ok(true)
}

fn process_path_write(
    tree: &mut WritableOpenTree,
    cache: &mut WritableOpenCache,
    blobs: &mut WritableOpenBlob,
    history: &mut WritableOpenHistory,
    dirty: &mut WritableOpenDirty,
    db: &Arc<ArenaDatabase>,
    path: &Path,
    scan: bool,
    exclude: &Vec<Path>,
    tx: &mpsc::Sender<FsEvent>,
) -> anyhow::Result<()> {
    let metadata = fs_utils::metadata_no_symlink_blocking(cache.datadir(), &path).ok();
    let realpath = path.within(cache.datadir());
    let fs_status = FsNodeStatus::new_blocking(&realpath, metadata.as_ref());
    let cache_status = cache.entry_status(tree, path)?;
    let actions = choose_actions(path, &fs_status, &cache_status, exclude);
    let tag = db.tag();
    if !actions.is_empty() {
        log::debug!("[{tag}](w) {path} ({fs_status:?}, {cache_status:?}) -> actions {actions:?}");
    }

    for action in actions {
        log::debug!("[{tag}](w) {path} execute {action:?}");
        match action {
            WatcherAction::ProcessDirContent => {
                process_dir_content(tag, tree, cache, path, scan, tx)?;
            }
            WatcherAction::Remove => {
                cache.remove_from_index_recursively(tree, blobs, history, dirty, path)?;
            }
            WatcherAction::CreateDir => {
                cache.mkdir(tree, path)?;
            }
            WatcherAction::Index(mtime, size) => {
                if size == 0 {
                    cache.index(tree, blobs, history, dirty, path, 0, mtime, hash::empty())?;
                } else {
                    let path = path.clone();
                    let tx = tx.clone();
                    let tag = db.tag();
                    tokio::spawn(async move {
                        if let Err(err) = tx.send(FsEvent::Index(path.clone(), mtime, size)).await {
                            log::debug!("[{tag}] failed to index {path} {mtime:?}: {err:?}")
                        }
                    });
                }
            }
            WatcherAction::Preindex => {
                cache.preindex(tree, blobs, dirty, path)?;
            }
            WatcherAction::Unindex => {
                cache.unindex(tree, blobs, history, dirty, path)?;
            }
        }
    }

    Ok(())
}

fn process_dir_content(
    tag: Tag,
    tree: &impl TreeReadOperations,
    cache: &impl CacheReadOperations,
    path: &Path,
    scan: bool,
    tx: &mpsc::Sender<FsEvent>,
) -> Result<(), anyhow::Error> {
    if scan {
        scan_dir_content(tag, tree, cache, path, tx)
    } else {
        process_dir_content_diff(tag, tree, cache, path, tx)
    }
}

fn process_dir_content_diff(
    tag: Tag,
    tree: &impl TreeReadOperations,
    cache: &impl CacheReadOperations,
    path: &Path,
    tx: &mpsc::Sender<FsEvent>,
) -> Result<(), anyhow::Error> {
    let cached = cached_dir_content(tree, cache, path)?;
    let real = fs_dir_content(&path.within(cache.datadir()))?;

    let diff = cached
        .symmetric_difference(&real)
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    if !diff.is_empty() {
        log::debug!("[{tag}] {path} processing dir diff {diff:?}",);
        let base = path.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            for name in diff {
                if let Ok(path) = base.join(&name) {
                    let _ = tx.send(FsEvent::Scan(path)).await;
                }
            }
        });
    }

    Ok(())
}

fn scan_dir_content(
    tag: Tag,
    tree: &impl TreeReadOperations,
    cache: &impl CacheReadOperations,
    path: &Path,
    tx: &mpsc::Sender<FsEvent>,
) -> Result<(), anyhow::Error> {
    let cached = cached_dir_content(tree, cache, path)?;
    let real = fs_dir_content(&path.within(cache.datadir()))?;
    let to_process = cached
        .into_iter()
        .chain(real.into_iter())
        .collect::<Vec<_>>();
    if !to_process.is_empty() {
        log::debug!("[{tag}](scan) {path} processing dir content {to_process:?}",);
        let base = path.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            for name in to_process {
                if let Ok(path) = base.join(&name) {
                    let _ = tx.send(FsEvent::Scan(path)).await;
                }
            }
        });
    }

    Ok(())
}

fn cached_dir_content(
    tree: &impl TreeReadOperations,
    cache: &impl CacheReadOperations,
    path: &Path,
) -> Result<HashSet<String>, StorageError> {
    let mut ret = HashSet::new();
    for res in tree.readdir(path) {
        let (name, pathid) = res?;
        if !matches!(
            cache.entry_status(tree, pathid)?,
            CacheEntryStatus::Missing | CacheEntryStatus::Dir { local: false, .. }
        ) {
            ret.insert(name);
        }
    }
    Ok(ret)
}

fn fs_dir_content(realpath: &std::path::Path) -> Result<HashSet<String>, StorageError> {
    let mut ret = HashSet::new();
    match std::fs::read_dir(realpath) {
        Err(err) => {
            if !matches!(
                err.kind(),
                std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::NotFound
            ) {
                return Err(err.into());
            }
            // treat dir as empty
        }
        Ok(readdir) => {
            for entry in readdir {
                match entry {
                    Ok(entry) => {
                        if let Ok(name) = entry.file_name().into_string() {
                            ret.insert(name);
                        }
                    }
                    Err(err) => {
                        if !matches!(
                            err.kind(),
                            std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::NotFound
                        ) {
                            return Err(err.into());
                        }
                    }
                }
            }
        }
    }

    Ok(ret)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arena::db::ArenaDatabase;
    use crate::arena::types::IndexedFile;
    use crate::realize_types::Arena;
    use crate::utils::hash;
    use assert_fs::TempDir;
    use assert_fs::fixture::ChildPath;
    use assert_fs::prelude::*;
    use std::os::unix::fs::PermissionsExt as _;
    use std::time::Duration;

    struct Fixture {
        db: Arc<ArenaDatabase>,
        root: ChildPath,
        tempdir: TempDir,
        exclude: Vec<realize_types::Path>,
    }

    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();
            let tempdir = TempDir::new()?;
            let root = tempdir.child("root");
            root.create_dir_all()?;

            let arena = Arena::from("test");
            let db = ArenaDatabase::for_testing_single_arena(
                arena,
                &std::path::Path::new("/dev/null"),
                root.path(),
            )?;
            Ok(Self {
                root,
                db,
                tempdir,
                exclude: vec![],
            })
        }

        /// Add to the exclusion list of any future watcher.
        fn exclude(&mut self, path: realize_types::Path) {
            self.exclude.push(path);
        }

        /// Catch up to any previous changes and watch for anything new.
        async fn scan_and_watch(&self) -> anyhow::Result<RealWatcher> {
            RealWatcher::builder(Arc::clone(&self.db))
                .with_initial_scan()
                .exclude_all(self.exclude.iter())
                .spawn()
                .await
        }

        /// Watch for changes; don't do initial scanning.
        ///
        /// Note that filesystem modifications made just before this
        /// is called might still get reported.
        async fn watch(&self) -> anyhow::Result<RealWatcher> {
            RealWatcher::builder(Arc::clone(&self.db))
                .exclude_all(self.exclude.iter())
                .spawn()
                .await
        }

        /// Wait for the given history entry to have been written.
        ///
        /// This is useful to wait for something to change in the index.
        async fn wait_for_history_event(&self, goal_index: u64) -> anyhow::Result<()> {
            tokio::time::timeout(
                Duration::from_secs(3),
                self.db
                    .history()
                    .watch()
                    .wait_for(|index| *index >= goal_index),
            )
            .await
            .map_err(|_| {
                anyhow::anyhow!("wait_for_history_event({goal_index}): deadline exceeded")
            })??;

            Ok(())
        }
    }

    #[tokio::test]
    async fn shutdown() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let watcher = fixture.scan_and_watch().await?;

        watcher.shutdown().await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let foobar = fixture.root.child("foobar");
        foobar.write_str("test")?;

        let mtime = UnixTime::mtime(&fs::metadata(foobar.path()).await?);

        let path = realize_types::Path::parse("foobar")?;
        fixture.wait_for_history_event(1).await?;
        assert_eq!(
            Some(IndexedFile {
                size: 4,
                mtime,
                hash: hash::digest(b"test"),
            }),
            index::indexed_file_async(&fixture.db, &path).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn create_empty_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let foobar = fixture.root.child("foobar");
        foobar.touch()?;

        let mtime = UnixTime::mtime(&fs::metadata(foobar.path()).await?);

        let path = realize_types::Path::parse("foobar")?;
        fixture.wait_for_history_event(1).await?;
        assert_eq!(
            Some(IndexedFile {
                size: 0,
                mtime,
                hash: hash::digest([]),
            }),
            index::indexed_file_async(&fixture.db, &path).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn modify_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let foobar = fixture.root.child("foobar");
        foobar.write_str("test")?;

        let path = realize_types::Path::parse("foobar")?;
        fixture.wait_for_history_event(1).await?;
        assert!(
            index::indexed_file_async(&fixture.db, &path)
                .await?
                .is_some()
        );

        foobar.write_str("boo")?;
        let mtime = UnixTime::mtime(&fs::metadata(foobar.path()).await?);
        fixture.wait_for_history_event(2).await?;
        assert!(
            index::indexed_file_async(&fixture.db, &path)
                .await?
                .unwrap()
                .matches(3, mtime)
        );

        Ok(())
    }

    #[tokio::test]
    async fn remove_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let foobar = fixture.root.child("foobar");

        foobar.write_str("test")?;
        fixture.wait_for_history_event(1).await?;
        let path = realize_types::Path::parse("foobar")?;
        assert!(
            index::indexed_file_async(&fixture.db, &path)
                .await?
                .is_some()
        );

        fs::remove_file(foobar.path()).await?;
        fixture.wait_for_history_event(2).await?;
        assert!(!index::has_local_file_async(&fixture.db, &path).await?);

        Ok(())
    }

    #[tokio::test]
    async fn create_dir_with_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let db = &fixture.db;
        let dir = fixture.root.child("a/b");
        dir.create_dir_all()?;

        fixture.root.child("a/b/foo").write_str("test")?;
        fixture.root.child("a/b/bar").write_str("test")?;

        fixture.wait_for_history_event(2).await?;
        assert!(
            index::indexed_file_async(db, &realize_types::Path::parse("a/b/foo")?)
                .await?
                .is_some()
        );
        assert!(
            index::indexed_file_async(db, &realize_types::Path::parse("a/b/bar")?)
                .await?
                .is_some()
        );

        Ok(())
    }

    #[tokio::test]
    async fn remove_dir_with_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let db = &fixture.db;
        let dir = fixture.root.child("a/b");
        dir.create_dir_all()?;

        fixture.root.child("a/b/foo").write_str("test")?;
        fixture.root.child("a/b/bar").write_str("test")?;

        let foo = realize_types::Path::parse("a/b/foo")?;
        let bar = realize_types::Path::parse("a/b/bar")?;

        fixture.wait_for_history_event(2).await?;
        assert!(index::indexed_file_async(db, &foo).await?.is_some());
        assert!(index::indexed_file_async(db, &bar).await?.is_some());

        fs::remove_dir_all(dir.path()).await?;

        fixture.wait_for_history_event(4).await?;
        assert!(!index::has_local_file_async(db, &foo).await?);
        assert!(!index::has_local_file_async(db, &bar).await?);

        Ok(())
    }

    #[tokio::test]
    async fn move_dir_with_files_into() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let db = &fixture.db;
        let dir = fixture.tempdir.child("newdir");
        dir.create_dir_all()?;

        dir.child("a/b/foo").write_str("test")?;
        dir.child("a/b/bar").write_str("test")?;

        let foo = realize_types::Path::parse("newdir/a/b/foo")?;
        let bar = realize_types::Path::parse("newdir/a/b/bar")?;

        fs::rename(dir, fixture.root.join("newdir")).await?;

        fixture.wait_for_history_event(2).await?;
        assert!(index::indexed_file_async(db, &foo).await?.is_some());
        assert!(index::indexed_file_async(db, &bar).await?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn move_dir_with_files_out() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let db = &fixture.db;
        let dir = fixture.root.child("a/b");
        dir.create_dir_all()?;

        fixture.root.child("a/b/foo").write_str("test")?;
        fixture.root.child("a/b/bar").write_str("test")?;

        let foo = realize_types::Path::parse("a/b/foo")?;
        let bar = realize_types::Path::parse("a/b/bar")?;

        fixture.wait_for_history_event(2).await?;
        assert!(index::indexed_file_async(db, &foo).await?.is_some());
        assert!(index::indexed_file_async(db, &bar).await?.is_some());

        fs::rename(dir.path(), fixture.tempdir.child("out").path()).await?;

        fixture.wait_for_history_event(4).await?;
        assert!(!index::has_local_file_async(db, &foo).await?);
        assert!(!index::has_local_file_async(db, &bar).await?);

        Ok(())
    }

    #[tokio::test]
    async fn rename_dir_with_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let db = &fixture.db;
        let dir = fixture.root.child("a/b");
        dir.create_dir_all()?;

        fixture.root.child("a/b/foo").write_str("test")?;
        fixture.root.child("a/b/bar").write_str("test")?;

        let foo = realize_types::Path::parse("a/b/foo")?;
        let bar = realize_types::Path::parse("a/b/bar")?;

        fixture.wait_for_history_event(2).await?;
        assert!(index::indexed_file_async(db, &foo).await?.is_some());
        assert!(index::indexed_file_async(db, &bar).await?.is_some());

        fs::rename(
            fixture.root.child("a").path(),
            fixture.root.child("newa").path(),
        )
        .await?;

        fixture.wait_for_history_event(6).await?;
        assert!(!index::has_local_file_async(db, &foo).await?);
        assert!(!index::has_local_file_async(db, &bar).await?);

        let newfoo = realize_types::Path::parse("newa/b/foo")?;
        let newbar = realize_types::Path::parse("newa/b/bar")?;
        assert!(index::indexed_file_async(db, &newfoo).await?.is_some());
        assert!(index::indexed_file_async(db, &newbar).await?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn move_file_into() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let db = &fixture.db;
        let newfile = fixture.tempdir.child("newfile");
        newfile.write_str("test")?;

        fs::rename(newfile.path(), fixture.root.join("newfile")).await?;

        fixture.wait_for_history_event(1).await?;
        assert!(
            index::indexed_file_async(db, &realize_types::Path::parse("newfile")?)
                .await?
                .is_some()
        );

        Ok(())
    }

    #[tokio::test]
    async fn move_file_out() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let foobar = fixture.root.child("foobar");

        foobar.write_str("test")?;
        fixture.wait_for_history_event(1).await?;
        let path = realize_types::Path::parse("foobar")?;
        assert!(
            index::indexed_file_async(&fixture.db, &path)
                .await?
                .is_some()
        );

        fs::rename(foobar.path(), fixture.tempdir.child("out").path()).await?;

        fixture.wait_for_history_event(2).await?;
        assert!(!index::has_local_file_async(&fixture.db, &path).await?);

        Ok(())
    }

    #[tokio::test]
    async fn rename_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let foo = fixture.root.child("foo");

        foo.write_str("test")?;
        fixture.wait_for_history_event(1).await?;
        let path = realize_types::Path::parse("foo")?;
        assert!(
            index::indexed_file_async(&fixture.db, &path)
                .await?
                .is_some()
        );

        fs::rename(foo.path(), fixture.root.child("bar")).await?;

        fixture.wait_for_history_event(3).await?;
        assert!(!index::has_local_file_async(&fixture.db, &path).await?);
        let path = realize_types::Path::parse("bar")?;
        assert!(
            index::indexed_file_async(&fixture.db, &path)
                .await?
                .is_some()
        );

        Ok(())
    }

    #[tokio::test]
    async fn change_file_accessibility() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let db = &fixture.db;
        let dir = fixture.root.child("a/b");
        dir.create_dir_all()?;

        fixture.root.child("a/b/foo").write_str("test")?;
        fixture.wait_for_history_event(1).await?;

        let foo = realize_types::Path::parse("a/b/foo")?;
        let foo_pathbuf = fixture.root.join("a/b/foo");
        make_inaccessible(&foo_pathbuf).await?;

        fixture.wait_for_history_event(2).await?;
        assert!(index::indexed_file_async(db, &foo).await?.is_none());
        assert!(index::has_local_file_async(db, &foo).await?);

        make_accessible(&foo_pathbuf).await?;
        fixture.wait_for_history_event(3).await?;
        assert!(index::indexed_file_async(db, &foo).await?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn change_dir_accessibility() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let db = &fixture.db;
        let dir = fixture.root.child("a/b");
        dir.create_dir_all()?;

        fixture.root.child("a/b/foo").write_str("test")?;
        fixture.root.child("a/b/bar").write_str("test")?;

        fixture.wait_for_history_event(2).await?;
        let foo = realize_types::Path::parse("a/b/foo")?;
        let bar = realize_types::Path::parse("a/b/foo")?;
        assert!(index::indexed_file_async(db, &foo).await?.is_some());
        assert!(index::indexed_file_async(db, &bar).await?.is_some());

        let dir = fixture.root.join("a");
        make_inaccessible(&dir).await?;

        fixture.wait_for_history_event(4).await?;

        // an inaccessible file is removed, not just unindexed.
        assert!(index::indexed_file_async(db, &foo).await?.is_none());
        assert!(index::indexed_file_async(db, &bar).await?.is_none());
        assert!(!index::has_local_file_async(db, &foo).await?);
        assert!(!index::has_local_file_async(db, &bar).await?);

        make_accessible(&dir).await?;

        fixture.wait_for_history_event(6).await?;
        assert!(index::indexed_file_async(db, &foo).await?.is_some());
        assert!(index::indexed_file_async(db, &bar).await?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn initial_scan_adds_existing_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let db = &fixture.db;

        fixture.root.child("foo").write_str("foo")?;
        fixture.root.child("a/b/c").create_dir_all()?;
        fixture.root.child("a/b/c/bar").write_str("bar")?;

        let _watcher = fixture.scan_and_watch().await?;

        fixture.wait_for_history_event(2).await?;
        let foo = realize_types::Path::parse("foo")?;
        let bar = realize_types::Path::parse("a/b/c/bar")?;
        assert!(index::indexed_file_async(db, &foo).await?.is_some());
        assert!(index::indexed_file_async(db, &bar).await?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn initial_scan_removes_old_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let db = &fixture.db;

        // Create actual files on disk and add them to index
        let foo = realize_types::Path::parse("foo")?;
        let bar = realize_types::Path::parse("a/b/c/bar")?;
        let foo_child = fixture.root.child("foo");
        foo_child.write_str("test")?;
        let bar_child = fixture.root.child("a/b/c/bar");
        bar_child.write_str("test")?;

        // Add files to index with their real metadata
        index::add_file_async(
            db,
            &foo,
            4,
            UnixTime::mtime(&fs::metadata(foo_child.path()).await?),
            hash::digest("test".as_bytes()),
        )
        .await?;
        index::add_file_async(
            db,
            &bar,
            4,
            UnixTime::mtime(&fs::metadata(bar_child.path()).await?),
            hash::digest("test".as_bytes()),
        )
        .await?;

        // Verify files are in index
        assert!(index::indexed_file_async(db, &foo).await?.is_some());
        assert!(index::indexed_file_async(db, &bar).await?.is_some());

        // Delete the files from disk (simulating external deletion)
        fs::remove_file(foo_child.path()).await?;
        fs::remove_dir_all(fixture.root.child("a").path()).await?;

        // Now run the initial scan - it should detect the files are gone and remove them
        let _watcher = fixture.scan_and_watch().await?;

        fixture.wait_for_history_event(4).await?;
        assert!(!index::has_local_file_async(db, &foo).await?);
        assert!(!index::has_local_file_async(db, &bar).await?);

        Ok(())
    }

    #[tokio::test]
    async fn initial_scan_updates_modified_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let db = &fixture.db;

        let foo = realize_types::Path::parse("foo")?;
        let bar = realize_types::Path::parse("a/b/c/bar")?;
        let foo_child = fixture.root.child("foo");
        foo_child.write_str("foo")?;
        let bar_child = fixture.root.child("a/b/c/bar");
        bar_child.write_str("bar")?;

        index::add_file_async(
            db,
            &foo,
            3,
            UnixTime::mtime(&fs::metadata(foo_child.path()).await?),
            hash::digest("foo".as_bytes()),
        )
        .await?;
        index::add_file_async(
            db,
            &bar,
            3,
            UnixTime::mtime(&fs::metadata(bar_child.path()).await?),
            hash::digest("bar".as_bytes()),
        )
        .await?;

        bar_child.write_str("barbar")?;

        let _watcher = fixture.scan_and_watch().await?;

        fixture.wait_for_history_event(3).await?;

        // Foo is as added initially
        assert_eq!(
            Some(IndexedFile {
                size: 3,
                mtime: UnixTime::mtime(&fs::metadata(foo_child.path()).await?),
                hash: hash::digest("foo".as_bytes()),
            }),
            index::indexed_file_async(&fixture.db, &foo).await?
        );

        // Bar was updated
        assert_eq!(
            Some(IndexedFile {
                size: 6,
                mtime: UnixTime::mtime(&fs::metadata(bar_child.path()).await?),
                hash: hash::digest(b"barbar"),
            }),
            index::indexed_file_async(&fixture.db, &bar).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn initial_scan_unindexes_inaccessible_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let db = &fixture.db;

        let foo = realize_types::Path::parse("foo")?;
        let bar = realize_types::Path::parse("a/b/c/bar")?;
        let foo_child = fixture.root.child("foo");
        foo_child.write_str("foo")?;
        let bar_child = fixture.root.child("a/b/c/bar");
        bar_child.write_str("bar")?;

        index::add_file_async(
            db,
            &foo,
            3,
            UnixTime::mtime(&fs::metadata(foo_child.path()).await?),
            hash::digest("foo".as_bytes()),
        )
        .await?;
        index::add_file_async(
            db,
            &bar,
            3,
            UnixTime::mtime(&fs::metadata(bar_child.path()).await?),
            hash::digest("bar".as_bytes()),
        )
        .await?;

        make_inaccessible(foo_child.path()).await?;
        make_inaccessible(bar_child.path()).await?;

        let _watcher = fixture.scan_and_watch().await?;

        fixture.wait_for_history_event(4).await?;

        assert!(index::indexed_file_async(db, &foo).await?.is_none());
        assert!(index::has_local_file_async(db, &foo).await?);

        assert!(index::indexed_file_async(db, &bar).await?.is_none());
        assert!(index::has_local_file_async(db, &bar).await?);

        Ok(())
    }

    #[tokio::test]
    async fn initial_scan_unindexes_files_in_inaccessible_dirs() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let db = &fixture.db;

        let foo = realize_types::Path::parse("a/b/c/foo")?;
        let bar = realize_types::Path::parse("a/b/d/bar")?;
        let foo_child = fixture.root.child("a/b/c/foo");
        foo_child.write_str("foo")?;
        let bar_child = fixture.root.child("a/b/d/bar");
        bar_child.write_str("bar")?;

        index::add_file_async(
            db,
            &foo,
            3,
            UnixTime::mtime(&fs::metadata(foo_child.path()).await?),
            hash::digest("foo".as_bytes()),
        )
        .await?;
        index::add_file_async(
            db,
            &bar,
            3,
            UnixTime::mtime(&fs::metadata(bar_child.path()).await?),
            hash::digest("bar".as_bytes()),
        )
        .await?;

        make_inaccessible(fixture.root.child("a").path()).await?;

        let _watcher = match fixture.scan_and_watch().await {
            Ok(w) => w,
            Err(err) => {
                // The inotify backend won't start if a subdirectory
                // is inacessible.
                // TODO: fix it
                log::warn!(
                    "[{tag}] FIXME: Watch with an inaccessible subdir failed: {err}",
                    tag = fixture.db.tag()
                );
                return Ok(());
            }
        };

        fixture.wait_for_history_event(4).await?;

        // foo and bar are removed, not just unindexed, as they're inaccessible.
        assert!(!index::has_local_file_async(db, &foo).await?);
        assert!(!index::has_local_file_async(db, &bar).await?);

        Ok(())
    }

    #[tokio::test]
    async fn ignore_new_symlinks() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let db = &fixture.db;

        let file_symlink = fixture.root.child("file_symlink");
        let dir_symlink = fixture.root.child("dir_symlink");
        let foo = fixture.root.child("foo");
        let bar = fixture.root.child("b/bar");
        foo.write_str("test")?;
        fs::symlink(foo.path(), file_symlink.path()).await?;
        fs::symlink(fixture.root.child("b").path(), dir_symlink.path()).await?;
        bar.write_str("test")?;

        fixture.wait_for_history_event(2).await?;

        // file_symlink is preindexed, but not indexed
        assert!(
            index::indexed_file_async(db, &realize_types::Path::parse("file_symlink")?)
                .await?
                .is_none()
        );
        assert!(
            index::has_local_file_async(db, &realize_types::Path::parse("file_symlink")?).await?
        );

        // dir_symlink is preindexed, dir_symlink/bar isn't because
        // that would require following the symlink.
        assert!(
            index::has_local_file_async(db, &realize_types::Path::parse("dir_symlink")?).await?
        );
        assert!(
            !index::has_local_file_async(db, &realize_types::Path::parse("dir_symlink/bar")?)
                .await?
        );

        // foo and bar are indexed
        assert!(
            index::indexed_file_async(db, &realize_types::Path::parse("foo")?)
                .await?
                .is_some()
        );
        assert!(
            index::indexed_file_async(db, &realize_types::Path::parse("b/bar")?)
                .await?
                .is_some()
        );

        Ok(())
    }

    #[tokio::test]
    async fn initial_scan_ignores_symlinks() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let db = &fixture.db;

        let foo = fixture.root.child("foo");
        foo.write_str("foo")?;
        fixture.root.child("a/b/c/bar").write_str("bar")?;
        fs::symlink(foo, fixture.root.child("file_symlink")).await?;
        fs::symlink(
            fixture.root.child("a").path(),
            fixture.root.child("dir_symlink").path(),
        )
        .await?;

        let _watcher = fixture.scan_and_watch().await?;

        fixture.wait_for_history_event(2).await?;
        let foo = realize_types::Path::parse("foo")?;
        let bar = realize_types::Path::parse("a/b/c/bar")?;
        let file_symlink = realize_types::Path::parse("file_symlink")?;
        let bar_through_symlink = realize_types::Path::parse("dir_symlink/b/c/bar")?;
        assert!(index::indexed_file_async(db, &foo).await?.is_some());
        assert!(index::indexed_file_async(db, &bar).await?.is_some());
        assert!(
            index::indexed_file_async(db, &file_symlink)
                .await?
                .is_none()
        );
        assert!(index::has_local_file_async(db, &file_symlink).await?);
        assert!(!index::has_local_file_async(db, &bar_through_symlink).await?);

        Ok(())
    }

    #[tokio::test]
    async fn turn_file_into_symlink() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let foo_child = fixture.root.child("foo");
        foo_child.write_str("foo")?;
        let bar_child = fixture.root.child("bar");
        bar_child.write_str("bar")?;

        let foo = realize_types::Path::parse("foo")?;
        let bar = realize_types::Path::parse("bar")?;
        fixture.wait_for_history_event(2).await?;
        assert!(
            index::indexed_file_async(&fixture.db, &foo)
                .await?
                .is_some()
        );
        assert!(
            index::indexed_file_async(&fixture.db, &bar)
                .await?
                .is_some()
        );

        fs::remove_file(bar_child.path()).await?;
        fs::symlink(foo_child.path(), bar_child.path()).await?;

        fixture.wait_for_history_event(3).await?;

        // File is unindexed, but not removed. It is reported as
        // removed to peers, though so there is a history event.
        assert!(
            index::indexed_file_async(&fixture.db, &bar)
                .await?
                .is_none()
        );
        assert!(index::has_local_file_async(&fixture.db, &bar).await?);

        Ok(())
    }

    #[tokio::test]
    async fn initial_scan_unindexes_files_turned_into_symlinks() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let db = &fixture.db;

        let foo_child = fixture.root.child("foo");
        let foo = realize_types::Path::parse("foo")?;
        let bar_child = fixture.root.child("bar");
        let bar = realize_types::Path::parse("bar")?;

        foo_child.write_str("foo")?;
        bar_child.write_str("bar")?;
        index::add_file_async(
            db,
            &foo,
            3,
            UnixTime::mtime(&fs::metadata(foo_child.path()).await?),
            hash::digest("foo".as_bytes()),
        )
        .await?;
        index::add_file_async(
            db,
            &bar,
            3,
            UnixTime::mtime(&fs::metadata(bar_child.path()).await?),
            hash::digest("bar".as_bytes()),
        )
        .await?;

        fs::remove_file(bar_child.path()).await?;
        fs::symlink(foo_child.path(), bar_child.path()).await?;

        let _watcher = fixture.scan_and_watch().await?;

        fixture.wait_for_history_event(3).await?;

        // The file is unindexed, not removed, though it is reported
        // as removed in the history.
        assert!(index::indexed_file_async(db, &foo).await?.is_some());
        assert!(index::indexed_file_async(db, &bar).await?.is_none());

        assert!(index::has_local_file_async(db, &foo).await?);
        assert!(index::has_local_file_async(db, &bar).await?);

        Ok(())
    }

    #[tokio::test]
    async fn initial_scan_removes_files_in_dirs_turned_into_symlinks() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let db = &fixture.db;

        let foo_child = fixture.root.child("a/foo");
        let foo = realize_types::Path::parse("a/foo")?;

        foo_child.write_str("foo")?;
        index::add_file_async(
            db,
            &foo,
            3,
            UnixTime::mtime(&fs::metadata(foo_child.path()).await?),
            hash::digest("foo".as_bytes()),
        )
        .await?;

        let dir = fixture.root.child("a");
        let newdir = fixture.root.child("b");
        fs::rename(dir.path(), newdir.path()).await?;
        fs::symlink(newdir.path(), dir.path()).await?;

        let _watcher = fixture.scan_and_watch().await?;

        fixture.wait_for_history_event(3).await?;
        let foo_in_b = realize_types::Path::parse("b/foo")?;
        assert_eq!(
            (true, false),
            (
                index::has_local_file_async(db, &foo_in_b).await?,
                index::has_local_file_async(db, &foo).await?,
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn create_hard_link() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let db = &fixture.db;
        let foo_child = fixture.root.child("foo");
        foo_child.write_str("test")?;
        let mtime = UnixTime::mtime(&fs::metadata(foo_child.path()).await?);

        fixture.wait_for_history_event(1).await?;

        let bar_child = fixture.root.child("bar");
        fs::hard_link(foo_child.path(), bar_child.path()).await?;
        fixture.wait_for_history_event(2).await?;

        assert_eq!(
            Some(IndexedFile {
                size: 4,
                mtime,
                hash: hash::digest("test".as_bytes()),
            }),
            index::indexed_file_async(db, &realize_types::Path::parse("bar")?).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn ignore_excluded() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture.exclude(realize_types::Path::parse("a/b")?);
        fixture.exclude(realize_types::Path::parse("excluded")?);

        let _watcher = fixture.watch().await?;

        fixture.root.child("excluded").write_str("test")?;
        fixture.root.child("a/b/also_excluded").write_str("test")?;
        fixture.root.child("a/not_excluded").write_str("test")?;

        fixture.wait_for_history_event(1).await?;

        let db = &fixture.db;
        let excluded = realize_types::Path::parse("excluded")?;
        let also_excluded = realize_types::Path::parse("a/b/also_excluded")?;
        let not_excluded = realize_types::Path::parse("a/not_excluded")?;

        // excluded are not indexed, but they are preindexed
        assert!(index::indexed_file_async(db, &excluded).await?.is_none());
        assert!(
            index::indexed_file_async(db, &also_excluded)
                .await?
                .is_none()
        );
        assert!(
            index::indexed_file_async(db, &not_excluded)
                .await?
                .is_some()
        );

        assert!(index::has_local_file_async(db, &excluded).await?);
        assert!(index::has_local_file_async(db, &also_excluded).await?);
        assert!(index::has_local_file_async(db, &not_excluded).await?);

        Ok(())
    }

    #[tokio::test]
    async fn initial_scan_ignores_excluded() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture.exclude(realize_types::Path::parse("a/b")?);
        fixture.exclude(realize_types::Path::parse("excluded")?);
        let db = &fixture.db;

        fixture.root.child("excluded").write_str("test")?;
        fixture.root.child("a/b/excluded_too").write_str("test")?;
        fixture.root.child("not_excluded").write_str("test")?;

        let _watcher = fixture.scan_and_watch().await?;

        fixture.wait_for_history_event(1).await?;
        let not_excluded = realize_types::Path::parse("not_excluded")?;
        assert!(
            index::indexed_file_async(db, &not_excluded)
                .await?
                .is_some()
        );

        Ok(())
    }

    #[tokio::test]
    async fn initial_scan_unindexes_excluded() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture.exclude(realize_types::Path::parse("a/b")?);
        fixture.exclude(realize_types::Path::parse("excluded")?);
        let db = &fixture.db;

        let excluded_child = fixture.root.child("excluded");
        excluded_child.write_str("test")?;
        let excluded_too_child = fixture.root.child("a/b/excluded_too");
        excluded_too_child.write_str("test")?;

        let excluded = realize_types::Path::parse("excluded")?;
        let excluded_too = realize_types::Path::parse("a/b/excluded_too")?;
        index::add_file_async(
            db,
            &excluded,
            4,
            UnixTime::mtime(&fs::metadata(excluded_child.path()).await?),
            hash::digest("test".as_bytes()),
        )
        .await?;
        index::add_file_async(
            db,
            &excluded_too,
            4,
            UnixTime::mtime(&fs::metadata(excluded_too_child.path()).await?),
            hash::digest("test".as_bytes()),
        )
        .await?;

        let _watcher = fixture.scan_and_watch().await?;

        fixture.wait_for_history_event(4).await?;
        assert!(index::indexed_file_async(db, &excluded).await?.is_none());
        assert!(
            index::indexed_file_async(db, &excluded_too)
                .await?
                .is_none()
        );
        assert!(index::has_local_file_async(db, &excluded).await?);
        assert!(index::has_local_file_async(db, &excluded_too).await?);

        Ok(())
    }

    #[tokio::test]
    async fn capture_ignore_and_removes_excluded() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture.exclude(realize_types::Path::parse("a/b")?);
        fixture.exclude(realize_types::Path::parse("excluded")?);

        let _watcher = fixture.scan_and_watch().await?;

        fixture.root.child("excluded").write_str("test")?;
        fixture.root.child("a/b/also_excluded").write_str("test")?;
        fixture.root.child("a/not_excluded").write_str("test")?;

        fixture.wait_for_history_event(1).await?;

        let db = &fixture.db;
        let excluded = realize_types::Path::parse("excluded")?;
        let also_excluded = realize_types::Path::parse("a/b/also_excluded")?;
        let not_excluded = realize_types::Path::parse("a/not_excluded")?;

        // excluded files are not indexed, but they are preindexed
        assert!(index::indexed_file_async(db, &excluded).await?.is_none());
        assert!(
            index::indexed_file_async(db, &also_excluded)
                .await?
                .is_none()
        );
        assert!(
            index::indexed_file_async(db, &not_excluded)
                .await?
                .is_some()
        );

        assert!(index::has_local_file_async(db, &excluded).await?);
        assert!(index::has_local_file_async(db, &also_excluded).await?);
        assert!(index::has_local_file_async(db, &not_excluded).await?);

        Ok(())
    }

    async fn make_inaccessible<P>(path: P) -> anyhow::Result<()>
    where
        P: AsRef<std::path::Path>,
    {
        let path = path.as_ref();
        let m = fs::metadata(path).await?;
        let mut permissions = m.permissions();
        permissions.set_mode(0);
        fs::set_permissions(path, permissions).await?;

        Ok(())
    }

    async fn make_accessible<P>(path: P) -> anyhow::Result<()>
    where
        P: AsRef<std::path::Path>,
    {
        let path = path.as_ref();
        let m = fs::metadata(path).await?;
        let mut permissions = m.permissions();
        permissions.set_mode(if m.is_dir() { 0o770 } else { 0o660 });
        fs::set_permissions(path, permissions).await?;

        Ok(())
    }
}
