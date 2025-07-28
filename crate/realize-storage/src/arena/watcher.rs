#![allow(dead_code)] // work in progress

use crate::utils::hash;

use super::hasher::{self, HashResult, Hasher};
use super::index::RealIndexAsync;
use futures::StreamExt as _;
use notify::event::{CreateKind, DataChange, MetadataKind, ModifyKind, RemoveKind};
use notify::{Event, EventKind, RecommendedWatcher, Watcher as _};
use realize_types::{self, UnixTime};
use std::fs::Metadata;
use std::os::unix::fs::MetadataExt as _;
use std::path::PathBuf;
use std::sync::Arc;
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
    root: std::path::PathBuf,
    index: RealIndexAsync,
    exclude: Vec<realize_types::Path>,
    catchup: bool,
}

impl RealWatcherBuilder {
    /// Create a new builder for watching the given root directory with the specified index.
    pub fn new(root: &std::path::Path, index: RealIndexAsync) -> Self {
        Self {
            root: root.to_path_buf(),
            index,
            exclude: Vec::new(),
            catchup: false,
        }
    }

    /// Look at existing files at startup, to catch up to any missed changes.
    pub fn with_catchup(mut self) -> Self {
        self.catchup = true;

        self
    }

    /// Add a single path to exclude from watching.
    pub fn exclude(mut self, path: &realize_types::Path) -> Self {
        self.exclude.push(path.clone());
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
        RealWatcher::spawn(&self.root, self.exclude, self.index, self.catchup).await
    }
}

impl RealWatcher {
    /// Create a builder for configuring and spawning a RealWatcher.
    pub fn builder(root: &std::path::Path, index: RealIndexAsync) -> RealWatcherBuilder {
        RealWatcherBuilder::new(root, index)
    }

    async fn spawn(
        root: &std::path::Path,
        exclude: Vec<realize_types::Path>,
        index: RealIndexAsync,
        catchup: bool,
    ) -> anyhow::Result<Self> {
        let root = fs::canonicalize(&root).await?;
        let arena = index.arena().clone();

        let (watch_tx, watch_rx) = mpsc::channel(100);

        let watcher = {
            let root = root.clone();
            let watch_tx = watch_tx.clone();
            tokio::task::spawn_blocking(move || {
                let mut watcher = notify::recommended_watcher(move |ev| {
                    let _ = watch_tx.blocking_send(ev);
                })?;
                watcher.configure(notify::Config::default().with_follow_symlinks(false))?;
                watcher.watch(&root, notify::RecursiveMode::Recursive)?;
                log::debug!("[{}] Watching {root:?} recursively.", arena);

                Ok::<RecommendedWatcher, notify::Error>(watcher)
            })
            .await??
        };

        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let (hashed_tx, hashed_rx) = mpsc::channel(128);

        // Transform excluded path::Path into realize_types::Path, when possible.
        let worker = Arc::new(RealWatcherWorker {
            root,
            index,
            hasher: hasher::Hasher::new(hashed_tx),
            exclude,
        });

        if catchup {
            task::spawn({
                let worker = Arc::clone(&worker);
                let watch_tx = watch_tx.clone();
                let shutdown_rx = shutdown_tx.subscribe();
                async move {
                    if let Err(err) = worker
                        .catchup_removed_or_modified(watch_tx, shutdown_rx)
                        .await
                    {
                        log::debug!(
                            "[{}] Catchup for modified or removed files failed: {err}",
                            arena
                        );
                    }
                }
            });
            task::spawn({
                let worker = Arc::clone(&worker);
                let watch_tx = watch_tx.clone();
                let shutdown_rx = shutdown_tx.subscribe();
                async move {
                    if let Err(err) = worker.catchup_added(watch_tx, shutdown_rx).await {
                        log::debug!("[{}] Catchup for added files failed: {err}", arena);
                    }
                }
            });
        }
        task::spawn({
            let worker = Arc::clone(&worker);
            let shutdown_rx = shutdown_tx.subscribe();
            async move { worker.hashed_loop(hashed_rx, shutdown_rx).await }
        });
        task::spawn(async move {
            let _watcher = watcher;
            worker.event_loop(watch_rx, shutdown_rx).await;
        });

        Ok(Self { shutdown_tx })
    }

    /// Shutdown background tasks and wait for them to be finished.
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        let _ = self.shutdown_tx.send(());
        self.shutdown_tx.closed().await;

        Ok(())
    }
}

struct RealWatcherWorker {
    root: PathBuf,
    index: RealIndexAsync,
    hasher: Hasher,

    /// Paths that should be excluded from the index. These may be
    /// files or directories. For directories, the whole directory
    /// content is excluded.
    exclude: Vec<realize_types::Path>,
}

impl RealWatcherWorker {
    /// Look for files in the index that have been deleted or modified
    /// and generate remove or modify events.
    async fn catchup_removed_or_modified(
        &self,
        watch_tx: mpsc::Sender<Result<Event, notify::Error>>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> anyhow::Result<()> {
        let mut files = std::pin::pin!(self.index.all_files());
        loop {
            tokio::select!(
            _ = shutdown_rx.recv() => {
                break;
            },
            next = files.next() => {
                let (path, entry) = match next {
                    None => {
                        break;
                    },
                    Some(e) => e,
                };

                let full_path = path.within(&self.root);

                let mut is_deleted = false;
                let mut is_modified = false;
                if self.is_excluded(&path) {
                    // If the file is now to be excluded, delete it
                    // from the index.
                    is_deleted = true;
                } else {
                    match fs::symlink_metadata(&full_path).await {
                        Err(_) => { is_deleted = true;
                        }
                        Ok(m) => {
                            if let Ok(canonical) = fs::canonicalize(&full_path).await && canonical != full_path {
                                is_deleted = true;
                            } else if !file_is_readable(&full_path).await {
                                is_deleted = true;
                            } else if m.len() != entry.size || UnixTime::mtime(&m) != entry.mtime {
                                is_modified = true;
                            }
                        }
                    }
                }

                if is_deleted {
                    let ev = Event::new(EventKind::Remove(RemoveKind::File))
                    .add_path(full_path)
                    .set_info("catchup");
                    watch_tx.send(Ok(ev)).await?;
                } else if is_modified {
                    let ev = Event::new(EventKind::Modify(ModifyKind::Data(DataChange::Content)))
                    .add_path(full_path)
                    .set_info("catchup");
                    watch_tx.send(Ok(ev)).await?;
                }
            });
        }
        Ok(())
    }

    /// Look for files not yet in the index yet and generate create events.
    async fn catchup_added(
        &self,
        watch_tx: mpsc::Sender<Result<Event, notify::Error>>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> anyhow::Result<()> {
        let mut direntries = async_walkdir::WalkDir::new(&self.root).filter(only_regular);

        loop {
            tokio::select!(
            _ = shutdown_rx.recv() => {
                break;
            }
            direntry = direntries.next() => {
                let direntry = match direntry {
                    None => {
                        break;
                    }
                    Some(Err(_)) => {
                        continue;
                    }
                    Some(Ok(e)) => e,
                };

                // Only take files into account.
                if !direntry.file_type().await.map(|t| t.is_file()).unwrap_or(false) {
                    continue;
                }

                let full_path = direntry.path();
                let path = match self.to_model_path(&full_path) {
                    Some(p) => p,
                    None => {
                        continue;
                    }
                };

                // Skip if the file is already in the index.
                if self.index.has_file(&path).await.unwrap_or(false) {
                    continue;
                }

                // Skip if the file is to be excluded
                if self.is_excluded(&path) {
                    continue;
                }

                // Send an event to add the file to the index. Note
                // that this is not a create event as we already have
                // file content.
                let ev = Event::new(EventKind::Modify(ModifyKind::Data(DataChange::Content)))
                    .add_path(full_path)
                    .set_info("catchup");
                watch_tx.send(Ok(ev)).await?;
            });
        }

        Ok(())
    }

    /// Listen to notifications from the filesystem or from catchup.
    async fn event_loop(
        &self,
        mut watch_rx: mpsc::Receiver<Result<Event, notify::Error>>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) {
        loop {
            tokio::select!(
                _ = shutdown_rx.recv() => {
                    break;
                }
                ev = watch_rx.recv() => {
                    let ev = match ev {
                        None => {
                            break;
                        }
                        Some(Err(err)) => {
                            log::debug!("[{}] Notify error: {err}", self.index.arena());
                            continue;
                        }
                        Some(Ok(ev)) => ev,
                    };

                    log::debug!("[{}] Notification: {ev:?}", self.index.arena());
                    if let Err(err) = self.handle_event(&ev).await {
                        log::warn!("[{}] Handling of {ev:?} failed: {err}", self.index.arena());
                    }
                }
            );
        }
    }

    async fn hashed_loop(
        &self,
        mut hashed_rx: mpsc::Receiver<(realize_types::Path, std::io::Result<HashResult>)>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) {
        loop {
            tokio::select!(
                _ = shutdown_rx.recv() => {
                    break;
                }
                res = hashed_rx.recv() => {
                    match res {
                        None => {
                            break;
                        }
                        Some((path, Ok(HashResult { size, mtime, hash }))) => {
                            let realpath = path.within(&self.root);
                            if let Ok(m) = fs::symlink_metadata(realpath).await && m.len() == size && UnixTime::mtime(&m) == mtime {
                                log::debug!("[{}] Add file {path} with hash {hash}", self.index.arena());
                                if let Err(err) = self.index.add_file(&path, size, &mtime, hash).await {
                                    log::debug!("[{}] Failed to add {path}: {err}", self.index.arena());
                                }
                            }
                        }
                        Some((path, Err(err))) => {
                            // TODO: should hash failures be retried?
                            // should some error types, such as access
                            // denied, cause removal?
                            log::debug!("[{}] Hashing failed for {path}: {err}", self.index.arena());
                        }
                    }

                }
            );
        }
    }

    async fn handle_event(&self, ev: &Event) -> anyhow::Result<()> {
        match ev.kind {
            EventKind::Remove(_) => {
                // Remove can't always tell whether a file or
                // directory was removed, so we don't bother checking.
                let realpath = ev.paths.last().ok_or(anyhow::anyhow!("No path in event"))?;
                self.file_or_dir_removed(realpath).await?;
            }

            EventKind::Create(CreateKind::Folder) => {
                let realpath = ev.paths.last().ok_or(anyhow::anyhow!("No path in event"))?;
                let m = fs::symlink_metadata(realpath).await?;
                // Checking is_dir() because the  folder might actually be a symlink.
                if m.is_dir() {
                    self.dir_created_or_modified(realpath).await?;
                }
            }

            EventKind::Create(CreateKind::File) => {
                let realpath = ev.paths.last().ok_or(anyhow::anyhow!("No path in event"))?;
                if let Ok(m) = fs::symlink_metadata(realpath).await
                    && m.is_file()
                {
                    // If not a hard link, not a rename and len > 0,
                    // this means that writing on the file has already
                    // started, so there's no point in creating an
                    // entry; we'll get a Modify event soon enough.
                    if m.nlink() > 1 || m.len() == 0 {
                        self.file_created_or_modified(realpath, &m).await?;
                    }
                }
            }

            EventKind::Modify(ModifyKind::Name(_)) => {
                // We can't trust that the notification tells us
                // whether a file or dir was moved to or from the
                // directory; check both.
                //
                // Anything outside the root directory is ignored when
                // converting to realize_types::Path, so we don't bother
                // checking here.

                for realpath in &ev.paths {
                    match fs::symlink_metadata(realpath).await {
                        Ok(m) => {
                            // Possibly moved to; add or update
                            if m.is_file() {
                                self.file_created_or_modified(realpath, &m).await?;
                            } else if m.is_dir() {
                                self.dir_created_or_modified(realpath).await?;
                            }
                        }
                        Err(_) => {
                            // Possibly moved from; remove
                            self.file_or_dir_removed(realpath).await?;
                        }
                    }
                }
            }
            EventKind::Modify(ModifyKind::Metadata(
                MetadataKind::Permissions | MetadataKind::Ownership | MetadataKind::Any,
            )) => {
                // Files that were accessible might have become
                // inacessible or the other way round. This is stored
                // as add/remove, with inaccessible files treated as
                // if they're gone.
                let realpath = ev.paths.last().ok_or(anyhow::anyhow!("No path in event"))?;
                match fs::symlink_metadata(realpath).await {
                    Err(_) => {
                        // Not accessible anymore.
                        self.file_or_dir_removed(realpath).await?;
                    }
                    Ok(m) => {
                        if m.is_dir() {
                            if fs::read_dir(realpath).await.is_ok() {
                                // Might have just become accessible.
                                self.dir_created_or_modified(realpath).await?;
                            } else {
                                // Might have just become inaccessible
                                self.file_or_dir_removed(realpath).await?;
                            }
                        } else {
                            if file_is_readable(realpath).await {
                                // Might have just become accessible.
                                self.file_created_or_modified(realpath, &m).await?;
                            } else {
                                // Might have just become inaccessible
                                self.file_or_dir_removed(realpath).await?;
                            }
                        }
                    }
                }
            }

            EventKind::Modify(ModifyKind::Data(_)) => {
                let realpath = ev.paths.last().ok_or(anyhow::anyhow!("No path in event"))?;
                let m = fs::symlink_metadata(realpath).await?;
                if m.is_file() {
                    // This event only matters if it's a file.
                    self.file_created_or_modified(realpath, &m).await?;
                }
            }
            _ => {}
        }

        Ok(())
    }

    async fn file_or_dir_removed(&self, realpath: &std::path::Path) -> anyhow::Result<()> {
        let path = match self.to_model_path(&realpath) {
            Some(p) => p,
            None => {
                return Ok(());
            }
        };

        self.index.remove_file_or_dir(&path).await?;

        Ok(())
    }

    async fn dir_created_or_modified(
        &self,
        dirpath: &std::path::Path,
    ) -> Result<(), anyhow::Error> {
        let mut direntries = async_walkdir::WalkDir::new(&dirpath).filter(only_regular);
        while let Some(direntry) = direntries.next().await {
            let direntry = match direntry {
                Err(_) => {
                    continue;
                }
                Ok(e) => e,
            };

            // Only take files into account.
            if !direntry
                .file_type()
                .await
                .map(|t| t.is_file())
                .unwrap_or(false)
            {
                continue;
            }

            let realpath = direntry.path();
            let path = match self.to_model_path(&realpath) {
                Some(p) => p,
                None => {
                    continue;
                }
            };

            let m = match direntry.metadata().await {
                Ok(m) => m,
                Err(_) => {
                    continue;
                }
            };

            if let Err(err) = self.file_created_or_modified(&realpath, &m).await {
                log::debug!("[{}] Failed to add {path}: {err}", self.index.arena());
            }
        }

        Ok(())
    }

    async fn file_created_or_modified(
        &self,
        realpath: &std::path::Path,
        m: &Metadata,
    ) -> Result<(), anyhow::Error> {
        if fs::canonicalize(&realpath).await? != *realpath {
            // Skip paths with symlinks in them.
            return Ok(());
        }
        let path = match self.to_model_path(&realpath) {
            Some(p) => p,
            None => {
                return Ok(());
            }
        };

        // Skip if the file is to be excluded from the index.
        if self.is_excluded(&path) {
            return Ok(());
        }

        let mtime = UnixTime::mtime(m);
        if self
            .index
            .has_matching_file(&path, m.len(), &mtime)
            .await
            .unwrap_or(false)
        {
            return Ok(());
        }
        if m.len() == 0 {
            log::debug!("[{}] Empty file at {path}", self.index.arena());
            self.index
                .add_file(&path, 0, &UnixTime::mtime(&m), hash::empty())
                .await?;
        } else {
            log::debug!("[{}] Requesting hash of {path}", self.index.arena());
            self.hasher.request_hash(realpath.to_path_buf(), path);
        }
        Ok(())
    }

    /// Convert a full path to a [realize_types::Path] within the arena, if possible.
    fn to_model_path(&self, path: &std::path::Path) -> Option<realize_types::Path> {
        // TODO: Should this use a PathResolver? We may or may not want
        // to care about partial/full files here.
        realize_types::Path::from_real_path_in(&path, &self.root).ok()
    }

    /// Check whether the given path should be excluded from the index.
    fn is_excluded(&self, path: &realize_types::Path) -> bool {
        self.exclude.iter().any(|e| path.starts_with(e))
    }
}

/// Check whether the given file can be read.
///
/// Instead of duplicating the access rules of the OS, which might not
/// be limited to the traditional unix rules, this function simply
/// tries to open the file for reading.
async fn file_is_readable(realpath: &std::path::Path) -> bool {
    File::open(realpath).await.is_ok()
}

/// Filter for [async_walkdir::WalkDir] to ignore everything except
/// regular files and directories.
///
/// This excludes symlinks. With this filter, WalkDir won't enter into
/// symlinks to directories.
async fn only_regular(e: async_walkdir::DirEntry) -> async_walkdir::Filtering {
    if e.file_type() // does not follow symlinks
        .await
        .map(|t| t.is_dir() || t.is_file())
        .unwrap_or(false)
    {
        async_walkdir::Filtering::Continue
    } else {
        async_walkdir::Filtering::IgnoreDir
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::arena::index::RealIndexBlocking;
    use crate::arena::types::IndexedFileTableEntry;
    use crate::realize_types::Arena;
    use crate::utils::{hash, redb_utils};
    use crate::{ArenaDatabase, DirtyPaths};
    use realize_types::Hash;

    use super::*;
    use assert_fs::TempDir;
    use assert_fs::fixture::ChildPath;
    use assert_fs::prelude::*;
    use std::os::unix::fs::PermissionsExt as _;

    struct Fixture {
        index: RealIndexAsync,
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
            let db = ArenaDatabase::new(redb_utils::in_memory()?)?;
            let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;
            let index = RealIndexBlocking::new(arena, db, dirty_paths)?.into_async();

            Ok(Self {
                root,
                index,
                tempdir,
                exclude: vec![],
            })
        }

        /// Add to the exclusion list of any future watcher.
        fn exclude(&mut self, path: realize_types::Path) {
            self.exclude.push(path);
        }

        /// Catchup to any previous changes and watch for anything new.
        async fn catchup_and_watch(&self) -> anyhow::Result<RealWatcher> {
            RealWatcher::builder(self.root.path(), self.index.clone())
                .with_catchup()
                .exclude_all(self.exclude.iter())
                .spawn()
                .await
        }

        /// Watch for changes; don't do any catchup.
        ///
        /// Note that filesystem modifications made just before this
        /// is called might still get reported.
        async fn watch(&self) -> anyhow::Result<RealWatcher> {
            RealWatcher::builder(self.root.path(), self.index.clone())
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
                self.index
                    .watch_history()
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
        let watcher = fixture.catchup_and_watch().await?;

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
            Some(IndexedFileTableEntry {
                size: 4,
                mtime,
                hash: hash::digest("test".as_bytes()),
                outdated_by: None,
            }),
            fixture.index.get_file(&path).await?
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
            Some(IndexedFileTableEntry {
                size: 0,
                mtime,
                hash: hash::digest([]),
                outdated_by: None,
            }),
            fixture.index.get_file(&path).await?
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
        assert!(fixture.index.has_file(&path).await?);

        foobar.write_str("boo")?;
        let mtime = UnixTime::mtime(&fs::metadata(foobar.path()).await?);
        fixture.wait_for_history_event(2).await?;
        assert!(fixture.index.has_matching_file(&path, 3, &mtime).await?);

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
        assert!(fixture.index.has_file(&path).await?);

        fs::remove_file(foobar.path()).await?;
        fixture.wait_for_history_event(2).await?;
        assert!(!fixture.index.has_file(&path).await?);

        Ok(())
    }

    #[tokio::test]
    async fn create_dir_with_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let index = &fixture.index;
        let dir = fixture.root.child("a/b");
        dir.create_dir_all()?;

        fixture.root.child("a/b/foo").write_str("test")?;
        fixture.root.child("a/b/bar").write_str("test")?;

        fixture.wait_for_history_event(2).await?;
        assert!(
            index
                .has_file(&realize_types::Path::parse("a/b/foo")?)
                .await?
        );
        assert!(
            index
                .has_file(&realize_types::Path::parse("a/b/bar")?)
                .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn remove_dir_with_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let index = &fixture.index;
        let dir = fixture.root.child("a/b");
        dir.create_dir_all()?;

        fixture.root.child("a/b/foo").write_str("test")?;
        fixture.root.child("a/b/bar").write_str("test")?;

        let foo = realize_types::Path::parse("a/b/foo")?;
        let bar = realize_types::Path::parse("a/b/bar")?;

        fixture.wait_for_history_event(2).await?;
        assert!(index.has_file(&foo).await?);
        assert!(index.has_file(&bar).await?);

        fs::remove_dir_all(dir.path()).await?;

        fixture.wait_for_history_event(4).await?;
        assert!(!index.has_file(&foo).await?);
        assert!(!index.has_file(&bar).await?);

        Ok(())
    }

    #[tokio::test]
    async fn move_dir_with_files_into() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let index = &fixture.index;
        let dir = fixture.tempdir.child("newdir");
        dir.create_dir_all()?;

        dir.child("a/b/foo").write_str("test")?;
        dir.child("a/b/bar").write_str("test")?;

        let foo = realize_types::Path::parse("newdir/a/b/foo")?;
        let bar = realize_types::Path::parse("newdir/a/b/bar")?;

        fs::rename(dir, fixture.root.join("newdir")).await?;

        fixture.wait_for_history_event(2).await?;
        assert!(index.has_file(&foo).await?);
        assert!(index.has_file(&bar).await?);

        Ok(())
    }

    #[tokio::test]
    async fn move_dir_with_files_out() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let index = &fixture.index;
        let dir = fixture.root.child("a/b");
        dir.create_dir_all()?;

        fixture.root.child("a/b/foo").write_str("test")?;
        fixture.root.child("a/b/bar").write_str("test")?;

        let foo = realize_types::Path::parse("a/b/foo")?;
        let bar = realize_types::Path::parse("a/b/bar")?;

        fixture.wait_for_history_event(2).await?;
        assert!(index.has_file(&foo).await?);
        assert!(index.has_file(&bar).await?);

        fs::rename(dir.path(), fixture.tempdir.child("out").path()).await?;

        fixture.wait_for_history_event(4).await?;
        assert!(!index.has_file(&foo).await?);
        assert!(!index.has_file(&bar).await?);

        Ok(())
    }

    #[tokio::test]
    async fn rename_dir_with_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let index = &fixture.index;
        let dir = fixture.root.child("a/b");
        dir.create_dir_all()?;

        fixture.root.child("a/b/foo").write_str("test")?;
        fixture.root.child("a/b/bar").write_str("test")?;

        let foo = realize_types::Path::parse("a/b/foo")?;
        let bar = realize_types::Path::parse("a/b/bar")?;

        fixture.wait_for_history_event(2).await?;
        assert!(index.has_file(&foo).await?);
        assert!(index.has_file(&bar).await?);

        fs::rename(
            fixture.root.child("a").path(),
            fixture.root.child("newa").path(),
        )
        .await?;

        fixture.wait_for_history_event(6).await?;
        assert!(!index.has_file(&foo).await?);
        assert!(!index.has_file(&bar).await?);

        let newfoo = realize_types::Path::parse("newa/b/foo")?;
        let newbar = realize_types::Path::parse("newa/b/bar")?;
        assert!(index.has_file(&newfoo).await?);
        assert!(index.has_file(&newbar).await?);

        Ok(())
    }

    #[tokio::test]
    async fn move_file_into() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let index = &fixture.index;
        let newfile = fixture.tempdir.child("newfile");
        newfile.write_str("test")?;

        fs::rename(newfile.path(), fixture.root.join("newfile")).await?;

        fixture.wait_for_history_event(1).await?;
        assert!(
            index
                .has_file(&realize_types::Path::parse("newfile")?)
                .await?
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
        assert!(fixture.index.has_file(&path).await?);

        fs::rename(foobar.path(), fixture.tempdir.child("out").path()).await?;

        fixture.wait_for_history_event(2).await?;
        assert!(!fixture.index.has_file(&path).await?);

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
        assert!(fixture.index.has_file(&path).await?);

        fs::rename(foo.path(), fixture.root.child("bar")).await?;

        fixture.wait_for_history_event(3).await?;
        assert!(!fixture.index.has_file(&path).await?);
        let path = realize_types::Path::parse("bar")?;
        assert!(fixture.index.has_file(&path).await?);

        Ok(())
    }

    #[tokio::test]
    async fn change_file_accessibility() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let index = &fixture.index;
        let dir = fixture.root.child("a/b");
        dir.create_dir_all()?;

        fixture.root.child("a/b/foo").write_str("test")?;
        fixture.wait_for_history_event(1).await?;

        let foo = realize_types::Path::parse("a/b/foo")?;
        let foo_pathbuf = fixture.root.join("a/b/foo");
        make_inaccessible(&foo_pathbuf).await?;

        fixture.wait_for_history_event(2).await?;
        assert!(!index.has_file(&foo).await?);

        make_accessible(&foo_pathbuf).await?;
        fixture.wait_for_history_event(3).await?;
        assert!(index.has_file(&foo).await?);

        Ok(())
    }

    #[tokio::test]
    async fn change_dir_accessibility() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let index = &fixture.index;
        let dir = fixture.root.child("a/b");
        dir.create_dir_all()?;

        fixture.root.child("a/b/foo").write_str("test")?;
        fixture.root.child("a/b/bar").write_str("test")?;

        fixture.wait_for_history_event(2).await?;
        let foo = realize_types::Path::parse("a/b/foo")?;
        let bar = realize_types::Path::parse("a/b/foo")?;
        assert!(index.has_file(&foo).await?);
        assert!(index.has_file(&bar).await?);

        let dir = fixture.root.join("a");
        make_inaccessible(&dir).await?;

        fixture.wait_for_history_event(4).await?;
        assert!(!index.has_file(&foo).await?);
        assert!(!index.has_file(&bar).await?);

        make_accessible(&dir).await?;

        fixture.wait_for_history_event(6).await?;
        assert!(index.has_file(&foo).await?);
        assert!(index.has_file(&bar).await?);

        Ok(())
    }

    #[tokio::test]
    async fn catchup_adds_existing_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;

        fixture.root.child("foo").write_str("foo")?;
        fixture.root.child("a/b/c").create_dir_all()?;
        fixture.root.child("a/b/c/bar").write_str("bar")?;

        let _watcher = fixture.catchup_and_watch().await?;

        fixture.wait_for_history_event(2).await?;
        let foo = realize_types::Path::parse("foo")?;
        let bar = realize_types::Path::parse("a/b/c/bar")?;
        assert!(index.has_file(&foo).await?);
        assert!(index.has_file(&bar).await?);

        Ok(())
    }

    #[tokio::test]
    async fn catchup_removes_old_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;

        let foo = realize_types::Path::parse("foo")?;
        let bar = realize_types::Path::parse("a/b/c/bar")?;
        let mtime = UnixTime::from_secs(1234567890);
        index.add_file(&foo, 4, &mtime, Hash([1; 32])).await?;
        index.add_file(&bar, 4, &mtime, Hash([2; 32])).await?;

        let _watcher = fixture.catchup_and_watch().await?;

        fixture.wait_for_history_event(4).await?;
        assert!(!index.has_file(&foo).await?);
        assert!(!index.has_file(&bar).await?);

        Ok(())
    }

    #[tokio::test]
    async fn catchup_updates_modified_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;

        let foo = realize_types::Path::parse("foo")?;
        let bar = realize_types::Path::parse("a/b/c/bar")?;
        let foo_child = fixture.root.child("foo");
        foo_child.write_str("foo")?;
        let bar_child = fixture.root.child("a/b/c/bar");
        bar_child.write_str("bar")?;

        index
            .add_file(
                &foo,
                3,
                &UnixTime::mtime(&fs::metadata(foo_child.path()).await?),
                hash::digest("foo".as_bytes()),
            )
            .await?;
        index
            .add_file(
                &bar,
                3,
                &UnixTime::mtime(&fs::metadata(bar_child.path()).await?),
                hash::digest("bar".as_bytes()),
            )
            .await?;

        bar_child.write_str("barbar")?;

        let _watcher = fixture.catchup_and_watch().await?;

        fixture.wait_for_history_event(3).await?;

        // Foo is as added initially
        assert_eq!(
            Some(IndexedFileTableEntry {
                size: 3,
                mtime: UnixTime::mtime(&fs::metadata(foo_child.path()).await?),
                hash: hash::digest("foo".as_bytes()),
                outdated_by: None,
            }),
            fixture.index.get_file(&foo).await?
        );

        // Bar was updated
        assert_eq!(
            Some(IndexedFileTableEntry {
                size: 6,
                mtime: UnixTime::mtime(&fs::metadata(bar_child.path()).await?),
                hash: hash::digest("barbar".as_bytes()),
                outdated_by: None,
            }),
            fixture.index.get_file(&bar).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn catchup_removes_inaccessible_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;

        let foo = realize_types::Path::parse("foo")?;
        let bar = realize_types::Path::parse("a/b/c/bar")?;
        let foo_child = fixture.root.child("foo");
        foo_child.write_str("foo")?;
        let bar_child = fixture.root.child("a/b/c/bar");
        bar_child.write_str("bar")?;

        index
            .add_file(
                &foo,
                3,
                &UnixTime::mtime(&fs::metadata(foo_child.path()).await?),
                hash::digest("foo".as_bytes()),
            )
            .await?;
        index
            .add_file(
                &bar,
                3,
                &UnixTime::mtime(&fs::metadata(bar_child.path()).await?),
                hash::digest("bar".as_bytes()),
            )
            .await?;

        make_inaccessible(foo_child.path()).await?;
        make_inaccessible(bar_child.path()).await?;

        let _watcher = fixture.catchup_and_watch().await?;

        fixture.wait_for_history_event(4).await?;

        assert!(!index.has_file(&foo).await?);
        assert!(!index.has_file(&bar).await?);

        Ok(())
    }

    #[tokio::test]
    async fn catchup_removes_files_in_inaccessible_dirs() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;

        let foo = realize_types::Path::parse("a/b/c/foo")?;
        let bar = realize_types::Path::parse("a/b/d/bar")?;
        let foo_child = fixture.root.child("a/b/c/foo");
        foo_child.write_str("foo")?;
        let bar_child = fixture.root.child("a/b/d/bar");
        bar_child.write_str("bar")?;

        index
            .add_file(
                &foo,
                3,
                &UnixTime::mtime(&fs::metadata(foo_child.path()).await?),
                hash::digest("foo".as_bytes()),
            )
            .await?;
        index
            .add_file(
                &bar,
                3,
                &UnixTime::mtime(&fs::metadata(bar_child.path()).await?),
                hash::digest("bar".as_bytes()),
            )
            .await?;

        make_inaccessible(fixture.root.child("a").path()).await?;

        let _watcher = match fixture.catchup_and_watch().await {
            Ok(w) => w,
            Err(err) => {
                // The inotify backend won't start if a subdirectory
                // is inacessible.
                // TODO: fix it
                log::warn!("FIXME: Watch with an inaccessible subdir failed: {err}");
                return Ok(());
            }
        };

        fixture.wait_for_history_event(4).await?;

        assert!(!index.has_file(&foo).await?);
        assert!(!index.has_file(&bar).await?);

        Ok(())
    }

    #[tokio::test]
    async fn ignore_new_symlinks() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let index = &fixture.index;

        let file_symlink = fixture.root.child("file_symlink");
        let dir_symlink = fixture.root.child("dir_symlink");
        let foo = fixture.root.child("foo");
        let bar = fixture.root.child("b/bar");
        foo.write_str("test")?;
        fs::symlink(foo.path(), file_symlink.path()).await?;
        fs::symlink(fixture.root.child("b").path(), dir_symlink.path()).await?;
        bar.write_str("test")?;

        fixture.wait_for_history_event(2).await?;
        assert!(
            !index
                .has_file(&realize_types::Path::parse("file_symlink")?)
                .await?
        );
        assert!(
            !index
                .has_file(&realize_types::Path::parse("dir_symlink/bar")?)
                .await?
        );
        assert!(index.has_file(&realize_types::Path::parse("foo")?).await?);
        assert!(
            index
                .has_file(&realize_types::Path::parse("b/bar")?)
                .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn catchup_ignores_symlinks() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;

        let foo = fixture.root.child("foo");
        foo.write_str("foo")?;
        fixture.root.child("a/b/c/bar").write_str("bar")?;
        fs::symlink(foo, fixture.root.child("file_symlink")).await?;
        fs::symlink(
            fixture.root.child("a").path(),
            fixture.root.child("dir_symlink").path(),
        )
        .await?;

        let _watcher = fixture.catchup_and_watch().await?;

        fixture.wait_for_history_event(2).await?;
        let foo = realize_types::Path::parse("foo")?;
        let bar = realize_types::Path::parse("a/b/c/bar")?;
        let file_symlink = realize_types::Path::parse("file_symlink")?;
        let bar_through_symlink = realize_types::Path::parse("dir_symlink/b/c/bar")?;
        assert!(index.has_file(&foo).await?);
        assert!(index.has_file(&bar).await?);
        assert!(!index.has_file(&file_symlink).await?);
        assert!(!index.has_file(&bar_through_symlink).await?);

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
        assert!(fixture.index.has_file(&foo).await?);
        assert!(fixture.index.has_file(&bar).await?);

        fs::remove_file(bar_child.path()).await?;
        fs::symlink(foo_child.path(), bar_child.path()).await?;

        fixture.wait_for_history_event(3).await?;
        assert!(!fixture.index.has_file(&bar).await?);

        Ok(())
    }

    #[tokio::test]
    async fn catchup_removes_files_turned_into_symlinks() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;

        let foo_child = fixture.root.child("foo");
        let foo = realize_types::Path::parse("foo")?;
        let bar_child = fixture.root.child("bar");
        let bar = realize_types::Path::parse("bar")?;

        foo_child.write_str("foo")?;
        bar_child.write_str("bar")?;
        index
            .add_file(
                &foo,
                3,
                &UnixTime::mtime(&fs::metadata(foo_child.path()).await?),
                hash::digest("foo".as_bytes()),
            )
            .await?;
        index
            .add_file(
                &bar,
                3,
                &UnixTime::mtime(&fs::metadata(bar_child.path()).await?),
                hash::digest("bar".as_bytes()),
            )
            .await?;

        fs::remove_file(bar_child.path()).await?;
        fs::symlink(foo_child.path(), bar_child.path()).await?;

        let _watcher = fixture.catchup_and_watch().await?;

        fixture.wait_for_history_event(3).await?;
        assert!(index.has_file(&foo).await?);
        assert!(!index.has_file(&bar).await?);

        Ok(())
    }

    #[tokio::test]
    async fn catchup_removes_files_in_dirs_turned_into_symlinks() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = &fixture.index;

        let foo_child = fixture.root.child("a/foo");
        let foo = realize_types::Path::parse("a/foo")?;

        foo_child.write_str("foo")?;
        index
            .add_file(
                &foo,
                3,
                &UnixTime::mtime(&fs::metadata(foo_child.path()).await?),
                hash::digest("foo".as_bytes()),
            )
            .await?;

        let dir = fixture.root.child("a");
        let newdir = fixture.root.child("b");
        fs::rename(dir.path(), newdir.path()).await?;
        fs::symlink(newdir.path(), dir.path()).await?;

        let _watcher = fixture.catchup_and_watch().await?;

        fixture.wait_for_history_event(3).await?;
        let foo_in_b = realize_types::Path::parse("b/foo")?;
        assert_eq!(
            (true, false),
            (
                index.has_file(&foo_in_b).await?,
                index.has_file(&foo).await?,
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn create_hard_link() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let _watcher = fixture.watch().await?;
        let index = &fixture.index;
        let foo_child = fixture.root.child("foo");
        foo_child.write_str("test")?;
        let mtime = UnixTime::mtime(&fs::metadata(foo_child.path()).await?);

        fixture.wait_for_history_event(1).await?;

        let bar_child = fixture.root.child("bar");
        fs::hard_link(foo_child.path(), bar_child.path()).await?;
        fixture.wait_for_history_event(2).await?;

        assert_eq!(
            Some(IndexedFileTableEntry {
                size: 4,
                mtime,
                hash: hash::digest("test".as_bytes()),
                outdated_by: None,
            }),
            index.get_file(&realize_types::Path::parse("bar")?).await?
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

        let index = &fixture.index;
        assert!(
            !index
                .has_file(&realize_types::Path::parse("excluded")?)
                .await?
        );
        assert!(
            !index
                .has_file(&realize_types::Path::parse("a/b/also_excluded")?)
                .await?
        );
        assert!(
            index
                .has_file(&realize_types::Path::parse("a/not_excluded")?)
                .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn catchup_ignores_excluded() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture.exclude(realize_types::Path::parse("a/b")?);
        fixture.exclude(realize_types::Path::parse("excluded")?);
        let index = &fixture.index;

        fixture.root.child("excluded").write_str("test")?;
        fixture.root.child("a/b/excluded_too").write_str("test")?;
        fixture.root.child("not_excluded").write_str("test")?;

        let _watcher = fixture.catchup_and_watch().await?;

        fixture.wait_for_history_event(1).await?;
        let not_excluded = realize_types::Path::parse("not_excluded")?;
        assert!(index.has_file(&not_excluded).await?);

        Ok(())
    }

    #[tokio::test]
    async fn catchup_removes_excluded() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture.exclude(realize_types::Path::parse("a/b")?);
        fixture.exclude(realize_types::Path::parse("excluded")?);
        let index = &fixture.index;

        let excluded_child = fixture.root.child("excluded");
        excluded_child.write_str("test")?;
        let excluded_too_child = fixture.root.child("a/b/excluded_too");
        excluded_too_child.write_str("test")?;

        let excluded = realize_types::Path::parse("excluded")?;
        let excluded_too = realize_types::Path::parse("a/b/excluded_too")?;
        index
            .add_file(
                &excluded,
                4,
                &UnixTime::mtime(&fs::metadata(excluded_child.path()).await?),
                hash::digest("test".as_bytes()),
            )
            .await?;
        index
            .add_file(
                &excluded_too,
                4,
                &UnixTime::mtime(&fs::metadata(excluded_too_child.path()).await?),
                hash::digest("test".as_bytes()),
            )
            .await?;

        let _watcher = fixture.catchup_and_watch().await?;

        fixture.wait_for_history_event(4).await?;
        assert!(!index.has_file(&excluded).await?);
        assert!(!index.has_file(&excluded_too).await?);

        Ok(())
    }

    #[tokio::test]
    async fn capture_ignore_and_removes_excluded() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture.exclude(realize_types::Path::parse("a/b")?);
        fixture.exclude(realize_types::Path::parse("excluded")?);

        let _watcher = fixture.catchup_and_watch().await?;

        fixture.root.child("excluded").write_str("test")?;
        fixture.root.child("a/b/also_excluded").write_str("test")?;
        fixture.root.child("a/not_excluded").write_str("test")?;

        fixture.wait_for_history_event(1).await?;

        let index = &fixture.index;
        assert!(
            !index
                .has_file(&realize_types::Path::parse("excluded")?)
                .await?
        );
        assert!(
            !index
                .has_file(&realize_types::Path::parse("a/b/also_excluded")?)
                .await?
        );
        assert!(
            index
                .has_file(&realize_types::Path::parse("a/not_excluded")?)
                .await?
        );

        Ok(())
    }

    async fn make_inaccessible(path: &std::path::Path) -> anyhow::Result<()> {
        let m = fs::metadata(path).await?;
        let mut permissions = m.permissions();
        permissions.set_mode(0);
        fs::set_permissions(path, permissions).await?;

        Ok(())
    }

    async fn make_accessible(path: &std::path::Path) -> anyhow::Result<()> {
        let m = fs::metadata(path).await?;
        let mut permissions = m.permissions();
        permissions.set_mode(if m.is_dir() { 0o770 } else { 0o660 });
        fs::set_permissions(path, permissions).await?;

        Ok(())
    }
}
