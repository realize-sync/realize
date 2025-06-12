use std::{collections::HashMap, ffi::OsString, fs::Metadata, path::PathBuf, time::SystemTime};

use futures::StreamExt as _;
use inotify::{Event, EventMask, Inotify, WatchDescriptor, WatchMask, Watches};
use tokio::fs;
use tokio::sync::{mpsc, oneshot};
use walkdir::WalkDir;

use crate::{
    model::{Arena, Path},
    storage::real::PathType,
};

use super::PathResolver;

/// Report something happening in arenas of the local file system.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Notification {
    /// Report that a file has been written to, added, moved in or
    /// modified.
    Link {
        arena: Arena,
        path: Path,
        size: u64,
        mtime: SystemTime,
    },

    /// Report that a file has been deleted or moved out of the given
    /// path.
    Unlink {
        arena: Arena,
        path: Path,
        mtime: SystemTime,
    },

    /// Report an already existing file at the time notifications were
    /// setup. Catchup notifications only ever sent at the beginning,
    /// before History reports itself ready.
    Catchup {
        arena: Arena,
        path: Path,
        size: u64,
        mtime: SystemTime,
    },
}

impl Notification {
    pub fn arena(&self) -> &Arena {
        use Notification::*;
        match self {
            Link { arena, .. } => &arena,
            Unlink { arena, .. } => &arena,
            Catchup { arena, .. } => &arena,
        }
    }
    pub fn path(&self) -> &Path {
        use Notification::*;
        match self {
            Link { path, .. } => &path,
            Unlink { path, .. } => &path,
            Catchup { path, .. } => &path,
        }
    }
}

/// A file watcher that can be used to get notifications.
///
/// The watcher can safely be cloned as necessary.
///
/// Subscribe to notifications for the given arena and root path.
///
/// Dropping the receiver drops the subscription.
///
/// All required inotify watches are guaranteed to have been
/// created when this command ends so any changes after that will
/// be caught. Note that this might take some time as creating
/// watches requires going through the whole arena, so use a join
/// if you need to subscribe to multiple arenas at the same time.
pub(crate) async fn subscribe(
    arena: &Arena,
    path_resolver: PathResolver,
    tx: mpsc::Sender<Notification>,
    catchup: bool,
) -> anyhow::Result<()> {
    let (ready_tx, ready_rx) = oneshot::channel();
    let inotify = Inotify::init()?;
    let arena = arena.clone();

    tokio::spawn(async move {
        if let Err(err) = run_loop(inotify, arena, path_resolver, tx, ready_tx, catchup).await {
            log::warn!("history loop was shut down: {}", err)
        }
    });

    // Wait until all watches have been created
    ready_rx.await?;

    Ok(())
}

/// Listen to subscription messages and inotify events.
async fn run_loop(
    inotify: Inotify,
    arena: Arena,
    path_resolver: PathResolver,
    tx: mpsc::Sender<Notification>,
    ready_tx: oneshot::Sender<()>,
    catchup: bool,
) -> anyhow::Result<()> {
    let (new_dirs_tx, mut new_dirs_rx) = mpsc::channel(10);
    let mut ready_tx = Some(ready_tx);

    let mut collector = Collector {
        watches: inotify.watches(),
        path_resolver,
        tx,
        arena,
        wd: HashMap::new(),
        new_dirs_tx,
    };

    let mut buffer = [0; 1024];
    let mut inotify = Box::pin(inotify.into_event_stream(&mut buffer)?);

    let mode = if catchup {
        WalkDirMode::InitialWatchDirsAndReportFiles
    } else {
        WalkDirMode::InitialWatchDirs
    };
    spawn_walk_dir(
        collector.path_resolver.root().to_path_buf(),
        mode,
        collector.new_dirs_tx.clone(),
    );

    loop {
        tokio::select! {
            Some(result) = new_dirs_rx.recv() => {
                handle_walk_dir_result(result, &mut collector, &mut ready_tx).await?;
            },
            ev = inotify.next() => {
                match ev {
                    None => { break; }
                    Some(Err(err)) =>{
                        log::warn!("inotify error:{err}")
                    }
                    Some(Ok(ev)) => {
                        log::debug!("inotify ev: {ev:?}");
                        if let Err(err) = collector.handle_event(ev).await {
                            log::warn!("failed to handle inotify event: {err}");
                        }
                    }
                };
            }
        };
    }
    log::debug!("collector loop ends");

    Ok(())
}

async fn handle_walk_dir_result(
    result: WalkDirResult,
    collector: &mut Collector,
    ready_tx: &mut Option<oneshot::Sender<()>>,
) -> anyhow::Result<()> {
    match result {
        WalkDirResult::Complete => {
            if let Some(tx) = ready_tx.take() {
                let _ = tx.send(());
            }
        }
        WalkDirResult::AddDir(path) => {
            collector.watch_dir(path).await?;
        }
        WalkDirResult::AddFile(path, metadata) => {
            collector.send_link(path, metadata, false).await;
        }
        WalkDirResult::AddCatchupFile(path, metadata) => {
            collector.send_link(path, metadata, true).await;
        }
    }
    Ok(())
}

struct Collector {
    /// Tool for adding watches to inotify.
    watches: Watches,

    path_resolver: PathResolver,
    tx: mpsc::Sender<Notification>,
    arena: Arena,

    /// Tracks watched directories.
    wd: HashMap<WatchDescriptor, PathBuf>,

    /// Channel used to get back results from spawn_walk_dir.
    new_dirs_tx: mpsc::Sender<WalkDirResult>,
}

impl Collector {
    /// Tell inotify to watch the given path, which must be a directory.
    async fn watch_dir(&mut self, path: PathBuf) -> anyhow::Result<()> {
        match self.watches.add(
            &path,
            WatchMask::CREATE
                | WatchMask::CLOSE_WRITE
                | WatchMask::DELETE
                | WatchMask::MOVE
                | WatchMask::DONT_FOLLOW
                | WatchMask::EXCL_UNLINK,
        ) {
            Ok(descr) => {
                log::debug!("watch path: {path:?} for {}", self.arena);
                self.wd.insert(descr, path);
            }
            Err(err) => {
                log::debug!("inotify failed to watch {path:?}: {err}");
            }
        }

        Ok(())
    }

    /// Handle inotify events.
    async fn handle_event(&mut self, ev: Event<OsString>) -> anyhow::Result<()> {
        log::debug!("inotify ev: {ev:?}");
        if let Some(path) = self.wd.get(&ev.wd) {
            if let Some(filename) = &ev.name {
                let full_path = path.join(filename);

                if ev.mask.contains(EventMask::ISDIR) {
                    if ev.mask.contains(EventMask::CREATE) || ev.mask.contains(EventMask::MOVED_TO)
                    {
                        // Watch directory and report its content as new.
                        spawn_walk_dir(
                            full_path,
                            WalkDirMode::WatchDirRecursivelyAndReportFiles,
                            self.new_dirs_tx.clone(),
                        );
                    }
                // Deletion of directory is implicitly handled by inotify removing the watch.
                // We should have received events for file deletions inside it before this.
                } else {
                    // It's a file event
                    if ev.mask.contains(EventMask::MOVED_TO)
                        || ev.mask.contains(EventMask::CLOSE_WRITE)
                    {
                        if let Ok(metadata) = fs::metadata(&full_path).await {
                            self.send_link(full_path, metadata, false).await;
                        }
                    } else if ev.mask.contains(EventMask::DELETE)
                        || ev.mask.contains(EventMask::MOVED_FROM)
                    {
                        self.send_unlink(full_path).await?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Send Unlink notification.
    async fn send_unlink(&mut self, full_path: PathBuf) -> anyhow::Result<()> {
        if let Some((PathType::Final, resolved)) = self.path_resolver.reverse(&full_path) {
            if let Some(mtime) = find_mtime_for_unlink(&self.path_resolver, &resolved).await? {
                self.send_notification(Notification::Unlink {
                    arena: self.arena.clone(),
                    path: resolved,
                    mtime,
                })
                .await;
            }
        }

        Ok(())
    }

    /// Send Link notification.
    async fn send_link(&mut self, path: PathBuf, metadata: Metadata, catchup: bool) {
        if !metadata.is_file() {
            return;
        }
        if let Some((PathType::Final, path)) = self.path_resolver.reverse(&path) {
            let arena = self.arena.clone();
            let size = metadata.len();
            let mtime = metadata.modified().expect("OS must support mtime");
            self.send_notification(if catchup {
                Notification::Catchup {
                    arena,
                    path,
                    size,
                    mtime,
                }
            } else {
                Notification::Link {
                    arena,
                    path,
                    size,
                    mtime,
                }
            })
            .await;
        }
    }

    /// Send the given notification.
    ///
    /// Also remove subscribers for which sending the notification
    /// fails, and eventually stop watching an arena, once all
    /// subscribers have failed.
    async fn send_notification(&mut self, notification: Notification) {
        if self.tx.send(notification.clone()).await.is_err() {
            log::debug!("subscriber removed for {}", self.arena);
            let to_remove: Vec<WatchDescriptor> = self.wd.keys().cloned().collect();
            for wd in to_remove {
                self.wd.remove(&wd);
                let _ = self.watches.remove(wd);
            }
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum WalkDirMode {
    /// Mode used for the very first walkdir in an arena.
    ///
    /// The first walkdir collects directories to ask inotify to watch.
    InitialWatchDirs,
    InitialWatchDirsAndReportFiles,

    /// Modu used for new directories in an arena.
    ///
    /// A walkdir is necessary to catch existing files and
    /// subdirectories, including files or directories created between
    /// the time the directory was initially created and the time the
    /// inotify event was processed.
    WatchDirRecursivelyAndReportFiles,
}

#[derive(Clone)]
enum WalkDirResult {
    /// Report a directory.
    AddDir(PathBuf),

    /// Report a file and its metadata.
    AddFile(PathBuf, Metadata),

    /// Report a file and its metadata during catchup.
    AddCatchupFile(PathBuf, Metadata),

    /// Report that the initial walkdir is complete.
    Complete,
}

/// Recursively go through the given path and send result to a channel.
///
/// - in mode [WalkDirMode::Initial], report only directories and
///   finish with [WalkDirResult::Complete].
///
/// - in mode [WalkDirMode::NewDir], report directories and files.
///   Don't report the end.
fn spawn_walk_dir(root: PathBuf, mode: WalkDirMode, tx: mpsc::Sender<WalkDirResult>) {
    tokio::task::spawn_blocking(move || {
        for entry in WalkDir::new(&root)
            .follow_links(false)
            .same_file_system(true)
            .into_iter()
            .flatten()
        {
            if entry.file_type().is_symlink() {
                continue; // ignore
            }
            if entry.file_type().is_dir() {
                tx.blocking_send(WalkDirResult::AddDir(entry.path().to_path_buf()))?;
            } else if entry.file_type().is_file() {
                if let Ok(metadata) = entry.metadata() {
                    match mode {
                        WalkDirMode::InitialWatchDirs => {}
                        WalkDirMode::InitialWatchDirsAndReportFiles => {
                            tx.blocking_send(WalkDirResult::AddCatchupFile(
                                entry.path().to_path_buf(),
                                metadata,
                            ))?;
                        }
                        WalkDirMode::WatchDirRecursivelyAndReportFiles => {
                            tx.blocking_send(WalkDirResult::AddFile(
                                entry.path().to_path_buf(),
                                metadata,
                            ))?;
                        }
                    }
                }
            }
        }
        if matches!(
            mode,
            WalkDirMode::InitialWatchDirs | WalkDirMode::InitialWatchDirsAndReportFiles
        ) {
            tx.blocking_send(WalkDirResult::Complete)?;
        }

        Ok::<(), anyhow::Error>(())
    });
}

/// Find the modification time for a file that has been unlinked.
///
/// This is tricky because the file is gone and the containing
/// directory might be gone, too.
async fn find_mtime_for_unlink(
    resolver: &PathResolver,
    path: &Path,
) -> anyhow::Result<Option<SystemTime>> {
    let full_path = path.within(resolver.root());
    if fs::metadata(&full_path).await.is_ok() {
        // File was re-created, so we don't send an unlink notification.
        return Ok(None);
    }

    let mut current = path.parent();
    while let Some(parent_path) = current {
        let parent_full_path = parent_path.within(resolver.root());
        if let Ok(metadata) = fs::metadata(&parent_full_path).await {
            return Ok(Some(metadata.modified().expect("OS must support mtime")));
        }
        current = parent_path.parent();
    }

    // Fallback to arena root
    if let Ok(metadata) = fs::metadata(resolver.root()).await {
        return Ok(Some(metadata.modified().expect("OS must support mtime")));
    }

    Err(anyhow::anyhow!(
        "arena root {:?} is gone; cannot report unlink of {:?}",
        resolver.root(),
        path
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::real::LocalStorage;
    use anyhow::Context as _;
    use assert_fs::{
        fixture::ChildPath,
        prelude::{FileWriteStr as _, PathChild as _, PathCreateDir as _},
        TempDir,
    };
    use std::time::Duration;
    use tokio::time::timeout;

    struct Fixture {
        _tempdir: TempDir,
        arena: Arena,
        arena_dir: ChildPath,
        storage: LocalStorage,
        notifications_tx: mpsc::Sender<Notification>,
        notifications_rx: mpsc::Receiver<Notification>,
    }

    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();

            let tempdir = TempDir::new()?;
            let arena = Arena::from("a");
            let arena_dir = tempdir.child("a");
            arena_dir.create_dir_all()?;

            let storage = LocalStorage::single(&arena, arena_dir.path());
            let (notifications_tx, notifications_rx) = mpsc::channel(10);

            Ok(Self {
                _tempdir: tempdir,
                arena,
                arena_dir,
                storage,
                notifications_tx,
                notifications_rx,
            })
        }

        async fn subscribe(&self, catchup: bool) -> anyhow::Result<()> {
            let subscribed = self
                .storage
                .subscribe(self.arena.clone(), self.notifications_tx.clone(), catchup)
                .await?;
            assert!(subscribed);

            Ok(())
        }

        fn arena(&self) -> Arena {
            self.arena.clone()
        }

        async fn next(&mut self, msg: &'static str) -> anyhow::Result<Notification> {
            timeout(Duration::from_secs(1), self.notifications_rx.recv())
                .await
                .context(msg)?
                .ok_or(anyhow::anyhow!("channel closed before {}", msg))
        }
    }

    #[tokio::test]
    async fn create_file() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture.subscribe(false).await?;

        let child = fixture.arena_dir.child("child.txt");
        child.write_str("content")?;

        assert_eq!(
            Notification::Link {
                arena: fixture.arena(),
                path: Path::parse("child.txt")?,
                size: 7,
                mtime: child.metadata()?.modified()?,
            },
            fixture.next("child.txt").await?,
        );
        Ok(())
    }

    #[tokio::test]
    async fn create_file_in_subdir() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture.subscribe(false).await?;

        let subdirs = fixture.arena_dir.child("subdir1/subdir2");
        subdirs.create_dir_all()?;
        let child = subdirs.child("child.txt");
        child.write_str("content")?;

        assert_eq!(
            Notification::Link {
                arena: fixture.arena(),
                path: Path::parse("subdir1/subdir2/child.txt")?,
                size: 7,
                mtime: child.metadata()?.modified()?,
            },
            fixture.next("child.txt").await?,
        );
        Ok(())
    }

    #[tokio::test]
    async fn delete_existing_file() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let child = fixture.arena_dir.child("child.txt");
        child.write_str("content")?;

        fixture.subscribe(false).await?;

        let mtime = fixture.arena_dir.metadata()?.modified()?;
        std::fs::remove_file(child.path())?;
        assert_eq!(
            Notification::Unlink {
                arena: fixture.arena(),
                path: Path::parse("child.txt")?,
                mtime,
            },
            fixture.next("unlink").await?,
        );
        Ok(())
    }
    #[tokio::test]
    async fn rewrite_existing_file() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let child = fixture.arena_dir.child("child.txt");
        child.write_str("content")?;

        fixture.subscribe(false).await?;

        child.write_str("new content")?;
        assert_eq!(
            Notification::Link {
                arena: fixture.arena(),
                path: Path::parse("child.txt")?,
                size: 11,
                mtime: child.metadata()?.modified()?,
            },
            fixture.next("rewrite").await?,
        );
        Ok(())
    }
    #[tokio::test]
    async fn delete_existing_dir_recursively() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let subdir = fixture.arena_dir.child("subdir");
        subdir.create_dir_all()?;
        let child1 = subdir.child("child1.txt");
        child1.write_str("c1")?;
        let child2 = subdir.child("child2.txt");
        child2.write_str("c2")?;

        fixture.subscribe(false).await?;

        std::fs::remove_dir_all(subdir.path())?;
        let all_n = vec![
            fixture.next("unlink 1").await?,
            fixture.next("unlink 2").await?,
        ];
        assert!(all_n
            .iter()
            .all(|n| matches!(n, Notification::Unlink { .. })));
        assert_unordered::assert_eq_unordered!(
            vec![
                Path::parse("subdir/child1.txt")?,
                Path::parse("subdir/child2.txt")?
            ],
            all_n.iter().map(|n| n.path().clone()).collect::<Vec<_>>(),
        );

        Ok(())
    }
    #[tokio::test]
    async fn move_file_out_of_arena() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let child = fixture.arena_dir.child("child.txt");
        child.write_str("content")?;

        fixture.subscribe(false).await?;

        let dest = fixture._tempdir.child("dest.txt");
        std::fs::rename(child.path(), dest.path())?;
        let mtime = fixture.arena_dir.metadata()?.modified()?;
        assert_eq!(
            Notification::Unlink {
                arena: fixture.arena(),
                path: Path::parse("child.txt")?,
                mtime,
            },
            fixture.next("move out").await?,
        );
        Ok(())
    }
    #[tokio::test]
    async fn move_file_into_arena() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let source = fixture._tempdir.child("source.txt");
        source.write_str("content")?;

        fixture.subscribe(false).await?;

        let dest = fixture.arena_dir.child("dest.txt");
        std::fs::rename(source.path(), dest.path())?;
        assert_eq!(
            Notification::Link {
                arena: fixture.arena(),
                path: Path::parse("dest.txt")?,
                size: 7,
                mtime: dest.metadata()?.modified()?,
            },
            fixture.next("move in").await?,
        );
        Ok(())
    }
    #[tokio::test]
    async fn move_file_within_arena() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let source = fixture.arena_dir.child("source.txt");
        source.write_str("content")?;

        fixture.subscribe(false).await?;

        let dest = fixture.arena_dir.child("dest.txt");
        std::fs::rename(source.path(), dest.path())?;
        let n1 = fixture.next("move within 1").await?;
        let n2 = fixture.next("move within 2").await?;
        let mtime = fixture.arena_dir.metadata()?.modified()?;
        let unlink = Notification::Unlink {
            arena: fixture.arena(),
            path: Path::parse("source.txt")?,
            mtime,
        };
        let link = Notification::Link {
            arena: fixture.arena(),
            path: Path::parse("dest.txt")?,
            size: 7,
            mtime: dest.metadata()?.modified()?,
        };
        if n1 == unlink {
            assert_eq!(n2, link);
        } else {
            assert_eq!(n1, link);
            assert_eq!(n2, unlink);
        }
        Ok(())
    }
    #[tokio::test]
    async fn create_then_delete_file() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture.subscribe(false).await?;

        let child = fixture.arena_dir.child("child.txt");
        child.write_str("content")?;
        assert_eq!(
            Notification::Link {
                arena: fixture.arena(),
                path: Path::parse("child.txt")?,
                size: 7,
                mtime: child.metadata()?.modified()?,
            },
            fixture.next("create").await?,
        );

        let mtime = fixture.arena_dir.metadata()?.modified()?;
        std::fs::remove_file(child.path())?;
        assert_eq!(
            Notification::Unlink {
                arena: fixture.arena(),
                path: Path::parse("child.txt")?,
                mtime,
            },
            fixture.next("delete").await?,
        );
        Ok(())
    }

    #[tokio::test]
    async fn catchup_reports_existing_files() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let child = fixture.arena_dir.child("child.txt");
        child.write_str("content")?;
        let child_in_subdir = fixture.arena_dir.child("subdir/child.txt");
        std::fs::create_dir_all(child_in_subdir.parent().unwrap())?;
        child_in_subdir.write_str("content2")?;

        fixture.subscribe(true).await?;

        let n1 = fixture.next("catchup 1").await?;
        let n2 = fixture.next("catchup 2").await?;

        assert_unordered::assert_eq_unordered!(
            vec![
                Notification::Catchup {
                    arena: fixture.arena(),
                    path: Path::parse("child.txt")?,
                    size: 7,
                    mtime: child.metadata()?.modified()?,
                },
                Notification::Catchup {
                    arena: fixture.arena(),
                    path: Path::parse("subdir/child.txt")?,
                    size: 8,
                    mtime: child_in_subdir.metadata()?.modified()?,
                }
            ],
            vec![n1, n2]
        );

        Ok(())
    }
}
