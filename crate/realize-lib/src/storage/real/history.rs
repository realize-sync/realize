use std::{
    collections::{HashMap, VecDeque},
    ffi::OsString,
    fs::Metadata,
    path::PathBuf,
    time::SystemTimeError,
};

use futures::StreamExt as _;
use inotify::{Event, EventMask, Inotify, WatchDescriptor, WatchMask, Watches};
use tokio::fs;
use tokio::sync::mpsc;

use crate::{
    model::{Arena, Path, UnixTime},
    storage::real::PathType,
};

use super::{Notification, PathResolver};

/// Spawn a file watcher task that can be used to get notifications.
///
/// The watcher can safely be cloned as necessary.
///
/// Subscribe to notifications for the given arena and root path.
///
/// Dropping the receiver drops the subscription.
///
/// To wait for inotify watches to have been created, wait
/// for [Notification::Ready] before moving on.
pub(crate) async fn subscribe(
    arena: &Arena,
    path_resolver: PathResolver,
    tx: mpsc::Sender<Notification>,
    catchup: bool,
) -> anyhow::Result<()> {
    let inotify = Inotify::init()?;
    let arena = arena.clone();

    tokio::spawn(async move {
        if let Err(err) = run_loop(inotify, arena, path_resolver, tx, catchup).await {
            log::warn!("history loop was shut down: {}", err)
        }
    });

    Ok(())
}

/// Listen to subscription messages and inotify events.
async fn run_loop(
    inotify: Inotify,
    arena: Arena,
    path_resolver: PathResolver,
    tx: mpsc::Sender<Notification>,
    catchup: bool,
) -> anyhow::Result<()> {
    let mut collector = Collector {
        watches: inotify.watches(),
        path_resolver,
        tx,
        arena,
        wd: HashMap::new(),
    };

    let mut buffer = [0; 1024];
    let mut inotify = inotify.into_event_stream(&mut buffer)?;

    if catchup {
        collector.send_catching_up().await?;
    }
    collector
        .walk_dir(
            collector.path_resolver.root().to_path_buf(),
            if catchup {
                WalkDirFileMode::ReportAsCatchup
            } else {
                WalkDirFileMode::Ignore
            },
        )
        .await?;
    collector.send_ready().await?;

    while let Some(ev) = inotify.next().await {
        match ev {
            Err(err) => {
                log::warn!("inotify error: {err}")
            }
            Ok(ev) => {
                log::debug!("inotify ev: {ev:?}");
                if let Err(err) = collector.handle_event(ev).await {
                    log::warn!("failed to handle inotify event: {err}");
                }
            }
        }
    }
    log::debug!("collector loop ends");

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
}

impl Collector {
    /// Tell inotify to watch the given path, which must be a directory.
    async fn watch_dir(&mut self, path: &std::path::Path) -> anyhow::Result<()> {
        // TODO: handle the case where a directory that was not
        // accessible becomes accessible; the whole directory needs to
        // be processed.
        let descr = self.watches.add(
            &path,
            WatchMask::CREATE
                | WatchMask::CLOSE_WRITE
                | WatchMask::DELETE
                | WatchMask::MOVE
                | WatchMask::DONT_FOLLOW
                | WatchMask::EXCL_UNLINK,
        )?;
        log::debug!("watching {:?}", path);
        self.wd.insert(descr, path.to_path_buf());

        Ok(())
    }

    /// Go through the directory recursively, adding watches to directories.
    async fn walk_dir(&mut self, root: PathBuf, mode: WalkDirFileMode) -> anyhow::Result<()> {
        let mut queue = VecDeque::new();
        self.watch_dir(&root).await?;
        queue.push_back(root);

        while let Some(path) = queue.pop_front() {
            if let Ok(mut readdir) = fs::read_dir(path).await {
                loop {
                    match readdir.next_entry().await {
                        Err(_) => {} // skip
                        Ok(None) => {
                            break;
                        }
                        Ok(Some(entry)) => {
                            if let Ok(m) = entry.metadata().await {
                                if m.file_type().is_dir() {
                                    let dir = entry.path();
                                    match self.watch_dir(&dir).await {
                                        Ok(()) => {
                                            queue.push_back(dir);
                                        }
                                        Err(err) => {
                                            log::debug!("failed to add watch to {:?}: {}", dir, err)
                                        }
                                    }
                                } else if m.file_type().is_file() {
                                    match mode {
                                        WalkDirFileMode::Ignore => {}
                                        WalkDirFileMode::ReportAsCatchup => {
                                            self.send_link(entry.path(), m, true).await;
                                        }
                                        WalkDirFileMode::ReportAsLink => {
                                            self.send_link(entry.path(), m, false).await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
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
                        // Watch directory and report its content as new; note that the directory might
                        // not be readable, in which case it should be ignored.
                        let _ = self
                            .walk_dir(full_path, WalkDirFileMode::ReportAsLink)
                            .await;
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

    async fn send_catching_up(&mut self) -> anyhow::Result<()> {
        self.send_notification(Notification::CatchingUp {
            arena: self.arena.clone(),
        })
        .await;

        Ok(())
    }

    async fn send_ready(&mut self) -> anyhow::Result<()> {
        self.send_notification(Notification::Ready {
            arena: self.arena.clone(),
        })
        .await;

        Ok(())
    }

    /// Report a deleted file.
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

    /// Report a file that was just created or a catchup..
    async fn send_link(&mut self, path: PathBuf, metadata: Metadata, catchup: bool) {
        if !metadata.is_file() {
            return;
        }
        if let Some((PathType::Final, path)) = self.path_resolver.reverse(&path) {
            let arena = self.arena.clone();
            let size = metadata.len();
            if let Ok(mtime) = unix_mtime(&metadata) {
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
enum WalkDirFileMode {
    Ignore,
    ReportAsCatchup,
    ReportAsLink,
}

/// Find the modification time for a file that has been unlinked.
///
/// This is tricky because the file is gone and the containing
/// directory might be gone, too.
async fn find_mtime_for_unlink(
    resolver: &PathResolver,
    path: &Path,
) -> anyhow::Result<Option<UnixTime>> {
    let full_path = path.within(resolver.root());
    if fs::metadata(&full_path).await.is_ok() {
        // File was re-created, so we don't send an unlink notification.
        return Ok(None);
    }

    let mut current = path.parent();
    while let Some(parent_path) = current {
        let parent_full_path = parent_path.within(resolver.root());
        if let Ok(metadata) = fs::metadata(&parent_full_path).await {
            if let Ok(mtime) = unix_mtime(&metadata) {
                return Ok(Some(mtime));
            }
        }
        current = parent_path.parent();
    }

    // Fallback to arena root
    if let Ok(metadata) = fs::metadata(resolver.root()).await {
        if let Ok(mtime) = unix_mtime(&metadata) {
            return Ok(Some(mtime));
        }
    }

    Err(anyhow::anyhow!(
        "arena root {:?} is gone; cannot report unlink of {:?}",
        resolver.root(),
        path
    ))
}

/// Extract modification time as [UnixTime] from metadata.
fn unix_mtime(metadata: &std::fs::Metadata) -> anyhow::Result<UnixTime, SystemTimeError> {
    Ok(UnixTime::from_system_time(
        metadata.modified().expect("OS must support mtime"),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::real::RealStore;
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
        storage: RealStore,
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

            let storage = RealStore::single(&arena, arena_dir.path());
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
            timeout(Duration::from_secs(5), self.notifications_rx.recv())
                .await
                .context(msg)?
                .ok_or(anyhow::anyhow!("channel closed before {}", msg))
        }
    }

    #[tokio::test]
    async fn create_file() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture.subscribe(false).await?;
        assert_eq!(
            Notification::Ready {
                arena: fixture.arena(),
            },
            fixture.next("ready").await?,
        );

        let child = fixture.arena_dir.child("child.txt");
        child.write_str("content")?;

        assert_eq!(
            Notification::Link {
                arena: fixture.arena(),
                path: Path::parse("child.txt")?,
                size: 7,
                mtime: UnixTime::from_system_time(child.metadata()?.modified()?)?,
            },
            fixture.next("child.txt").await?,
        );
        Ok(())
    }

    #[tokio::test]
    async fn create_file_in_subdir() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture.subscribe(false).await?;
        assert_eq!(
            Notification::Ready {
                arena: fixture.arena(),
            },
            fixture.next("ready").await?,
        );

        let subdirs = fixture.arena_dir.child("subdir1/subdir2");
        subdirs.create_dir_all()?;
        let child = subdirs.child("child.txt");
        child.write_str("content")?;

        assert_eq!(
            Notification::Link {
                arena: fixture.arena(),
                path: Path::parse("subdir1/subdir2/child.txt")?,
                size: 7,
                mtime: UnixTime::from_system_time(child.metadata()?.modified()?)?,
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
        assert_eq!(
            Notification::Ready {
                arena: fixture.arena(),
            },
            fixture.next("ready").await?,
        );

        let start = UnixTime::from_system_time(fixture.arena_dir.metadata()?.modified()?)?;
        std::fs::remove_file(child.path())?;
        let notification = fixture.next("unlink").await?;
        match &notification {
            Notification::Unlink { arena, path, mtime } => {
                assert_eq!(fixture.arena(), *arena);
                assert_eq!("child.txt", path.as_str());
                assert!(*mtime >= start);
            }
            _ => {
                panic!("unexpected: {notification:?}");
            }
        }

        Ok(())
    }
    #[tokio::test]
    async fn rewrite_existing_file() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let child = fixture.arena_dir.child("child.txt");
        child.write_str("content")?;

        fixture.subscribe(false).await?;
        assert_eq!(
            Notification::Ready {
                arena: fixture.arena(),
            },
            fixture.next("ready").await?,
        );

        child.write_str("new content")?;
        assert_eq!(
            Notification::Link {
                arena: fixture.arena(),
                path: Path::parse("child.txt")?,
                size: 11,
                mtime: UnixTime::from_system_time(child.metadata()?.modified()?)?,
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
        assert_eq!(
            Notification::Ready {
                arena: fixture.arena(),
            },
            fixture.next("ready").await?,
        );

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
                Some(Path::parse("subdir/child1.txt")?),
                Some(Path::parse("subdir/child2.txt")?)
            ],
            all_n
                .iter()
                .map(|n| n.path().map(|p| p.clone()))
                .collect::<Vec<_>>(),
        );

        Ok(())
    }
    #[tokio::test]
    async fn move_file_out_of_arena() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let child = fixture.arena_dir.child("child.txt");
        child.write_str("content")?;

        fixture.subscribe(false).await?;
        assert_eq!(
            Notification::Ready {
                arena: fixture.arena(),
            },
            fixture.next("ready").await?,
        );

        let dest = fixture._tempdir.child("dest.txt");
        std::fs::rename(child.path(), dest.path())?;
        let mtime = UnixTime::from_system_time(fixture.arena_dir.metadata()?.modified()?)?;
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
        assert_eq!(
            Notification::Ready {
                arena: fixture.arena(),
            },
            fixture.next("ready").await?,
        );

        let dest = fixture.arena_dir.child("dest.txt");
        std::fs::rename(source.path(), dest.path())?;
        assert_eq!(
            Notification::Link {
                arena: fixture.arena(),
                path: Path::parse("dest.txt")?,
                size: 7,
                mtime: UnixTime::from_system_time(dest.metadata()?.modified()?)?,
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
        assert_eq!(
            Notification::Ready {
                arena: fixture.arena(),
            },
            fixture.next("ready").await?,
        );

        let dest = fixture.arena_dir.child("dest.txt");
        std::fs::rename(source.path(), dest.path())?;
        let n1 = fixture.next("move within 1").await?;
        let n2 = fixture.next("move within 2").await?;
        let mtime = UnixTime::from_system_time(fixture.arena_dir.metadata()?.modified()?)?;
        let unlink = Notification::Unlink {
            arena: fixture.arena(),
            path: Path::parse("source.txt")?,
            mtime,
        };
        let link = Notification::Link {
            arena: fixture.arena(),
            path: Path::parse("dest.txt")?,
            size: 7,
            mtime: UnixTime::from_system_time(dest.metadata()?.modified()?)?,
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
        assert_eq!(
            Notification::Ready {
                arena: fixture.arena(),
            },
            fixture.next("ready").await?,
        );

        let child = fixture.arena_dir.child("child.txt");
        child.write_str("content")?;
        assert_eq!(
            Notification::Link {
                arena: fixture.arena(),
                path: Path::parse("child.txt")?,
                size: 7,
                mtime: UnixTime::from_system_time(child.metadata()?.modified()?)?,
            },
            fixture.next("create").await?,
        );

        let mtime = UnixTime::from_system_time(fixture.arena_dir.metadata()?.modified()?)?;
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
    async fn catchup_reports_existing_file() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let child = fixture.arena_dir.child("child.txt");
        child.write_str("content")?;

        fixture.subscribe(true).await?;

        let mut notifications = vec![];
        let mut ready = false;
        while !ready {
            let n = fixture.next("notifications").await?;
            ready = matches!(n, Notification::Ready { .. });
            notifications.push(n);
        }

        assert_eq!(
            vec![
                Notification::CatchingUp {
                    arena: fixture.arena()
                },
                Notification::Catchup {
                    arena: fixture.arena(),
                    path: Path::parse("child.txt")?,
                    size: 7,
                    mtime: UnixTime::from_system_time(child.metadata()?.modified()?)?,
                },
                Notification::Ready {
                    arena: fixture.arena()
                },
            ],
            notifications,
        );

        Ok(())
    }

    #[tokio::test]
    async fn catchup_reports_existing_files_in_dirs() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        fixture.arena_dir.child("a/b").create_dir_all()?;
        let child1 = fixture.arena_dir.child("a/child1.txt");
        child1.write_str("1")?;
        let child2 = fixture.arena_dir.child("a/b/child2.txt");
        child2.write_str("2")?;

        fixture.subscribe(true).await?;

        let mut notifications = vec![];
        let mut ready = false;
        while !ready {
            let n = fixture.next("notifications").await?;
            ready = matches!(n, Notification::Ready { .. });
            if matches!(n, Notification::Catchup { .. }) {
                notifications.push(n);
            }
        }

        assert_unordered::assert_eq_unordered!(
            vec![
                Notification::Catchup {
                    arena: fixture.arena(),
                    path: Path::parse("a/child1.txt")?,
                    size: 1,
                    mtime: UnixTime::from_system_time(child1.metadata()?.modified()?)?,
                },
                Notification::Catchup {
                    arena: fixture.arena(),
                    path: Path::parse("a/b/child2.txt")?,
                    size: 1,
                    mtime: UnixTime::from_system_time(child2.metadata()?.modified()?)?,
                },
            ],
            notifications,
        );

        Ok(())
    }

    #[tokio::test]
    async fn hold_off_notifications_during_catchup() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;

        let root = fixture.arena_dir.to_path_buf();
        std::fs::write(root.join("one.txt"), "1")?;
        std::fs::write(root.join("three.txt"), "3")?;
        let (res1, res2) = tokio::join!(
            async move {
                fs::remove_file(root.join("one.txt")).await?;
                fs::write(root.join("two.txt"), "3").await?;
                fs::write(root.join("one.txt"), "111").await?;
                fs::remove_file(root.join("one.txt")).await?;
                fs::write(root.join("one.txt"), "1111").await?;
                fs::write(root.join("four.txt"), "4").await?;

                Ok::<(), anyhow::Error>(())
            },
            fixture.subscribe(true),
        );
        res1?;
        res2?;

        // Processing the notifications, we should end up with goal.
        let goal: Option<Vec<(String, u64)>> = Some(vec![
            ("four.txt".to_string(), 1),
            ("one.txt".to_string(), 4),
            ("three.txt".to_string(), 1),
            ("two.txt".to_string(), 1),
            // sorted
        ]);
        let mut files: HashMap<Path, (UnixTime, u64)> = HashMap::new();
        fn process(
            files: &mut HashMap<Path, (UnixTime, u64)>,
            n: Notification,
        ) -> Vec<(String, u64)> {
            match n {
                Notification::Catchup {
                    path, size, mtime, ..
                }
                | Notification::Link {
                    path, size, mtime, ..
                } => {
                    if files.get(&path).is_none() || files.get(&path).unwrap().0 <= mtime {
                        files.insert(path, (mtime, size));
                    }
                }
                Notification::Unlink { path, mtime, .. } => {
                    if files.get(&path).is_some() && files.get(&path).unwrap().0 <= mtime {
                        files.remove(&path);
                    }
                }
                Notification::CatchingUp { .. } | Notification::Ready { .. } => {
                    panic!("Unexpected: {n:?}");
                }
            }

            // Build a summary to compare to goal
            let mut got = files
                .iter()
                .map(|(p, (_, size))| (p.to_string(), *size))
                .collect::<Vec<_>>();
            got.sort();

            got
        }

        // We process notifications until the goal is reached
        // (goal==got) or reading notifications times out.

        // Processing also checks that notifications come in the right order.

        let mut matching = false;
        let mut got = None;
        assert_eq!(
            Notification::CatchingUp {
                arena: fixture.arena().clone()
            },
            fixture.next("catching_up").await?
        );
        loop {
            let n = fixture.next("catchup").await?;
            if matches!(n, Notification::Ready { .. }) {
                break;
            }
            assert!(matches!(n, Notification::Catchup { .. }), "{n:?}");

            got = Some(process(&mut files, n));
            matching = goal == got;
        }
        while !matching {
            if let Ok(n) = fixture.next("(un)link").await {
                assert!(
                    matches!(n, Notification::Link { .. } | Notification::Unlink { .. }),
                    "{n:?}"
                );
                got = Some(process(&mut files, n));
                matching = goal == got;
            } else {
                panic!("{goal:?} != {got:?}")
            }
        }

        Ok(())
    }
}
