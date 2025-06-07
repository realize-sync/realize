use std::{
    collections::HashMap,
    ffi::OsString,
    fs::Metadata,
    io,
    path::{self, PathBuf},
    time::SystemTime,
};

use futures::StreamExt as _;
use inotify::{Event, EventMask, Inotify, WatchDescriptor, WatchMask, Watches};
use tokio::{
    fs,
    sync::{mpsc, oneshot},
};
use walkdir::WalkDir;

use crate::{
    model::{Arena, Path},
    storage::real::PathType,
};

use super::PathResolver;

/// Report something happening in arenas of the local file system.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Notification {
    Link {
        arena: Arena,
        path: Path,
        size: u64,
        mtime: SystemTime,
    },

    Unlink {
        arena: Arena,
        path: Path,
    },
}

impl Notification {
    fn arena(&self) -> &Arena {
        use Notification::*;
        match self {
            Link { arena, .. } => arena,
            Unlink { arena, .. } => arena,
        }
    }
}

/// Pass a new receiver to the collector.
struct Subscription {
    arena: Arena,
    path_resolver: PathResolver,
    tx: mpsc::Sender<Notification>,

    // Channel to notify when all watches have been created.
    ready_tx: oneshot::Sender<()>,
}

/// A file watcher that can be used to get notifications.
///
/// The watcher can safely be cloned as necessary.
///
/// Usually created through [LocalStorage::subscribe].
#[derive(Clone)]
pub(crate) struct History {
    subscribe: mpsc::Sender<Subscription>,
}

impl History {
    /// Create a history instance.
    ///
    /// This sets up inotify and spawns a loop that listens to
    /// subscriptions and file notification. The loop gets killed when
    /// the last clone is dropped.
    pub(crate) fn new() -> Result<Self, io::Error> {
        let (tx, rx) = mpsc::channel(1);
        let inotify = Inotify::init()?;
        tokio::spawn(async move {
            if let Err(err) = run_collector(inotify, rx).await {
                log::warn!("history collector was shut down: {}", err)
            }
        });

        Ok(Self { subscribe: tx })
    }

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
        &self,
        arena: &Arena,
        path_resolver: PathResolver,
        tx: mpsc::Sender<Notification>,
    ) -> anyhow::Result<()> {
        let (ready_tx, ready_rx) = oneshot::channel();
        self.subscribe
            .send(Subscription {
                arena: arena.clone(),
                path_resolver,
                tx,
                ready_tx,
            })
            .await?;

        // Wait until all watches have been created
        ready_rx.await?;

        Ok(())
    }
}

/// Listen to subscription messages and inotify events.
async fn run_collector(
    inotify: Inotify,
    mut rx: mpsc::Receiver<Subscription>,
) -> anyhow::Result<()> {
    let (new_dirs_tx, mut new_dirs_rx) = mpsc::channel(10);

    let mut collector = Collector {
        watches: inotify.watches(),
        watched: HashMap::new(),
        wd: HashMap::new(),
        new_dirs_tx,
    };

    let mut buffer = [0; 1024];
    let mut inotify = Box::pin(inotify.into_event_stream(&mut buffer)?);

    loop {
        tokio::select!(
            res = rx.recv() => {
                match res {
                    None => { break; }
                    Some(Subscription { arena, path_resolver, tx, ready_tx }) => {
                        collector.subscribe(arena, path_resolver, tx, ready_tx).await?;
                    }
                }
            },
            Some(result) = new_dirs_rx.recv() => {
                match result {
                    WalkDirResult::Complete(arena) => {
                        collector.mark_ready(arena).await?;
                    },
                    WalkDirResult::AddDir(arena, path) => {
                        collector.watch_dir(arena, path).await?;
                    }
                    WalkDirResult::AddFile(arena, path, metadata) =>{
                        collector.send_link(arena, path, metadata).await;
                    }
                }
            },
            ev = inotify.next() => {
                match ev {
                    None => { break; }
                    Some(Err(err)) =>{
                        log::warn!("inotify error:{err}")
                    }
                    Some(Ok(ev)) => {
                        log::debug!("inotify ev: {ev:?}");
                        collector.handle_event(ev).await?;
                    }
                };
            }
        );
    }
    log::debug!("collector loop ends");

    Ok(())
}

struct Collector {
    /// Tool for adding watches to inotify.
    watches: Watches,

    /// Tracks subscribers for all watched arenas.
    watched: HashMap<Arena, ArenaWatch>,

    /// Tracks watched directories.
    wd: HashMap<WatchDescriptor, PathWatch>,

    /// Channel used to get back results from spawn_walk_dir.
    new_dirs_tx: mpsc::Sender<WalkDirResult>,
}

struct ArenaWatch {
    path_resolver: PathResolver,

    /// Oneshot channels to send a message to once the watches fro
    /// this arena have all been created.
    ///
    /// When this is empty, the channel is ready.
    ready: Vec<oneshot::Sender<()>>,

    /// Set of active subscribers.
    subscribers: Vec<Option<mpsc::Sender<Notification>>>,
}

struct PathWatch {
    /// The arena for which the watch was created.
    arena: Arena,

    /// The path that is watched within the arena. This might be the
    /// root of the arena or one of its subdirs.
    path: path::PathBuf,
}

impl Collector {
    async fn subscribe(
        &mut self,
        arena: Arena,
        path_resolver: PathResolver,
        tx: mpsc::Sender<Notification>,
        ready_tx: oneshot::Sender<()>,
    ) -> anyhow::Result<()> {
        match self.watched.get_mut(&arena) {
            Some(existing) => {
                log::debug!("new subscriber for {arena}");
                existing.subscribers.push(Some(tx));
                if existing.ready.is_empty() {
                    let _ = ready_tx.send(());
                } else {
                    existing.ready.push(ready_tx);
                }
            }
            None => {
                log::debug!("watch: {arena}");
                let root = path_resolver.root().to_path_buf();
                self.watched.insert(
                    arena.clone(),
                    ArenaWatch {
                        path_resolver,
                        ready: vec![ready_tx],
                        subscribers: vec![Some(tx)],
                    },
                );
                spawn_walk_dir(arena, root, WalkDirMode::Initial, self.new_dirs_tx.clone());
            }
        }

        Ok(())
    }

    /// Send a Ready notification for the given arena.
    async fn mark_ready(&mut self, arena: Arena) -> anyhow::Result<()> {
        if let Some(watch) = self.watched.get_mut(&arena) {
            if !watch.ready.is_empty() {
                for tx in std::mem::take(&mut watch.ready).into_iter() {
                    let _ = tx.send(());
                }
            }
            assert!(watch.ready.is_empty());
        }

        Ok(())
    }

    /// Tell inotify to watch the given path, which must be a directory.
    async fn watch_dir(&mut self, arena: Arena, path: PathBuf) -> anyhow::Result<()> {
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
                log::debug!("watch path: {path:?} for {arena}");
                self.wd.insert(descr, PathWatch { arena, path });
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
        if let Some(PathWatch { arena, path }) = self.wd.get(&ev.wd) {
            if ev.mask.contains(EventMask::CREATE | EventMask::ISDIR) {
                if let Some(filename) = ev.name {
                    spawn_walk_dir(
                        arena.clone(),
                        path.join(filename),
                        WalkDirMode::NewDir,
                        self.new_dirs_tx.clone(),
                    );
                }
            } else if ev.mask.contains(EventMask::CLOSE_WRITE) {
                if let Some(filename) = ev.name {
                    let path = path.join(filename);
                    if let Ok(metadata) = fs::metadata(&path).await {
                        self.send_link(arena.clone(), path, metadata).await;
                    }
                }
            }
        }

        Ok(())
    }

    /// Send Link notifications.
    async fn send_link(&mut self, arena: Arena, path: PathBuf, metadata: Metadata) {
        if !metadata.is_file() {
            return;
        }
        if let Some(watch) = self.watched.get(&arena) {
            if let Some((PathType::Final, resolved)) = watch.path_resolver.reverse(&path) {
                self.send_notification(Notification::Link {
                    arena: arena.clone(),
                    path: resolved,
                    size: metadata.len(),
                    mtime: metadata.modified().expect("OS must support mtime"),
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
        let arena = notification.arena();
        if let Some(watch) = self.watched.get_mut(arena) {
            for cell in &mut watch.subscribers {
                if let Some(sub) = cell {
                    if sub.send(notification.clone()).await.is_err() {
                        log::debug!("subscriber removed for {arena}");
                        cell.take();
                    }
                }
            }

            // Get rid of failed subscribers. After the last
            // subscriber is gone, remove the watch.
            watch.subscribers.retain(|cell| cell.is_some());
            if watch.subscribers.is_empty() {
                log::debug!("stop watching {arena}");
                self.watched.remove(arena);

                let to_remove: Vec<WatchDescriptor> = self
                    .wd
                    .iter()
                    .filter(|(_, watch)| watch.arena == *arena)
                    .map(|(d, _)| d.clone())
                    .collect();
                to_remove.into_iter().for_each(|wd| {
                    self.wd.remove(&wd);
                    let _ = self.watches.remove(wd);
                });
            }
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum WalkDirMode {
    /// Mode used for the very first walkdir in an arena.
    ///
    /// The first walkdir collects directories to ask inotify to watch.
    Initial,

    /// Modu used for new directories in an arena.
    ///
    /// A walkdir is necessary to catch existing files and
    /// subdirectories, including files or directories created between
    /// the time the directory was initially created and the time the
    /// inotify event was processed.
    NewDir,
}

#[derive(Clone)]
enum WalkDirResult {
    /// Report a directory.
    AddDir(Arena, PathBuf),

    /// Report a file and its metadata.
    AddFile(Arena, PathBuf, Metadata),

    /// Report that the initial walkdir is complete.
    Complete(Arena),
}

/// Recursively go through the given path and send result to a channel.
///
/// - in mode [WalkDirMode::Initial], report only directories and
///   finish with [WalkDirResult::Complete].
///
/// - in mode [WalkDirMode::NewDir], report directories and files.
///   Don't report the end.
fn spawn_walk_dir(arena: Arena, root: PathBuf, mode: WalkDirMode, tx: mpsc::Sender<WalkDirResult>) {
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
                tx.blocking_send(WalkDirResult::AddDir(
                    arena.clone(),
                    root.join(entry.path()),
                ))?;
            } else if mode == WalkDirMode::NewDir && entry.file_type().is_file() {
                if let Ok(metadata) = entry.metadata() {
                    tx.blocking_send(WalkDirResult::AddFile(
                        arena.clone(),
                        root.join(entry.path()),
                        metadata,
                    ))?;
                }
            }
        }
        if mode == WalkDirMode::Initial {
            tx.blocking_send(WalkDirResult::Complete(arena))?;
        }

        Ok::<(), anyhow::Error>(())
    });
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
        _storage: LocalStorage,
        notifications: mpsc::Receiver<Notification>,
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
            storage.subscribe(arena.clone(), notifications_tx).await?;

            Ok(Self {
                _tempdir: tempdir,
                arena,
                arena_dir,
                _storage: storage,
                notifications: notifications_rx,
            })
        }

        fn arena(&self) -> Arena {
            self.arena.clone()
        }

        async fn next(&mut self, msg: &'static str) -> anyhow::Result<Notification> {
            timeout(Duration::from_secs(1), self.notifications.recv())
                .await
                .context(msg)?
                .ok_or(anyhow::anyhow!("channel closed before {}", msg))
        }
    }

    #[tokio::test]
    async fn create_file() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
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

    // TODO: Add tests for the following cases:
    // - file that exists before subscription is deleted. Events:Unlink
    // - file that exists before subscription gets re-written. Events:Link (with new size and mtime)
    // - file that exists before subscription gets partially modified. Events:Link (with new size and mtime)
    // - dir (with subdirs and files) that exists before subscription is deleted recursively. Events:Unlink for all files in dir, recursively
    // - file that exists before subscription is moved out of the arena. Events:Unlink
    // - file that exists before subscription is moved into the arena. Events: Link
    // - file that exists before subscription is moved within the arena. Events: Unlink, Link
    // - file that exists before subscription is moved within the arena, from subdir1 to subdir2. Events: Unlink, Link
    // - file gets created after subscription, then deleted. Events:Link, then Unlink
    // - dir (with subdirs and files) that exists before subscription is moved out of the arena. Events:Unlink for all files in dir, recursively
    // - dir (with subdirs and files) that exists before subscription is moved into the arena. Events: Link for all files in dir, recursively
    // - dir (with subdirs and files) that exists before subscription is moved within the arena. Events: Unlink and Link for all files in dir, recursively
    //
    // Make sure to run the test with debug logging enabled. For example:
    //   RUST_LOG=debug cargo test -p realize-lib --lib real::history::tests
}
