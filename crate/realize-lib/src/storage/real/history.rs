use std::{
    collections::HashMap,
    fs::Metadata,
    io,
    path::{self, PathBuf},
    time::SystemTime,
};

use futures::StreamExt as _;
use inotify::{EventMask, Inotify, WatchDescriptor, WatchMask};
use tokio::{fs, sync::mpsc};
use walkdir::WalkDir;

use crate::{
    model::{Arena, Path},
    storage::real::PathType,
};

use super::PathResolver;

/// Report something happening in arenas of the local file system.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum HistoryNotification {
    Ready(Arena),
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

/// Pass a new receiver to the collector.
struct HistorySubscription {
    arena: Arena,
    path_resolver: PathResolver,
    tx: mpsc::Sender<HistoryNotification>,
}

#[derive(Clone)]
pub(crate) struct History {
    subscribe: mpsc::Sender<HistorySubscription>,
}

impl History {
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
    pub(crate) async fn subscribe(
        &self,
        arena: &Arena,
        path_resolver: PathResolver,
        tx: mpsc::Sender<HistoryNotification>,
    ) -> anyhow::Result<()> {
        self.subscribe
            .send(HistorySubscription {
                arena: arena.clone(),
                path_resolver,
                tx,
            })
            .await?;

        Ok(())
    }
}

struct ArenaWatch {
    path_resolver: PathResolver,
    enabled: bool,
    subscribers: Vec<mpsc::Sender<HistoryNotification>>,
}
struct PathWatch {
    arena: Arena,
    path: path::PathBuf,
}

async fn run_collector(
    inotify: Inotify,
    mut rx: mpsc::Receiver<HistorySubscription>,
) -> anyhow::Result<()> {
    let mut buffer = [0; 1024];
    let mut inotify = Box::pin(inotify.into_event_stream(&mut buffer)?);
    let mut watches = inotify.watches();
    let mut watched: HashMap<Arena, ArenaWatch> = HashMap::new();
    let mut wd: HashMap<WatchDescriptor, PathWatch> = HashMap::new();

    let (new_dirs_tx, mut new_dirs_rx) = mpsc::channel(10);

    log::debug!("collector loop starts");
    loop {
        tokio::select!(
            Some(HistorySubscription { arena, path_resolver, tx }) = rx.recv() => {
                match watched.get_mut(&arena) {
                    Some(existing) => {
                        log::debug!("new subscriber for {arena}");
                        existing.subscribers.push(tx);
                        if existing.enabled {
                            let notification = HistoryNotification::Ready(arena);
                            for sub in &existing.subscribers {
                                // TODO: remove dead subscribers, and then arenas without subscribers.
                                let _ = sub.send(notification.clone()).await;
                            }
                        }
                    }
                    None => {
                        log::debug!("watch: {arena}");
                        let root = path_resolver.root().to_path_buf();
                        watched.insert(arena.clone(), ArenaWatch { path_resolver, enabled:false, subscribers: vec![tx] });
                        spawn_walk_dir(arena, root, WalkDirMode::Initial, new_dirs_tx.clone());
                    }
                }
                continue;
            },
            Some(result) = new_dirs_rx.recv() => {
                match result {
                    WalkDirResult::Complete(arena) => {
                        log::debug!("walkdir arena complete {arena}");
                        if let Some(watch) = watched.get_mut(&arena) {
                            log::debug!("got {arena}");
                            if !watch.enabled  {
                                log::debug!("{arena} not enabled");
                                watch.enabled = true;

                                let notification = HistoryNotification::Ready(arena);
                                log::debug!("will send ready");
                                for sub in &watch.subscribers {
                                    log::debug!("send ready");
                                    // TODO: remove dead subscribers, and then arenas without subscribers.
                                    let _ = sub.send(notification.clone()).await;
                                }
                            }
                        }
                    },
                    WalkDirResult::AddDir(arena, path) => {
                        match watches.add(
                            &path,
                            WatchMask::CREATE |
                            WatchMask::CLOSE_WRITE |
                            WatchMask::DELETE |
                            WatchMask::MOVE |
                            WatchMask::DONT_FOLLOW |
                            WatchMask::EXCL_UNLINK
                        ) {
                            Ok(descr) => {
                                log::debug!("watch path: {path:?} for {arena}");
                                wd.insert(descr, PathWatch { arena, path });
                            },
                            Err(err)=> {
                                log::debug!("inotify failed to watch {path:?}: {err}");
                            }
                        }
                    }
                    WalkDirResult::AddFile(arena, path, metadata) =>{
                        if let Some(watch) = watched.get(&arena) {
                            if let Some((PathType::Final, resolved)) = watch.path_resolver.reverse(&path) {
                                let notification = HistoryNotification::Link {
                                    arena: arena.clone(),
                                    path: resolved,
                                    size: metadata.len(),
                                    mtime: metadata.modified()
                                    .expect("OS must support mtime"),
                                };
                                for sub in &watch.subscribers {
                                    // TODO: remove dead subscribers, and then arenas without subscribers.
                                    let _ = sub.send(notification.clone()).await;
                                }
                            }
                        }
                    }
                }
                continue;
            },
            ev = inotify.next() => {
                match ev {
                    None => {
                        log::error!("inotify sent back nothing");
                    }
                    Some(Err(err)) =>{
                        log::error!("inotify error:{err}")
                    }
                    Some(Ok(ev)) => {
                    log::debug!("inotify ev: {ev:?}");
                    if let Some(PathWatch { arena, path }) = wd.get(&ev.wd) {
                        if ev.mask.contains(EventMask::CREATE | EventMask::ISDIR) {
                            if let Some(filename) = ev.name {
                                spawn_walk_dir(arena.clone(), path.join(filename), WalkDirMode::NewDir, new_dirs_tx.clone());
                            }
                        } else if ev.mask.contains(EventMask::CLOSE_WRITE) {
                            if let Some(filename) = ev.name {
                                let path = path.join(filename);
                                if let Ok(metadata)  = fs::metadata(&path).await {
                                    if metadata.is_file() {
                                        if let Some(watch) = watched.get(&arena) {
                                            if let Some((PathType::Final, resolved)) = watch.path_resolver.reverse(&path) {
                                                let notification = HistoryNotification::Link {
                                                    arena: arena.clone(),
                                                path: resolved,
                                                    size: metadata.len(),
                                                    mtime: metadata.modified()
                                                    .expect("OS must support mtime"),
                                                };
                                                for sub in &watch.subscribers {
                                                    // TODO: remove dead subscribers, and then arenas without subscribers.
                                                    let _ = sub.send(notification.clone()).await;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    continue;
                    }
                }
            },
        );
        break;
    }
    log::debug!("collector loop ends");

    Ok(())
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum WalkDirMode {
    Initial,
    NewDir,
}

#[derive(Clone)]
enum WalkDirResult {
    AddDir(Arena, PathBuf),
    AddFile(Arena, PathBuf, Metadata),
    Complete(Arena),
}

fn spawn_walk_dir(arena: Arena, root: PathBuf, mode: WalkDirMode, tx: mpsc::Sender<WalkDirResult>) {
    tokio::task::spawn_blocking(move || {
        log::debug!("walkdir {arena} {root:?}");
        for entry in WalkDir::new(&root)
            .follow_links(false)
            .same_file_system(true)
            .into_iter()
            .flatten()
        {
            log::debug!("entry {:?}", entry.path());
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
        log::debug!("walkdir {arena} done");
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
        prelude::{FileWriteStr as _, PathChild as _, PathCreateDir as _},
        TempDir,
    };
    use std::time::Duration;
    use tokio::time::timeout;

    struct Fixture {
        tempdir: TempDir,
        arena: Arena,
        _storage: LocalStorage,
        notifications: mpsc::Receiver<HistoryNotification>,
    }

    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();

            let tempdir = TempDir::new()?;
            let arena = Arena::from("a");
            let storage = LocalStorage::single(&arena, tempdir.path());
            let (notifications_tx, notifications_rx) = mpsc::channel(10);
            storage.subscribe(&arena, notifications_tx).await?;

            Ok(Self {
                tempdir,
                arena,
                _storage: storage,
                notifications: notifications_rx,
            })
        }

        fn arena(&self) -> Arena {
            self.arena.clone()
        }

        async fn next(&mut self, msg: &'static str) -> anyhow::Result<HistoryNotification> {
            timeout(Duration::from_secs(1), self.notifications.recv())
                .await
                .context(msg)?
                .ok_or(anyhow::anyhow!("channel closed before {}", msg))
        }
    }

    #[tokio::test]
    async fn create_file() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        assert_eq!(
            HistoryNotification::Ready(fixture.arena()),
            fixture.next("ready").await?
        );

        let child = fixture.tempdir.child("child.txt");
        child.write_str("content")?;

        assert_eq!(
            HistoryNotification::Link {
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
        assert_eq!(
            HistoryNotification::Ready(fixture.arena()),
            fixture.next("ready").await?
        );

        let subdirs = fixture.tempdir.child("subdir1/subdir2");
        log::debug!("creating subdirs");
        subdirs.create_dir_all()?;
        log::debug!("creating subdirs... done");
        log::debug!("creating child");
        let child = subdirs.child("child.txt");
        child.write_str("content")?;
        log::debug!("creating child... done");

        assert_eq!(
            HistoryNotification::Link {
                arena: fixture.arena(),
                path: Path::parse("subdir1/subdir2/child.txt")?,
                size: 7,
                mtime: child.metadata()?.modified()?,
            },
            fixture.next("child.txt").await?,
        );
        Ok(())
    }
}
