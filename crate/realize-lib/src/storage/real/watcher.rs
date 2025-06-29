#![allow(dead_code)] // work in progress

use super::index::RealIndexAsync;
use crate::model::{self, UnixTime};
use crate::utils::hash;
use futures::TryStreamExt as _;
use notify::event::{DataChange, ModifyKind};
use notify::{Event, EventKind, RecommendedWatcher, Watcher as _};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{self, File};
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::task;
use tokio_util::io::ReaderStream;

/// Watch an arena directory and update its index.
pub struct RealWatcher {
    shutdown_tx: broadcast::Sender<()>,
}

impl RealWatcher {
    /// Spawn the worker.
    ///
    /// To stop the background work cleanly, call [RealWatcher::shutdown].
    ///
    /// Background work is also stopped at some point after the instance is dropped.
    pub async fn spawn(root: &std::path::Path, index: RealIndexAsync) -> anyhow::Result<Self> {
        let root = fs::canonicalize(&root).await?;

        let (watch_tx, watch_rx) = mpsc::channel(100);

        let watcher = {
            let arena = index.arena().clone();
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

        let worker = Arc::new(RealWatcherWorker::new(root, index));

        // TODO: add catchup
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
}

impl RealWatcherWorker {
    fn new(root: PathBuf, index: RealIndexAsync) -> Self {
        RealWatcherWorker { root, index }
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

    async fn handle_event(&self, ev: &Event) -> anyhow::Result<()> {
        match ev.kind {
            EventKind::Remove(_) => {
                // Remove can't always tell whether a file or
                // directory was removed, so we don't bother checking.
                if let Some(path) = ev.paths.last().map(|p| self.to_model_path(p)).flatten() {
                    self.index.remove_file_or_dir(&path).await?;
                }
            }
            EventKind::Modify(ModifyKind::Data(DataChange::Content)) => {
                let realpath = match ev.paths.last() {
                    Some(p) => p.as_ref(),
                    None => {
                        return Ok(());
                    }
                };
                let m = fs::symlink_metadata(realpath).await?;
                // TODO: if ev is create and it is a directory, add all files recursively.
                if !m.is_file() {
                    return Ok(());
                }
                let path = match self.to_model_path(&realpath) {
                    Some(p) => p,
                    None => {
                        return Ok(());
                    }
                };

                let mtime = UnixTime::mtime(&m);
                if self
                    .index
                    .has_matching_file(&path, m.len(), &mtime)
                    .await
                    .unwrap_or(false)
                {
                    return Ok(());
                }

                // TODO: Use a Merkle tree for files above a certain size.
                // TODO: spawn here, as it can take a long time to compute the hash of a large file
                // TODO: check len and mtime after creating the hash, to be sure they did not change
                let mut hasher = hash::running();
                ReaderStream::with_capacity(File::open(realpath).await?, 64 * 1024)
                    .try_for_each(|chunk| {
                        hasher.update(chunk);

                        std::future::ready(Ok(()))
                    })
                    .await?;
                let hash = hasher.finalize();
                log::debug!("[{}] Add file {path} with hash {hash}", self.index.arena());
                self.index.add_file(&path, m.len(), &mtime, hash).await?;
            }
            _ => {}
        }

        // TODO: move file to, create dir, move dir to, change attributes (make file readable/unreadable)

        Ok(())
    }

    /// Convert a full path to a [model::Path] within the arena, if possible.
    fn to_model_path(&self, path: &std::path::Path) -> Option<model::Path> {
        // TODO: Should this use a PathResolver? We may or may not want
        // to care about partial/full files here.

        let relative = pathdiff::diff_paths(&path, &self.root)?;

        model::Path::from_real_path(&relative).ok()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{model::Arena, storage::real::index::RealIndexBlocking};

    use super::*;
    use assert_fs::TempDir;
    use assert_fs::fixture::ChildPath;
    use assert_fs::prelude::*;
    use tokio_retry::strategy::FixedInterval;

    struct Fixture {
        watcher: RealWatcher,
        index: RealIndexAsync,
        root: ChildPath,
        _tempdir: TempDir,
    }

    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();
            let tempdir = TempDir::new()?;
            let path = tempdir.path().join("index.db");
            let root = tempdir.child("root");
            root.create_dir_all()?;

            let index = RealIndexBlocking::open(Arena::from("test"), &path)?.into_async();
            let watcher = RealWatcher::spawn(root.path(), index.clone()).await?;

            Ok(Self {
                watcher,
                root,
                index,
                _tempdir: tempdir,
            })
        }
    }

    #[tokio::test]
    async fn shutdown() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        fixture.watcher.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn create_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        fixture.root.child("foobar").write_str("test")?;

        let path = model::Path::parse("foobar")?;

        let mut retry = FixedInterval::new(Duration::from_millis(50)).take(100);
        while !fixture.index.has_file(&path).await? {
            match retry.next() {
                Some(delay) => tokio::time::sleep(delay).await,
                None => panic!("'foobar' not added"),
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn modify_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let foobar = fixture.root.child("foobar");
        foobar.write_str("test")?;

        let path = model::Path::parse("foobar")?;

        let mut retry = FixedInterval::new(Duration::from_millis(50)).take(100);
        while !fixture.index.has_file(&path).await? {
            match retry.next() {
                Some(delay) => tokio::time::sleep(delay).await,
                None => panic!("'foobar' not added"),
            }
        }

        foobar.write_str("boo")?;
        let mtime = UnixTime::mtime(&fs::metadata(foobar.path()).await?);
        while !fixture.index.has_matching_file(&path, 3, &mtime).await? {
            match retry.next() {
                Some(delay) => tokio::time::sleep(delay).await,
                None => panic!("'foobar' not modified"),
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn remove_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let foobar = fixture.root.child("foobar");
        foobar.write_str("test")?;

        let path = model::Path::parse("foobar")?;

        let mut retry = FixedInterval::new(Duration::from_millis(50)).take(100);
        while !fixture.index.has_file(&path).await? {
            match retry.next() {
                Some(delay) => tokio::time::sleep(delay).await,
                None => panic!("'foobar' not added"),
            }
        }
        fs::remove_file(foobar.path()).await?;

        let mut retry = FixedInterval::new(Duration::from_millis(50)).take(100);
        while fixture.index.has_file(&path).await? {
            match retry.next() {
                Some(delay) => tokio::time::sleep(delay).await,
                None => panic!("'foobar' not removed"),
            }
        }

        Ok(())
    }
}
