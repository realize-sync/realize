#![allow(dead_code)] // work in progress

use super::index::{FileTableEntry, HistoryTableEntry, RealIndexAsync};
use crate::model::{Arena, Hash, Path, UnixTime};
use futures::StreamExt as _;
use tokio::{sync::mpsc, task::JoinHandle};
use uuid::Uuid;

/// Report files from a peer's local store.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Notification {
    Add {
        /// Containing arena.
        arena: Arena,

        /// Notification index.
        ///
        /// Should be stored and reported back as [Progress::last_seen] when re-subscribing.
        index: u64,

        /// File path within the arena.
        path: Path,

        /// File modification time, as reported by the Peer this file originates from..
        mtime: UnixTime,

        /// File size.
        size: u64,

        /// File content hash, used here to version the file content.
        hash: Hash,
    },
    Replace {
        /// Containing arena.
        arena: Arena,

        /// Notification index.
        ///
        /// Should be stored and reported back as [Progress::last_seen] when re-subscribing.
        index: u64,

        /// File path within the arena.
        path: Path,

        /// File modification time, as reported by the Peer this file originates from..
        mtime: UnixTime,

        /// File size.
        size: u64,

        /// File content hash, used here to version the file content.
        hash: Hash,

        /// Hash of the replaced content.
        old_hash: Hash,
    },
    Remove {
        /// Containing arena.
        arena: Arena,

        /// Notification index.
        ///
        /// Should be stored and reported back as [Progress::last_seen] when re-subscribing.
        index: u64,

        /// File path within the arena.
        path: Path,

        /// Hash of the removed content.
        old_hash: Hash,
    },

    /// Let the subscriber know that catchup has started.
    ///
    /// Catchup might need to be run even when the subscriber provided
    /// a [Progress]. When doing catchup, reported file versions
    /// overwrite existing versions. Once catchup is finished any file
    /// not reported during catchup should be considered removed.
    ///
    /// This design allows history trimming and rebuilding of the
    /// index.
    CatchupStart(Arena),

    /// A file reported during catchup.
    Catchup {
        /// Containing arena.
        arena: Arena,

        /// File path within the arena.
        path: Path,

        /// File modification time, as reported by the Peer this file originates from..
        mtime: UnixTime,

        /// File size
        size: u64,

        /// Hash of the file content
        hash: Hash,
    },

    /// Let the subscriber know that catchup is complete.
    CatchupComplete {
        /// Containing arena.
        arena: Arena,

        /// Content is complete up to this notification index.
        index: u64,
    },

    Connected {
        /// Containing arena.
        arena: Arena,

        /// UUID of the peer's store. This should be stored to
        /// generate [Progress].
        uuid: Uuid,
    },
}

impl Notification {
    fn arena(&self) -> &Arena {
        match self {
            Notification::Add { arena, .. } => arena,
            Notification::Replace { arena, .. } => arena,
            Notification::Remove { arena, .. } => arena,
            Notification::CatchupStart(arena) => arena,
            Notification::Catchup { arena, .. } => arena,
            Notification::CatchupComplete { arena, .. } => arena,
            Notification::Connected { arena, .. } => arena,
        }
    }
    fn path(&self) -> Option<&Path> {
        match self {
            Notification::Add { path, .. } => Some(path),
            Notification::Replace { path, .. } => Some(path),
            Notification::Remove { path, .. } => Some(path),
            Notification::CatchupStart(_) => None,
            Notification::Catchup { path, .. } => Some(path),
            Notification::CatchupComplete { .. } => None,
            Notification::Connected { .. } => None,
        }
    }
    fn index(&self) -> Option<u64> {
        match self {
            Notification::Add { index, .. } => Some(*index),
            Notification::Replace { index, .. } => Some(*index),
            Notification::Remove { index, .. } => Some(*index),
            Notification::CatchupStart(_) => None,
            Notification::Catchup { .. } => None,
            Notification::CatchupComplete { index, .. } => Some(*index),
            Notification::Connected { .. } => None,
        }
    }
}

/// A structure tracking the progress of
/// a subscriber.
///
/// It can passed by a subscriber at subscription time to catch up
/// notifications where it left off.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Progress {
    /// UUID of the database being subscribed to. Subscription indexes
    /// are only meaningful within that specific database.
    ///
    /// Usually from [RealIndexAsync::uuid]
    pub uuid: Uuid,

    /// Index of the last notification seen by the subscriber.
    pub last_seen: u64,
}

impl Progress {
    /// A new progress instance.
    pub fn new(uuid: Uuid, last_seen: u64) -> Self {
        Self { uuid, last_seen }
    }
}

/// Subscribe to notifications from the given index.
///
/// This call spawns a background that that sends notifications as
/// long as the given channel is alive.
///
/// This call creates a task that sends notifications in the background.
pub async fn subscribe(
    index: RealIndexAsync,
    tx: mpsc::Sender<Notification>,
    progress: Option<Progress>,
) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
    let mut last_seen = if let Some(progress) = progress
        && progress.uuid == *index.uuid()
    {
        progress.last_seen
    } else {
        0
    };

    tx.send(Notification::Connected {
        arena: index.arena().clone(),
        uuid: index.uuid().clone(),
    })
    .await?;

    let mut watch_rx = index.watch_history();
    let current = *watch_rx.borrow_and_update();

    Ok(tokio::spawn(async move {
        if last_seen == 0 && current > last_seen {
            catchup(&index, current, &tx).await?;
        } else {
            send_notifications(&index, last_seen, current, &tx).await?;
        }
        last_seen = current;
        loop {
            tokio::select!(
                _ = tx.closed() => {
                    break;
                }
                res = watch_rx.changed() => {
                    if res.is_err() {
                        break;
                    }
                    let current = *watch_rx.borrow_and_update();
                    if let Err(err) =  send_notifications(&index, last_seen, current, &tx).await {
                        if tx.is_closed() {
                            break;
                        }
                        return Err(err);
                    }
                    last_seen = current;
                }
            );
        }
        Ok(())
    }))
}

/// Report all files currently in the index as catchup.
///
/// catchup_index should be the current history index, as the
/// subscriber is caught up to the current index state, so to
/// everything up to that history index.
async fn catchup(
    index: &RealIndexAsync,
    catchup_index: u64,
    tx: &mpsc::Sender<Notification>,
) -> anyhow::Result<()> {
    tx.send(Notification::CatchupStart(index.arena().clone()))
        .await?;

    let mut all_files = index.all_files();
    while let Some((
        path,
        FileTableEntry {
            size, mtime, hash, ..
        },
    )) = all_files.next().await
    {
        tx.send(Notification::Catchup {
            arena: index.arena().clone(),
            path,
            size,
            mtime,
            hash,
        })
        .await?;
    }

    tx.send(Notification::CatchupComplete {
        arena: index.arena().clone(),
        index: catchup_index,
    })
    .await?;

    Ok(())
}

/// Report changes that happened after the given history index until
/// the given current index.
///
/// Note that since older file versions are not kept, the
/// notifications that are reported will only ever report the existing
/// file version even for older changes, so notifications might not
/// match history events.
async fn send_notifications(
    index: &RealIndexAsync,
    last_seen: u64,
    current: u64,
    tx: &mpsc::Sender<Notification>,
) -> anyhow::Result<()> {
    let mut range = index.history(last_seen + 1..current + 1);
    while let Some(entry) = range.next().await {
        let (hist_index, hist_entry) = entry?;
        let notification = match hist_entry {
            HistoryTableEntry::Add(path) => {
                if let Some(FileTableEntry {
                    size, mtime, hash, ..
                }) = index.get_file(&path).await?
                {
                    Some(Notification::Add {
                        index: hist_index,
                        arena: index.arena().clone(),
                        path,
                        size,
                        mtime,
                        hash,
                    })
                } else {
                    // The file might have been removed since the
                    // history entry was added.
                    None
                }
            }
            HistoryTableEntry::Replace(path, old_hash)
            | HistoryTableEntry::Remove(path, old_hash) => {
                // Replace and Remove are treated the same way,
                // because the difference depends on what is currently
                // in the index, which might have changed since the
                // entry was added.

                if let Some(FileTableEntry {
                    size, mtime, hash, ..
                }) = index.get_file(&path).await?
                {
                    Some(Notification::Replace {
                        index: hist_index,
                        arena: index.arena().clone(),
                        path,
                        size,
                        mtime,
                        hash,
                        old_hash,
                    })
                } else {
                    Some(Notification::Remove {
                        index: hist_index,
                        arena: index.arena().clone(),
                        path,
                        old_hash,
                    })
                }
            }
        };

        if let Some(notification) = notification {
            tx.send(notification).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::storage::testing;
    use crate::utils::hash;

    use super::*;

    fn test_arena() -> Arena {
        Arena::from("myarena")
    }

    struct Fixture {
        index: RealIndexAsync,
        current_time: UnixTime,
    }

    impl Fixture {
        fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();
            let index = testing::in_memory_index(test_arena())?.into_async();
            Ok(Self {
                index,
                current_time: UnixTime::from_secs(1234567890),
            })
        }

        fn now(&self) -> UnixTime {
            self.current_time.clone()
        }

        fn increment_time(&mut self, seconds: u64) {
            self.current_time = UnixTime::from_secs(self.current_time.as_secs() + seconds);
        }

        async fn subscribe(&self) -> anyhow::Result<mpsc::Receiver<Notification>> {
            let (tx, mut rx) = mpsc::channel(128);
            subscribe(self.index.clone(), tx, None).await?;
            self.expect_connected(&mut rx).await?;

            Ok(rx)
        }

        async fn subscribe_with_progress(
            &self,
            index: u64,
        ) -> anyhow::Result<mpsc::Receiver<Notification>> {
            let (tx, mut rx) = mpsc::channel(128);
            subscribe(
                self.index.clone(),
                tx,
                Some(Progress::new(self.index.uuid().clone(), index)),
            )
            .await?;
            self.expect_connected(&mut rx).await?;

            Ok(rx)
        }

        async fn expect_connected(
            &self,
            rx: &mut mpsc::Receiver<Notification>,
        ) -> anyhow::Result<()> {
            assert_eq!(
                Notification::Connected {
                    arena: test_arena(),
                    uuid: self.index.uuid().clone()
                },
                next(rx, "connected").await?
            );

            Ok(())
        }

        async fn add(&self, path: &str, content: &str) -> anyhow::Result<Path> {
            let path = Path::parse(path)?;
            let last = self.index.last_history_index().await?;
            self.index
                .add_file(
                    &path,
                    content.len() as u64,
                    &self.current_time,
                    hash::digest(content),
                )
                .await?;
            self.index.watch_history().wait_for(|v| *v > last).await?;

            Ok(path)
        }

        async fn delete(&self, path: &str) -> anyhow::Result<()> {
            let last = self.index.last_history_index().await?;
            self.index.remove_file_or_dir(&Path::parse(path)?).await?;
            self.index.watch_history().wait_for(|v| *v > last).await?;

            Ok(())
        }

        async fn consume(
            &self,
            mut rx: mpsc::Receiver<Notification>,
        ) -> anyhow::Result<Vec<Notification>> {
            let mut all = vec![];
            let last = self.index.last_history_index().await?;
            while let Some(notification) = rx.recv().await {
                let index = notification.index();
                all.push(notification);

                if let Some(index) = index
                    && index >= last
                {
                    break;
                }
            }

            return Ok(all);
        }
    }

    async fn next(
        rx: &mut mpsc::Receiver<Notification>,
        name: &str,
    ) -> anyhow::Result<Notification> {
        tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .map_err(|_| anyhow::anyhow!("{name}: timeout out"))?
            .ok_or_else(|| anyhow::anyhow!("{name}: channel closed"))
    }

    #[tokio::test]
    async fn add_notification() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let rx = fixture.subscribe().await?;
        let foo = fixture.add("foo", "foofoo").await?;
        let bar = fixture.add("bar", "barbar").await?;

        assert_eq!(
            vec![
                Notification::Add {
                    arena: test_arena(),
                    index: 1,
                    path: foo.clone(),
                    size: 6,
                    mtime: fixture.now(),
                    hash: hash::digest("foofoo")
                },
                Notification::Add {
                    arena: test_arena(),
                    index: 2,
                    path: bar.clone(),
                    size: 6,
                    mtime: fixture.now(),
                    hash: hash::digest("barbar")
                },
            ],
            fixture.consume(rx).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn replace_notification() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup()?;

        let mut rx = fixture.subscribe().await?;
        let foo = fixture.add("foo", "foo").await?;
        let foo_mtime = fixture.now();

        assert_eq!(
            Notification::Add {
                arena: test_arena(),
                index: 1,
                path: foo.clone(),
                size: 3,
                mtime: foo_mtime,
                hash: hash::digest("foo")
            },
            next(&mut rx, "add").await?
        );

        fixture.increment_time(10);
        fixture.add("foo", "foobar").await?;

        assert_eq!(
            Notification::Replace {
                arena: test_arena(),
                index: 2,
                path: foo.clone(),
                size: 6,
                mtime: fixture.now(),
                hash: hash::digest("foobar"),
                old_hash: hash::digest("foo"),
            },
            next(&mut rx, "replace").await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn remove_notification() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let mut rx = fixture.subscribe().await?;

        let foo = fixture.add("foo", "foo").await?;
        assert_eq!(
            Notification::Add {
                arena: test_arena(),
                index: 1,
                path: foo.clone(),
                size: 3,
                mtime: fixture.now(),
                hash: hash::digest("foo")
            },
            next(&mut rx, "add foo").await?
        );

        fixture.delete("foo").await?;
        assert_eq!(
            Notification::Remove {
                arena: test_arena(),
                index: 2,
                path: foo.clone(),
                old_hash: hash::digest("foo"),
            },
            next(&mut rx, "delete foo").await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn catchup_then_continue() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let foo = fixture.add("foo", "foo").await?;

        // Bar has existed in the index, but it's been deleted before
        // the first call to subscribe(). Neither the creation nor the
        // deletion will be reported during catchup.
        fixture.add("bar", "bar").await?;
        fixture.delete("bar").await?;

        let mut rx = fixture.subscribe().await?;
        assert_eq!(
            Notification::CatchupStart(test_arena()),
            next(&mut rx, "catchup start").await?
        );
        assert_eq!(
            Notification::Catchup {
                arena: test_arena(),
                path: foo.clone(),
                size: 3,
                mtime: fixture.now(),
                hash: hash::digest("foo")
            },
            next(&mut rx, "catchup").await?
        );
        assert_eq!(
            Notification::CatchupComplete {
                arena: test_arena(),
                index: 3,
            },
            next(&mut rx, "catchup complete").await?
        );
        let foobar = fixture.add("foobar", "foobar").await?;
        assert_eq!(
            Notification::Add {
                arena: test_arena(),
                index: 4,
                path: foobar.clone(),
                size: 6,
                mtime: fixture.now(),
                hash: hash::digest("foobar"),
            },
            next(&mut rx, "after catchup").await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn continue_after_resubscribing() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        fixture.add("foo", "foo").await?;
        let bar = fixture.add("bar", "bar").await?;

        // Previous subscription has seen foo, but not bar
        let rx = fixture.subscribe_with_progress(1).await?;
        assert_eq!(
            vec![Notification::Add {
                arena: test_arena(),
                index: 2,
                path: bar.clone(),
                size: 3,
                mtime: fixture.now(),
                hash: hash::digest("bar"),
            },],
            fixture.consume(rx).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn continue_after_file_modified_multiple_times() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let foo = fixture.add("foo", "1").await?;
        fixture.add("foo", "2").await?;
        fixture.add("foo", "3").await?;
        fixture.add("foo", "4").await?;

        // Only the latest version is reported as available, but all
        // intermediate versions are reported as replaced.
        let rx = fixture.subscribe_with_progress(1).await?;
        assert_eq!(
            vec![
                Notification::Replace {
                    arena: test_arena(),
                    index: 2,
                    path: foo.clone(),
                    size: 1,
                    mtime: fixture.now(),
                    hash: hash::digest("4"),
                    old_hash: hash::digest("1"),
                },
                Notification::Replace {
                    arena: test_arena(),
                    index: 3,
                    path: foo.clone(),
                    size: 1,
                    mtime: fixture.now(),
                    hash: hash::digest("4"),
                    old_hash: hash::digest("2"),
                },
                Notification::Replace {
                    arena: test_arena(),
                    index: 4,
                    path: foo.clone(),
                    size: 1,
                    mtime: fixture.now(),
                    hash: hash::digest("4"),
                    old_hash: hash::digest("3"),
                }
            ],
            fixture.consume(rx).await?
        );

        Ok(())
    }
}
