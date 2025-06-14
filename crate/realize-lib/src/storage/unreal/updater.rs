use crate::model::Peer;
use crate::storage::real::Notification;
use crate::storage::unreal::UnrealCacheAsync;
use tokio::sync::mpsc;

/// Update the cache based on peer notifications.
///
/// This runs as long as the receiver is up.
pub async fn keep_cache_updated(
    cache: UnrealCacheAsync,
    mut rx: mpsc::Receiver<(Peer, Notification)>,
) {
    while let Some((peer, notification)) = rx.recv().await {
        if let Err(err) = match &notification {
            Notification::CatchingUp { .. } => Ok(()),
            Notification::Catchup {
                arena,
                path,
                size,
                mtime,
            } => cache.link(&peer, arena, path, *size, *mtime).await,
            Notification::Ready { .. } => Ok(()),
            Notification::Link {
                arena,
                path,
                size,
                mtime,
            } => cache.link(&peer, arena, path, *size, *mtime).await,
            Notification::Unlink { arena, path, mtime } => {
                cache.unlink(&peer, arena, path, *mtime).await
            }
        } {
            log::warn!(
                "Error updating {}/{:?} in cache: {}",
                notification.arena(),
                notification.path(),
                err
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::Duration;

    use super::*;
    use crate::model::Arena;
    use crate::storage::real::LocalStorage;
    use crate::storage::real::Notification;
    use assert_fs::fixture::ChildPath;
    use assert_fs::prelude::FileWriteStr as _;
    use assert_fs::prelude::PathChild as _;
    use assert_fs::prelude::PathCreateDir as _;
    use assert_fs::TempDir;
    use tokio_retry::strategy::FixedInterval;

    struct Fixture {
        storage: LocalStorage,
        arena: Arena,
        arena_root: ChildPath,
        cache: UnrealCacheAsync,
        peer: Peer,
        tx: mpsc::Sender<(Peer, Notification)>,
        _tempdir: TempDir,
    }

    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            let tempdir = TempDir::new()?;
            let arena = Arena::from("test");
            let arena_root = tempdir.child("arena");
            arena_root.create_dir_all()?;

            let storage = LocalStorage::single(&arena, arena_root.path());
            let peer = Peer::from("remote");
            let (tx, rx) = mpsc::channel(10);

            let cache = UnrealCacheAsync::open(tempdir.child("cache.redb").path()).await?;

            tokio::spawn(keep_cache_updated(cache.clone(), rx));

            Ok(Self {
                storage,
                arena,
                arena_root,
                cache,
                peer,
                tx,
                _tempdir: tempdir,
            })
        }

        async fn subscribe(&self) -> anyhow::Result<()> {
            let (peerless_tx, mut peerless_rx) = mpsc::channel(10);
            let subscribed = self
                .storage
                .subscribe(self.arena.clone(), peerless_tx, true /*catchup*/)
                .await?;
            assert!(subscribed);

            // Pretend the notifications come from a remote peer
            let tx = self.tx.clone();
            let peer = self.peer.clone();
            tokio::spawn(async move {
                while let Some(n) = peerless_rx.recv().await {
                    tx.send((peer.clone(), n)).await?;
                }

                Ok::<(), anyhow::Error>(())
            });

            Ok(())
        }
    }

    #[tokio::test]
    async fn cache_updater_applies_notifications() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        fixture.arena_root.child("one.txt").write_str("test")?;
        let two = fixture.arena_root.child("two.txt");
        two.write_str("test")?;
        fixture.subscribe().await?;
        fs::remove_file(two.path())?;
        fixture.arena_root.child("three.txt").write_str("test")?;

        // It may take some time, but eventually, we should end up
        // with just the goal files.
        let goal = vec!["one.txt".to_string(), "three.txt".to_string()];
        let mut retry = FixedInterval::new(Duration::from_millis(50)).take(100);
        loop {
            let mut got = fixture
                .cache
                .blocking()
                .readdir(1)?
                .map(|(n, _)| n)
                .collect::<Vec<_>>();
            got.sort();

            if got == goal {
                break;
            }
            if let Some(delay) = retry.next() {
                tokio::time::sleep(delay).await;
            } else {
                assert_unordered::assert_eq_unordered!(goal.clone(), got);
            }
        }

        Ok(())
    }
}
