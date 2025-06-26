use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use tokio::sync::mpsc;

use crate::model::Arena;

use super::{
    real::{Notification, StoreSubscribe},
    unreal::UnrealCacheBlocking,
};

pub fn in_memory_cache() -> anyhow::Result<UnrealCacheBlocking> {
    let cache = UnrealCacheBlocking::new(
        redb::Builder::new().create_with_backend(redb::backends::InMemoryBackend::new())?,
    )?;

    Ok(cache)
}

/// Fake implementation of StoreSubscribe.
///
/// This allows testing notification on platform that don't support
/// inotify.
pub struct FakeStoreSubscribe {
    arenas: HashSet<Arena>,
    subs: Mutex<InternalData>,
}

struct InternalData {
    subs: Vec<(Arena, mpsc::Sender<Notification>, bool)>,
    buffer: Vec<Notification>,
}

impl FakeStoreSubscribe {
    pub fn new<T>(arenas: T) -> Arc<Self>
    where
        T: IntoIterator<Item = Arena>,
    {
        Arc::new(Self {
            arenas: arenas.into_iter().collect::<HashSet<_>>(),
            subs: Mutex::new(InternalData {
                subs: vec![],
                buffer: vec![],
            }),
        })
    }

    /// Send a fake notification to all relevant subscriptions.
    pub async fn send(&self, n: Notification) -> anyhow::Result<()> {
        let mut lock = self.subs.lock().unwrap();
        for (arena, tx, catchup) in lock.subs.iter() {
            send_notification(&n, arena, tx, *catchup).await?;
        }
        lock.buffer.push(n);

        Ok(())
    }
}

async fn send_notification(
    n: &Notification,
    arena: &Arena,
    tx: &mpsc::Sender<Notification>,
    catchup: bool,
) -> Result<(), anyhow::Error> {
    Ok(if *(n.arena()) == *arena && (catchup || !n.is_catchup()) {
        tx.send(n.clone()).await?;
    })
}

impl StoreSubscribe for FakeStoreSubscribe {
    fn arenas(&self) -> Vec<Arena> {
        self.arenas.clone().into_iter().collect()
    }

    fn subscribe(
        &self,
        arena: Arena,
        tx: mpsc::Sender<Notification>,
        catchup: bool,
    ) -> anyhow::Result<bool> {
        if !self.arenas.contains(&arena) {
            return Ok(false);
        }

        let buffer;
        {
            let mut lock = self.subs.lock().unwrap();
            lock.subs.push((arena.clone(), tx.clone(), catchup));
            buffer = lock.buffer.clone();
        }

        tokio::spawn(async move {
            for n in buffer {
                let _ = send_notification(&n, &arena, &tx, catchup).await;
            }
        });

        Ok(true)
    }
}
