use std::{sync::Arc, time::Duration};
use tokio::{sync::Semaphore, task::JoinError};
use tokio_util::task::JoinMap;

pub(crate) struct DebouncerMap<K, V> {
    map: JoinMap<K, V>,
    debounce: Duration,
    sem: Option<Arc<Semaphore>>,
}

impl<K, V> DebouncerMap<K, V>
where
    K: std::hash::Hash + Eq,
    V: 'static,
{
    pub(crate) fn new(debounce: Duration, max_parallelism: usize) -> Self {
        let sem = if max_parallelism > 0 {
            Some(Arc::new(Semaphore::new(max_parallelism)))
        } else {
            None
        };
        Self {
            map: JoinMap::new(),
            debounce,
            sem,
        }
    }

    pub(crate) async fn join_next(&mut self) -> Option<(K, Result<V, JoinError>)> {
        self.map.join_next().await
    }

    /// Debounce `task`, don't limit parallelism.
    pub(crate) fn spawn_unlimited<F>(&mut self, key: K, task: F)
    where
        F: Future<Output = V>,
        F: Send + 'static,
        V: Send,
    {
        self.spawn_limited(key, false, task)
    }

    /// Debounce `task` and, if `limited` is true, limit its parallelism.
    pub(crate) fn spawn_limited<F>(&mut self, key: K, limited: bool, task: F)
    where
        F: Future<Output = V>,
        F: Send + 'static,
        V: Send,
    {
        let debounce = self.debounce;
        let sem = if limited { self.sem.clone() } else { None };
        self.map.spawn(key, async move {
            if !debounce.is_zero() {
                tokio::time::sleep(debounce).await;
            }
            let _permit = if let Some(sem) = &sem {
                Some(sem.acquire().await.unwrap())
            } else {
                None
            };

            task.await
        })
    }
}
