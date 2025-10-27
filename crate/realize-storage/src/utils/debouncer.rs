use std::{sync::Arc, time::Duration};
use tokio::{sync::Semaphore, task::JoinError};
use tokio_util::task::JoinMap;

pub(crate) struct DebouncerMap<K, V> {
    map: JoinMap<K, V>,
    delay: Duration,
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
            delay: debounce,
            sem,
        }
    }

    pub(crate) async fn join_next(&mut self) -> Option<(K, Result<V, JoinError>)> {
        self.map.join_next().await
    }

    /// Debounce `task` and, if `limited` is true, limit its parallelism.
    pub(crate) fn spawn_limited<F>(&mut self, key: K, limited: bool, task: F)
    where
        F: Future<Output = V>,
        F: Send + 'static,
        V: Send,
    {
        let debounce = self.delay;
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
