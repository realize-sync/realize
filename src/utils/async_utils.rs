use tokio::task::{JoinError, JoinHandle};

// Abort a Tokio task when instance is dropped.
#[must_use]
pub struct AbortOnDrop<T> {
    handle: Option<JoinHandle<T>>,
}
impl<T> AbortOnDrop<T> {
    pub fn new(handle: JoinHandle<T>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    /// Take the original join handle.
    pub fn as_handle(mut self) -> JoinHandle<T> {
        // The handle should be there, because it's only removed by
        // drop.
        self.handle.take().expect("missing handle")
    }

    /// Abort right now.
    pub fn abort(self) {
        self.as_handle().abort();
    }

    /// Wait for the task to finish.
    pub async fn join(self) -> Result<T, JoinError> {
        self.as_handle().await
    }
}
impl<T> Drop for AbortOnDrop<T> {
    #[inline]
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}
