use tokio::task::JoinHandle;

// Abort a Tokio task when instance is dropped.
pub struct AbortJoinHandleOnDrop<T> {
    handle: Option<JoinHandle<T>>,
}
impl<T> AbortJoinHandleOnDrop<T> {
    pub fn new(handle: JoinHandle<T>) -> Self {
        Self {
            handle: Some(handle),
        }
    }
}
impl<T> Drop for AbortJoinHandleOnDrop<T> {
    #[inline]
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}
