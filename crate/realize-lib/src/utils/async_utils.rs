//! Async utilities for Realize - Symmetric File Syncer
//!
//! This module provides helpers for managing async tasks, including automatic abort on drop.
//!
//! # Example
//!
//! ```rust
//! use realize_lib::utils::async_utils::AbortOnDrop;
//! use tokio::task;
//!
//! # fn do_something() {}
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let handle = task::spawn(async move { do_something(); });
//! let abortable = AbortOnDrop::new(handle);
//! // abortable will abort the task if dropped
//! # });
//! ```

use tokio::task::{JoinError, JoinHandle};

/// RAII guard that aborts a Tokio task when dropped.
///
/// Useful for ensuring background tasks do not outlive their scope.
#[must_use]
pub struct AbortOnDrop<T> {
    handle: Option<JoinHandle<T>>,
}
impl<T> AbortOnDrop<T> {
    /// Create a new AbortOnDrop from a JoinHandle.
    pub fn new(handle: JoinHandle<T>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    /// Abort the task immediately.
    pub fn abort(self) {
        self.as_handle().abort();
    }

    /// Wait for the task to finish and return its result.
    pub async fn join(self) -> Result<T, JoinError> {
        self.as_handle().await
    }

    /// Take the original join handle (internal use).
    pub fn as_handle(mut self) -> JoinHandle<T> {
        // The handle should be there, because it's only removed by
        // drop.
        self.handle.take().expect("missing handle")
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
