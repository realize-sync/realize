//! Service definition for HistoryService
//!
//! This module defines the HistoryService trait for use with tarpc 0.36.

pub mod client;
pub mod server;

use crate::{model::Arena, storage::real::Notification};

/// Tag that identifies [HistoryService] when connecting.
pub const TAG: &[u8; 4] = b"HIST";

#[tarpc::service]
pub trait HistoryService {
    /// Arena to provide history for.
    ///
    /// Caller will ignore arenas for which it has no local data.
    async fn arenas() -> Vec<Arena>;

    /// Reports the subset of arenas for which notifications
    /// will be sent.
    async fn available(arenas: Vec<Arena>);

    /// Receive a batch of history notifications.
    async fn notify(batch: Vec<Notification>);
}
