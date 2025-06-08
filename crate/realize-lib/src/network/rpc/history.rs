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

    /// Called before sending any notifications, to let the history
    /// service know any filesystem change will be reported from then
    /// on.
    ///
    /// Only the first call matters; further calls are ignored.
    ///
    /// The arena vector is the subset of arenas that are watched. It
    /// might be empty.
    async fn ready(arenas: Vec<Arena>);

    /// Receive a batch of history notifications.
    async fn notify(batch: Vec<Notification>);
}
