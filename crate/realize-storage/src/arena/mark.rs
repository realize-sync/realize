#![allow(dead_code)] // work in progress

use super::db::ArenaReadTransaction;
use super::types::Mark;
use crate::StorageError;
use realize_types::Path;

/// Trait for managing marks hierarchically by paths in an arena.
///
/// Paths can be files or directories, and marks are inherited from the most specific
/// path that has a mark set (file > directory > arena default).
///
/// When a mark changes, affected paths in the index and arena cache
/// are marked dirty. Changing the root mark will mark all files in
/// the cache and index dirty.
pub trait PathMarks: Send + Sync {
    /// Get the mark for a specific path, which can be a file or a directory.
    /// The mark can be inherited from one of its
    /// parents or it can be the root mark.
    ///
    /// If called on a non-existent file, get_mark returns the mark of
    /// the latest node it finds. This way, it can predict the mark of
    /// files not yet added.
    fn get_mark(&self, path: &Path) -> Result<Mark, StorageError>;

    fn get_mark_txn(&self, txn: &ArenaReadTransaction, path: &Path) -> Result<Mark, StorageError>;

    /// Set the default mark for the arena.
    fn set_arena_mark(&self, mark: Mark) -> Result<(), StorageError>;

    /// Set a mark for a specific path, which can be a file or a directory.
    fn set_mark(&self, path: &Path, mark: Mark) -> Result<(), StorageError>;

    /// Unset a mark for a specific path.
    fn clear_mark(&self, path: &Path) -> Result<(), StorageError>;
}
