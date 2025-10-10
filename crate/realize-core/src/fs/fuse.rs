//! FUSE filesystem implementation for realize
//!
//! This module implements a FUSE filesystem that presents realize's distributed
//! file storage as a unified filesystem view.
//!
//! The architecture is split into several layers:
//! - `interface`: FUSE protocol handling and mount management
//! - `operations`: Core filesystem business logic
//! - `handles`: File handle management and lifecycle
//! - `error`: Error types and conversions
//! - `format`: Formatting utilities for xattr values

mod error;
mod format;
mod handles;
mod interface;
mod operations;

// Re-export the public API
pub use interface::{FuseHandle, export};
