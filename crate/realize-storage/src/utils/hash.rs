//! Hashing utilities for Realize - Symmetric File Syncer
//!
//! This module provides BLAKE2b-256 hashing for file and buffer
//! content, used for file verification and delta computation.
//! It exposes both one-shot and incremental hash APIs, as required by
//! the sync protocol.
//!
//! # Examples
//!
//! Hash a buffer in one shot:
//! ```rust
//! use realize_lib::utils::hash::digest;
//! let hash = digest(b"hello world");
//! ```
//!
//! Incrementally hash data:
//! ```rust
//! use realize_lib::utils::hash::running;
//! let mut hasher = running();
//! hasher.update(b"foo");
//! hasher.update(b"bar");
//! let hash = hasher.finalize();
//! ```

use realize_types::Hash;
use blake2::digest::consts::U32;
use blake2::{Blake2b, Digest};

type Blake2b256 = Blake2b<U32>;

/// Produce a hash from an in-memory buffer.
pub fn digest(data: impl AsRef<[u8]>) -> Hash {
    Hash(Blake2b256::digest(data).into())
}

/// Produce a hash by appending multiple buffers.
///
///
/// # Examples
///
/// ```
/// let mut hasher = realize_lib::utils::hash::running();
/// hasher.update(b"hello");
/// hasher.update(b", world");
/// assert_eq!(hasher.finalize(),
///     realize_lib::utils::hash::digest("hello, world"));
/// ```
pub fn running() -> RunningHash {
    RunningHash {
        digest: Blake2b256::new(),
    }
}

pub struct RunningHash {
    digest: Blake2b256,
}

impl RunningHash {
    /// Add data to be hashed
    pub fn update(&mut self, data: impl AsRef<[u8]>) {
        self.digest.update(data)
    }

    /// Build the hash with the data added so far.
    pub fn finalize(self) -> Hash {
        Hash(self.digest.finalize().into())
    }
}
