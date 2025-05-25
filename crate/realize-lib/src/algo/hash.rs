//! Produce hashes using the appropriate algorithm.

use crate::model::service::Hash;
use blake2::{Blake2b, Digest, digest::consts::U32};

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
/// let mut hasher = realize_lib::algo::hash::running();
/// hasher.update(b"hello");
/// hasher.update(b", world");
/// assert_eq!(hasher.finalize(),
///     realize_lib::algo::hash::digest("hello, world"));
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
