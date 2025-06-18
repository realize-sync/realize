mod cache;
mod error;
mod future;
mod sync;
#[cfg(target_os = "linux")]
mod updater;

pub use cache::FileEntry;
pub use cache::FileMetadata;
pub use cache::InodeAssignment;
pub use cache::ReadDirEntry;
pub use cache::ROOT_DIR;
pub use error::UnrealCacheError;
pub use future::UnrealCacheAsync;
pub use sync::UnrealCacheBlocking;

#[cfg(target_os = "linux")]
pub use updater::keep_cache_updated;
