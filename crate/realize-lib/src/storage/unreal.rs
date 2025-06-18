mod r#async;
mod cache;
mod error;
#[cfg(target_os = "linux")]
mod updater;

pub use cache::FileEntry;
pub use cache::FileMetadata;
pub use cache::InodeAssignment;
pub use cache::ReadDirEntry;
pub use cache::UnrealCacheBlocking;
pub use cache::ROOT_DIR;
pub use error::UnrealCacheError;
pub use r#async::UnrealCacheAsync;

#[cfg(target_os = "linux")]
pub use updater::keep_cache_updated;
