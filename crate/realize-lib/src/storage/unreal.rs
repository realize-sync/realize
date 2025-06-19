mod cache;
mod downloader;
mod error;
mod future;
mod sync;
#[cfg(test)]
pub mod testing;
#[cfg(target_os = "linux")]
mod updater;

pub use cache::FileEntry;
pub use cache::FileMetadata;
pub use cache::InodeAssignment;
pub use cache::ReadDirEntry;
pub use cache::ROOT_DIR;
pub use downloader::Download;
pub use downloader::Downloader;
pub use error::UnrealCacheError;
pub use future::UnrealCacheAsync;
pub use sync::UnrealCacheBlocking;

#[cfg(target_os = "linux")]
pub use updater::keep_cache_updated;
