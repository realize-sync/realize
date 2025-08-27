#![allow(dead_code)] // in progress
use crate::fs::downloader::Downloader;
use fuser::{Filesystem, MountOption};
use nix::{errno::Errno, libc::c_int};
use realize_storage::{FileMetadata, GlobalCache, Inode, InodeAssignment, StorageError};
use std::ffi::OsString;
use std::{sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::runtime::Handle;

/// Mount the cache as FUSE filesystem at the given mountpoint.
pub fn export(
    cache: Arc<GlobalCache>,
    downloader: Downloader,
    mountpoint: &std::path::Path,
) -> anyhow::Result<FuseHandle> {
    let fs = RealizeFs {
        cache,
        downloader,
        handle: Handle::current(),
    };
    let bgsession =
        fuser::spawn_mount2(fs, mountpoint, &[MountOption::AutoUnmount, MountOption::RO])?;
    log::info!("FUSE filesystem mounted on {mountpoint:?}");

    Ok(FuseHandle { inner: bgsession })
}

/// Handle that must be kept as long as the filesystem must
/// remain mounted.
///
/// To unmount the filesystem, call join() on the handle.
pub struct FuseHandle {
    inner: fuser::BackgroundSession,
}

impl FuseHandle {
    /// Unmount the filesystem and wait for the fuse run loop to stop.
    pub async fn join(self) -> Result<(), tokio::task::JoinError> {
        let Self { inner } = self;
        tokio::task::spawn_blocking(move || inner.join()).await
    }
}

struct RealizeFs {
    /// Handle on the main tokio runtime (multithreaded)
    handle: Handle,

    cache: Arc<GlobalCache>,
    downloader: Downloader,
}

// Code in this impl runs on a custom thread started by fuser. Use
// Handle::spawn to run async code. reply can moved into the spawn and
// captured there; there's no need for the function to return before
// filling in the reply.
impl Filesystem for RealizeFs {
    fn init(
        &mut self,
        _req: &fuser::Request<'_>,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), nix::libc::c_int> {
        Ok(())
    }

    fn destroy(&mut self) {}

    fn lookup(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let cache = self.cache.clone();
        let name = name.to_owned();

        self.handle.spawn(async move {
            match do_lookup(cache, parent, name).await {
                Err(err) => reply.error(err.into_errno()),
                Ok(attr) => reply.entry(&Duration::from_secs(1), &attr, 0),
            }
        });
    }

    fn forget(&mut self, _req: &fuser::Request<'_>, _ino: u64, _nlookup: u64) {}

    fn batch_forget(&mut self, req: &fuser::Request<'_>, nodes: &[fuser::fuse_forget_one]) {
        for node in nodes {
            self.forget(req, node.nodeid, node.nlookup);
        }
    }

    fn getattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: Option<u64>,
        reply: fuser::ReplyAttr,
    ) {
        let cache = self.cache.clone();

        self.handle.spawn(async move {
            match do_getattr(cache, ino).await {
                Err(err) => reply.error(err.into_errno()),
                Ok(attr) => reply.attr(&Duration::from_secs(1), &attr),
            }
        });
    }

    fn readlink(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyData) {
        log::debug!("[Not Implemented] readlink(ino: {:#x?})", ino);
        reply.error(nix::libc::ENOSYS);
    }

    fn open(&mut self, _req: &fuser::Request<'_>, _ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        reply.opened(0, 0);
    }

    fn read(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        let downloader = self.downloader.clone();

        self.handle.spawn(async move {
            match do_read(downloader, ino, offset, size).await {
                Err(err) => reply.error(err.into_errno()),
                Ok(data) => reply.data(&data),
            }
        });
    }

    fn release(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn opendir(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        _flags: i32,
        reply: fuser::ReplyOpen,
    ) {
        reply.opened(0, 0);
    }

    fn readdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        let cache = self.cache.clone();

        self.handle.spawn(async move {
            match do_readdir(cache, ino, offset).await {
                Err(err) => reply.error(err.into_errno()),
                Ok(entries) => {
                    for (ino, offset, kind, name) in entries {
                        if reply.add(ino, offset, kind, name) {
                            break;
                        }
                    }
                    reply.ok();
                }
            }
        });
    }

    fn readdirplus(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        reply: fuser::ReplyDirectoryPlus,
    ) {
        log::debug!(
            "[Not Implemented] readdirplus(ino: {:#x?}, offset: {})",
            ino,
            offset
        );
        reply.error(nix::libc::ENOSYS);
    }

    fn releasedir(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn statfs(&mut self, _req: &fuser::Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        reply.statfs(0, 0, 0, 0, 0, 512, 255, 0);
    }

    fn getxattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        name: &std::ffi::OsStr,
        size: u32,
        reply: fuser::ReplyXattr,
    ) {
        log::debug!(
            "[Not Implemented] getxattr(ino: {:#x?}, name: {:?}, size: {})",
            ino,
            name,
            size
        );
        reply.error(nix::libc::ENOSYS);
    }

    fn listxattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        size: u32,
        reply: fuser::ReplyXattr,
    ) {
        log::debug!(
            "[Not Implemented] listxattr(ino: {:#x?}, size: {})",
            ino,
            size
        );
        reply.error(nix::libc::ENOSYS);
    }

    fn removexattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        log::debug!(
            "[Not Implemented] removexattr(ino: {:#x?}, name: {:?})",
            ino,
            name
        );
        reply.error(nix::libc::ENOSYS);
    }

    fn access(&mut self, _req: &fuser::Request<'_>, ino: u64, mask: i32, reply: fuser::ReplyEmpty) {
        log::debug!("[Not Implemented] access(ino: {:#x?}, mask: {})", ino, mask);
        reply.error(nix::libc::ENOSYS);
    }

    fn lseek(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        whence: i32,
        reply: fuser::ReplyLseek,
    ) {
        log::debug!(
            "[Not Implemented] lseek(ino: {:#x?}, fh: {}, offset: {}, whence: {})",
            ino,
            fh,
            offset,
            whence
        );
        reply.error(nix::libc::ENOSYS);
    }
}

async fn do_lookup(
    cache: Arc<GlobalCache>,
    parent: u64,
    name: OsString,
) -> Result<fuser::FileAttr, FuseError> {
    let name_str = name.to_str().ok_or(FuseError::Utf8)?;

    let (inode, assignment) = cache.lookup(Inode(parent), name_str).await?;
    match assignment {
        InodeAssignment::Directory => {
            let mtime = cache.dir_mtime(inode).await?;
            return Ok(build_dir_attr(inode, mtime));
        }
        InodeAssignment::File => {
            let metadata = cache.file_metadata(inode).await?;
            return Ok(build_file_attr(inode, &metadata));
        }
    };
}

async fn do_getattr(cache: Arc<GlobalCache>, ino: u64) -> Result<fuser::FileAttr, FuseError> {
    let (file_metadata, dir_mtime) =
        tokio::join!(cache.file_metadata(Inode(ino)), cache.dir_mtime(Inode(ino)));
    if let Ok(mtime) = dir_mtime {
        Ok(build_dir_attr(Inode(ino), mtime))
    } else {
        Ok(build_file_attr(
            Inode(ino),
            &file_metadata.map_err(FuseError::Cache)?,
        ))
    }
}

async fn do_read(
    downloader: Downloader,
    ino: u64,
    offset: i64,
    size: u32,
) -> Result<Vec<u8>, FuseError> {
    let reader = downloader
        .reader(Inode(ino))
        .await
        .map_err(FuseError::Cache)?;
    let mut reader = tokio::io::BufReader::new(reader);
    reader
        .seek(tokio::io::SeekFrom::Start(offset as u64))
        .await
        .map_err(FuseError::Io)?;

    let mut buffer = vec![0; size as usize];
    let bytes_read = reader.read(&mut buffer).await.map_err(FuseError::Io)?;
    buffer.truncate(bytes_read);

    Ok(buffer)
}

async fn do_readdir(
    cache: Arc<GlobalCache>,
    ino: u64,
    offset: i64,
) -> Result<Vec<(u64, i64, fuser::FileType, std::ffi::OsString)>, FuseError> {
    let mut entries = cache.readdir(Inode(ino)).await.map_err(FuseError::Cache)?;
    entries.sort_by(|a, b| a.1.cmp(&b.1));

    let mut fuse_entries = Vec::new();
    for (name, inode, assignment) in entries.into_iter().skip(offset as usize) {
        fuse_entries.push((
            inode.as_u64(),
            fuse_entries.len() as i64 + 1,
            match assignment {
                InodeAssignment::Directory => fuser::FileType::Directory,
                InodeAssignment::File => fuser::FileType::RegularFile,
            },
            name.into(),
        ));
    }

    Ok(fuse_entries)
}

fn build_file_attr(inode: Inode, metadata: &FileMetadata) -> fuser::FileAttr {
    let uid = nix::unistd::getuid().as_raw();
    let gid = nix::unistd::getgid().as_raw();
    let mtime = std::time::SystemTime::UNIX_EPOCH
        + std::time::Duration::from_secs(metadata.mtime.as_secs())
        + std::time::Duration::from_nanos(metadata.mtime.subsec_nanos() as u64);

    fuser::FileAttr {
        ino: inode.as_u64(),
        size: metadata.size,
        blocks: (metadata.size + 511) / 512, // Round up to block size
        atime: mtime,
        mtime,
        ctime: mtime,
        crtime: std::time::SystemTime::UNIX_EPOCH,
        kind: fuser::FileType::RegularFile,
        perm: 0o0440,
        nlink: 1,
        uid,
        gid,
        rdev: 0,
        blksize: 512,
        flags: 0,
    }
}

fn build_dir_attr(inode: Inode, mtime: realize_types::UnixTime) -> fuser::FileAttr {
    let uid = nix::unistd::getuid().as_raw();
    let gid = nix::unistd::getgid().as_raw();
    let mtime_systime = std::time::SystemTime::UNIX_EPOCH
        + std::time::Duration::from_secs(mtime.as_secs())
        + std::time::Duration::from_nanos(mtime.subsec_nanos() as u64);

    fuser::FileAttr {
        ino: inode.as_u64(),
        size: 512,
        blocks: 1,
        atime: mtime_systime,
        mtime: mtime_systime,
        ctime: mtime_systime,
        crtime: std::time::SystemTime::UNIX_EPOCH,
        kind: fuser::FileType::Directory,
        perm: 0o0550,
        nlink: 1,
        uid,
        gid,
        rdev: 0,
        blksize: 512,
        flags: 0,
    }
}

/// Intermediate error type to catch and convert to libc errno to
/// report errors to fuser.
#[derive(Debug, thiserror::Error)]
enum FuseError {
    #[error(transparent)]
    Cache(#[from] StorageError),

    #[error("invalid UTF-8 string")]
    Utf8,

    #[error("I/O error")]
    Io(#[from] std::io::Error),
}

impl FuseError {
    fn into_errno(self) -> c_int {
        match self {
            FuseError::Cache(err) => storage_errno(err),
            FuseError::Utf8 => nix::libc::EINVAL,
            FuseError::Io(ioerr) => io_errno(ioerr),
        }
    }
}

fn storage_errno(err: StorageError) -> c_int {
    io_errno(err.into())
}

fn io_errno(err: std::io::Error) -> c_int {
    if let Ok(errno) = <std::io::Error as TryInto<Errno>>::try_into(err) {
        errno as c_int
    } else {
        nix::libc::EIO
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::testing::HouseholdFixture;
    use realize_storage::utils::hash;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::path::PathBuf;
    use std::time::Instant;
    use tempfile::TempDir;
    use tokio::fs;

    /// Fixture for mounting FUSE filesystem for testing
    struct FuseFixture {
        inner: HouseholdFixture,
        mountpoint: TempDir,
        fuse_handle: Option<FuseHandle>,
    }

    impl FuseFixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();
            let household_fixture = HouseholdFixture::setup().await?;
            let mountpoint = TempDir::new()?;

            Ok(Self {
                inner: household_fixture,
                mountpoint,
                fuse_handle: None,
            })
        }

        /// Mount the FUSE filesystem for the given peer
        ///
        /// WARNING: use async I/O operations from tokio *exclusively*
        /// on this filesystem. Using blocking I/O will block, as
        /// there would be no tokio free thread to execute them on.
        async fn mount(&mut self, household: Arc<crate::rpc::Household>) -> anyhow::Result<()> {
            let a = HouseholdFixture::a();
            let cache = self.inner.cache(a)?;
            let downloader = Downloader::new(household, cache.clone());

            let m = fs::metadata(self.mountpoint.path()).await?;
            let original_dev = m.dev();
            log::debug!("mounting {}", self.mountpoint.path().display());
            let handle = export(cache.clone(), downloader, self.mountpoint.path())?;
            self.fuse_handle = Some(handle);

            let limit = Instant::now() + Duration::from_secs(3);
            while fs::metadata(self.mountpoint.path()).await?.dev() == original_dev
                && Instant::now() < limit
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert_ne!(
                fs::metadata(self.mountpoint.path()).await?.dev(),
                original_dev
            );
            log::debug!("mounting {}", self.mountpoint.path().display());

            Ok(())
        }

        /// Get the path to the mounted filesystem
        fn mount_path(&self) -> PathBuf {
            self.mountpoint.path().to_path_buf()
        }

        /// Unmount the filesystem.
        ///
        /// WARNING: keeping open files might cause this function to block.
        async fn unmount(&mut self) -> anyhow::Result<()> {
            log::debug!("unmounting {}...", self.mountpoint.path().display());
            if let Some(handle) = self.fuse_handle.take() {
                tokio::time::timeout(Duration::from_secs(3), handle.join()).await??;
            }
            log::debug!("unmounted {}...", self.mountpoint.path().display());
            Ok(())
        }
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn root_dir() -> anyhow::Result<()> {
        let start_time =
            std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH)?;
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let _a = HouseholdFixture::a();

                // Mount the filesystem
                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let root_attr = fs::metadata(&mount_path).await?;

                // Check root directory attributes
                assert!(root_attr.is_dir());
                assert_eq!(0o0550, root_attr.permissions().mode() & 0o777);
                assert_eq!(nix::unistd::getuid().as_raw(), root_attr.uid());
                assert_eq!(nix::unistd::getgid().as_raw(), root_attr.gid());

                // mtime should have been set when the arena was added
                let mtime = root_attr
                    .modified()?
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)?;
                assert!(mtime.as_secs() >= start_time.as_secs());

                fixture.unmount().await?;

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn arena_dir() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let _a = HouseholdFixture::a();

                // Mount the filesystem
                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena = HouseholdFixture::test_arena();
                let arena_path = mount_path.join(arena.as_str());

                // Check that arena directory exists and has correct attributes
                let arena_attr = fs::metadata(&arena_path).await?;
                assert!(arena_attr.is_dir());
                assert_eq!(0o0550, arena_attr.permissions().mode() & 0o777);
                assert_eq!(nix::unistd::getuid().as_raw(), arena_attr.uid());
                assert_eq!(nix::unistd::getgid().as_raw(), arena_attr.gid());

                fixture.unmount().await?;

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn file_attrs() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                // Create a file in peer B's arena
                fixture.inner.write_file(b, "somefile.txt", "test!").await?;
                fixture
                    .inner
                    .wait_for_file_in_cache(a, "somefile.txt", &hash::digest("test!"))
                    .await?;

                // Mount the filesystem
                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                let file_path = arena_path.join("somefile.txt");

                // Check file attributes
                let file_attr = fs::metadata(&file_path).await?;
                assert!(file_attr.is_file());
                assert_eq!(0o0440, file_attr.permissions().mode() & 0o777);
                assert_eq!(nix::unistd::getuid().as_raw(), file_attr.uid());
                assert_eq!(nix::unistd::getgid().as_raw(), file_attr.gid());
                assert_eq!(5, file_attr.len());

                fixture.unmount().await?;

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn file_content() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                let b_dir = fixture.inner.arena_root(b);
                let file = b_dir.join("hello.txt");
                fs::write(&file, "world").await?;
                fixture
                    .inner
                    .wait_for_file_in_cache(a, "hello.txt", &hash::digest("world"))
                    .await?;

                // Mount the filesystem and access its content through
                // normal async I/O operation.
                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                let file_path = arena_path.join("hello.txt");

                // Test normal read
                let content = fs::read_to_string(&file_path).await?;
                assert_eq!("world", content);

                // Test reading the entire file as bytes
                let content_bytes = fs::read(&file_path).await?;
                assert_eq!(b"world", content_bytes.as_slice());

                fixture.unmount().await?;

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }
}
