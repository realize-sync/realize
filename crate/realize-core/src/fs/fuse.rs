#![allow(dead_code)] // in progress
use crate::fs::downloader::Downloader;
use fuser::{Filesystem, MountOption};
use nix::libc::c_int;
use realize_storage::{
    DirMetadata, FileMetadata, GlobalCache, PathId, PathAssignment, StorageError,
};
use std::ffi::OsString;
use std::time::SystemTime;
use std::{sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::runtime::Handle;

/// Mount the cache as FUSE filesystem at the given mountpoint.
pub fn export(
    cache: Arc<GlobalCache>,
    downloader: Downloader,
    mountpoint: &std::path::Path,
    umask: u16,
) -> anyhow::Result<FuseHandle> {
    let fs = RealizeFs {
        handle: Handle::current(),
        inner: Arc::new(InnerRealizeFs {
            cache,
            downloader,
            umask,
        }),
    };
    let bgsession = fuser::spawn_mount2(
        fs,
        mountpoint,
        &[
            MountOption::AutoUnmount,
            MountOption::AllowOther,
            MountOption::DefaultPermissions,
            MountOption::NoDev,
            MountOption::NoSuid,
            MountOption::NoExec,
            MountOption::NoAtime,
            MountOption::Async,
            MountOption::FSName("realized".to_string()),
            MountOption::Subtype("realize".to_string()),
        ],
    )?;

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

    /// Unmount the filesystem and wait for the fuse run loop to stop.
    pub fn join_blocking(self) {
        self.inner.join();
    }
}

struct RealizeFs {
    /// Handle on the main tokio runtime (multithreaded)
    handle: Handle,

    inner: Arc<InnerRealizeFs>,
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
        let inner = Arc::clone(&self.inner);
        let name = name.to_owned();

        self.handle.spawn(async move {
            match inner.lookup(parent, name).await {
                Err(err) => reply.error(err.log_and_convert()),
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
        let inner = Arc::clone(&self.inner);

        self.handle.spawn(async move {
            match inner.getattr(ino).await {
                Err(err) => reply.error(err.log_and_convert()),
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
        let inner = Arc::clone(&self.inner);

        self.handle.spawn(async move {
            match inner.read(ino, offset, size).await {
                Err(err) => reply.error(err.log_and_convert()),
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
        let inner = Arc::clone(&self.inner);

        self.handle.spawn(async move {
            match inner.readdir(ino, offset, &mut reply).await {
                Err(err) => reply.error(err.log_and_convert()),
                Ok(()) => reply.ok(),
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

    fn unlink(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let inner = Arc::clone(&self.inner);
        let name = name.to_owned();

        self.handle.spawn(async move {
            match inner.unlink(parent, name).await {
                Err(err) => reply.error(err.log_and_convert()),
                Ok(()) => reply.ok(),
            }
        });
    }

    fn link(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        newparent: u64,
        newname: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let inner = Arc::clone(&self.inner);
        let newname = newname.to_owned();

        self.handle.spawn(async move {
            match inner.link(ino, newparent, newname).await {
                Err(err) => reply.error(err.log_and_convert()),
                Ok(attr) => reply.entry(&Duration::from_secs(1), &attr, 0),
            }
        });
    }

    fn mkdir(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        umask: u32,
        reply: fuser::ReplyEntry,
    ) {
        let inner = Arc::clone(&self.inner);
        let name = name.to_owned();

        self.handle.spawn(async move {
            match inner.mkdir(parent, name, mode, umask).await {
                Err(err) => reply.error(err.log_and_convert()),
                Ok(attr) => reply.entry(&Duration::from_secs(1), &attr, 0),
            }
        });
    }

    fn rmdir(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let inner = Arc::clone(&self.inner);
        let name = name.to_owned();

        self.handle.spawn(async move {
            match inner.rmdir(parent, name).await {
                Err(err) => reply.error(err.log_and_convert()),
                Ok(()) => reply.ok(),
            }
        });
    }
}

struct InnerRealizeFs {
    cache: Arc<GlobalCache>,
    downloader: Downloader,
    umask: u16,
}

impl InnerRealizeFs {
    async fn lookup(&self, parent: u64, name: OsString) -> Result<fuser::FileAttr, FuseError> {
        let name = name.to_str().ok_or(FuseError::Utf8)?;

        let (pathid, assignment) = self.cache.lookup(PathId(parent), name).await?;
        match assignment {
            PathAssignment::Directory => {
                let metadata = self.cache.dir_metadata(pathid).await?;
                return Ok(self.build_dir_attr(pathid, metadata));
            }
            PathAssignment::File => {
                let metadata = self.cache.file_metadata(pathid).await?;
                return Ok(self.build_file_attr(pathid, &metadata));
            }
        };
    }

    async fn getattr(&self, ino: u64) -> Result<fuser::FileAttr, FuseError> {
        let (file_metadata, dir_mtime) = tokio::join!(
            self.cache.file_metadata(PathId(ino)),
            self.cache.dir_metadata(PathId(ino))
        );
        if let Ok(mtime) = dir_mtime {
            Ok(self.build_dir_attr(PathId(ino), mtime))
        } else {
            Ok(self.build_file_attr(PathId(ino), &file_metadata.map_err(FuseError::Cache)?))
        }
    }

    async fn read(&self, ino: u64, offset: i64, size: u32) -> Result<Vec<u8>, FuseError> {
        let reader = self
            .downloader
            .reader(PathId(ino))
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

    async fn readdir(
        &self,
        ino: u64,
        offset: i64,
        reply: &mut fuser::ReplyDirectory,
    ) -> Result<(), FuseError> {
        let mut entries = self
            .cache
            .readdir(PathId(ino))
            .await
            .map_err(FuseError::Cache)?;
        entries.sort_by(|a, b| a.1.cmp(&b.1));

        let pivot = PathId(offset as u64); // offset is actually a u64 in fuse
        let start = match entries.binary_search_by(|(_, pathid, _)| pathid.cmp(&pivot)) {
            Ok(i) => i + 1,
            Err(i) => i,
        };
        for (name, pathid, assignment) in entries.into_iter().skip(start) {
            if reply.add(
                pathid.as_u64(),
                pathid.as_u64() as i64,
                match assignment {
                    PathAssignment::Directory => fuser::FileType::Directory,
                    PathAssignment::File => fuser::FileType::RegularFile,
                },
                name,
            ) {
                // buffer full
                break;
            }
        }

        Ok(())
    }

    async fn unlink(&self, parent: u64, name: OsString) -> Result<(), FuseError> {
        let name = name.to_str().ok_or(FuseError::Utf8)?;
        self.cache.unlink(PathId(parent), name).await?;

        Ok(())
    }

    async fn link(
        &self,
        source: u64,
        parent: u64,
        name: OsString,
    ) -> Result<fuser::FileAttr, FuseError> {
        let name = name.to_str().ok_or(FuseError::Utf8)?;
        let (dest, metadata) = self
            .cache
            .branch(PathId(source), PathId(parent), name)
            .await?;

        Ok(self.build_file_attr(dest, &metadata))
    }

    async fn mkdir(
        &self,
        parent: u64,
        name: OsString,
        _mode: u32,
        _umask: u32,
    ) -> Result<fuser::FileAttr, FuseError> {
        let name = name.to_str().ok_or(FuseError::Utf8)?;

        let (dest, metadata) = self.cache.mkdir(PathId(parent), name).await?;

        Ok(self.build_dir_attr(dest, metadata))
    }

    async fn rmdir(&self, parent: u64, name: OsString) -> Result<(), FuseError> {
        let name = name.to_str().ok_or(FuseError::Utf8)?;

        self.cache.rmdir(PathId(parent), name).await?;

        Ok(())
    }

    fn build_file_attr(&self, pathid: PathId, metadata: &FileMetadata) -> fuser::FileAttr {
        let uid = nix::unistd::getuid().as_raw();
        let gid = nix::unistd::getgid().as_raw();
        let mtime = metadata
            .mtime
            .as_system_time()
            .unwrap_or(SystemTime::UNIX_EPOCH);

        fuser::FileAttr {
            ino: pathid.as_u64(),
            size: metadata.size,
            blocks: (metadata.size + 511) / 512, // Round up to block size
            atime: mtime,
            mtime: mtime,
            ctime: mtime,
            crtime: SystemTime::UNIX_EPOCH,
            kind: fuser::FileType::RegularFile,
            perm: 0o0666 & !self.umask,
            nlink: 1,
            uid,
            gid,
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
    }

    fn build_dir_attr(&self, pathid: PathId, metadata: DirMetadata) -> fuser::FileAttr {
        let uid = nix::unistd::getuid().as_raw();
        let gid = nix::unistd::getgid().as_raw();
        let mtime = metadata
            .mtime
            .as_system_time()
            .unwrap_or(SystemTime::UNIX_EPOCH);

        fuser::FileAttr {
            ino: pathid.as_u64(),
            size: 512,
            blocks: 1,
            atime: mtime,
            mtime: mtime,
            ctime: mtime,
            crtime: SystemTime::UNIX_EPOCH,
            kind: fuser::FileType::Directory,
            perm: if metadata.read_only { 0o0555 } else { 0o0777 } & !self.umask,
            nlink: 1,
            uid,
            gid,
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
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
    /// Return a libc error code to represent this error, fuse-side.
    fn errno(&self) -> c_int {
        match &self {
            FuseError::Cache(err) => io_errno(err.io_kind()),
            FuseError::Utf8 => nix::libc::EINVAL,
            FuseError::Io(ioerr) => io_errno(ioerr.kind()),
        }
    }

    /// Convert into a libc error code.
    fn log_and_convert(self) -> c_int {
        let errno = self.errno();

        log::debug!("FUSE operation error: {self:?} -> {errno}");

        errno
    }
}

/// Convert a Rust [std::io::ErrorKind] into a libc error code.
fn io_errno(kind: std::io::ErrorKind) -> c_int {
    match kind {
        std::io::ErrorKind::NotFound => nix::libc::ENOENT,
        std::io::ErrorKind::PermissionDenied => nix::libc::EACCES,
        std::io::ErrorKind::ConnectionRefused => nix::libc::ECONNREFUSED,
        std::io::ErrorKind::ConnectionReset => nix::libc::ECONNRESET,
        std::io::ErrorKind::HostUnreachable => nix::libc::EHOSTUNREACH,
        std::io::ErrorKind::NetworkUnreachable => nix::libc::ENETUNREACH,
        std::io::ErrorKind::ConnectionAborted => nix::libc::ECONNABORTED,
        std::io::ErrorKind::NotConnected => nix::libc::ENOTCONN,
        std::io::ErrorKind::AddrInUse => nix::libc::EADDRINUSE,
        std::io::ErrorKind::AddrNotAvailable => nix::libc::EADDRNOTAVAIL,
        std::io::ErrorKind::NetworkDown => nix::libc::ENETDOWN,
        std::io::ErrorKind::BrokenPipe => nix::libc::EPIPE,
        std::io::ErrorKind::AlreadyExists => nix::libc::EEXIST,
        std::io::ErrorKind::WouldBlock => nix::libc::EAGAIN,
        std::io::ErrorKind::NotADirectory => nix::libc::ENOTDIR,
        std::io::ErrorKind::IsADirectory => nix::libc::EISDIR,
        std::io::ErrorKind::DirectoryNotEmpty => nix::libc::ENOTEMPTY,
        std::io::ErrorKind::ReadOnlyFilesystem => nix::libc::EROFS,
        std::io::ErrorKind::StaleNetworkFileHandle => nix::libc::ESTALE,
        std::io::ErrorKind::InvalidInput => nix::libc::EINVAL,
        std::io::ErrorKind::InvalidData => nix::libc::EINVAL,
        std::io::ErrorKind::TimedOut => nix::libc::ETIMEDOUT,
        std::io::ErrorKind::WriteZero => nix::libc::EIO,
        std::io::ErrorKind::StorageFull => nix::libc::ENOSPC,
        std::io::ErrorKind::NotSeekable => nix::libc::ESPIPE,
        std::io::ErrorKind::QuotaExceeded => nix::libc::EDQUOT,
        std::io::ErrorKind::FileTooLarge => nix::libc::EFBIG,
        std::io::ErrorKind::ResourceBusy => nix::libc::EBUSY,
        std::io::ErrorKind::ExecutableFileBusy => nix::libc::ETXTBSY,
        std::io::ErrorKind::Deadlock => nix::libc::EDEADLK,
        std::io::ErrorKind::CrossesDevices => nix::libc::EXDEV,
        std::io::ErrorKind::TooManyLinks => nix::libc::EMLINK,
        std::io::ErrorKind::InvalidFilename => nix::libc::EINVAL,
        std::io::ErrorKind::ArgumentListTooLong => nix::libc::E2BIG,
        std::io::ErrorKind::Interrupted => nix::libc::EINTR,
        std::io::ErrorKind::Unsupported => nix::libc::ENOSYS,
        std::io::ErrorKind::OutOfMemory => nix::libc::ENOMEM,
        _ => nix::libc::EIO,
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
            let handle = export(cache.clone(), downloader, self.mountpoint.path(), 0o027)?;
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

    impl Drop for FuseFixture {
        fn drop(&mut self) {
            if let Some(handle) = self.fuse_handle.take() {
                handle.join_blocking();
            }
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

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;

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
                assert_eq!(0o0750, arena_attr.permissions().mode() & 0o777);
                assert_eq!(nix::unistd::getuid().as_raw(), arena_attr.uid());
                assert_eq!(nix::unistd::getgid().as_raw(), arena_attr.gid());

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        fixture.unmount().await?;
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
                assert_eq!(0o0640, file_attr.permissions().mode() & 0o777);
                assert_eq!(nix::unistd::getuid().as_raw(), file_attr.uid());
                assert_eq!(nix::unistd::getgid().as_raw(), file_attr.gid());
                assert_eq!(5, file_attr.len());

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;

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

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn large_dir() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                fn fname(i: usize) -> String {
                    format!("dir/file{i:03}.txt")
                }
                let b_dir = fixture.inner.arena_root(b);
                fs::create_dir_all(b_dir.join("dir")).await?;
                for i in 0..250 {
                    let file = b_dir.join(fname(i));
                    fs::write(&file, "test").await?;
                }
                for i in 0..250 {
                    fixture
                        .inner
                        .wait_for_file_in_cache(a, &fname(i), &hash::digest("test"))
                        .await?;
                }

                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());

                let mut read_dir = fs::read_dir(arena_path.join("dir")).await?;
                let mut collected = vec![];
                while let Some(entry) = read_dir.next_entry().await? {
                    let name = entry.file_name();
                    collected.push(format!("dir/{}", name.to_string_lossy()));
                }
                drop(read_dir);
                collected.sort();
                assert_eq!(250, collected.len());
                for i in 0..250 {
                    assert_eq!(fname(i), collected[i]);
                }

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        fixture.unmount().await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn file_not_found() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());

                let ret = fs::metadata(arena_path.join("doesnotexist")).await;
                assert!(ret.is_err());
                assert_eq!(std::io::ErrorKind::NotFound, ret.err().unwrap().kind());

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn unlink() -> anyhow::Result<()> {
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
                fixture
                    .inner
                    .write_file(b, "todelete.txt", "delete me")
                    .await?;
                let realpath_in_b = fixture.inner.arena_root(b).join("todelete.txt");
                assert!(fs::metadata(&realpath_in_b).await.is_ok());

                fixture
                    .inner
                    .wait_for_file_in_cache(a, "todelete.txt", &hash::digest("delete me"))
                    .await?;

                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                let file_path = arena_path.join("todelete.txt");

                assert!(fs::metadata(&file_path).await.is_ok());

                fs::remove_file(&file_path).await?;

                // The file is deleted on a.
                let ret = fs::metadata(&file_path).await;
                assert!(ret.is_err());
                assert_eq!(std::io::ErrorKind::NotFound, ret.err().unwrap().kind());

                // Deletion is eventually propagated back to b.
                let limit = Instant::now() + Duration::from_secs(3);
                while fs::metadata(&realpath_in_b).await.is_ok() && Instant::now() < limit {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                assert!(fs::metadata(&realpath_in_b).await.is_err());

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn link() -> anyhow::Result<()> {
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
                fixture
                    .inner
                    .write_file(b, "source.txt", "source content")
                    .await?;
                let realpath_in_b = fixture.inner.arena_root(b).join("source.txt");
                assert!(fs::metadata(&realpath_in_b).await.is_ok());

                fixture
                    .inner
                    .wait_for_file_in_cache(a, "source.txt", &hash::digest("source content"))
                    .await?;

                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                let source_path = arena_path.join("source.txt");
                let link_path = arena_path.join("link.txt");

                assert!(fs::metadata(&source_path).await.is_ok());

                fs::hard_link(&source_path, &link_path).await?;

                // The files should have the same content
                assert_eq!("source content", fs::read_to_string(&source_path).await?);
                assert_eq!("source content", fs::read_to_string(&link_path).await?);

                // The link should be propagated back to peer B
                let limit = Instant::now() + Duration::from_secs(3);
                let link_in_b = fixture.inner.arena_root(b).join("link.txt");
                while !link_in_b.exists() && Instant::now() < limit {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                assert_eq!("source content", fs::read_to_string(link_in_b).await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn mkdir() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                // Create a directory with a file in peer B's arena
                let b_dir = fixture.inner.arena_root(b);
                let new_dir = b_dir.join("new_directory");
                fs::create_dir_all(&new_dir).await?;
                fs::write(new_dir.join("test.txt"), "test content").await?;

                // Wait for the file to appear in peer A's cache
                fixture
                    .inner
                    .wait_for_file_in_cache(
                        a,
                        "new_directory/test.txt",
                        &hash::digest("test content"),
                    )
                    .await?;

                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                let new_dir_path = arena_path.join("new_directory");

                // Check that the directory exists and has correct attributes
                let dir_attr = fs::metadata(&new_dir_path).await?;
                assert!(dir_attr.is_dir());
                assert_eq!(0o0750, dir_attr.permissions().mode() & 0o777);
                assert_eq!(nix::unistd::getuid().as_raw(), dir_attr.uid());
                assert_eq!(nix::unistd::getgid().as_raw(), dir_attr.gid());

                // Check that the file within the directory is accessible
                let file_path = new_dir_path.join("test.txt");
                let file_content = fs::read_to_string(&file_path).await?;
                assert_eq!("test content", file_content);

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn rmdir() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                // Create a directory with a file in peer B's arena
                let b_dir = fixture.inner.arena_root(b);
                let dir_to_remove = b_dir.join("dir_to_remove");
                fs::create_dir_all(&dir_to_remove).await?;
                fs::write(dir_to_remove.join("test.txt"), "test content").await?;

                // Wait for the file to appear in peer A's cache
                fixture
                    .inner
                    .wait_for_file_in_cache(
                        a,
                        "dir_to_remove/test.txt",
                        &hash::digest("test content"),
                    )
                    .await?;

                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                let dir_to_remove_path = arena_path.join("dir_to_remove");

                // Verify the directory and file exist
                assert!(fs::metadata(&dir_to_remove_path).await.is_ok());
                let file_path = dir_to_remove_path.join("test.txt");
                assert!(fs::metadata(&file_path).await.is_ok());

                // Remove the file first (directories must be empty to be removed)
                fs::remove_file(&file_path).await?;

                // Now remove the directory
                fs::remove_dir(&dir_to_remove_path).await?;

                // The directory should no longer exist
                let ret = fs::metadata(&dir_to_remove_path).await;
                assert!(ret.is_err());
                assert_eq!(std::io::ErrorKind::NotFound, ret.err().unwrap().kind());

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;

        Ok(())
    }
}
