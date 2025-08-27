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

async fn do_read(downloader: Downloader, ino: u64, offset: i64, size: u32) -> Result<Vec<u8>, FuseError> {
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

async fn do_readdir(cache: Arc<GlobalCache>, ino: u64, offset: i64) -> Result<Vec<(u64, i64, fuser::FileType, std::ffi::OsString)>, FuseError> {
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
