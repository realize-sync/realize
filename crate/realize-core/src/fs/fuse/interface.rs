//! FUSE interface layer - protocol handling and mount management
//!
//! This module contains the external interface for mounting FUSE filesystems,
//! the FUSE protocol implementation, and mount handle management.

use crate::fs::downloader::Downloader;
use fuser::MountOption;
use nix::libc;
use realize_storage::{Filesystem, Inode};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::runtime::Handle;

use super::operations::InnerRealizeFs;

const TTL: Duration = Duration::ZERO;

/// Mount the cache as FUSE filesystem at the given mountpoint.
pub fn export(
    fs: Arc<Filesystem>,
    downloader: Downloader,
    mountpoint: &std::path::Path,
    umask: u16,
) -> anyhow::Result<FuseHandle> {
    let fs = RealizeFs {
        handle: Handle::current(),
        inner: Arc::new(InnerRealizeFs::new(fs, downloader, umask)),
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
    pub(crate) fn join_blocking(self) {
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
impl fuser::Filesystem for RealizeFs {
    fn init(
        &mut self,
        _req: &fuser::Request<'_>,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
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
                Ok(attr) => reply.entry(&TTL, &attr, 0),
            }
        });
    }

    fn getattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: Option<u64>,
        reply: fuser::ReplyAttr,
    ) {
        let inner = Arc::clone(&self.inner);

        self.handle.spawn(async move {
            match inner.getattr(Inode(ino), fh).await {
                Err(err) => reply.error(err.log_and_convert()),
                Ok(attr) => reply.attr(&TTL, &attr),
            }
        });
    }

    /// Set file attributes.
    fn setattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: fuser::ReplyAttr,
    ) {
        // Note: ctime, crtime, chgtime, bkuptime, flags are
        // not supported/ignored for now as they are macOS-specific

        let inner = Arc::clone(&self.inner);
        self.handle.spawn(async move {
            match inner
                .setattr(Inode(ino), mode, uid, gid, size, atime, mtime, fh)
                .await
            {
                Err(err) => reply.error(err.log_and_convert()),
                Ok(attr) => reply.attr(&TTL, &attr),
            }
        });
    }

    fn readlink(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyData) {
        log::debug!("[Not Implemented] readlink(ino: {:#x?})", ino);
        reply.error(libc::ENOSYS);
    }

    fn open(&mut self, _req: &fuser::Request<'_>, ino: u64, flags: i32, reply: fuser::ReplyOpen) {
        let inner = Arc::clone(&self.inner);

        self.handle.spawn(async move {
            match inner.open(Inode(ino), flags).await {
                Err(err) => reply.error(err.log_and_convert()),
                Ok((fh, flags)) => reply.opened(fh, flags),
            }
        });
    }

    fn release(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        let inner = Arc::clone(&self.inner);

        self.handle.spawn(async move {
            match inner.release(fh).await {
                Err(err) => reply.error(err.log_and_convert()),
                Ok(()) => reply.ok(),
            }
        });
    }

    fn read(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        let inner = Arc::clone(&self.inner);

        self.handle.spawn(async move {
            match inner.read(fh, Inode(ino), offset, size).await {
                Err(err) => reply.error(err.log_and_convert()),
                Ok(data) => reply.data(&data),
            }
        });
    }

    fn write(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        let inner = Arc::clone(&self.inner);
        let data = data.to_vec();
        self.handle.spawn(async move {
            match inner.write(fh, Inode(ino), offset, &data).await {
                Err(err) => reply.error(err.log_and_convert()),
                Ok(nbytes) => reply.written(nbytes),
            }
        });
    }

    fn flush(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        _lock_owner: u64,
        reply: fuser::ReplyEmpty,
    ) {
        let inner = Arc::clone(&self.inner);

        self.handle.spawn(async move {
            match inner.flush(fh, Inode(ino)).await {
                Err(err) => reply.error(err.log_and_convert()),
                Ok(()) => reply.ok(),
            }
        });
    }

    fn opendir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _flags: i32,
        reply: fuser::ReplyOpen,
    ) {
        let inner = Arc::clone(&self.inner);

        self.handle.spawn(async move {
            match inner.opendir(Inode(ino)).await {
                Err(err) => reply.error(err.log_and_convert()),
                Ok(fh) => reply.opened(fh, 0),
            }
        });
    }

    fn readdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        let inner = Arc::clone(&self.inner);

        self.handle.spawn(async move {
            match inner.readdir(fh, Inode(ino), offset, &mut reply).await {
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
        reply.error(libc::ENOSYS);
    }

    fn releasedir(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        reply: fuser::ReplyEmpty,
    ) {
        let inner = Arc::clone(&self.inner);

        self.handle.spawn(async move {
            match inner.releasedir(fh).await {
                Err(err) => reply.error(err.log_and_convert()),
                Ok(()) => reply.ok(),
            }
        });
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
        let inner = Arc::clone(&self.inner);
        let name = name.to_owned();

        self.handle.spawn(async move {
            match inner.getxattr(Inode(ino), name).await {
                Err(err) => reply.error(err.log_and_convert()),
                Ok(val) => {
                    let val = val.as_bytes();
                    if size == 0 {
                        reply.size(val.len() as u32);
                    } else if size >= (val.len() as u32) {
                        reply.data(val);
                    } else {
                        reply.error(libc::ERANGE);
                    }
                }
            }
        });
    }

    /// Set an extended attribute.
    fn setxattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        name: &std::ffi::OsStr,
        value: &[u8],
        _flags: i32,
        _position: u32,
        reply: fuser::ReplyEmpty,
    ) {
        let inner = Arc::clone(&self.inner);
        let name = name.to_owned();
        let value = value.to_vec();

        self.handle.spawn(async move {
            match inner.setxattr(Inode(ino), name, value).await {
                Err(err) => reply.error(err.log_and_convert()),
                Ok(()) => reply.ok(),
            }
        });
    }

    fn removexattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let inner = Arc::clone(&self.inner);
        let name = name.to_owned();

        self.handle.spawn(async move {
            match inner.setxattr(Inode(ino), name, vec![]).await {
                Err(err) => reply.error(err.log_and_convert()),
                Ok(()) => reply.ok(),
            }
        });
    }

    fn listxattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        size: u32,
        reply: fuser::ReplyXattr,
    ) {
        let inner = Arc::clone(&self.inner);
        self.handle.spawn(async move {
            match inner.listxattr(Inode(ino)).await {
                Err(err) => reply.error(err.log_and_convert()),
                Ok(names) => {
                    let mut bytes = names.join("\0").into_bytes();
                    if !bytes.is_empty() {
                        bytes.push(0);
                    }
                    if size == 0 {
                        log::debug!("listxattr returns size");
                        reply.size(bytes.len() as u32);
                    } else if size >= (bytes.len() as u32) {
                        log::debug!("listxattr returns data: {bytes:?}");
                        reply.data(bytes.as_slice());
                    } else {
                        reply.error(libc::ERANGE);
                    }
                }
            }
        });
    }

    fn access(&mut self, _req: &fuser::Request<'_>, ino: u64, mask: i32, reply: fuser::ReplyEmpty) {
        log::debug!("[Not Implemented] access(ino: {:#x?}, mask: {})", ino, mask);
        reply.error(libc::ENOSYS);
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
        reply.error(libc::ENOSYS);
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
                Ok(attr) => reply.entry(&TTL, &attr, 0),
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
                Ok(attr) => reply.entry(&TTL, &attr, 0),
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

    /// Rename a file or directory.
    ///
    /// The `noreplace` semantics (RENAME_NOREPLACE flag) are only supported on Linux.
    /// On other platforms, the noreplace flag is always treated as false.
    fn rename(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        newparent: u64,
        newname: &std::ffi::OsStr,
        flags: u32,
        reply: fuser::ReplyEmpty,
    ) {
        #[cfg(target_os = "linux")]
        const RENAME_NOREPLACE_FLAG: u32 = libc::RENAME_NOREPLACE;
        #[cfg(not(target_os = "linux"))]
        const RENAME_NOREPLACE_FLAG: u32 = 0;

        if (flags & (!RENAME_NOREPLACE_FLAG)) != 0 {
            // only NOREPLACE is supported
            reply.error(libc::EINVAL);
            return;
        }

        // On non-Linux platforms, noreplace is always false
        let noreplace = cfg!(target_os = "linux") && (flags & RENAME_NOREPLACE_FLAG) != 0;

        let inner = Arc::clone(&self.inner);
        let name = name.to_owned();
        let newname = newname.to_owned();

        self.handle.spawn(async move {
            match inner
                .rename(parent, name, newparent, newname, noreplace)
                .await
            {
                Err(err) => reply.error(err.log_and_convert()),
                Ok(()) => reply.ok(),
            }
        });
    }

    /// Create and open a file.
    /// If the file does not exist, first create it with the specified mode, and then
    /// open it. Open flags (with the exception of O_NOCTTY) are available in flags.
    /// Filesystem may store an arbitrary file handle (pointer, index, etc) in fh,
    /// and use this in other all other file operations (read, write, flush, release,
    /// fsync). There are also some flags (direct_io, keep_cache) which the
    /// filesystem may set, to change the way the file is opened. See fuse_file_info
    /// structure in <fuse_common.h> for more details. If this method is not
    /// implemented or under Linux kernel versions earlier than 2.6.15, the mknod()
    /// and open() methods will be called instead.
    fn create(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let inner = Arc::clone(&self.inner);
        let name = name.to_owned();

        self.handle.spawn(async move {
            match inner.create(parent, name, mode, umask, flags).await {
                Err(err) => reply.error(err.log_and_convert()),
                Ok((attr, fh)) => reply.created(&TTL, &attr, 0, fh, 0),
            }
        });
    }
}
