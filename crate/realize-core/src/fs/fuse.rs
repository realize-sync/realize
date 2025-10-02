#![allow(dead_code)] // in progress
use crate::fs::downloader::{Download, Downloader};
use crate::rpc::HouseholdOperationError;
use fuser::{FileType, MountOption};
use multimap::MultiMap;
use nix::libc::{self, c_int};
use nix::sys::{stat, time::TimeSpec};
use nix::unistd::{Gid, Uid};
use realize_storage::{
    CacheStatus, DirMetadata, FileContent, FileMetadata, FileRealm, Filesystem, Inode, Mark,
    Metadata, StorageError, Version,
};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io::SeekFrom;
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::runtime::Handle;
use tokio::sync::Mutex;
use tokio_util::bytes::BufMut;

const TTL: Duration = Duration::ZERO;
const XATTR_MARK: &str = "realize.mark";
const XATTR_STATUS: &str = "realize.status";
const XATTR_VERSION: &str = "realize.version";

/// Mount the cache as FUSE filesystem at the given mountpoint.
pub fn export(
    fs: Arc<Filesystem>,
    downloader: Downloader,
    mountpoint: &std::path::Path,
    umask: u16,
) -> anyhow::Result<FuseHandle> {
    let fs = RealizeFs {
        handle: Handle::current(),
        inner: Arc::new(InnerRealizeFs {
            fs,
            downloader,
            umask,
            handles: FHRegistry::new(),
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
            match inner.getattr(ino, fh).await {
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
                .setattr(ino, mode, uid, gid, size, atime, mtime, fh)
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
            match inner.open(ino, flags).await {
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
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        let inner = Arc::clone(&self.inner);

        self.handle.spawn(async move {
            match inner.read(fh, offset, size).await {
                Err(err) => reply.error(err.log_and_convert()),
                Ok(data) => reply.data(&data),
            }
        });
    }

    fn write(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
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
            match inner.write(fh, offset, &data).await {
                Err(err) => reply.error(err.log_and_convert()),
                Ok(nbytes) => reply.written(nbytes),
            }
        });
    }

    fn flush(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        _lock_owner: u64,
        reply: fuser::ReplyEmpty,
    ) {
        let inner = Arc::clone(&self.inner);

        self.handle.spawn(async move {
            match inner.flush(fh).await {
                Err(err) => reply.error(err.log_and_convert()),
                Ok(()) => reply.ok(),
            }
        });
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
        reply.error(libc::ENOSYS);
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
        let inner = Arc::clone(&self.inner);
        let name = name.to_owned();

        self.handle.spawn(async move {
            match inner.getxattr(ino, name).await {
                Err(err) => reply.error(err.log_and_convert()),
                Ok(val) => {
                    let val = val.as_bytes();
                    if size == 0 {
                        reply.size(val.len() as u32);
                    } else if size <= (val.len() as u32) {
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
            match inner.setxattr(ino, name, value).await {
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
            match inner.setxattr(ino, name, vec![]).await {
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
            match inner.listxattr(ino).await {
                Err(err) => reply.error(err.log_and_convert()),
                Ok(names) => {
                    let mut bytes = names.join("\0").into_bytes();
                    if !bytes.is_empty() {
                        bytes.push(0);
                    }
                    if size == 0 {
                        log::debug!("listxattr returns size");
                        reply.size(bytes.len() as u32);
                    } else if size <= (bytes.len() as u32) {
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

struct InnerRealizeFs {
    fs: Arc<Filesystem>,
    downloader: Downloader,
    umask: u16,
    handles: FHRegistry,
}

enum FileHandle {
    Cached(Download),
    Real(tokio::fs::File, FHMode),
}

/// Keeps track of open file handles.
struct FHRegistry {
    state: Arc<Mutex<FHRegistryState>>,
}
struct FHRegistryState {
    by_fh: BTreeMap<u64, (Inode, Arc<Mutex<FileHandle>>)>,
    by_inode: MultiMap<Inode, u64>,
}

impl FHRegistry {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(FHRegistryState {
                by_fh: BTreeMap::new(),
                by_inode: MultiMap::new(),
            })),
        }
    }
    async fn add(&self, ino: Inode, handle: FileHandle) -> u64 {
        let mut this = self.state.lock().await;
        let fh = this
            .by_fh
            .last_key_value()
            .map(|(k, _)| *k + 1)
            .unwrap_or(1);
        this.by_fh.insert(fh, (ino, Arc::new(Mutex::new(handle))));
        this.by_inode.insert(ino, fh);

        fh
    }

    /// Gets a specific file handle.
    async fn get(&self, fh: u64) -> Option<Arc<Mutex<FileHandle>>> {
        self.state
            .lock()
            .await
            .by_fh
            .get(&fh)
            .map(|(_, h)| Arc::clone(h))
    }

    async fn get_or_err(&self, fh: u64) -> Result<Arc<Mutex<FileHandle>>, FuseError> {
        self.get(fh).await.ok_or(FuseError::Errno(libc::EBADF))
    }

    /// Gets all file handles for the given inode.
    async fn iter_by_inode(&self, ino: Inode) -> Vec<Arc<Mutex<FileHandle>>> {
        let this = self.state.lock().await;

        this.by_inode
            .get_vec(&ino)
            .cloned()
            .unwrap_or_else(|| vec![])
            .into_iter()
            .flat_map(|fh| this.by_fh.get(&fh).map(|(_, h)| Arc::clone(h)))
            .collect()
    }

    /// Removes a file handle from the registry.
    async fn remove(&self, fh: u64) -> Option<Arc<Mutex<FileHandle>>> {
        let mut this = self.state.lock().await;
        if let Some((ino, handle)) = this.by_fh.remove(&fh) {
            if let Some(vec) = this.by_inode.get_vec_mut(&ino) {
                vec.retain(|e| *e != fh);
            }

            return Some(handle);
        }

        None
    }
}

// File mode, stored in the FileHandle.
//
// Sometimes, the underlying handle would allow operations that
// weren't asked in FUSE open(). [FHMode] helps make sure that these
// aren't allowed.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum FHMode {
    Invalid,
    ReadOnly,
    ReadWrite,
    WriteOnly,
}

impl FHMode {
    fn from_flags(flags: i32) -> Self {
        let mode = flags & (libc::O_RDONLY | libc::O_WRONLY | libc::O_RDWR);

        let res = match mode {
            libc::O_RDONLY => FHMode::ReadOnly,
            libc::O_WRONLY => FHMode::WriteOnly,
            libc::O_RDWR => FHMode::ReadWrite,
            _ => {
                log::debug!("Invalid open flags: mode={mode:o} (all flags= 0x{flags:x})");

                FHMode::Invalid
            }
        };

        res
    }

    fn allow_read(&self) -> bool {
        match self {
            FHMode::ReadOnly => true,
            FHMode::ReadWrite => true,
            FHMode::WriteOnly => false,
            FHMode::Invalid => false,
        }
    }
    fn allow_write(&self) -> bool {
        match self {
            FHMode::ReadOnly => false,
            FHMode::ReadWrite => true,
            FHMode::WriteOnly => true,
            FHMode::Invalid => false,
        }
    }

    fn check_allow_read(&self) -> Result<(), FuseError> {
        if !self.allow_read() {
            return Err(FuseError::Errno(libc::EPERM));
        }

        Ok(())
    }

    fn check_allow_write(&self) -> Result<(), FuseError> {
        if !self.allow_write() {
            return Err(FuseError::Errno(libc::EPERM));
        }

        Ok(())
    }
}

impl InnerRealizeFs {
    async fn lookup(&self, parent: u64, name: OsString) -> Result<fuser::FileAttr, FuseError> {
        let name = name.to_str().ok_or(FuseError::Utf8)?;
        let (inode, metadata) = self.fs.lookup((Inode(parent), name)).await?;
        match metadata {
            Metadata::File(file_metadata) => Ok(self.build_file_attr(inode, &file_metadata)),
            Metadata::Dir(dir_metadata) => Ok(self.build_dir_attr(inode, dir_metadata)),
        }
    }

    async fn getattr(&self, ino: u64, fh: Option<u64>) -> Result<fuser::FileAttr, FuseError> {
        if let Some(fh) = fh {
            let handle = self.handles.get_or_err(fh).await?;
            if let FileHandle::Real(file, _) = &*handle.lock().await {
                return Ok(metadata_to_attr(&file.metadata().await?, ino));
            }
        }
        let metadata = self.fs.metadata(Inode(ino)).await?;
        match metadata {
            Metadata::File(file_metadata) => Ok(self.build_file_attr(Inode(ino), &file_metadata)),
            Metadata::Dir(dir_metadata) => Ok(self.build_dir_attr(Inode(ino), dir_metadata)),
        }
    }

    async fn listxattr(&self, ino: u64) -> Result<Vec<&'static str>, FuseError> {
        if let Metadata::File(_) = self.fs.metadata(Inode(ino)).await? {
            return Ok(vec![XATTR_MARK, XATTR_STATUS, XATTR_VERSION]);
        }

        Ok(vec![XATTR_MARK])
    }

    async fn getxattr(&self, ino: u64, name: OsString) -> Result<String, FuseError> {
        if name == XATTR_MARK {
            let (mark, direct) = self.fs.get_mark(Inode(ino)).await?;
            if direct {
                return Ok(mark.to_string());
            }
            return Ok(format!("{} (derived)", mark));
        }

        if name == XATTR_STATUS {
            return Ok(match self.fs.file_realm(Inode(ino)).await? {
                FileRealm::Local(_) => "local 100%".to_string(),
                FileRealm::Remote(CacheStatus::Missing) => "remote 0%".to_string(),
                FileRealm::Remote(CacheStatus::Complete) => "remote 100%".to_string(),
                FileRealm::Remote(CacheStatus::Verified) => "remote 100% verified".to_string(),
                FileRealm::Remote(CacheStatus::Partial(size, available_ranges)) => {
                    format!(
                        "remote {:0.0}%",
                        (available_ranges.bytecount() as f64) / (size as f64) * 100.0
                    )
                }
            });
        }

        if name == XATTR_VERSION {
            let m = self.fs.file_metadata(Inode(ino)).await?;
            return Ok(match m.version {
                Version::Modified(_) => "modified".to_string(),
                Version::Indexed(hash) => hash.to_string(),
            });
        }

        Err(FuseError::Errno(libc::ENODATA))
    }

    async fn setxattr(&self, ino: u64, name: OsString, value: Vec<u8>) -> Result<(), FuseError> {
        if name == XATTR_MARK {
            // Parse the mark value from the byte slice
            let value_str = std::str::from_utf8(&value)
                .map_err(|_| FuseError::Errno(libc::EINVAL))?
                .trim();

            if value_str.is_empty() {
                // Clear the mark
                self.fs.clear_mark(Inode(ino)).await?;
            } else {
                // Parse and set the mark
                let mark = Mark::parse(value_str).ok_or(FuseError::Errno(libc::EINVAL))?;
                self.fs.set_mark(Inode(ino), mark).await?;
            }
            return Ok(());
        }

        // Other attributes are not supported for setting
        Err(FuseError::Errno(libc::ENOTSUP))
    }

    async fn read(&self, fh: u64, offset: i64, size: u32) -> Result<Vec<u8>, FuseError> {
        let handle = self.handles.get_or_err(fh).await?;
        let size = size as usize;

        // TODO: clarify type situation for offset. offset is i64 in
        // fuser, but u64 in libfuse and Linux. What's happening?
        let offset = offset as u64;
        let mut buffer = Vec::with_capacity(size).limit(size);
        match &mut *handle.lock().await {
            FileHandle::Cached(reader) => {
                reader.read_all_at(offset, &mut buffer).await?;
            }
            FileHandle::Real(file, mode) => {
                mode.check_allow_read()?;
                file.seek(SeekFrom::Start(offset)).await?;
                while buffer.has_remaining_mut() {
                    if file.read_buf(&mut buffer).await? == 0 {
                        break;
                    }
                }
            }
        }
        // Note: As requested by FUSE: the above reads as much as
        // possible up to size (no short reads). Only stop if EOF is
        // reached.
        Ok(buffer.into_inner())
    }

    async fn write(&self, fh: u64, offset: i64, data: &[u8]) -> Result<u32, FuseError> {
        let handle = self.handles.get_or_err(fh).await?;
        let offset = offset as u64; // TODO: clarify type situation for offset.
        match &mut *handle.lock().await {
            FileHandle::Cached(_) => Err(FuseError::Errno(libc::EBADF)),
            FileHandle::Real(file, mode) => {
                mode.check_allow_write()?;
                file.seek(SeekFrom::Start(offset)).await?;
                file.write_all(data).await?;

                Ok(data.len() as u32)
            }
        }
    }

    async fn setattr(
        &self,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        fh: Option<u64>,
    ) -> Result<fuser::FileAttr, FuseError> {
        if let Some(size) = size {
            // truncate is called first because it'll move the file
            // from the remote to the local realm, if needed, which
            // allows other attributes to be set.
            self.truncate(ino, fh, size).await?;
        }
        match self.fs.file_realm(Inode(ino)).await? {
            FileRealm::Remote(_) => {
                if uid.is_some_and(|uid| uid != nix::unistd::getuid().as_raw())
                    || gid.is_some_and(|gid| gid != nix::unistd::getgid().as_raw())
                {
                    log::debug!(
                        "SETATTR Inode({ino})@remote: cannot change uid or gid of remote file"
                    );
                    return Err(FuseError::Errno(libc::EPERM));
                }

                // Ignore the rest
                log::debug!("SETATTR Inode({ino})@remote: ignored");
            }
            FileRealm::Local(path) => {
                log::debug!("SETATTR Inode({ino})@remote: change {path:?}");
                if mode.is_some()
                    || uid.is_some()
                    || gid.is_some()
                    || atime.is_some()
                    || mtime.is_some()
                {
                    tokio::task::spawn_blocking(move || {
                        // TODO: reuse FH if available
                        let fd = nix::fcntl::open(
                            &path,
                            nix::fcntl::OFlag::O_WRONLY,
                            stat::Mode::empty(),
                        )?;
                        if let Some(mode) = mode {
                            let mode = stat::Mode::from_bits_truncate(mode as libc::mode_t);
                            log::debug!("SETATTR Inode({ino})@remote: set mode=0o{:o} {mode:?}", mode.bits());
                            nix::sys::stat::fchmod(&fd, mode)?;
                        }
                        if uid.is_some() || gid.is_some() {
                            let uid_val = uid.map(Uid::from_raw);
                            let gid_val = gid.map(Gid::from_raw);
                            log::debug!(
                                "SETATTR Inode({ino})@remote: set ownership {uid_val:?}:{gid_val:?}"
                            );
                            nix::unistd::fchown(&fd, uid_val, gid_val)?;
                        }
                        if atime.is_some() || mtime.is_some() {
                            let atime_spec = atime
                                .map(Self::time_or_now_to_timespec)
                                .unwrap_or_else(Self::timespec_unchanged);
                            let mtime_spec = mtime
                                .map(Self::time_or_now_to_timespec)
                                .unwrap_or_else(Self::timespec_unchanged);

                            log::debug!(
                                "SETATTR Inode({ino})@remote: set atime={atime_spec:?} mtime={mtime_spec:?}"
                            );

                            stat::futimens(&fd, &atime_spec, &mtime_spec)?;
                        }

                        Ok::<(), FuseError>(())
                    })
                    .await??;
                }
            }
        }

        self.getattr(ino, fh).await
    }

    async fn truncate(&self, ino: u64, fh: Option<u64>, size: u64) -> Result<(), FuseError> {
        if let Some(fh) = fh {
            let handle = self.handles.get_or_err(fh).await?;
            if let FileHandle::Real(file, mode) = &mut *handle.lock().await {
                mode.check_allow_write()?;
                file.set_len(size).await?;
                log::debug!("SETATTR {ino}: Truncate file FH#{fh} to {size}");

                return Ok(());
            }
        }
        let ino = Inode(ino);
        match self.fs.file_content(ino).await? {
            FileContent::Local(realpath) => {
                log::debug!("SETATTR {ino}: Truncate file, mapped to {realpath:?} to {size}");
                let mut file = tokio::fs::OpenOptions::new()
                    .write(true)
                    .open(&realpath)
                    .await?;
                file.set_len(size).await?;
                file.flush().await?;
                return Ok(());
            }
            FileContent::Remote(mut blob) => {
                self.downloader.complete_blob(&mut blob).await?;
                let (_, mut file) = blob.realize().await?;
                file.set_len(size).await?;
                file.flush().await?;
                return Ok(());
            }
        }
    }

    async fn readdir(
        &self,
        ino: u64,
        offset: i64,
        reply: &mut fuser::ReplyDirectory,
    ) -> Result<(), FuseError> {
        let mut entries = self
            .fs
            .readdir(Inode(ino))
            .await
            .map_err(FuseError::Cache)?;
        entries.sort_by(|a, b| a.1.cmp(&b.1));

        let pivot = Inode(offset as u64); // offset is actually a u64 in fuse
        let start = match entries.binary_search_by(|(_, pathid, _)| pathid.cmp(&pivot)) {
            Ok(i) => i + 1,
            Err(i) => i,
        };
        for (name, pathid, metadata) in entries.into_iter().skip(start) {
            if reply.add(
                pathid.as_u64(),
                pathid.as_u64() as i64,
                match metadata {
                    Metadata::Dir(_) => fuser::FileType::Directory,
                    Metadata::File(_) => fuser::FileType::RegularFile,
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
        let (inode, _) = self.fs.lookup((Inode(parent), name)).await?;
        self.fs.unlink(inode).await?;

        Ok(())
    }

    async fn link(
        &self,
        source: u64,
        parent: u64,
        name: OsString,
    ) -> Result<fuser::FileAttr, FuseError> {
        let name = name.to_str().ok_or(FuseError::Utf8)?;

        let (dest, metadata) = self.fs.branch(Inode(source), (Inode(parent), name)).await?;

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

        let (dest, metadata) = self.fs.mkdir((Inode(parent), name)).await?;

        Ok(self.build_dir_attr(dest, metadata))
    }

    async fn rmdir(&self, parent: u64, name: OsString) -> Result<(), FuseError> {
        let name = name.to_str().ok_or(FuseError::Utf8)?;

        self.fs.rmdir((Inode(parent), name)).await?;

        Ok(())
    }

    async fn rename(
        &self,
        old_parent: u64,
        old_name: OsString,
        new_parent: u64,
        new_name: OsString,
        noreplace: bool,
    ) -> Result<(), FuseError> {
        let old_name = old_name.to_str().ok_or(FuseError::Utf8)?;
        let new_name = new_name.to_str().ok_or(FuseError::Utf8)?;
        self.fs
            .rename(
                (Inode(old_parent), old_name),
                (Inode(new_parent), new_name),
                noreplace,
            )
            .await?;

        Ok(())
    }

    async fn open(&self, ino: u64, flags: i32) -> Result<(u64, u32), FuseError> {
        let ino = Inode(ino);
        let mode = flags & (libc::O_RDONLY | libc::O_WRONLY | libc::O_RDWR);
        let handle = match self.fs.file_content(ino).await? {
            FileContent::Local(path) => FileHandle::Real(
                openoptions_from_flags(flags).open(path).await?,
                FHMode::from_flags(flags),
            ),
            FileContent::Remote(mut blob) => {
                if mode == libc::O_RDONLY {
                    let reader = self.downloader.reader(blob).await?;
                    log::debug!("Opened Inode({ino}) read-only (download enabled)");

                    FileHandle::Cached(reader)
                } else {
                    log::debug!("Opening {ino}; realizing it first...");
                    self.downloader.complete_blob(&mut blob).await?;
                    let (path, file) = blob.realize().await?;
                    log::debug!("Map Inode({ino}) to {path:?}");
                    log::debug!("File at Inode({ino}) realized successfully");
                    if (flags & libc::O_TRUNC) != 0 {
                        // TODO: don't bother downloading in this case and when
                        // setattr is called immediately after open
                        file.set_len(0).await?;
                        log::debug!("File at Inode({ino}) truncated");
                    }
                    if (flags & libc::O_APPEND) != 0 {
                        drop(file);

                        FileHandle::Real(
                            openoptions_from_flags(flags).open(path).await?,
                            FHMode::from_flags(flags),
                        )
                    } else {
                        FileHandle::Real(file, FHMode::from_flags(flags))
                    }
                }
            }
        };
        let fh = self.handles.add(ino, handle).await;
        log::debug!("Opened file {ino:?} as FH#{fh}");

        return Ok((fh, 0));
    }

    async fn create(
        &self,
        parent: u64,
        name: OsString,
        mode: u32,
        _umask: u32,
        flags: i32,
    ) -> Result<(fuser::FileAttr, u64), FuseError> {
        let name = name.to_str().ok_or(FuseError::Utf8)?;
        log::debug!("CREATE parent={parent} name={name} mode={mode:o} flags={flags:#x}");

        let options = openoptions_from_flags(flags);
        let (ino, file) = self.fs.create(options, (Inode(parent), name)).await?;
        let attr = metadata_to_attr(&file.metadata().await?, ino.as_u64());
        let fh = self
            .handles
            .add(ino, FileHandle::Real(file, FHMode::from_flags(flags)))
            .await;
        log::debug!("Created and opened Inode({ino}) in FH#{fh}",);

        Ok((attr, fh))
    }

    async fn flush(&self, fh: u64) -> Result<(), FuseError> {
        let handle = self.handles.get_or_err(fh).await?;
        match &mut *handle.lock().await {
            FileHandle::Cached(blob) => {
                blob.update_db().await?;
            }
            FileHandle::Real(file, mode) => {
                if mode.allow_write() {
                    log::debug!("Flush FH#{fh}");
                    file.flush().await?;
                }
            }
        }

        Ok(())
    }

    async fn release(&self, fh: u64) -> Result<(), FuseError> {
        if let Some(handle) = self.handles.remove(fh).await {
            match &mut *handle.lock().await {
                FileHandle::Cached(_) => {}
                FileHandle::Real(file, mode) => {
                    if mode.allow_write() {
                        log::debug!("Flush FH#{fh}");
                        file.flush().await?;
                    }
                }
            }
        }

        Ok(())
    }

    fn build_file_attr(&self, pathid: Inode, metadata: &FileMetadata) -> fuser::FileAttr {
        let uid = metadata.uid.unwrap_or(nix::unistd::getuid().as_raw());
        let gid = metadata.gid.unwrap_or(nix::unistd::getgid().as_raw());
        let mtime = metadata.mtime.as_system_time();
        let ctime = metadata.ctime.map(|t| t.as_system_time()).unwrap_or(mtime);

        fuser::FileAttr {
            ino: pathid.as_u64(),
            size: metadata.size,
            blocks: metadata.blocks,
            atime: mtime,
            mtime: mtime,
            ctime: ctime,
            crtime: mtime,
            kind: fuser::FileType::RegularFile,
            perm: (metadata.mode & 0xffff) as u16 & !self.umask,
            nlink: 1,
            uid,
            gid,
            rdev: 0,
            blksize: 512,
            flags: 0, // macOS ony
        }
    }

    fn build_dir_attr(&self, pathid: Inode, metadata: DirMetadata) -> fuser::FileAttr {
        let uid = metadata.uid.unwrap_or(nix::unistd::getuid().as_raw());
        let gid = metadata.gid.unwrap_or(nix::unistd::getgid().as_raw());
        let mtime = metadata.mtime.as_system_time();

        fuser::FileAttr {
            ino: pathid.as_u64(),
            size: 512,
            blocks: 1,
            atime: mtime,
            mtime: mtime,
            ctime: mtime,
            crtime: SystemTime::UNIX_EPOCH,
            kind: fuser::FileType::Directory,
            perm: (metadata.mode & 0xffff) as u16 & !self.umask,
            nlink: 1,
            uid,
            gid,
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
    }

    /// Convert fuser::TimeOrNow to nix::sys::time::TimeSpec
    fn time_or_now_to_timespec(time_or_now: fuser::TimeOrNow) -> TimeSpec {
        match time_or_now {
            fuser::TimeOrNow::SpecificTime(system_time) => {
                let duration = system_time
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or(Duration::ZERO);
                TimeSpec::from(duration)
            }
            fuser::TimeOrNow::Now => {
                let duration = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or(Duration::ZERO);
                TimeSpec::from(duration)
            }
        }
    }

    /// Create a timespec that means "do not change"
    fn timespec_unchanged() -> TimeSpec {
        TimeSpec::UTIME_OMIT
    }
}

fn openoptions_from_flags(flags: i32) -> tokio::fs::OpenOptions {
    let mut opt = tokio::fs::OpenOptions::new();
    let mode = flags & (libc::O_RDONLY | libc::O_WRONLY | libc::O_RDWR);
    opt.write(mode == libc::O_WRONLY || mode == libc::O_RDWR)
        .read(mode == libc::O_RDONLY || mode == libc::O_RDWR)
        .truncate((flags & libc::O_TRUNC) != 0)
        .append((flags & libc::O_APPEND) != 0);

    opt
}

/// Intermediate error type to catch and convert to libc errno to
/// report errors to fuser.
#[derive(Debug, thiserror::Error)]
enum FuseError {
    #[error(transparent)]
    Cache(#[from] StorageError),

    #[error(transparent)]
    Rpc(#[from] HouseholdOperationError),

    #[error("invalid UTF-8 string")]
    Utf8,

    #[error("I/O error")]
    Io(#[from] std::io::Error),

    #[error("errno {0}")]
    Errno(c_int),

    #[error("tokio runtime error {0}")]
    Join(#[from] tokio::task::JoinError),
}

impl FuseError {
    /// Return a libc error code to represent this error, fuse-side.
    fn errno(&self) -> c_int {
        match &self {
            FuseError::Cache(err) => io_errno(err.io_kind()),
            FuseError::Utf8 => libc::EINVAL,
            FuseError::Io(ioerr) => io_errno(ioerr.kind()),
            FuseError::Errno(errno) => *errno,
            FuseError::Rpc(err) => io_errno(err.io_kind()),
            FuseError::Join(_) => libc::EIO,
        }
    }

    /// Convert into a libc error code.
    fn log_and_convert(self) -> c_int {
        let errno = self.errno();

        log::debug!("FUSE operation error: {self:?} -> {errno}");

        errno
    }
}

impl From<nix::errno::Errno> for FuseError {
    fn from(value: nix::errno::Errno) -> Self {
        FuseError::Errno(value as c_int)
    }
}

/// Convert a Rust [std::io::ErrorKind] into a libc error code.
fn io_errno(kind: std::io::ErrorKind) -> c_int {
    match kind {
        std::io::ErrorKind::NotFound => libc::ENOENT,
        std::io::ErrorKind::PermissionDenied => libc::EACCES,
        std::io::ErrorKind::ConnectionRefused => libc::ECONNREFUSED,
        std::io::ErrorKind::ConnectionReset => libc::ECONNRESET,
        std::io::ErrorKind::HostUnreachable => libc::EHOSTUNREACH,
        std::io::ErrorKind::NetworkUnreachable => libc::ENETUNREACH,
        std::io::ErrorKind::ConnectionAborted => libc::ECONNABORTED,
        std::io::ErrorKind::NotConnected => libc::ENOTCONN,
        std::io::ErrorKind::AddrInUse => libc::EADDRINUSE,
        std::io::ErrorKind::AddrNotAvailable => libc::EADDRNOTAVAIL,
        std::io::ErrorKind::NetworkDown => libc::ENETDOWN,
        std::io::ErrorKind::BrokenPipe => libc::EPIPE,
        std::io::ErrorKind::AlreadyExists => libc::EEXIST,
        std::io::ErrorKind::WouldBlock => libc::EAGAIN,
        std::io::ErrorKind::NotADirectory => libc::ENOTDIR,
        std::io::ErrorKind::IsADirectory => libc::EISDIR,
        std::io::ErrorKind::DirectoryNotEmpty => libc::ENOTEMPTY,
        std::io::ErrorKind::ReadOnlyFilesystem => libc::EROFS,
        std::io::ErrorKind::StaleNetworkFileHandle => libc::ESTALE,
        std::io::ErrorKind::InvalidInput => libc::EINVAL,
        std::io::ErrorKind::InvalidData => libc::EINVAL,
        std::io::ErrorKind::TimedOut => libc::ETIMEDOUT,
        std::io::ErrorKind::WriteZero => libc::EIO,
        std::io::ErrorKind::StorageFull => libc::ENOSPC,
        std::io::ErrorKind::NotSeekable => libc::ESPIPE,
        std::io::ErrorKind::QuotaExceeded => libc::EDQUOT,
        std::io::ErrorKind::FileTooLarge => libc::EFBIG,
        std::io::ErrorKind::ResourceBusy => libc::EBUSY,
        std::io::ErrorKind::ExecutableFileBusy => libc::ETXTBSY,
        std::io::ErrorKind::Deadlock => libc::EDEADLK,
        std::io::ErrorKind::CrossesDevices => libc::EXDEV,
        std::io::ErrorKind::TooManyLinks => libc::EMLINK,
        std::io::ErrorKind::InvalidFilename => libc::EINVAL,
        std::io::ErrorKind::ArgumentListTooLong => libc::E2BIG,
        std::io::ErrorKind::Interrupted => libc::EINTR,
        std::io::ErrorKind::Unsupported => libc::ENOSYS,
        std::io::ErrorKind::OutOfMemory => libc::ENOMEM,
        _ => libc::EIO,
    }
}

// Build a FileAttr from a real file metadata and map it to `ino`.
fn metadata_to_attr(m: &std::fs::Metadata, ino: u64) -> fuser::FileAttr {
    fuser::FileAttr {
        ino,
        size: m.len(),
        blocks: m.blocks(),
        atime: m.accessed().unwrap_or(SystemTime::UNIX_EPOCH),
        mtime: m.modified().unwrap_or(SystemTime::UNIX_EPOCH),
        ctime: SystemTime::UNIX_EPOCH + Duration::from_secs(m.ctime() as u64),
        crtime: m.created().unwrap_or(SystemTime::UNIX_EPOCH),
        kind: match (m.mode() as libc::mode_t) & libc::S_IFMT {
            libc::S_IFIFO => FileType::NamedPipe,
            libc::S_IFCHR => FileType::CharDevice,
            libc::S_IFBLK => FileType::BlockDevice,
            libc::S_IFDIR => FileType::Directory,
            libc::S_IFREG => FileType::RegularFile,
            libc::S_IFLNK => FileType::Symlink,
            libc::S_IFSOCK => FileType::Socket,
            _ => FileType::RegularFile,
        },
        perm: (m.mode() & 0xffff) as u16,
        nlink: 1,
        uid: m.uid(),
        gid: m.gid(),
        rdev: m.rdev() as u32,
        blksize: m.blksize() as u32,
        flags: 0, // macOS only
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::testing::{self, HouseholdFixture};
    use nix::fcntl::AT_FDCWD;
    use realize_storage::Mark;
    use realize_storage::utils::hash;
    use std::io::{Read, Write};
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
            let downloader = Downloader::new(household);

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

    // Helper functions for xattr operations. Returns FuseError to
    // make it easier to test I/O error codes than anyhow::Error.
    async fn getxattr<P: AsRef<std::path::Path>>(
        path: P,
        name: &str,
    ) -> Result<Option<String>, FuseError> {
        let path = path.as_ref().to_path_buf();
        let name = name.to_string();

        tokio::task::spawn_blocking(move || match xattr::get(&path, &name)? {
            Some(data) => Ok(Some(String::from_utf8(data).unwrap())),
            None => Ok(None),
        })
        .await?
    }

    // Helper functions for xattr operations. Returns FuseError to
    // make it easier to test I/O error codes than anyhow::Error.
    async fn setxattr<P: AsRef<std::path::Path>>(
        path: P,
        name: &str,
        value: &str,
    ) -> Result<(), FuseError> {
        let path = path.as_ref().to_path_buf();
        let name = name.to_string();
        let value = value.to_string();

        tokio::task::spawn_blocking(move || {
            xattr::set(&path, &name, value.as_bytes())?;
            Ok(())
        })
        .await?
    }

    // Helper functions for xattr operations. Returns FuseError to
    // make it easier to test I/O error codes than anyhow::Error.
    async fn clearxattr<P: AsRef<std::path::Path>>(path: P, name: &str) -> Result<(), FuseError> {
        let path = path.as_ref().to_path_buf();
        let name = name.to_string();

        tokio::task::spawn_blocking(move || {
            xattr::remove(&path, &name)?;
            Ok(())
        })
        .await?
    }

    // Helper functions for xattr operations. Returns FuseError to
    // make it easier to test I/O error codes than anyhow::Error.
    async fn listxattr<P: AsRef<std::path::Path>>(path: P) -> Result<Vec<String>, FuseError> {
        let path = path.as_ref().to_path_buf();

        tokio::task::spawn_blocking(move || {
            let list = xattr::list(&path)?;
            let attrs: Vec<String> = list
                .map(|name| name.to_string_lossy().to_string())
                .collect();
            log::debug!("xattr::list({:?}) returned: {:?}", path, attrs);
            Ok(attrs)
        })
        .await?
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

                fixture
                    .inner
                    .write_file_and_wait(b, a, "somefile.txt", "test!")
                    .await?;

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

                fixture
                    .inner
                    .write_file_and_wait(b, a, "hello.txt", "world")
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

                fixture
                    .inner
                    .write_file_and_wait(b, a, "todelete.txt", "delete me")
                    .await?;
                let realpath_in_b = fixture.inner.arena_root(b).join("todelete.txt");
                assert!(fs::metadata(&realpath_in_b).await.is_ok());

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

                fixture
                    .inner
                    .write_file_and_wait(b, a, "source.txt", "source content")
                    .await?;
                let realpath_in_b = fixture.inner.arena_root(b).join("source.txt");
                assert!(fs::metadata(&realpath_in_b).await.is_ok());

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

                fixture
                    .inner
                    .write_file_and_wait(b, a, "new_directory/test.txt", "test content")
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

                fixture
                    .inner
                    .write_file_and_wait(b, a, "dir_to_remove/test.txt", "test content")
                    .await?;

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

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn rename() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                testing::connect(&household_a, b).await?;

                fixture
                    .inner
                    .write_file_and_wait(b, a, "source.txt", "source content")
                    .await?;
                fixture.mount(household_a).await?;

                let mountpoint = fixture.mount_path();
                let source_in_a = mountpoint.join("myarena/source.txt");
                let dest_in_a = mountpoint.join("myarena/dest.txt");

                assert!(fs::metadata(&source_in_a).await.is_ok());
                fs::rename(&source_in_a, &dest_in_a).await.unwrap();
                assert!(fs::metadata(&source_in_a).await.is_err());
                assert!(fs::metadata(&dest_in_a).await.is_ok());
                let deadline = Instant::now() + Duration::from_secs(10);
                while Instant::now() < deadline {
                    match fs::read_to_string(&dest_in_a).await {
                        Ok(content) => {
                            assert_eq!("source content", content);
                            break;
                        }
                        Err(err) => {
                            if err.kind() != std::io::ErrorKind::NotFound {
                                panic!("{dest_in_a:?}: {err:?}");
                            }
                        }
                    }
                }

                // Rename should be executed on B.
                let deadline = Instant::now() + Duration::from_secs(10);
                let root_b = fixture.inner.arena_root(b);
                let source_in_b = root_b.join("source.txt");
                let dest_in_b = root_b.join("dest.txt");
                while fs::metadata(&source_in_b).await.is_ok() && Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                while fs::metadata(&dest_in_b).await.is_err() && Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                assert!(fs::metadata(&dest_in_b).await.is_ok());
                assert!(fs::metadata(&source_in_b).await.is_err());
                assert_eq!("source content", fs::read_to_string(dest_in_b).await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn overwrite_cached_file() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                testing::connect(&household_a, b).await?;

                fixture
                    .inner
                    .write_file_and_wait(b, a, "testfile.txt", "will be overritten")
                    .await?;
                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                let file_path = arena_path.join("testfile.txt");

                // Write shorter string to the cached file
                let new_content = "short";
                tokio::fs::write(&file_path, new_content).await?;

                // Read it back from FUSE mountpoint. We should get
                // the updated value. If truncating the file didn't
                // work, some data from the original content might be
                // left over.
                let updated_content = tokio::fs::read_to_string(&file_path).await?;
                assert_eq!(new_content, updated_content);

                // Check that the file has been written to datadir
                let datadir_path = fixture.inner.arena_root(a).join("testfile.txt");
                let datadir_content = tokio::fs::read_to_string(&datadir_path).await?;
                assert_eq!(new_content, datadir_content);

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn overwritten_file_removed_from_original() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                testing::connect(&household_a, b).await?;

                fixture
                    .inner
                    .write_file_and_wait(b, a, "testfile.txt", "original")
                    .await?;
                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                let file_path = arena_path.join("testfile.txt");

                tokio::fs::write(&file_path, "overwrite").await?;

                // Check that the file has been written to datadir
                let datadir_path = fixture.inner.arena_root(a).join("testfile.txt");
                assert!(tokio::fs::metadata(&datadir_path).await.is_ok());

                // file_in_b should be deleted from b, since it's marked watch (by default) and
                // a now has the latest version.
                let b_datadir = fixture.inner.arena_root(b);
                let deadline = Instant::now() + Duration::from_secs(3);
                let file_in_b = b_datadir.join("testfile.txt");
                while tokio::fs::metadata(&file_in_b).await.is_ok() && Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                assert!(
                    tokio::fs::metadata(&file_in_b).await.is_err(),
                    "{file_in_b:?} should eventually be removed"
                );

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn partially_overwrite_cached_file() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                testing::connect(&household_a, b).await?;

                // Create a file in peer B's arena
                let original_content = "123456789";
                fixture
                    .inner
                    .write_file_and_wait(b, a, "partial.txt", original_content)
                    .await?;

                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                let file_path = arena_path.join("partial.txt");

                tokio::task::spawn_blocking({
                    // Flushing AsyncWrite file gets stuck if there's
                    // only one thread. Using blocking I/O and
                    // spawn_blocking avoids the issue.
                    let file_path = file_path.clone();

                    move || {
                        let mut file = std::fs::OpenOptions::new()
                            .write(true)
                            .truncate(false)
                            .open(&file_path)?;
                        file.write_all(b"abc")?;
                        file.flush()?;
                        drop(file);

                        Ok::<(), std::io::Error>(())
                    }
                })
                .await??;

                // File should have new prefix + original suffix
                let result_content = tokio::fs::read_to_string(&file_path).await?;
                let expected = "abc456789";
                assert_eq!(expected, result_content);

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn chown_fails() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                testing::connect(&household_a, b).await?;

                fixture
                    .inner
                    .write_file_and_wait(b, a, "unmodified", "content")
                    .await?;
                fixture
                    .inner
                    .write_file_and_wait(b, a, "modified", "content")
                    .await?;

                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                let local_path = arena_path.join("modified");
                let remote_path = arena_path.join("unmodified");
                tokio::fs::write(&local_path, "modified").await.unwrap();

                let current_uid = nix::unistd::getuid();
                let current_gid = nix::unistd::getgid();
                #[cfg(target_os = "linux")]
                let mut groups = nix::unistd::getgroups()?;
                #[cfg(not(target_os = "linux"))]
                let mut groups = vec![]; // no getgroups on macos
                groups.retain(|gid| *gid != current_gid);
                let othergroup = groups.into_iter().next();

                for path in [&local_path, &remote_path] {
                    // no-op chown succeeds
                    tokio::task::spawn_blocking({
                        let path = path.clone();
                        move || nix::unistd::chown(&path, Some(current_uid), Some(current_gid))
                    })
                    .await?
                    .expect("chown {path:?}");
                }

                // Realize doesn't allow changing groups on remote files
                if let Some(othergroup) = othergroup {
                    assert_eq!(
                        tokio::task::spawn_blocking({
                            let path = remote_path.clone();
                            move || nix::unistd::chown(&path, Some(current_uid), Some(othergroup))
                        })
                        .await?,
                        Err(nix::errno::Errno::EPERM)
                    )
                }

                // Changing groups on local files is allowed
                if let Some(othergroup) = othergroup {
                    tokio::task::spawn_blocking({
                        let path = local_path.clone();
                        move || nix::unistd::chown(&path, Some(current_uid), Some(othergroup))
                    })
                    .await??;
                }

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn truncate_cached_file() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                testing::connect(&household_a, b).await?;

                // Create a file in peer B's arena
                let original_content = "0123456789";
                fixture
                    .inner
                    .write_file_and_wait(b, a, "setlen.txt", original_content)
                    .await?;
                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                let file_path = arena_path.join("setlen.txt");

                tokio::task::spawn_blocking({
                    // Flushing AsyncWrite file gets stuck if there's
                    // only one thread. Using blocking I/O and
                    // spawn_blocking avoids the issue.
                    let file_path = file_path.clone();

                    move || {
                        let file = std::fs::OpenOptions::new()
                            .write(true)
                            .truncate(false)
                            .open(&file_path)?;
                        file.set_len(5)?;
                        drop(file);

                        Ok::<(), std::io::Error>(())
                    }
                })
                .await??;

                // Check the file is now 5 bytes
                assert_eq!(5, tokio::fs::metadata(&file_path).await?.len());
                assert_eq!("01234", tokio::fs::read_to_string(&file_path).await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn expand_cached_file() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                testing::connect(&household_a, b).await?;

                // Create a file in peer B's arena
                let original_content = "0123456789";
                fixture
                    .inner
                    .write_file_and_wait(b, a, "setlen.txt", original_content)
                    .await?;
                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                let file_path = arena_path.join("setlen.txt");

                tokio::task::spawn_blocking({
                    // Flushing AsyncWrite file gets stuck if there's
                    // only one thread. Using blocking I/O and
                    // spawn_blocking avoids the issue.
                    let file_path = file_path.clone();

                    move || {
                        let file = std::fs::OpenOptions::new()
                            .write(true)
                            .truncate(false)
                            .open(&file_path)?;
                        file.set_len(12)?;
                        drop(file);

                        Ok::<(), std::io::Error>(())
                    }
                })
                .await??;

                // Check the file is now 12 bytes
                assert_eq!(12, tokio::fs::metadata(&file_path).await?.len());
                assert_eq!(
                    "0123456789\0\0",
                    tokio::fs::read_to_string(&file_path).await?
                );

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn cached_file_written_to_multiple_times() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                testing::connect(&household_a, b).await?;

                fixture
                    .inner
                    .write_file_and_wait(b, a, "multi.txt", "start")
                    .await?;
                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                let file_path = arena_path.join("multi.txt");

                // Write multiple times by reading current content and appending
                let mut current_content = String::from("start");
                for i in 1..=5 {
                    let addition = format!("-{}", i);
                    current_content.push_str(&addition);

                    // Write the complete updated content
                    tokio::fs::write(&file_path, &current_content).await?;

                    // Verify after each write
                    let read_content = tokio::fs::read_to_string(&file_path).await?;
                    assert_eq!(current_content, read_content);
                }

                // Final check - should be "start-1-2-3-4-5"
                let final_content = tokio::fs::read_to_string(&file_path).await?;
                assert_eq!("start-1-2-3-4-5", final_content);

                // Check datadir has the same
                let datadir_path = fixture.inner.arena_root(a).join("multi.txt");
                let datadir_content = tokio::fs::read_to_string(&datadir_path).await?;
                assert_eq!("start-1-2-3-4-5", datadir_content);

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;
        Ok(())
    }

    /// Test unlink behavior on cached files that were just overwritten.
    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn unlink_overwritten_cached_file() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                // Create file in peer B and wait for sync to A's cache
                fixture
                    .inner
                    .write_file_and_wait(b, a, "test_file.txt", "original content")
                    .await?;

                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                let file_path = arena_path.join("test_file.txt");

                tokio::fs::write(&file_path, "new overwritten content").await?;

                fs::remove_file(&file_path).await.unwrap();

                // Remove file applies on the overwritten file, in datadir.
                let datadir_path = fixture.inner.arena_root(a).join("testfile.txt");
                assert!(tokio::fs::metadata(datadir_path).await.is_err());

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;

        Ok(())
    }

    /// Test link behavior on cached files that were just overwritten.
    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn link_overwritten_cached_file() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                fixture
                    .inner
                    .write_file_and_wait(b, a, "source.txt", "original content")
                    .await?;

                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                let file_path = arena_path.join("source.txt");
                let link_path = arena_path.join("dest.txt");

                tokio::fs::write(&file_path, "new overwritten content").await?;

                fs::hard_link(&file_path, &link_path).await?;

                assert!(fs::metadata(&file_path).await.is_ok());
                assert!(fs::metadata(&link_path).await.is_ok());
                assert_eq!(
                    "new overwritten content",
                    fs::read_to_string(&link_path).await?
                );

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;

        Ok(())
    }

    /// Test rename behavior on cached files that were just overwritten.
    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn rename_overwritten_cached_file() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                testing::connect(&household_a, b).await?;

                fixture
                    .inner
                    .write_file_and_wait(b, a, "source.txt", "original content")
                    .await?;

                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                let file_path = arena_path.join("source.txt");
                let new_path = arena_path.join("dest.txt");

                tokio::fs::write(&file_path, "new overwritten content").await?;

                fs::rename(&file_path, &new_path).await?;
                tokio::time::sleep(Duration::from_secs(3)).await;

                assert!(!fs::metadata(&file_path).await.is_ok());
                assert!(fs::metadata(&new_path).await.is_ok());
                assert_eq!(
                    "new overwritten content",
                    fs::read_to_string(&new_path).await?
                );

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn create_file() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let _a = HouseholdFixture::a();
                let _b = HouseholdFixture::b();

                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                let file_path = arena_path.join("new_file.txt");

                // Create a new file using spawn_blocking to test the FUSE create syscall
                tokio::task::spawn_blocking({
                    let file_path = file_path.clone();
                    move || {
                        let mut file = std::fs::OpenOptions::new()
                            .create(true)
                            .write(true)
                            .read(true)
                            .open(&file_path)?;
                        file.write_all(b"Hello, created file!")?;
                        file.flush()?;
                        drop(file);
                        Ok::<(), std::io::Error>(())
                    }
                })
                .await??;

                // Just check that we can read the content back - simplify first
                let content = fs::read_to_string(&file_path).await?;
                assert_eq!("Hello, created file!", content);

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn create_file_already_exists() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                // First create a file
                fixture
                    .inner
                    .write_file_and_wait(b, a, "existing_file.txt", "original content")
                    .await?;

                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                let file_path = arena_path.join("existing_file.txt");

                // Try to create the same file with O_CREAT|O_EXCL should fail
                let result = tokio::task::spawn_blocking({
                    let file_path = file_path.clone();
                    move || {
                        std::fs::OpenOptions::new()
                            .create_new(true)
                            .write(true)
                            .open(&file_path)
                    }
                })
                .await?;

                assert!(result.is_err());
                assert_eq!(
                    std::io::ErrorKind::AlreadyExists,
                    result.err().unwrap().kind()
                );

                // Original content should still be there
                let content = fs::read_to_string(&file_path).await?;
                assert_eq!("original content", content);

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn setattr_chmod_local_file() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();

                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                let file_path = arena_path.join("chmod_test.txt");

                // Create a new file to make it local
                tokio::fs::write(&file_path, "test content").await?;

                // Verify initial permissions
                let initial_meta = tokio::fs::metadata(&file_path).await?;
                let initial_mode = initial_meta.permissions().mode() & 0o777;
                assert!((initial_mode & 0o777) != 0o750);

                // Change permissions
                let new_perms = std::fs::Permissions::from_mode(0o750);
                tokio::fs::set_permissions(&file_path, new_perms).await?;

                // Check permissions via FUSE mountpoint
                let fuse_meta = tokio::fs::metadata(&file_path).await?;
                let expected_mode = 0o750;
                assert_eq!(expected_mode, fuse_meta.permissions().mode() & 0o777);

                // Check permissions in underlying datadir
                let datadir_path = fixture.inner.arena_root(a).join("chmod_test.txt");
                let datadir_meta = tokio::fs::metadata(&datadir_path).await?;
                assert_eq!(expected_mode, datadir_meta.permissions().mode() & 0o777);

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn setattr_chown_local_file() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();

                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                let file_path = arena_path.join("chown_test.txt");

                // Create a new file to make it local
                tokio::fs::write(&file_path, "test content").await?;

                let current_uid = nix::unistd::getuid();
                let current_gid = nix::unistd::getgid();

                // No-op chown should succeed
                tokio::task::spawn_blocking({
                    let path = file_path.clone();
                    move || nix::unistd::chown(&path, Some(current_uid), Some(current_gid))
                })
                .await?
                .expect("chown with same uid/gid should succeed");

                // Verify ownership via FUSE mountpoint
                let fuse_meta = tokio::fs::metadata(&file_path).await?;
                assert_eq!(current_uid.as_raw(), fuse_meta.uid());
                assert_eq!(current_gid.as_raw(), fuse_meta.gid());

                // Verify ownership in underlying datadir
                let datadir_path = fixture.inner.arena_root(a).join("chown_test.txt");
                let datadir_meta = tokio::fs::metadata(&datadir_path).await?;
                assert_eq!(current_uid.as_raw(), datadir_meta.uid());
                assert_eq!(current_gid.as_raw(), datadir_meta.gid());

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn setattr_utimens_local_file() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();

                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                let file_path = arena_path.join("utimens_test.txt");

                // Create a new file to make it local
                tokio::fs::write(&file_path, "test content").await?;

                // Wait a bit to ensure different timestamps
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                // Set specific timestamps using nix directly
                let target_time =
                    SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1234567890);
                let target_timespec =
                    TimeSpec::from(target_time.duration_since(SystemTime::UNIX_EPOCH).unwrap());

                tokio::task::spawn_blocking({
                    let path = file_path.clone();
                    let atime_spec = target_timespec.clone();
                    let mtime_spec = target_timespec.clone();
                    move || {
                        stat::utimensat(
                            AT_FDCWD,
                            &path,
                            &atime_spec,
                            &mtime_spec,
                            stat::UtimensatFlags::FollowSymlink,
                        )
                    }
                })
                .await?
                .expect("utimensat should succeed");

                // Check timestamps via FUSE mountpoint
                let fuse_meta = tokio::fs::metadata(&file_path).await?;
                let fuse_mtime = fuse_meta.modified()?;
                let fuse_mtime_secs = fuse_mtime.duration_since(SystemTime::UNIX_EPOCH)?.as_secs();
                assert_eq!(1234567890, fuse_mtime_secs);

                // Check timestamps in underlying datadir
                let datadir_path = fixture.inner.arena_root(a).join("utimens_test.txt");
                let datadir_meta = tokio::fs::metadata(&datadir_path).await?;
                let datadir_mtime = datadir_meta.modified()?;
                let datadir_mtime_secs = datadir_mtime
                    .duration_since(SystemTime::UNIX_EPOCH)?
                    .as_secs();
                assert_eq!(1234567890, datadir_mtime_secs);

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn create_file_in_subdir() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let _a = HouseholdFixture::a();
                let _b = HouseholdFixture::b();

                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                let subdir_path = arena_path.join("subdir");

                // Create the subdirectory first using FUSE mkdir
                tokio::fs::create_dir(&subdir_path).await?;

                // Verify directory was created
                let dir_attr = fs::metadata(&subdir_path).await?;
                assert!(dir_attr.is_dir());

                let file_path = subdir_path.join("subfile.txt");

                // Create a new file in the subdirectory using spawn_blocking
                tokio::task::spawn_blocking({
                    let file_path = file_path.clone();
                    move || {
                        let mut file = std::fs::OpenOptions::new()
                            .create(true)
                            .write(true)
                            .read(true)
                            .open(&file_path)?;
                        file.write_all(b"subdir content")?;
                        file.flush()?;
                        drop(file);
                        Ok::<(), std::io::Error>(())
                    }
                })
                .await??;

                // Check that we can read the content back
                let content = fs::read_to_string(&file_path).await?;
                assert_eq!("subdir content", content);

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn list_file_and_dir_xattrs() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                fixture
                    .inner
                    .write_file_and_wait(b, a, "foo/bar", "test")
                    .await?;

                fixture.mount(household_a).await?;

                let mount_path = fixture.mount_path();
                let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                let file_path = arena_path.join("foo/bar");
                let dir_path = arena_path.join("foo");

                assert_eq!(
                    vec!["realize.mark".to_string(),],
                    listxattr(&dir_path).await.unwrap()
                );
                assert_eq!(
                    vec![
                        "realize.mark".to_string(),
                        "realize.status".to_string(),
                        "realize.version".to_string()
                    ],
                    listxattr(&file_path).await.unwrap()
                );

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn get_mark_xattrs() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                let arena = HouseholdFixture::test_arena();

                fixture
                    .inner
                    .write_file_and_wait(b, a, "foo/bar", "test")
                    .await?;

                fixture.mount(household_a).await?;

                let mountpoint = fixture.mount_path();
                let datadir = mountpoint.join(HouseholdFixture::test_arena().as_str());
                let file_path = realize_types::Path::parse("foo/bar")?;
                let dir_path = realize_types::Path::parse("foo")?;
                let file_realpath = file_path.within(&datadir);
                let dir_realpath = dir_path.within(&datadir);

                assert_eq!(
                    Some("watch (derived)".to_string()),
                    getxattr(&dir_realpath, "realize.mark").await.unwrap()
                );
                assert_eq!(
                    Some("watch (derived)".to_string()),
                    getxattr(&file_realpath, "realize.mark").await.unwrap()
                );

                let storage = fixture.inner.storage(a)?;
                storage.set_mark(arena, &dir_path, Mark::Keep).await?;

                assert_eq!(
                    Some("keep".to_string()),
                    getxattr(&dir_realpath, "realize.mark").await.unwrap()
                );
                assert_eq!(
                    Some("keep (derived)".to_string()),
                    getxattr(&file_realpath, "realize.mark").await.unwrap()
                );

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn set_mark_xattrs() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                let _arena = HouseholdFixture::test_arena();

                fixture
                    .inner
                    .write_file_and_wait(b, a, "foo/bar", "test")
                    .await?;

                fixture.mount(household_a).await?;

                let mountpoint = fixture.mount_path();
                let datadir = mountpoint.join(HouseholdFixture::test_arena().as_str());
                let file_path = realize_types::Path::parse("foo/bar")?;
                let dir_path = realize_types::Path::parse("foo")?;
                let file_realpath = file_path.within(&datadir);
                let dir_realpath = dir_path.within(&datadir);

                // Initial state should be watch (derived)
                assert_eq!(
                    Some("watch (derived)".to_string()),
                    getxattr(&dir_realpath, "realize.mark").await.unwrap()
                );
                assert_eq!(
                    Some("watch (derived)".to_string()),
                    getxattr(&file_realpath, "realize.mark").await.unwrap()
                );

                // Set directory mark to keep via FUSE setxattr
                setxattr(&dir_realpath, "realize.mark", "keep").await?;

                // Directory should now be keep (direct)
                assert_eq!(
                    Some("keep".to_string()),
                    getxattr(&dir_realpath, "realize.mark").await.unwrap()
                );
                // File should now be keep (derived)
                assert_eq!(
                    Some("keep (derived)".to_string()),
                    getxattr(&file_realpath, "realize.mark").await.unwrap()
                );

                // Set file mark to own via FUSE setxattr
                setxattr(&file_realpath, "realize.mark", "own").await?;

                // File should now be own (direct)
                assert_eq!(
                    Some("own".to_string()),
                    getxattr(&file_realpath, "realize.mark").await.unwrap()
                );
                // Directory should still be keep (direct)
                assert_eq!(
                    Some("keep".to_string()),
                    getxattr(&dir_realpath, "realize.mark").await.unwrap()
                );

                // Clear file mark by setting empty value
                setxattr(&file_realpath, "realize.mark", "").await?;

                // File should now inherit from directory: keep (derived)
                assert_eq!(
                    Some("keep (derived)".to_string()),
                    getxattr(&file_realpath, "realize.mark").await.unwrap()
                );

                // Clear directory mark
                setxattr(&dir_realpath, "realize.mark", "").await?;

                // Both should now be watch (derived from arena root)
                assert_eq!(
                    Some("watch (derived)".to_string()),
                    getxattr(&dir_realpath, "realize.mark").await.unwrap()
                );
                assert_eq!(
                    Some("watch (derived)".to_string()),
                    getxattr(&file_realpath, "realize.mark").await.unwrap()
                );

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn clear_mark_xattrs() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                let _arena = HouseholdFixture::test_arena();

                fixture
                    .inner
                    .write_file_and_wait(b, a, "foo/bar", "test")
                    .await?;

                fixture.mount(household_a).await?;

                let mountpoint = fixture.mount_path();
                let datadir = mountpoint.join(HouseholdFixture::test_arena().as_str());
                let file_path = realize_types::Path::parse("foo/bar")?;
                let file_realpath = file_path.within(&datadir);

                setxattr(&file_realpath, "realize.mark", "keep").await?;

                assert_eq!(
                    Some("keep".to_string()),
                    getxattr(&file_realpath, "realize.mark").await.unwrap()
                );

                clearxattr(&file_realpath, "realize.mark").await.unwrap();

                assert_eq!(
                    Some("watch (derived)".to_string()),
                    getxattr(&file_realpath, "realize.mark").await.unwrap()
                );

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn get_version_xattrs() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                let (file_path, hash) = fixture
                    .inner
                    .write_file_and_wait(b, a, "foo/bar", "test")
                    .await?;

                fixture.mount(household_a).await?;

                let mountpoint = fixture.mount_path();
                let datadir = mountpoint.join(HouseholdFixture::test_arena().as_str());
                let file_realpath = file_path.within(&datadir);
                let dir_realpath = datadir.join("foo");

                assert_eq!(
                    Some(hash.to_string()),
                    getxattr(&file_realpath, "realize.version").await.unwrap()
                );
                tokio::fs::write(&file_realpath, "overwrite").await?;

                // Depending on how fast this is executed, the local
                // file may or may not have been re-hashed yet.
                let got = getxattr(&file_realpath, "realize.version")
                    .await
                    .unwrap()
                    .unwrap();
                assert!(
                    got == "modified" || got == hash::digest("overwrite").to_string(),
                    "version: '{got}'"
                );

                assert_eq!(
                    Some(libc::EISDIR),
                    getxattr(&dir_realpath, "realize.version")
                        .await
                        .err()
                        .map(|e| e.errno())
                );

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn get_status_xattr() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();
                let arena = HouseholdFixture::test_arena();

                let data = "x".to_string().repeat(32 * 1024);
                let (file_path, _) = fixture
                    .inner
                    .write_file_and_wait(b, a, "foo/bar", &data)
                    .await?;

                fixture.mount(household_a).await?;

                let mountpoint = fixture.mount_path();
                let datadir = mountpoint.join(HouseholdFixture::test_arena().as_str());
                let file_realpath = file_path.within(&datadir);
                let dir_realpath = datadir.join("foo");

                assert_eq!(
                    Some(libc::EISDIR),
                    getxattr(&dir_realpath, "realize.version")
                        .await
                        .err()
                        .map(|e| e.errno())
                );

                assert_eq!(
                    Some("remote 0%".to_string()),
                    getxattr(&file_realpath, "realize.status").await.unwrap()
                );

                // read a part of the file, so it'll be incomplete
                tokio::task::spawn_blocking({
                    let file_realpath = file_realpath.clone();
                    move || {
                        let mut file = std::fs::File::open(&file_realpath)?;
                        let mut buf = [1; 10];
                        file.read(&mut buf)?;
                        Ok::<(), std::io::Error>(())
                    }
                })
                .await??;

                assert_eq!(
                    Some("remote 75%".to_string()),
                    getxattr(&file_realpath, "realize.status").await.unwrap()
                );

                // download the whole of the file
                tokio::fs::read(&file_realpath).await?;

                assert_eq!(
                    Some("remote 100%".to_string()),
                    getxattr(&file_realpath, "realize.status").await.unwrap()
                );

                fixture
                    .inner
                    .cache(a)?
                    .file_content((arena, &file_path))
                    .await?
                    .blob()
                    .unwrap()
                    .mark_verified()
                    .await?;

                assert_eq!(
                    Some("remote 100% verified".to_string()),
                    getxattr(&file_realpath, "realize.status").await.unwrap()
                );

                let local_realpath = datadir.join("local");
                tokio::task::spawn_blocking({
                    let local_realpath = local_realpath.clone();
                    move || {
                        let mut file = std::fs::OpenOptions::new()
                            .create(true)
                            .write(true)
                            .open(&local_realpath)?;
                        file.write_all(b"test")?;
                        file.flush()?;
                        drop(file);
                        Ok::<(), std::io::Error>(())
                    }
                })
                .await??;

                assert_eq!(
                    Some("local 100%".to_string()),
                    getxattr(&local_realpath, "realize.status").await.unwrap()
                );

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;

        Ok(())
    }
}
