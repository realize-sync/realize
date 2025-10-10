//! FUSE operations layer - business logic for filesystem operations
//!
//! This module contains the core business logic for filesystem operations,
//! including InnerRealizeFs and all the filesystem operation implementations.

use super::error::FuseError;
use super::format;
use super::handles::{FHMode, FHRegistry, FileHandle};
use crate::fs::downloader::Downloader;
use crate::fs::fuse::handles::ReadDirData;
use fuser::FileType;
use nix::errno::Errno;
use nix::libc;
use nix::sys::stat;
use nix::sys::time::TimeSpec;
use nix::unistd::{Gid, Uid};
use realize_storage::{
    CacheStatus, DirMetadata, FileContent, FileMetadata, FileRealm, Filesystem, Inode, Mark,
    Metadata, StorageError, Version,
};
use realize_types::Hash;
use std::ffi::OsString;
use std::io::SeekFrom;
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio_util::bytes::BufMut;

const XATTR_MARK: &str = "realize.mark";
const XATTR_STATUS: &str = "realize.status";
const XATTR_VERSION: &str = "realize.version";
const XATTR_VERSIONS: &str = "realize.versions";

pub(crate) struct InnerRealizeFs {
    fs: Arc<Filesystem>,
    downloader: Downloader,
    umask: u16,
    handles: FHRegistry,
}

impl InnerRealizeFs {
    pub(crate) fn new(fs: Arc<Filesystem>, downloader: Downloader, umask: u16) -> Self {
        Self {
            fs,
            downloader,
            umask,
            handles: FHRegistry::new(),
        }
    }

    pub(crate) async fn lookup(
        &self,
        parent: u64,
        name: OsString,
    ) -> Result<fuser::FileAttr, FuseError> {
        let name = name.to_str().ok_or(FuseError::utf8())?;
        let (inode, metadata) = self.fs.lookup((Inode(parent), name)).await?;
        match metadata {
            Metadata::File(file_metadata) => Ok(self.build_file_attr(inode, &file_metadata)),
            Metadata::Dir(dir_metadata) => Ok(self.build_dir_attr(inode, dir_metadata)),
        }
    }

    pub(crate) async fn getattr(
        &self,
        ino: Inode,
        fh: Option<u64>,
    ) -> Result<fuser::FileAttr, FuseError> {
        if let Some(fh) = fh {
            let handle = self.handles.get_or_err(fh, ino).await?;
            if let FileHandle::Local(file, _) = &*handle.lock().await {
                return Ok(metadata_to_attr(&file.metadata().await?, ino));
            }
        }
        let metadata = self.fs.metadata(ino).await?;
        match metadata {
            Metadata::File(file_metadata) => Ok(self.build_file_attr(ino, &file_metadata)),
            Metadata::Dir(dir_metadata) => Ok(self.build_dir_attr(ino, dir_metadata)),
        }
    }

    pub(crate) async fn listxattr(&self, ino: Inode) -> Result<Vec<&'static str>, FuseError> {
        if let Metadata::File(_) = self.fs.metadata(ino).await? {
            return Ok(vec![
                XATTR_MARK,
                XATTR_STATUS,
                XATTR_VERSION,
                XATTR_VERSIONS,
            ]);
        }

        Ok(vec![XATTR_MARK])
    }

    pub(crate) async fn getxattr(&self, ino: Inode, name: OsString) -> Result<String, FuseError> {
        if name == XATTR_MARK {
            let (mark, direct) = self.fs.get_mark(ino).await?;
            if direct {
                return Ok(mark.to_string());
            }
            return Ok(format!("{} (derived)", mark));
        }

        if name == XATTR_STATUS {
            return Ok(match self.fs.file_realm(ino).await? {
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
            let m = self.fs.file_metadata(ino).await?;
            return Ok(match m.version {
                Version::Modified(_) => "modified".to_string(),
                Version::Indexed(hash) => hash.to_string(),
            });
        }

        if name == XATTR_VERSIONS {
            let alternatives = self.fs.list_alternatives(ino).await?;
            return Ok(format::format_versions(&alternatives));
        }

        Err(Errno::ENODATA.into())
    }

    pub(crate) async fn setxattr(
        &self,
        ino: Inode,
        name: OsString,
        value: Vec<u8>,
    ) -> Result<(), FuseError> {
        if name == XATTR_MARK {
            // Parse the mark value from the byte slice
            let value_str = std::str::from_utf8(&value)
                .map_err(|_| FuseError::utf8())?
                .trim();

            if value_str.is_empty() {
                // Clear the mark
                self.fs.clear_mark(ino).await?;
            } else {
                // Parse and set the mark
                let mark = Mark::parse(value_str).ok_or(FuseError::from(Errno::EINVAL))?;
                self.fs.set_mark(ino, mark).await?;
            }
            return Ok(());
        }

        if name == XATTR_VERSION {
            let value_str = std::str::from_utf8(&value)
                .map_err(|_| FuseError::from(Errno::EINVAL))?
                .trim();

            let hash = match Hash::from_base64(value_str) {
                Some(hash) => hash,
                None => return Err(FuseError::from(Errno::EINVAL)),
            };

            match self.fs.select_alternative(ino, &hash).await {
                Ok(()) => return Ok(()),
                Err(StorageError::UnknownVersion) => {
                    return Err(FuseError::from(Errno::ENOENT));
                }
                Err(err) => return Err(err.into()),
            }
        }

        // Other attributes are not supported for setting
        Err(Errno::ENOTSUP.into())
    }

    pub(crate) async fn read(
        &self,
        fh: u64,
        ino: Inode,
        offset: i64,
        size: u32,
    ) -> Result<Vec<u8>, FuseError> {
        let handle = self.handles.get_or_err(fh, ino).await?;
        let size = size as usize;

        // TODO: clarify type situation for offset. offset is i64 in
        // fuser, but u64 in libfuse and Linux. What's happening?
        let offset = offset as u64;
        let mut buffer = Vec::with_capacity(size).limit(size);
        match &mut *handle.lock().await {
            FileHandle::Dir(_) => return Err(Errno::EBADF.into()),
            FileHandle::Remote(reader, mode) => {
                mode.check_allow_read()?;
                reader.read_all_at(offset, &mut buffer).await?;
            }
            FileHandle::Local(file, mode) => {
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

    pub(crate) async fn write(
        &self,
        fh: u64,
        ino: Inode,
        offset: i64,
        data: &[u8],
    ) -> Result<u32, FuseError> {
        let handle = self.handles.get_or_err(fh, ino).await?;
        let offset = offset as u64; // TODO: clarify type situation for offset.
        let mut guard = handle.lock().await;
        let file = self.prepare_for_write(fh, &mut guard, None).await?;
        file.seek(SeekFrom::Start(offset)).await?;
        file.write_all(data).await?;

        Ok(data.len() as u32)
    }

    pub(crate) async fn setattr(
        &self,
        ino: Inode,
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
        match self.fs.file_realm(ino).await? {
            FileRealm::Remote(_) => {
                if uid.is_some_and(|uid| uid != nix::unistd::getuid().as_raw())
                    || gid.is_some_and(|gid| gid != nix::unistd::getgid().as_raw())
                {
                    log::debug!(
                        "SETATTR Inode({ino})@remote: cannot change uid or gid of remote file"
                    );
                    return Err(Errno::EPERM.into());
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

    pub(crate) async fn truncate(
        &self,
        ino: Inode,
        fh: Option<u64>,
        size: u64,
    ) -> Result<(), FuseError> {
        if let Some(fh) = fh {
            let handle = self.handles.get_or_err(fh, ino).await?;
            let mut guard = handle.lock().await;
            let file = self.prepare_for_write(fh, &mut guard, Some(size)).await?;
            file.set_len(size).await?;
            log::debug!("SETATTR {ino}: Truncate file FH#{fh} to {size}");

            return Ok(());
        }
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
                if size > 0 {
                    self.downloader.complete_blob(&mut blob).await?;
                }
                let mut file = blob.realize().await?;
                file.set_len(size).await?;
                file.flush().await?;
                return Ok(());
            }
        }
    }

    pub(crate) async fn opendir(&self, ino: Inode) -> Result<u64, FuseError> {
        let fh = self
            .handles
            .add(
                ino,
                FileHandle::Dir(ReadDirData::new(self.fs.readdir(ino).await?)),
            )
            .await;

        Ok(fh)
    }

    pub(crate) async fn releasedir(&self, fh: u64) -> Result<(), FuseError> {
        self.handles.remove(fh).await;

        Ok(())
    }

    pub(crate) async fn readdir(
        &self,
        fh: u64,
        ino: Inode,
        offset: i64,
        reply: &mut fuser::ReplyDirectory,
    ) -> Result<(), FuseError> {
        let handle = self.handles.get_or_err(fh, ino).await?;
        match &mut *handle.lock().await {
            FileHandle::Dir(entries) => {
                let pivot = Inode(offset as u64); // offset is actually a u64 in fuse
                for (name, pathid, metadata) in entries.after(pivot) {
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
            _ => return Err(Errno::EBADF.into()),
        }
    }

    pub(crate) async fn unlink(&self, parent: u64, name: OsString) -> Result<(), FuseError> {
        let name = name.to_str().ok_or(FuseError::utf8())?;
        let (inode, _) = self.fs.lookup((Inode(parent), name)).await?;
        self.fs.unlink(inode).await?;

        Ok(())
    }

    pub(crate) async fn link(
        &self,
        source: u64,
        parent: u64,
        name: OsString,
    ) -> Result<fuser::FileAttr, FuseError> {
        let name = name.to_str().ok_or(FuseError::utf8())?;

        let (dest, metadata) = self.fs.branch(Inode(source), (Inode(parent), name)).await?;

        Ok(self.build_file_attr(dest, &metadata))
    }

    pub(crate) async fn mkdir(
        &self,
        parent: u64,
        name: OsString,
        _mode: u32,
        _umask: u32,
    ) -> Result<fuser::FileAttr, FuseError> {
        let name = name.to_str().ok_or(FuseError::utf8())?;

        let (dest, metadata) = self.fs.mkdir((Inode(parent), name)).await?;

        Ok(self.build_dir_attr(dest, metadata))
    }

    pub(crate) async fn rmdir(&self, parent: u64, name: OsString) -> Result<(), FuseError> {
        let name = name.to_str().ok_or(FuseError::utf8())?;

        self.fs.rmdir((Inode(parent), name)).await?;

        Ok(())
    }

    pub(crate) async fn rename(
        &self,
        old_parent: u64,
        old_name: OsString,
        new_parent: u64,
        new_name: OsString,
        noreplace: bool,
    ) -> Result<(), FuseError> {
        let old_name = old_name.to_str().ok_or(FuseError::utf8())?;
        let new_name = new_name.to_str().ok_or(FuseError::utf8())?;
        self.fs
            .rename(
                (Inode(old_parent), old_name),
                (Inode(new_parent), new_name),
                noreplace,
            )
            .await?;

        Ok(())
    }

    pub(crate) async fn open(&self, ino: Inode, flags: i32) -> Result<(u64, u32), FuseError> {
        let handle = match self.fs.file_content(ino).await? {
            FileContent::Local(path) => FileHandle::Local(
                openoptions_from_flags(flags).open(path).await?,
                FHMode::from_flags(flags),
            ),
            FileContent::Remote(mut blob) => {
                let mode = FHMode::from_flags(flags);
                let rw_mode = flags & (libc::O_RDONLY | libc::O_WRONLY | libc::O_RDWR);
                if (flags & (libc::O_TRUNC | libc::O_APPEND)) != 0
                    && (rw_mode == libc::O_WRONLY || rw_mode == libc::O_RDWR)
                {
                    // Special cases where COW isn't available: write
                    // with O_TRUNC and/or O_APPEND.

                    if (flags & libc::O_TRUNC) == 0 {
                        self.downloader.complete_blob(&mut blob).await?;
                    }
                    let mut file = blob.realize().await?;
                    if (flags & libc::O_TRUNC) != 0 {
                        file.set_len(0).await?;
                        file.flush().await?;
                    }
                    let mode = FHMode::from_flags(flags);
                    if (flags & libc::O_APPEND) == 0 {
                        FileHandle::Local(file, mode)
                    } else {
                        drop(file);

                        // The file handle is not usable in this case.
                        // Reopen the file as a local file.
                        let path = self
                            .fs
                            .file_content(ino)
                            .await?
                            .path()
                            .ok_or(StorageError::NotFound)?;
                        FileHandle::Local(openoptions_from_flags(flags).open(path).await?, mode)
                    }
                } else {
                    log::debug!("Opened {ino:?} as blob with COW enabled");
                    let reader = self.downloader.reader(blob).await?;

                    FileHandle::Remote(reader, mode)
                }
            }
        };
        let fh = self.handles.add(ino, handle).await;
        log::debug!("Opened file {ino:?} as FH#{fh}");

        return Ok((fh, 0));
    }

    pub(crate) async fn create(
        &self,
        parent: u64,
        name: OsString,
        mode: u32,
        _umask: u32,
        flags: i32,
    ) -> Result<(fuser::FileAttr, u64), FuseError> {
        let name = name.to_str().ok_or(FuseError::utf8())?;
        log::debug!("CREATE parent={parent} name={name} mode={mode:o} flags={flags:#x}");

        let options = openoptions_from_flags(flags);
        let (ino, file) = self.fs.create(options, (Inode(parent), name)).await?;
        let attr = metadata_to_attr(&file.metadata().await?, ino);
        let fh = self
            .handles
            .add(ino, FileHandle::Local(file, FHMode::from_flags(flags)))
            .await;
        log::debug!("Created and opened Inode({ino}) in FH#{fh}",);

        Ok((attr, fh))
    }

    pub(crate) async fn flush(&self, fh: u64, ino: Inode) -> Result<(), FuseError> {
        let handle = self.handles.get_or_err(fh, ino).await?;
        match &mut *handle.lock().await {
            FileHandle::Dir(_) => return Err(Errno::EBADF.into()),
            FileHandle::Remote(blob, _) => {
                blob.update_db().await?;
            }
            FileHandle::Local(file, mode) => {
                if mode.allow_write() {
                    log::debug!("Flush FH#{fh}");
                    file.flush().await?;
                }
            }
        }

        Ok(())
    }

    pub(crate) async fn release(&self, fh: u64) -> Result<(), FuseError> {
        if let Some(handle) = self.handles.remove(fh).await {
            match &mut *handle.lock().await {
                FileHandle::Dir(_) => {}
                FileHandle::Remote(_, _) => {}
                FileHandle::Local(file, mode) => {
                    if mode.allow_write() {
                        log::debug!("Flush FH#{fh}");
                        file.flush().await?;
                    }
                }
            }
        }

        Ok(())
    }

    async fn prepare_for_write<'a>(
        &self,
        fh: u64,
        guard: &'a mut tokio::sync::MutexGuard<'_, FileHandle>,
        size: Option<u64>,
    ) -> Result<&'a mut tokio::fs::File, FuseError> {
        if let FileHandle::Remote(reader, mode) = &mut **guard
            && mode.allow_write()
        {
            log::debug!("Realizing blob in FH#{fh} COW");
            let mut blob = reader.take_blob().unwrap();
            if size != Some(0) {
                // TODO: complete only partially if 0 < size < file size
                self.downloader.complete_blob(&mut blob).await?;
            }
            let file = blob.realize().await?;
            log::debug!("Blob in FH#{fh} realized successfully");

            **guard = FileHandle::Local(file, mode.clone())
        }

        match &mut **guard {
            FileHandle::Local(file, mode) => {
                mode.check_allow_write()?;

                Ok(file)
            }
            _ => Err(Errno::EPERM.into()),
        }
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

pub(crate) fn openoptions_from_flags(flags: i32) -> tokio::fs::OpenOptions {
    let mut opt = tokio::fs::OpenOptions::new();
    let mode = flags & (libc::O_RDONLY | libc::O_WRONLY | libc::O_RDWR);
    opt.write(mode == libc::O_WRONLY || mode == libc::O_RDWR)
        .read(mode == libc::O_RDONLY || mode == libc::O_RDWR)
        .truncate((flags & libc::O_TRUNC) != 0)
        .append((flags & libc::O_APPEND) != 0);

    opt
}

// Build a FileAttr from a real file metadata and map it to `ino`.
pub(crate) fn metadata_to_attr(m: &std::fs::Metadata, ino: Inode) -> fuser::FileAttr {
    fuser::FileAttr {
        ino: ino.as_u64(),
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
    use crate::fs::fuse::{self, FuseHandle};
    use crate::rpc::testing::{self, HouseholdFixture};
    use nix::fcntl::AT_FDCWD;
    use realize_storage::Mark;
    use realize_storage::utils::hash;
    use realize_types::UnixTime;
    use std::io::{Read, Seek, Write};
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
            let handle = fuse::export(cache.clone(), downloader, self.mountpoint.path(), 0o027)?;
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
                        "realize.version".to_string(),
                        "realize.versions".to_string()
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

                let old_hash = hash::digest("test").to_string();
                let new_hash = hash::digest("overwrite").to_string();

                // Depending on how fast this is executed, we might see
                // the old hash, then "modified", before seeing the new hash.
                async fn get_version(path: &std::path::Path) -> String {
                    getxattr(&path, "realize.version").await.unwrap().unwrap()
                }
                let deadline = Instant::now() + Duration::from_secs(10);
                while get_version(&file_realpath).await == old_hash && Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                while get_version(&file_realpath).await == "modified" && Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                assert_eq!(new_hash, get_version(&file_realpath).await);

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
    async fn get_versions_xattr() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        fixture
            .inner
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                let (file_path, _) = fixture
                    .inner
                    .write_file_and_wait(b, a, "foo/versions_test", "test content")
                    .await?;

                fixture.mount(household_a).await?;

                let mountpoint = fixture.mount_path();
                let datadir = mountpoint.join(HouseholdFixture::test_arena().as_str());
                let file_realpath = file_path.within(&datadir);

                let ts = UnixTime::mtime(
                    &fixture
                        .inner
                        .arena_root(b)
                        .join("foo/versions_test")
                        .metadata()?,
                )
                .display();
                assert_eq!(
                    format!("b jTvZv9AF9BeWmbkhInMoTG3oUa6RDu4v0bsNlu3mWj0 12 {ts}\n"),
                    getxattr(&file_realpath, "realize.versions")
                        .await
                        .unwrap()
                        .unwrap()
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

                let verified = fixture
                    .inner
                    .cache(a)?
                    .file_content((arena, &file_path))
                    .await?
                    .blob()
                    .unwrap()
                    .verify()
                    .await?;
                assert!(verified);

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

    // Helper functions for testing file opening with specific flags
    fn open_with_flags<P: AsRef<std::path::Path>>(
        path: P,
        flags: i32,
    ) -> Result<std::fs::File, anyhow::Error> {
        use std::os::unix::io::FromRawFd;
        let path = path.as_ref();
        let fd = unsafe {
            libc::open(
                std::ffi::CString::new(path.to_string_lossy().as_ref())?.as_ptr(),
                flags,
                0o644,
            )
        };
        if fd == -1 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(unsafe { std::fs::File::from_raw_fd(fd) })
    }

    async fn get_file_status<P: AsRef<std::path::Path>>(path: P) -> anyhow::Result<String> {
        match getxattr(path, "realize.status").await {
            Ok(Some(status)) => Ok(status),
            Ok(None) => anyhow::bail!("No realize.status xattr found"),
            Err(e) => anyhow::bail!("Failed to read realize.status: {:?}", e),
        }
    }

    fn assert_status_contains(status: &str, expected: &str) {
        assert!(
            status.contains(expected),
            "Expected status to contain '{}', but got '{}'",
            expected,
            status
        );
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn open_with_o_trunc_flag() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        tokio::time::timeout(Duration::from_secs(30), async {
            fixture
                .inner
                .with_two_peers()
                .await?
                .interconnected()
                .run(async |household_a, _household_b| {
                    let a = HouseholdFixture::a();
                    let b = HouseholdFixture::b();

                    // Create a remote file in peer B
                    let original_content = "hello world from remote";
                    fixture
                        .inner
                        .write_file_and_wait(b, a, "trunc_test.txt", original_content)
                        .await?;

                    fixture.mount(household_a).await?;

                    let mount_path = fixture.mount_path();
                    let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                    let file_path = arena_path.join("trunc_test.txt");

                    // Verify file is initially remote
                    let initial_status = get_file_status(&file_path).await?;
                    assert_status_contains(&initial_status, "remote");

                    // Open with O_TRUNC | O_RDWR - should realize and truncate immediately
                    let flags = libc::O_TRUNC | libc::O_RDWR;
                    tokio::task::spawn_blocking({
                        let file_path = file_path.clone();
                        move || {
                            let file = open_with_flags(&file_path, flags)?;
                            drop(file); // Close the file
                            Ok::<(), anyhow::Error>(())
                        }
                    })
                    .await??;

                    // File should now be local and empty (truncated)
                    let size = tokio::fs::metadata(&file_path).await?.len();
                    assert_eq!(0, size, "File should be truncated to 0 bytes");

                    let final_status = get_file_status(&file_path).await?;
                    assert_status_contains(&final_status, "local");

                    // Verify datadir has the file
                    let datadir_path = fixture.inner.arena_root(a).join("trunc_test.txt");
                    assert!(tokio::fs::metadata(&datadir_path).await.is_ok());
                    assert_eq!(0, tokio::fs::metadata(&datadir_path).await?.len());

                    Ok::<(), anyhow::Error>(())
                })
                .await?;
            fixture.unmount().await?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn open_with_o_append_flag() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        tokio::time::timeout(Duration::from_secs(30), async {
            fixture
                .inner
                .with_two_peers()
                .await?
                .interconnected()
                .run(async |household_a, _household_b| {
                    let a = HouseholdFixture::a();
                    let b = HouseholdFixture::b();

                    // Create a remote file in peer B
                    let original_content = "original content";
                    fixture
                        .inner
                        .write_file_and_wait(b, a, "append_test.txt", original_content)
                        .await?;

                    fixture.mount(household_a).await?;

                    let mount_path = fixture.mount_path();
                    let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                    let file_path = arena_path.join("append_test.txt");

                    // Verify file is initially remote
                    let initial_status = get_file_status(&file_path).await?;
                    assert_status_contains(&initial_status, "remote");

                    // Open with O_APPEND | O_WRONLY - should realize before writing
                    let flags = libc::O_APPEND | libc::O_WRONLY;
                    tokio::task::spawn_blocking({
                        let file_path = file_path.clone();
                        move || {
                            let mut file = open_with_flags(&file_path, flags)?;
                            file.write_all(b" appended text")?;
                            file.flush()?;
                            file.sync_all()?;
                            Ok::<(), anyhow::Error>(())
                        }
                    })
                    .await??;

                    // File should now be local with appended content
                    let final_content = tokio::fs::read_to_string(&file_path).await?;
                    assert_eq!(
                        "original content appended text", final_content,
                        "Content should have original plus appended text"
                    );

                    let final_status = get_file_status(&file_path).await?;
                    assert_status_contains(&final_status, "local");

                    // Verify datadir has the same content
                    let datadir_path = fixture.inner.arena_root(a).join("append_test.txt");
                    let datadir_content = tokio::fs::read_to_string(&datadir_path).await?;
                    assert_eq!("original content appended text", datadir_content);

                    Ok::<(), anyhow::Error>(())
                })
                .await?;
            fixture.unmount().await?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn open_with_o_trunc_and_o_append_flags() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        tokio::time::timeout(Duration::from_secs(30), async {
            fixture
                .inner
                .with_two_peers()
                .await?
                .interconnected()
                .run(async |household_a, _household_b| {
                    let a = HouseholdFixture::a();
                    let b = HouseholdFixture::b();

                    // Create a remote file in peer B
                    let original_content = "content that will be truncated";
                    fixture
                        .inner
                        .write_file_and_wait(b, a, "trunc_append_test.txt", original_content)
                        .await?;

                    fixture.mount(household_a).await?;

                    let mount_path = fixture.mount_path();
                    let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                    let file_path = arena_path.join("trunc_append_test.txt");

                    // Verify file is initially remote
                    let initial_status = get_file_status(&file_path).await?;
                    assert_status_contains(&initial_status, "remote");

                    // Open with O_TRUNC | O_APPEND | O_RDWR - should realize, truncate, then append
                    let flags = libc::O_TRUNC | libc::O_APPEND | libc::O_RDWR;
                    tokio::task::spawn_blocking({
                        let file_path = file_path.clone();
                        move || {
                            let mut file = open_with_flags(&file_path, flags)?;
                            file.write_all(b"new content after truncate")?;
                            file.flush()?;
                            drop(file);
                            Ok::<(), anyhow::Error>(())
                        }
                    })
                    .await??;

                    // File should now be local with only the new content
                    let final_content = tokio::fs::read_to_string(&file_path).await?;
                    assert_eq!(
                        "new content after truncate", final_content,
                        "Content should be only the new text (original should be truncated)"
                    );

                    let final_status = get_file_status(&file_path).await?;
                    assert_status_contains(&final_status, "local");

                    // Verify datadir has the same content
                    let datadir_path = fixture.inner.arena_root(a).join("trunc_append_test.txt");
                    let datadir_content = tokio::fs::read_to_string(&datadir_path).await?;
                    assert_eq!("new content after truncate", datadir_content);

                    Ok::<(), anyhow::Error>(())
                })
                .await?;
            fixture.unmount().await?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn copy_on_write_behavior() -> anyhow::Result<()> {
        let mut fixture = FuseFixture::setup().await?;
        tokio::time::timeout(Duration::from_secs(30), async {
            fixture
                .inner
                .with_two_peers()
                .await?
                .interconnected()
                .run(async |household_a, _household_b| {
                    let a = HouseholdFixture::a();
                    let b = HouseholdFixture::b();

                    // Create a remote file in peer B
                    let original_content = "original remote content for COW test";
                    fixture
                        .inner
                        .write_file_and_wait(b, a, "cow_test.txt", original_content)
                        .await?;

                    fixture.mount(household_a).await?;

                    let mount_path = fixture.mount_path();
                    let arena_path = mount_path.join(HouseholdFixture::test_arena().as_str());
                    let file_path = arena_path.join("cow_test.txt");

                    // Verify file is initially remote
                    let initial_status = get_file_status(&file_path).await?;
                    assert_status_contains(&initial_status, "remote");

                    // Open with O_RDWR (no O_TRUNC or O_APPEND) - should enable COW
                    let flags = libc::O_RDWR;
                    tokio::task::spawn_blocking({
                        let file_path = file_path.clone();
                        move || {
                            let mut file = open_with_flags(&file_path, flags)?;

                            // Read some data - should still be remote
                            let mut buffer = vec![0u8; 8];
                            file.read_exact(&mut buffer)?;
                            assert_eq!(b"original".as_slice(), &buffer);

                            Ok::<(), anyhow::Error>(())
                        }
                    })
                    .await??;

                    // After read-only access, file should still be remote
                    let status_after_read = get_file_status(&file_path).await?;
                    assert_status_contains(&status_after_read, "remote");

                    // Now write to the file - this should trigger COW
                    tokio::task::spawn_blocking({
                        let file_path = file_path.clone();
                        move || {
                            let mut file = open_with_flags(&file_path, flags)?;
                            // Seek to middle and overwrite part of the content
                            file.seek(std::io::SeekFrom::Start(9))?; // After "original "
                            file.write_all(b"LOCAL")?;
                            file.flush()?;
                            drop(file);
                            Ok::<(), anyhow::Error>(())
                        }
                    })
                    .await??;

                    // After write, file should be local
                    let status_after_write = get_file_status(&file_path).await?;
                    assert_status_contains(&status_after_write, "local");

                    // Verify the merged content is correct
                    let final_content = tokio::fs::read_to_string(&file_path).await?;
                    assert_eq!(
                        "original LOCALe content for COW test", final_content,
                        "Content should be merged correctly after COW"
                    );

                    // Verify datadir has the same content
                    let datadir_path = fixture.inner.arena_root(a).join("cow_test.txt");
                    let datadir_content = tokio::fs::read_to_string(&datadir_path).await?;
                    assert_eq!("original LOCALe content for COW test", datadir_content);

                    Ok::<(), anyhow::Error>(())
                })
                .await?;
            fixture.unmount().await?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    async fn set_version_xattr() -> anyhow::Result<()> {
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
                    .storage(b)?
                    .set_arena_mark(arena, Mark::Own)
                    .await?;

                // Create a remote file from peer B first
                let remote_content = "remote version from peer b";
                let (file_path, remote_hash) = fixture
                    .inner
                    .write_file_and_wait(b, a, "versioned_file.txt", remote_content)
                    .await?;
                let remote_mtime = UnixTime::mtime(
                    &file_path
                        .within(fixture.inner.arena_root(b))
                        .metadata()
                        .unwrap(),
                );

                fixture.mount(household_a).await?;

                let mountpoint = fixture.mount_path();
                let datadir = mountpoint.join(arena.as_str());
                let file_realpath = file_path.within(&datadir);

                // Initially, the file should be remote (from peer b)
                let initial_version = getxattr(&file_realpath, "realize.version").await?.unwrap();
                assert_eq!(remote_hash.to_string(), initial_version);

                let initial_status = getxattr(&file_realpath, "realize.status").await?.unwrap();
                assert!(initial_status.contains("remote"));

                // Create a local version by writing to the file
                let local_content = "local version from peer a";
                let local_hash = hash::digest(local_content);
                tokio::fs::write(&file_realpath, local_content).await?;

                // Wait for the file to be indexed locally
                let deadline = Instant::now() + Duration::from_secs(10);
                while getxattr(&file_realpath, "realize.version").await?.unwrap()
                    != local_hash.to_string()
                    && Instant::now() < deadline
                {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                // Verify we have two alternatives available
                let versions_list = getxattr(&file_realpath, "realize.versions").await?.unwrap();
                assert_eq!(
                    format!(
                        "local {local_hash}\nb {remote_hash} 26 {}\n",
                        remote_mtime.display()
                    ),
                    versions_list
                );

                // Select the remote version via setxattr
                setxattr(&file_realpath, "realize.version", &remote_hash.to_string()).await?;

                // Verify the version changed back to remote
                let new_version = getxattr(&file_realpath, "realize.version").await?.unwrap();
                assert_eq!(remote_hash.to_string(), new_version);

                // Verify the status changed back to remote
                let new_status = getxattr(&file_realpath, "realize.status").await?.unwrap();
                assert!(new_status.contains("remote"));

                // Test error cases
                // 1. Invalid hash format should return EINVAL
                let invalid_hash_result =
                    setxattr(&file_realpath, "realize.version", "invalid_hash").await;
                assert!(invalid_hash_result.is_err());
                if let Err(e) = invalid_hash_result {
                    assert_eq!(libc::EINVAL, e.errno());
                }

                // 2. Unknown hash should return ENOENT
                let unknown_hash = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
                let unknown_hash_result =
                    setxattr(&file_realpath, "realize.version", unknown_hash).await;
                assert!(unknown_hash_result.is_err());
                if let Err(e) = unknown_hash_result {
                    assert_eq!(libc::ENOENT, e.errno());
                }

                // 3. Non-UTF8 bytes should return EINVAL
                let non_utf8_result = tokio::task::spawn_blocking({
                    let file_realpath = file_realpath.clone();
                    move || -> Result<(), std::io::Error> {
                        use std::ffi::CString;
                        use std::os::unix::ffi::OsStrExt;

                        let path_cstr = CString::new(file_realpath.as_os_str().as_bytes())
                            .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
                        let name_cstr = CString::new("realize.version")
                            .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
                        let value = [0xFFu8, 0xFEu8]; // Invalid UTF-8 sequence

                        let result = unsafe {
                            libc::setxattr(
                                path_cstr.as_ptr(),
                                name_cstr.as_ptr(),
                                value.as_ptr() as *const libc::c_void,
                                value.len(),
                                0,
                            )
                        };

                        if result == -1 {
                            let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
                            if errno == libc::EINVAL {
                                return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
                            }
                        }
                        Ok(())
                    }
                })
                .await?;

                assert!(non_utf8_result.is_err());
                if let Err(e) = non_utf8_result {
                    assert_eq!(Some(libc::EINVAL), e.raw_os_error());
                }

                // Verify the file is still at the remote version after error attempts
                let final_version = getxattr(&file_realpath, "realize.version").await?.unwrap();
                assert_eq!(remote_hash.to_string(), final_version);

                Ok::<(), anyhow::Error>(())
            })
            .await?;
        fixture.unmount().await?;

        Ok(())
    }
}
