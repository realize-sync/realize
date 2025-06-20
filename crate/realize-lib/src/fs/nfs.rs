use std::{
    io::{ErrorKind, SeekFrom},
    net::SocketAddr,
    str::Utf8Error,
    sync::Arc,
    time::{Duration, SystemTime},
};

use moka::future::Cache;
use nfsserve::{
    nfs::{
        fattr3, fileid3, filename3, ftype3, gid3, nfspath3, nfsstat3, nfstime3, sattr3, specdata3,
        uid3,
    },
    tcp::{NFSTcp as _, NFSTcpListener},
    vfs::{NFSFileSystem, ReadDirResult, VFSCapabilities},
};

use crate::storage::unreal::{
    self, Download, Downloader, FileMetadata, InodeAssignment, UnrealCacheAsync, UnrealCacheError,
};
use async_trait::async_trait;
use tokio::{
    io::{AsyncReadExt as _, AsyncSeekExt as _},
    sync::Mutex,
    task::JoinHandle,
};

/// Export the given cache at the given socket address.
pub async fn export(
    cache: UnrealCacheAsync,
    downloader: Downloader,
    addr: SocketAddr,
) -> std::io::Result<JoinHandle<std::io::Result<()>>> {
    log::debug!("Listening to {}", addr);
    let listener =
        NFSTcpListener::bind(&addr.to_string(), UnrealFs::new(cache, downloader)).await?;

    Ok(tokio::spawn(async move {
        log::debug!("Running listener to {}", addr);
        listener.handle_forever().await
    }))
}

struct UnrealFs {
    cache: UnrealCacheAsync,
    downloader: Downloader,
    readers: Cache<u64, Arc<Mutex<Download>>>,
    uid: uid3,
    gid: gid3,
}

impl UnrealFs {
    fn new(cache: UnrealCacheAsync, downloader: Downloader) -> UnrealFs {
        Self {
            cache,
            downloader,
            uid: nix::unistd::getuid().into(),
            gid: nix::unistd::getgid().into(),
            // TODO: get notified when file modified or deleted and keep for a longer time.
            readers: Cache::builder()
                .max_capacity(128)
                .time_to_idle(Duration::from_secs(10))
                .build(),
        }
    }

    async fn do_read(
        &self,
        reader: Arc<Mutex<Download>>,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), UnrealFsError> {
        let mut reader = reader.lock().await;
        reader.seek(SeekFrom::Start(offset)).await?;
        let mut vec = vec![0; count as usize];
        let n = reader.read(&mut vec).await?;
        vec.truncate(n);

        Ok((vec, reader.at_end()))
    }

    async fn do_lookup(
        &self,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, UnrealFsError> {
        Ok(self
            .cache
            .lookup(dirid, std::str::from_utf8(filename)?)
            .await?
            .inode)
    }

    async fn do_readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, UnrealFsError> {
        let mut entries = self.cache.readdir(dirid).await?;
        entries.sort_by(|a, b| a.1.inode.cmp(&b.1.inode));

        let mut res = ReadDirResult {
            entries: vec![],
            end: true,
        };
        for (name, entry) in entries.into_iter().skip_while(|e| e.1.inode <= start_after) {
            res.entries.push(nfsserve::vfs::DirEntry {
                fileid: entry.inode,
                name: name.as_bytes().into(),
                attr: match entry.assignment {
                    InodeAssignment::Directory => {
                        self.build_dir_attr(entry.inode, self.cache.dir_mtime(entry.inode).await?)
                    }
                    InodeAssignment::File => self.build_file_attr(
                        entry.inode,
                        &self.cache.file_metadata(entry.inode).await?,
                    ),
                },
            });
            if res.entries.len() >= max_entries {
                res.end = false;
                break;
            }
        }

        Ok(res)
    }

    async fn do_getattr(&self, id: fileid3) -> Result<fattr3, UnrealFsError> {
        let (file_metadata, dir_mtime) =
            tokio::join!(self.cache.file_metadata(id), self.cache.dir_mtime(id));
        if let Ok(mtime) = dir_mtime {
            return Ok(self.build_dir_attr(id, mtime));
        }
        return Ok(self.build_file_attr(id, &file_metadata?));
    }

    fn build_file_attr(&self, inode: u64, metadata: &FileMetadata) -> fattr3 {
        let mtime = system_to_nfs_time(metadata.mtime);

        fattr3 {
            ftype: ftype3::NF3REG,
            mode: 0o0440,
            nlink: 1,
            uid: self.uid,
            gid: self.gid,
            size: metadata.size,
            used: metadata.size,
            rdev: specdata3::default(),
            fsid: 0,
            fileid: inode,
            atime: nfstime3::default(),
            mtime,
            ctime: mtime,
        }
    }

    fn build_dir_attr(&self, inode: u64, mtime: SystemTime) -> fattr3 {
        let mtime = system_to_nfs_time(mtime);
        fattr3 {
            ftype: ftype3::NF3DIR,
            mode: 0o0550,
            nlink: 1,
            uid: self.uid,
            gid: self.gid,
            size: 512,
            used: 512,
            rdev: specdata3::default(),
            fsid: 0,
            fileid: inode,
            atime: nfstime3::default(),
            mtime,
            ctime: mtime,
        }
    }
}

#[async_trait]
impl NFSFileSystem for UnrealFs {
    fn root_dir(&self) -> fileid3 {
        unreal::ROOT_DIR
    }

    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadOnly
    }

    async fn write(&self, _id: fileid3, _offset: u64, _data: &[u8]) -> Result<fattr3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn create(
        &self,
        _dirid: fileid3,
        _filename: &filename3,
        _attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn create_exclusive(
        &self,
        _dirid: fileid3,
        _filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        Ok(self.do_lookup(dirid, filename).await?)
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        Ok(self.do_getattr(id).await?)
    }
    async fn setattr(&self, _id: fileid3, _setattr: sattr3) -> Result<fattr3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn read(
        &self,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        let reader = self
            .readers
            .entry(id)
            .or_try_insert_with(async {
                let reader = self.downloader.reader(id).await?;

                Ok::<_, UnrealCacheError>(Arc::new(Mutex::new(reader)))
            })
            .await
            .map_err(|e| unreal_to_nfsstat3(e.as_ref()))?
            .into_value();

        Ok(self.do_read(reader, offset, count).await?)
    }

    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        Ok(self.do_readdir(dirid, start_after, max_entries).await?)
    }

    #[allow(unused)]
    async fn remove(&self, _dirid: fileid3, _filename: &filename3) -> Result<(), nfsstat3> {
        return Err(nfsstat3::NFS3ERR_ROFS);
    }

    #[allow(unused)]
    async fn rename(
        &self,
        _from_dirid: fileid3,
        _from_filename: &filename3,
        _to_dirid: fileid3,
        _to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        return Err(nfsstat3::NFS3ERR_ROFS);
    }

    #[allow(unused)]
    async fn mkdir(
        &self,
        _dirid: fileid3,
        _dirname: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn symlink(
        &self,
        _dirid: fileid3,
        _linkname: &filename3,
        _symlink: &nfspath3,
        _attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn readlink(&self, _id: fileid3) -> Result<nfspath3, nfsstat3> {
        return Err(nfsstat3::NFS3ERR_NOTSUPP);
    }
}

#[derive(Debug, thiserror::Error)]
enum UnrealFsError {
    #[error(transparent)]
    Cache(#[from] UnrealCacheError),

    #[error("invalid UTF-8 string")]
    Utf8(#[from] Utf8Error),

    #[error("I/O error")]
    Io(#[from] std::io::Error),
}

impl From<UnrealFsError> for nfsstat3 {
    fn from(err: UnrealFsError) -> nfsstat3 {
        use nfsstat3::*;
        match err {
            UnrealFsError::Utf8(_) => NFS3ERR_NOENT,
            UnrealFsError::Cache(e) => unreal_to_nfsstat3(&e),
            UnrealFsError::Io(e) => io_to_nfsstat3(&e),
        }
    }
}

fn unreal_to_nfsstat3(err: &UnrealCacheError) -> nfsstat3 {
    use nfsstat3::*;
    use UnrealCacheError::*;
    match err {
        NotFound => NFS3ERR_NOENT,
        NotADirectory => NFS3ERR_NOTDIR,
        IsADirectory => NFS3ERR_ISDIR,
        Io(e) => io_to_nfsstat3(e),
        _ => NFS3ERR_SERVERFAULT,
    }
}

fn io_to_nfsstat3(err: &std::io::Error) -> nfsstat3 {
    use nfsstat3::*;
    match err.kind() {
        ErrorKind::NotFound => NFS3ERR_NOENT,
        ErrorKind::NotADirectory => NFS3ERR_NOTDIR,
        ErrorKind::IsADirectory => NFS3ERR_ISDIR,
        ErrorKind::PermissionDenied => NFS3ERR_PERM,
        ErrorKind::AlreadyExists => NFS3ERR_EXIST,
        ErrorKind::InvalidInput => NFS3ERR_INVAL,
        ErrorKind::DirectoryNotEmpty => NFS3ERR_NOTEMPTY,
        // TODO: Go through the list again to find matches
        _ => NFS3ERR_IO,
    }
}

fn system_to_nfs_time(t: SystemTime) -> nfstime3 {
    let epoch_time = t
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    nfstime3 {
        seconds: epoch_time.as_secs() as u32,
        nseconds: epoch_time.subsec_nanos(),
    }
}
