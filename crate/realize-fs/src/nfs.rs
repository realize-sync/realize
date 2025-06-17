use std::{
    net::SocketAddr,
    str::Utf8Error,
    time::{Duration, SystemTime},
};

use nfsserve::{
    nfs::{
        fattr3, fileid3, filename3, ftype3, gid3, nfspath3, nfsstat3, nfstime3, sattr3, specdata3,
        uid3,
    },
    tcp::{NFSTcp as _, NFSTcpListener},
    vfs::{NFSFileSystem, ReadDirResult, VFSCapabilities},
};

use async_trait::async_trait;
use realize_lib::storage::unreal::{
    self, FileMetadata, InodeAssignment, UnrealCacheAsync, UnrealCacheError,
};
use tokio::task::JoinHandle;

/// Export the given cache at the given socket address.
pub async fn export(
    cache: UnrealCacheAsync,
    addr: SocketAddr,
) -> std::io::Result<JoinHandle<std::io::Result<()>>> {
    log::debug!("Listening to {}", addr);
    let listener = NFSTcpListener::bind(&addr.to_string(), UnrealFs::new(cache)).await?;

    Ok(tokio::spawn(async move {
        log::debug!("Running listener to {}", addr);
        listener.handle_forever().await
    }))
}

struct UnrealFs {
    cache: UnrealCacheAsync,
    uid: uid3,
    gid: gid3,
}

impl UnrealFs {
    fn new(cache: UnrealCacheAsync) -> UnrealFs {
        Self {
            cache,
            uid: nix::unistd::getuid().into(),
            gid: nix::unistd::getgid().into(),
        }
    }

    async fn do_lookup(
        &self,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, UnrealFsError> {
        Ok(self
            .cache
            .lookup(dirid, str::from_utf8(filename)?)
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
                    InodeAssignment::Directory => self.build_dir_attr(entry.inode),
                    InodeAssignment::File => {
                        self.build_file_attr(
                            entry.inode,
                            &self.cache.file_metadata(entry.inode).await?,
                        )
                        .await?
                    }
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
        match self.cache.file_metadata(id).await {
            Ok(metadata) => Ok(self.build_file_attr(id, &metadata).await?),

            // TODO: check that it exists and that it is indeed a
            // directory; we're just assuming.
            Err(UnrealCacheError::NotFound) => Ok(self.build_dir_attr(id)),
            Err(err) => Err(err.into()),
        }
    }

    async fn build_file_attr(
        &self,
        inode: u64,
        metadata: &FileMetadata,
    ) -> Result<fattr3, UnrealFsError> {
        let mtime = system_to_nfs_time(metadata.mtime);

        Ok(fattr3 {
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
        })
    }

    fn build_dir_attr(&self, inode: u64) -> fattr3 {
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
            mtime: nfstime3::default(),
            ctime: nfstime3::default(),
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
        _id: fileid3,
        _offset: u64,
        _count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        return Err(nfsstat3::NFS3ERR_NOTSUPP);
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
}

impl From<UnrealFsError> for nfsstat3 {
    fn from(err: UnrealFsError) -> nfsstat3 {
        use nfsstat3::*;
        use UnrealCacheError::*;
        use UnrealFsError::*;
        match err {
            Utf8(_) => NFS3ERR_NOENT,
            Cache(NotFound) => NFS3ERR_NOENT,
            Cache(NotADirectory) => NFS3ERR_NOTDIR,
            Cache(IsADirectory) => NFS3ERR_ISDIR,
            Cache(Io(_)) => NFS3ERR_IO,
            _ => NFS3ERR_SERVERFAULT,
        }
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
