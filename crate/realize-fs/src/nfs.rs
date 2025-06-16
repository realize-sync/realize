use nfsserve::{
    nfs::{fattr3, fileid3, filename3, nfspath3, nfsstat3, sattr3},
    vfs::{NFSFileSystem, ReadDirResult, VFSCapabilities},
};

use async_trait::async_trait;

pub struct UnrealFs {}

#[async_trait]
impl NFSFileSystem for UnrealFs {
    fn root_dir(&self) -> fileid3 {
        todo!()
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

    async fn lookup(&self, _dirid: fileid3, _filename: &filename3) -> Result<fileid3, nfsstat3> {
        todo!()
    }
    async fn getattr(&self, _id: fileid3) -> Result<fattr3, nfsstat3> {
        todo!()
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
        todo!()
    }

    async fn readdir(
        &self,
        _dirid: fileid3,
        _start_after: fileid3,
        _max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        todo!()
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
        todo!()
    }
}
