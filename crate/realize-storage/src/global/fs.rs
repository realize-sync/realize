use super::db::GlobalDatabase;
use super::pathid_allocator::PathIdAllocator;
use crate::arena::fs::{ArenaFilesystem, ArenaFsLoc};
use crate::arena::notifier::{Notification, Progress};
use crate::arena::types::{DirMetadata, FileRealm};
use crate::global::db::GlobalWriteTransaction;
use crate::global::pathid_allocator;
use crate::global::types::PathTableEntry;
use crate::types::{PartialInode, PathIdPrefix};
use crate::utils::holder::Holder;
use crate::{Blob, FileMetadata, Inode, StorageError};
use realize_types::{Arena, Path, Peer};
use redb::ReadableTable;
use std::borrow::Cow;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::task;

/// File content, returned by [FileSystem::file_content].
///
/// The content that's returned is different depending on whether the
/// file is local or remote.
///
/// You may with [Filesystem::file_realm] first to know whether the file is
/// local or remote.
pub enum FileContent {
    Local(PathBuf),
    Remote(Blob),
}

impl FileContent {
    pub fn path(self) -> Option<PathBuf> {
        match self {
            FileContent::Local(path) => Some(path),
            _ => None,
        }
    }

    pub fn blob(self) -> Option<Blob> {
        match self {
            FileContent::Remote(blob) => Some(blob),
            _ => None,
        }
    }
}

/// A view on remote and local files.
pub struct Filesystem {
    allocator: Arc<PathIdAllocator>,
    by_arena: HashMap<Arena, Arc<ArenaFilesystem>>,

    /// An in-memory copy of PATH_TABLE.
    globals: HashMap<Inode, PathTableEntry>,
}

impl Filesystem {
    /// Create a new Filesystems with the database at the given path.
    pub(crate) async fn with_db<T>(
        db: Arc<GlobalDatabase>,
        all_fs: T,
    ) -> Result<Arc<Self>, anyhow::Error>
    where
        T: IntoIterator<Item = Arc<ArenaFilesystem>> + Send + 'static,
    {
        task::spawn_blocking(move || {
            let allocator = PathIdAllocator::setup(&db)?;

            let mut by_arena = HashMap::new();
            let mut globals = HashMap::new();
            let txn = db.begin_write()?;
            {
                let mut path_table = txn.path_table()?;

                if path_table.get(Inode::ROOT)?.is_none() {
                    path_table.insert(Inode::ROOT, Holder::with_content(PathTableEntry::new())?)?;
                }

                for fs in all_fs.into_iter() {
                    let arena = fs.arena();
                    for existing in by_arena.keys() {
                        check_arena_compatibility(arena, *existing)?;
                    }
                    let prefix = allocator.allocate_prefix(&txn, arena)?;
                    add_arena_path(arena, prefix, &txn, &mut path_table)?;
                    by_arena.insert(arena, fs);
                }

                for val in path_table.iter()? {
                    let (inode, entry) = val?;
                    globals.insert(inode.value(), entry.value().parse()?);
                }
            }
            txn.commit()?;

            Ok::<_, anyhow::Error>(Arc::new(Self {
                allocator,
                by_arena,
                globals,
            }))
        })
        .await?
    }

    /// Lists arenas available in this database
    pub fn arenas(&self) -> impl Iterator<Item = Arena> {
        self.by_arena.keys().map(|a| *a)
    }

    /// Returns the prefix of an arena.
    ///
    /// Will return [StorageError::UnknownArena] unless the arena
    /// is available.
    fn prefix(&self, arena: Arena) -> Result<PathIdPrefix, StorageError> {
        self.allocator
            .prefix(arena)
            .ok_or_else(|| StorageError::UnknownArena(arena))
    }

    /// Returns the FS for the given arena or fail.
    fn arena_fs(&self, arena: Arena) -> Result<&ArenaFilesystem, StorageError> {
        Ok(self.by_arena.get(&arena).ok_or(StorageError::NotFound)?)
    }

    /// Returns the FS for the given prefix or fail.
    fn arena_fs_for_prefix(&self, prefix: PathIdPrefix) -> Result<&ArenaFilesystem, StorageError> {
        let arena = self
            .allocator
            .arena_for_prefix(prefix)?
            .ok_or(StorageError::NotFound)?;
        self.arena_fs(arena)
    }

    fn arena_for_prefix(&self, prefix: PathIdPrefix) -> Result<Option<Arena>, StorageError> {
        self.allocator.arena_for_prefix(prefix)
    }

    /// Convert a [GlobalTreeLoc] into an arena or global location.
    fn resolve_loc<L: Into<FsLoc>>(&self, loc: L) -> Result<ResolvedLoc<'_>, StorageError> {
        Ok(match self.resolve_arena_root(loc.into()) {
            FsLoc::Inode(inode) => {
                let prefix = inode.prefix();
                match self.arena_for_prefix(prefix)? {
                    Some(arena) => {
                        ResolvedLoc::InArena(arena, prefix, ArenaFsLoc::Inode(inode.partial()))
                    }
                    None => ResolvedLoc::Global(self.globals.get(&inode).map(|e| (inode, e))),
                }
            }
            FsLoc::InodeAndName(inode, name) => {
                let prefix = inode.prefix();
                match self.arena_for_prefix(prefix)? {
                    Some(arena) => ResolvedLoc::InArena(
                        arena,
                        prefix,
                        ArenaFsLoc::InodeAndName(inode.partial(), name),
                    ),
                    None => {
                        if let Some(child_inode) = self
                            .globals
                            .get(&inode)
                            .and_then(|entry| entry.subdirs.get(&name))
                        {
                            ResolvedLoc::Global(
                                self.globals.get(child_inode).map(|e| (*child_inode, e)),
                            )
                        } else {
                            ResolvedLoc::Global(None)
                        }
                    }
                }
            }
            FsLoc::Path(arena, path) => {
                ResolvedLoc::InArena(arena, self.prefix(arena)?, ArenaFsLoc::Path(path))
            }
        })
    }

    /// Convert a [GlobalTreeLoc] into an arena and arena [TreeLoc] or fail.
    fn resolve_arena_loc<L: Into<FsLoc>>(
        &self,
        loc: L,
    ) -> Result<(&ArenaFilesystem, PathIdPrefix, ArenaFsLoc), StorageError> {
        Ok(match self.resolve_arena_root(loc.into()) {
            FsLoc::Inode(inode) => {
                let prefix = inode.prefix();

                (
                    self.arena_fs_for_prefix(prefix)?,
                    prefix,
                    ArenaFsLoc::Inode(inode.partial()),
                )
            }
            FsLoc::InodeAndName(inode, name) => {
                let prefix = inode.prefix();
                (
                    self.arena_fs_for_prefix(prefix)?,
                    prefix,
                    ArenaFsLoc::InodeAndName(inode.partial(), name.into()),
                )
            }
            FsLoc::Path(arena, path) => (
                self.arena_fs(arena)?,
                self.prefix(arena)?,
                ArenaFsLoc::Path(path),
            ),
        })
    }

    /// Cover the special case of a InodeAndName where name points to an arena root.
    fn resolve_arena_root(&self, loc: FsLoc) -> FsLoc {
        if let FsLoc::InodeAndName(inode, name) = &loc {
            if let Some(PathTableEntry { subdirs, .. }) = self.globals.get(&inode) {
                if let Some(inode) = subdirs.get(name) {
                    if inode.is_partial_root() {
                        return FsLoc::Inode(*inode);
                    }
                }
            }
        }

        loc
    }

    /// Lookup a directory entry.
    pub async fn lookup<L: Into<FsLoc>>(
        self: &Arc<Self>,
        loc: L,
    ) -> Result<(Inode, crate::arena::types::Metadata), StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || match this.resolve_loc(loc)? {
            ResolvedLoc::InArena(arena, prefix, loc) => {
                let fs = this.arena_fs(arena)?;
                let (inode, metadata) = fs.lookup(loc)?;

                Ok((inode.with(prefix), metadata))
            }
            ResolvedLoc::Global(None) => Err(StorageError::NotFound),
            ResolvedLoc::Global(Some((inode, entry))) => Ok((
                inode,
                crate::arena::types::Metadata::Dir(DirMetadata::readonly(entry.mtime)),
            )),
        })
        .await?
    }

    /// Return the mtime of the directory.
    pub async fn dir_metadata<L: Into<FsLoc>>(
        self: &Arc<Self>,
        loc: L,
    ) -> Result<DirMetadata, StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || match this.resolve_loc(loc)? {
            ResolvedLoc::InArena(arena, _, loc) => {
                let fs = this.arena_fs(arena)?;
                fs.dir_metadata(loc)
            }
            ResolvedLoc::Global(None) => Err(StorageError::NotFound),
            ResolvedLoc::Global(Some((_, entry))) => Ok(DirMetadata::readonly(entry.mtime)),
        })
        .await?
    }

    pub async fn readdir<L: Into<FsLoc>>(
        self: &Arc<Self>,
        loc: L,
    ) -> Result<Vec<(String, Inode, crate::arena::types::Metadata)>, StorageError> {
        let loc = loc.into();
        match self.resolve_loc(loc)? {
            ResolvedLoc::InArena(arena, prefix, loc) => {
                let this = Arc::clone(self);
                let vec = task::spawn_blocking(move || {
                    let fs = this.arena_fs(arena)?;
                    fs.readdir(loc)
                })
                .await??;
                Ok(vec
                    .into_iter()
                    .map(|(n, inode, m)| (n, inode.with(prefix), m))
                    .collect())
            }
            ResolvedLoc::Global(None) => Err(StorageError::NotFound),
            ResolvedLoc::Global(Some((_, entry))) => {
                let mut res = vec![];
                for (name, inode) in entry.subdirs.iter() {
                    res.push((
                        name.to_string(),
                        *inode,
                        crate::arena::types::Metadata::Dir(self.dir_metadata(*inode).await?),
                    ));
                }

                Ok(res)
            }
        }
    }

    pub async fn update(
        self: &Arc<Self>,
        peer: Peer,
        notification: Notification,
    ) -> Result<(), StorageError> {
        let this = Arc::clone(self);
        task::spawn_blocking(move || {
            let fs = this.arena_fs(notification.arena())?;
            fs.update(peer, notification)
        })
        .await?
    }
    /// Return a [Progress] instance that represents how up-to-date
    /// the information in the database is for that peer and arena.
    ///
    /// This should be passed to the peer when subscribing.
    pub async fn peer_progress(
        self: &Arc<Self>,
        peer: Peer,
        arena: Arena,
    ) -> Result<Option<Progress>, StorageError> {
        let this = Arc::clone(self);
        task::spawn_blocking(move || {
            let fs = this.arena_fs(arena)?;
            fs.peer_progress(peer)
        })
        .await?
    }

    /// Specifies the type of file (local or remote) and its cache status.
    pub async fn file_realm<L: Into<FsLoc>>(
        self: &Arc<Self>,
        loc: L,
    ) -> Result<FileRealm, StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let (fs, _, loc) = this.resolve_arena_loc(loc)?;
            fs.file_realm(loc)
        })
        .await?
    }

    /// Get hold of the file content.
    ///
    /// For local files, this returns the path of the local file that can then be accessed normally.
    ///
    /// For remote files, this returns a [Blob]. The returned Blob is
    /// available for reading. However, reading outside the range of
    /// data that is locally available causes [crate::BlobIncomplete]
    /// error.
    ///
    /// Blobs are usually used through the `Downloader`, which can
    /// download incomplete portions of the file.
    pub async fn file_content<L: Into<FsLoc>>(
        self: &Arc<Self>,
        loc: L,
    ) -> Result<FileContent, StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let (fs, _, loc) = this.resolve_arena_loc(loc)?;
            fs.file_content(loc)
        })
        .await?
    }

    pub async fn file_metadata<L: Into<FsLoc>>(
        self: &Arc<Self>,
        loc: L,
    ) -> Result<FileMetadata, StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let (fs, _, loc) = this.resolve_arena_loc(loc)?;
            fs.file_metadata(loc)
        })
        .await?
    }

    pub async fn metadata<L: Into<FsLoc>>(
        self: &Arc<Self>,
        loc: L,
    ) -> Result<crate::arena::types::Metadata, StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || match this.resolve_loc(loc)? {
            ResolvedLoc::InArena(arena, _, loc) => {
                let fs = this.arena_fs(arena)?;
                fs.metadata(loc)
            }
            ResolvedLoc::Global(None) => Err(StorageError::NotFound),
            ResolvedLoc::Global(Some((_, entry))) => Ok(crate::arena::types::Metadata::Dir(
                DirMetadata::readonly(entry.mtime),
            )),
        })
        .await?
    }

    pub async fn list_xattrs<L: Into<FsLoc>>(
        self: &Arc<Self>,
        loc: L,
    ) -> Result<Vec<&'static str>, StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);
        task::spawn_blocking(move || match this.resolve_loc(loc)? {
            ResolvedLoc::InArena(arena, _, loc) => {
                let fs = this.arena_fs(arena)?;
                fs.list_xattrs(loc)
            }
            ResolvedLoc::Global(None) => Err(StorageError::NotFound),
            ResolvedLoc::Global(Some(_)) => Ok(vec![]),
        })
        .await?
    }

    pub async fn get_xattr<L: Into<FsLoc>>(
        self: &Arc<Self>,
        loc: L,
        xattr: &str,
    ) -> Result<String, StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);
        let xattr = xattr.to_string();
        task::spawn_blocking(move || match this.resolve_loc(loc)? {
            ResolvedLoc::InArena(arena, _, loc) => {
                let fs = this.arena_fs(arena)?;
                fs.get_xattr(loc, &xattr)
            }
            ResolvedLoc::Global(None) => Err(StorageError::NotFound),
            ResolvedLoc::Global(Some(_)) => Err(StorageError::NoSuchAttribute),
        })
        .await?
    }

    pub async fn set_xattr<L: Into<FsLoc>>(
        self: &Arc<Self>,
        loc: L,
        xattr: &str,
        value: Cow<'_, str>,
    ) -> Result<(), StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);
        let xattr = xattr.to_string();
        let value = value.into_owned();
        task::spawn_blocking(move || match this.resolve_loc(loc)? {
            ResolvedLoc::InArena(arena, _, loc) => {
                let fs = this.arena_fs(arena)?;
                fs.set_xattr(loc, &xattr, value.into())
            }
            ResolvedLoc::Global(None) => Err(StorageError::NotFound),
            ResolvedLoc::Global(Some(_)) => Err(StorageError::NoSuchAttribute),
        })
        .await?
    }

    pub async fn unlink<L: Into<FsLoc>>(self: &Arc<Self>, loc: L) -> Result<(), StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || match this.resolve_loc(loc)? {
            ResolvedLoc::InArena(arena, _, loc) => {
                let fs = this.arena_fs(arena)?;
                fs.unlink(loc)
            }
            ResolvedLoc::Global(None) => Err(StorageError::NotFound),
            ResolvedLoc::Global(Some(_)) => Err(StorageError::IsADirectory),
        })
        .await?
    }

    pub async fn branch<L1: Into<FsLoc>, L2: Into<FsLoc>>(
        self: &Arc<Self>,
        source: L1,
        dest: L2,
    ) -> Result<(Inode, FileMetadata), StorageError> {
        let source = source.into();
        let dest = dest.into();
        let this = Arc::clone(self);

        task::spawn_blocking(
            move || match (this.resolve_loc(source)?, this.resolve_loc(dest)?) {
                (
                    ResolvedLoc::InArena(source_arena, prefix, source),
                    ResolvedLoc::InArena(dest_arena, _, dest),
                ) => {
                    if source_arena != dest_arena {
                        return Err(StorageError::CrossesDevices);
                    }
                    let fs = this.arena_fs(source_arena)?;
                    let (inode, m) = fs.branch(source, dest)?;

                    Ok((inode.with(prefix), m))
                }
                (ResolvedLoc::Global(_), ResolvedLoc::Global(_)) => Err(StorageError::IsADirectory),
                (_, _) => Err(StorageError::CrossesDevices),
            },
        )
        .await?
    }

    pub async fn rename<L1: Into<FsLoc>, L2: Into<FsLoc>>(
        self: &Arc<Self>,
        source: L1,
        dest: L2,
        noreplace: bool,
    ) -> Result<(), StorageError> {
        let source = source.into();
        let dest = dest.into();
        let this = Arc::clone(self);

        task::spawn_blocking(
            move || match (this.resolve_loc(source)?, this.resolve_loc(dest)?) {
                (
                    ResolvedLoc::InArena(source_arena, _, source),
                    ResolvedLoc::InArena(dest_arena, _, dest),
                ) => {
                    if source_arena != dest_arena {
                        return Err(StorageError::CrossesDevices);
                    }
                    let fs = this.arena_fs(source_arena)?;
                    fs.rename(source, dest, noreplace)
                }
                (ResolvedLoc::Global(_), ResolvedLoc::Global(_)) => Err(StorageError::IsADirectory),
                (_, _) => Err(StorageError::CrossesDevices),
            },
        )
        .await?
    }

    /// Create a directory at the given path in the specified arena.
    pub async fn mkdir<L: Into<FsLoc>>(
        self: &Arc<Self>,
        loc: L,
    ) -> Result<(Inode, DirMetadata), StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let (fs, prefix, loc) = this.resolve_arena_loc(loc)?;

            let (inode, m) = fs.mkdir(loc)?;

            Ok((inode.with(prefix), m))
        })
        .await?
    }

    /// Remove an empty directory at the given path in the specified arena.
    pub async fn rmdir<L: Into<FsLoc>>(self: &Arc<Self>, loc: L) -> Result<(), StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let (fs, _, loc) = this.resolve_arena_loc(loc)?;

            fs.rmdir(loc)
        })
        .await?
    }

    /// Create a file at the given path in the specified arena with the given options.
    pub async fn create<L: Into<FsLoc>>(
        self: &Arc<Self>,
        options: tokio::fs::OpenOptions,
        loc: L,
    ) -> Result<(Inode, tokio::fs::File), StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let (fs, prefix, loc) = this.resolve_arena_loc(loc)?;

            let (inode, file) = fs.create(options, loc)?;

            Ok((inode.with(prefix), file))
        })
        .await?
    }
}

/// A location within the Filesystem.
///
/// This is usually a [Path] within an [Arena] or a [PathId], but can
/// also be a [PathId] and a name to specify a child of a known
/// directory.
pub enum FsLoc {
    Inode(Inode),
    Path(Arena, Path),
    InodeAndName(Inode, String),
}

impl From<Inode> for FsLoc {
    fn from(value: Inode) -> Self {
        FsLoc::Inode(value)
    }
}

impl From<(Arena, Path)> for FsLoc {
    fn from(value: (Arena, Path)) -> Self {
        FsLoc::Path(value.0, value.1)
    }
}

impl From<(Arena, &Path)> for FsLoc {
    fn from(value: (Arena, &Path)) -> Self {
        FsLoc::Path(value.0, value.1.clone())
    }
}

impl From<(Inode, &str)> for FsLoc {
    fn from(value: (Inode, &str)) -> Self {
        FsLoc::InodeAndName(value.0, value.1.to_string())
    }
}

impl From<(Inode, &String)> for FsLoc {
    fn from(value: (Inode, &String)) -> Self {
        FsLoc::InodeAndName(value.0, value.1.clone())
    }
}
impl From<(Inode, String)> for FsLoc {
    fn from(value: (Inode, String)) -> Self {
        FsLoc::InodeAndName(value.0, value.1)
    }
}

enum ResolvedLoc<'a> {
    Global(Option<(Inode, &'a PathTableEntry)>),
    InArena(Arena, PathIdPrefix, ArenaFsLoc),
}

fn check_arena_compatibility(arena: Arena, existing: Arena) -> anyhow::Result<()> {
    fn is_path_prefix(prefix: &str, arena: &str) -> bool {
        if let Some(rest) = arena.strip_prefix(prefix) {
            rest.starts_with("/")
        } else {
            false
        }
    }
    if is_path_prefix(arena.as_str(), existing.as_str())
        || is_path_prefix(existing.as_str(), arena.as_str())
    {
        return Err(anyhow::anyhow!(
            "arena {arena} incompatible with existing arena {existing}"
        ));
    }

    Ok(())
}

/// Register root of the given arena in the PATH_TABLE.
fn add_arena_path(
    arena: Arena,
    prefix: PathIdPrefix,
    txn: &GlobalWriteTransaction,
    path_table: &mut redb::Table<Inode, Holder<'static, PathTableEntry>>,
) -> Result<(), StorageError> {
    let arena_path = Path::parse(arena.as_str())?;
    let mut current = Inode::ROOT;
    let mut current_entry = get_or_create_dir(path_table, current)?;

    // Create intermediate directories, if necessary
    if let Some(parent) = arena_path.parent() {
        for dirname in parent.components() {
            match current_entry.subdirs.get(dirname) {
                Some(inode) => {
                    if inode.is_partial_root() {
                        return Err(StorageError::AlreadyExists);
                    }
                    current = *inode;
                    current_entry = get_or_create_dir(path_table, current)?;
                }
                None => {
                    let subdir = pathid_allocator::allocal_global_inode(txn)?;
                    current_entry.subdirs.insert(dirname.to_string(), subdir);
                    path_table.insert(current, Holder::new(&current_entry)?)?;

                    current = subdir;
                    current_entry = get_or_create_dir(path_table, current)?;
                }
            }
        }
    }

    // Add entry for arena if necessary
    let name = arena_path.name();
    let arena_root = PartialInode::ROOT.with(prefix);
    match current_entry.subdirs.get(name) {
        None => {
            current_entry.subdirs.insert(name.to_string(), arena_root);
            path_table.insert(current, Holder::new(&current_entry)?)?;
        }
        Some(inode) => {
            if *inode != arena_root {
                return Err(StorageError::AlreadyExists);
            }
        }
    }

    Ok(())
}

fn get_or_create_dir(
    path_table: &mut redb::Table<Inode, Holder<'static, PathTableEntry>>,
    inode: Inode,
) -> Result<PathTableEntry, StorageError> {
    if let Some(existing) = path_table.get(inode)? {
        return Ok(existing.value().parse()?);
    }
    let entry = PathTableEntry::new();
    path_table.insert(inode, Holder::new(&entry)?)?;

    Ok(entry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::redb_utils;
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use realize_types::Arena;
    use realize_types::UnixTime;

    fn test_arena() -> Arena {
        Arena::from("test_arena")
    }

    struct Fixture {
        fs: Arc<Filesystem>,
        _tempdir: TempDir,
    }
    impl Fixture {
        async fn setup_with_arena(arena: Arena) -> anyhow::Result<Self> {
            Self::setup_with_arenas([arena]).await
        }

        async fn setup_with_arenas<T>(arenas: T) -> anyhow::Result<Self>
        where
            T: IntoIterator<Item = Arena>,
        {
            let _ = env_logger::try_init();
            let tempdir = TempDir::new()?;

            let arenas = arenas.into_iter().collect::<Vec<_>>();
            let mut arena_fs = vec![];
            for arena in arenas {
                let blob_dir = tempdir.child(format!("{arena}/blobs"));
                blob_dir.create_dir_all()?;
                let datadir = tempdir.child(format!("{arena}/data"));
                datadir.create_dir_all()?;
                arena_fs.push(ArenaFilesystem::for_testing(
                    arena,
                    blob_dir.path(),
                    datadir.path(),
                )?);
            }
            let db = GlobalDatabase::new(redb_utils::in_memory()?)?;
            let fs = Filesystem::with_db(db, arena_fs).await?;

            Ok(Self {
                fs,
                _tempdir: tempdir,
            })
        }
    }

    #[tokio::test]
    async fn empty_fs_readdir() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arenas([]).await?;

        assert!(fixture.fs.readdir(Inode::ROOT).await?.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn empty_fs_metadata() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arenas([]).await?;

        let m = fixture.fs.dir_metadata(Inode::ROOT).await?;
        assert_eq!(0o555, m.mode);
        assert_ne!(UnixTime::ZERO, m.mtime);

        Ok(())
    }

    #[tokio::test]
    async fn initial_dir_mtime() -> anyhow::Result<()> {
        let arena = Arena::from("documents/letters");
        let fixture = Fixture::setup_with_arena(arena).await?;

        let root_m = fixture.fs.dir_metadata(Inode::ROOT).await?;
        assert_eq!(0o555, root_m.mode);
        assert_ne!(UnixTime::ZERO, root_m.mtime);

        let (documents, _) = fixture.fs.lookup((Inode::ROOT, "documents")).await?;
        let documents_m = fixture.fs.dir_metadata(documents).await?;
        assert_eq!(0o555, documents_m.mode);
        assert_ne!(UnixTime::ZERO, documents_m.mtime);

        let (letters, _) = fixture.fs.lookup((documents, "letters")).await?;
        let letters_m = fixture.fs.dir_metadata(letters).await?;
        assert_eq!(0o777, letters_m.mode); // arena root is writable
        assert_ne!(UnixTime::ZERO, letters_m.mtime);

        Ok(())
    }

    #[tokio::test]
    async fn lookup_finds_entry() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arenas([
            Arena::from("arenas/test1"),
            Arena::from("arenas/test2"),
            Arena::from("other"),
        ])
        .await?;

        let fs = &fixture.fs;

        let (arenas, metadata) = fs.lookup((Inode::ROOT, "arenas")).await.unwrap();
        assert!(matches!(metadata, crate::arena::types::Metadata::Dir(_)));

        let (_, metadata) = fs.lookup((Inode::ROOT, "other")).await.unwrap();
        assert!(matches!(metadata, crate::arena::types::Metadata::Dir(_)));

        let (_, metadata) = fs.lookup((arenas, "test1")).await.unwrap();
        assert!(matches!(metadata, crate::arena::types::Metadata::Dir(_)));

        let (_, metadata) = fs.lookup((arenas, "test2")).await.unwrap();
        assert!(matches!(metadata, crate::arena::types::Metadata::Dir(_)));

        Ok(())
    }

    #[tokio::test]
    async fn lookup_returns_notfound_for_missing_entry() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let fs = &fixture.fs;

        assert!(matches!(
            fs.lookup((Inode::ROOT, "nonexistent")).await,
            Err(StorageError::NotFound),
        ));

        Ok(())
    }

    #[tokio::test]
    async fn readdir_returns_arena_dirs() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arenas([
            Arena::from("arenas/test1"),
            Arena::from("arenas/test2"),
            Arena::from("other"),
        ])
        .await?;

        let fs = &fixture.fs;
        let entries = fs.readdir(Inode::ROOT).await?;
        assert_eq!(entries.len(), 2);

        let mut names: Vec<String> = entries.iter().map(|(name, _, _)| name.clone()).collect();
        names.sort();
        assert_eq!(names, vec!["arenas", "other"]);

        // Verify all entries are directories and read-only
        for (name, inode, metadata) in entries {
            match metadata {
                crate::arena::types::Metadata::Dir(dir_meta) => {
                    if inode.is_partial_root() {
                        // arena root is writable
                        assert_eq!(0o777, dir_meta.mode);
                    } else {
                        assert_eq!(0o555, dir_meta.mode);
                    }
                    assert_ne!(dir_meta.mtime, UnixTime::ZERO);
                }
                _ => panic!("Expected directory metadata for {}", name),
            }
        }

        let (arenas, _) = fs.lookup((Inode::ROOT, "arenas")).await?;
        let entries = fs.readdir(arenas).await?;
        assert_eq!(entries.len(), 2);

        let mut names: Vec<String> = entries.iter().map(|(name, _, _)| name.clone()).collect();
        names.sort();
        assert_eq!(names, vec!["test1", "test2"]);

        // Verify all entries are directories and read-only
        for (name, inode, metadata) in entries {
            match metadata {
                crate::arena::types::Metadata::Dir(dir_meta) => {
                    if inode.is_partial_root() {
                        // arena root is writable
                        assert_eq!(0o777, dir_meta.mode);
                    } else {
                        assert_eq!(0o555, dir_meta.mode);
                    }
                    assert_ne!(dir_meta.mtime, UnixTime::ZERO);
                }
                _ => panic!("Expected directory metadata for {}", name),
            }
        }

        assert!(
            fs.readdir((Arena::from("arenas/test1"), Path::root()))
                .await?
                .is_empty()
        );
        assert!(
            fs.readdir((Arena::from("arenas/test2"), Path::root()))
                .await?
                .is_empty()
        );
        assert!(
            fs.readdir((Arena::from("other"), Path::root()))
                .await?
                .is_empty()
        );

        Ok(())
    }

    #[tokio::test]
    async fn unlink() -> anyhow::Result<()> {
        let arena = Arena::from("arenas/1");
        let fixture = Fixture::setup_with_arena(arena).await?;
        let fs = &fixture.fs;

        let res = fs.unlink((Inode::ROOT, "doesnotexist")).await;
        assert!(matches!(res, Err(StorageError::NotFound)), "{res:?}");
        assert!(matches!(
            fs.unlink((Inode::ROOT, "arenas")).await,
            Err(StorageError::IsADirectory)
        ));
        let (arenas_inode, _) = fs.lookup((Inode::ROOT, "arenas")).await?;
        assert!(matches!(
            fs.unlink((arenas_inode, "1")).await,
            Err(StorageError::IsADirectory)
        ));

        // This just checks that the call is dispatched down to the
        // arena fs.
        assert!(matches!(
            fs.unlink((arena, Path::parse("doesnotexist")?)).await,
            Err(StorageError::NotFound)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn branch() -> anyhow::Result<()> {
        let arena1 = Arena::from("arenas/1");
        let arena2 = Arena::from("arenas/2");
        let fixture = Fixture::setup_with_arenas([arena1, arena2]).await?;
        let fs = &fixture.fs;

        let res = fs
            .branch((Inode::ROOT, "arenas"), (Inode::ROOT, "test_arena2"))
            .await;
        assert!(matches!(res, Err(StorageError::IsADirectory)), "{res:?}");

        assert!(matches!(
            fs.branch(
                (arena1, Path::root()),
                (arena2, Path::parse("test_arena2")?)
            )
            .await,
            Err(StorageError::CrossesDevices)
        ));

        Ok(())
    }
}
