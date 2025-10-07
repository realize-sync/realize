use super::db::{GlobalDatabase, GlobalReadTransaction};
use super::pathid_allocator::PathIdAllocator;
use crate::arena::fs::{ArenaFilesystem, ArenaFsLoc};
use crate::arena::notifier::{Notification, Progress};
use crate::arena::types::{DirMetadata, FileAlternative, FileRealm};
use crate::global::db::GlobalWriteTransaction;
use crate::global::types::PathTableEntry;
use crate::utils::holder::Holder;
use crate::{Blob, FileMetadata, Inode, Mark, PathId, StorageError};
use realize_types::{Arena, Path, Peer, UnixTime};
use redb::ReadableTable;
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
    db: Arc<GlobalDatabase>,
    allocator: Arc<PathIdAllocator>,

    by_arena: HashMap<Arena, Arc<ArenaFilesystem>>,

    /// PathIds to intermediate paths, before arenas, includes root.
    globals: HashMap<PathId, IntermediatePath>,
}

impl Filesystem {
    /// PathId of the root dir.
    pub const ROOT_DIR: PathId = PathIdAllocator::ROOT_INODE;

    /// Create a new Filesystems with the database at the given path.
    pub(crate) async fn with_db<T>(
        db: Arc<GlobalDatabase>,
        allocator: Arc<PathIdAllocator>,
        by_arena: T,
    ) -> Result<Arc<Self>, anyhow::Error>
    where
        T: IntoIterator<Item = Arc<ArenaFilesystem>> + Send + 'static,
    {
        task::spawn_blocking(move || {
            let mut map = HashMap::new();
            let mut globals = HashMap::new();
            let txn = db.begin_write()?;
            {
                let mut path_table = txn.path_table()?;

                // Make sure root is setup, even if there are no arenas.
                let root_mtime =
                    get_or_add_path_entry(&txn, &mut path_table, &allocator, "")?.mtime;
                globals.insert(
                    PathIdAllocator::ROOT_INODE,
                    IntermediatePath {
                        entries: HashMap::new(),
                        mtime: root_mtime,
                    },
                );

                for fs in by_arena.into_iter() {
                    register(
                        fs,
                        &txn,
                        &mut path_table,
                        &allocator,
                        &mut map,
                        &mut globals,
                    )?;
                }
            }
            txn.commit()?;

            Ok::<_, anyhow::Error>(Arc::new(Self {
                db,
                allocator,
                by_arena: map,
                globals,
            }))
        })
        .await?
    }

    /// Lists arenas available in this database
    pub fn arenas(&self) -> impl Iterator<Item = Arena> {
        self.by_arena.keys().map(|a| *a)
    }

    /// Returns the pathid of an arena.
    ///
    /// Will return [StorageError::UnknownArena] unless the arena
    /// is available.
    pub fn arena_root(&self, arena: Arena) -> Result<PathId, StorageError> {
        self.allocator
            .arena_root(arena)
            .ok_or_else(|| StorageError::UnknownArena(arena))
    }

    /// Returns the FS for the given arena or fail.
    fn arena_fs(&self, arena: Arena) -> Result<&ArenaFilesystem, StorageError> {
        Ok(self.by_arena.get(&arena).ok_or(StorageError::NotFound)?)
    }

    /// Returns the FS for the given pathid or fail.
    fn arena_fs_for_pathid(&self, pathid: PathId) -> Result<&ArenaFilesystem, StorageError> {
        let arena = self
            .allocator
            .arena_for_pathid(&self.db.begin_read()?, pathid)?
            .ok_or(StorageError::NotFound)?;
        self.arena_fs(arena)
    }

    fn arena_fs_for_inode(&self, inode: Inode) -> Result<&ArenaFilesystem, StorageError> {
        // We ignore the difference between PathId and Inodes for this
        // lookup, as they come from the same ranges.
        self.arena_fs_for_pathid(PathId(inode.as_u64()))
    }

    fn arena_for_inode(
        &self,
        txn: &GlobalReadTransaction,
        inode: Inode,
    ) -> Result<Option<Arena>, StorageError> {
        // We ignore the difference between PathId and Inodes for this
        // lookup, as they come from the same ranges.
        self.allocator.arena_for_pathid(txn, PathId(inode.as_u64()))
    }

    /// Convert a [GlobalTreeLoc] into an arena or global location.
    fn resolve_loc<L: Into<FsLoc>>(
        &self,
        txn: &GlobalReadTransaction,
        loc: L,
    ) -> Result<ResolvedLoc, StorageError> {
        Ok(match self.resolve_arena_root(loc.into()) {
            FsLoc::PathId(pathid) => match self.allocator.arena_for_pathid(txn, pathid)? {
                Some(arena) => ResolvedLoc::InArena(arena, ArenaFsLoc::PathId(pathid)),
                None => ResolvedLoc::Global(if self.globals.contains_key(&pathid) {
                    Some(pathid)
                } else {
                    None
                }),
            },
            FsLoc::Inode(inode) => {
                match self.arena_for_inode(txn, inode)? {
                    Some(arena) => ResolvedLoc::InArena(arena, ArenaFsLoc::Inode(inode)),
                    None => {
                        // global inodes and pathids are identical
                        let pathid = PathId(inode.as_u64());

                        ResolvedLoc::Global(if self.globals.contains_key(&pathid) {
                            Some(pathid)
                        } else {
                            None
                        })
                    }
                }
            }
            FsLoc::PathIdAndName(pathid, name) => {
                match self.allocator.arena_for_pathid(txn, pathid)? {
                    Some(arena) => {
                        ResolvedLoc::InArena(arena, ArenaFsLoc::PathIdAndName(pathid, name))
                    }
                    None => ResolvedLoc::Global(match self.globals.get(&pathid) {
                        None => return Err(StorageError::NotFound),
                        Some(IntermediatePath { entries, .. }) => {
                            entries.get(&name).map(|pathid| *pathid)
                        }
                    }),
                }
            }
            FsLoc::InodeAndName(inode, name) => {
                match self.arena_for_inode(txn, inode)? {
                    Some(arena) => {
                        ResolvedLoc::InArena(arena, ArenaFsLoc::InodeAndName(inode, name))
                    }
                    None => {
                        // global inodes and pathids are identical
                        let pathid = PathId(inode.as_u64());

                        ResolvedLoc::Global(match self.globals.get(&pathid) {
                            None => return Err(StorageError::NotFound),
                            Some(IntermediatePath { entries, .. }) => {
                                entries.get(&name).map(|pathid| *pathid)
                            }
                        })
                    }
                }
            }
            FsLoc::Path(arena, path) => ResolvedLoc::InArena(arena, ArenaFsLoc::Path(path)),
        })
    }

    /// Convert a [GlobalTreeLoc] into an arena and arena [TreeLoc] or fail.
    fn resolve_arena_loc<L: Into<FsLoc>>(
        &self,
        loc: L,
    ) -> Result<(&ArenaFilesystem, ArenaFsLoc), StorageError> {
        Ok(match self.resolve_arena_root(loc.into()) {
            FsLoc::PathId(pathid) => (
                self.arena_fs_for_pathid(pathid)?,
                ArenaFsLoc::PathId(pathid),
            ),
            FsLoc::Inode(inode) => (self.arena_fs_for_inode(inode)?, ArenaFsLoc::Inode(inode)),
            FsLoc::PathIdAndName(pathid, name) => (
                self.arena_fs_for_pathid(pathid)?,
                ArenaFsLoc::PathIdAndName(pathid, name.into()),
            ),
            FsLoc::InodeAndName(inode, name) => (
                self.arena_fs_for_inode(inode)?,
                ArenaFsLoc::InodeAndName(inode, name.into()),
            ),
            FsLoc::Path(arena, path) => (self.arena_fs(arena)?, ArenaFsLoc::Path(path)),
        })
    }

    /// Cover the special case of a PathIdAndName where name points to an arena root.
    fn resolve_arena_root(&self, loc: FsLoc) -> FsLoc {
        if let Some((pathid, name)) = match &loc {
            FsLoc::PathIdAndName(pathid, name) => Some((*pathid, name)),
            FsLoc::InodeAndName(inode, name) => Some((PathId(inode.as_u64()), name)),
            _ => None,
        } {
            if let Some(IntermediatePath { entries, .. }) = self.globals.get(&pathid) {
                if let Some(pathid) = entries.get(name) {
                    if self.allocator.is_arena_root(*pathid) {
                        return FsLoc::PathId(*pathid);
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

        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            match this.resolve_loc(&txn, loc)? {
                ResolvedLoc::InArena(arena, loc) => {
                    let fs = this.arena_fs(arena)?;
                    fs.lookup(loc)
                }
                ResolvedLoc::Global(pathid) => {
                    if let Some(pathid) = pathid
                        && this.globals.contains_key(&pathid)
                    {
                        // For global directories, construct DirMetadata
                        if let Some(IntermediatePath { mtime, .. }) = this.globals.get(&pathid) {
                            Ok((
                                Inode(pathid.as_u64()),
                                crate::arena::types::Metadata::Dir(DirMetadata::readonly(*mtime)),
                            ))
                        } else {
                            Err(StorageError::NotFound)
                        }
                    } else {
                        Err(StorageError::NotFound)
                    }
                }
            }
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

        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            match this.resolve_loc(&txn, loc)? {
                ResolvedLoc::InArena(arena, loc) => {
                    let fs = this.arena_fs(arena)?;
                    fs.dir_metadata(loc)
                }
                ResolvedLoc::Global(None) => Err(StorageError::NotFound),
                ResolvedLoc::Global(Some(pathid)) => match this.globals.get(&pathid) {
                    None => Err(StorageError::NotFound),
                    Some(IntermediatePath { mtime, .. }) => Ok(DirMetadata::readonly(*mtime)),
                },
            }
        })
        .await?
    }

    pub async fn readdir<L: Into<FsLoc>>(
        self: &Arc<Self>,
        loc: L,
    ) -> Result<Vec<(String, Inode, crate::arena::types::Metadata)>, StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            match this.resolve_loc(&txn, loc)? {
                ResolvedLoc::InArena(arena, loc) => {
                    let fs = this.arena_fs(arena)?;
                    fs.readdir(loc)
                }
                ResolvedLoc::Global(None) => Err(StorageError::NotFound),
                ResolvedLoc::Global(Some(pathid)) => match this.globals.get(&pathid) {
                    None => Err(StorageError::NotFound),
                    Some(IntermediatePath { entries, mtime, .. }) => Ok(entries
                        .iter()
                        .map(|(name, pathid)| {
                            (
                                name.to_string(),
                                // global inodes and pathids map 1:1
                                Inode(pathid.as_u64()),
                                crate::arena::types::Metadata::Dir(DirMetadata::readonly(*mtime)),
                            )
                        })
                        .collect()),
                },
            }
        })
        .await?
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
            let (fs, loc) = this.resolve_arena_loc(loc)?;
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
            let (fs, loc) = this.resolve_arena_loc(loc)?;
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
            let (fs, loc) = this.resolve_arena_loc(loc)?;
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

        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            match this.resolve_loc(&txn, loc)? {
                ResolvedLoc::InArena(arena, loc) => {
                    let fs = this.arena_fs(arena)?;
                    fs.metadata(loc)
                }
                ResolvedLoc::Global(Some(pathid)) => {
                    // For global directories, we need to construct DirMetadata
                    if let Some(IntermediatePath { mtime, .. }) = this.globals.get(&pathid) {
                        Ok(crate::arena::types::Metadata::Dir(DirMetadata::readonly(
                            *mtime,
                        )))
                    } else {
                        Err(StorageError::NotFound)
                    }
                }
                ResolvedLoc::Global(None) => Err(StorageError::NotFound),
            }
        })
        .await?
    }

    pub async fn unlink<L: Into<FsLoc>>(self: &Arc<Self>, loc: L) -> Result<(), StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            match this.resolve_loc(&txn, loc)? {
                ResolvedLoc::InArena(arena, loc) => {
                    let fs = this.arena_fs(arena)?;
                    fs.unlink(loc)
                }
                ResolvedLoc::Global(Some(_)) => Err(StorageError::IsADirectory),
                ResolvedLoc::Global(None) => Err(StorageError::NotFound),
            }
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

        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            match (
                this.resolve_loc(&txn, source)?,
                this.resolve_loc(&txn, dest)?,
            ) {
                (
                    ResolvedLoc::InArena(source_arena, source),
                    ResolvedLoc::InArena(dest_arena, dest),
                ) => {
                    if source_arena != dest_arena {
                        return Err(StorageError::CrossesDevices);
                    }
                    let fs = this.arena_fs(source_arena)?;
                    fs.branch(source, dest)
                }
                (ResolvedLoc::Global(_), ResolvedLoc::Global(_)) => Err(StorageError::IsADirectory),
                (_, _) => Err(StorageError::CrossesDevices),
            }
        })
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

        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            match (
                this.resolve_loc(&txn, source)?,
                this.resolve_loc(&txn, dest)?,
            ) {
                (
                    ResolvedLoc::InArena(source_arena, source),
                    ResolvedLoc::InArena(dest_arena, dest),
                ) => {
                    if source_arena != dest_arena {
                        return Err(StorageError::CrossesDevices);
                    }
                    let fs = this.arena_fs(source_arena)?;
                    fs.rename(source, dest, noreplace)
                }
                (ResolvedLoc::Global(_), ResolvedLoc::Global(_)) => Err(StorageError::IsADirectory),
                (_, _) => Err(StorageError::CrossesDevices),
            }
        })
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
            let (fs, loc) = this.resolve_arena_loc(loc)?;

            fs.mkdir(loc)
        })
        .await?
    }

    /// Remove an empty directory at the given path in the specified arena.
    pub async fn rmdir<L: Into<FsLoc>>(self: &Arc<Self>, loc: L) -> Result<(), StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let (fs, loc) = this.resolve_arena_loc(loc)?;

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
            let (fs, loc) = this.resolve_arena_loc(loc)?;

            fs.create(options, loc)
        })
        .await?
    }

    /// Return the mark value and whether it was set directly on this inode
    pub async fn get_mark<L: Into<FsLoc>>(
        self: &Arc<Self>,
        loc: L,
    ) -> Result<(Mark, bool), StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let (fs, loc) = this.resolve_arena_loc(loc)?;

            fs.get_mark(loc)
        })
        .await?
    }

    /// Set a mark on the specified location
    pub async fn set_mark<L: Into<FsLoc>>(
        self: &Arc<Self>,
        loc: L,
        mark: Mark,
    ) -> Result<(), StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let (fs, loc) = this.resolve_arena_loc(loc)?;

            fs.set_mark(loc, mark)
        })
        .await?
    }

    /// Clear a mark from the specified location
    pub async fn clear_mark<L: Into<FsLoc>>(self: &Arc<Self>, loc: L) -> Result<(), StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let (fs, loc) = this.resolve_arena_loc(loc)?;

            fs.clear_mark(loc)
        })
        .await?
    }

    pub async fn list_alternatives<L: Into<FsLoc>>(
        self: &Arc<Self>,
        loc: L,
    ) -> Result<Vec<FileAlternative>, StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let (fs, loc) = this.resolve_arena_loc(loc)?;

            fs.list_alternatives(loc)
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
    PathId(PathId),
    Inode(Inode),
    Path(Arena, Path),
    PathIdAndName(PathId, String),
    InodeAndName(Inode, String),
}

impl From<PathId> for FsLoc {
    fn from(value: PathId) -> Self {
        FsLoc::PathId(value)
    }
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

impl From<(PathId, &str)> for FsLoc {
    fn from(value: (PathId, &str)) -> Self {
        FsLoc::PathIdAndName(value.0, value.1.to_string())
    }
}

impl From<(PathId, &String)> for FsLoc {
    fn from(value: (PathId, &String)) -> Self {
        FsLoc::PathIdAndName(value.0, value.1.clone())
    }
}
impl From<(PathId, String)> for FsLoc {
    fn from(value: (PathId, String)) -> Self {
        FsLoc::PathIdAndName(value.0, value.1)
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

enum ResolvedLoc {
    Global(Option<PathId>),
    InArena(Arena, ArenaFsLoc),
}

#[derive(Debug, Clone)]
struct IntermediatePath {
    entries: HashMap<String, PathId>,
    mtime: UnixTime,
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

/// Register an [ArenaFilesystem] that handles calls for a specific arena.
///
/// Calls for pathid assigned to the arena will be directed there.
fn register(
    fs: Arc<ArenaFilesystem>,
    txn: &GlobalWriteTransaction,
    path_table: &mut redb::Table<&'static str, Holder<'static, PathTableEntry>>,
    allocator: &Arc<PathIdAllocator>,
    map: &mut HashMap<Arena, Arc<ArenaFilesystem>>,
    paths: &mut HashMap<PathId, IntermediatePath>,
) -> anyhow::Result<()> {
    let arena = fs.arena();
    let arena_root = allocator
        .arena_root(arena)
        .ok_or_else(|| StorageError::UnknownArena(arena))?;

    for existing in map.keys().map(|a| *a) {
        check_arena_compatibility(arena, existing)?;
    }
    add_arena_root(arena, arena_root, txn, path_table, allocator, paths)?;
    map.insert(fs.arena(), fs);

    Ok(())
}

fn add_arena_root(
    arena: Arena,
    arena_root: PathId,
    txn: &GlobalWriteTransaction,
    path_table: &mut redb::Table<&'static str, Holder<'static, PathTableEntry>>,
    allocator: &Arc<PathIdAllocator>,
    paths: &mut HashMap<PathId, IntermediatePath>,
) -> anyhow::Result<()> {
    let arena_path = Path::parse(arena.as_str())?;
    let mut names = Path::components(Some(&arena_path))
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .chain(/* root */ std::iter::once(""));
    let mut current_pathid = arena_root;
    let mut current_name = names.next().unwrap();
    for dirname in names {
        let entry = get_or_add_path_entry(txn, path_table, allocator, dirname)?;
        add_intermediate_path_entry(
            entry.pathid,
            entry.mtime,
            current_name,
            current_pathid,
            paths,
        );
        current_name = dirname;
        current_pathid = entry.pathid;
    }

    Ok(())
}

fn get_or_add_path_entry(
    txn: &GlobalWriteTransaction,
    path_table: &mut redb::Table<'_, &'static str, Holder<'static, PathTableEntry>>,
    allocator: &Arc<PathIdAllocator>,
    dirname: &str,
) -> Result<PathTableEntry, anyhow::Error> {
    let entry = if let Some(e) = path_table.get(dirname)? {
        e.value().parse()?
    } else {
        let entry_pathid = if dirname == "" {
            PathIdAllocator::ROOT_INODE
        } else {
            allocator.allocate_global_pathid(&txn)?
        };
        let entry = PathTableEntry {
            pathid: entry_pathid,
            mtime: UnixTime::now(),
        };
        path_table.insert(dirname, Holder::new(&entry)?)?;

        entry
    };
    Ok(entry)
}

/// Adds an entry in the intermediate path with the given pathid.
///
/// If no such intermediate path exists, it is added.
fn add_intermediate_path_entry(
    pathid: PathId,
    mtime: UnixTime,
    entry_name: &str,
    entry_pathid: PathId,
    paths: &mut HashMap<PathId, IntermediatePath>,
) {
    let mapping = paths.entry(pathid).or_insert_with(|| IntermediatePath {
        entries: HashMap::new(),
        mtime,
    });
    mapping.entries.insert(entry_name.to_string(), entry_pathid);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::redb_utils;
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use realize_types::Arena;

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
            let db = GlobalDatabase::new(redb_utils::in_memory()?)?;
            let allocator = PathIdAllocator::new(Arc::clone(&db), arenas.clone())?;
            let mut arena_fs = vec![];
            for arena in arenas {
                let blob_dir = tempdir.child(format!("{arena}/blobs"));
                blob_dir.create_dir_all()?;
                let datadir = tempdir.child(format!("{arena}/data"));
                datadir.create_dir_all()?;
                arena_fs.push(ArenaFilesystem::for_testing(
                    arena,
                    Arc::clone(&allocator),
                    blob_dir.path(),
                    datadir.path(),
                )?);
            }
            let fs = Filesystem::with_db(Arc::clone(&db), Arc::clone(&allocator), arena_fs).await?;

            Ok(Self {
                fs,
                _tempdir: tempdir,
            })
        }
    }

    #[tokio::test]
    async fn empty_fs_readdir() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arenas([]).await?;

        assert!(fixture.fs.readdir(PathId(1)).await?.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn empty_fs_metadata() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arenas([]).await?;

        let m = fixture.fs.dir_metadata(PathId(1)).await?;
        assert_eq!(0o555, m.mode);
        assert_ne!(UnixTime::ZERO, m.mtime);

        Ok(())
    }

    #[tokio::test]
    async fn initial_dir_mtime() -> anyhow::Result<()> {
        let arena = Arena::from("documents/letters");
        let fixture = Fixture::setup_with_arena(arena).await?;

        let root_m = fixture.fs.dir_metadata(PathId(1)).await?;
        assert_eq!(0o555, root_m.mode);
        assert_ne!(UnixTime::ZERO, root_m.mtime);

        let (documents, _) = fixture.fs.lookup((PathId(1), "documents")).await?;
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

        let (arenas, metadata) = fs.lookup((PathId(1), "arenas")).await.unwrap();
        assert!(matches!(metadata, crate::arena::types::Metadata::Dir(_)));

        let (_, metadata) = fs.lookup((PathId(1), "other")).await.unwrap();
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
            fs.lookup((Filesystem::ROOT_DIR, "nonexistent")).await,
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
        let entries = fs.readdir(PathId(1)).await?;
        assert_eq!(entries.len(), 2);

        let mut names: Vec<String> = entries.iter().map(|(name, _, _)| name.clone()).collect();
        names.sort();
        assert_eq!(names, vec!["arenas", "other"]);

        // Verify all entries are directories and read-only
        for (name, _, metadata) in entries {
            match metadata {
                crate::arena::types::Metadata::Dir(dir_meta) => {
                    assert_eq!(0o555, dir_meta.mode);
                    assert_ne!(dir_meta.mtime, UnixTime::ZERO);
                }
                _ => panic!("Expected directory metadata for {}", name),
            }
        }

        let (arenas, _) = fs.lookup((PathId(1), "arenas")).await?;
        let entries = fs.readdir(arenas).await?;
        assert_eq!(entries.len(), 2);

        let mut names: Vec<String> = entries.iter().map(|(name, _, _)| name.clone()).collect();
        names.sort();
        assert_eq!(names, vec!["test1", "test2"]);

        // Verify all entries are directories and read-only
        for (name, _, metadata) in entries {
            match metadata {
                crate::arena::types::Metadata::Dir(dir_meta) => {
                    assert_eq!(0o555, dir_meta.mode);
                    assert_ne!(dir_meta.mtime, UnixTime::ZERO);
                }
                _ => panic!("Expected directory metadata for {}", name),
            }
        }

        assert!(
            fs.readdir(fs.arena_root(Arena::from("arenas/test1"))?)
                .await?
                .is_empty()
        );
        assert!(
            fs.readdir(fs.arena_root(Arena::from("arenas/test2"))?)
                .await?
                .is_empty()
        );
        assert!(
            fs.readdir(fs.arena_root(Arena::from("other"))?)
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

        let res = fs.unlink((Filesystem::ROOT_DIR, "doesnotexist")).await;
        assert!(matches!(res, Err(StorageError::NotFound)), "{res:?}");
        assert!(matches!(
            fs.unlink((Filesystem::ROOT_DIR, "arenas")).await,
            Err(StorageError::IsADirectory)
        ));
        let (arenas_pathid, _) = fs.lookup((Filesystem::ROOT_DIR, "arenas")).await?;
        assert!(matches!(
            fs.unlink((arenas_pathid, "1")).await,
            Err(StorageError::IsADirectory)
        ));

        // This just checks that the call is dispatched down to the
        // arena fs.
        assert!(matches!(
            fs.unlink((fs.arena_root(arena)?, "doesnotexist")).await,
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

        let arenas_dir = fs.lookup((Filesystem::ROOT_DIR, "arenas")).await?.0;
        let res = fs
            .branch(arenas_dir, (Filesystem::ROOT_DIR, "test_arena2"))
            .await;
        assert!(matches!(res, Err(StorageError::IsADirectory)), "{res:?}");

        assert!(matches!(
            fs.branch(
                fs.arena_root(arena1)?,
                (fs.arena_root(arena2)?, "test_arena2")
            )
            .await,
            Err(StorageError::CrossesDevices)
        ));

        Ok(())
    }
}
