//! The Unreal cache - a partial local cache of remote files.
//!
//! See `spec/unreal.md` for details.

use super::db::{GlobalDatabase, GlobalReadTransaction};
use super::pathid_allocator::PathIdAllocator;
use super::types::PathAssignment;
use crate::arena::arena_cache::{ArenaCache, CacheLoc};
use crate::arena::notifier::{Notification, Progress};
use crate::arena::types::{DirMetadata, LocalAvailability};
use crate::global::db::GlobalWriteTransaction;
use crate::global::types::PathTableEntry;
use crate::utils::holder::Holder;
use crate::{Blob, FileMetadata, Inode, PathId, StorageError};
use realize_types::{Arena, Path, Peer, UnixTime};
use redb::ReadableTable;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task;

/// A cache of remote files.
pub struct GlobalCache {
    db: Arc<GlobalDatabase>,
    allocator: Arc<PathIdAllocator>,

    arena_caches: HashMap<Arena, Arc<ArenaCache>>,

    /// PathIds to intermediate paths, before arenas, includes root.
    paths: HashMap<PathId, IntermediatePath>,
}

impl GlobalCache {
    /// PathId of the root dir.
    pub const ROOT_DIR: PathId = PathIdAllocator::ROOT_INODE;

    /// Create a new cache with the database at the given path.
    pub(crate) async fn with_db<T>(
        db: Arc<GlobalDatabase>,
        allocator: Arc<PathIdAllocator>,
        caches: T,
    ) -> Result<Arc<Self>, anyhow::Error>
    where
        T: IntoIterator<Item = Arc<ArenaCache>> + Send + 'static,
    {
        task::spawn_blocking(move || {
            let mut arena_caches = HashMap::new();
            let mut paths = HashMap::new();
            let txn = db.begin_write()?;
            {
                let mut path_table = txn.path_table()?;

                // Make sure root is setup, even if there are no arenas.
                let root_mtime =
                    get_or_add_path_entry(&txn, &mut path_table, &allocator, "")?.mtime;
                paths.insert(
                    PathIdAllocator::ROOT_INODE,
                    IntermediatePath {
                        entries: HashMap::new(),
                        mtime: root_mtime,
                    },
                );

                for arena_cache in caches.into_iter() {
                    register(
                        arena_cache,
                        &txn,
                        &mut path_table,
                        &allocator,
                        &mut arena_caches,
                        &mut paths,
                    )?;
                }
            }
            txn.commit()?;

            Ok::<_, anyhow::Error>(Arc::new(Self {
                db,
                allocator,
                arena_caches,
                paths,
            }))
        })
        .await?
    }

    /// Lists arenas available in this database
    pub fn arenas(&self) -> impl Iterator<Item = Arena> {
        self.arena_caches.keys().map(|a| *a)
    }

    /// Returns the pathid of an arena.
    ///
    /// Will return [StorageError::UnknownArena] unless the arena
    /// is available in the cache.
    pub fn arena_root(&self, arena: Arena) -> Result<PathId, StorageError> {
        self.allocator
            .arena_root(arena)
            .ok_or_else(|| StorageError::UnknownArena(arena))
    }

    /// Returns the cache for the given arena or fail.
    fn arena_cache(&self, arena: Arena) -> Result<&ArenaCache, StorageError> {
        Ok(self
            .arena_caches
            .get(&arena)
            .ok_or(StorageError::NotFound)?)
    }

    /// Returns the cache for the given pathid or fail.
    fn arena_cache_for_pathid(&self, pathid: PathId) -> Result<&ArenaCache, StorageError> {
        let arena = self
            .allocator
            .arena_for_pathid(&self.db.begin_read()?, pathid)?
            .ok_or(StorageError::NotFound)?;
        self.arena_cache(arena)
    }

    fn arena_cache_for_inode(&self, inode: Inode) -> Result<&ArenaCache, StorageError> {
        // We ignore the difference between PathId and Inodes for this
        // lookup, as they come from the same ranges.
        self.arena_cache_for_pathid(PathId(inode.as_u64()))
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
    fn resolve_loc<L: Into<GlobalLoc>>(
        &self,
        txn: &GlobalReadTransaction,
        loc: L,
    ) -> Result<ResolvedLoc, StorageError> {
        Ok(match self.resolve_arena_root(loc.into()) {
            GlobalLoc::PathId(pathid) => match self.allocator.arena_for_pathid(txn, pathid)? {
                Some(arena) => ResolvedLoc::InArena(arena, CacheLoc::PathId(pathid)),
                None => ResolvedLoc::Global(if self.paths.contains_key(&pathid) {
                    Some(pathid)
                } else {
                    None
                }),
            },
            GlobalLoc::Inode(inode) => {
                match self.arena_for_inode(txn, inode)? {
                    Some(arena) => ResolvedLoc::InArena(arena, CacheLoc::Inode(inode)),
                    None => {
                        // global inodes and pathids are identical
                        let pathid = PathId(inode.as_u64());

                        ResolvedLoc::Global(if self.paths.contains_key(&pathid) {
                            Some(pathid)
                        } else {
                            None
                        })
                    }
                }
            }
            GlobalLoc::PathIdAndName(pathid, name) => {
                match self.allocator.arena_for_pathid(txn, pathid)? {
                    Some(arena) => {
                        ResolvedLoc::InArena(arena, CacheLoc::PathIdAndName(pathid, name))
                    }
                    None => ResolvedLoc::Global(match self.paths.get(&pathid) {
                        None => return Err(StorageError::NotFound),
                        Some(IntermediatePath { entries, .. }) => {
                            entries.get(&name).map(|pathid| *pathid)
                        }
                    }),
                }
            }
            GlobalLoc::InodeAndName(inode, name) => {
                match self.arena_for_inode(txn, inode)? {
                    Some(arena) => ResolvedLoc::InArena(arena, CacheLoc::InodeAndName(inode, name)),
                    None => {
                        // global inodes and pathids are identical
                        let pathid = PathId(inode.as_u64());

                        ResolvedLoc::Global(match self.paths.get(&pathid) {
                            None => return Err(StorageError::NotFound),
                            Some(IntermediatePath { entries, .. }) => {
                                entries.get(&name).map(|pathid| *pathid)
                            }
                        })
                    }
                }
            }
            GlobalLoc::Path(arena, path) => ResolvedLoc::InArena(arena, CacheLoc::Path(path)),
        })
    }

    /// Convert a [GlobalTreeLoc] into an arena and arena [TreeLoc] or fail.
    fn resolve_arena_loc<L: Into<GlobalLoc>>(
        &self,
        loc: L,
    ) -> Result<(&ArenaCache, CacheLoc), StorageError> {
        Ok(match self.resolve_arena_root(loc.into()) {
            GlobalLoc::PathId(pathid) => (
                self.arena_cache_for_pathid(pathid)?,
                CacheLoc::PathId(pathid),
            ),
            GlobalLoc::Inode(inode) => (self.arena_cache_for_inode(inode)?, CacheLoc::Inode(inode)),
            GlobalLoc::PathIdAndName(pathid, name) => (
                self.arena_cache_for_pathid(pathid)?,
                CacheLoc::PathIdAndName(pathid, name.into()),
            ),
            GlobalLoc::InodeAndName(inode, name) => (
                self.arena_cache_for_inode(inode)?,
                CacheLoc::InodeAndName(inode, name.into()),
            ),
            GlobalLoc::Path(arena, path) => (self.arena_cache(arena)?, CacheLoc::Path(path)),
        })
    }

    /// Cover the special case of a PathIdAndName where name points to an arena root.
    fn resolve_arena_root(&self, loc: GlobalLoc) -> GlobalLoc {
        if let Some((pathid, name)) = match &loc {
            GlobalLoc::PathIdAndName(pathid, name) => Some((*pathid, name)),
            GlobalLoc::InodeAndName(inode, name) => Some((PathId(inode.as_u64()), name)),
            _ => None,
        } {
            if let Some(IntermediatePath { entries, .. }) = self.paths.get(&pathid) {
                if let Some(pathid) = entries.get(name) {
                    if self.allocator.is_arena_root(*pathid) {
                        return GlobalLoc::PathId(*pathid);
                    }
                }
            }
        }

        loc
    }

    /// Lookup a directory entry.
    pub async fn lookup<L: Into<GlobalLoc>>(
        self: &Arc<Self>,
        loc: L,
    ) -> Result<(Inode, PathAssignment), StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            match this.resolve_loc(&txn, loc)? {
                ResolvedLoc::InArena(arena, loc) => {
                    let cache = this.arena_cache(arena)?;
                    cache.lookup(loc)
                }
                ResolvedLoc::Global(pathid) => {
                    if let Some(pathid) = pathid
                        && this.paths.contains_key(&pathid)
                    {
                        Ok((Inode(pathid.as_u64()), PathAssignment::Directory))
                    } else {
                        Err(StorageError::NotFound)
                    }
                }
            }
        })
        .await?
    }

    /// Return the mtime of the directory.
    pub async fn dir_metadata<L: Into<GlobalLoc>>(
        self: &Arc<Self>,
        loc: L,
    ) -> Result<DirMetadata, StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            match this.resolve_loc(&txn, loc)? {
                ResolvedLoc::InArena(arena, loc) => {
                    let cache = this.arena_cache(arena)?;
                    cache.dir_metadata(loc)
                }
                ResolvedLoc::Global(None) => Err(StorageError::NotFound),
                ResolvedLoc::Global(Some(pathid)) => match this.paths.get(&pathid) {
                    None => Err(StorageError::NotFound),
                    Some(IntermediatePath { mtime, .. }) => Ok(DirMetadata {
                        read_only: true,
                        mtime: *mtime,
                    }),
                },
            }
        })
        .await?
    }

    pub async fn readdir<L: Into<GlobalLoc>>(
        self: &Arc<Self>,
        loc: L,
    ) -> Result<Vec<(String, Inode, PathAssignment)>, StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            match this.resolve_loc(&txn, loc)? {
                ResolvedLoc::InArena(arena, loc) => {
                    let cache = this.arena_cache(arena)?;
                    cache.readdir(loc)
                }
                ResolvedLoc::Global(None) => Err(StorageError::NotFound),
                ResolvedLoc::Global(Some(pathid)) => match this.paths.get(&pathid) {
                    None => Err(StorageError::NotFound),
                    Some(IntermediatePath { entries, .. }) => Ok(entries
                        .iter()
                        .map(|(name, pathid)| {
                            (
                                name.to_string(),
                                // global inodes and pathids map 1:1
                                Inode(pathid.as_u64()),
                                PathAssignment::Directory,
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
            let cache = this.arena_cache(notification.arena())?;
            cache.update(peer, notification, None)
        })
        .await?
    }
    /// Return a [Progress] instance that represents how up-to-date
    /// the information in the cache is for that peer and arena.
    ///
    /// This should be passed to the peer when subscribing.
    pub async fn peer_progress(
        self: &Arc<Self>,
        peer: Peer,
        arena: Arena,
    ) -> Result<Option<Progress>, StorageError> {
        let this = Arc::clone(self);
        task::spawn_blocking(move || {
            let cache = this.arena_cache(arena)?;
            cache.peer_progress(peer)
        })
        .await?
    }

    /// Check local file content availability.
    pub async fn local_availability<L: Into<GlobalLoc>>(
        self: &Arc<Self>,
        loc: L,
    ) -> Result<LocalAvailability, StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let (cache, loc) = this.resolve_arena_loc(loc)?;
            cache.local_availability(loc)
        })
        .await?
    }

    /// Open a file for reading/writing, creating a new blob entry.
    ///
    /// The returned [Blob] is available for reading. However, reading outside
    /// the range of data that is locally available causes [crate::BlobIncomplete] error.
    ///
    /// This is usually used through the `Downloader`, which can
    /// download incomplete portions of the file.
    pub async fn open_file<L: Into<GlobalLoc>>(
        self: &Arc<Self>,
        loc: L,
    ) -> Result<Blob, StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let (cache, loc) = this.resolve_arena_loc(loc)?;
            cache.open_file(loc)
        })
        .await?
    }

    pub async fn file_metadata<L: Into<GlobalLoc>>(
        self: &Arc<Self>,
        loc: L,
    ) -> Result<FileMetadata, StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let (cache, loc) = this.resolve_arena_loc(loc)?;
            cache.file_metadata(loc)
        })
        .await?
    }

    pub async fn unlink<L: Into<GlobalLoc>>(self: &Arc<Self>, loc: L) -> Result<(), StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            match this.resolve_loc(&txn, loc)? {
                ResolvedLoc::InArena(arena, loc) => {
                    let cache = this.arena_cache(arena)?;
                    cache.unlink(loc)
                }
                ResolvedLoc::Global(Some(_)) => Err(StorageError::IsADirectory),
                ResolvedLoc::Global(None) => Err(StorageError::NotFound),
            }
        })
        .await?
    }

    pub async fn branch<L1: Into<GlobalLoc>, L2: Into<GlobalLoc>>(
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
                    let cache = this.arena_cache(source_arena)?;
                    cache.branch(source, dest)
                }
                (ResolvedLoc::Global(_), ResolvedLoc::Global(_)) => Err(StorageError::IsADirectory),
                (_, _) => Err(StorageError::CrossesDevices),
            }
        })
        .await?
    }

    pub async fn rename<L1: Into<GlobalLoc>, L2: Into<GlobalLoc>>(
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
                    let cache = this.arena_cache(source_arena)?;
                    cache.rename(source, dest, noreplace)
                }
                (ResolvedLoc::Global(_), ResolvedLoc::Global(_)) => Err(StorageError::IsADirectory),
                (_, _) => Err(StorageError::CrossesDevices),
            }
        })
        .await?
    }

    /// Create a directory at the given path in the specified arena.
    pub async fn mkdir<L: Into<GlobalLoc>>(
        self: &Arc<Self>,
        loc: L,
    ) -> Result<(Inode, DirMetadata), StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let (cache, loc) = this.resolve_arena_loc(loc)?;

            cache.mkdir(loc)
        })
        .await?
    }

    /// Remove an empty directory at the given path in the specified arena.
    pub async fn rmdir<L: Into<GlobalLoc>>(self: &Arc<Self>, loc: L) -> Result<(), StorageError> {
        let loc = loc.into();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let (cache, loc) = this.resolve_arena_loc(loc)?;

            cache.rmdir(loc)
        })
        .await?
    }
}

/// A location within the cache.
///
/// This is usually a [Path] within an [Arena] or a [PathId], but can
/// also be a [PathId] and a name to specify a child of a known
/// directory.
pub enum GlobalLoc {
    PathId(PathId),
    Inode(Inode),
    Path(Arena, Path),
    PathIdAndName(PathId, String),
    InodeAndName(Inode, String),
}

impl From<PathId> for GlobalLoc {
    fn from(value: PathId) -> Self {
        GlobalLoc::PathId(value)
    }
}

impl From<Inode> for GlobalLoc {
    fn from(value: Inode) -> Self {
        GlobalLoc::Inode(value)
    }
}

impl From<(Arena, Path)> for GlobalLoc {
    fn from(value: (Arena, Path)) -> Self {
        GlobalLoc::Path(value.0, value.1)
    }
}

impl From<(Arena, &Path)> for GlobalLoc {
    fn from(value: (Arena, &Path)) -> Self {
        GlobalLoc::Path(value.0, value.1.clone())
    }
}

impl From<(PathId, &str)> for GlobalLoc {
    fn from(value: (PathId, &str)) -> Self {
        GlobalLoc::PathIdAndName(value.0, value.1.to_string())
    }
}

impl From<(PathId, &String)> for GlobalLoc {
    fn from(value: (PathId, &String)) -> Self {
        GlobalLoc::PathIdAndName(value.0, value.1.clone())
    }
}
impl From<(PathId, String)> for GlobalLoc {
    fn from(value: (PathId, String)) -> Self {
        GlobalLoc::PathIdAndName(value.0, value.1)
    }
}

impl From<(Inode, &str)> for GlobalLoc {
    fn from(value: (Inode, &str)) -> Self {
        GlobalLoc::InodeAndName(value.0, value.1.to_string())
    }
}

impl From<(Inode, &String)> for GlobalLoc {
    fn from(value: (Inode, &String)) -> Self {
        GlobalLoc::InodeAndName(value.0, value.1.clone())
    }
}
impl From<(Inode, String)> for GlobalLoc {
    fn from(value: (Inode, String)) -> Self {
        GlobalLoc::InodeAndName(value.0, value.1)
    }
}

enum ResolvedLoc {
    Global(Option<PathId>),
    InArena(Arena, CacheLoc),
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

/// Register an [ArenaCache] that handles calls for a specific arena.
///
/// Calls for pathid assigned to the cache arena will be directed there.
fn register(
    cache: Arc<ArenaCache>,
    txn: &GlobalWriteTransaction,
    path_table: &mut redb::Table<&'static str, Holder<'static, PathTableEntry>>,
    allocator: &Arc<PathIdAllocator>,
    arena_caches: &mut HashMap<Arena, Arc<ArenaCache>>,
    paths: &mut HashMap<PathId, IntermediatePath>,
) -> anyhow::Result<()> {
    let arena = cache.arena();
    let arena_root = allocator
        .arena_root(arena)
        .ok_or_else(|| StorageError::UnknownArena(arena))?;

    for existing in arena_caches.keys().map(|a| *a) {
        check_arena_compatibility(arena, existing)?;
    }
    add_arena_root(arena, arena_root, txn, path_table, allocator, paths)?;
    arena_caches.insert(cache.arena(), cache);

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
        cache: Arc<GlobalCache>,
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
            let mut arena_caches = vec![];
            for arena in arenas {
                let blob_dir = tempdir.child(format!("{arena}/blobs"));
                blob_dir.create_dir_all()?;
                arena_caches.push(ArenaCache::for_testing(
                    arena,
                    Arc::clone(&allocator),
                    blob_dir.path(),
                )?);
            }
            let cache =
                GlobalCache::with_db(Arc::clone(&db), Arc::clone(&allocator), arena_caches).await?;

            Ok(Self {
                cache,
                _tempdir: tempdir,
            })
        }
    }

    #[tokio::test]
    async fn empty_cache_readdir() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arenas([]).await?;

        assert!(fixture.cache.readdir(PathId(1)).await?.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn empty_cache_metadata() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arenas([]).await?;

        let m = fixture.cache.dir_metadata(PathId(1)).await?;
        assert!(m.read_only);
        assert_ne!(UnixTime::ZERO, m.mtime);

        Ok(())
    }

    #[tokio::test]
    async fn initial_dir_mtime() -> anyhow::Result<()> {
        let arena = Arena::from("documents/letters");
        let fixture = Fixture::setup_with_arena(arena).await?;

        let root_m = fixture.cache.dir_metadata(PathId(1)).await?;
        assert!(root_m.read_only);
        assert_ne!(UnixTime::ZERO, root_m.mtime);

        let (documents, _) = fixture.cache.lookup((PathId(1), "documents")).await?;
        let documents_m = fixture.cache.dir_metadata(documents).await?;
        assert!(documents_m.read_only);
        assert_ne!(UnixTime::ZERO, documents_m.mtime);

        let (letters, _) = fixture.cache.lookup((documents, "letters")).await?;
        let letters_m = fixture.cache.dir_metadata(letters).await?;
        assert!(!letters_m.read_only);
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

        let cache = &fixture.cache;

        let (arenas, assignment) = cache.lookup((PathId(1), "arenas")).await.unwrap();
        assert_eq!(assignment, PathAssignment::Directory);

        let (_, assignment) = cache.lookup((PathId(1), "other")).await.unwrap();
        assert_eq!(assignment, PathAssignment::Directory);

        let (_, assignment) = cache.lookup((arenas, "test1")).await.unwrap();
        assert_eq!(assignment, PathAssignment::Directory);

        let (_, assignment) = cache.lookup((arenas, "test2")).await.unwrap();
        assert_eq!(assignment, PathAssignment::Directory);

        Ok(())
    }

    #[tokio::test]
    async fn lookup_returns_notfound_for_missing_entry() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let cache = &fixture.cache;

        assert!(matches!(
            cache.lookup((GlobalCache::ROOT_DIR, "nonexistent")).await,
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

        let cache = &fixture.cache;
        assert_unordered::assert_eq_unordered!(
            vec![
                ("arenas".to_string(), PathAssignment::Directory),
                ("other".to_string(), PathAssignment::Directory)
            ],
            cache
                .readdir(PathId(1))
                .await?
                .into_iter()
                .map(|(name, _, assignment)| (name, assignment))
                .collect::<Vec<_>>(),
        );

        let (arenas, _) = cache.lookup((PathId(1), "arenas")).await?;
        assert_unordered::assert_eq_unordered!(
            vec![
                ("test1".to_string(), PathAssignment::Directory),
                ("test2".to_string(), PathAssignment::Directory)
            ],
            cache
                .readdir(arenas)
                .await?
                .into_iter()
                .map(|(name, _, assignment)| (name, assignment))
                .collect::<Vec<_>>(),
        );

        assert!(
            cache
                .readdir(cache.arena_root(Arena::from("arenas/test1"))?)
                .await?
                .is_empty()
        );
        assert!(
            cache
                .readdir(cache.arena_root(Arena::from("arenas/test2"))?)
                .await?
                .is_empty()
        );
        assert!(
            cache
                .readdir(cache.arena_root(Arena::from("other"))?)
                .await?
                .is_empty()
        );

        Ok(())
    }

    #[tokio::test]
    async fn unlink() -> anyhow::Result<()> {
        let arena = Arena::from("arenas/1");
        let fixture = Fixture::setup_with_arena(arena).await?;
        let cache = &fixture.cache;

        let res = cache.unlink((GlobalCache::ROOT_DIR, "doesnotexist")).await;
        assert!(matches!(res, Err(StorageError::NotFound)), "{res:?}");
        assert!(matches!(
            cache.unlink((GlobalCache::ROOT_DIR, "arenas")).await,
            Err(StorageError::IsADirectory)
        ));
        let (arenas_pathid, _) = cache.lookup((GlobalCache::ROOT_DIR, "arenas")).await?;
        assert!(matches!(
            cache.unlink((arenas_pathid, "1")).await,
            Err(StorageError::IsADirectory)
        ));

        // This just checks that the call is dispatched down to the
        // arena cache.
        assert!(matches!(
            cache
                .unlink((cache.arena_root(arena)?, "doesnotexist"))
                .await,
            Err(StorageError::NotFound)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn branch() -> anyhow::Result<()> {
        let arena1 = Arena::from("arenas/1");
        let arena2 = Arena::from("arenas/2");
        let fixture = Fixture::setup_with_arenas([arena1, arena2]).await?;
        let cache = &fixture.cache;

        let arenas_dir = cache.lookup((GlobalCache::ROOT_DIR, "arenas")).await?.0;
        let res = cache
            .branch(arenas_dir, (GlobalCache::ROOT_DIR, "test_arena2"))
            .await;
        assert!(matches!(res, Err(StorageError::IsADirectory)), "{res:?}");

        assert!(matches!(
            cache
                .branch(
                    cache.arena_root(arena1)?,
                    (cache.arena_root(arena2)?, "test_arena2")
                )
                .await,
            Err(StorageError::CrossesDevices)
        ));

        Ok(())
    }
}
