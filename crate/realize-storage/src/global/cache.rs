//! The Unreal cache - a partial local cache of remote files.
//!
//! See `spec/unreal.md` for details.

use super::db::GlobalDatabase;
use super::inode_allocator::InodeAllocator;
use super::types::InodeAssignment;
use crate::arena::arena_cache::ArenaCache;
use crate::arena::notifier::{Notification, Progress};
use crate::arena::types::LocalAvailability;
use crate::global::types::PathTableEntry;
use crate::utils::holder::Holder;
use crate::{Blob, FileAvailability, FileMetadata, Inode, StorageError};
use realize_types::{Arena, Path, Peer, UnixTime};
use redb::ReadableTable;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task;

/// A cache of remote files.
pub struct UnrealCacheBlocking {
    db: Arc<GlobalDatabase>,
    allocator: Arc<InodeAllocator>,
    arena_caches: HashMap<Arena, Arc<ArenaCache>>,

    /// Inodes to intermediate paths, before arenas, includes root.
    paths: HashMap<Inode, IntermediatePath>,
}
#[derive(Debug)]
struct IntermediatePath {
    entries: HashMap<String, Inode>,
    mtime: UnixTime,
}

impl UnrealCacheBlocking {
    /// Inode of the root dir.
    pub const ROOT_DIR: Inode = InodeAllocator::ROOT_INODE;

    /// Create a new UnrealCache from a redb database.
    pub(crate) fn new(db: Arc<GlobalDatabase>, allocator: Arc<InodeAllocator>) -> Self {
        Self {
            db,
            allocator,
            arena_caches: HashMap::new(),
            paths: HashMap::new(),
        }
    }

    /// Lists arenas available in this database
    pub fn arenas(&self) -> impl Iterator<Item = Arena> {
        self.arena_caches.keys().map(|a| *a)
    }

    /// Returns the inode of an arena.
    ///
    /// Will return [StorageError::UnknownArena] unless the arena
    /// is available in the cache.
    pub fn arena_root(&self, arena: Arena) -> Result<Inode, StorageError> {
        self.allocator
            .arena_root(arena)
            .ok_or_else(|| StorageError::UnknownArena(arena))
    }

    /// Returns the cache for the given arena or fail.
    pub(crate) fn arena_cache(&self, arena: Arena) -> Result<&ArenaCache, StorageError> {
        Ok(self
            .arena_caches
            .get(&arena)
            .ok_or(StorageError::NotFound)?)
    }

    /// Returns the cache for the given inode or fail.
    fn arena_cache_for_inode(&self, inode: Inode) -> Result<&ArenaCache, StorageError> {
        let arena = self
            .allocator
            .arena_for_inode(&self.db.begin_read()?, inode)?
            .ok_or(StorageError::NotFound)?;
        self.arena_cache(arena)
    }

    /// Register an [ArenaCache] that handles calls for a specific arena.
    ///
    /// Calls for inode assigned to the cache arena will be directed there.
    pub(crate) fn register(&mut self, cache: Arc<ArenaCache>) -> anyhow::Result<()> {
        let arena = cache.arena();
        let arena_root = self
            .allocator
            .arena_root(arena)
            .ok_or_else(|| StorageError::UnknownArena(arena))?;

        for existing in self.arena_caches.keys().map(|a| *a) {
            check_arena_compatibility(arena, existing)?;
        }
        self.add_arena_root(arena, arena_root)?;
        self.arena_caches.insert(cache.arena(), cache);

        Ok(())
    }

    /// Transform this cache into an async cache.
    pub fn into_async(self) -> UnrealCacheAsync {
        UnrealCacheAsync::new(self)
    }

    /// Lookup a directory entry.
    pub fn lookup(
        &self,
        parent_inode: Inode,
        name: &str,
    ) -> Result<(Inode, InodeAssignment), StorageError> {
        let txn = self.db.begin_read()?;
        match self.allocator.arena_for_inode(&txn, parent_inode)? {
            Some(arena) => self.arena_cache(arena)?.lookup(parent_inode, name),
            None => match self.paths.get(&parent_inode) {
                None => Err(StorageError::NotFound),
                Some(IntermediatePath { entries, .. }) => entries
                    .get(name)
                    .map(|inode| (*inode, InodeAssignment::Directory))
                    .ok_or(StorageError::NotFound),
            },
        }
    }

    /// Lookup the inode and type of the file or directory pointed to by a path.
    pub fn lookup_path(
        &self,
        arena: Arena,
        path: &Path,
    ) -> Result<(Inode, InodeAssignment), StorageError> {
        self.arena_cache(arena)?.lookup_path(path)
    }

    /// Return the mtime of the directory.
    pub fn dir_mtime(&self, inode: Inode) -> Result<UnixTime, StorageError> {
        let txn = self.db.begin_read()?;
        match self.allocator.arena_for_inode(&txn, inode)? {
            Some(arena) => self.arena_cache(arena)?.dir_mtime(inode),
            None => match self.paths.get(&inode) {
                None => Err(StorageError::NotFound),
                Some(IntermediatePath { mtime, .. }) => Ok(*mtime),
            },
        }
    }

    pub fn readdir(
        &self,
        inode: Inode,
    ) -> Result<Vec<(String, Inode, InodeAssignment)>, StorageError> {
        let txn = self.db.begin_read()?;
        match self.allocator.arena_for_inode(&txn, inode)? {
            Some(arena) => self.arena_cache(arena)?.readdir(inode),
            None => match self.paths.get(&inode) {
                None => Err(StorageError::NotFound),
                Some(IntermediatePath { entries, .. }) => Ok(entries
                    .iter()
                    .map(|(name, inode)| (name.to_string(), *inode, InodeAssignment::Directory))
                    .collect()),
            },
        }
    }

    pub(crate) fn update(
        &self,
        peer: Peer,
        notification: Notification,
    ) -> Result<(), StorageError> {
        let arena = notification.arena();
        let cache = self.arena_cache(arena)?;
        cache.update(peer, notification, None)
    }

    fn add_arena_root(&mut self, arena: Arena, arena_root: Inode) -> anyhow::Result<()> {
        let arena_path = Path::parse(arena.as_str())?;
        let txn = self.db.begin_write()?;
        {
            let mut path_table = txn.path_table()?;
            let mut names = Path::components(Some(&arena_path))
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .chain(/* root */ std::iter::once(""));
            let mut current_inode = arena_root;
            let mut current_name = names.next().unwrap();
            for dirname in names {
                let entry = if let Some(e) = path_table.get(dirname)? {
                    e.value().parse()?
                } else {
                    let entry_inode = if dirname == "" {
                        InodeAllocator::ROOT_INODE
                    } else {
                        self.allocator.allocate_global_inode(&txn)?
                    };
                    let entry = PathTableEntry {
                        inode: entry_inode,
                        mtime: UnixTime::now(),
                    };
                    path_table.insert(dirname, Holder::new(&entry)?)?;

                    entry
                };
                self.add_intermediate_path_entry(
                    entry.inode,
                    entry.mtime,
                    current_name,
                    current_inode,
                );
                current_name = dirname;
                current_inode = entry.inode;
            }
        }
        txn.commit()?;

        Ok(())
    }

    /// Adds an entry in the intermediate path with the given inode.
    ///
    /// If no such intermediate path exists, it is added.
    fn add_intermediate_path_entry(
        &mut self,
        inode: Inode,
        mtime: UnixTime,
        entry_name: &str,
        entry_inode: Inode,
    ) {
        let mapping = self.paths.entry(inode).or_insert_with(|| IntermediatePath {
            entries: HashMap::new(),
            mtime,
        });
        mapping.entries.insert(entry_name.to_string(), entry_inode);
    }
}

#[derive(Clone)]
pub struct UnrealCacheAsync {
    inner: Arc<UnrealCacheBlocking>,
}

impl UnrealCacheAsync {
    /// Inode of the root dir.
    pub const ROOT_DIR: Inode = UnrealCacheBlocking::ROOT_DIR;

    /// Create a new cache from a blocking one.
    pub fn new(inner: UnrealCacheBlocking) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Create a new cache with the database at the given path.
    pub(crate) async fn with_db<T>(
        db: Arc<GlobalDatabase>,
        allocator: Arc<InodeAllocator>,
        caches: T,
    ) -> Result<Self, anyhow::Error>
    where
        T: IntoIterator<Item = Arc<ArenaCache>> + Send + 'static,
    {
        task::spawn_blocking(move || {
            let mut cache = UnrealCacheBlocking::new(db, allocator);
            for arena_cache in caches.into_iter() {
                cache.register(arena_cache)?;
            }

            Ok::<_, anyhow::Error>(Self::new(cache))
        })
        .await?
    }

    /// Return a reference on the blocking cache.
    pub fn blocking(&self) -> Arc<UnrealCacheBlocking> {
        Arc::clone(&self.inner)
    }

    pub fn arenas(&self) -> impl Iterator<Item = Arena> {
        self.inner.arenas()
    }

    pub fn arena_root(&self, arena: Arena) -> Result<Inode, StorageError> {
        self.inner.arena_root(arena)
    }

    pub async fn lookup(
        &self,
        parent_inode: Inode,
        name: &str,
    ) -> Result<(Inode, InodeAssignment), StorageError> {
        let name = name.to_string();
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || inner.lookup(parent_inode, &name)).await?
    }

    pub async fn lookup_path(
        &self,
        arena: Arena,
        path: &Path,
    ) -> Result<(Inode, InodeAssignment), StorageError> {
        let path = path.clone();
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || inner.lookup_path(arena, &path)).await?
    }

    pub async fn file_metadata(&self, inode: Inode) -> Result<FileMetadata, StorageError> {
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || {
            let arena_cache = inner.arena_cache_for_inode(inode)?;
            arena_cache.file_metadata(inode)
        })
        .await?
    }

    pub async fn file_availability(&self, inode: Inode) -> Result<FileAvailability, StorageError> {
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || {
            let arena_cache = inner.arena_cache_for_inode(inode)?;
            arena_cache.file_availability(inode)
        })
        .await?
    }

    pub async fn dir_mtime(&self, inode: Inode) -> Result<UnixTime, StorageError> {
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || inner.dir_mtime(inode)).await?
    }

    pub async fn readdir(
        &self,
        inode: Inode,
    ) -> Result<Vec<(String, Inode, InodeAssignment)>, StorageError> {
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || inner.readdir(inode)).await?
    }

    /// Return a [Progress] instance that represents how up-to-date
    /// the information in the cache is for that peer and arena.
    ///
    /// This should be passed to the peer when subscribing.
    pub async fn peer_progress(
        &self,
        peer: Peer,
        arena: Arena,
    ) -> Result<Option<Progress>, StorageError> {
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || {
            let arena_cache = inner.arena_cache(arena)?;
            arena_cache.peer_progress(peer)
        })
        .await?
    }

    /// Update the cache by applying a notification coming from the given peer.
    /// TODO: remove. This is replaced by [crate::arena::ArenaStorage::update].
    pub async fn update(&self, peer: Peer, notification: Notification) -> Result<(), StorageError> {
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || inner.update(peer, notification)).await?
    }

    /// Check local file content availability.
    pub async fn local_availability(
        &self,
        inode: Inode,
    ) -> Result<LocalAvailability, StorageError> {
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || {
            let arena_cache = inner.arena_cache_for_inode(inode)?;
            arena_cache.local_availability(inode)
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
    pub async fn open_file(&self, inode: Inode) -> Result<Blob, StorageError> {
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || {
            let arena_cache = inner.arena_cache_for_inode(inode)?;
            arena_cache.open_file(inode)
        })
        .await?
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DirtyPaths;
    use crate::arena::db::ArenaDatabase;
    use crate::utils::redb_utils;
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use realize_types::Arena;

    fn test_arena() -> Arena {
        Arena::from("test_arena")
    }

    struct Fixture {
        cache: UnrealCacheBlocking,
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
            let allocator = InodeAllocator::new(Arc::clone(&db), arenas.clone())?;
            let mut cache = UnrealCacheBlocking::new(Arc::clone(&db), Arc::clone(&allocator));

            for arena in arenas {
                let blob_dir = tempdir.child(format!("{arena}/blobs"));
                blob_dir.create_dir_all()?;
                let db = ArenaDatabase::new(redb_utils::in_memory()?)?;
                let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;
                cache.register(ArenaCache::new(
                    arena,
                    Arc::clone(&allocator),
                    db,
                    blob_dir.path(),
                    dirty_paths,
                )?)?;
            }
            Ok(Self {
                cache,
                _tempdir: tempdir,
            })
        }
    }

    #[tokio::test]
    async fn initial_dir_mtime() -> anyhow::Result<()> {
        let arena = Arena::from("documents/letters");
        let fixture = Fixture::setup_with_arena(arena).await?;

        // There might not be any mtime at this point, but dir_mtime should not fail.
        fixture.cache.dir_mtime(Inode(1))?;
        fixture.cache.dir_mtime(fixture.cache.arena_root(arena)?)?;

        let (documents, _) = fixture.cache.lookup(Inode(1), "documents")?;
        fixture.cache.dir_mtime(documents)?;

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

        let (arenas, assignment) = cache.lookup(Inode(1), "arenas")?;
        assert_eq!(assignment, InodeAssignment::Directory);

        let (_, assignment) = cache.lookup(Inode(1), "other")?;
        assert_eq!(assignment, InodeAssignment::Directory);

        let (_, assignment) = cache.lookup(arenas, "test1")?;
        assert_eq!(assignment, InodeAssignment::Directory);

        let (_, assignment) = cache.lookup(arenas, "test2")?;
        assert_eq!(assignment, InodeAssignment::Directory);

        Ok(())
    }

    #[tokio::test]
    async fn lookup_returns_notfound_for_missing_entry() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let cache = &fixture.cache;

        assert!(matches!(
            cache.lookup(UnrealCacheBlocking::ROOT_DIR, "nonexistent"),
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
                ("arenas".to_string(), InodeAssignment::Directory),
                ("other".to_string(), InodeAssignment::Directory)
            ],
            cache
                .readdir(Inode(1))?
                .into_iter()
                .map(|(name, _, assignment)| (name, assignment))
                .collect::<Vec<_>>(),
        );

        let (arenas, _) = cache.lookup(Inode(1), "arenas")?;
        assert_unordered::assert_eq_unordered!(
            vec![
                ("test1".to_string(), InodeAssignment::Directory),
                ("test2".to_string(), InodeAssignment::Directory)
            ],
            cache
                .readdir(arenas)?
                .into_iter()
                .map(|(name, _, assignment)| (name, assignment))
                .collect::<Vec<_>>(),
        );

        Ok(())
    }
}
