//! The Unreal cache - a partial local cache of remote files.
//!
//! See `spec/unreal.md` for details.

use super::db::{GlobalDatabase, GlobalReadTransaction, GlobalWriteTransaction};
use super::types::{FileAvailability, FileMetadata, InodeAssignment, ReadDirEntry};
use crate::arena::arena_cache::{self, ArenaCache};
use crate::arena::notifier::{Notification, Progress};
use crate::arena::types::LocalAvailability;
use crate::{ArenaDatabase, Blob, DirtyPaths, Inode, StorageError};
use bimap::BiMap;
use realize_types::{Arena, Hash, Path, Peer, UnixTime};
use redb::ReadableTable;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::task;

/// Size of allocated ranges.
const RANGE_SIZE: u64 = 10000;

/// A cache of remote files.
pub struct UnrealCacheBlocking {
    db: Arc<GlobalDatabase>,
    arena_roots: BiMap<Arena, Inode>,
    arena_caches: HashMap<Arena, ArenaCache>,
}

impl UnrealCacheBlocking {
    /// Inode of the root dir.
    pub const ROOT_DIR: Inode = Inode(1);

    /// Create a new UnrealCache from a redb database.
    pub(crate) fn new(db: Arc<GlobalDatabase>) -> Result<Self, StorageError> {
        let arena_map = {
            let txn = db.begin_read()?;
            do_read_arena_map(&txn)?
        };

        Ok(Self {
            db,
            arena_roots: arena_map,
            arena_caches: HashMap::new(),
        })
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
        self.arena_roots
            .get_by_left(&arena)
            .copied()
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
            .arena_for_inode(&self.db.begin_read()?, inode)?
            .ok_or(StorageError::NotFound)?;
        self.arena_cache(arena)
    }

    /// Add an arena to the database.
    pub(crate) fn add_arena(
        &mut self,
        arena: Arena,
        db: Arc<ArenaDatabase>,
        blob_dir: PathBuf,
        dirty_paths: Arc<DirtyPaths>,
    ) -> anyhow::Result<()> {
        let arena_root = match self.arena_roots.get_by_left(&arena) {
            Some(inode) => *inode,
            None => {
                for existing in self.arena_roots.left_values() {
                    check_arena_compatibility(arena, *existing)?;
                }

                let txn = self.db.begin_write()?;
                let arena_root = do_add_arena_root(&txn, arena)?;
                txn.commit()?;

                self.arena_roots.insert(arena, arena_root);

                arena_root
            }
        };
        self.arena_caches.insert(
            arena,
            ArenaCache::new(arena, arena_root, db, blob_dir, dirty_paths)?,
        );

        Ok(())
    }

    /// Transform this cache into an async cache.
    pub fn into_async(self) -> UnrealCacheAsync {
        UnrealCacheAsync::new(self)
    }

    /// Lookup a directory entry.
    pub fn lookup(&self, parent_inode: Inode, name: &str) -> Result<ReadDirEntry, StorageError> {
        let txn = self.db.begin_read()?;
        match self.arena_for_inode(&txn, parent_inode)? {
            Some(arena) => self.arena_cache(arena)?.lookup(parent_inode, name),
            None => arena_cache::do_lookup(&txn.directory_table()?, parent_inode, name),
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
        match self.arena_for_inode(&txn, inode)? {
            None => arena_cache::do_dir_mtime(
                &txn.directory_table()?,
                inode,
                UnrealCacheBlocking::ROOT_DIR,
            ),
            Some(arena) => self.arena_cache(arena)?.dir_mtime(inode),
        }
    }

    pub fn readdir(&self, inode: Inode) -> Result<Vec<(String, ReadDirEntry)>, StorageError> {
        let txn = self.db.begin_read()?;
        match self.arena_for_inode(&txn, inode)? {
            None => arena_cache::do_readdir(&txn.directory_table()?, inode),
            Some(arena) => self.arena_cache(arena)?.readdir(inode),
        }
    }

    pub(crate) fn update(
        &self,
        peer: Peer,
        notification: Notification,
    ) -> Result<(), StorageError> {
        let arena = notification.arena();
        let cache = self.arena_cache(arena)?;
        cache.update(peer, notification, || self.alloc_inode_range(arena))
    }

    /// Allocate a new range of inodes to an arena.
    ///
    /// This function allocates a range of `range_size` inodes to the given arena.
    /// It finds the last allocated range and allocates the next available range.
    ///
    /// Returns the allocated range as (start, end) where end is exclusive.
    fn alloc_inode_range(&self, arena: Arena) -> Result<(Inode, Inode), StorageError> {
        let arena_root = self.arena_root(arena)?;

        let txn = self.db.begin_write()?;
        let ret = do_alloc_inode_range(&txn, arena_root, RANGE_SIZE)?;

        // If the transaction inside the arena cache fails to commit,
        // the range is lost. TODO: find a way of avoiding such
        // issues.
        txn.commit()?;

        Ok(ret)
    }

    /// Lookup the arena assigned to `inode`, fail if it has no assigned
    /// arenas yet.
    ///
    /// None is for global nodes, such as the root node and intermediate
    /// directories created for arenas with compound names, such as
    /// "documents/others". Note that inodes of arena roots are assigned
    /// to their own arena.
    fn arena_for_inode(
        &self,
        txn: &GlobalReadTransaction,
        inode: Inode,
    ) -> Result<Option<Arena>, StorageError> {
        if inode == UnrealCacheBlocking::ROOT_DIR {
            return Ok(None);
        }
        if let Some(arena) = self.arena_roots.get_by_right(&inode) {
            return Ok(Some(*arena));
        }

        let range_table = txn.inode_range_allocation_table()?;
        for entry in range_table.range(inode..)? {
            let (_, root) = entry?;
            let root = root.value();
            if root == UnrealCacheAsync::ROOT_DIR {
                return Ok(None);
            } else if let Some(arena) = self.arena_roots.get_by_right(&root) {
                return Ok(Some(*arena));
            }
        }

        Err(StorageError::NotFound)
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
        arenas: T,
    ) -> Result<Self, anyhow::Error>
    where
        T: IntoIterator<Item = (Arena, Arc<ArenaDatabase>, PathBuf, Arc<DirtyPaths>)>
            + Send
            + 'static,
    {
        task::spawn_blocking(move || {
            let mut cache = UnrealCacheBlocking::new(db)?;
            for (arena, db, blob_dir, dirty_paths) in arenas.into_iter() {
                cache.add_arena(arena, db, blob_dir, dirty_paths)?;
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
    ) -> Result<ReadDirEntry, StorageError> {
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

    pub async fn readdir(&self, inode: Inode) -> Result<Vec<(String, ReadDirEntry)>, StorageError> {
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

    pub async fn move_blob(
        &self,
        arena: Arena,
        path: &Path,
        hash: &Hash,
        dest: &std::path::Path,
    ) -> Result<bool, StorageError> {
        let inner = Arc::clone(&self.inner);
        let path = path.clone();
        let hash = hash.clone();
        let dest = dest.to_path_buf();

        task::spawn_blocking(move || {
            let arena_cache = inner.arena_cache(arena)?;
            arena_cache.move_blob(&path, &hash, &dest)
        })
        .await?
    }
}

fn do_read_arena_map(txn: &GlobalReadTransaction) -> Result<BiMap<Arena, Inode>, StorageError> {
    let mut map = BiMap::new();
    let arena_table = txn.arena_table()?;
    for elt in arena_table.iter()? {
        let (k, v) = elt?;
        let arena = Arena::from(k.value());
        let inode = v.value();
        map.insert(arena, inode);
    }

    Ok(map)
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

fn do_add_arena_root(txn: &GlobalWriteTransaction, arena: Arena) -> anyhow::Result<Inode> {
    let mut arena_table = txn.arena_table()?;
    let arena_str = arena.as_str();
    if let Some(v) = arena_table.get(arena_str)? {
        return Ok(v.value());
    }
    let mut dir_table = txn.directory_table()?;
    let mut current_range_table = txn.current_inode_range_table()?;
    let arena_path = Path::parse(arena.as_str())?;
    let alloc_inode_range = || do_alloc_inode_range(txn, UnrealCacheBlocking::ROOT_DIR, 100);
    let parent_inode = arena_cache::do_mkdirs(
        &mut current_range_table,
        &mut dir_table,
        UnrealCacheBlocking::ROOT_DIR,
        arena_path.parent().as_ref(),
        &alloc_inode_range,
    )?;
    let arena_root = arena_cache::alloc_inode(&mut current_range_table, &alloc_inode_range)?;
    arena_cache::add_dir_entry(
        &mut dir_table,
        parent_inode,
        arena_root,
        arena_path.name(),
        InodeAssignment::Directory,
    )?;
    arena_table.insert(arena.as_str(), arena_root)?;

    log::debug!("Arena root {arena}: {arena_root}");

    Ok(arena_root)
}

fn do_alloc_inode_range(
    txn: &GlobalWriteTransaction,
    assigned_root: Inode,
    range_size: u64,
) -> Result<(Inode, Inode), StorageError> {
    let mut range_table = txn.inode_range_allocation_table()?;

    let last_end = range_table
        .last()?
        .map(|(key, _)| key.value())
        .unwrap_or(Inode(1));

    let new_range_start = last_end.plus(1);
    let new_range_end = new_range_start.plus(range_size);
    range_table.insert(new_range_end.minus(1), assigned_root)?;

    Ok((new_range_start, new_range_end))
}

#[cfg(test)]
mod tests {
    use crate::utils::redb_utils;

    use super::*;
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use realize_types::Arena;

    fn test_arena() -> Arena {
        Arena::from("test_arena")
    }

    struct Fixture {
        cache: UnrealCacheBlocking,
        tempdir: TempDir,
    }
    impl Fixture {
        fn setup() -> anyhow::Result<Fixture> {
            let _ = env_logger::try_init();
            let tempdir = TempDir::new()?;
            let cache = UnrealCacheBlocking::new(GlobalDatabase::new(redb_utils::in_memory()?)?)?;
            Ok(Self { cache, tempdir })
        }

        async fn setup_with_arena(arena: Arena) -> anyhow::Result<Fixture> {
            let mut fixture = Self::setup()?;
            fixture.add_arena(arena).await?;

            Ok(fixture)
        }

        async fn add_arena(&mut self, arena: Arena) -> anyhow::Result<()> {
            let blob_dir = self.tempdir.child(format!("{arena}/blobs"));
            blob_dir.create_dir_all()?;
            let db = ArenaDatabase::new(redb_utils::in_memory()?)?;
            let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;
            self.cache
                .add_arena(arena, db, blob_dir.to_path_buf(), dirty_paths)?;
            Ok(())
        }

        /// Get the current inode range in the current database.
        /// If no range exists, returns None.
        fn get_current_inode_range(&self) -> Result<Option<(Inode, Inode)>, StorageError> {
            let txn = self.cache.db.begin_write()?;
            let range_table = txn.current_inode_range_table()?;

            match range_table.get(())? {
                Some(value) => {
                    let (current, end) = value.value();
                    Ok(Some((current, end)))
                }
                None => Ok(None),
            }
        }
    }

    #[tokio::test]
    async fn initial_dir_mtime() -> anyhow::Result<()> {
        let arena = Arena::from("documents/letters");
        let fixture = Fixture::setup_with_arena(arena).await?;

        // There might not be any mtime at this point, but dir_mtime should not fail.
        fixture.cache.dir_mtime(Inode(1))?;
        fixture.cache.dir_mtime(fixture.cache.arena_root(arena)?)?;

        let documents = fixture.cache.lookup(Inode(1), "documents")?.inode;
        fixture.cache.dir_mtime(documents)?;

        Ok(())
    }

    #[tokio::test]
    async fn lookup_finds_entry() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup()?;
        fixture.add_arena(Arena::from("arenas/test1")).await?;
        fixture.add_arena(Arena::from("arenas/test2")).await?;
        fixture.add_arena(Arena::from("other")).await?;

        let cache = &fixture.cache;

        let arenas = cache.lookup(Inode(1), "arenas")?;
        assert_eq!(arenas.assignment, InodeAssignment::Directory);

        let other = cache.lookup(Inode(1), "other")?;
        assert_eq!(other.assignment, InodeAssignment::Directory);

        let test1 = cache.lookup(arenas.inode, "test1")?;
        assert_eq!(test1.assignment, InodeAssignment::Directory);

        let test2 = cache.lookup(arenas.inode, "test2")?;
        assert_eq!(test2.assignment, InodeAssignment::Directory);

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
        let mut fixture = Fixture::setup()?;
        fixture.add_arena(Arena::from("arenas/test1")).await?;
        fixture.add_arena(Arena::from("arenas/test2")).await?;
        fixture.add_arena(Arena::from("other")).await?;

        let cache = &fixture.cache;
        assert_unordered::assert_eq_unordered!(
            vec![
                ("arenas".to_string(), InodeAssignment::Directory),
                ("other".to_string(), InodeAssignment::Directory)
            ],
            cache
                .readdir(Inode(1))?
                .into_iter()
                .map(|(name, entry)| (name, entry.assignment))
                .collect::<Vec<_>>(),
        );

        let arenas = cache.lookup(Inode(1), "arenas")?;
        assert_unordered::assert_eq_unordered!(
            vec![
                ("test1".to_string(), InodeAssignment::Directory),
                ("test2".to_string(), InodeAssignment::Directory)
            ],
            cache
                .readdir(arenas.inode)?
                .into_iter()
                .map(|(name, entry)| (name, entry.assignment))
                .collect::<Vec<_>>(),
        );

        Ok(())
    }

    #[tokio::test]
    async fn alloc_inode() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup()?;

        assert!(fixture.get_current_inode_range()?.is_none());

        fixture.add_arena(Arena::from("others")).await?;
        assert_eq!(
            Some((Inode(2), Inode(102))),
            fixture.get_current_inode_range()?
        );
        assert_eq!(Inode(2), fixture.cache.arena_root(Arena::from("others"))?);

        fixture.add_arena(Arena::from("arenas/test1")).await?;
        assert_eq!(
            Some((Inode(4), Inode(102))),
            fixture.get_current_inode_range()?
        );
        assert_eq!(
            Inode(4),
            fixture.cache.arena_root(Arena::from("arenas/test1"))?
        );

        fixture.add_arena(Arena::from("arenas/test2")).await?;
        assert_eq!(
            Some((Inode(5), Inode(102))),
            fixture.get_current_inode_range()?
        );
        assert_eq!(
            Inode(5),
            fixture.cache.arena_root(Arena::from("arenas/test2"))?
        );

        // Nearly exhaust the range
        {
            let txn = fixture.cache.db.begin_write()?;
            {
                let mut range_table = txn.current_inode_range_table()?;
                range_table.insert((), (Inode(100), Inode(102)))?;
            }
            txn.commit()?;
        }

        // Next allocation exhausts the range
        fixture.add_arena(Arena::from("another")).await?;
        assert_eq!(
            Some((Inode(101), Inode(102))),
            fixture.get_current_inode_range()?
        );
        assert_eq!(
            Inode(101),
            fixture.cache.arena_root(Arena::from("another"))?
        );

        // Next inode allocation require a new range allocation.
        fixture.add_arena(Arena::from("theone")).await?;
        assert_eq!(
            Some((Inode(102), Inode(202))),
            fixture.get_current_inode_range()?
        );
        assert_eq!(Inode(102), fixture.cache.arena_root(Arena::from("theone"))?);

        Ok(())
    }
}
