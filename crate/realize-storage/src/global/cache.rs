//! The Unreal cache - a partial local cache of remote files.
//!
//! See `spec/unreal.md` for details.

use super::db::GlobalDatabase;
use super::inode_allocator::InodeAllocator;
use super::types::InodeAssignment;
use crate::arena::arena_cache::ArenaCache;
use crate::arena::notifier::{Notification, Progress};
use crate::arena::types::{DirMetadata, LocalAvailability};
use crate::global::db::GlobalWriteTransaction;
use crate::global::types::PathTableEntry;
use crate::utils::holder::Holder;
use crate::{Blob, FileAvailability, FileMetadata, Inode, StorageError};
use realize_types::{Arena, Path, Peer, UnixTime};
use redb::ReadableTable;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task;

/// A cache of remote files.
pub struct GlobalCache {
    db: Arc<GlobalDatabase>,
    allocator: Arc<InodeAllocator>,

    arena_caches: HashMap<Arena, Arc<ArenaCache>>,

    /// Inodes to intermediate paths, before arenas, includes root.
    paths: HashMap<Inode, IntermediatePath>,
}

impl GlobalCache {
    /// Inode of the root dir.
    pub const ROOT_DIR: Inode = InodeAllocator::ROOT_INODE;

    /// Create a new cache with the database at the given path.
    pub(crate) async fn with_db<T>(
        db: Arc<GlobalDatabase>,
        allocator: Arc<InodeAllocator>,
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
                    InodeAllocator::ROOT_INODE,
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
    fn arena_cache(&self, arena: Arena) -> Result<&ArenaCache, StorageError> {
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
    /// Lookup a directory entry.
    pub async fn lookup(
        self: &Arc<Self>,
        parent_inode: Inode,
        name: &str,
    ) -> Result<(Inode, InodeAssignment), StorageError> {
        let name = name.to_string();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            match this.allocator.arena_for_inode(&txn, parent_inode)? {
                Some(arena) => {
                    let cache = this.arena_cache(arena)?;
                    cache.lookup(parent_inode, &name)
                }
                None => match this.paths.get(&parent_inode) {
                    None => Err(StorageError::NotFound),
                    Some(IntermediatePath { entries, .. }) => entries
                        .get(&name)
                        .map(|inode| (*inode, InodeAssignment::Directory))
                        .ok_or(StorageError::NotFound),
                },
            }
        })
        .await?
    }

    /// Lookup the inode and type of the file or directory pointed to
    /// by a path.
    ///
    /// Fail with [StorageError::NotFound] if the path is not found in the tree. Note
    /// that a successful return doesn't mean that a file or directory
    /// with that inode actually exists in the cache.
    pub async fn expect(
        self: &Arc<Self>,
        arena: Arena,
        path: &Path,
    ) -> Result<Inode, StorageError> {
        let path = path.clone();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let cache = this.arena_cache(arena)?;
            cache.expect(path)
        })
        .await?
    }

    /// Lookup the inode and type of the file or directory pointed to by a path.
    ///
    /// Return Non if the path is not found in the tree. Note that a
    /// successful return doesn't mean that a file or directory with
    /// that inode actually exists in the cache.
    pub async fn resolve(
        self: &Arc<Self>,
        arena: Arena,
        path: &Path,
    ) -> Result<Option<Inode>, StorageError> {
        let path = path.clone();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let cache = this.arena_cache(arena)?;
            cache.resolve(path)
        })
        .await?
    }

    /// Return the mtime of the directory.
    pub async fn dir_metadata(self: &Arc<Self>, inode: Inode) -> Result<DirMetadata, StorageError> {
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            match this.allocator.arena_for_inode(&txn, inode)? {
                Some(arena) => {
                    let cache = this.arena_cache(arena)?;
                    cache.dir_metadata(inode)
                }
                None => match this.paths.get(&inode) {
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

    pub async fn readdir(
        self: &Arc<Self>,
        inode: Inode,
    ) -> Result<Vec<(String, Inode, InodeAssignment)>, StorageError> {
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            match this.allocator.arena_for_inode(&txn, inode)? {
                Some(arena) => {
                    let cache = this.arena_cache(arena)?;
                    cache.readdir(inode)
                }
                None => match this.paths.get(&inode) {
                    None => Err(StorageError::NotFound),
                    Some(IntermediatePath { entries, .. }) => Ok(entries
                        .iter()
                        .map(|(name, inode)| (name.to_string(), *inode, InodeAssignment::Directory))
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
    pub async fn local_availability(
        self: &Arc<Self>,
        inode: Inode,
    ) -> Result<LocalAvailability, StorageError> {
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let cache = this.arena_cache_for_inode(inode)?;
            cache.local_availability(inode)
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
    pub async fn open_file(self: &Arc<Self>, inode: Inode) -> Result<Blob, StorageError> {
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let cache = this.arena_cache_for_inode(inode)?;
            cache.open_file(inode)
        })
        .await?
    }

    pub async fn file_metadata(
        self: &Arc<Self>,
        inode: Inode,
    ) -> Result<FileMetadata, StorageError> {
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let cache = this.arena_cache_for_inode(inode)?;
            cache.file_metadata(inode)
        })
        .await?
    }

    pub async fn file_availability(
        self: &Arc<Self>,
        inode: Inode,
    ) -> Result<FileAvailability, StorageError> {
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let cache = this.arena_cache_for_inode(inode)?;
            cache.file_availability(inode)
        })
        .await?
    }

    pub async fn unlink(
        self: &Arc<Self>,
        parent_inode: Inode,
        name: &str,
    ) -> Result<(), StorageError> {
        let name = name.to_string();
        let this = Arc::clone(self);

        task::spawn_blocking(move || {
            let txn = this.db.begin_read()?;
            match this.allocator.arena_for_inode(&txn, parent_inode)? {
                Some(arena) => {
                    let cache = this.arena_cache(arena)?;
                    cache.unlink(parent_inode, &name)
                }
                None => {
                    if this
                        .paths
                        .get(&parent_inode)
                        .and_then(|paths| paths.entries.get(&name))
                        .is_some()
                    {
                        Err(StorageError::IsADirectory)
                    } else {
                        Err(StorageError::NotFound)
                    }
                }
            }
        })
        .await?
    }
}

#[derive(Debug, Clone)]
struct IntermediatePath {
    entries: HashMap<String, Inode>,
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
/// Calls for inode assigned to the cache arena will be directed there.
fn register(
    cache: Arc<ArenaCache>,
    txn: &GlobalWriteTransaction,
    path_table: &mut redb::Table<&'static str, Holder<'static, PathTableEntry>>,
    allocator: &Arc<InodeAllocator>,
    arena_caches: &mut HashMap<Arena, Arc<ArenaCache>>,
    paths: &mut HashMap<Inode, IntermediatePath>,
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
    arena_root: Inode,
    txn: &GlobalWriteTransaction,
    path_table: &mut redb::Table<&'static str, Holder<'static, PathTableEntry>>,
    allocator: &Arc<InodeAllocator>,
    paths: &mut HashMap<Inode, IntermediatePath>,
) -> anyhow::Result<()> {
    let arena_path = Path::parse(arena.as_str())?;
    let mut names = Path::components(Some(&arena_path))
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .chain(/* root */ std::iter::once(""));
    let mut current_inode = arena_root;
    let mut current_name = names.next().unwrap();
    for dirname in names {
        let entry = get_or_add_path_entry(txn, path_table, allocator, dirname)?;
        add_intermediate_path_entry(entry.inode, entry.mtime, current_name, current_inode, paths);
        current_name = dirname;
        current_inode = entry.inode;
    }

    Ok(())
}

fn get_or_add_path_entry(
    txn: &GlobalWriteTransaction,
    path_table: &mut redb::Table<'_, &'static str, Holder<'static, PathTableEntry>>,
    allocator: &Arc<InodeAllocator>,
    dirname: &str,
) -> Result<PathTableEntry, anyhow::Error> {
    let entry = if let Some(e) = path_table.get(dirname)? {
        e.value().parse()?
    } else {
        let entry_inode = if dirname == "" {
            InodeAllocator::ROOT_INODE
        } else {
            allocator.allocate_global_inode(&txn)?
        };
        let entry = PathTableEntry {
            inode: entry_inode,
            mtime: UnixTime::now(),
        };
        path_table.insert(dirname, Holder::new(&entry)?)?;

        entry
    };
    Ok(entry)
}

/// Adds an entry in the intermediate path with the given inode.
///
/// If no such intermediate path exists, it is added.
fn add_intermediate_path_entry(
    inode: Inode,
    mtime: UnixTime,
    entry_name: &str,
    entry_inode: Inode,
    paths: &mut HashMap<Inode, IntermediatePath>,
) {
    let mapping = paths.entry(inode).or_insert_with(|| IntermediatePath {
        entries: HashMap::new(),
        mtime,
    });
    mapping.entries.insert(entry_name.to_string(), entry_inode);
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
            let allocator = InodeAllocator::new(Arc::clone(&db), arenas.clone())?;
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

        assert!(fixture.cache.readdir(Inode(1)).await?.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn empty_cache_metadata() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arenas([]).await?;

        let m = fixture.cache.dir_metadata(Inode(1)).await?;
        assert!(m.read_only);
        assert_ne!(UnixTime::ZERO, m.mtime);

        Ok(())
    }

    #[tokio::test]
    async fn initial_dir_mtime() -> anyhow::Result<()> {
        let arena = Arena::from("documents/letters");
        let fixture = Fixture::setup_with_arena(arena).await?;

        let root_m = fixture.cache.dir_metadata(Inode(1)).await?;
        assert!(root_m.read_only);
        assert_ne!(UnixTime::ZERO, root_m.mtime);

        let (documents, _) = fixture.cache.lookup(Inode(1), "documents").await?;
        let documents_m = fixture.cache.dir_metadata(documents).await?;
        assert!(documents_m.read_only);
        assert_ne!(UnixTime::ZERO, documents_m.mtime);

        let (letters, _) = fixture.cache.lookup(documents, "letters").await?;
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

        let (arenas, assignment) = cache.lookup(Inode(1), "arenas").await?;
        assert_eq!(assignment, InodeAssignment::Directory);

        let (_, assignment) = cache.lookup(Inode(1), "other").await?;
        assert_eq!(assignment, InodeAssignment::Directory);

        let (_, assignment) = cache.lookup(arenas, "test1").await?;
        assert_eq!(assignment, InodeAssignment::Directory);

        let (_, assignment) = cache.lookup(arenas, "test2").await?;
        assert_eq!(assignment, InodeAssignment::Directory);

        Ok(())
    }

    #[tokio::test]
    async fn lookup_returns_notfound_for_missing_entry() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let cache = &fixture.cache;

        assert!(matches!(
            cache.lookup(GlobalCache::ROOT_DIR, "nonexistent").await,
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
                .readdir(Inode(1))
                .await?
                .into_iter()
                .map(|(name, _, assignment)| (name, assignment))
                .collect::<Vec<_>>(),
        );

        let (arenas, _) = cache.lookup(Inode(1), "arenas").await?;
        assert_unordered::assert_eq_unordered!(
            vec![
                ("test1".to_string(), InodeAssignment::Directory),
                ("test2".to_string(), InodeAssignment::Directory)
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
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let cache = &fixture.cache;

        assert!(matches!(
            cache.unlink(GlobalCache::ROOT_DIR, "test_arena").await,
            Err(StorageError::IsADirectory)
        ));
        assert!(matches!(
            cache.unlink(GlobalCache::ROOT_DIR, "doesnotexist").await,
            Err(StorageError::NotFound)
        ));

        // This just checks that the call is dispatched down to the
        // arena cache.
        assert!(matches!(
            cache
                .unlink(cache.arena_root(test_arena())?, "doesnotexist")
                .await,
            Err(StorageError::NotFound)
        ));

        Ok(())
    }
}
