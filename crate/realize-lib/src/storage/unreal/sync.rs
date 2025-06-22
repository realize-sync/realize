//! The Unreal cache - a partial local cache of remote files.
//!
//! See `spec/unreal.md` for details.

use super::{
    DirTableEntry, FileMetadata, FileTableEntry, InodeAssignment, ReadDirEntry, UnrealCacheAsync,
    UnrealError, ROOT_DIR,
};
use crate::model::{Arena, Path, Peer, UnixTime};
use crate::storage::unreal::FileContent;
use crate::utils::holder::Holder;
use redb::{Database, ReadTransaction, ReadableTable, TableDefinition, WriteTransaction};
use std::collections::HashMap;
use std::path;

/// Maps arena to their root directory inode.
///
/// Arenas in this table can also be accessed as subdirectories of the
/// root directory (1).
///
/// Key: arena name
/// Value: root inode of arena
const ARENA_TABLE: TableDefinition<&str, u64> = TableDefinition::new("arena");

/// Tracks directory content.
///
/// Each entry in a directory has an entry in this table, keyed with
/// the directory inode and the entry name.
///
/// To list directory content, do a range scan.
///
/// The special name "." store information about the current
/// directory, in a DirTableEntry::Self.
///
/// Key: (inode, name)
/// Value: DirTableEntry
const DIRECTORY_TABLE: TableDefinition<(u64, &str), Holder<DirTableEntry>> =
    TableDefinition::new("directory");

/// Track peer files.
///
/// Each known peer file has an entry in this table, keyed with the
/// file inode and the peer name. More than one peer might have the
/// same entry.
///
/// An inode available in no peers should be remove from all
/// directories. This is handled by [do_rm_file_entry].
///
/// Key: (inode, peer)
/// Value: FileEntry
const FILE_TABLE: TableDefinition<(u64, &str), Holder<FileTableEntry>> =
    TableDefinition::new("file");

/// Track max inode.
///
/// This table contains a single entry, whose value is the maximum
/// inode that's been used in the database. It defaults to 1, the root
/// inode.
///
/// This table is used by [alloc_inode].
///
/// Key: ()
/// Value: inode
const MAX_INODE_TABLE: TableDefinition<(), u64> = TableDefinition::new("max_inode");

/// Track peer files that might have been deleted remotely.
///
/// When a peer starts catchup of an arena, all its files are added to
/// this table. Calls to catchup for that peer and arena removes the
/// corresponding entry in the table. At the end of catchup, files
/// still in this table are deleted.
///
/// This is handled by [do_mark_peer_files], [do_delete_marked_files]
/// and [do_unmark_peer_file].
///
/// Key: (peer, arena, file inode)
/// Value: parent dir inode
const PENDING_CATCHUP_TABLE: TableDefinition<(&str, &str, u64), u64> =
    TableDefinition::new("pending_catchup");

/// A cache of remote files.
pub struct UnrealCacheBlocking {
    db: Database,
    arena_map: HashMap<Arena, u64>,
}

impl UnrealCacheBlocking {
    /// Create a new UnrealCache from a redb database.
    pub fn new(db: Database) -> Result<Self, UnrealError> {
        {
            let txn = db.begin_write()?;
            txn.open_table(ARENA_TABLE)?;
            txn.open_table(DIRECTORY_TABLE)?;
            txn.open_table(FILE_TABLE)?;
            txn.open_table(PENDING_CATCHUP_TABLE)?;
            txn.commit()?;
        }

        let arena_map = {
            let txn = db.begin_read()?;
            do_read_arena_map(&txn)?
        };

        Ok(Self { db, arena_map })
    }

    /// Open or create an UnrealCache at the given path.
    pub fn open(path: &path::Path) -> Result<Self, UnrealError> {
        Self::new(Database::create(path)?)
    }

    /// Lists arenas available in this database
    pub fn arenas(&self) -> impl Iterator<Item = &Arena> {
        self.arena_map.keys()
    }

    /// Returns the inode of an arena.
    ///
    /// Will return [UnrealCacheError::UnknownArena] unless the arena
    /// is available in the cache.
    pub fn arena_root(&self, arena: &Arena) -> Result<u64, UnrealError> {
        self.arena_map
            .get(arena)
            .map(|inode| *inode)
            .ok_or_else(|| UnrealError::UnknownArena(arena.clone()))
    }

    /// Add an arena to the database.
    pub fn add_arena(&mut self, arena: &Arena) -> anyhow::Result<()> {
        if self.arena_map.contains_key(arena) {
            return Ok(());
        }

        for existing in self.arena_map.keys() {
            check_arena_compatibility(arena, existing)?;
        }

        let inode: u64;
        {
            let txn = self.db.begin_write()?;
            inode = do_add_arena(&txn, arena)?;
            txn.commit()?;
        }

        self.arena_map.insert(arena.clone(), inode);

        Ok(())
    }

    /// Transform this cache into an async cache.
    pub fn into_async(self) -> UnrealCacheAsync {
        UnrealCacheAsync::new(self)
    }

    pub fn catchup(
        &self,
        peer: &Peer,
        arena: &Arena,
        path: &Path,
        size: u64,
        mtime: &UnixTime,
    ) -> Result<(), UnrealError> {
        let txn = self.db.begin_write()?;
        let inode = do_link(
            &txn,
            peer,
            arena,
            self.arena_root(arena)?,
            path,
            size,
            mtime,
        )?;
        do_unmark_peer_file(&txn, peer, arena, inode)?;
        txn.commit()?;

        Ok(())
    }

    pub fn link(
        &self,
        peer: &Peer,
        arena: &Arena,
        path: &Path,
        size: u64,
        mtime: &UnixTime,
    ) -> Result<(), UnrealError> {
        let txn = self.db.begin_write()?;
        do_link(
            &txn,
            peer,
            arena,
            self.arena_root(arena)?,
            path,
            size,
            mtime,
        )?;
        txn.commit()?;

        Ok(())
    }

    pub fn unlink(
        &self,
        peer: &Peer,
        arena: &Arena,
        path: &Path,
        mtime: &UnixTime,
    ) -> Result<(), UnrealError> {
        let txn = self.db.begin_write()?;
        do_unlink(&txn, peer, self.arena_root(arena)?, path, &mtime)?;
        txn.commit()?;
        Ok(())
    }

    /// Lookup a directory entry.
    pub fn lookup(&self, parent_inode: u64, name: &str) -> Result<ReadDirEntry, UnrealError> {
        let txn = self.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;

        Ok(dir_table
            .get((parent_inode, name))?
            .ok_or(UnrealError::NotFound)?
            .value()
            .parse()?
            .as_readdir_entry(parent_inode))
    }

    /// Lookup the inode and type of the file or directory pointed to by a path.
    pub fn lookup_path(
        &self,
        parent_inode: u64,
        path: &Path,
    ) -> Result<(u64, InodeAssignment), UnrealError> {
        let txn = self.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;

        do_lookup_path(&dir_table, parent_inode, &Some(path.clone()))
    }

    /// Return the best metadata for the file.
    pub fn file_metadata(&self, inode: u64) -> Result<FileMetadata, UnrealError> {
        let txn = self.db.begin_read()?;
        if let Some((_, entry)) = do_file_availability(&txn, inode)?.into_iter().next() {
            return Ok(entry.metadata);
        }

        Err(UnrealError::NotFound)
    }

    /// Return the mtime of the directory.
    pub fn dir_mtime(&self, inode: u64) -> Result<UnixTime, UnrealError> {
        let txn = self.db.begin_read()?;

        do_dir_mtime(&txn, inode)
    }

    /// Return valid peer file entries for the file.
    ///
    /// The returned vector might be empty if the file isn't available in any peer.
    pub fn file_availability(
        &self,
        inode: u64,
    ) -> Result<Vec<(Peer, FileTableEntry)>, UnrealError> {
        let txn = self.db.begin_read()?;

        do_file_availability(&txn, inode)
    }

    pub fn readdir(&self, inode: u64) -> Result<Vec<(String, ReadDirEntry)>, UnrealError> {
        let txn = self.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;

        // This is not ideal, but redb iterators are bound to the transaction.
        // We must collect the results.
        let mut entries = Vec::new();

        // The range will go from (inode, "") up to the next inode.
        for item in dir_table.range((inode, "")..)? {
            let (key, value) = item?;
            if key.value().0 != inode {
                break;
            }
            let name = key.value().1.to_string();
            if let DirTableEntry::Regular(entry) = value.value().parse()? {
                entries.push((name, entry.clone()));
            }
        }

        Ok(entries)
    }

    pub fn mark_peer_files(&self, peer: &Peer, arena: &Arena) -> Result<(), UnrealError> {
        let txn = self.db.begin_write()?;
        do_mark_peer_files(&txn, peer, arena)?;
        txn.commit()?;
        Ok(())
    }

    pub fn delete_marked_files(&self, peer: &Peer, arena: &Arena) -> Result<(), UnrealError> {
        let txn = self.db.begin_write()?;
        do_delete_marked_files(&txn, peer, arena)?;
        txn.commit()?;
        Ok(())
    }
}

fn do_dir_mtime(txn: &ReadTransaction, inode: u64) -> Result<UnixTime, UnrealError> {
    let dir_table = txn.open_table(DIRECTORY_TABLE)?;
    match dir_table.get((inode, "."))? {
        Some(e) => {
            if let DirTableEntry::Dot(mtime) = e.value().parse()? {
                return Ok(mtime);
            }
        }
        None => {
            if inode == ROOT_DIR {
                // When the filesystem is empty, the root dir might not
                // have a mtime. This is not an error.
                return Ok(UnixTime::ZERO);
            }
        }
    }

    Err(UnrealError::NotFound)
}

fn do_read_arena_map(txn: &ReadTransaction) -> Result<HashMap<Arena, u64>, UnrealError> {
    let mut map = HashMap::new();
    let arena_table = txn.open_table(ARENA_TABLE)?;
    for elt in arena_table.iter()? {
        let (k, v) = elt?;
        let arena = Arena::from(k.value());
        let inode = v.value();
        map.insert(arena, inode);
    }

    Ok(map)
}

fn check_arena_compatibility(arena: &Arena, existing: &Arena) -> anyhow::Result<()> {
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

fn do_add_arena(txn: &WriteTransaction, arena: &Arena) -> anyhow::Result<u64> {
    let mut arena_table = txn.open_table(ARENA_TABLE)?;
    let arena_str = arena.as_str();
    if let Some(v) = arena_table.get(arena_str)? {
        return Ok(v.value());
    }
    let mut dir_table = txn.open_table(DIRECTORY_TABLE)?;
    let inode = do_mkdirs(
        txn,
        &mut dir_table,
        ROOT_DIR,
        // TODO: constrain arena names to valid paths at creation, to get earlier error
        &Some(Path::parse(arena.as_str())?),
    )?;
    arena_table.insert(arena.as_str(), inode)?;

    Ok(inode)
}

/// Implement [UnrealCache::unlink] within a transaction.
fn do_unlink(
    txn: &WriteTransaction,
    peer: &Peer,
    arena_root: u64,
    path: &Path,
    mtime: &UnixTime,
) -> Result<(), UnrealError> {
    let mut dir_table = txn.open_table(DIRECTORY_TABLE)?;
    let (parent_inode, parent_assignment) = do_lookup_path(&dir_table, arena_root, &path.parent())?;
    if parent_assignment != InodeAssignment::Directory {
        return Err(UnrealError::NotADirectory);
    }

    let dir_entry =
        get_dir_entry(&dir_table, parent_inode, path.name())?.ok_or(UnrealError::NotFound)?;
    if dir_entry.assignment != InodeAssignment::File {
        return Err(UnrealError::IsADirectory);
    }

    let inode = dir_entry.inode;
    let mut file_table = txn.open_table(FILE_TABLE)?;
    do_rm_file_entry(
        &mut file_table,
        &mut dir_table,
        parent_inode,
        inode,
        peer,
        Some(mtime),
    )?;

    Ok(())
}

/// Get a [FileEntry] for a specific peer.
fn get_file_entry(
    file_table: &redb::Table<'_, (u64, &str), Holder<FileTableEntry>>,
    inode: u64,
    peer: &Peer,
) -> Result<Option<FileTableEntry>, UnrealError> {
    match file_table.get((inode, peer.as_str()))? {
        None => Ok(None),
        Some(e) => Ok(Some(e.value().parse()?)),
    }
}

fn do_file_availability(
    txn: &ReadTransaction,
    inode: u64,
) -> Result<Vec<(Peer, FileTableEntry)>, UnrealError> {
    let file_table = txn.open_table(FILE_TABLE)?;

    let mut all = vec![];
    for entry in file_table.range((inode, "")..(inode + 1, ""))? {
        let entry = entry?;
        let peer = Peer::from(entry.0.value().1);
        let file_entry: FileTableEntry = entry.1.value().parse()?;
        all.push((peer, file_entry));
    }

    if let Some(best_mtime) = all.iter().map(|(_, e)| e.metadata.mtime.clone()).max() {
        all.retain(|(_, e)| e.metadata.mtime == best_mtime);
    }

    Ok(all)
}

/// Implement [UnrealCache::link] in a transaction.
///
/// Return the inode of the file.
fn do_link(
    txn: &WriteTransaction,
    peer: &Peer,
    arena: &Arena,
    arena_root: u64,
    path: &Path,
    size: u64,
    mtime: &UnixTime,
) -> Result<u64, UnrealError> {
    let mut dir_table = txn.open_table(DIRECTORY_TABLE)?;
    let mut file_table = txn.open_table(FILE_TABLE)?;

    let filename = path.name();
    let parent_inode = do_mkdirs(txn, &mut dir_table, arena_root, &path.parent())?;
    let dir_entry = get_dir_entry(&dir_table, parent_inode, filename)?;
    let inode = match dir_entry {
        None => add_dir_entry(
            txn,
            &mut dir_table,
            parent_inode,
            filename,
            InodeAssignment::File,
        )?,
        Some(dir_entry) => {
            if let Some(existing) = get_file_entry(&file_table, dir_entry.inode, peer)? {
                if existing.metadata.mtime > *mtime {
                    return Ok(dir_entry.inode);
                }
            }

            dir_entry.inode
        }
    };
    log::debug!("new file entry ({inode} {peer})");
    file_table.insert(
        (inode, peer.as_str()),
        Holder::new(FileTableEntry {
            metadata: FileMetadata {
                size,
                mtime: mtime.clone(),
            },
            content: FileContent {
                arena: arena.clone(),
                path: path.clone(),
            },
            parent_inode,
        })?,
    )?;

    Ok(inode)
}

/// Make sure that the given path is a directory; create it if necessary.
///
/// Returns the inode of the directory pointed to by the path.
fn do_mkdirs(
    txn: &WriteTransaction,
    dir_table: &mut redb::Table<'_, (u64, &str), Holder<DirTableEntry>>,
    root_inode: u64,
    path: &Option<Path>,
) -> Result<u64, UnrealError> {
    log::debug!("mkdirs {root_inode} {path:?}");
    let mut current = root_inode;
    for component in Path::components(path) {
        current = if let Some(entry) = get_dir_entry(dir_table, current, component)? {
            if entry.assignment != InodeAssignment::Directory {
                return Err(UnrealError::NotADirectory);
            }
            log::debug!("found {component} in {current} -> {entry:?}");
            entry.inode
        } else {
            log::debug!("add {component} in {current}");
            add_dir_entry(
                txn,
                dir_table,
                current,
                component,
                InodeAssignment::Directory,
            )?
        };
        log::debug!("current={current}");
    }

    Ok(current)
}

/// Find the file or directory pointed to by the given path.
fn do_lookup_path(
    dir_table: &impl redb::ReadableTable<(u64, &'static str), Holder<'static, DirTableEntry>>,
    root_inode: u64,
    path: &Option<Path>,
) -> Result<(u64, InodeAssignment), UnrealError> {
    let mut current = (root_inode, InodeAssignment::Directory);
    for component in Path::components(path) {
        if current.1 != InodeAssignment::Directory {
            return Err(UnrealError::NotADirectory);
        }
        if let Some(entry) = get_dir_entry(dir_table, current.0, component)? {
            current = (entry.inode, entry.assignment);
        } else {
            return Err(UnrealError::NotFound);
        };
    }

    Ok(current)
}

/// Get a [ReadDirEntry] from a directory, if it exists.
fn get_dir_entry(
    dir_table: &impl redb::ReadableTable<(u64, &'static str), Holder<'static, DirTableEntry>>,
    parent_inode: u64,
    name: &str,
) -> Result<Option<ReadDirEntry>, UnrealError> {
    match dir_table.get((parent_inode, name))? {
        None => Ok(None),
        Some(e) => Ok(Some(e.value().parse()?.as_readdir_entry(parent_inode))),
    }
}

/// Add an entry to the given directory.
fn add_dir_entry(
    txn: &WriteTransaction,
    dir_table: &mut redb::Table<'_, (u64, &str), Holder<DirTableEntry>>,
    parent_inode: u64,
    name: &str,
    assignment: InodeAssignment,
) -> Result<u64, UnrealError> {
    let new_inode = alloc_inode(txn)?;
    log::debug!("new dir entry {parent_inode} {name} -> {new_inode} {assignment:?}");
    dir_table.insert(
        (parent_inode, name),
        Holder::new(DirTableEntry::Regular(ReadDirEntry {
            inode: new_inode,
            assignment,
        }))?,
    )?;
    let mtime = UnixTime::now();
    let dot = Holder::new(DirTableEntry::Dot(mtime))?;
    dir_table.insert((parent_inode, "."), dot.clone())?;
    if assignment == InodeAssignment::Directory {
        dir_table.insert((new_inode, "."), dot.clone())?;
    }

    Ok(new_inode)
}

fn alloc_inode(txn: &WriteTransaction) -> Result<u64, UnrealError> {
    let mut table = txn.open_table(MAX_INODE_TABLE)?;
    let max_inode = if let Some(v) = table.get(())? {
        v.value()
    } else {
        ROOT_DIR
    };
    let inode = max_inode + 1;
    table.insert((), inode)?;

    Ok(inode)
}

fn do_mark_peer_files(
    txn: &WriteTransaction,
    peer: &Peer,
    arena: &Arena,
) -> Result<(), UnrealError> {
    let file_table = txn.open_table(FILE_TABLE)?;
    let mut pending_catchup_table = txn.open_table(PENDING_CATCHUP_TABLE)?;
    let peer_str = peer.as_str();
    for elt in file_table.iter()? {
        let (k, v) = elt?;
        let k = k.value();
        if k.1 != peer_str {
            continue;
        }
        let v = v.value().parse()?;
        if v.content.arena != *arena {
            continue;
        }
        let inode = k.0;
        pending_catchup_table.insert((peer_str, arena.as_str(), inode), v.parent_inode)?;
    }

    Ok(())
}

fn do_unmark_peer_file(
    txn: &WriteTransaction,
    peer: &Peer,
    arena: &Arena,
    inode: u64,
) -> Result<(), UnrealError> {
    let mut pending_catchup_table = txn.open_table(PENDING_CATCHUP_TABLE)?;
    pending_catchup_table.remove((peer.as_str(), arena.as_str(), inode))?;

    Ok(())
}

fn do_delete_marked_files(
    txn: &WriteTransaction,
    peer: &Peer,
    arena: &Arena,
) -> Result<(), UnrealError> {
    let mut pending_catchup_table = txn.open_table(PENDING_CATCHUP_TABLE)?;
    let mut file_table = txn.open_table(FILE_TABLE)?;
    let mut directory_table = txn.open_table(DIRECTORY_TABLE)?;
    let peer_str = peer.as_str();
    let arena_str = arena.as_str();
    for elt in pending_catchup_table.extract_from_if(
        (peer_str, arena_str, 0)..(peer_str, arena_str, u64::MAX),
        |_, _| true,
    )? {
        let elt = elt?;
        let (_, _, inode) = elt.0.value();
        let parent_inode = elt.1.value();
        do_rm_file_entry(
            &mut file_table,
            &mut directory_table,
            parent_inode,
            inode,
            peer,
            None,
        )?;
    }
    Ok(())
}

fn do_rm_file_entry(
    file_table: &mut redb::Table<'_, (u64, &str), Holder<FileTableEntry>>,
    dir_table: &mut redb::Table<'_, (u64, &str), Holder<DirTableEntry>>,
    parent_inode: u64,
    inode: u64,
    peer: &Peer,
    mtime: Option<&UnixTime>,
) -> Result<(), UnrealError> {
    let mut range_size = 0;
    let mut delete = false;
    let peer_str = peer.as_str();
    for elt in file_table.range((inode, "")..(inode + 1, ""))? {
        range_size += 1;
        let elt = elt?;
        if peer_str != elt.0.value().1 {
            continue;
        }
        match mtime {
            None => delete = true,
            Some(mtime) => {
                let m = elt.1.value().parse()?.metadata.mtime;
                if m <= *mtime {
                    delete = true;
                }
            }
        }
    }

    if delete {
        file_table.remove((inode, peer_str))?;
        range_size -= 1;
    }
    if range_size == 0 {
        dir_table.retain_in((parent_inode, "")..(parent_inode + 1, ""), |_, v| {
            match v.parse() {
                Ok(DirTableEntry::Regular(v)) => v.inode != inode,
                _ => true,
            }
        })?;
        dir_table.insert(
            (parent_inode, "."),
            Holder::new(DirTableEntry::Dot(UnixTime::now()))?,
        )?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Arena, Path, Peer};
    use assert_fs::TempDir;

    const TEST_TIME: u64 = 1234567890;

    fn test_peer() -> Peer {
        Peer::from("test_peer")
    }

    fn test_arena() -> Arena {
        Arena::from("test_arena")
    }

    fn test_time() -> UnixTime {
        UnixTime::from_secs(TEST_TIME)
    }

    fn later_time() -> UnixTime {
        UnixTime::from_secs(TEST_TIME + 1)
    }

    fn earlier_time() -> UnixTime {
        UnixTime::from_secs(TEST_TIME - 1)
    }

    struct Fixture {
        cache: UnrealCacheBlocking,
        _tempdir: TempDir,
    }
    impl Fixture {
        fn setup() -> anyhow::Result<Fixture> {
            let _ = env_logger::try_init();
            let tempdir = TempDir::new()?;
            let path = tempdir.path().join("unreal.db");
            let mut cache = UnrealCacheBlocking::open(&path)?;
            cache.add_arena(&test_arena())?;

            Ok(Self {
                cache,
                _tempdir: tempdir,
            })
        }

        fn parent_dir_mtime(&self, arena: &Arena, path: &Path) -> anyhow::Result<UnixTime> {
            let arena_root = self.cache.arena_root(arena).expect("arena was added");
            match path.parent() {
                None => Ok(self.cache.dir_mtime(arena_root)?),
                Some(path) => {
                    let (inode, _) = self.cache.lookup_path(arena_root, &path)?;

                    Ok(self.cache.dir_mtime(inode)?)
                }
            }
        }
    }

    #[test]
    fn open_creates_tables() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let cache = &fixture.cache;

        let txn = cache.db.begin_read()?;
        assert!(txn.open_table(DIRECTORY_TABLE).is_ok());
        assert!(txn.open_table(FILE_TABLE).is_ok());
        Ok(())
    }

    #[test]
    fn link_creates_directories() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let cache = &fixture.cache;
        let peer = test_peer();
        let arena = test_arena();
        let file_path = Path::parse("a/b/c.txt")?;
        let mtime = test_time();

        cache.link(&peer, &arena, &file_path, 100, &mtime)?;

        let txn = cache.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;
        let entry = dir_table.get((ROOT_DIR, "test_arena"))?.unwrap();
        let entry = match entry.value().parse()? {
            DirTableEntry::Dot(_) => panic!("Unexpected dot entry"),
            DirTableEntry::Regular(e) => e,
        };

        let entry = dir_table.get((entry.inode, "a"))?.unwrap();
        let entry = match entry.value().parse()? {
            DirTableEntry::Dot(_) => panic!("Unexpected dot entry"),
            DirTableEntry::Regular(e) => e,
        };
        assert_eq!(entry.assignment, InodeAssignment::Directory);

        let entry = dir_table.get((entry.inode, "b"))?.unwrap();
        let entry = match entry.value().parse()? {
            DirTableEntry::Dot(_) => panic!("Unexpected dot entry"),
            DirTableEntry::Regular(e) => e,
        };
        assert_eq!(entry.assignment, InodeAssignment::Directory);

        Ok(())
    }

    #[test]
    fn link_update_dir_mtime() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let cache = &fixture.cache;
        let peer = test_peer();
        let arena = test_arena();
        let mtime = test_time();
        let path1 = Path::parse("a/b/1.txt")?;
        cache.link(&peer, &arena, &path1, 100, &mtime)?;
        let dir_mtime = fixture.parent_dir_mtime(&arena, &path1)?;

        let path2 = Path::parse("a/b/2.txt")?;
        cache.link(&peer, &arena, &path2, 100, &mtime)?;

        assert!(fixture.parent_dir_mtime(&arena, &path2)? > dir_mtime);
        Ok(())
    }

    #[test]
    fn link_updates_existing_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let cache = &fixture.cache;
        let peer = test_peer();
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;
        let old_mtime = test_time();
        let new_mtime = later_time();

        cache.link(&peer, &arena, &file_path, 100, &old_mtime)?;
        cache.link(&peer, &arena, &file_path, 200, &new_mtime)?;

        let txn = cache.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;
        let dir_entry = dir_table.get((ROOT_DIR, "test_arena"))?.unwrap();
        let dir_entry = match dir_entry.value().parse()? {
            DirTableEntry::Dot(_) => panic!("Unexpected dot entry"),
            DirTableEntry::Regular(e) => e,
        };
        let dir_entry = dir_table.get((dir_entry.inode, "file.txt"))?.unwrap();
        let dir_entry = match dir_entry.value().parse()? {
            DirTableEntry::Dot(_) => panic!("Unexpected dot entry"),
            DirTableEntry::Regular(e) => e,
        };

        let file_table = txn.open_table(FILE_TABLE)?;
        let entry = file_table
            .get((dir_entry.inode, peer.as_str()))?
            .unwrap()
            .value()
            .parse()?;
        assert_eq!(entry.metadata.size, 200);
        assert_eq!(entry.metadata.mtime, new_mtime);

        Ok(())
    }

    #[test]
    fn link_ignores_older_mtime() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let cache = &fixture.cache;
        let peer = test_peer();
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;
        let new_mtime = test_time();
        let old_mtime = earlier_time();

        cache.link(&peer, &arena, &file_path, 100, &new_mtime)?;
        cache.link(&peer, &arena, &file_path, 200, &old_mtime)?;

        let txn = cache.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;
        let dir_entry = dir_table.get((ROOT_DIR, "test_arena"))?.unwrap();
        let dir_entry = match dir_entry.value().parse()? {
            DirTableEntry::Dot(_) => panic!("Unexpected dot entry"),
            DirTableEntry::Regular(e) => e,
        };
        let dir_entry = dir_table.get((dir_entry.inode, "file.txt"))?.unwrap();
        let dir_entry = match dir_entry.value().parse()? {
            DirTableEntry::Dot(_) => panic!("Unexpected dot entry"),
            DirTableEntry::Regular(e) => e,
        };

        let file_table = txn.open_table(FILE_TABLE)?;
        let entry = file_table
            .get((dir_entry.inode, peer.as_str()))?
            .unwrap()
            .value()
            .parse()?;
        assert_eq!(entry.metadata.size, 100);
        assert_eq!(entry.metadata.mtime, new_mtime);

        Ok(())
    }

    #[test]
    fn unlink_removes_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let cache = &fixture.cache;
        let peer = test_peer();
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;
        let mtime = test_time();

        cache.link(&peer, &arena, &file_path, 100, &mtime)?;
        cache.unlink(&peer, &arena, &file_path, &later_time())?;

        let txn = cache.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;
        assert!(dir_table.get((ROOT_DIR, "file.txt"))?.is_none());

        Ok(())
    }

    #[test]
    fn unlink_update_dir_mtime() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let cache = &fixture.cache;
        let peer = test_peer();
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;
        let mtime = test_time();

        cache.link(&peer, &arena, &file_path, 100, &mtime)?;
        let dir_mtime = fixture.parent_dir_mtime(&arena, &file_path)?;
        cache.unlink(&peer, &arena, &file_path, &later_time())?;

        assert!(fixture.parent_dir_mtime(&arena, &file_path)? > dir_mtime);

        Ok(())
    }

    #[test]
    fn unlink_ignores_older_mtime() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let cache = &fixture.cache;
        let peer = test_peer();
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;
        let mtime = test_time();

        cache.link(&peer, &arena, &file_path, 100, &mtime)?;
        cache.unlink(&peer, &arena, &file_path, &earlier_time())?;

        let txn = cache.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;
        let dir_entry = dir_table.get((ROOT_DIR, "test_arena"))?.unwrap();
        let dir_entry = match dir_entry.value().parse()? {
            DirTableEntry::Dot(_) => panic!("Unexpected dot entry"),
            DirTableEntry::Regular(e) => e,
        };
        assert!(dir_table.get((dir_entry.inode, "file.txt"))?.is_some());

        Ok(())
    }

    #[test]
    fn lookup_finds_entry() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let cache = &fixture.cache;
        let peer = test_peer();
        let arena = test_arena();
        let file_path = Path::parse("a/file.txt")?;
        let mtime = test_time();

        cache.link(&peer, &arena, &file_path, 100, &mtime)?;

        // Lookup directory
        let dir_entry = cache.lookup(cache.arena_root(&arena)?, "a")?;
        assert_eq!(dir_entry.assignment, InodeAssignment::Directory);

        // Lookup file
        let file_entry = cache.lookup(dir_entry.inode, "file.txt")?;
        assert_eq!(file_entry.assignment, InodeAssignment::File);

        let metadata = cache.file_metadata(file_entry.inode)?;
        assert_eq!(metadata.mtime, mtime);
        assert_eq!(metadata.size, 100);

        Ok(())
    }

    #[test]
    fn lookup_returns_notfound_for_missing_entry() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let cache = &fixture.cache;

        assert!(matches!(
            cache.lookup(ROOT_DIR, "nonexistent"),
            Err(UnrealError::NotFound),
        ));

        Ok(())
    }

    #[test]
    fn lookup_path_finds_entry() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let cache = &fixture.cache;
        let peer = test_peer();
        let arena = test_arena();
        let path = Path::parse("a/b/c/file.txt")?;
        let mtime = test_time();

        cache.link(&peer, &arena, &path, 100, &mtime)?;

        let (inode, assignment) = cache.lookup_path(cache.arena_root(&arena)?, &path)?;
        assert_eq!(assignment, InodeAssignment::File);

        let metadata = cache.file_metadata(inode)?;
        assert_eq!(metadata.mtime, mtime);
        assert_eq!(metadata.size, 100);

        Ok(())
    }

    #[test]
    fn readdir_returns_all_entries() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let cache = &fixture.cache;
        let peer = test_peer();
        let arena = test_arena();
        let mtime = test_time();

        cache.link(&peer, &arena, &Path::parse("dir/file1.txt")?, 100, &mtime)?;
        cache.link(&peer, &arena, &Path::parse("dir/file2.txt")?, 200, &mtime)?;
        cache.link(
            &peer,
            &arena,
            &Path::parse("dir/subdir/file3.txt")?,
            300,
            &mtime,
        )?;

        assert_unordered::assert_eq_unordered!(
            vec![(arena.to_string(), InodeAssignment::Directory),],
            cache
                .readdir(ROOT_DIR)?
                .into_iter()
                .map(|(name, entry)| (name, entry.assignment))
                .collect::<Vec<_>>(),
        );

        let dir_entry = cache.lookup(ROOT_DIR, arena.as_str())?;
        assert_unordered::assert_eq_unordered!(
            vec![("dir".to_string(), InodeAssignment::Directory),],
            cache
                .readdir(dir_entry.inode)?
                .into_iter()
                .map(|(name, entry)| (name, entry.assignment))
                .collect::<Vec<_>>(),
        );

        let dir_entry = cache.lookup(dir_entry.inode, "dir")?;
        assert_unordered::assert_eq_unordered!(
            vec![
                ("file1.txt".to_string(), InodeAssignment::File),
                ("file2.txt".to_string(), InodeAssignment::File),
                ("subdir".to_string(), InodeAssignment::Directory),
            ],
            cache
                .readdir(dir_entry.inode)?
                .into_iter()
                .map(|(name, entry)| (name, entry.assignment))
                .collect::<Vec<_>>(),
        );

        Ok(())
    }

    #[test]
    fn get_file_metadata_resolves_conflict() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let cache = &fixture.cache;

        let peer1 = Peer::from("peer1");
        let peer2 = Peer::from("peer2");
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;

        let mtime1 = test_time();
        let mtime2 = later_time();

        cache.link(&peer1, &arena, &file_path, 100, &mtime1)?;
        cache.link(&peer2, &arena, &file_path, 200, &mtime2)?;

        let file_entry = cache.lookup(cache.arena_root(&arena)?, "file.txt")?;
        let metadata = cache.file_metadata(file_entry.inode)?;
        assert_eq!(metadata.size, 200);
        assert_eq!(metadata.mtime, mtime2);

        Ok(())
    }

    #[test]
    fn file_available_from_multiple_peers() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let cache = &fixture.cache;

        let a = Peer::from("a");
        let b = Peer::from("b");
        let c = Peer::from("c");
        let arena = test_arena();
        let path = Path::parse("file.txt")?;

        let mtime1 = test_time();
        let mtime2 = later_time();

        cache.link(&a, &arena, &path, 100, &mtime1)?;
        cache.link(&b, &arena, &path, 200, &mtime2)?;
        cache.link(&c, &arena, &path, 200, &mtime2)?;

        let parent_inode = cache.arena_root(&arena)?;
        let inode = cache.lookup(parent_inode, "file.txt")?.inode;
        let avail = cache.file_availability(inode)?;
        assert_unordered::assert_eq_unordered!(
            vec![
                (
                    b.clone(),
                    FileTableEntry {
                        content: FileContent {
                            arena: arena.clone(),
                            path: path.clone(),
                        },
                        metadata: FileMetadata {
                            size: 200,
                            mtime: mtime2.clone(),
                        },
                        parent_inode
                    }
                ),
                (
                    c.clone(),
                    FileTableEntry {
                        content: FileContent {
                            arena: arena.clone(),
                            path: path.clone(),
                        },
                        metadata: FileMetadata {
                            size: 200,
                            mtime: mtime2.clone(),
                        },
                        parent_inode
                    }
                )
            ],
            avail
        );

        Ok(())
    }

    #[test]
    fn mark_and_delete_peer_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let cache = &fixture.cache;
        let arena = test_arena();
        let peer1 = Peer::from("1");
        let peer2 = Peer::from("2");
        let peer3 = Peer::from("3");
        let file1 = Path::parse("file1")?;
        let file2 = Path::parse("afile2")?;
        let file3 = Path::parse("file3")?;
        let file4 = Path::parse("file4")?;

        let mtime = test_time();

        cache.link(&peer1, &arena, &file1, 10, &mtime)?;

        cache.link(&peer1, &arena, &file2, 10, &mtime)?;
        cache.link(&peer2, &arena, &file2, 10, &mtime)?;

        cache.link(&peer1, &arena, &file1, 10, &mtime)?;
        cache.link(&peer2, &arena, &file2, 10, &mtime)?;
        cache.link(&peer3, &arena, &file3, 10, &mtime)?;

        cache.link(&peer1, &arena, &file4, 10, &mtime)?;

        let arena_root = cache.arena_root(&arena)?;
        let file1_inode = cache.lookup(arena_root, file1.name())?.inode;

        // Simulate a catchup that only reports file2 and file4.
        cache.mark_peer_files(&peer1, &arena)?;
        cache.catchup(&peer1, &arena, &file2, 10, &mtime)?;
        cache.catchup(&peer1, &arena, &file4, 10, &mtime)?;
        cache.delete_marked_files(&peer1, &arena)?;

        // File1 should have been deleted, since it was only on peer1,
        assert!(matches!(
            cache.lookup(ROOT_DIR, file1.name()),
            Err(UnrealError::NotFound)
        ));
        // File2 and 3 should still be available, from other peers
        let file2_inode = cache.lookup(arena_root, file2.name())?.inode;
        let file3_inode = cache.lookup(arena_root, file3.name())?.inode;

        // File4 should still be available, from peer1
        let file4_inode = cache.lookup(arena_root, file4.name())?.inode;

        // Check file table entries directly
        {
            let txn = cache.db.begin_read()?;
            let file_table = txn.open_table(FILE_TABLE)?;
            assert!(file_table.get((file1_inode, peer1.as_str()))?.is_none());
            assert!(file_table.get((file2_inode, peer1.as_str()))?.is_some());
            assert!(file_table.get((file3_inode, peer1.as_str()))?.is_none());
            assert!(file_table.get((file4_inode, peer1.as_str()))?.is_some());
        }

        Ok(())
    }
}
