//! The Unreal cache - a partial local cache of remote files.
//!
//! See `spec/unreal.md` for details.

use super::{UnrealCacheAsync, UnrealCacheError};
use crate::model::{self, Arena, Path, Peer};
use redb::{Database, ReadTransaction, ReadableTable, TableDefinition, Value, WriteTransaction};
use std::cmp::max;
use std::path;
use std::time::SystemTime;

const DIRECTORY_TABLE: TableDefinition<(u64, &str), ReadDirEntry> =
    TableDefinition::new("directory_table");

const FILE_TABLE: TableDefinition<(u64, &str), FileEntry> = TableDefinition::new("file_table");
const PENDING_CATCHUP_TABLE: TableDefinition<(&str, u64), u64> =
    TableDefinition::new("pending_catchup");

/// An entry in a directory listing.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ReadDirEntry {
    /// The inode of the entry.
    pub inode: u64,
    /// The type of the entry.
    pub assignment: InodeAssignment,
}

impl Value for ReadDirEntry {
    type SelfType<'a>
        = ReadDirEntry
    where
        Self: 'a;

    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        bincode::deserialize::<ReadDirEntry>(data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        bincode::serialize(value).unwrap()
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("ReadDirEntry")
    }
}

/// The type of an inode.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum InodeAssignment {
    /// The inode of a file, look it up in the file table.
    File,
    /// The inode of a directory, look it up in the directory table.
    ///
    /// Note that an empty directory won't have any entries in
    /// the directory table.
    Directory,
}

/// An entry in the file table.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FileEntry {
    /// The arena to use to fetch file content in the peer.
    ///
    /// This is stored here as a key to fetch file content,
    /// to be replaced by a blob id.
    pub arena: model::Arena,

    /// The path to use to fetch file content in the peer.
    ///
    /// Note that it shouldn't matter whether the path
    /// here matches the path which led to this file. This
    /// is to be treated as a key for downloading and nothing else..
    ///
    /// This is stored here as a key to fetch file content,
    /// to be replaced by a blob id.
    pub path: model::Path,

    /// The metadata of the file.
    pub metadata: FileMetadata,

    /// Inode of the containing directory
    parent_inode: u64,
}

impl Value for FileEntry {
    type SelfType<'a>
        = FileEntry
    where
        Self: 'a;

    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        bincode::deserialize::<FileEntry>(data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        bincode::serialize(value).unwrap()
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("FileEntry")
    }
}

/// The metadata of a file.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FileMetadata {
    /// The size of the file in bytes.
    pub size: u64,
    /// The modification time of the file.
    pub mtime: SystemTime,
}

/// A cache of remote files.
pub struct UnrealCacheBlocking {
    db: Database,
}

impl UnrealCacheBlocking {
    /// Create a new UnrealCache from a redb database.
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    /// Open or create an UnrealCache at the given path.
    pub fn open(path: &path::Path) -> Result<Self, UnrealCacheError> {
        let db = Database::create(path)?;
        let write_txn = db.begin_write()?;
        {
            let _ = write_txn.open_table(DIRECTORY_TABLE)?;
            let _ = write_txn.open_table(FILE_TABLE)?;
        }
        write_txn.commit()?;
        Ok(Self::new(db))
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
        mtime: SystemTime,
    ) -> Result<(), UnrealCacheError> {
        let txn = self.db.begin_write()?;
        let inode = do_link(&txn, peer, arena, path, size, mtime)?;
        do_unmark_peer_file(&txn, peer, inode)?;
        txn.commit()?;

        Ok(())
    }

    pub fn link(
        &self,
        peer: &Peer,
        arena: &Arena,
        path: &Path,
        size: u64,
        mtime: SystemTime,
    ) -> Result<(), UnrealCacheError> {
        let txn = self.db.begin_write()?;
        do_link(&txn, peer, arena, path, size, mtime)?;
        txn.commit()?;

        Ok(())
    }

    pub fn unlink(
        &self,
        peer: &Peer,
        _arena: &Arena,
        path: &Path,
        mtime: SystemTime,
    ) -> Result<(), UnrealCacheError> {
        let txn = self.db.begin_write()?;
        do_unlink(&txn, peer, path, mtime)?;
        txn.commit()?;
        Ok(())
    }

    pub fn lookup(
        &self,
        parent_inode: u64,
        name: &str,
    ) -> Result<(ReadDirEntry, Option<FileMetadata>), UnrealCacheError> {
        let txn = self.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;
        let entry = dir_table
            .get((parent_inode, name))?
            .ok_or(UnrealCacheError::NotFound)?
            .value();
        match &entry.assignment {
            InodeAssignment::File => {
                let file_entry = get_best_file_entry(&txn, entry.inode)?;

                Ok((entry, Some(file_entry.metadata)))
            }
            InodeAssignment::Directory => Ok((entry, None)),
        }
    }

    pub fn readdir(
        &self,
        inode: u64,
    ) -> Result<impl Iterator<Item = (String, ReadDirEntry)>, UnrealCacheError> {
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
            entries.push((name, value.value().clone()));
        }

        Ok(entries.into_iter())
    }

    pub fn mark_peer_files(&self, peer: &Peer, _arena: &Arena) -> Result<(), UnrealCacheError> {
        // TODO: take arena into account
        let txn = self.db.begin_write()?;
        do_mark_peer_files(&txn, peer)?;
        txn.commit()?;
        Ok(())
    }

    pub fn delete_marked_files(&self, peer: &Peer, _arena: &Arena) -> Result<(), UnrealCacheError> {
        // TODO: take arena into account
        let txn = self.db.begin_write()?;
        do_delete_marked_files(&txn, peer)?;
        txn.commit()?;
        Ok(())
    }
}

/// Implement [UnrealCache::unlink] within a transaction.
fn do_unlink(
    txn: &WriteTransaction,
    peer: &Peer,
    path: &Path,
    mtime: SystemTime,
) -> Result<(), UnrealCacheError> {
    let mut dir_table = txn.open_table(DIRECTORY_TABLE)?;
    let (parent_inode, parent_assignment) = do_lookup_path(&mut dir_table, &path.parent())?;
    if parent_assignment != InodeAssignment::Directory {
        return Err(UnrealCacheError::NotADirectory);
    }

    let dir_entry =
        get_dir_entry(&dir_table, parent_inode, path.name())?.ok_or(UnrealCacheError::NotFound)?;
    if dir_entry.assignment != InodeAssignment::File {
        return Err(UnrealCacheError::IsADirectory);
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
    file_table: &redb::Table<'_, (u64, &str), FileEntry>,
    inode: u64,
    peer: &Peer,
) -> Result<Option<FileEntry>, UnrealCacheError> {
    Ok(file_table.get((inode, peer.as_str()))?.map(|e| e.value()))
}

/// Choose the best available [FileEntry] from all peer's entries.
///
/// Strictly-speaking, there might be several best ones, with the same
/// mtime, from multiple peers. This implementation just returns the
/// first one.
fn get_best_file_entry(txn: &ReadTransaction, inode: u64) -> Result<FileEntry, UnrealCacheError> {
    let file_table = txn.open_table(FILE_TABLE)?;

    let mut best: Option<FileEntry> = None;
    for entry in file_table.range((inode, "")..)? {
        let entry = entry?;
        let file_entry: FileEntry = entry.1.value();
        let replace = match &best {
            None => true,
            Some(best) => file_entry.metadata.mtime > best.metadata.mtime,
        };
        if replace {
            best = Some(file_entry);
        }
    }

    best.ok_or(UnrealCacheError::NotFound)
}

/// Implement [UnrealCache::link] in a transaction.
///
/// Return the inode of the file.
fn do_link(
    txn: &WriteTransaction,
    peer: &Peer,
    arena: &Arena,
    path: &Path,
    size: u64,
    mtime: SystemTime,
) -> Result<u64, UnrealCacheError> {
    let mut dir_table = txn.open_table(DIRECTORY_TABLE)?;
    let mut file_table = txn.open_table(FILE_TABLE)?;

    let filename = path.name();
    let parent_inode = do_mkdirs(&mut dir_table, &file_table, &path.parent())?;
    let dir_entry = get_dir_entry(&dir_table, parent_inode, filename)?;
    let inode = match dir_entry {
        None => add_dir_entry(
            &mut dir_table,
            &file_table,
            parent_inode,
            filename,
            InodeAssignment::File,
        )?,
        Some(dir_entry) => {
            if let Some(existing) = get_file_entry(&file_table, dir_entry.inode, peer)? {
                if existing.metadata.mtime > mtime {
                    return Ok(dir_entry.inode);
                }
            }

            dir_entry.inode
        }
    };
    file_table.insert(
        (inode, peer.as_str()),
        FileEntry {
            arena: arena.clone(),
            path: path.clone(),
            metadata: FileMetadata { size, mtime },
            parent_inode,
        },
    )?;

    Ok(inode)
}

/// Make sure that the given path is a directory; create it if necessary.
///
/// Returns the inode of the directory pointed to by the path.
fn do_mkdirs(
    dir_table: &mut redb::Table<'_, (u64, &str), ReadDirEntry>,
    file_table: &redb::Table<'_, (u64, &str), FileEntry>,
    path: &Option<Path>,
) -> Result<u64, UnrealCacheError> {
    let mut current = 1;
    for component in Path::components(path) {
        current = if let Some(entry) = get_dir_entry(dir_table, current, component)? {
            if entry.assignment != InodeAssignment::Directory {
                return Err(UnrealCacheError::NotADirectory);
            }

            entry.inode
        } else {
            add_dir_entry(
                dir_table,
                file_table,
                current,
                component,
                InodeAssignment::Directory,
            )?
        };
    }

    Ok(current)
}

/// Find the file or directory pointed to by the given path.
fn do_lookup_path(
    dir_table: &mut redb::Table<'_, (u64, &str), ReadDirEntry>,
    path: &Option<Path>,
) -> Result<(u64, InodeAssignment), UnrealCacheError> {
    let mut current = (1, InodeAssignment::Directory);
    for component in Path::components(path) {
        if let Some(entry) = get_dir_entry(dir_table, current.0, component)? {
            if entry.assignment != InodeAssignment::Directory {
                return Err(UnrealCacheError::NotADirectory);
            }

            current = (entry.inode, entry.assignment);
        } else {
            return Err(UnrealCacheError::NotFound);
        };
    }

    Ok(current)
}

/// Get a [ReadDirEntry] from a directory, if it exists.
fn get_dir_entry(
    dir_table: &redb::Table<'_, (u64, &str), ReadDirEntry>,
    parent_inode: u64,
    name: &str,
) -> Result<Option<ReadDirEntry>, UnrealCacheError> {
    Ok(dir_table.get((parent_inode, name))?.map(|e| e.value()))
}

/// Add an entry to the given directory.
fn add_dir_entry(
    dir_table: &mut redb::Table<'_, (u64, &str), ReadDirEntry>,
    file_table: &redb::Table<'_, (u64, &str), FileEntry>,
    parent_inode: u64,
    name: &str,
    assignment: InodeAssignment,
) -> Result<u64, UnrealCacheError> {
    let new_inode = 1 + max(
        dir_table.last()?.map(|(k, _)| k.value().0).unwrap_or(1),
        file_table.last()?.map(|(k, _)| k.value().0).unwrap_or(1),
    );
    dir_table.insert(
        (parent_inode, name),
        ReadDirEntry {
            inode: new_inode,
            assignment,
        },
    )?;

    Ok(new_inode)
}

fn do_mark_peer_files(txn: &WriteTransaction, peer: &Peer) -> Result<(), UnrealCacheError> {
    let file_table = txn.open_table(FILE_TABLE)?;
    let mut pending_catchup_table = txn.open_table(PENDING_CATCHUP_TABLE)?;
    let peer_str = peer.as_str();
    for elt in file_table.iter()? {
        let (k, v) = elt?;
        let k = k.value();
        if k.1 != peer_str {
            continue;
        }
        let inode = k.0;
        pending_catchup_table.insert((peer_str, inode), v.value().parent_inode)?;
    }

    Ok(())
}

fn do_unmark_peer_file(
    txn: &WriteTransaction,
    peer: &Peer,
    inode: u64,
) -> Result<(), UnrealCacheError> {
    let mut pending_catchup_table = txn.open_table(PENDING_CATCHUP_TABLE)?;
    pending_catchup_table.remove((peer.as_str(), inode))?;

    Ok(())
}

fn do_delete_marked_files(txn: &WriteTransaction, peer: &Peer) -> Result<(), UnrealCacheError> {
    let mut pending_catchup_table = txn.open_table(PENDING_CATCHUP_TABLE)?;
    let mut file_table = txn.open_table(FILE_TABLE)?;
    let mut directory_table = txn.open_table(DIRECTORY_TABLE)?;
    let peer_str = peer.as_str();
    for elt in
        pending_catchup_table.extract_from_if((peer_str, 0)..(peer_str, u64::MAX), |_, _| true)?
    {
        let elt = elt?;
        let (_, inode) = elt.0.value();
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
    file_table: &mut redb::Table<'_, (u64, &str), FileEntry>,
    dir_table: &mut redb::Table<'_, (u64, &str), ReadDirEntry>,
    parent_inode: u64,
    inode: u64,
    peer: &Peer,
    mtime: Option<SystemTime>,
) -> Result<(), UnrealCacheError> {
    let mut range_size = 0;
    let mut delete = false;
    let peer_str = peer.as_str();
    for elt in file_table.range((inode, "")..(inode + 1, ""))? {
        range_size += 1;
        let elt = elt?;
        if peer_str == elt.0.value().1
            && mtime
                .as_ref()
                .map(|t| elt.1.value().metadata.mtime <= *t)
                .unwrap_or(true)
        {
            delete = true;
        }
    }

    if delete {
        file_table.remove((inode, peer_str))?;
        range_size -= 1;
    }
    if range_size == 0 {
        dir_table.retain_in((parent_inode, "")..(parent_inode + 1, ""), |_, v| {
            v.inode != inode
        })?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Arena, Path, Peer};
    use assert_fs::TempDir;
    use std::time::{Duration, SystemTime};

    fn test_peer() -> Peer {
        Peer::from("test_peer")
    }

    fn test_arena() -> Arena {
        Arena::from("test_arena")
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
            let cache = UnrealCacheBlocking::open(&path)?;

            Ok(Self {
                cache,
                _tempdir: tempdir,
            })
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
        let mtime = SystemTime::now();

        cache.link(&peer, &arena, &file_path, 100, mtime)?;

        let txn = cache.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;
        let entry = dir_table.get((1, "a"))?.unwrap().value();
        assert_eq!(entry.assignment, InodeAssignment::Directory);

        let entry = dir_table.get((entry.inode, "b"))?.unwrap().value();
        assert_eq!(entry.assignment, InodeAssignment::Directory);

        Ok(())
    }

    #[test]
    fn link_updates_existing_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let cache = &fixture.cache;
        let peer = test_peer();
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;
        let old_mtime = SystemTime::now();
        let new_mtime = old_mtime + Duration::from_secs(1);

        cache.link(&peer, &arena, &file_path, 100, old_mtime)?;
        cache.link(&peer, &arena, &file_path, 200, new_mtime)?;

        let txn = cache.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;
        let dir_entry = dir_table.get((1, "file.txt"))?.unwrap().value();

        let file_table = txn.open_table(FILE_TABLE)?;
        let entry = file_table
            .get((dir_entry.inode, peer.as_str()))?
            .unwrap()
            .value();
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
        let new_mtime = SystemTime::now();
        let old_mtime = new_mtime - Duration::from_secs(1);

        cache.link(&peer, &arena, &file_path, 100, new_mtime)?;
        cache.link(&peer, &arena, &file_path, 200, old_mtime)?;

        let txn = cache.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;
        let dir_entry = dir_table.get((1, "file.txt"))?.unwrap().value();

        let file_table = txn.open_table(FILE_TABLE)?;
        let entry = file_table
            .get((dir_entry.inode, peer.as_str()))?
            .unwrap()
            .value();
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
        let mtime = SystemTime::now();

        cache.link(&peer, &arena, &file_path, 100, mtime)?;
        cache.unlink(&peer, &arena, &file_path, mtime + Duration::from_secs(1))?;

        let txn = cache.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;
        assert!(dir_table.get((1, "file.txt"))?.is_none());

        Ok(())
    }

    #[test]
    fn unlink_ignores_older_mtime() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let cache = &fixture.cache;
        let peer = test_peer();
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;
        let mtime = SystemTime::now();

        cache.link(&peer, &arena, &file_path, 100, mtime)?;
        cache.unlink(&peer, &arena, &file_path, mtime - Duration::from_secs(1))?;

        let txn = cache.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;
        assert!(dir_table.get((1, "file.txt"))?.is_some());

        Ok(())
    }

    #[test]
    fn lookup_finds_entry() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let cache = &fixture.cache;
        let peer = test_peer();
        let arena = test_arena();
        let file_path = Path::parse("a/file.txt")?;
        let mtime = SystemTime::now();

        cache.link(&peer, &arena, &file_path, 100, mtime)?;

        // Lookup directory
        let (dir_entry, metadata) = cache.lookup(1, "a")?;
        assert_eq!(dir_entry.assignment, InodeAssignment::Directory);
        assert_eq!(metadata, None);

        // Lookup file
        let (file_entry, metadata) = cache.lookup(dir_entry.inode, "file.txt")?;
        assert_eq!(file_entry.assignment, InodeAssignment::File);
        let metadata = metadata.expect("files must have metadata");
        assert_eq!(metadata.mtime, mtime);
        assert_eq!(metadata.size, 100);

        Ok(())
    }

    #[test]
    fn lookup_returns_notfound_for_missing_entry() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let cache = &fixture.cache;

        assert!(matches!(
            cache.lookup(1, "nonexistent"),
            Err(UnrealCacheError::NotFound),
        ));

        Ok(())
    }

    #[test]
    fn readdir_returns_all_entries() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let cache = &fixture.cache;
        let peer = test_peer();
        let arena = test_arena();
        let mtime = SystemTime::now();

        cache.link(&peer, &arena, &Path::parse("dir/file1.txt")?, 100, mtime)?;
        cache.link(&peer, &arena, &Path::parse("dir/file2.txt")?, 200, mtime)?;
        cache.link(
            &peer,
            &arena,
            &Path::parse("dir/subdir/file3.txt")?,
            300,
            mtime,
        )?;

        let (dir_entry, _) = cache.lookup(1, "dir")?;
        let entries: Vec<_> = cache.readdir(dir_entry.inode)?.collect::<Vec<_>>();

        assert_eq!(entries.len(), 3);

        let file1 = entries
            .iter()
            .find(|(name, _)| name == "file1.txt")
            .unwrap();
        assert_eq!(file1.1.assignment, InodeAssignment::File);

        let file2 = entries
            .iter()
            .find(|(name, _)| name == "file2.txt")
            .unwrap();
        assert_eq!(file2.1.assignment, InodeAssignment::File);

        let subdir = entries.iter().find(|(name, _)| name == "subdir").unwrap();
        assert_eq!(subdir.1.assignment, InodeAssignment::Directory);

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

        let mtime1 = SystemTime::now();
        let mtime2 = mtime1 + Duration::from_secs(1);

        cache.link(&peer1, &arena, &file_path, 100, mtime1)?;
        cache.link(&peer2, &arena, &file_path, 200, mtime2)?;

        let (_, metadata) = cache.lookup(1, "file.txt")?;
        let metadata = metadata.expect("files must have metadata");
        assert_eq!(metadata.size, 200);
        assert_eq!(metadata.mtime, mtime2);

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

        let mtime = SystemTime::now();

        cache.link(&peer1, &arena, &file1, 10, mtime)?;

        cache.link(&peer1, &arena, &file2, 10, mtime)?;
        cache.link(&peer2, &arena, &file2, 10, mtime)?;

        cache.link(&peer1, &arena, &file1, 10, mtime)?;
        cache.link(&peer2, &arena, &file2, 10, mtime)?;
        cache.link(&peer3, &arena, &file3, 10, mtime)?;

        cache.link(&peer1, &arena, &file4, 10, mtime)?;

        let file1_inode = cache.lookup(1, file1.name())?.0.inode;

        // Simulate a catchup that only reports file2 and file4.
        cache.mark_peer_files(&peer1, &arena)?;
        cache.catchup(&peer1, &arena, &file2, 10, mtime)?;
        cache.catchup(&peer1, &arena, &file4, 10, mtime)?;
        cache.delete_marked_files(&peer1, &arena)?;

        // File1 should have been deleted, since it was only on peer1,
        assert!(matches!(
            cache.lookup(1, file1.name()),
            Err(UnrealCacheError::NotFound)
        ));
        // File2 and 3 should still be available, from other peers
        let file2_inode = cache.lookup(1, file2.name())?.0.inode;
        let file3_inode = cache.lookup(1, file3.name())?.0.inode;

        // File4 should still be available, from peer1
        let file4_inode = cache.lookup(1, file4.name())?.0.inode;

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
