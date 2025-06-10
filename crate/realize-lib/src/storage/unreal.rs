//! The Unreal cache - a partial local cache of remote files.
//!
//! See `spec/unreal.md` for details.

use crate::model::{self, Arena, Path, Peer};
use redb::{Database, ReadableTable, TableDefinition};
use std::cmp::max;
use std::path;
use std::time::SystemTime;

const DIRECTORY_TABLE: TableDefinition<(u64, &str), &[u8]> =
    TableDefinition::new("directory_table");

const FILE_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("file_table");

/// An entry in a directory listing.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ReadDirEntry {
    /// The inode of the entry.
    pub inode: u64,
    /// The type of the entry.
    pub assignment: InodeAssignment,
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

    /// File is marked for deletion.
    ///
    /// See mark_peer_files and delete_marked_files above.
    pub marked: bool,
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
pub struct UnrealCache {
    db: Database,
}

impl UnrealCache {
    /// Create a new UnrealCache from a redb database.
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    /// Open or create an UnrealCache at the given path.
    pub fn open(path: &path::Path) -> Result<Self, anyhow::Error> {
        let db = Database::create(path)?;
        let write_txn = db.begin_write()?;
        {
            let _ = write_txn.open_table(DIRECTORY_TABLE)?;
            let _ = write_txn.open_table(FILE_TABLE)?;
        }
        write_txn.commit()?;
        Ok(Self::new(db))
    }

    pub fn link(
        &self,
        peer: &Peer,
        arena: &Arena,
        path: &Path,
        size: u64,
        mtime: SystemTime,
    ) -> Result<(), anyhow::Error> {
        let txn = self.db.begin_write()?;
        {
            let mut dir_table = txn.open_table(DIRECTORY_TABLE)?;
            let mut file_table = txn.open_table(FILE_TABLE)?;

            let mut current_inode = 1;

            let components: Vec<_> = path
                .as_real_path()
                .iter()
                .map(|c| c.to_str().unwrap())
                .collect();

            for (i, &component_str) in components.iter().enumerate() {
                let is_last = i == components.len() - 1;

                let (inode, assignment) = {
                    if let Some(entry) = dir_table.get((current_inode, component_str))? {
                        let entry: ReadDirEntry = bincode::deserialize(entry.value())?;
                        (entry.inode, entry.assignment)
                    } else {
                        let new_inode = 1 + max(
                            dir_table.last()?.map(|(k, _)| k.value().0).unwrap_or(1),
                            file_table.last()?.map(|(k, _)| k.value()).unwrap_or(1),
                        );
                        let assignment = if is_last {
                            InodeAssignment::File
                        } else {
                            InodeAssignment::Directory
                        };
                        let new_entry = ReadDirEntry {
                            inode: new_inode,
                            assignment: assignment.clone(),
                        };
                        let entry_val = bincode::serialize(&new_entry)?;
                        dir_table.insert((current_inode, component_str), entry_val.as_slice())?;
                        (new_inode, assignment)
                    }
                };

                match assignment {
                    InodeAssignment::File if !is_last => anyhow::bail!("Not a directory"),
                    _ => current_inode = inode,
                }
            }

            let files_vec = if let Some(file_entry) = file_table.get(current_inode)? {
                bincode::deserialize(file_entry.value())?
            } else {
                Vec::new()
            };

            let mut files: Vec<(Peer, FileEntry)> = files_vec;

            if let Some((_, entry)) = files.iter_mut().find(|(p, _)| p == peer) {
                if mtime > entry.metadata.mtime {
                    entry.metadata.mtime = mtime;
                    entry.metadata.size = size;
                }
            } else {
                files.push((
                    peer.clone(),
                    FileEntry {
                        arena: arena.clone(),
                        path: path.clone(),
                        metadata: FileMetadata { size, mtime },
                        marked: false,
                    },
                ));
            }

            file_table.insert(current_inode, bincode::serialize(&files)?.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    pub fn unlink(
        &self,
        peer: &Peer,
        _arena: &Arena,
        path: &Path,
        mtime: SystemTime,
    ) -> Result<(), anyhow::Error> {
        let txn = self.db.begin_write()?;

        let mut current_inode = 1;
        let mut parent_inode = 1;
        let mut last_component = "";
        let mut found = false;

        let components: Vec<_> = path
            .as_real_path()
            .iter()
            .map(|c| c.to_str().unwrap())
            .collect();

        {
            let dir_table = txn.open_table(DIRECTORY_TABLE)?;
            for component_str in &components {
                if let Some(entry) = dir_table.get((current_inode, *component_str))? {
                    let entry: ReadDirEntry = bincode::deserialize(entry.value())?;
                    parent_inode = current_inode;
                    current_inode = entry.inode;
                    last_component = component_str;
                    found = true;
                } else {
                    found = false;
                    break;
                }
            }
        }

        if !found {
            txn.abort()?;
            return Ok(());
        }

        let files_data = {
            let file_table = txn.open_table(FILE_TABLE)?;
            if let Some(entry) = file_table.get(current_inode)? {
                Some(bincode::deserialize::<Vec<(Peer, FileEntry)>>(
                    entry.value(),
                )?)
            } else {
                None
            }
        };

        if let Some(mut files) = files_data {
            let initial_len = files.len();
            files.retain(|(p, f)| {
                if p == peer {
                    mtime <= f.metadata.mtime
                } else {
                    true
                }
            });

            if files.is_empty() {
                let mut file_table = txn.open_table(FILE_TABLE)?;
                file_table.remove(current_inode)?;
                let mut dir_table = txn.open_table(DIRECTORY_TABLE)?;
                dir_table.remove((parent_inode, last_component))?;
            } else if files.len() < initial_len {
                let mut file_table = txn.open_table(FILE_TABLE)?;
                file_table.insert(current_inode, bincode::serialize(&files)?.as_slice())?;
            }
        }

        txn.commit()?;
        Ok(())
    }

    pub fn lookup(
        &self,
        parent_inode: u64,
        name: &str,
    ) -> Result<Option<ReadDirEntry>, anyhow::Error> {
        let txn = self.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;
        if let Some(entry) = dir_table.get((parent_inode, name))? {
            let entry: ReadDirEntry = bincode::deserialize(entry.value())?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    pub fn readdir(
        &self,
        inode: u64,
    ) -> Result<impl Iterator<Item = Result<(String, ReadDirEntry), anyhow::Error>>, anyhow::Error>
    {
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
            let entry: ReadDirEntry = bincode::deserialize(value.value())?;
            entries.push(Ok((name, entry)));
        }

        Ok(entries.into_iter())
    }

    pub fn get_file_metadata(&self, inode: u64) -> Result<Option<FileMetadata>, anyhow::Error> {
        let txn = self.db.begin_read()?;
        let file_table = txn.open_table(FILE_TABLE)?;
        if let Some(entry) = file_table.get(inode)? {
            let files: Vec<(Peer, FileEntry)> = bincode::deserialize(entry.value())?;
            let best_entry = files.into_iter().max_by_key(|(_, e)| e.metadata.mtime);
            Ok(best_entry.map(|(_, e)| e.metadata))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Arena, Path, Peer};
    use std::time::{Duration, SystemTime};
    use tempfile::tempdir;

    fn test_peer() -> Peer {
        Peer::from("test_peer")
    }

    fn test_arena() -> Arena {
        Arena::from("test_arena")
    }

    #[test]
    fn open_creates_tables() -> anyhow::Result<()> {
        let _ = env_logger::try_init();

        let dir = tempdir()?;
        let path = dir.path().join("unreal.db");
        let cache = UnrealCache::open(&path)?;
        let txn = cache.db.begin_read()?;
        assert!(txn.open_table(DIRECTORY_TABLE).is_ok());
        assert!(txn.open_table(FILE_TABLE).is_ok());
        Ok(())
    }

    #[test]
    fn link_creates_directories() -> anyhow::Result<()> {
        let _ = env_logger::try_init();

        let dir = tempdir()?;
        let path = dir.path().join("unreal.db");
        let cache = UnrealCache::open(&path)?;
        let peer = test_peer();
        let arena = test_arena();
        let file_path = Path::parse("a/b/c.txt")?;
        let mtime = SystemTime::now();

        cache.link(&peer, &arena, &file_path, 100, mtime)?;

        let txn = cache.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;
        let entry = dir_table.get((1, "a"))?.unwrap();
        let entry: ReadDirEntry = bincode::deserialize(entry.value()).unwrap();
        assert_eq!(entry.assignment, InodeAssignment::Directory);

        let entry = dir_table.get((entry.inode, "b"))?.unwrap();
        let entry: ReadDirEntry = bincode::deserialize(entry.value()).unwrap();
        assert_eq!(entry.assignment, InodeAssignment::Directory);

        Ok(())
    }

    #[test]
    fn link_updates_existing_file() -> anyhow::Result<()> {
        let _ = env_logger::try_init();

        let dir = tempdir()?;
        let path = dir.path().join("unreal.db");
        let cache = UnrealCache::open(&path)?;
        let peer = test_peer();
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;
        let old_mtime = SystemTime::now();
        let new_mtime = old_mtime + Duration::from_secs(1);

        cache.link(&peer, &arena, &file_path, 100, old_mtime)?;
        cache.link(&peer, &arena, &file_path, 200, new_mtime)?;

        let txn = cache.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;
        let entry = dir_table.get((1, "file.txt"))?.unwrap();
        let dir_entry: ReadDirEntry = bincode::deserialize(entry.value()).unwrap();

        let file_table = txn.open_table(FILE_TABLE)?;
        let file_entry = file_table.get(dir_entry.inode)?.unwrap();
        let files: Vec<(Peer, FileEntry)> = bincode::deserialize(file_entry.value()).unwrap();
        assert_eq!(files[0].1.metadata.size, 200);
        assert_eq!(files[0].1.metadata.mtime, new_mtime);

        Ok(())
    }

    #[test]
    fn link_ignores_older_mtime() -> anyhow::Result<()> {
        let _ = env_logger::try_init();

        let dir = tempdir()?;
        let path = dir.path().join("unreal.db");
        let cache = UnrealCache::open(&path)?;
        let peer = test_peer();
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;
        let new_mtime = SystemTime::now();
        let old_mtime = new_mtime - Duration::from_secs(1);

        cache.link(&peer, &arena, &file_path, 100, new_mtime)?;
        cache.link(&peer, &arena, &file_path, 200, old_mtime)?;

        let txn = cache.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;
        let entry = dir_table.get((1, "file.txt"))?.unwrap();
        let dir_entry: ReadDirEntry = bincode::deserialize(entry.value()).unwrap();

        let file_table = txn.open_table(FILE_TABLE)?;
        let file_entry = file_table.get(dir_entry.inode)?.unwrap();
        let files: Vec<(Peer, FileEntry)> = bincode::deserialize(file_entry.value()).unwrap();
        assert_eq!(files[0].1.metadata.size, 100);
        assert_eq!(files[0].1.metadata.mtime, new_mtime);

        Ok(())
    }

    #[test]
    fn unlink_removes_file() -> anyhow::Result<()> {
        let _ = env_logger::try_init();

        let dir = tempdir()?;
        let path = dir.path().join("unreal.db");
        let cache = UnrealCache::open(&path)?;
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
        let _ = env_logger::try_init();

        let dir = tempdir()?;
        let path = dir.path().join("unreal.db");
        let cache = UnrealCache::open(&path)?;
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
        let _ = env_logger::try_init();

        let dir = tempdir()?;
        let path = dir.path().join("unreal.db");
        let cache = UnrealCache::open(&path)?;
        let peer = test_peer();
        let arena = test_arena();
        let file_path = Path::parse("a/file.txt")?;
        let mtime = SystemTime::now();

        cache.link(&peer, &arena, &file_path, 100, mtime)?;

        // Lookup directory
        let dir_entry = cache.lookup(1, "a")?.unwrap();
        assert_eq!(dir_entry.assignment, InodeAssignment::Directory);

        // Lookup file
        let file_entry = cache.lookup(dir_entry.inode, "file.txt")?.unwrap();
        assert_eq!(file_entry.assignment, InodeAssignment::File);

        Ok(())
    }

    #[test]
    fn lookup_returns_none_for_missing_entry() -> anyhow::Result<()> {
        let _ = env_logger::try_init();

        let dir = tempdir()?;
        let path = dir.path().join("unreal.db");
        let cache = UnrealCache::open(&path)?;

        assert!(cache.lookup(1, "nonexistent")?.is_none());

        Ok(())
    }

    #[test]
    fn readdir_returns_all_entries() -> anyhow::Result<()> {
        let _ = env_logger::try_init();

        let dir = tempdir()?;
        let path = dir.path().join("unreal.db");
        let cache = UnrealCache::open(&path)?;
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

        let dir_entry = cache.lookup(1, "dir")?.unwrap();
        let entries: Vec<_> = cache
            .readdir(dir_entry.inode)?
            .collect::<Result<Vec<_>, _>>()?;

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
        let _ = env_logger::try_init();

        let dir = tempdir()?;
        let path = dir.path().join("unreal.db");
        let cache = UnrealCache::open(&path)?;

        let peer1 = Peer::from("peer1");
        let peer2 = Peer::from("peer2");
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;

        let mtime1 = SystemTime::now();
        let mtime2 = mtime1 + Duration::from_secs(1);

        cache.link(&peer1, &arena, &file_path, 100, mtime1)?;
        cache.link(&peer2, &arena, &file_path, 200, mtime2)?;

        let file_entry = cache.lookup(1, "file.txt")?.unwrap();
        let metadata = cache.get_file_metadata(file_entry.inode)?.unwrap();

        assert_eq!(metadata.size, 200);
        assert_eq!(metadata.mtime, mtime2);

        Ok(())
    }
}
