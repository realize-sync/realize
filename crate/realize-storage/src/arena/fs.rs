use super::blob::BlobReadOperations;
use super::cache::{CacheExt, CacheReadOperations};
use super::db::ArenaDatabase;
use super::peer::PeersReadOperations;
use super::tree::TreeExt;
use super::types::FileMetadata;
use super::update;
use crate::arena::mark::MarkExt;
use crate::arena::notifier::{Notification, Progress};
use crate::arena::tree::TreeReadOperations;
use crate::arena::types::DirMetadata;
use crate::global::fs::FileContent;
use crate::types::Inode;
use crate::{Blob, FileAlternative, FileRealm, Mark};
use crate::{PathId, StorageError};
use realize_types::{Arena, Path, Peer};
use std::sync::Arc;
use tokio::runtime::Handle;

// TreeLoc is necessary to call ArenaCache methods
pub(crate) use super::tree::TreeLoc;

/// A per-arena cache of remote files.
///
/// This struct handles all cache operations for a specific arena.
/// It contains the arena's database and root pathid.
pub(crate) struct ArenaFilesystem {
    arena: Arena,
    db: Arc<ArenaDatabase>,
}

impl ArenaFilesystem {
    #[cfg(test)]
    #[allow(dead_code)]
    pub fn for_testing_single_arena(
        arena: realize_types::Arena,
        blob_dir: &std::path::Path,
        datadir: &std::path::Path,
    ) -> anyhow::Result<Arc<Self>> {
        ArenaFilesystem::for_testing(
            arena,
            crate::PathIdAllocator::new(
                crate::GlobalDatabase::new(crate::utils::redb_utils::in_memory()?)?,
                [arena],
            )?,
            blob_dir,
            datadir,
        )
    }

    #[cfg(test)]
    pub fn for_testing(
        arena: realize_types::Arena,
        allocator: Arc<crate::PathIdAllocator>,
        blob_dir: &std::path::Path,
        datadir: &std::path::Path,
    ) -> anyhow::Result<Arc<Self>> {
        let db = ArenaDatabase::new(
            crate::utils::redb_utils::in_memory()?,
            arena,
            allocator,
            blob_dir,
            datadir,
        )?;

        Ok(ArenaFilesystem::new(arena, Arc::clone(&db))?)
    }

    /// Create a new ArenaCache from an arena, root pathid, database, and blob directory.
    pub(crate) fn new(arena: Arena, db: Arc<ArenaDatabase>) -> Result<Arc<Self>, StorageError> {
        Ok(Arc::new(Self { arena, db }))
    }

    pub(crate) fn arena(&self) -> Arena {
        self.arena
    }

    pub(crate) fn lookup(
        &self,
        loc: impl Into<ArenaFsLoc>,
    ) -> Result<(Inode, crate::arena::types::Metadata), StorageError> {
        let txn = self.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        let pathid = tree.expect(loc.into().into_tree_loc(&cache)?)?;
        let metadata = cache
            .metadata(&tree, pathid)?
            .ok_or(StorageError::NotFound)?;
        let inode = cache.map_to_inode(pathid)?;
        Ok((inode, metadata))
    }

    pub(crate) fn file_metadata(
        &self,
        loc: impl Into<ArenaFsLoc>,
    ) -> Result<FileMetadata, StorageError> {
        let txn = self.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        cache.file_metadata(&tree, loc.into().into_tree_loc(&cache)?)
    }

    pub(crate) fn dir_metadata(
        &self,
        loc: impl Into<ArenaFsLoc>,
    ) -> Result<DirMetadata, StorageError> {
        let txn = self.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        Ok(DirMetadata::modifiable(
            cache.dir_mtime(&tree, loc.into().into_tree_loc(&cache)?)?,
        ))
    }

    pub(crate) fn metadata(
        &self,
        loc: impl Into<ArenaFsLoc>,
    ) -> Result<crate::arena::types::Metadata, StorageError> {
        let txn = self.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        cache
            .metadata(&tree, loc.into().into_tree_loc(&cache)?)?
            .ok_or(StorageError::NotFound)
    }

    pub(crate) fn readdir(
        &self,
        loc: impl Into<ArenaFsLoc>,
    ) -> Result<Vec<(String, Inode, crate::arena::types::Metadata)>, StorageError> {
        let mut ret = vec![];
        let mut no_pathid = vec![];
        let dir_pathid;
        {
            let txn = self.db.begin_read()?;
            let tree = txn.read_tree()?;
            let cache = txn.read_cache()?;
            dir_pathid = tree.expect(loc.into().into_tree_loc(&cache)?)?;
            for res in cache.readdir(&tree, dir_pathid) {
                let (name, pathid, metadata) = res?;
                if let Some(pathid) = pathid {
                    ret.push((name, cache.map_to_inode(pathid)?, metadata));
                } else {
                    no_pathid.push((name, metadata));
                }
            }
        }
        if !no_pathid.is_empty() {
            let txn = self.db.begin_write()?;
            {
                let mut tree = txn.write_tree()?;
                let mut cache = txn.write_cache()?;
                let mut blobs = txn.write_blobs()?;
                let mut dirty = txn.write_dirty()?;
                for (name, metadata) in no_pathid {
                    let pathid = match metadata {
                        crate::Metadata::File(_) => cache.preindex(
                            &mut tree,
                            &mut blobs,
                            &mut dirty,
                            (dir_pathid, &name),
                        )?,
                        crate::Metadata::Dir(_) => {
                            cache
                                .mkdir(&mut tree, (dir_pathid, &name), Some(metadata.mtime()))?
                                .0
                        }
                    };
                    ret.push((name, cache.map_to_inode(pathid)?, metadata));
                }
            }
            txn.commit()?;
        }

        Ok(ret)
    }

    pub(crate) fn peer_progress(&self, peer: Peer) -> Result<Option<Progress>, StorageError> {
        let txn = self.db.begin_read()?;
        let peers = txn.read_peers()?;
        peers.progress(peer)
    }

    pub(crate) fn unlink(&self, loc: impl Into<ArenaFsLoc>) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let mut blobs = txn.write_blobs()?;
            let mut history = txn.write_history()?;
            let mut dirty = txn.write_dirty()?;
            let mut cache = txn.write_cache()?;

            cache.unlink(
                &mut tree,
                &mut blobs,
                &mut history,
                &mut dirty,
                loc.into().into_tree_loc(&cache)?,
            )?;
        }
        txn.commit()?;

        Ok(())
    }

    /// Create a local file with the given options.
    ///
    /// This call takes care of immediately registering the new file as modified.
    pub(crate) fn create(
        &self,
        mut options: tokio::fs::OpenOptions,
        loc: impl Into<ArenaFsLoc>,
    ) -> Result<(Inode, tokio::fs::File), StorageError> {
        let txn = self.db.begin_write()?;
        let file = {
            let mut tree = txn.write_tree()?;
            let mut blobs = txn.write_blobs()?;
            let mut dirty = txn.write_dirty()?;
            let mut cache = txn.write_cache()?;

            let loc = loc.into().into_tree_loc(&cache)?;
            let pathid = tree.setup(loc.borrow())?;
            let path = tree
                .backtrack(loc.borrow())?
                .ok_or(StorageError::AlreadyExists)?;

            // Parent must exist in the cache, but might not exist on
            // the filesystem.
            if let Some(parent) = tree.parent(pathid)? {
                match cache.metadata(&tree, parent)? {
                    Some(crate::arena::types::Metadata::Dir(_)) => {}
                    Some(crate::arena::types::Metadata::File(_)) => {
                        return Err(StorageError::NotADirectory);
                    }
                    None => {
                        return Err(StorageError::NotFound);
                    }
                }
            }
            let realpath = path.within(cache.datadir());
            if let Some(parent) = realpath.parent() {
                std::fs::create_dir_all(parent)?;
            }

            // Create the file
            options.create(true);
            let file = Handle::current().block_on(options.open(realpath))?;

            // Store it as modified file; don't add history entry yet.
            cache.preindex(&mut tree, &mut blobs, &mut dirty, pathid)?;

            (cache.map_to_inode(pathid)?, file)
        };
        txn.commit()?;

        Ok(file)
    }

    pub(crate) fn branch(
        &self,
        source: impl Into<ArenaFsLoc>,
        dest: impl Into<ArenaFsLoc>,
    ) -> Result<(Inode, FileMetadata), StorageError> {
        let txn = self.db.begin_write()?;
        let result = {
            let mut tree = txn.write_tree()?;
            let mut blobs = txn.write_blobs()?;
            let mut history = txn.write_history()?;
            let mut dirty = txn.write_dirty()?;
            let mut cache = txn.write_cache()?;

            let (pathid, m) = cache.branch(
                &mut tree,
                &mut blobs,
                &mut history,
                &mut dirty,
                source.into().into_tree_loc(&cache)?,
                dest.into().into_tree_loc(&cache)?,
            )?;

            (cache.map_to_inode(pathid)?, m)
        };
        txn.commit()?;

        Ok(result)
    }

    pub(crate) fn rename(
        &self,
        source: impl Into<ArenaFsLoc>,
        dest: impl Into<ArenaFsLoc>,
        noreplace: bool,
    ) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let mut blobs = txn.write_blobs()?;
            let mut history = txn.write_history()?;
            let mut dirty = txn.write_dirty()?;
            let mut cache = txn.write_cache()?;

            cache.rename(
                &mut tree,
                &mut blobs,
                &mut history,
                &mut dirty,
                source.into().into_tree_loc(&cache)?,
                dest.into().into_tree_loc(&cache)?,
                noreplace,
            )?;
        };
        txn.commit()?;

        Ok(())
    }

    pub(crate) fn update(
        &self,
        peer: Peer,
        notification: Notification,
    ) -> Result<(), StorageError> {
        update::apply(&self.db, peer, notification)
    }

    /// Open a file for reading/writing.
    pub(crate) fn file_content(
        &self,
        loc: impl Into<ArenaFsLoc>,
    ) -> Result<FileContent, StorageError> {
        // Optimistically start with a read transaction, which may
        // need to be upgraded to a write transaction if a blob need
        // to be created.
        let pathid;
        {
            let txn = self.db.begin_read()?;
            let tree = txn.read_tree()?;
            let loc = loc.into().into_tree_loc(&txn.read_cache()?)?;
            pathid = tree.expect(loc.borrow())?;
            let cache = txn.read_cache()?;
            if cache.file_entry_or_err(&tree, pathid)?.is_local() {
                let path = tree.backtrack(loc)?.ok_or(StorageError::IsADirectory)?;
                let realpath = path.within(cache.datadir());
                return Ok(FileContent::Local(realpath));
            }
            let blobs = txn.read_blobs()?;
            if let Some(info) = blobs.get_with_pathid(pathid)? {
                return Ok(FileContent::Remote(Blob::open_with_info(&self.db, info)?));
            }
        }

        // Switch to a write transaction to create the blob. We need to read
        // the file entry again because it might have changed.
        let txn = self.db.begin_write()?;
        let info = {
            let mut cache = txn.write_cache()?;
            cache.create_blob(
                &mut txn.write_tree()?,
                &mut txn.write_blobs()?,
                &txn.read_marks()?,
                pathid,
            )?
        };
        txn.commit()?;

        Ok(FileContent::Remote(Blob::open_with_info(&self.db, info)?))
    }

    /// Specifies the type of file (local or remote) and its cache status.
    pub(crate) fn file_realm(&self, loc: impl Into<ArenaFsLoc>) -> Result<FileRealm, StorageError> {
        let txn = self.db.begin_read()?;
        let tree = txn.read_tree()?;
        let blobs = txn.read_blobs()?;
        let cache = txn.read_cache()?;
        cache.file_realm(&tree, &blobs, loc.into().into_tree_loc(&cache)?)
    }

    /// Create a directory at the given path.
    pub(crate) fn mkdir(
        &self,
        loc: impl Into<ArenaFsLoc>,
    ) -> Result<(Inode, DirMetadata), StorageError> {
        let txn = self.db.begin_write()?;
        let result = {
            let mut tree = txn.write_tree()?;
            let mut cache = txn.write_cache()?;

            let (pathid, m) = cache.mkdir(&mut tree, loc.into().into_tree_loc(&cache)?, None)?;

            (cache.map_to_inode(pathid)?, m)
        };
        txn.commit()?;

        Ok(result)
    }

    /// Remove an empty directory at the given path.
    pub(crate) fn rmdir(&self, loc: impl Into<ArenaFsLoc>) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let mut cache = txn.write_cache()?;
            cache.rmdir(&mut tree, loc.into().into_tree_loc(&cache)?)?;
        }
        txn.commit()?;

        Ok(())
    }

    pub(crate) fn get_mark(
        &self,
        loc: impl Into<ArenaFsLoc>,
    ) -> Result<(Mark, bool), StorageError> {
        let txn = self.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        let marks = txn.read_marks()?;
        let loc = loc.into().into_tree_loc(&cache)?;

        marks.get_full(&tree, loc)
    }

    pub(crate) fn set_mark(
        &self,
        loc: impl Into<ArenaFsLoc>,
        mark: Mark,
    ) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut marks = txn.write_marks()?;
            let mut tree = txn.write_tree()?;
            let mut dirty = txn.write_dirty()?;
            let cache = txn.read_cache()?;
            let loc = loc.into().into_tree_loc(&cache)?;

            marks.set(&mut tree, &mut dirty, loc, mark)?;
        }
        txn.commit()?;
        Ok(())
    }

    pub(crate) fn clear_mark(&self, loc: impl Into<ArenaFsLoc>) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut marks = txn.write_marks()?;
            let mut tree = txn.write_tree()?;
            let mut dirty = txn.write_dirty()?;
            let cache = txn.read_cache()?;
            let loc = loc.into().into_tree_loc(&cache)?;

            marks.clear(&mut tree, &mut dirty, loc)?;
        }
        txn.commit()?;
        Ok(())
    }

    pub(crate) fn list_alternatives(
        &self,
        loc: impl Into<ArenaFsLoc>,
    ) -> Result<Vec<FileAlternative>, StorageError> {
        let txn = self.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        let loc = loc.into().into_tree_loc(&cache)?;

        cache.list_alternatives(&tree, loc)
    }

    /// Select an alternative version of the file, identified by its hash.
    ///
    /// The hash passed to this method should be one of the hashes
    /// reported by [list_alternatives] for the same file.
    ///
    /// This is typically used to switch to another peer's version
    /// when peers don't all agree on the version.
    pub(crate) fn select_alternative(
        &self,
        loc: impl Into<ArenaFsLoc>,
        goal: &realize_types::Hash,
    ) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let mut blobs = txn.write_blobs()?;
            let mut history = txn.write_history()?;
            let mut dirty = txn.write_dirty()?;
            let mut cache = txn.write_cache()?;
            let loc = loc.into().into_tree_loc(&cache)?;

            cache.select_alternative(&mut tree, &mut blobs, &mut history, &mut dirty, loc, goal)?;
        }
        txn.commit()?;

        Ok(())
    }
}

pub(crate) enum ArenaFsLoc {
    PathId(PathId),
    Inode(Inode),
    Path(Path),
    PathIdAndName(PathId, String),
    InodeAndName(Inode, String),
}

impl ArenaFsLoc {
    fn into_tree_loc(
        self,
        cache: &impl CacheReadOperations,
    ) -> Result<TreeLoc<'static>, StorageError> {
        Ok(match self {
            ArenaFsLoc::PathId(pathid) => TreeLoc::PathId(pathid),
            ArenaFsLoc::Path(path) => TreeLoc::Path(path),
            ArenaFsLoc::PathIdAndName(pathid, name) => TreeLoc::PathIdAndName(pathid, name.into()),
            ArenaFsLoc::Inode(inode) => TreeLoc::PathId(cache.map_to_pathid(inode)?),
            ArenaFsLoc::InodeAndName(inode, name) => {
                TreeLoc::PathIdAndName(cache.map_to_pathid(inode)?, name.into())
            }
        })
    }
}
impl From<PathId> for ArenaFsLoc {
    fn from(value: PathId) -> Self {
        ArenaFsLoc::PathId(value)
    }
}

impl From<Inode> for ArenaFsLoc {
    fn from(value: Inode) -> Self {
        ArenaFsLoc::Inode(value)
    }
}

impl From<Path> for ArenaFsLoc {
    fn from(value: Path) -> Self {
        ArenaFsLoc::Path(value)
    }
}

impl From<&Path> for ArenaFsLoc {
    fn from(value: &Path) -> Self {
        ArenaFsLoc::Path(value.clone())
    }
}

impl From<(PathId, &str)> for ArenaFsLoc {
    fn from(value: (PathId, &str)) -> Self {
        ArenaFsLoc::PathIdAndName(value.0, value.1.to_string())
    }
}

impl From<(PathId, &String)> for ArenaFsLoc {
    fn from(value: (PathId, &String)) -> Self {
        ArenaFsLoc::PathIdAndName(value.0, value.1.to_string())
    }
}
impl From<(PathId, String)> for ArenaFsLoc {
    fn from(value: (PathId, String)) -> Self {
        ArenaFsLoc::PathIdAndName(value.0, value.1)
    }
}

impl From<(Inode, &str)> for ArenaFsLoc {
    fn from(value: (Inode, &str)) -> Self {
        ArenaFsLoc::InodeAndName(value.0, value.1.to_string())
    }
}

impl From<(Inode, &String)> for ArenaFsLoc {
    fn from(value: (Inode, &String)) -> Self {
        ArenaFsLoc::InodeAndName(value.0, value.1.to_string())
    }
}
impl From<(Inode, String)> for ArenaFsLoc {
    fn from(value: (Inode, String)) -> Self {
        ArenaFsLoc::InodeAndName(value.0, value.1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_fs::TempDir;
    use assert_fs::fixture::ChildPath;
    use assert_fs::prelude::*;
    use realize_types::{Hash, Path, Peer, UnixTime};

    fn test_peer() -> Peer {
        Peer::from("test")
    }

    struct Fixture {
        fs: Arc<ArenaFilesystem>,
        db: Arc<ArenaDatabase>,
        datadir: ChildPath,
        _tempdir: TempDir,
    }
    impl Fixture {
        fn setup() -> anyhow::Result<Fixture> {
            let _ = env_logger::try_init();
            let arena = Arena::from("myarena");
            let tempdir = TempDir::new()?;
            let blob_dir = tempdir.child(format!("{arena}/blobs"));
            let datadir = tempdir.child(format!("{arena}/blobs"));
            blob_dir.create_dir_all()?;
            datadir.create_dir_all()?;
            let fs =
                ArenaFilesystem::for_testing_single_arena(arena, blob_dir.path(), datadir.path())?;
            let db = Arc::clone(&fs.db);
            Ok(Self {
                fs,
                db,
                datadir,
                _tempdir: tempdir,
            })
        }
    }

    #[test]
    fn empty_cache_readdir() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        assert!(fixture.fs.readdir(fixture.db.tree().root())?.is_empty());

        Ok(())
    }

    #[test]
    fn lookup_in_cache() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let test_path = Path::parse("dir/test_file.txt")?;
        update::apply(
            &fixture.db,
            test_peer(),
            Notification::Add {
                arena: fixture.fs.arena(),
                index: 1,
                path: test_path.clone(),
                mtime: UnixTime::from_secs(1234567890),
                size: 1024,
                hash: Hash([1u8; 32]),
            },
        )?;

        // lookup should find the file and dir metadata in cache
        fixture.fs.lookup(&test_path).unwrap();
        fixture.fs.lookup(Path::parse("dir")?).unwrap();

        Ok(())
    }

    #[test]
    fn lookup_not_found() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let nonexistent_path = Path::parse("nonexistent.txt")?;
        let result = fixture.fs.lookup(&nonexistent_path);
        assert!(matches!(result, Err(StorageError::NotFound)));

        Ok(())
    }

    #[test]
    fn readdir_detects_new_files_and_dir() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let dir = Path::parse("localdir")?;
        fixture.fs.mkdir(&dir)?;
        fixture
            .datadir
            .child("localdir/localfile1")
            .write_str(".")?;
        fixture
            .datadir
            .child("localdir/localfile2")
            .write_str("..")?;
        fixture
            .datadir
            .child("localdir/localfile3")
            .write_str("...")?;
        fixture
            .datadir
            .child("localdir/subdir/localfile4")
            .write_str("....")?;

        let dircontent = fixture.fs.readdir(&dir)?;
        // the files should be returned, even though they haven't been indexed or assigned
        // pathids yet.
        assert_unordered::assert_eq_unordered!(
            vec![
                ("localfile1", false),
                ("localfile2", false),
                ("localfile3", false),
                ("subdir", true)
            ],
            dircontent
                .iter()
                .map(|(name, _, m)| (name.as_str(), m.is_dir()))
                .collect::<Vec<_>>()
        );
        // Make sure readdir and lookup are consistent
        for (name, inode, metadata) in dircontent {
            assert_eq!(
                fixture.fs.lookup(dir.join(&name)?).unwrap(),
                (inode, metadata)
            );
        }

        Ok(())
    }
}
