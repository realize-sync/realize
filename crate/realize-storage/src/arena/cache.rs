use super::blob::{BlobExt, BlobInfo, WritableOpenBlob};
use super::dirty::WritableOpenDirty;
use super::tree::{TreeExt, TreeReadOperations, WritableOpenTree};
use super::types::{
    CacheTableEntry, DirTableEntry, FileEntryKind, FileMetadata, FileRealm, FileTableEntry,
    IndexedFile, Layer, RemoteAvailability,
};
use crate::arena::blob::BlobReadOperations;
use crate::arena::db::Tag;
use crate::arena::history::WritableOpenHistory;
use crate::arena::mark::MarkReadOperations;
use crate::arena::tree::{self, TreeLoc};
use crate::arena::types::{DirMetadata, Version};
use crate::global::types::PathAssignment;
use crate::types::Inode;
use crate::utils::holder::Holder;
use crate::utils::inhibit::{self, Inhibit};
use crate::{FileAlternative, PathId, StorageError};
use realize_types::{Hash, Path, Peer, UnixTime};
use redb::{ReadableTable, Table};
use std::collections::HashSet;
use std::path::PathBuf;

/// Read operations for cache. See also [CacheExt].
pub(crate) trait CacheReadOperations {
    /// Get metadata for a file or directory.
    fn metadata<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<Option<crate::Metadata>, StorageError>;

    /// Specifies the type of file (local or remote) and its cache status.
    fn file_realm<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        blobs: &impl BlobReadOperations,
        loc: L,
    ) -> Result<FileRealm, StorageError>;

    /// Get remote file availability information for the given pathid and version.
    fn remote_availability<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
        hash: &Hash,
    ) -> Result<Option<RemoteAvailability>, StorageError>;

    /// List available alternative versions for a specific file.
    ///
    /// Return an empty vector if `loc` is a directory or doesn't
    /// exist.
    fn list_alternatives<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<Vec<FileAlternative>, StorageError>;

    /// Read directory contents for the given pathid.
    fn readdir<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> impl Iterator<Item = Result<(String, Option<PathId>, crate::Metadata), StorageError>>;

    /// Get the default file entry for the given pathid.
    fn file_at_pathid(&self, pathid: PathId) -> Result<Option<FileTableEntry>, StorageError>;

    /// Get the default file entry for the given pathid; fail if the entry
    /// cannot be found or if it is a directory.
    fn file_at_pathid_or_err(&self, pathid: PathId) -> Result<FileTableEntry, StorageError>;

    /// Return the [Inode] appropriate for the given [PathId].
    #[allow(dead_code)]
    fn map_to_inode(&self, pathid: PathId) -> Result<Inode, StorageError>;

    /// Return the [PathId] appropriate for the given [Inode].
    #[allow(dead_code)]
    fn map_to_pathid(&self, inode: Inode) -> Result<PathId, StorageError>;

    /// Get the indexed entry at the given pathid
    fn index_entry_at_pathid(&self, pathid: PathId) -> Result<Option<IndexedFile>, StorageError>;

    /// Return all indexed files in the index layer
    fn all_indexed(&self) -> impl Iterator<Item = Result<(PathId, IndexedFile), StorageError>>;

    /// Return the datadir path.
    fn datadir(&self) -> &std::path::Path;

    /// Return the path of the given location within the datadir path.
    fn local_path<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<std::path::PathBuf, StorageError>;
}

/// A cache open for reading with a read transaction.
pub(crate) struct ReadableOpenCache<'a, T, PN, NP>
where
    T: ReadableTable<(PathId, Layer), Holder<'static, CacheTableEntry>>,
    PN: ReadableTable<PathId, Inode>,
    NP: ReadableTable<Inode, PathId>,
{
    table: T,
    #[allow(dead_code)]
    pathid_to_inode: PN,
    #[allow(dead_code)]
    inode_to_pathid: NP,
    cache: &'a Cache,
}

impl<'a, T, PN, NP> ReadableOpenCache<'a, T, PN, NP>
where
    T: ReadableTable<(PathId, Layer), Holder<'static, CacheTableEntry>>,
    PN: ReadableTable<PathId, Inode>,
    NP: ReadableTable<Inode, PathId>,
{
    pub(crate) fn new(
        table: T,
        pathid_to_inode: PN,
        inode_to_pathid: NP,
        cache: &'a Cache,
    ) -> Self {
        Self {
            table,
            pathid_to_inode,
            inode_to_pathid,
            cache,
        }
    }
}

impl<'a, T, PN, NP> CacheReadOperations for ReadableOpenCache<'a, T, PN, NP>
where
    T: ReadableTable<(PathId, Layer), Holder<'static, CacheTableEntry>>,
    PN: ReadableTable<PathId, Inode>,
    NP: ReadableTable<Inode, PathId>,
{
    fn metadata<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<Option<crate::Metadata>, StorageError> {
        return metadata(&self.table, tree, &self.cache.datadir, loc);
    }

    fn file_realm<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        blobs: &impl BlobReadOperations,
        loc: L,
    ) -> Result<FileRealm, StorageError> {
        file_realm(&self.table, tree, blobs, &self.cache.datadir, loc)
    }

    fn remote_availability<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
        hash: &Hash,
    ) -> Result<Option<RemoteAvailability>, StorageError> {
        remote_availability(&self.table, tree, loc, hash)
    }

    fn list_alternatives<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<Vec<FileAlternative>, StorageError> {
        list_alternatives(&self.table, tree, loc)
    }

    fn readdir<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> impl Iterator<Item = Result<(String, Option<PathId>, crate::Metadata), StorageError>> {
        let loc = loc.into();
        ReadDirIterator::new(
            &self.table,
            tree,
            self.local_path(tree, loc.borrow()).ok(),
            loc,
        )
    }

    fn file_at_pathid(&self, pathid: PathId) -> Result<Option<FileTableEntry>, StorageError> {
        default_file_entry(&self.table, pathid)
    }

    fn file_at_pathid_or_err(&self, pathid: PathId) -> Result<FileTableEntry, StorageError> {
        default_file_entry_or_err(&self.table, pathid)
    }

    fn map_to_inode(&self, pathid: PathId) -> Result<Inode, StorageError> {
        map_to_inode(&self.pathid_to_inode, pathid)
    }

    fn map_to_pathid(&self, inode: Inode) -> Result<PathId, StorageError> {
        map_to_pathid(&self.inode_to_pathid, inode)
    }

    fn index_entry_at_pathid(&self, pathid: PathId) -> Result<Option<IndexedFile>, StorageError> {
        if let Some(e) = default_file_entry(&self.table, pathid)? {
            return Ok(e.into());
        }
        Ok(None)
    }

    fn all_indexed(&self) -> impl Iterator<Item = Result<(PathId, IndexedFile), StorageError>> {
        AllIndexedIterator::new(&self.table)
    }

    fn datadir(&self) -> &std::path::Path {
        self.cache.datadir()
    }

    /// Return the path of the given location within the datadir path.
    fn local_path<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<std::path::PathBuf, StorageError> {
        local_path(self.cache.datadir(), tree, loc)
    }
}

impl<'a> CacheReadOperations for WritableOpenCache<'a> {
    fn metadata<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<Option<crate::Metadata>, StorageError> {
        return metadata(&self.table, tree, &self.cache.datadir, loc);
    }

    fn file_realm<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        blobs: &impl BlobReadOperations,
        loc: L,
    ) -> Result<FileRealm, StorageError> {
        file_realm(&self.table, tree, blobs, &self.cache.datadir, loc)
    }

    fn remote_availability<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
        hash: &Hash,
    ) -> Result<Option<RemoteAvailability>, StorageError> {
        remote_availability(&self.table, tree, loc, hash)
    }

    fn list_alternatives<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<Vec<FileAlternative>, StorageError> {
        list_alternatives(&self.table, tree, loc)
    }

    fn readdir<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> impl Iterator<Item = Result<(String, Option<PathId>, crate::Metadata), StorageError>> {
        let loc = loc.into();

        ReadDirIterator::new(
            &self.table,
            tree,
            self.local_path(tree, loc.borrow()).ok(),
            loc,
        )
    }

    fn file_at_pathid(&self, pathid: PathId) -> Result<Option<FileTableEntry>, StorageError> {
        default_file_entry(&self.table, pathid)
    }

    fn file_at_pathid_or_err(&self, pathid: PathId) -> Result<FileTableEntry, StorageError> {
        default_file_entry_or_err(&self.table, pathid)
    }

    fn map_to_inode(&self, pathid: PathId) -> Result<Inode, StorageError> {
        map_to_inode(&self.pathid_to_inode, pathid)
    }

    fn map_to_pathid(&self, inode: Inode) -> Result<PathId, StorageError> {
        map_to_pathid(&self.inode_to_pathid, inode)
    }

    fn index_entry_at_pathid(&self, pathid: PathId) -> Result<Option<IndexedFile>, StorageError> {
        if let Some(e) = default_file_entry(&self.table, pathid)? {
            return Ok(e.into());
        }
        Ok(None)
    }

    fn all_indexed(&self) -> impl Iterator<Item = Result<(PathId, IndexedFile), StorageError>> {
        AllIndexedIterator::new(&self.table)
    }

    fn datadir(&self) -> &std::path::Path {
        self.cache.datadir()
    }

    /// Return the path of the given location within the datadir path.
    fn local_path<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<std::path::PathBuf, StorageError> {
        local_path(self.cache.datadir(), tree, loc)
    }
}

/// Extend [CacheReadOperations] with convenience functions for working
/// with paths and tree locations.
pub(crate) trait CacheExt {
    /// Get a file entry.
    ///
    /// Return None if the file is not found or if it is a directory.
    #[allow(dead_code)]
    fn file_entry<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<Option<FileTableEntry>, StorageError>;

    /// Get a file entry, failing if the file is not found or if it is a directory.
    #[allow(dead_code)]
    fn file_entry_or_err<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<FileTableEntry, StorageError>;

    fn file_metadata<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<FileMetadata, StorageError>;

    fn dir_mtime<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<UnixTime, StorageError>;

    /// Get a file entry by path.
    fn indexed<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<Option<IndexedFile>, StorageError>;

    fn has_local_file<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<bool, StorageError>;
}

impl<T: CacheReadOperations> CacheExt for T {
    fn file_metadata<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<FileMetadata, StorageError> {
        self.metadata(tree, loc)?
            .ok_or(StorageError::NotFound)?
            .expect_file()
    }

    fn dir_mtime<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<UnixTime, StorageError> {
        Ok(self
            .metadata(tree, loc)?
            .ok_or(StorageError::NotFound)?
            .expect_dir()?
            .mtime)
    }

    fn file_entry<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<Option<FileTableEntry>, StorageError> {
        if let Some(pathid) = tree.resolve(loc)? {
            self.file_at_pathid(pathid)
        } else {
            Ok(None)
        }
    }
    fn file_entry_or_err<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<FileTableEntry, StorageError> {
        self.file_at_pathid_or_err(tree.expect(loc)?)
    }

    fn indexed<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<Option<IndexedFile>, StorageError> {
        if let Some(pathid) = tree.resolve(loc)? {
            self.index_entry_at_pathid(pathid)
        } else {
            Ok(None)
        }
    }
    fn has_local_file<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<bool, StorageError> {
        Ok(self.file_entry(tree, loc)?.is_some_and(|e| e.is_local()))
    }
}

/// A cache open for writing with a write transaction.
pub(crate) struct WritableOpenCache<'a> {
    table: Table<'a, (PathId, Layer), Holder<'static, CacheTableEntry>>,
    pathid_to_inode: Table<'a, PathId, Inode>,
    inode_to_pathid: Table<'a, Inode, PathId>,
    pending_catchup_table: Table<'a, (&'static str, PathId), ()>,
    tag: Tag,
    cache: &'a Cache,
    _guard: inhibit::Guard,
}

impl<'a> WritableOpenCache<'a> {
    pub(crate) fn new(
        tag: Tag,
        table: Table<'a, (PathId, Layer), Holder<CacheTableEntry>>,
        pathid_to_inode: Table<'a, PathId, Inode>,
        inode_to_pathid: Table<'a, Inode, PathId>,
        pending_catchup_table: Table<'a, (&'static str, PathId), ()>,
        cache: &'a Cache,
    ) -> Self {
        Self {
            table,
            pathid_to_inode,
            inode_to_pathid,
            pending_catchup_table,
            tag,
            cache,
            _guard: cache.inhibit_watcher(),
        }
    }

    /// Create a blob for that file, unless one already exists.
    pub(crate) fn create_blob<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        blobs: &mut WritableOpenBlob,
        marks: &impl MarkReadOperations,
        loc: L,
    ) -> Result<BlobInfo, StorageError> {
        let pathid = tree.expect(loc)?;
        let entry = default_file_entry_or_err(&self.table, pathid)?;
        blobs.create(
            tree,
            marks,
            pathid,
            entry.version.expect_indexed()?,
            entry.size,
        )
    }

    /// Remove file locally, even though it might still be available in other peers.
    ///
    /// Also lets other peers know about this local change.
    pub(crate) fn unlink<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        blobs: &mut WritableOpenBlob,
        history: &mut WritableOpenHistory,
        dirty: &mut WritableOpenDirty,
        loc: L,
    ) -> Result<(), StorageError> {
        let loc = loc.into();
        let pathid = tree.expect(loc.borrow())?;
        let e = default_file_entry_or_err(&self.table, pathid)?;
        let path = tree.backtrack(loc)?;
        log::debug!(
            "[{}]@local Local removal of \"{}\" pathid {pathid} {:?}",
            self.tag,
            path,
            e.version
        );

        // not using cleanup_local_file to delete unconditionally,
        // even a locally-modified file as the "unlink" command comes
        // directly from the user.
        let realpath = path.within(self.datadir());
        if realpath.exists() {
            std::fs::remove_file(&realpath)?;
        }
        log::debug!("[{}]@local delete {realpath:?}", self.tag);

        self.rm_default_file_entry(tree, blobs, dirty, pathid)?;
        history.report_removed(&path, &e.version)?;

        Ok(())
    }

    /// Rename a file from one location to another.
    pub(crate) fn rename<'l1, 'l2, L1: Into<TreeLoc<'l1>>, L2: Into<TreeLoc<'l2>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        blobs: &mut WritableOpenBlob,
        history: &mut WritableOpenHistory,
        dirty: &mut WritableOpenDirty,
        source: L1,
        dest: L2,
        noreplace: bool,
    ) -> Result<(), StorageError> {
        let (source_pathid, dest_pathid) =
            self.rename_internal(tree, blobs, history, dirty, source, dest, noreplace)?;

        // swap source and destination inodes, to meet FUSE's
        // expectation of what rename does.
        let source_inode = self.map_to_inode(source_pathid)?;
        let dest_inode = self.map_to_inode(dest_pathid)?;
        self.pathid_to_inode.insert(source_pathid, dest_inode)?;
        self.pathid_to_inode.insert(dest_pathid, source_inode)?;
        self.inode_to_pathid.insert(source_inode, dest_pathid)?;
        self.inode_to_pathid.insert(dest_inode, source_pathid)?;
        Ok(())
    }

    fn rename_internal<'l1, 'l2, L1: Into<TreeLoc<'l1>>, L2: Into<TreeLoc<'l2>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        blobs: &mut WritableOpenBlob,
        history: &mut WritableOpenHistory,
        dirty: &mut WritableOpenDirty,
        source: L1,
        dest: L2,
        noreplace: bool,
    ) -> Result<(PathId, PathId), StorageError> {
        let source = source.into();
        let dest = dest.into();
        let source_pathid = tree.expect(source.borrow())?;
        if source_pathid == tree.root() {
            return Err(StorageError::PermissionDenied);
        }
        let dest_pathid; // delay calling tree.setup until source is checked
        match default_entry(&self.table, source_pathid)?.ok_or(StorageError::NotFound)? {
            CacheTableEntry::Dir(dir_entry) => {
                dest_pathid = tree.setup(dest.borrow())?;
                if dest_pathid == tree.root() {
                    return Err(StorageError::PermissionDenied);
                }
                match default_entry(&self.table, dest_pathid)? {
                    None => {}
                    Some(CacheTableEntry::File(_)) => return Err(StorageError::NotADirectory),
                    Some(CacheTableEntry::Dir(_)) => {
                        // Reproduces Linux renameat behavior: with noreplace=false, if dest
                        // is a directory, it must be empty.
                        if noreplace || !self.readdir(tree, dest_pathid).next().is_none() {
                            return Err(StorageError::AlreadyExists);
                        }
                    }
                }
                for (direntry_name, direntry_pathid) in tree
                    .readdir(source_pathid)
                    .map(|res| res.map(|(n, p)| (n.to_string(), p)))
                    .collect::<Result<Vec<_>, _>>()?
                {
                    match self.rename_internal(
                        tree,
                        blobs,
                        history,
                        dirty,
                        direntry_pathid,
                        (dest_pathid, direntry_name),
                        noreplace,
                    ) {
                        Ok(_) => {}
                        Err(StorageError::NotFound) => {}
                        Err(err) => return Err(err),
                    };
                }
                write_dir(&mut self.table, tree, dest_pathid, &dir_entry)?;
                tree.remove_and_decref(
                    source_pathid,
                    &mut self.table,
                    (source_pathid, Layer::Default),
                )?;
                let _ = std::fs::remove_dir(self.local_path(tree, source_pathid)?);
            }
            CacheTableEntry::File(mut source_entry) => {
                dest_pathid = tree.setup(dest.borrow())?;
                let mut old_dest = None;
                match default_entry(&self.table, dest_pathid)? {
                    None => {}
                    Some(CacheTableEntry::File(entry)) => {
                        old_dest = Some(entry);
                    }
                    Some(CacheTableEntry::Dir(_)) => return Err(StorageError::IsADirectory),
                };
                if old_dest.is_some() && noreplace {
                    return Err(StorageError::AlreadyExists);
                }

                let source_path = tree.backtrack(source)?;
                let dest_path = tree.backtrack(dest)?;
                if source_entry.is_local() {
                    let source_realpath = source_path.within(self.datadir());
                    let dest_realpath = dest_path.within(self.datadir());

                    if let Some(parent) = dest_realpath.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    std::fs::rename(&source_realpath, &dest_realpath)?;

                    // (over)write new entry in database
                    self.write_default_file_entry(tree, blobs, dirty, dest_pathid, &source_entry)?;

                    // delete older entry in database
                    self.rm_default_file_entry(tree, blobs, dirty, source_pathid)?;

                    history.report_added(&dest_path, old_dest.as_ref().map(|e| &e.version))?;
                    history.report_removed(&source_path, &source_entry.version)?;
                } else {
                    // (over)write new entry
                    source_entry.kind = FileEntryKind::Branched(source_pathid);
                    if let Some(e) = &old_dest {
                        self.cleanup_local_file(tree, &dest_path, e);
                    }
                    self.write_default_file_entry(tree, blobs, dirty, dest_pathid, &source_entry)?;

                    // delete older entry
                    self.rm_default_file_entry(tree, blobs, dirty, source_pathid)?;

                    if let Some(e) = &old_dest {
                        history.report_removed(&dest_path, &e.version)?;
                    }
                    history.request_rename(
                        &source_path,
                        &dest_path,
                        source_entry.version.expect_indexed()?,
                    )?;
                }
            }
        }

        Ok((source_pathid, dest_pathid))
    }

    /// Branch a file to another location in the tree.
    pub(crate) fn branch<'l1, 'l2, L1: Into<TreeLoc<'l1>>, L2: Into<TreeLoc<'l2>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        blobs: &mut WritableOpenBlob,
        history: &mut WritableOpenHistory,
        dirty: &mut WritableOpenDirty,
        source: L1,
        dest: L2,
    ) -> Result<(PathId, FileMetadata), StorageError> {
        let source = source.into();
        let source_pathid = tree.expect(source.borrow())?;
        if source_pathid == tree.root() {
            return Err(StorageError::PermissionDenied);
        }
        let source_path = tree.backtrack(source)?;
        let mut source_entry = self.file_at_pathid_or_err(source_pathid)?;

        // prepare dest, but only after checking source existence
        let dest = dest.into();
        let dest_pathid = tree.setup(dest.borrow())?;
        if source_pathid == tree.root() {
            return Err(StorageError::PermissionDenied);
        }
        let dest_path = tree.backtrack(dest)?;
        check_parent_is_dir(&self.table, tree, dest_pathid)?;
        if default_entry(&self.table, dest_pathid)?.is_some() {
            return Err(StorageError::AlreadyExists);
        }

        if source_entry.is_local() {
            // We own the source, make change on the filesystem
            std::fs::hard_link(
                &source_path.within(self.datadir()),
                &dest_path.within(self.datadir()),
            )?;

            // write dest in the database
            self.write_default_file_entry(tree, blobs, dirty, dest_pathid, &source_entry)?;

            history.report_added(&dest_path, None)?;
        } else {
            // We don't own the source, so request the owner of the
            // source to execute the branch, and simulate it in the
            // cache for now.
            history.request_branch(
                &source_path,
                &dest_path,
                source_entry.version.expect_indexed()?,
            )?;

            // write dest in the database and optionally the filesystem
            source_entry.kind = FileEntryKind::Branched(source_pathid);
            self.write_default_file_entry(tree, blobs, dirty, dest_pathid, &source_entry)?;
        }

        Ok((dest_pathid, source_entry.into()))
    }

    /// Creates a local directory is the database and on the filesystem.
    ///
    /// A directory might already exist in the database, but be only
    /// as remote directory, in which case this calls marks the entry
    /// as being local.
    ///
    /// This call won't return the error "already exists", even if the
    /// directory exists on the database as well as on the filesystem
    /// already.
    pub(crate) fn mkdir<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        loc: L,
    ) -> Result<(PathId, DirMetadata), StorageError> {
        let loc = loc.into();
        let pathid = tree.setup(loc.borrow())?;
        check_parent_is_dir(&self.table, tree, pathid)?;
        let cached = write_dir_local(&mut self.table, tree, pathid)?;

        let realpath = self.local_path(tree, loc)?;
        std::fs::create_dir_all(&realpath)?;

        let metadata = DirMetadata::merged(&cached, &realpath.symlink_metadata()?);
        Ok((pathid, metadata))
    }

    /// Remove a local directory on the database and on the filesystem.
    ///
    /// The directory must be empty for this call to work, that is,
    /// there must not be any local nor remote files.
    pub(crate) fn rmdir<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        loc: L,
    ) -> Result<(), StorageError> {
        let loc = loc.into();
        let pathid = tree.expect(loc.borrow())?;
        if pathid == tree.root() {
            return Err(StorageError::PermissionDenied);
        }
        match default_entry(&self.table, pathid)? {
            None => Err(StorageError::NotFound),
            Some(CacheTableEntry::File(_)) => Err(StorageError::NotADirectory),
            Some(CacheTableEntry::Dir(existing)) => {
                if !self.readdir(tree, pathid).next().is_none() {
                    return Err(StorageError::DirectoryNotEmpty);
                }
                tree.remove_and_decref(pathid, &mut self.table, (pathid, Layer::Default))?;
                if existing.is_remote() {
                    if let Some(parent_pathid) = tree.parent(pathid)? {
                        write_dir_mtime(&mut self.table, tree, parent_pathid, UnixTime::now())?;
                    }
                }
                let realpath = self.local_path(tree, loc)?;
                if realpath.exists() {
                    std::fs::remove_dir(realpath)?;
                }

                Ok(())
            }
        }
    }

    /// Add a file entry for a peer.
    pub(crate) fn notify_added(
        &mut self,
        tree: &mut WritableOpenTree,
        blobs: &mut WritableOpenBlob,
        dirty: &mut WritableOpenDirty,
        peer: Peer,
        path: Path,
        mtime: UnixTime,
        size: u64,
        hash: Hash,
    ) -> Result<(), StorageError> {
        let file_pathid = tree.setup(&path)?;
        let entry = FileTableEntry::new(
            size,
            mtime,
            Version::Indexed(hash.clone()),
            FileEntryKind::RemoteFile,
        );
        let mut updated_default = false;
        if peer_file_entry(&self.table, file_pathid, None)?.is_none() {
            log::debug!("[{}]@local Add \"{path}\" {hash} size={size}", self.tag);
            self.write_default_file_entry(tree, blobs, dirty, file_pathid, &entry)?;
            updated_default = true;
        }
        if peer_file_entry(&self.table, file_pathid, Some(peer))?.is_none() {
            log::debug!("[{}]@{peer} Add \"{path}\" {hash} size={size}", self.tag);
            self.write_file_entry(tree, file_pathid, peer, &entry)?;
            if !updated_default {
                dirty.mark_dirty(file_pathid, "add_from_peer")?;
            }
        }
        Ok(())
    }

    /// Replace a file entry for a peer.
    pub(crate) fn notify_replaced(
        &mut self,
        tree: &mut WritableOpenTree,
        blobs: &mut WritableOpenBlob,
        dirty: &mut WritableOpenDirty,
        peer: Peer,
        path: &Path,
        mtime: UnixTime,
        size: u64,
        hash: &Hash,
        old_hash: &Hash,
    ) -> Result<(), StorageError> {
        let pathid = tree.setup(path)?;
        let entry = FileTableEntry::new(
            size,
            mtime,
            Version::Indexed(hash.clone()),
            FileEntryKind::RemoteFile,
        );
        let mut default_modified = false;
        let prev = peer_file_entry(&self.table, pathid, None)?;
        if prev
            .as_ref()
            .map(|e| e.version.matches_hash(old_hash))
            .unwrap_or(true)
        {
            log::debug!(
                "[{}]@local \"{path}\" {hash} size={size} replaces {old_hash}",
                self.tag
            );
            default_modified = true;
            if let Some(prev) = &prev {
                self.cleanup_local_file(tree, path, prev);
            }
            self.write_default_file_entry(tree, blobs, dirty, pathid, &entry)?;
        }
        if peer_file_entry(&self.table, pathid, Some(peer))?
            .map(|e| e.version.matches_hash(old_hash))
            .unwrap_or(true)
        {
            log::debug!(
                "[{}]@{peer} \"{path}\" {hash} size={size} replaces {old_hash}",
                self.tag
            );
            self.write_file_entry(tree, pathid, peer, &entry)?;
            if !default_modified {
                dirty.mark_dirty(pathid, "replace_from_peer")?;
            }
        }
        Ok(())
    }

    /// Start catchup for a peer.
    pub(crate) fn catchup_start(&mut self, peer: Peer) -> Result<(), StorageError> {
        for elt in self.table.iter()? {
            let (k, _) = elt?;
            let (pathid, layer) = k.value();
            if let Layer::Remote(elt_peer) = layer
                && elt_peer == peer
            {
                self.pending_catchup_table
                    .insert((peer.as_str(), pathid), ())?;
            }
        }
        Ok(())
    }

    /// Process a catchup notification.
    pub(crate) fn catchup(
        &mut self,
        tree: &mut WritableOpenTree,
        blobs: &mut WritableOpenBlob,
        dirty: &mut WritableOpenDirty,
        peer: Peer,
        path: Path,
        mtime: UnixTime,
        size: u64,
        hash: Hash,
    ) -> Result<(), StorageError> {
        let file_pathid = tree.setup(&path)?;
        unmark_peer_file(&mut self.pending_catchup_table, peer, file_pathid)?;
        let entry = FileTableEntry::new(
            size,
            mtime,
            Version::Indexed(hash.clone()),
            FileEntryKind::RemoteFile,
        );
        self.write_file_entry(tree, file_pathid, peer, &entry)?;
        if !peer_file_entry(&self.table, file_pathid, None)?.is_some() {
            self.write_default_file_entry(tree, blobs, dirty, file_pathid, &entry)?;
        };

        Ok(())
    }

    /// Complete catchup for a peer.
    pub(crate) fn catchup_complete(
        &mut self,
        tree: &mut WritableOpenTree,
        blobs: &mut WritableOpenBlob,
        dirty: &mut WritableOpenDirty,
        peer: Peer,
    ) -> Result<(), StorageError> {
        self.delete_marked_files(tree, blobs, dirty, peer)?;
        Ok(())
    }

    /// Write a file entry for a specific peer.
    fn write_file_entry(
        &mut self,
        tree: &mut WritableOpenTree,
        file_pathid: PathId,
        peer: Peer,
        entry: &FileTableEntry,
    ) -> Result<(), StorageError> {
        tree.insert_and_incref(
            file_pathid,
            &mut self.table,
            (file_pathid, Layer::Remote(peer)),
            Holder::new(&CacheTableEntry::File(entry.clone()))?,
        )?;

        Ok(())
    }

    /// Write the default file entry.
    fn write_default_file_entry(
        &mut self,
        tree: &mut WritableOpenTree,
        blobs: &mut WritableOpenBlob,
        dirty: &mut WritableOpenDirty,
        pathid: PathId,
        new_entry: &FileTableEntry,
    ) -> Result<(), StorageError> {
        self.before_default_file_entry_change(tree, blobs, dirty, pathid)?;
        if tree.insert_and_incref(
            pathid,
            &mut self.table,
            (pathid, Layer::Default),
            Holder::new(&CacheTableEntry::File(new_entry.clone()))?,
        )? {
            if let Some(parent_pathid) = tree.parent(pathid)? {
                if new_entry.is_local() {
                    write_dir_local(&mut self.table, tree, parent_pathid)?;
                } else {
                    write_dir_mtime(&mut self.table, tree, parent_pathid, UnixTime::now())?;
                }
            }
        }

        Ok(())
    }

    /// This must be executed before updating or removing the default
    /// file entry.
    fn before_default_file_entry_change(
        &mut self,
        tree: &mut WritableOpenTree,
        blobs: &mut WritableOpenBlob,
        dirty: &mut WritableOpenDirty,
        pathid: PathId,
    ) -> Result<(), StorageError> {
        blobs.delete(tree, pathid)?;

        // This entry is the outside world view of the file, so
        // changes should be reported.
        dirty.mark_dirty(pathid, "cache")?;

        Ok(())
    }

    /// Remove a file entry for a specific peer. The default entry
    /// must be updated separately, as needed.
    fn rm_peer_file_entry(
        &mut self,
        tree: &mut WritableOpenTree,
        pathid: PathId,
        peer: Peer,
    ) -> Result<(), StorageError> {
        // Remove the entry
        tree.remove_and_decref(pathid, &mut self.table, (pathid, Layer::Remote(peer)))?;

        Ok(())
    }

    /// Remove any local file if it exists and matches the entry. This is best effort.
    fn cleanup_local_file<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
        entry: &FileTableEntry,
    ) {
        if !entry.is_local() {
            return;
        }
        if let Ok(realpath) = self.local_path(tree, loc) {
            if !entry.matches_file(&realpath) {
                return;
            }
            match std::fs::remove_file(&realpath) {
                Err(err) => log::debug!("[{}]@local delete {realpath:?} failed: {err:?}", self.tag),
                Ok(_) => log::debug!("[{}]@local delete {realpath:?}", self.tag),
            };
        }
    }

    /// Remove a default file entry, leaving peer entries untouched.
    fn rm_default_file_entry(
        &mut self,
        tree: &mut WritableOpenTree,
        blobs: &mut WritableOpenBlob,
        dirty: &mut WritableOpenDirty,
        pathid: PathId,
    ) -> Result<(), StorageError> {
        // We check the parent before removing, since the link to
        // parent might not exist anymore afterwards if the pathid is
        // deleted as well.
        let parent = tree.parent(pathid)?;

        self.before_default_file_entry_change(tree, blobs, dirty, pathid)?;
        if tree.remove_and_decref(pathid, &mut self.table, (pathid, Layer::Default))? {
            // Update the parent modification time, as removing an
            // entry modifies it.
            if let Some(parent) = parent {
                write_dir_mtime(&mut self.table, tree, parent, UnixTime::now())?;
            }
        }

        Ok(())
    }

    /// Process a remove or drop notification from a peer.
    pub(crate) fn notify_dropped_or_removed<T>(
        &mut self,
        tree: &mut WritableOpenTree,
        blobs: &mut WritableOpenBlob,
        dirty: &mut WritableOpenDirty,
        peer: Peer,
        path: T,
        old_hash: &Hash,
        dropped: bool,
    ) -> Result<(), StorageError>
    where
        T: AsRef<Path>,
    {
        let path = path.as_ref();
        if let Some(pathid) = tree.resolve(path)? {
            if let Some(e) = peer_file_entry(&self.table, pathid, Some(peer))?
                && e.version.matches_hash(old_hash)
            {
                log::debug!(
                    "[{}]@{peer} Remove \"{path}\" pathid {pathid} {old_hash}",
                    self.tag
                );

                self.rm_peer_file_entry(tree, pathid, peer)?;
            }
            if !dropped {
                if let Some(e) = peer_file_entry(&self.table, pathid, None)?
                    && e.version.matches_hash(old_hash)
                {
                    log::debug!(
                        "[{}]@local Remove \"{path}\" pathid {pathid} {old_hash}",
                        self.tag
                    );
                    if e.matches_file(path.within(self.datadir())) {
                        let realpath = path.within(self.datadir());
                        log::debug!("[{}]@local delete {realpath:?}", self.tag);
                        std::fs::remove_file(realpath)?;
                    }
                    let parent = tree.parent(pathid)?;
                    self.rm_default_file_entry(tree, blobs, dirty, pathid)?;
                    if let Some(parent) = parent {
                        // Get rid of unnecessary parent dirs. This only happens when
                        // files are removed due to remote notifications and when
                        // dirs have no local equivalent.
                        delete_empty_remote_dir(&mut self.table, tree, parent)?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Delete all marked files for a peer.
    fn delete_marked_files(
        &mut self,
        tree: &mut WritableOpenTree,
        blobs: &mut WritableOpenBlob,
        dirty: &mut WritableOpenDirty,
        peer: Peer,
    ) -> Result<(), StorageError> {
        let peer_str = peer.as_str();
        let mut pathids = vec![];
        for elt in self.pending_catchup_table.extract_from_if(
            (peer_str, PathId::ZERO)..=(peer_str, PathId::MAX),
            |_, _| true,
        )? {
            let elt = elt?;
            let (_, pathid) = elt.0.value();
            pathids.push(pathid);
        }
        for pathid in pathids {
            self.rm_peer_file_entry(tree, pathid, peer)?;
            if let Some(entry) = self.file_at_pathid(pathid)? {
                if !entry.is_local()
                    && self
                        .remote_availability(tree, pathid, entry.version.expect_indexed()?)?
                        .is_none()
                {
                    // If the file has become unavailable because of
                    // this removal, remove the file itself as well
                    // and any now empty containing directories.
                    let parent = tree.parent(pathid)?;
                    self.rm_default_file_entry(tree, blobs, dirty, pathid)?;
                    if let Some(parent) = parent {
                        delete_empty_remote_dir(&mut self.table, tree, parent)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn preindex<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        blobs: &mut WritableOpenBlob,
        dirty: &mut WritableOpenDirty,
        loc: L,
    ) -> Result<PathId, StorageError> {
        let loc = loc.into();
        let pathid = tree.setup(loc.borrow())?;
        let realpath = self.local_path(tree, loc)?;
        let m = match realpath.symlink_metadata() {
            Ok(m) => m,
            Err(_) => return Err(StorageError::LocalFileMismatch),
        };
        if m.is_dir() {
            return Err(StorageError::IsADirectory);
        }
        let original = default_file_entry(&self.table, pathid)?;
        let kind = if m.is_file() {
            FileEntryKind::LocalFile
        } else {
            FileEntryKind::SpecialFile
        };
        if let Some(e) = &original
            && e.kind == kind
            && matches!(e.version, Version::Modified(_))
        {
            // Update is not needed
            return Ok(pathid);
        }
        let version = Version::modification_of(original.map(|e| e.version));
        let entry = FileTableEntry::new(m.len(), UnixTime::mtime(&m), version, kind);
        self.write_default_file_entry(tree, blobs, dirty, pathid, &entry)?;
        log::debug!(
            "[{}]@local record modified for version {:?} of {realpath:?} as {:?}",
            self.tag,
            entry.version,
            entry.kind,
        );
        log::debug!("ook0= {entry:?}");
        Ok(pathid)
    }

    pub(crate) fn index<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        blobs: &mut WritableOpenBlob,
        history: &mut WritableOpenHistory,
        dirty: &mut WritableOpenDirty,
        loc: L,
        size: u64,
        mtime: UnixTime,
        hash: Hash,
    ) -> Result<PathId, StorageError> {
        let loc = loc.into();
        let pathid = tree.setup(loc.borrow())?;
        let path = tree.backtrack(loc.borrow())?;
        let old_version = default_file_entry(&self.table, pathid)?.map(|e| e.version);
        let entry = IndexedFile { hash, mtime, size };
        let realpath = path.within(self.datadir());
        if !entry.matches_file(&realpath) {
            return Err(StorageError::LocalFileMismatch);
        }
        self.write_default_file_entry(tree, blobs, dirty, pathid, &entry.into_file())?;
        history.report_added(&path, old_version.as_ref())?;

        Ok(pathid)
    }

    /// Prepare the database to realize the given entry, that is
    /// make entry in the index layer the default entry.
    ///
    /// Return (source, dest); to really realize the file, source must
    /// be moved to dest after committing the change.
    pub(crate) fn realize<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        blobs: &mut WritableOpenBlob,
        dirty: &mut WritableOpenDirty,
        history: &mut WritableOpenHistory,
        loc: L,
        modified: bool,
    ) -> Result<(PathBuf, PathBuf), StorageError> {
        let loc = loc.into();
        let pathid = tree.expect(loc.borrow())?;
        let path = tree.backtrack(loc)?;
        // Note that entry.path is not usable here as it would be
        // incorrect in a branched entry.

        let entry = default_file_entry_or_err(&self.table, pathid)?;
        let source = blobs.realize(tree, pathid)?.ok_or(StorageError::NotFound)?;
        let dest = path.within(self.datadir());

        self.write_default_file_entry(
            tree,
            blobs,
            dirty,
            pathid,
            &FileTableEntry::new(
                entry.size,
                entry.mtime,
                if modified {
                    Version::modification_of(Some(entry.version))
                } else {
                    entry.version
                },
                FileEntryKind::LocalFile,
            ),
        )?;
        if !modified {
            // TODO: should it be report_available?
            history.report_added(&path, None)?;
        }

        return Ok((source, dest));
    }

    /// Prepare the database to unrealize the given entry, that is
    /// remove the entry from the index and keep the current file as a
    /// blob, if possible.
    ///
    /// An indexed entry must exist.
    pub(crate) fn unrealize<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        blobs: &mut WritableOpenBlob,
        history: &mut WritableOpenHistory,
        dirty: &mut WritableOpenDirty,
        marks: &impl MarkReadOperations,
        loc: L,
    ) -> Result<(PathBuf, PathBuf), StorageError> {
        let loc = loc.into();
        let pathid = tree.expect(loc.borrow())?;
        let path = tree.backtrack(loc.borrow())?;
        let source = path.within(self.datadir());
        let metadata = source.symlink_metadata()?;

        let entry = default_file_entry_or_err(&self.table, pathid)?;
        if !entry.is_local() {
            return Err(StorageError::NotFound);
        }
        if entry.size != metadata.len() || entry.mtime != UnixTime::mtime(&metadata) {
            return Err(StorageError::DatabaseOutdated(format!(
                "[{}] index outdated for {pathid} \"{path:?}\"",
                self.tag
            )));
        }
        let hash = entry.version.expect_indexed()?;
        history.report_dropped(&path, hash)?;

        // The file is useful as a blob; import it
        log::debug!(
            "[{}] Unrealize; import \"{path:?}\" {:?}",
            self.tag,
            entry.version
        );

        // The default entry doesn't reference the local entry
        // anymore, but rather an entry from cache. Caller should
        // have made sure it exists.
        let (_, peer_entry) = find_peer_entry(&self.table, pathid, hash)?
            .next()
            .ok_or(StorageError::NoPeers)??;
        self.write_default_file_entry(tree, blobs, dirty, pathid, &peer_entry)?;

        let dest = blobs.import(tree, marks, pathid, hash, &metadata)?;
        return Ok((source, dest));
    }

    /// Switch an indexed file back to preindex.
    ///
    /// This is reported to remote peers as a removal, but the local remains available locally.
    ///
    /// Return true if an indexed file was unindexed.
    pub(crate) fn unindex<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        blobs: &mut WritableOpenBlob,
        history: &mut WritableOpenHistory,
        dirty: &mut WritableOpenDirty,
        loc: L,
    ) -> Result<bool, StorageError> {
        let loc = loc.into();
        if let Some(pathid) = tree.resolve(loc.borrow())? {
            if let Some(mut entry) = default_file_entry(&self.table, pathid)?
                && entry.is_local()
                && matches!(entry.version, Version::Indexed(_))
            {
                let path = tree.backtrack(loc)?;
                history.report_removed(&path, &entry.version)?;
                entry.version = Version::modification_of(Some(entry.version));
                self.write_default_file_entry(tree, blobs, dirty, pathid, &entry)?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Remove a local file or directory entry at the given location, if it exists.
    ///
    /// Note that this only touches the local portion of the file and directory. Remote files and
    /// their parent directories are not removed.
    ///
    /// The actual file isn't modified or checked by this call.
    ///
    /// Return true if something was removed.
    pub(crate) fn remove_from_index<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        blobs: &mut WritableOpenBlob,
        history: &mut WritableOpenHistory,
        dirty: &mut WritableOpenDirty,
        loc: L,
    ) -> Result<bool, StorageError> {
        let loc = loc.into();
        if let Some(pathid) = tree.resolve(loc.borrow())? {
            match default_entry(&self.table, pathid)? {
                None => {}
                Some(CacheTableEntry::Dir(mut entry)) => {
                    if pathid == tree.root() {
                        return Err(StorageError::PermissionDenied);
                    }
                    if entry.local {
                        if entry.mtime.is_some()
                            && ReadDirIterator::new(&self.table, tree, None, pathid)
                                .next()
                                .is_some()
                        {
                            // The directory is available remotely and isn't
                            // empty; keep it.
                            entry.local = false;
                            self.table.insert(
                                (pathid, Layer::Default),
                                Holder::new(&CacheTableEntry::Dir(entry))?,
                            )?;
                        } else {
                            // This was a local-only directory; we can get rid of it.
                            tree.remove_and_decref(
                                pathid,
                                &mut self.table,
                                (pathid, Layer::Default),
                            )?;
                        }
                        return Ok(true);
                    }
                }
                Some(CacheTableEntry::File(entry)) => {
                    if entry.is_local() {
                        let path = tree.backtrack(loc)?;
                        history.report_removed(&path, &entry.version)?;
                        self.rm_default_file_entry(tree, blobs, dirty, pathid)?;
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    /// Remove a tree location (path or pathid) that can be a file or a
    /// directory.
    ///
    /// The actual file isn't modified or checked by this call.
    ///
    /// If the location is a directory, all files within that
    /// directory are removed, recursively.
    pub(crate) fn remove_from_index_recursively<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        blobs: &mut WritableOpenBlob,
        history: &mut WritableOpenHistory,
        dirty: &mut WritableOpenDirty,
        loc: L,
    ) -> Result<(), StorageError> {
        let pathid = match tree.resolve(loc)? {
            Some(p) => p,
            None => return Ok(()),
        };
        let children = tree.readdir_pathid(pathid).collect::<Result<Vec<_>, _>>()?;
        for (_, pathid) in children.into_iter() {
            self.remove_from_index_recursively(tree, blobs, history, dirty, pathid)?;
        }
        self.remove_from_index(tree, blobs, history, dirty, pathid)?;
        Ok(())
    }

    /// Select an alternative version of the file, identified by its hash.
    ///
    /// The hash passed to this method should be one of the hashes
    /// reported by [CacheReadOperations::list_alternatives] for the
    /// same file.
    ///
    /// This is typically used to switch to another peer's version
    /// when peers don't all agree on the version.
    ///
    /// When called on a local file, this deletes the local file in
    /// favor of a remote one. This can't be undone.
    ///
    /// The hash should be the hash of one of the [FileAlternative]s
    /// for the path or the operation fails with error
    /// [StorageError::UnknownVersion].
    pub(crate) fn select_alternative<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        blobs: &mut WritableOpenBlob,
        history: &mut WritableOpenHistory,
        dirty: &mut WritableOpenDirty,
        loc: L,
        goal: &Hash,
    ) -> Result<(), StorageError> {
        let loc = loc.into();
        let pathid = tree.expect(loc.borrow())?;
        let original = default_file_entry_or_err(&self.table, pathid)?;
        if original.version.indexed_hash().is_some_and(|h| *h == *goal) {
            // Nothing to do
            return Ok(());
        }
        let peer_entry = match find_peer_entry(&self.table, pathid, goal)?.next() {
            None => return Err(StorageError::UnknownVersion),
            Some(e) => {
                let (_peer, file_entry) = e?;

                file_entry
            }
        };
        self.write_default_file_entry(tree, blobs, dirty, pathid, &peer_entry)?;

        // Report changes to local, as this is a user's decision that
        // needs to be propagated to other peers currently tracking
        // the original version.
        if original.is_local() {
            let path = tree.backtrack(loc)?;
            self.cleanup_local_file(tree, &path, &original);

            // Report it added (replaced) so other peers are aware
            // of the user's decision, and then immediately drop
            // it, since it's not downloadable from this peer.
            history.report_added(&path, Some(&original.version))?;
            history.report_dropped(&path, goal)?;
        }
        Ok(())
    }
}

pub(crate) struct Cache {
    datadir: PathBuf,
    inhibit_watcher: Inhibit,
}

impl Cache {
    /// Initialize the database. This should be called at startup, in the
    /// transaction that crates new tables.
    pub(crate) fn setup(
        cache_table: &mut redb::Table<'_, (PathId, Layer), Holder<CacheTableEntry>>,
        root_pathid: PathId,
        datadir: &std::path::Path,
    ) -> Result<Self, StorageError> {
        if cache_table.get((root_pathid, Layer::Default))?.is_none() {
            // Exceptionally not using write_dir and not going
            // through tree because , arena roots aren't refcounted by
            // tree and are never deleted.
            cache_table.insert(
                (root_pathid, Layer::Default),
                Holder::with_content(CacheTableEntry::Dir(DirTableEntry {
                    local: true,
                    mtime: Some(UnixTime::now()),
                }))?,
            )?;
        }

        Ok(Cache {
            datadir: datadir.to_path_buf(),
            inhibit_watcher: Inhibit::new(),
        })
    }

    pub(crate) fn datadir(&self) -> &std::path::Path {
        &self.datadir
    }

    /// Return a barrier the file watcher should check before
    /// processing a filesystem change.
    ///
    /// This is used to prevent the watcher from running until all
    /// filesystem changes tied to a transactions have been executed.
    pub(crate) fn watcher_barrier(&self) -> inhibit::Barrier {
        self.inhibit_watcher.barrier()
    }

    /// Prevent the watcher from running.
    pub(crate) fn inhibit_watcher(&self) -> inhibit::Guard {
        self.inhibit_watcher.inhibit()
    }
}

/// Get metadata for a file or directory.
fn metadata<'b, L: Into<TreeLoc<'b>>>(
    cache_table: &impl ReadableTable<(PathId, Layer), Holder<'static, CacheTableEntry>>,
    tree: &impl TreeReadOperations,
    datadir: &std::path::Path,
    loc: L,
) -> Result<Option<crate::Metadata>, StorageError> {
    let loc = loc.into();
    let m = local_path(datadir, tree, loc.borrow())
        .ok()
        .and_then(|p| p.symlink_metadata().ok());
    let pathid = tree.resolve(loc)?;

    expand_metadata(cache_table, pathid, m)
}

/// Builds a [crate::Metadata], if possible, by merging
/// [std::fs::Metadata] and with a cache entry, identified by its
/// [PathId].
///
/// Either or both of the filesystem metadata or the cache entry may
/// not exist.
fn expand_metadata(
    cache_table: &impl ReadableTable<(PathId, Layer), Holder<'static, CacheTableEntry>>,
    pathid: Option<PathId>,
    real: Option<std::fs::Metadata>,
) -> Result<Option<crate::Metadata>, StorageError> {
    let cached = if let Some(pathid) = pathid {
        default_entry(cache_table, pathid)?
    } else {
        None
    };
    Ok(match (real, cached) {
        (None, None) => None,
        (None, Some(CacheTableEntry::File(cached))) => {
            if cached.is_local() {
                None
            } else {
                Some(crate::Metadata::File(cached.into()))
            }
        }
        (None, Some(CacheTableEntry::Dir(cached))) => Some(crate::Metadata::Dir(cached.into())),
        (Some(real), Some(cached)) => Some(crate::Metadata::merged(&cached, &real)),
        (Some(real), None) => Some(real.into()),
    })
}

fn file_realm<'b, L: Into<TreeLoc<'b>>>(
    cache_table: &impl ReadableTable<(PathId, Layer), Holder<'static, CacheTableEntry>>,
    tree: &impl TreeReadOperations,
    blobs: &impl BlobReadOperations,
    datadir: &std::path::Path,
    loc: L,
) -> Result<FileRealm, StorageError> {
    let loc = loc.into();
    let pathid = tree.expect(loc.borrow())?;
    if default_file_entry_or_err(cache_table, pathid)?.is_local() {
        return Ok(FileRealm::Local(tree.backtrack(loc)?.within(datadir)));
    }

    Ok(FileRealm::Remote(blobs.cache_status(tree, pathid)?))
}

/// Get file availability information for the given pathid.
fn remote_availability<'b, L: Into<TreeLoc<'b>>>(
    cache_table: &impl ReadableTable<(PathId, Layer), Holder<'static, CacheTableEntry>>,
    tree: &impl TreeReadOperations,
    loc: L,
    hash: &Hash,
) -> Result<Option<RemoteAvailability>, StorageError> {
    let mut avail = None;
    let pathid = tree.expect(loc)?;
    let mut visited = HashSet::new();
    let mut next = Some(pathid);
    while let Some(pathid) = next
        && !visited.contains(&pathid)
    {
        visited.insert(pathid); // avoid loops
        for entry in find_peer_entry(cache_table, pathid, hash)? {
            let (peer, file_entry) = entry?;
            if avail.is_none() {
                let path = tree.backtrack(pathid)?;
                avail = Some(RemoteAvailability {
                    arena: tree.arena(),
                    path,
                    size: file_entry.size,
                    hash: file_entry.version.expect_indexed()?.clone(),
                    peers: vec![],
                });
            }
            if let Some(avail) = &mut avail {
                avail.peers.push(peer);
            }
        }
        if avail.is_none() {
            next = default_file_entry(cache_table, pathid)?.and_then(|e| match e.kind {
                FileEntryKind::Branched(pathid) => Some(pathid),
                _ => None,
            });
        }
    }
    Ok(avail)
}

fn list_alternatives<'b, L: Into<TreeLoc<'b>>>(
    cache_table: &impl ReadableTable<(PathId, Layer), Holder<'static, CacheTableEntry>>,
    tree: &impl TreeReadOperations,
    loc: L,
) -> Result<Vec<FileAlternative>, StorageError> {
    let mut alternatives = vec![];
    if let Some(pathid) = tree.resolve(loc)? {
        for entry in
            cache_table.range((pathid, Layer::Default)..(pathid.plus(1), Layer::Default))?
        {
            let (k, v) = entry?;
            let (_, layer) = k.value();
            let entry = v.value().parse()?;
            log::debug!("{pathid:?} entry {entry:?}");
            if let CacheTableEntry::File(entry) = entry {
                match layer {
                    Layer::Default => {
                        match entry.kind {
                            FileEntryKind::LocalFile | FileEntryKind::SpecialFile => {
                                alternatives.push(FileAlternative::Local(entry.version.clone()));
                            }
                            FileEntryKind::RemoteFile => {
                                // will be reported from the peer layer
                            }
                            FileEntryKind::Branched(path_id) => {
                                let path = tree.backtrack(path_id)?;
                                alternatives.push(FileAlternative::Branched(
                                    path,
                                    entry.version.expect_indexed()?.clone(),
                                ));
                            }
                        }
                    }
                    Layer::Remote(peer) => {
                        alternatives.push(FileAlternative::Remote(
                            peer,
                            entry.version.expect_indexed()?.clone(),
                            entry.size,
                            entry.mtime,
                        ));
                    }
                }
            }
        }
    }

    Ok(alternatives)
}

fn find_peer_entry(
    cache_table: &impl ReadableTable<(PathId, Layer), Holder<'static, CacheTableEntry>>,
    pathid: PathId,
    hash: &Hash,
) -> Result<impl Iterator<Item = Result<(Peer, FileTableEntry), StorageError>>, StorageError> {
    Ok(cache_table
        .range((pathid, Layer::Remote(Peer::from("")))..(pathid.plus(1), Layer::Default))?
        .map(|entry| {
            let (k, v) = entry?;
            let (_, layer) = k.value();
            let peer = layer.peer().unwrap(); // range should guarantee it
            let entry = v.value().parse()?.expect_file()?;
            Ok::<(Peer, FileTableEntry), StorageError>((peer, entry))
        })
        .filter(|e| {
            e.as_ref()
                .ok()
                .map(|(_, e)| e.version.matches_hash(hash))
                .unwrap_or(false)
        }))
}

struct ReadDirIterator<'a, 'b, T>
where
    T: ReadableTable<(PathId, Layer), Holder<'static, CacheTableEntry>>,
{
    table: &'a T,
    iter: tree::ReadDirIterator<'b>,
    realentries: Option<(PathBuf, HashSet<String>)>,
}

impl<'a, 'b, T> ReadDirIterator<'a, 'b, T>
where
    T: ReadableTable<(PathId, Layer), Holder<'static, CacheTableEntry>>,
{
    fn new<'l, L: Into<TreeLoc<'l>>>(
        table: &'a T,
        tree: &'b impl TreeReadOperations,
        realpath: Option<PathBuf>,
        loc: L,
    ) -> Self {
        let realentries = realpath.and_then(|p| {
            let res = build_realentries(p);
            res.ok()
        });
        let iter = match tree.expect(loc) {
            Ok(pathid) => tree.readdir_pathid(pathid),
            Err(StorageError::NotFound) => {
                if realentries.is_none() {
                    tree::ReadDirIterator::failed(StorageError::NotFound)
                } else {
                    // Return local entries
                    tree::ReadDirIterator::empty()
                }
            }
            Err(err) => tree::ReadDirIterator::failed(err),
        };
        ReadDirIterator {
            table,
            iter,
            realentries,
        }
    }
}

fn build_realentries(realpath: PathBuf) -> Result<(PathBuf, HashSet<String>), std::io::Error> {
    let mut entries = HashSet::new();
    let readdir = std::fs::read_dir(&realpath)?;
    for entry in readdir {
        let entry = entry?;
        if let Ok(name) = entry.file_name().into_string() {
            // ignore files with invalid (non-unicode) names
            entries.insert(name);
        }
    }

    Ok((realpath, entries))
}

impl<'a, 'b, T> Iterator for ReadDirIterator<'a, 'b, T>
where
    T: ReadableTable<(PathId, Layer), Holder<'static, CacheTableEntry>>,
{
    type Item = Result<(String, Option<PathId>, crate::Metadata), StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(entry) = self.iter.next() {
            let (name, pathid) = match entry {
                Err(err) => return Some(Err(err)),
                Ok(ret) => ret,
            };
            let real = if let Some((realpath, names)) = self.realentries.as_mut() {
                names
                    .take(&name)
                    .and_then(|n| realpath.join(n).symlink_metadata().ok())
            } else {
                None
            };
            match expand_metadata(self.table, Some(pathid), real) {
                Ok(None) => {}
                Ok(Some(m)) => return Some(Ok((name, Some(pathid), m))),
                Err(err) => return Some(Err(err)),
            }
        }

        if let Some((realpath, entries)) = &mut self.realentries {
            let name = entries.iter().next().cloned();
            if let Some(name) = name {
                entries.remove(&name);
                if let Some(m) = realpath.join(&name).symlink_metadata().ok() {
                    return Some(Ok((name, None, m.into())));
                }
            }
        }

        None
    }
}

/// Iterator for all indexed files in the index layer
struct AllIndexedIterator<'a> {
    iter: Option<
        Result<redb::Range<'a, (PathId, Layer), Holder<'static, CacheTableEntry>>, StorageError>,
    >,
}

impl<'a> AllIndexedIterator<'a> {
    fn new(
        table: &'a impl ReadableTable<(PathId, Layer), Holder<'static, CacheTableEntry>>,
    ) -> Self {
        AllIndexedIterator {
            iter: Some(table.iter().map_err(StorageError::from)),
        }
    }
}

impl<'a> Iterator for AllIndexedIterator<'a> {
    type Item = Result<(PathId, IndexedFile), StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.iter {
            None => None,
            Some(Err(_)) => Some(Err(self.iter.take().unwrap().err().unwrap())),
            Some(Ok(range)) => {
                while let Some(entry) = range.next() {
                    match entry {
                        Err(err) => return Some(Err(err.into())),
                        Ok((key, value)) => {
                            let (pathid, layer) = key.value();
                            if let Layer::Default = layer {
                                match value.value().parse() {
                                    Err(err) => return Some(Err(StorageError::from(err))),
                                    Ok(CacheTableEntry::File(e)) => {
                                        if let Some(indexed) = e.into() {
                                            return Some(Ok((pathid, indexed)));
                                        }
                                        // continue
                                    }
                                    Ok(_) => {} // continue
                                }
                            }
                        }
                    }
                }
                None
            }
        }
    }
}

/// Get a [FileTableEntry] for a specific peer.
fn peer_file_entry(
    cache_table: &impl redb::ReadableTable<(PathId, Layer), Holder<'static, CacheTableEntry>>,
    pathid: PathId,
    peer: Option<Peer>,
) -> Result<Option<FileTableEntry>, StorageError> {
    let key = peer
        .map(|p| (pathid, Layer::Remote(p)))
        .unwrap_or_else(|| (pathid, Layer::Default));
    match cache_table.get(key)? {
        None => Ok(None),
        Some(e) => Ok(e.value().parse()?.file()),
    }
}

/// Get the default entry for the given pathid.
///
/// Returns None if the file cannot be found or if it is a directory.
fn default_file_entry(
    cache_table: &impl redb::ReadableTable<(PathId, Layer), Holder<'static, CacheTableEntry>>,
    pathid: PathId,
) -> Result<Option<FileTableEntry>, StorageError> {
    if let Some(CacheTableEntry::File(entry)) = default_entry(cache_table, pathid)? {
        return Ok(Some(entry));
    }

    Ok(None)
}

/// Get the default entry for the given pathid.
///
/// Returns None if the file cannot be found or if it is a directory.
fn default_entry(
    cache_table: &impl redb::ReadableTable<(PathId, Layer), Holder<'static, CacheTableEntry>>,
    pathid: PathId,
) -> Result<Option<CacheTableEntry>, StorageError> {
    if let Some(entry) = cache_table.get((pathid, Layer::Default))? {
        return Ok(Some(entry.value().parse()?));
    }

    Ok(None)
}

/// Get the default entry for the given pathid.
///
/// Fail if the file cannot be found or if it is a directory.
fn default_file_entry_or_err(
    cache_table: &impl redb::ReadableTable<(PathId, Layer), Holder<'static, CacheTableEntry>>,
    pathid: PathId,
) -> Result<FileTableEntry, StorageError> {
    default_entry(cache_table, pathid)?
        .ok_or(StorageError::NotFound)?
        .expect_file()
}

/// Update mtime of the current directory and its parents.
///
/// Does nothing if the given mtime is earlier than the one from the
/// directory.
///
/// If the directory doesn't exist, this calls creates it.
///
/// Returns the updated and merged [DirTableEntry].
fn write_dir_mtime(
    cache_table: &mut redb::Table<'_, (PathId, Layer), Holder<CacheTableEntry>>,
    tree: &mut WritableOpenTree,
    pathid: PathId,
    mtime: UnixTime,
) -> Result<DirTableEntry, StorageError> {
    write_dir(
        cache_table,
        tree,
        pathid,
        &DirTableEntry {
            mtime: Some(mtime),
            ..Default::default()
        },
    )
}

/// Mark dir and its parents as local.
///
/// Does nothing if the directories are already local.
///
/// If the directory doesn't exist, this calls creates it.
///
/// Returns the updated and merged [DirTableEntry].
fn write_dir_local(
    cache_table: &mut redb::Table<'_, (PathId, Layer), Holder<CacheTableEntry>>,
    tree: &mut WritableOpenTree,
    pathid: PathId,
) -> Result<DirTableEntry, StorageError> {
    write_dir(
        cache_table,
        tree,
        pathid,
        &DirTableEntry {
            local: true,
            ..Default::default()
        },
    )
}

/// Write or update a directory entry at the given id and its
/// ancestors.
///
/// `update` is merged with any existing entry using
/// [DirTableEntry::update].
///
/// If the directory doesn't exist, this calls creates it.
///
/// Returns the updated and merged [DirTableEntry].
fn write_dir(
    cache_table: &mut redb::Table<'_, (PathId, Layer), Holder<CacheTableEntry>>,
    tree: &mut WritableOpenTree,
    pathid: PathId,
    update: &DirTableEntry,
) -> Result<DirTableEntry, StorageError> {
    let entry = if let Some(existing) = default_entry(cache_table, pathid)? {
        let mut entry = existing.expect_dir()?;
        if !entry.update(update) {
            // No changes; don't bother updating this entry not its ancestors
            return Ok(entry);
        }

        entry
    } else {
        update.clone()
    };
    tree.insert_and_incref(
        pathid,
        cache_table,
        (pathid, Layer::Default),
        Holder::with_content(CacheTableEntry::Dir(entry.clone()))?,
    )?;

    if let Some(parent_pathid) = tree.parent(pathid)? {
        write_dir(cache_table, tree, parent_pathid, update)?;
    }

    Ok(entry)
}

fn delete_empty_remote_dir(
    cache_table: &mut redb::Table<'_, (PathId, Layer), Holder<CacheTableEntry>>,
    tree: &mut WritableOpenTree,
    pathid: PathId,
) -> Result<(), StorageError> {
    if let Some(existing) = default_entry(cache_table, pathid)? {
        let parent = tree.parent(pathid)?;
        let entry = existing.expect_dir()?;
        if !entry.local
            && ReadDirIterator::new(cache_table, tree, None, pathid)
                .next()
                .is_none()
        {
            tree.remove_and_decref(pathid, cache_table, (pathid, Layer::Default))?;
            if let Some(parent) = parent {
                return delete_empty_remote_dir(cache_table, tree, parent);
            }
        }
    }

    Ok(())
}

fn unmark_peer_file(
    pending_catchup_table: &mut Table<'_, (&'static str, PathId), ()>,
    peer: Peer,
    pathid: PathId,
) -> Result<(), StorageError> {
    pending_catchup_table.remove((peer.as_str(), pathid))?;

    Ok(())
}

/// Check whether the given pathid exists in the cache and whether it
/// is a file or a directory.
fn pathid_assignment(
    cache_table: &impl ReadableTable<(PathId, Layer), Holder<'static, CacheTableEntry>>,
    pathid: PathId,
) -> Result<Option<PathAssignment>, StorageError> {
    match cache_table.get((pathid, Layer::Default))? {
        Some(e) => match e.value().parse()? {
            CacheTableEntry::Dir(_) => Ok(Some(PathAssignment::Directory)),
            CacheTableEntry::File(_) => Ok(Some(PathAssignment::File)),
        },
        None => Ok(None),
    }
}

/// The parent must exist and be a directory. This is typically used
/// just before creating a file entry, to reproduce the strict behavior
/// of filesystems.
fn check_parent_is_dir(
    cache_table: &impl ReadableTable<(PathId, Layer), Holder<'static, CacheTableEntry>>,
    tree: &impl TreeReadOperations,
    pathid: PathId,
) -> Result<(), StorageError> {
    if let Some(parent) = tree.parent(pathid)? {
        check_is_dir(cache_table, parent)?;
    }

    Ok(())
}

/// Make sure the given pathid exists and is a directory.
fn check_is_dir(
    cache_table: &impl ReadableTable<(PathId, Layer), Holder<'static, CacheTableEntry>>,
    pathid: PathId,
) -> Result<(), StorageError> {
    match pathid_assignment(cache_table, pathid)? {
        None => Err(StorageError::NotFound),
        Some(PathAssignment::File) => Err(StorageError::NotADirectory),
        Some(PathAssignment::Directory) => Ok(()), // continue
    }
}

#[allow(dead_code)]
fn map_to_inode(
    pathid_to_inode: &impl ReadableTable<PathId, Inode>,
    pathid: PathId,
) -> Result<Inode, StorageError> {
    if let Some(inode) = pathid_to_inode.get(pathid)? {
        return Ok(inode.value());
    }

    Ok(Inode(pathid.as_u64()))
}

#[allow(dead_code)]
fn map_to_pathid(
    inode_to_pathid: &impl ReadableTable<Inode, PathId>,
    inode: Inode,
) -> Result<PathId, StorageError> {
    if let Some(pathid) = inode_to_pathid.get(inode)? {
        return Ok(pathid.value());
    }

    Ok(PathId(inode.as_u64()))
}

fn local_path<'b, L: Into<TreeLoc<'b>>>(
    datadir: &std::path::Path,
    tree: &impl TreeReadOperations,
    loc: L,
) -> Result<std::path::PathBuf, StorageError> {
    Ok(tree.backtrack(loc)?.within(datadir))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arena::blob::BlobExt;
    use crate::arena::db::ArenaDatabase;
    use crate::arena::dirty::DirtyReadOperations;
    use crate::arena::history::HistoryReadOperations;
    use crate::arena::notifier::Notification;
    use crate::arena::tree::{TreeExt, TreeLoc, TreeReadOperations};
    use crate::arena::types::{DirMetadata, HistoryTableEntry};
    use crate::arena::{index, update};
    use crate::utils::hash;
    use crate::{CacheStatus, FileMetadata};
    use crate::{PathId, StorageError};
    use assert_fs::TempDir;
    use assert_fs::fixture::ChildPath;
    use assert_fs::prelude::*;
    use nix::sys::stat::{Mode, SFlag};
    use realize_types::{Arena, Hash, Path, Peer, UnixTime};
    use std::collections::{HashMap, HashSet};
    use std::os::unix;
    use std::os::unix::fs::MetadataExt;
    use std::sync::Arc;
    use std::u64;

    const TEST_TIME: u64 = 1234567890;

    fn test_peer() -> Peer {
        Peer::from("test_peer")
    }

    fn test_arena() -> Arena {
        Arena::from("test_arena")
    }

    fn test_hash() -> Hash {
        Hash([1u8; 32])
    }

    fn test_time() -> UnixTime {
        UnixTime::from_secs(TEST_TIME)
    }

    fn later_time() -> UnixTime {
        UnixTime::from_secs(TEST_TIME + 1)
    }

    /// Helper function to create a test file with specific content.
    /// Returns (size, actual_mtime, hash) for use in index() calls.
    fn create_test_file(
        childpath: &ChildPath,
        contents: &str,
    ) -> anyhow::Result<(u64, UnixTime, Hash)> {
        childpath.write_str(contents)?;
        let size = contents.len() as u64;
        let hash = hash::digest(contents);
        let actual_mtime = UnixTime::mtime(&childpath.metadata()?);

        Ok((size, actual_mtime, hash))
    }

    struct Fixture {
        arena: Arena,
        db: Arc<ArenaDatabase>,
        datadir: ChildPath,
        _tempdir: TempDir,
    }
    impl Fixture {
        fn setup() -> anyhow::Result<Fixture> {
            Self::setup_with_arena(test_arena())
        }

        fn setup_with_arena(arena: Arena) -> anyhow::Result<Fixture> {
            let _ = env_logger::try_init();
            let tempdir = TempDir::new()?;
            let child = tempdir.child(format!("{arena}-cache.db"));
            if let Some(p) = child.parent() {
                std::fs::create_dir_all(p)?;
            }
            let blob_dir = tempdir.child(format!("{arena}/blobs"));
            blob_dir.create_dir_all()?;
            let datadir = tempdir.child(format!("{arena}/data"));
            datadir.create_dir_all()?;
            let db =
                ArenaDatabase::for_testing_single_arena(arena, blob_dir.path(), datadir.path())?;
            Ok(Self {
                arena,
                db,
                datadir,
                _tempdir: tempdir,
            })
        }

        fn dir_metadata<'b, L: Into<TreeLoc<'b>>>(
            &self,
            loc: L,
        ) -> Result<DirMetadata, StorageError> {
            let txn = self.db.begin_read()?;
            let tree = txn.read_tree()?;
            let cache = txn.read_cache()?;

            cache
                .metadata(&tree, loc)?
                .ok_or(StorageError::NotFound)?
                .expect_dir()
        }

        fn metadata<'b, L: Into<TreeLoc<'b>>>(
            &self,
            loc: L,
        ) -> Result<crate::Metadata, StorageError> {
            let txn = self.db.begin_read()?;
            let tree = txn.read_tree()?;
            let cache = txn.read_cache()?;

            cache.metadata(&tree, loc)?.ok_or(StorageError::NotFound)
        }

        fn file_metadata<'b, L: Into<TreeLoc<'b>>>(
            &self,
            loc: L,
        ) -> Result<FileMetadata, StorageError> {
            let txn = self.db.begin_read()?;
            let tree = txn.read_tree()?;
            let cache = txn.read_cache()?;

            cache.file_metadata(&tree, loc)
        }

        fn lookup<'b, L: Into<TreeLoc<'b>>>(
            &self,
            loc: L,
        ) -> Result<(PathId, crate::Metadata), StorageError> {
            let txn = self.db.begin_read()?;
            let tree = txn.read_tree()?;
            let cache = txn.read_cache()?;
            let pathid = tree.expect(loc)?;

            Ok((
                pathid,
                cache
                    .metadata(&tree, pathid)?
                    .ok_or(StorageError::NotFound)?,
            ))
        }

        fn readdir<'b, L: Into<TreeLoc<'b>>>(
            &self,
            loc: L,
        ) -> Result<Vec<(String, Option<PathId>, crate::Metadata)>, StorageError> {
            let txn = self.db.begin_read()?;
            let tree = txn.read_tree()?;
            let cache = txn.read_cache()?;

            cache.readdir(&tree, loc).collect()
        }

        fn add_to_cache<T>(&self, path: T, size: u64, mtime: UnixTime) -> anyhow::Result<()>
        where
            T: AsRef<Path>,
        {
            let path = path.as_ref();
            update::apply(
                &self.db,
                test_peer(),
                Notification::Add {
                    arena: self.arena,
                    index: 1,
                    path: path.clone(),
                    mtime: mtime.clone(),
                    size,
                    hash: test_hash(),
                },
            )?;

            Ok(())
        }

        fn remove_from_cache<T>(&self, path: T) -> anyhow::Result<()>
        where
            T: AsRef<Path>,
        {
            let path = path.as_ref();
            update::apply(
                &self.db,
                test_peer(),
                Notification::Remove {
                    arena: self.arena,
                    index: 1,
                    path: path.clone(),
                    old_hash: test_hash(),
                },
            )?;

            Ok(())
        }

        fn has_blob<'b, L: Into<TreeLoc<'b>>>(&self, loc: L) -> Result<bool, StorageError> {
            let txn = self.db.begin_read()?;
            let blobs = txn.read_blobs()?;
            let tree = txn.read_tree()?;

            Ok(blobs.get(&tree, loc)?.is_some())
        }

        fn clear_dirty(&self) -> Result<(), StorageError> {
            let txn = self.db.begin_write()?;
            clear_dirty(&mut txn.write_dirty()?)?;
            txn.commit()?;

            Ok(())
        }

        fn dirty_pathids(&self) -> Result<HashSet<PathId>, StorageError> {
            let txn = self.db.begin_read()?;
            let dirty = txn.read_dirty()?;

            dirty_pathids(&dirty)
        }

        fn dirty_paths(&self) -> Result<HashSet<Path>, StorageError> {
            let pathids = self.dirty_pathids()?;
            let txn = self.db.begin_read()?;
            let tree = txn.read_tree()?;

            Ok(pathids
                .into_iter()
                .filter_map(|i| tree.backtrack(i).ok())
                .collect())
        }

        fn list_alternatives<'b, L: Into<TreeLoc<'b>>>(
            &self,
            loc: L,
        ) -> Result<Vec<FileAlternative>, StorageError> {
            let txn = self.db.begin_read()?;
            let tree = txn.read_tree()?;
            let cache = txn.read_cache()?;

            cache.list_alternatives(&tree, loc)
        }

        fn add_file_from_peer(
            &self,
            peer: Peer,
            path: &Path,
            size: u64,
            mtime: UnixTime,
            hash: Hash,
        ) -> anyhow::Result<()> {
            update::apply(
                &self.db,
                peer,
                Notification::Add {
                    arena: self.arena,
                    index: 1,
                    path: path.clone(),
                    mtime,
                    size,
                    hash,
                },
            )?;
            Ok(())
        }
    }

    /// Return a value to pass to `collect_history_entries` to ignore
    /// all history entries before this point.
    fn history_start(
        history: &impl crate::arena::history::HistoryReadOperations,
    ) -> Result<u64, StorageError> {
        Ok(history.last_history_index()? + 1)
    }

    /// Collect history entries starting from `start`.
    ///
    /// To compute `history_start` call [history_start]
    /// just before making the change under test. This way,
    /// history entries that are part of the test setup are not
    /// included.
    fn collect_history_entries(
        history: &impl crate::arena::history::HistoryReadOperations,
        history_start: u64,
    ) -> Result<Vec<HistoryTableEntry>, StorageError> {
        history
            .history(history_start..)
            .map(|res| res.map(|(_, e)| e))
            .collect()
    }

    fn clear_dirty(dirty: &mut WritableOpenDirty) -> Result<(), StorageError> {
        dirty.delete_range(0, 99)?;

        Ok(())
    }

    fn dirty_pathids(
        dirty: &impl crate::arena::dirty::DirtyReadOperations,
    ) -> Result<HashSet<PathId>, StorageError> {
        let mut start = 0;
        let mut ret = HashSet::new();
        while let Some((pathid, counter)) = dirty.next_dirty(start)? {
            ret.insert(pathid);
            start = counter + 1;
        }
        Ok(ret)
    }

    fn dirty_paths(
        dirty: &impl DirtyReadOperations,
        tree: &impl TreeReadOperations,
    ) -> Result<HashSet<Path>, StorageError> {
        Ok(dirty_pathids(dirty)?
            .into_iter()
            .filter_map(|i| tree.backtrack(i).ok())
            .collect())
    }

    #[tokio::test]
    async fn empty_cache_readdir() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;

        assert!(fixture.readdir(fixture.db.tree().root())?.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn empty_cache_mtime() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;

        assert_ne!(
            UnixTime::ZERO,
            fixture.dir_metadata(fixture.db.tree().root())?.mtime
        );

        Ok(())
    }

    #[tokio::test]
    async fn add_creates_directories() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let file_path = Path::parse("a/b/c.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&file_path, 100, mtime)?;

        let (inode, metadata) = fixture.lookup((fixture.db.tree().root(), "a"))?;
        assert!(matches!(metadata, crate::Metadata::Dir(_)), "a");

        let (_, metadata) = fixture.lookup((inode, "b"))?;
        assert!(matches!(metadata, crate::Metadata::Dir(_)), "b");

        Ok(())
    }

    #[tokio::test]
    async fn add_and_remove_mirrored_in_tree() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let mtime = test_time();

        let a_path = Path::parse("a")?;
        let b_path = Path::parse("a/b")?;
        let c_path = Path::parse("a/b/c.txt")?;

        fixture.add_to_cache(&c_path, 100, mtime)?;

        let a: PathId;
        let b: PathId;
        let c: PathId;
        {
            let txn = fixture.db.begin_read()?;
            let tree = txn.read_tree()?;
            let cache = txn.read_cache()?;
            assert_eq!(fixture.db.tree().root(), tree.root());

            a = tree.resolve(&a_path)?.unwrap();
            b = tree.resolve(&b_path)?.unwrap();
            c = tree.resolve(&c_path)?.unwrap();
            log::debug!("a:{a}, b:{b}, c:{c}");

            assert!(tree.resolve(&a_path)?.is_some());
            assert!(tree.resolve(&b_path)?.is_some());
            assert!(tree.resolve(&c_path)?.is_some());

            assert!(cache.metadata(&tree, b)?.is_some());
            assert!(cache.metadata(&tree, a)?.is_some());
            assert!(cache.metadata(&tree, c)?.is_some());
        }

        fixture.remove_from_cache(&c_path)?;

        {
            let txn = fixture.db.begin_read()?;
            let tree = txn.read_tree()?;
            let cache = txn.read_cache()?;

            assert!(cache.metadata(&tree, &c_path)?.is_none());

            // The file is gone from tree, since this was the only
            // reference to it.
            assert_eq!(None, tree.resolve(&c_path)?);

            // The directories were cleaned up.
            assert_eq!(None, tree.resolve(&a_path)?);
            assert_eq!(None, tree.resolve(&b_path)?);
        }
        Ok(())
    }

    #[tokio::test]
    async fn add_updates_dir_mtime() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        let mtime = test_time();
        let path1 = Path::parse("a/b/1.txt")?;
        fixture.add_to_cache(&path1, 100, mtime)?;
        let dir = Path::parse("a/b")?;
        let dir_mtime = fixture.dir_metadata(&dir)?.mtime;

        let path2 = Path::parse("a/b/2.txt")?;
        fixture.add_to_cache(&path2, 100, mtime)?;

        assert!(fixture.dir_metadata(&dir)?.mtime > dir_mtime);
        Ok(())
    }

    #[tokio::test]
    async fn add_marks_dirty() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let file_path = Path::parse("a/b/c.txt")?;

        fixture.add_to_cache(&file_path, 100, test_time())?;
        assert_eq!(HashSet::from([file_path.clone()]), fixture.dirty_paths()?);

        Ok(())
    }

    #[tokio::test]
    async fn replace_existing_file() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        let peer = test_peer();
        let file_path = Path::parse("file.txt")?;

        update::apply(
            &fixture.db,
            peer,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 100,
                hash: test_hash(),
            },
        )?;
        update::apply(
            &fixture.db,
            peer,
            Notification::Replace {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: later_time(),
                size: 200,
                hash: Hash([2u8; 32]),
                old_hash: test_hash(),
            },
        )?;

        let metadata = fixture.file_metadata(&file_path)?;
        assert_eq!(metadata.size, 200);
        assert_eq!(metadata.mtime, later_time());

        Ok(())
    }

    #[tokio::test]
    async fn replace_marks_dirty() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        let peer = test_peer();
        let file_path = Path::parse("file.txt")?;

        update::apply(
            &fixture.db,
            peer,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 100,
                hash: test_hash(),
            },
        )?;
        fixture.clear_dirty()?;

        update::apply(
            &fixture.db,
            peer,
            Notification::Replace {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: later_time(),
                size: 200,
                hash: Hash([2u8; 32]),
                old_hash: test_hash(),
            },
        )?;
        assert_eq!(HashSet::from([file_path.clone()]), fixture.dirty_paths()?);

        Ok(())
    }

    #[tokio::test]
    async fn ignored_replace_does_not_mark_dirty() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        let peer = test_peer();
        let file_path = Path::parse("file.txt")?;

        update::apply(
            &fixture.db,
            peer,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
        )?;
        fixture.clear_dirty()?;

        // Replace is ignored because old_hash != current hash.
        update::apply(
            &fixture.db,
            peer,
            Notification::Replace {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: later_time(),
                size: 200,
                hash: Hash([3u8; 32]),
                old_hash: Hash([2u8; 32]),
            },
        )?;
        assert_eq!(HashSet::new(), fixture.dirty_paths()?);

        Ok(())
    }

    #[tokio::test]
    async fn replace_nothing() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        let peer = test_peer();
        let file_path = Path::parse("file.txt")?;

        update::apply(
            &fixture.db,
            peer,
            Notification::Replace {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 200,
                hash: Hash([2u8; 32]),
                old_hash: test_hash(),
            },
        )?;

        let metadata = fixture.file_metadata(&file_path).unwrap();
        assert_eq!(metadata.size, 200);
        assert_eq!(metadata.version, Version::Indexed(Hash([2u8; 32])));
        assert_eq!(metadata.mtime, test_time());

        let txn = fixture.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;

        let avail = cache
            .remote_availability(&tree, &file_path, &Hash([2u8; 32]))?
            .unwrap();
        assert_eq!(vec![peer], avail.peers);

        Ok(())
    }

    #[tokio::test]
    async fn ignore_duplicate_add() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let file_path = Path::parse("file.txt")?;

        fixture.add_to_cache(&file_path, 100, test_time())?;
        fixture.add_to_cache(&file_path, 200, test_time())?;

        let metadata = fixture.file_metadata(&file_path)?;
        assert_eq!(metadata.size, 100);

        Ok(())
    }

    #[tokio::test]
    async fn ignore_replace_with_wrong_hash() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let file_path = Path::parse("file.txt")?;
        let peer = test_peer();

        update::apply(
            &fixture.db,
            peer,
            Notification::Add {
                arena: test_arena(),
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
        )?;
        update::apply(
            &fixture.db,
            peer,
            Notification::Replace {
                arena: test_arena(),
                index: 0,
                path: file_path.clone(),
                mtime: later_time(),
                size: 200,
                hash: Hash([2u8; 32]),
                old_hash: Hash([0xffu8; 32]), // wrong
            },
        )?;

        let metadata = fixture.file_metadata(&file_path)?;
        assert_eq!(metadata.size, 100);

        Ok(())
    }

    #[tokio::test]
    async fn remove_removes_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let file_path = Path::parse("file.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&file_path, 100, mtime)?;
        let arena_root = fixture.db.tree().root();
        fixture.lookup((arena_root, "file.txt"))?;
        fixture.remove_from_cache(&file_path)?;
        assert!(matches!(
            fixture.lookup((arena_root, "file.txt")),
            Err(StorageError::NotFound)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn remove_marks_dirty() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let file_path = Path::parse("file.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&file_path, 100, mtime)?;
        let txn = fixture.db.begin_read()?;
        let tree = txn.read_tree()?;
        let pathid = tree.resolve(&file_path)?.unwrap();

        fixture.clear_dirty()?;
        fixture.remove_from_cache(&file_path)?;
        assert_eq!(HashSet::from([pathid]), fixture.dirty_pathids()?);

        Ok(())
    }

    #[tokio::test]
    async fn remove_updates_dir_mtime() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        let file_path = Path::parse("file.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&file_path, 100, mtime)?;
        let arena_root = fixture.db.tree().root();
        let dir_mtime = fixture.dir_metadata(arena_root)?.mtime;
        fixture.remove_from_cache(&file_path)?;

        assert!(fixture.dir_metadata(arena_root)?.mtime > dir_mtime);

        Ok(())
    }

    #[tokio::test]
    async fn remove_ignores_wrong_old_hash() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        let file_path = Path::parse("file.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&file_path, 100, mtime)?;
        update::apply(
            &fixture.db,
            test_peer(),
            Notification::Remove {
                arena,
                index: 1,
                path: file_path.clone(),
                old_hash: Hash([2u8; 32]), // != test_hash()
            },
        )?;

        // File should still exist because wrong hash was provided
        let metadata = fixture.file_metadata(&file_path)?;
        assert_eq!(metadata.size, 100);

        Ok(())
    }

    #[tokio::test]
    async fn remove_applied_to_tracked_version_immediately() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        let peer1 = Peer::from("peer1");
        let peer2 = Peer::from("peer2");
        let file_path = Path::parse("file.txt")?;

        update::apply(
            &fixture.db,
            peer1,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
        )?;
        update::apply(
            &fixture.db,
            peer2,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
        )?;

        assert!(fixture.file_metadata(&file_path).is_ok());

        update::apply(
            &fixture.db,
            peer1,
            Notification::Remove {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                old_hash: Hash([1u8; 32]),
            },
        )?;

        // The default version has been removed by this notification,
        // even though it's still available in peer2; this is
        // considered outdated.
        assert!(matches!(
            fixture.file_metadata(&file_path),
            Err(StorageError::NotFound)
        ));
        assert!(matches!(
            fixture.lookup((fixture.db.tree().root(), "file.txt")),
            Err(StorageError::NotFound)
        ));

        // PathId should still be resolvable, because some peers still have it.
        let txn = fixture.db.begin_read()?;
        let tree = txn.read_tree()?;
        assert!(tree.resolve(&file_path)?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn remove_applied_to_indexed_version() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        let peer1 = Peer::from("peer1");
        let file_path = Path::parse("file.txt")?;

        let childpath = fixture.datadir.child("file.txt");
        childpath.write_str("test")?;
        let hash = hash::digest("test");
        let mtime = UnixTime::mtime(&childpath.metadata()?);
        index::add_file(&fixture.db, &file_path, 4, mtime, hash.clone())?;

        update::apply(
            &fixture.db,
            peer1,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 4,
                hash: hash.clone(),
            },
        )?;

        update::apply(
            &fixture.db,
            peer1,
            Notification::Remove {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                old_hash: hash.clone(),
            },
        )?;

        // Both the local file and the file entry must have been removed
        assert!(matches!(
            fixture.file_metadata(&file_path),
            Err(StorageError::NotFound)
        ));
        assert!(!childpath.exists());

        Ok(())
    }

    #[tokio::test]
    async fn drop_not_applied_to_tracked_version() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        let peer1 = Peer::from("peer1");
        let peer2 = Peer::from("peer2");
        let file_path = Path::parse("file.txt")?;

        update::apply(
            &fixture.db,
            peer1,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
        )?;
        update::apply(
            &fixture.db,
            peer2,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
        )?;

        assert!(fixture.file_metadata(&file_path).is_ok());

        update::apply(
            &fixture.db,
            peer1,
            Notification::Drop {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                old_hash: Hash([1u8; 32]),
            },
        )?;

        // The default version has not been removed by this notification,
        // since this is a Drop, not a Remove.
        assert!(fixture.file_metadata(&file_path).is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn lookup_finds_entry() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        let file_path = Path::parse("a/file.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&file_path, 100, mtime)?;

        // Lookup directory
        let (inode, metadata) = fixture.lookup((fixture.db.tree().root(), "a"))?;
        assert!(matches!(metadata, crate::Metadata::Dir(_)));

        // Lookup file
        let (file_inode, metadata) = fixture.lookup((inode, "file.txt"))?;
        assert!(matches!(metadata, crate::Metadata::File(_)));

        let metadata = fixture.file_metadata(file_inode)?;
        assert_eq!(metadata.mtime, mtime);
        assert_eq!(metadata.size, 100);

        Ok(())
    }

    #[tokio::test]
    async fn lookup_returns_notfound_for_missing_entry() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;

        assert!(matches!(
            fixture.lookup((fixture.db.tree().root(), "nonexistent")),
            Err(StorageError::NotFound),
        ));

        Ok(())
    }

    #[tokio::test]
    async fn lookup_path_finds_entry() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        let path = Path::parse("a/b/c/file.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&path, 100, mtime)?;

        let metadata = fixture.file_metadata(&path)?;
        assert_eq!(metadata.mtime, mtime);
        assert_eq!(metadata.size, 100);

        Ok(())
    }

    #[tokio::test]
    async fn readdir_returns_entries_from_cache() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        let mtime = test_time();

        fixture.add_to_cache(&Path::parse("dir/file1.txt")?, 100, mtime)?;
        fixture.add_to_cache(&Path::parse("dir/file2.txt")?, 200, mtime)?;
        fixture.add_to_cache(&Path::parse("dir/subdir/file3.txt")?, 300, mtime)?;

        let arena_root = fixture.db.tree().root();
        let entries = fixture.readdir(arena_root)?;
        assert_eq!(entries.len(), 1);
        let (name, _, metadata) = &entries[0];
        assert_eq!(name, "dir");
        match metadata {
            crate::Metadata::Dir(_) => {}
            _ => panic!("Expected directory metadata"),
        }

        let (dir_inode, _) = fixture.lookup((arena_root, "dir"))?;
        let entries = fixture.readdir(dir_inode)?;
        assert_eq!(entries.len(), 3);

        let mut names: Vec<String> = entries.iter().map(|(name, _, _)| name.clone()).collect();
        names.sort();
        assert_eq!(names, vec!["file1.txt", "file2.txt", "subdir"]);

        // Verify metadata types
        for (name, _, metadata) in entries {
            match (name.as_str(), &metadata) {
                ("file1.txt" | "file2.txt", crate::Metadata::File(_)) => {}
                ("subdir", crate::Metadata::Dir(_)) => {}
                _ => panic!("Unexpected entry: {} with metadata {:?}", name, metadata),
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn readdir_detects_local_file_changes() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        let file_path = Path::parse("dir/file")?;
        let file_child = fixture.datadir.child("dir/file");
        file_child.write_str("test")?;
        index::add_file(
            &fixture.db,
            &file_path,
            4,
            UnixTime::mtime(&file_child.metadata().unwrap()),
            hash::digest("test"),
        )?;

        let txn = fixture.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        let file_pathid = tree.resolve(&file_path)?.unwrap();

        // indexed local file is returned
        let entries = cache
            .readdir(&tree, Path::parse("dir")?)
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(1, entries.len());
        let (name, pathid, m) = entries.into_iter().next().unwrap();
        assert_eq!("file", name);
        assert_eq!(Some(file_pathid), pathid);
        let m = m.expect_file().unwrap();
        assert_eq!(4, m.size);
        assert_eq!(UnixTime::mtime(&file_child.metadata().unwrap()), m.mtime);
        assert_eq!(Version::Indexed(hash::digest("test")), m.version);

        // file modification is detected and returned immediately as
        // modified file, even though in the cache, the entry still
        // contains an unmodified, indexed local file.
        file_child.write_str("modified")?;
        let entries = cache
            .readdir(&tree, Path::parse("dir")?)
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(1, entries.len());
        let (name, pathid, m) = entries.into_iter().next().unwrap();
        assert_eq!("file", name);
        assert_eq!(Some(file_pathid), pathid);
        let m = m.expect_file().unwrap();
        assert_eq!(8, m.size);
        assert_eq!(UnixTime::mtime(&file_child.metadata().unwrap()), m.mtime);
        assert_eq!(Version::Indexed(hash::digest("test")), m.version);

        // deletion is detected immediately and the file is not returned anymore
        std::fs::remove_file(file_child.path())?;

        let entries = cache
            .readdir(&tree, Path::parse("dir")?)
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(0, entries.len());

        // This works the same way if the entire local directory is deleted
        std::fs::remove_dir_all(fixture.datadir.child("dir").path())?;

        let entries = cache
            .readdir(&tree, Path::parse("dir")?)
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(0, entries.len());

        Ok(())
    }

    #[tokio::test]
    async fn readdir_lists_unindexed_local_dir() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        fixture.datadir.child("dir/file1").write_str("test")?;
        fixture.datadir.child("dir/file2").write_str("test")?;
        fixture.datadir.child("dir/file3").write_str("test")?;

        let txn = fixture.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        let entries = cache
            .readdir(&tree, Path::parse("dir")?)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_unordered::assert_eq_unordered!(
            vec!["file1", "file2", "file3"],
            entries
                .iter()
                .map(|(name, _, _)| name.as_str())
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    #[tokio::test]
    async fn readdir_lists_empty_unindexed_local_dir() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        fixture.datadir.child("dir").create_dir_all()?;

        let txn = fixture.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        let entries = cache
            .readdir(&tree, Path::parse("dir")?)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(entries.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn readdir_lists_new_special_files() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        fixture.datadir.child("dir/empty").write_str("")?;
        unix::fs::symlink(
            std::path::Path::new("doesnotexist"),
            fixture.datadir.child("dir/sym").path(),
        )?;
        nix::sys::stat::mknod(
            fixture.datadir.child("dir/nod").path(),
            SFlag::S_IFIFO,
            Mode::S_IRWXU,
            0,
        )?;

        let txn = fixture.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        let entries = cache
            .readdir(&tree, Path::parse("dir")?)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let entries = entries
            .into_iter()
            .map(|(name, _, m)| (name, m))
            .collect::<HashMap<String, crate::Metadata>>();
        let filenames = vec!["empty", "sym", "nod"];
        assert_unordered::assert_eq_unordered!(
            filenames.clone(),
            entries.keys().map(|s| s.as_str()).collect::<Vec<_>>()
        );
        for filename in filenames {
            let real_metadata =
                std::fs::symlink_metadata(fixture.datadir.child("dir").join(filename))?;
            let metadata = &entries[filename];
            match metadata {
                crate::Metadata::File(m) => {
                    // Mode specifies the type; it must not be filtered out.
                    assert_eq!(real_metadata.mode(), m.mode);
                    assert_eq!(real_metadata.len(), m.size);
                }
                _ => {
                    panic!("unexpected metadata type {metadata:?}")
                }
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn readdir_lists_preindexed_special_files() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        fixture.datadir.child("dir/empty").write_str("")?;
        unix::fs::symlink(
            std::path::Path::new("doesnotexist"),
            fixture.datadir.child("dir/sym").path(),
        )?;
        nix::sys::stat::mknod(
            fixture.datadir.child("dir/nod").path(),
            SFlag::S_IFIFO,
            Mode::S_IRWXU,
            0,
        )?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let empty_pathid =
            cache.preindex(&mut tree, &mut blobs, &mut dirty, Path::parse("dir/empty")?)?;
        let sym_pathid =
            cache.preindex(&mut tree, &mut blobs, &mut dirty, Path::parse("dir/sym")?)?;
        let nod_pathid =
            cache.preindex(&mut tree, &mut blobs, &mut dirty, Path::parse("dir/nod")?)?;

        let entries = cache
            .readdir(&tree, Path::parse("dir")?)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_unordered::assert_eq_unordered!(
            vec![
                ("empty", empty_pathid),
                ("sym", sym_pathid),
                ("nod", nod_pathid)
            ],
            entries
                .iter()
                .map(|(n, pathid, _)| (n.as_str(), *pathid.as_ref().unwrap()))
                .collect::<Vec<(&str, PathId)>>()
        );

        Ok(())
    }

    #[tokio::test]
    async fn readdir_dir_not_found() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;

        let txn = fixture.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        let ret = cache
            .readdir(&tree, Path::parse("doesnotexist")?)
            .collect::<Result<Vec<_>, _>>();
        assert!(matches!(ret, Err(StorageError::NotFound)));

        Ok(())
    }

    #[tokio::test]
    async fn readdir_detects_cached_file_modification() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        let file_path = Path::parse("dir/file")?;
        update::apply(
            &fixture.db,
            test_peer(),
            Notification::Add {
                arena,
                index: 1,
                path: file_path.clone(),
                mtime: test_time(),
                size: 6,
                hash: hash::digest("cached"),
            },
        )?;

        let file_child = fixture.datadir.child("dir/file");
        file_child.write_str("local")?;

        let txn = fixture.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        let file_pathid = tree.resolve(&file_path)?.unwrap();

        // indexed local file is returned, as a modification of the cached file
        let entries = cache
            .readdir(&tree, Path::parse("dir")?)
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(1, entries.len());
        let (name, pathid, m) = entries.into_iter().next().unwrap();
        assert_eq!("file", name);
        assert_eq!(Some(file_pathid), pathid);
        let m = m.expect_file().unwrap();
        assert_eq!(5, m.size);
        assert_eq!(UnixTime::mtime(&file_child.metadata().unwrap()), m.mtime);
        assert_eq!(Version::Modified(Some(hash::digest("cached"))), m.version);

        Ok(())
    }

    #[tokio::test]
    async fn get_file_metadata_tracks_hash_chain() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        let peer1 = Peer::from("peer1");
        let peer2 = Peer::from("peer2");
        let file_path = Path::parse("file.txt")?;

        update::apply(
            &fixture.db,
            peer1,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
        )?;
        update::apply(
            &fixture.db,
            peer2,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
        )?;
        update::apply(
            &fixture.db,
            peer2,
            Notification::Replace {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: later_time(),
                size: 200,
                hash: Hash([2u8; 32]),
                old_hash: Hash([1u8; 32]),
            },
        )?;

        let (pathid, _) = fixture.lookup((fixture.db.tree().root(), "file.txt"))?;
        let metadata = fixture.file_metadata(pathid)?;
        assert_eq!(metadata.size, 200);
        assert_eq!(metadata.mtime, later_time());

        Ok(())
    }

    #[tokio::test]
    async fn remote_availability_deals_with_conflicting_adds() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;

        let a = Peer::from("a");
        let b = Peer::from("b");
        let c = Peer::from("c");
        let arena = test_arena();
        let path = Path::parse("file.txt")?;

        update::apply(
            &fixture.db,
            a,
            Notification::Add {
                arena: arena,
                index: 0,
                path: path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
        )?;
        update::apply(
            &fixture.db,
            b,
            Notification::Add {
                arena: arena,
                index: 0,
                path: path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
        )?;
        update::apply(
            &fixture.db,
            c,
            Notification::Add {
                arena: arena,
                index: 0,
                path: path.clone(),
                mtime: later_time(),
                size: 200,
                hash: Hash([2u8; 32]),
            },
        )?;
        let txn = fixture.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;

        let avail = cache
            .remote_availability(&tree, &path, &Hash([1u8; 32]))?
            .unwrap();
        assert_eq!(arena, avail.arena);
        assert_eq!(path, avail.path);
        // Since they're just independent additions, the cache chooses
        // the first one.
        assert_eq!(Hash([1u8; 32]), avail.hash);
        assert_eq!(100, avail.size,);
        assert_unordered::assert_eq_unordered!(vec![a, b], avail.peers);

        assert_eq!(
            vec![c],
            cache
                .remote_availability(&tree, &path, &Hash([2u8; 32]))?
                .unwrap()
                .peers
        );

        assert_eq!(
            None,
            cache.remote_availability(&tree, &path, &Hash([3u8; 32]))?
        );

        Ok(())
    }

    #[tokio::test]
    async fn file_availablility_deals_with_different_versions() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;

        let a = Peer::from("a");
        let b = Peer::from("b");
        let c = Peer::from("c");
        let arena = test_arena();
        let path = Path::parse("file.txt")?;

        update::apply(
            &fixture.db,
            a,
            Notification::Add {
                arena: arena,
                index: 0,
                path: path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
        )?;
        update::apply(
            &fixture.db,
            b,
            Notification::Add {
                arena: arena,
                index: 0,
                path: path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
        )?;
        update::apply(
            &fixture.db,
            c,
            Notification::Add {
                arena: arena,
                index: 0,
                path: path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
        )?;
        update::apply(
            &fixture.db,
            b,
            Notification::Replace {
                arena: arena,
                index: 0,
                path: path.clone(),
                mtime: later_time(),
                size: 200,
                hash: Hash([2u8; 32]),
                old_hash: Hash([1u8; 32]),
            },
        )?;
        {
            let txn = fixture.db.begin_read()?;
            let cache = txn.read_cache()?;
            let tree = txn.read_tree()?;
            let entry = cache.file_entry(&tree, &path)?.unwrap();

            //  Replace with old_hash=1 means that hash=2 is the most
            //  recent one. This is the one chosen.
            assert_eq!(Version::Indexed(Hash([2u8; 32])), entry.version);
            assert_eq!(200, entry.size);
            assert_eq!(later_time(), entry.mtime);
            assert_eq!(
                vec![b],
                cache
                    .remote_availability(&tree, &path, &Hash([2u8; 32]))?
                    .unwrap()
                    .peers
            );
        }

        // We reconnect to c, which has yet another version. Following
        // the hash chain, Hash 3 is now the newest version.
        update::apply(
            &fixture.db,
            c,
            Notification::Replace {
                arena: arena,
                index: 0,
                path: path.clone(),
                mtime: later_time(),
                size: 300,
                hash: Hash([3u8; 32]),
                old_hash: Hash([1u8; 32]),
            },
        )?;
        update::apply(
            &fixture.db,
            c,
            Notification::Replace {
                arena: arena,
                index: 0,
                path: path.clone(),
                mtime: later_time(),
                size: 300,
                hash: Hash([3u8; 32]),
                old_hash: Hash([2u8; 32]),
            },
        )?;
        {
            let txn = fixture.db.begin_read()?;
            let cache = txn.read_cache()?;
            let tree = txn.read_tree()?;
            let entry = cache.file_entry(&txn.read_tree()?, &path)?.unwrap();

            //  Replace with old_hash=1 means that hash=2 is the most
            //  recent one. This is the one chosen.
            assert_eq!(Version::Indexed(Hash([3u8; 32])), entry.version);
            assert_eq!(
                vec![c],
                cache
                    .remote_availability(&tree, &path, &Hash([3u8; 32]))?
                    .unwrap()
                    .peers
            );
        }

        {
            let txn = fixture.db.begin_read()?;
            let cache = txn.read_cache()?;
            let tree = txn.read_tree()?;
            let entry = cache.file_entry(&txn.read_tree()?, &path)?.unwrap();

            assert_eq!(Version::Indexed(Hash([3u8; 32])), entry.version);
            assert_eq!(
                vec![c],
                cache
                    .remote_availability(&tree, &path, &Hash([3u8; 32]))?
                    .unwrap()
                    .peers
            );
        }

        // Later on, b joins the party
        update::apply(
            &fixture.db,
            b,
            Notification::Replace {
                arena: arena,
                index: 0,
                path: path.clone(),
                mtime: later_time(),
                size: 300,
                hash: Hash([3u8; 32]),
                old_hash: Hash([2u8; 32]),
            },
        )?;

        {
            let txn = fixture.db.begin_read()?;
            let cache = txn.read_cache()?;
            let tree = txn.read_tree()?;
            let entry = cache.file_entry(&txn.read_tree()?, &path)?.unwrap();

            assert_eq!(Version::Indexed(Hash([3u8; 32])), entry.version);
            assert_eq!(
                vec![b, c],
                cache
                    .remote_availability(&tree, &path, &Hash([3u8; 32]))?
                    .unwrap()
                    .peers
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn remote_availability_when_a_peer_goes_away() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;

        let a = Peer::from("a");
        let b = Peer::from("b");
        let c = Peer::from("c");
        let arena = test_arena();
        let path = Path::parse("file.txt")?;

        update::apply(
            &fixture.db,
            a,
            Notification::Add {
                arena: arena,
                index: 0,
                path: path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
        )?;
        update::apply(
            &fixture.db,
            b,
            Notification::Add {
                arena: arena,
                index: 0,
                path: path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
        )?;
        update::apply(
            &fixture.db,
            c,
            Notification::Add {
                arena: arena,
                index: 0,
                path: path.clone(),
                mtime: later_time(),
                size: 100,
                hash: Hash([2u8; 32]),
            },
        )?;
        update::apply(
            &fixture.db,
            a,
            Notification::Replace {
                arena: arena,
                index: 0,
                path: path.clone(),
                mtime: later_time(),
                size: 200,
                hash: Hash([3u8; 32]),
                old_hash: Hash([1u8; 32]),
            },
        )?;
        let pathid: PathId;
        {
            let txn = fixture.db.begin_read()?;
            let cache = txn.read_cache()?;
            let tree = txn.read_tree()?;
            pathid = tree
                .resolve((fixture.db.tree().root(), "file.txt"))?
                .unwrap();
            let entry = cache.file_at_pathid(pathid)?.unwrap();

            assert_eq!(Version::Indexed(Hash([3u8; 32])), entry.version);
            assert_eq!(
                vec![a],
                cache
                    .remote_availability(&tree, pathid, &Hash([3u8; 32]))?
                    .unwrap()
                    .peers
            );
        }

        update::apply(&fixture.db, a, Notification::CatchupStart(fixture.arena))?;
        update::apply(
            &fixture.db,
            a,
            Notification::CatchupComplete {
                arena: arena,
                index: 0,
            },
        )?;
        // We've lost the single peer that had the tracked version.
        // This is handled as if A had reported that file as deleted.
        assert!(matches!(
            fixture.file_metadata(pathid),
            Err(StorageError::NotFound)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn mark_and_delete_peer_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let arena = test_arena();
        let peer1 = Peer::from("1");
        let peer2 = Peer::from("2");
        let peer3 = Peer::from("3");
        let file1 = Path::parse("file1")?;
        let file2 = Path::parse("afile2")?;
        let file3 = Path::parse("file3")?;
        let file4 = Path::parse("file4")?;

        let mtime = test_time();

        update::apply(
            &fixture.db,
            peer1,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file1.clone(),
                mtime: mtime.clone(),
                size: 10,
                hash: test_hash(),
            },
        )?;

        update::apply(
            &fixture.db,
            peer1,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file2.clone(),
                mtime: mtime.clone(),
                size: 10,
                hash: test_hash(),
            },
        )?;
        update::apply(
            &fixture.db,
            peer2,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file2.clone(),
                mtime: mtime.clone(),
                size: 10,
                hash: test_hash(),
            },
        )?;

        update::apply(
            &fixture.db,
            peer1,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file1.clone(),
                mtime: mtime.clone(),
                size: 10,
                hash: test_hash(),
            },
        )?;
        update::apply(
            &fixture.db,
            peer2,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file2.clone(),
                mtime: mtime.clone(),
                size: 10,
                hash: test_hash(),
            },
        )?;
        update::apply(
            &fixture.db,
            peer3,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file3.clone(),
                mtime: mtime.clone(),
                size: 10,
                hash: test_hash(),
            },
        )?;

        update::apply(
            &fixture.db,
            peer1,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file4.clone(),
                mtime: mtime.clone(),
                size: 10,
                hash: test_hash(),
            },
        )?;

        // Simulate a catchup that only reports file2 and file4.
        update::apply(
            &fixture.db,
            peer1,
            Notification::CatchupStart(fixture.arena),
        )?;
        update::apply(
            &fixture.db,
            peer1,
            Notification::Catchup {
                arena: arena,
                path: file2.clone(),
                size: 10,
                mtime: mtime.clone(),
                hash: test_hash(),
            },
        )?;
        update::apply(
            &fixture.db,
            peer1,
            Notification::Catchup {
                arena: arena,
                path: file4.clone(),
                size: 10,
                mtime: mtime.clone(),
                hash: test_hash(),
            },
        )?;
        update::apply(
            &fixture.db,
            peer1,
            Notification::CatchupComplete {
                arena: arena,
                index: 0,
            },
        )?;
        let txn = fixture.db.begin_read()?;
        let cache = txn.read_cache()?;
        let tree = txn.read_tree()?;

        // File1 should have been deleted, since it was only on peer1,
        assert!(cache.file_entry(&tree, &file1)?.is_none());

        assert_eq!(
            vec![peer1, peer2],
            cache
                .remote_availability(&tree, &file2, &test_hash())?
                .unwrap()
                .peers
        );

        assert_eq!(
            vec![peer3],
            cache
                .remote_availability(&tree, &file3, &test_hash())?
                .unwrap()
                .peers
        );

        assert_eq!(
            vec![peer1],
            cache
                .remote_availability(&tree, &file4, &test_hash())?
                .unwrap()
                .peers
        );

        Ok(())
    }

    #[tokio::test]
    async fn mark_and_delete_peer_dirs() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let arena = test_arena();
        let peer = Peer::from("peer");
        let dir1 = Path::parse("dir1")?;
        let dir2 = dir1.join("dir2")?;
        let file = dir2.join("file")?;

        let mtime = test_time();

        update::apply(
            &fixture.db,
            peer,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file.clone(),
                mtime: mtime.clone(),
                size: 10,
                hash: test_hash(),
            },
        )?;

        // Catchup reports nothing, so deletes everything
        update::apply(&fixture.db, peer, Notification::CatchupStart(fixture.arena))?;
        update::apply(
            &fixture.db,
            peer,
            Notification::CatchupComplete {
                arena: arena,
                index: 0,
            },
        )?;

        let txn = fixture.db.begin_read()?;
        let cache = txn.read_cache()?;
        let tree = txn.read_tree()?;

        // the file and its containing directories should have been deleted
        assert_eq!(None, cache.metadata(&tree, &file)?);
        assert_eq!(None, cache.metadata(&tree, &dir2)?);
        assert_eq!(None, cache.metadata(&tree, &dir1)?);

        Ok(())
    }

    #[tokio::test]
    async fn blob_deleted_on_file_overwrite() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;

        // Create a file
        fixture.add_to_cache(&file_path, 100, test_time())?;

        let txn = fixture.db.begin_write()?;
        {
            let mut cache = txn.write_cache()?;
            cache.create_blob(
                &mut txn.write_tree()?,
                &mut txn.write_blobs()?,
                &txn.read_marks()?,
                &file_path,
            )?;
        };
        txn.commit()?;

        update::apply(
            &fixture.db,
            test_peer(),
            Notification::Replace {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: later_time(),
                size: 200,
                hash: Hash([2u8; 32]),
                old_hash: test_hash(),
            },
        )?;

        // The version has changed, so the blob must have been deleted.
        assert!(!fixture.has_blob(&file_path)?);
        Ok(())
    }

    #[tokio::test]
    async fn file_realm() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let testfile = Path::parse("testfile")?;
        let childpath = fixture.datadir.child("testfile");

        fixture.add_to_cache(&testfile, 5, test_time())?;

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;

        assert_eq!(
            FileRealm::Remote(CacheStatus::Missing),
            cache.file_realm(&tree, &blobs, &testfile)?,
        );

        let contents = "testfile content for file_realm";
        let (size, mtime, hash) = create_test_file(&childpath, contents)?;

        let mut history = txn.write_history()?;
        let mut dirty = txn.write_dirty()?;
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &testfile,
            size,
            mtime,
            hash,
        )?;

        assert_eq!(
            FileRealm::Local(childpath.to_path_buf()),
            cache.file_realm(&tree, &blobs, &testfile)?,
        );

        Ok(())
    }

    #[tokio::test]
    async fn all_indexed_empty() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_read()?;
        let cache = txn.read_cache()?;

        // Collect all indexed files - should be empty
        let indexed_files = cache.all_indexed().collect::<Result<Vec<_>, _>>()?;
        assert!(indexed_files.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn all_indexed_with_entries() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        // Create test files with actual content
        let path1 = Path::parse("test1.txt")?;
        let path2 = Path::parse("dir/test2.txt")?;
        let path3 = Path::parse("dir/subdir/test3.txt")?;

        let childpath1 = fixture.datadir.child("test1.txt");
        let childpath2 = fixture.datadir.child("dir/test2.txt");
        let childpath3 = fixture.datadir.child("dir/subdir/test3.txt");

        let contents1 = "content for test1.txt file";
        let contents2 = "different content for test2.txt file";
        let contents3 = "unique content for test3.txt file in subdir";

        let (size1, mtime1, hash1) = create_test_file(&childpath1, contents1)?;
        let (size2, mtime2, hash2) = create_test_file(&childpath2, contents2)?;
        let (size3, mtime3, hash3) = create_test_file(&childpath3, contents3)?;

        let indexed_file1 = IndexedFile {
            hash: hash1.clone(),
            mtime: mtime1,
            size: size1,
        };

        let indexed_file2 = IndexedFile {
            hash: hash2.clone(),
            mtime: mtime2,
            size: size2,
        };

        let indexed_file3 = IndexedFile {
            hash: hash3.clone(),
            mtime: mtime3,
            size: size3,
        };

        // Add some regular cache entries (should not appear in all_indexed)
        fixture.add_to_cache(&Path::parse("regular.txt")?, 500, test_time())?;
        fixture.add_to_cache(&path1, 500, test_time())?;

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        for (path, indexed) in [
            (&path1, &indexed_file1),
            (&path2, &indexed_file2),
            (&path3, &indexed_file3),
        ] {
            cache.index(
                &mut tree,
                &mut blobs,
                &mut history,
                &mut dirty,
                path,
                indexed.size,
                indexed.mtime,
                indexed.hash.clone(),
            )?;
        }

        let got = cache
            .all_indexed()
            .collect::<Result<HashMap<PathId, IndexedFile>, _>>()?;
        let expect = HashMap::from([
            (tree.resolve(path1)?.unwrap(), indexed_file1),
            (tree.resolve(path2)?.unwrap(), indexed_file2),
            (tree.resolve(path3)?.unwrap(), indexed_file3),
        ]);

        assert_eq!(expect, got);

        Ok(())
    }

    #[tokio::test]
    async fn blob_deleted_on_file_removal() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let file_path = Path::parse("file.txt")?;

        // Create a file and make sure it has a blob
        fixture.add_to_cache(&file_path, 100, test_time())?;
        let txn = fixture.db.begin_write()?;
        {
            let mut cache = txn.write_cache()?;
            cache.create_blob(
                &mut txn.write_tree()?,
                &mut txn.write_blobs()?,
                &txn.read_marks()?,
                &file_path,
            )?;
        };
        txn.commit()?;

        fixture.remove_from_cache(&file_path)?;

        // the blob must be gone
        assert!(!fixture.has_blob(&file_path)?);

        Ok(())
    }

    #[tokio::test]
    async fn blob_deleted_on_catchup_removal() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;

        // Create a file and make sure it has a blob
        fixture.add_to_cache(&file_path, 100, test_time())?;
        let txn = fixture.db.begin_write()?;
        {
            let mut cache = txn.write_cache()?;
            cache.create_blob(
                &mut txn.write_tree()?,
                &mut txn.write_blobs()?,
                &txn.read_marks()?,
                &file_path,
            )?;
        };
        txn.commit()?;

        // Do a catchup that doesn't include this file (simulating file removal)
        update::apply(
            &fixture.db,
            test_peer(),
            Notification::CatchupStart(fixture.arena),
        )?;
        // Note: No Catchup notification for the file, so it will be deleted
        update::apply(
            &fixture.db,
            test_peer(),
            Notification::CatchupComplete { arena, index: 0 },
        )?;

        // The blob must be gone
        assert!(!fixture.has_blob(&file_path)?);

        Ok(())
    }

    #[tokio::test]
    async fn unlink_cached() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let file_path = Path::parse("file.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&file_path, 100, mtime)?;
        fixture.clear_dirty()?;

        let arena_root = fixture.db.tree().root();
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;
        let mut blobs = txn.write_blobs()?;

        let pathid = tree.resolve((arena_root, "file.txt"))?.unwrap();
        assert!(cache.file_metadata(&tree, pathid).is_ok());

        let dir_mtime_before = cache.dir_mtime(&tree, arena_root)?;

        cache.unlink(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            (arena_root, "file.txt"),
        )?;

        assert!(cache.metadata(&tree, (arena_root, "file.txt"))?.is_none(),);
        assert!(cache.metadata(&tree, pathid)?.is_none(),);

        assert_eq!(HashSet::from([pathid]), dirty_pathids(&dirty)?);

        assert!(cache.dir_mtime(&tree, arena_root)? > dir_mtime_before);

        let (_, history_entry) = history.history(0..).last().unwrap().unwrap();
        assert_eq!(
            HistoryTableEntry::Remove(file_path, test_hash()),
            history_entry
        );

        Ok(())
    }

    #[tokio::test]
    async fn unlink_indexed() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let file_path = Path::parse("file.txt")?;
        let childpath = fixture.datadir.child("file.txt");
        childpath.write_str("test")?;
        let mtime = UnixTime::mtime(&childpath.metadata()?);
        let hash = hash::digest("test");

        fixture.add_to_cache(&file_path, 100, mtime)?;
        index::add_file(&fixture.db, &file_path, 4, mtime, hash.clone())?; // actual size of "test"

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;
        let mut blobs = txn.write_blobs()?;

        cache.unlink(&mut tree, &mut blobs, &mut history, &mut dirty, &file_path)?;

        assert!(cache.metadata(&tree, &file_path)?.is_none());
        assert!(!cache.has_local_file(&tree, &file_path)?);
        assert!(!childpath.exists());

        let (_, history_entry) = history.history(0..).last().unwrap().unwrap();
        assert_eq!(HistoryTableEntry::Remove(file_path, hash), history_entry);

        Ok(())
    }

    #[tokio::test]
    async fn branch_remote() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let source_path = Path::parse("mydir/source")?;
        let dest_path = Path::parse("mydir/dest")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        let peer1 = Peer::from("peer1");
        let peer2 = Peer::from("peer2");
        let hash = hash::digest("foobar");
        cache.notify_added(
            &mut tree,
            &mut blobs,
            &mut dirty,
            peer1,
            source_path.clone(),
            test_time(),
            6,
            hash.clone(),
        )?;
        clear_dirty(&mut dirty)?;

        cache
            .branch(
                &mut tree,
                &mut blobs,
                &mut history,
                &mut dirty,
                &source_path,
                &dest_path,
            )
            .unwrap();

        assert_eq!(
            HashSet::from([dest_path.clone()]),
            dirty_paths(&dirty, &tree)?
        );

        let (_, history_entry) = history.history(0..).last().unwrap().unwrap();
        assert_eq!(
            HistoryTableEntry::Branch(source_path.clone(), dest_path.clone(), hash.clone()),
            history_entry
        );

        // Dest file exists and can be downloaded from peer1, using source path
        let m = cache.file_metadata(&tree, &dest_path).unwrap();
        assert_eq!(6, m.size);
        assert_eq!(Version::Indexed(hash.clone()), m.version);

        let avail = cache
            .remote_availability(&tree, &dest_path, &hash)
            .unwrap()
            .unwrap();
        assert_eq!(source_path, avail.path);
        assert_eq!(vec![peer1], avail.peers);

        cache.notify_added(
            &mut tree,
            &mut blobs,
            &mut dirty,
            peer2,
            dest_path.clone(),
            test_time(),
            6,
            hash.clone(),
        )?;

        // Dest file can now downloaded normally from peer2, the normal way
        let avail = cache
            .remote_availability(&tree, &dest_path, &hash)
            .unwrap()
            .unwrap();
        assert_eq!(dest_path, avail.path);
        assert_eq!(vec![peer2], avail.peers);

        Ok(())
    }

    #[tokio::test]
    async fn branch_local() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let source_path = Path::parse("source")?;
        let dest_path = Path::parse("dest")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        let datadir = fixture.datadir.path();
        let source_realpath = source_path.within(&datadir);
        std::fs::write(&source_realpath, "test")?;
        let mtime = UnixTime::mtime(&source_realpath.metadata()?);
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &source_path,
            4,
            mtime,
            hash::digest("test"),
        )?;

        clear_dirty(&mut dirty)?;
        cache
            .branch(
                &mut tree,
                &mut blobs,
                &mut history,
                &mut dirty,
                &source_path,
                &dest_path,
            )
            .unwrap();

        let dest_realpath = dest_path.within(&datadir);
        assert!(dest_realpath.exists());
        assert!(source_realpath.exists());
        assert_eq!(
            source_realpath.metadata()?.ino(),
            dest_realpath.metadata()?.ino()
        );

        assert_eq!(
            HashSet::from([dest_path.clone()]),
            dirty_paths(&dirty, &tree)?
        );

        let (_, last_history_entry) = history.history(0..).last().unwrap().unwrap();
        assert_eq!(
            HistoryTableEntry::Add(dest_path.clone()),
            last_history_entry,
        );
        let metadata = cache.file_metadata(&tree, &dest_path).unwrap();
        assert_eq!(4, metadata.size);
        assert_eq!(mtime, metadata.mtime);
        assert_eq!(Version::Indexed(hash::digest("test")), metadata.version);
        assert!(matches!(
            cache.file_realm(&tree, &blobs, &dest_path).unwrap(),
            FileRealm::Local(_)
        ),);

        Ok(())
    }

    #[tokio::test]
    async fn branch_overwrite_local() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let source_path = Path::parse("source")?;
        let dest_path = Path::parse("dest")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        let datadir = fixture.datadir.path();
        let source_realpath = source_path.within(&datadir);
        std::fs::write(&source_realpath, "test")?;
        let source_mtime = UnixTime::mtime(&source_realpath.metadata()?);
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &source_path,
            4,
            source_mtime,
            hash::digest("test"),
        )?;

        let dest_realpath = dest_path.within(&datadir);
        std::fs::write(&dest_realpath, "overwrite")?;
        let dest_mtime = UnixTime::mtime(&dest_realpath.metadata()?);
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &dest_path,
            9,
            dest_mtime,
            hash::digest("overwrite"),
        )?;

        assert_eq!(
            Some(std::io::ErrorKind::AlreadyExists),
            cache
                .branch(
                    &mut tree,
                    &mut blobs,
                    &mut history,
                    &mut dirty,
                    &source_path,
                    &dest_path,
                )
                .err()
                .map(|e| e.io_kind())
        );

        Ok(())
    }

    #[tokio::test]
    async fn branch_overwrite_cached() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let source_path = Path::parse("source")?;
        let dest_path = Path::parse("dest")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        let datadir = fixture.datadir.path();
        let source_realpath = source_path.within(&datadir);
        std::fs::write(&source_realpath, "test")?;
        let source_mtime = UnixTime::mtime(&source_realpath.metadata()?);
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &source_path,
            4,
            source_mtime,
            hash::digest("test"),
        )?;

        cache.notify_added(
            &mut tree,
            &mut blobs,
            &mut dirty,
            Peer::from("peer"),
            dest_path.clone(),
            test_time(),
            9,
            hash::digest("overwrite"),
        )?;

        assert_eq!(
            Some(std::io::ErrorKind::AlreadyExists),
            cache
                .branch(
                    &mut tree,
                    &mut blobs,
                    &mut history,
                    &mut dirty,
                    &source_path,
                    &dest_path,
                )
                .err()
                .map(|e| e.io_kind())
        );

        Ok(())
    }

    #[tokio::test]
    async fn mkdir_creates_directory() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let dir_path = Path::parse("newdir")?;
        let dir_realpath = dir_path.within(fixture.datadir.path());

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;

        let (pathid, metadata) = cache.mkdir(&mut tree, &dir_path)?;

        assert_eq!(Some(pathid), tree.resolve(dir_path)?);

        assert!(dir_realpath.exists());

        // mtime is from the real directory
        assert_eq!(metadata.mtime, UnixTime::mtime(&dir_realpath.metadata()?));
        assert_eq!(
            Some(crate::Metadata::Dir(metadata)),
            cache.metadata(&tree, pathid)?
        );

        Ok(())
    }

    #[tokio::test]
    async fn mkdir_marks_remote_directory_as_local() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let dir_path = Path::parse("newdir")?;
        let dir_realpath = dir_path.within(fixture.datadir.path());

        fixture.add_to_cache(&dir_path.join("file")?, 4, test_time())?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;

        // mkdir succeeds and creates a local directory
        let (pathid, metadata) = cache.mkdir(&mut tree, &dir_path)?;

        assert!(dir_realpath.exists());
        assert_eq!(
            Some(crate::Metadata::Dir(metadata)),
            cache.metadata(&tree, pathid)?
        );
        Ok(())
    }

    #[tokio::test]
    async fn mkdir_does_not_create_nested_directories() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;

        // The following fails, because a/b doesn't exist
        assert!(cache.mkdir(&mut tree, Path::parse("a/b/c")?).is_err());

        let (a_pathid, _) = cache.mkdir(&mut tree, Path::parse("a")?)?;
        let (b_pathid, _) = cache.mkdir(&mut tree, Path::parse("a/b")?)?;
        let (c_pathid, _) = cache.mkdir(&mut tree, Path::parse("a/b/c")?)?;

        assert!(cache.dir_mtime(&tree, a_pathid).is_ok());
        assert!(cache.dir_mtime(&tree, b_pathid).is_ok());
        assert!(cache.dir_mtime(&tree, c_pathid).is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn rmdir_removes_empty_directory() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let dir_path = Path::parse("empty_dir")?;
        let dir_realpath = dir_path.within(fixture.datadir.path());

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;

        // Create directory
        let (pathid, _) = cache.mkdir(&mut tree, &dir_path)?;
        assert_eq!(tree.resolve(&dir_path)?, Some(pathid));

        // Remove directory
        cache.rmdir(&mut tree, &dir_path)?;

        assert_eq!(None, cache.metadata(&tree, &dir_path)?);
        assert!(!dir_realpath.exists());

        Ok(())
    }

    #[tokio::test]
    async fn rmdir_fails_if_directory_contains_remote_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let dir_path = Path::parse("non_empty_dir")?;
        let dir_realpath = dir_path.within(fixture.datadir.path());
        let file_path = Path::parse("non_empty_dir/file.txt")?;

        fixture.add_to_cache(file_path, 4, test_time())?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;

        // Create local directory
        let (_, _) = cache.mkdir(&mut tree, &dir_path)?;

        // Try to remove non-empty directory
        let result = cache.rmdir(&mut tree, &dir_path);
        assert!(matches!(result, Err(StorageError::DirectoryNotEmpty)));

        // Verify directory still exists in the database and fs
        assert!(matches!(
            cache.metadata(&tree, &dir_path)?,
            Some(crate::Metadata::Dir(_))
        ));
        assert!(dir_realpath.exists());

        Ok(())
    }

    #[tokio::test]
    async fn rmdir_fails_if_directory_contains_local_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let dir_path = Path::parse("non_empty_dir")?;
        let dir_realpath = dir_path.within(fixture.datadir.path());
        let file_path = Path::parse("non_empty_dir/file.txt")?;
        let file_realpath = file_path.within(fixture.datadir.path());

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;

        // Create local directory and local file
        cache.mkdir(&mut tree, &dir_path)?;
        std::fs::write(file_realpath, "test")?;

        let result = cache.rmdir(&mut tree, &dir_path);
        assert!(matches!(result, Err(StorageError::DirectoryNotEmpty)));

        // directory still exists in the database and fs
        assert!(matches!(
            cache.metadata(&tree, &dir_path)?,
            Some(crate::Metadata::Dir(_))
        ));
        assert!(dir_realpath.exists());

        Ok(())
    }

    #[tokio::test]
    async fn rmdir_fails_if_not_a_directory() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let file_path = Path::parse("file.txt")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut dirty = txn.write_dirty()?;
        let mut blobs = txn.write_blobs()?;

        // Add a file
        let peer = Peer::from("test_peer");
        cache.notify_added(
            &mut tree,
            &mut blobs,
            &mut dirty,
            peer,
            file_path.clone(),
            test_time(),
            100,
            test_hash(),
        )?;

        let file_pathid = tree.expect(&file_path)?;

        // Try to remove file as directory
        let result = cache.rmdir(&mut tree, &file_path);
        assert!(matches!(result, Err(StorageError::NotADirectory)));

        // Verify file still exists
        assert_eq!(Some(file_pathid), tree.resolve(&file_path)?);

        Ok(())
    }

    #[tokio::test]
    async fn rmdir_fails_if_directory_not_found() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let nonexistent_path = Path::parse("nonexistent_dir")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;

        // Try to remove non-existent directory
        let result = cache.rmdir(&mut tree, &nonexistent_path);
        assert!(matches!(result, Err(StorageError::NotFound)));

        Ok(())
    }

    #[tokio::test]
    async fn rmdir_updates_parent_mtime() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let parent_path = Path::parse("parent")?;
        let child_path = Path::parse("parent/child")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;

        // Create parent and child directories
        let (parent_pathid, _) = cache.mkdir(&mut tree, &parent_path)?;
        let (child_pathid, _) = cache.mkdir(&mut tree, &child_path)?;

        assert_eq!(Some(parent_pathid), tree.resolve(&parent_path)?);
        assert_eq!(Some(child_pathid), tree.resolve(&child_path)?);

        // Get parent mtime before removal
        let parent_mtime_before = cache.dir_mtime(&tree, parent_pathid)?;

        // Remove child directory
        cache.rmdir(&mut tree, &child_path)?;

        // Verify parent mtime was updated (should be >= since operations can be fast)
        let parent_mtime_after = cache.dir_mtime(&tree, parent_pathid)?;
        assert!(parent_mtime_after >= parent_mtime_before);

        // Verify child is gone
        assert_eq!(None, tree.resolve(&child_path)?);

        Ok(())
    }

    #[tokio::test]
    async fn dir_or_file_metadata() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let file_path = Path::parse("test_file")?;
        let dir_path = Path::parse("test_dir")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;

        cache.notify_added(
            &mut tree,
            &mut blobs,
            &mut dirty,
            test_peer(),
            file_path.clone(),
            test_time(),
            1024,
            test_hash(),
        )?;
        let file_pathid = tree.expect(&file_path)?;

        // Create a directory
        let (dir_pathid, _) = cache.mkdir(&mut tree, &dir_path)?;

        // Test file metadata
        let file_metadata = cache.metadata(&tree, file_pathid)?;
        match file_metadata {
            Some(crate::Metadata::File(meta)) => {
                assert_eq!(meta.size, 1024);
                assert_eq!(meta.mtime, test_time());
                assert_eq!(meta.version, Version::Indexed(test_hash()));
            }
            Some(crate::Metadata::Dir(_)) => {
                panic!("Expected file metadata, got directory metadata");
            }
            None => {
                panic!("Expected file metadata, got None");
            }
        }

        // Test directory metadata
        let dir_metadata = cache.metadata(&tree, dir_pathid)?;
        match dir_metadata {
            Some(crate::Metadata::Dir(dirmeta)) => {
                let m = dir_path.within(cache.datadir()).symlink_metadata()?;
                assert_eq!(m.mode(), dirmeta.mode);
                assert_eq!(UnixTime::mtime(&m), dirmeta.mtime);
            }
            Some(crate::Metadata::File(_)) => {
                panic!("Expected directory metadata, got file metadata");
            }
            None => {
                panic!("Expected directory metadata, got None");
            }
        }

        // Test error cases
        let nonexistent_pathid = PathId(99999);
        let result = cache.metadata(&tree, nonexistent_pathid);
        assert!(matches!(result, Ok(None)), "result.ok:{:?}", result.ok());

        Ok(())
    }

    #[tokio::test]
    async fn dir_or_file_metadata_acache() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let file_path = Path::parse("test_file")?;
        let dir_path = Path::parse("test_dir")?;

        let txn = fixture.db.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let mut cache = txn.write_cache()?;
            let mut blobs = txn.write_blobs()?;
            let mut dirty = txn.write_dirty()?;

            cache.notify_added(
                &mut tree,
                &mut blobs,
                &mut dirty,
                test_peer(),
                file_path.clone(),
                test_time(),
                1024,
                test_hash(),
            )?;
            cache.mkdir(&mut tree, &dir_path)?;
        }
        txn.commit()?;

        // Test file metadata through ArenaCache
        match fixture.metadata(&file_path)? {
            crate::Metadata::File(meta) => {
                assert_eq!(meta.size, 1024);
                assert_eq!(meta.mtime, test_time());
                assert_eq!(meta.version, Version::Indexed(test_hash()));
            }
            crate::Metadata::Dir(_) => {
                panic!("Expected file metadata, got directory metadata");
            }
        }

        // Test directory metadata through ArenaCache
        match fixture.metadata(&dir_path)? {
            crate::Metadata::Dir(dirmeta) => {
                let m = dir_path
                    .within(fixture.db.cache().datadir())
                    .symlink_metadata()?;
                assert_eq!(m.mode(), dirmeta.mode);
                assert_eq!(UnixTime::mtime(&m), dirmeta.mtime);
            }
            crate::Metadata::File(_) => {
                panic!("Expected directory metadata, got file metadata");
            }
        }

        // Test error cases
        let nonexistent_pathid = PathId(99999);
        let result = fixture.metadata(nonexistent_pathid);
        assert!(matches!(result, Err(StorageError::NotFound)));

        Ok(())
    }

    #[tokio::test]
    async fn rename_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let source_path = Path::parse("source")?;
        let dest_path = Path::parse("dest")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut dirty = txn.write_dirty()?;
        let mut blobs = txn.write_blobs()?;
        let mut history = txn.write_history()?;

        let hash = hash::digest("foobar");
        cache.notify_added(
            &mut tree,
            &mut blobs,
            &mut dirty,
            Peer::from("peer"),
            source_path.clone(),
            test_time(),
            6,
            hash.clone(),
        )?;

        let source_pathid = tree.expect(&source_path)?;
        clear_dirty(&mut dirty)?;
        cache.rename(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &source_path,
            &dest_path,
            true,
        )?;

        // dest is gone, source replaces it and points to it
        assert!(cache.file_entry(&tree, &source_path)?.is_none());
        let dest_entry = cache.file_entry_or_err(&tree, &dest_path).unwrap();
        assert_eq!(
            tree.resolve(&source_path)?,
            match dest_entry.kind {
                FileEntryKind::Branched(pathid) => Some(pathid),
                _ => None,
            }
        );
        assert_eq!(Version::Indexed(hash.clone()), dest_entry.version);
        assert_eq!(6, dest_entry.size);
        assert_eq!(test_time(), dest_entry.mtime);

        // both dest and source are dirty
        let dest_pathid = tree.expect(&dest_path)?;
        assert_eq!(
            HashSet::from([source_pathid, dest_pathid]),
            dirty_pathids(&dirty)?
        );

        // inodes are swapped
        assert_eq!(
            Inode(source_pathid.as_u64()),
            cache.map_to_inode(dest_pathid)?
        );
        assert_eq!(
            Inode(dest_pathid.as_u64()),
            cache.map_to_inode(source_pathid)?
        );
        assert_eq!(
            source_pathid,
            cache.map_to_pathid(Inode(dest_pathid.as_u64()))?
        );
        assert_eq!(
            dest_pathid,
            cache.map_to_pathid(Inode(source_pathid.as_u64()))?
        );

        // history entry was added
        let (_, history_entry) = history.history(0..).last().unwrap().unwrap();
        assert_eq!(
            HistoryTableEntry::Rename(source_path, dest_path, hash),
            history_entry
        );

        Ok(())
    }

    #[tokio::test]
    async fn rename_file_replace() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let source_path = Path::parse("source")?;
        let dest_path = Path::parse("dest")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut dirty = txn.write_dirty()?;
        let mut blobs = txn.write_blobs()?;
        let mut history = txn.write_history()?;

        let source_hash = hash::digest("source");
        cache.notify_added(
            &mut tree,
            &mut blobs,
            &mut dirty,
            Peer::from("peer"),
            source_path.clone(),
            test_time(),
            6,
            source_hash.clone(),
        )?;

        let dest_hash = hash::digest("dest");
        cache.notify_added(
            &mut tree,
            &mut blobs,
            &mut dirty,
            Peer::from("peer"),
            dest_path.clone(),
            test_time(),
            4,
            dest_hash.clone(),
        )?;

        assert!(matches!(
            cache.rename(
                &mut tree,
                &mut blobs,
                &mut history,
                &mut dirty,
                &source_path,
                &dest_path,
                /*noreplace=*/ true,
            ),
            Err(StorageError::AlreadyExists)
        ));

        cache.rename(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &source_path,
            &dest_path,
            /*noreplace=*/ false,
        )?;

        // dest is gone, source replaces it and points to it
        assert!(cache.file_entry(&tree, &source_path)?.is_none());
        let dest_entry = cache.file_entry_or_err(&tree, &dest_path).unwrap();
        assert_eq!(
            tree.resolve(&source_path)?,
            match dest_entry.kind {
                FileEntryKind::Branched(pathid) => Some(pathid),
                _ => None,
            }
        );
        assert_eq!(Version::Indexed(source_hash.clone()), dest_entry.version);
        assert_eq!(6, dest_entry.size);

        // history entry reports old hash
        let (_, history_entry) = history.history(0..).last().unwrap().unwrap();
        assert_eq!(
            HistoryTableEntry::Rename(source_path, dest_path, source_hash),
            history_entry
        );

        Ok(())
    }

    #[tokio::test]
    async fn rename_local_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let source_path = Path::parse("source")?;
        let dest_path = Path::parse("dest")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut dirty = txn.write_dirty()?;
        let mut blobs = txn.write_blobs()?;
        let mut history = txn.write_history()?;

        // Create and index a local file
        let datadir = fixture.datadir.path();
        let source_realpath = source_path.within(&datadir);
        std::fs::write(&source_realpath, "test content")?;
        let mtime = UnixTime::mtime(&source_realpath.metadata()?);
        let hash = hash::digest("test content");
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &source_path,
            12,
            mtime,
            hash.clone(),
        )?;

        let source_pathid = tree.expect(&source_path)?;
        let dest_realpath = dest_path.within(&datadir);

        // Verify source file exists and dest doesn't
        assert!(source_realpath.exists());
        assert!(!dest_realpath.exists());

        let history_start = history_start(&history)?;
        clear_dirty(&mut dirty)?;
        cache.rename(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &source_path,
            &dest_path,
            true, // noreplace
        )?;

        // File should be moved on disk
        assert!(!source_realpath.exists());
        assert!(dest_realpath.exists());
        assert_eq!(std::fs::read_to_string(&dest_realpath)?, "test content");

        // Source entry should be gone, dest should exist
        assert!(cache.file_entry(&tree, &source_path)?.is_none());
        let dest_entry = cache.file_entry_or_err(&tree, &dest_path).unwrap();
        assert_eq!(Version::Indexed(hash.clone()), dest_entry.version);
        assert_eq!(12, dest_entry.size);
        assert_eq!(mtime, dest_entry.mtime);
        assert!(dest_entry.is_local());

        // Check dirty entries
        let dest_pathid = tree.expect(&dest_path)?;
        let dirty_set = dirty_pathids(&dirty)?;
        assert!(dirty_set.contains(&source_pathid));
        assert!(dirty_set.contains(&dest_pathid));

        // inodes are swapped
        assert_eq!(
            Inode(source_pathid.as_u64()),
            cache.map_to_inode(dest_pathid)?
        );
        assert_eq!(
            Inode(dest_pathid.as_u64()),
            cache.map_to_inode(source_pathid)?
        );

        // history entry was added

        assert_eq!(
            vec![
                HistoryTableEntry::Add(dest_path),
                HistoryTableEntry::Remove(source_path, hash)
            ],
            collect_history_entries(&history, history_start)?
        );

        Ok(())
    }

    #[tokio::test]
    async fn rename_local_file_replace() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let source_path = Path::parse("source")?;
        let dest_path = Path::parse("dest")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut dirty = txn.write_dirty()?;
        let mut blobs = txn.write_blobs()?;
        let mut history = txn.write_history()?;

        let datadir = fixture.datadir.path();

        // Create and index source file
        let source_realpath = source_path.within(&datadir);
        std::fs::write(&source_realpath, "source content")?;
        let source_mtime = UnixTime::mtime(&source_realpath.metadata()?);
        let source_hash = hash::digest("source content");
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &source_path,
            14,
            source_mtime,
            source_hash.clone(),
        )?;

        // Create and index dest file
        let dest_realpath = dest_path.within(&datadir);
        std::fs::write(&dest_realpath, "dest content")?;
        let dest_mtime = UnixTime::mtime(&dest_realpath.metadata()?);
        let dest_hash = hash::digest("dest content");
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &dest_path,
            12,
            dest_mtime,
            dest_hash.clone(),
        )?;

        // Both files should exist
        assert!(source_realpath.exists());
        assert!(dest_realpath.exists());

        let history_start = history_start(&history)?;

        // Test noreplace=true first (should fail)
        assert!(matches!(
            cache.rename(
                &mut tree,
                &mut blobs,
                &mut history,
                &mut dirty,
                &source_path,
                &dest_path,
                true, // noreplace
            ),
            Err(StorageError::AlreadyExists)
        ));

        cache.rename(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &source_path,
            &dest_path,
            false, // allow replace
        )?;

        assert!(!source_realpath.exists());
        assert!(dest_realpath.exists());
        assert_eq!(std::fs::read_to_string(&dest_realpath)?, "source content");

        assert!(cache.file_entry(&tree, &source_path)?.is_none());
        let dest_entry = cache.file_entry_or_err(&tree, &dest_path).unwrap();
        assert_eq!(
            tree.resolve(&source_path)?,
            match dest_entry.kind {
                FileEntryKind::Branched(pathid) => Some(pathid),
                _ => None,
            }
        );
        assert_eq!(Version::Indexed(source_hash.clone()), dest_entry.version);
        assert_eq!(14, dest_entry.size);
        assert!(dest_entry.is_local());

        assert_eq!(
            vec![
                HistoryTableEntry::Replace(dest_path, dest_hash),
                HistoryTableEntry::Remove(source_path, source_hash)
            ],
            collect_history_entries(&history, history_start)?
                .into_iter()
                .rev()
                .take(2)
                .rev()
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    #[tokio::test]
    async fn rename_local_file_replace_cached() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let source_path = Path::parse("source")?;
        let dest_path = Path::parse("dest")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut dirty = txn.write_dirty()?;
        let mut blobs = txn.write_blobs()?;
        let mut history = txn.write_history()?;

        let datadir = fixture.datadir.path();

        // Create and index source file (local)
        let source_realpath = source_path.within(&datadir);
        std::fs::write(&source_realpath, "source content")?;
        let source_mtime = UnixTime::mtime(&source_realpath.metadata()?);
        let source_hash = hash::digest("source content");
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &source_path,
            14,
            source_mtime,
            source_hash.clone(),
        )?;

        // Create dest as cached only (notify_added)
        let dest_hash = hash::digest("cached dest content");
        cache.notify_added(
            &mut tree,
            &mut blobs,
            &mut dirty,
            Peer::from("peer"),
            dest_path.clone(),
            test_time(),
            19,
            dest_hash.clone(),
        )?;

        let dest_realpath = dest_path.within(&datadir);
        let history_start = history_start(&history)?;
        cache.rename(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &source_path,
            &dest_path,
            false, // allow replace
        )?;

        // Source file should be gone, dest should now exist with source content
        assert!(!source_realpath.exists());
        assert!(dest_realpath.exists());
        assert_eq!(std::fs::read_to_string(&dest_realpath)?, "source content");

        // Source entry should be gone, dest should exist with source data and be local
        assert!(cache.file_entry(&tree, &source_path)?.is_none());
        let dest_entry = cache.file_entry_or_err(&tree, &dest_path).unwrap();
        assert_eq!(
            tree.resolve(&source_path)?,
            match dest_entry.kind {
                FileEntryKind::Branched(pathid) => Some(pathid),
                _ => None,
            }
        );
        assert_eq!(Version::Indexed(source_hash.clone()), dest_entry.version);
        assert_eq!(14, dest_entry.size);
        assert!(dest_entry.is_local());

        assert_eq!(
            vec![
                HistoryTableEntry::Replace(dest_path, dest_hash),
                HistoryTableEntry::Remove(source_path, source_hash)
            ],
            collect_history_entries(&history, history_start)?
                .into_iter()
                .rev()
                .take(2)
                .rev()
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    #[tokio::test]
    async fn rename_cached_file_replace_local() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let source_path = Path::parse("source")?;
        let dest_path = Path::parse("dest")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut dirty = txn.write_dirty()?;
        let mut blobs = txn.write_blobs()?;
        let mut history = txn.write_history()?;

        let datadir = fixture.datadir.path();

        // Create source as cached only (notify_added)
        let source_hash = hash::digest("cached source content");
        cache.notify_added(
            &mut tree,
            &mut blobs,
            &mut dirty,
            Peer::from("peer"),
            source_path.clone(),
            test_time(),
            21,
            source_hash.clone(),
        )?;

        // Create and index dest file (local)
        let dest_realpath = dest_path.within(&datadir);
        std::fs::write(&dest_realpath, "dest content")?;
        let dest_mtime = UnixTime::mtime(&dest_realpath.metadata()?);
        let dest_hash = hash::digest("dest content");
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &dest_path,
            12,
            dest_mtime,
            dest_hash.clone(),
        )?;

        cache.rename(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &source_path,
            &dest_path,
            false, // allow replace
        )?;

        // Since source was not local, dest file should be removed from disk
        assert!(!dest_realpath.exists());

        // Source entry should be gone, dest should exist with source data but not be local
        assert!(cache.file_entry(&tree, &source_path)?.is_none());
        let dest_entry = cache.file_entry_or_err(&tree, &dest_path).unwrap();
        assert_eq!(
            tree.resolve(&source_path)?,
            match dest_entry.kind {
                FileEntryKind::Branched(pathid) => Some(pathid),
                _ => None,
            }
        );
        assert_eq!(Version::Indexed(source_hash.clone()), dest_entry.version);
        assert_eq!(21, dest_entry.size);
        assert!(!dest_entry.is_local());

        // history entry reports old hash
        let (_, history_entry) = history.history(0..).last().unwrap().unwrap();
        assert_eq!(
            HistoryTableEntry::Rename(source_path, dest_path, source_hash),
            history_entry
        );

        Ok(())
    }

    #[tokio::test]
    async fn rename_dir() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let source_dir = Path::parse("source_dir")?;
        let dest_dir = Path::parse("dest_dir")?;
        let file1_path = Path::parse("source_dir/file1.txt")?;
        let file2_path = Path::parse("source_dir/subdir/file2.txt")?;
        let file3_path = Path::parse("source_dir/subdir/file3.txt")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut dirty = txn.write_dirty()?;
        let mut blobs = txn.write_blobs()?;
        let mut history = txn.write_history()?;

        // Create source directory with files
        let hash1 = hash::digest("file1");
        cache.notify_added(
            &mut tree,
            &mut blobs,
            &mut dirty,
            Peer::from("peer"),
            file1_path.clone(),
            test_time(),
            5,
            hash1.clone(),
        )?;
        let file1_pathid = tree.resolve(&file1_path)?.unwrap();

        let hash2 = hash::digest("file2");
        cache.notify_added(
            &mut tree,
            &mut blobs,
            &mut dirty,
            Peer::from("peer"),
            file2_path.clone(),
            test_time(),
            5,
            hash2.clone(),
        )?;
        let file2_pathid = tree.resolve(&file2_path)?.unwrap();

        let file3_realpath = file3_path.within(cache.datadir());
        std::fs::create_dir_all(file3_realpath.parent().unwrap()).unwrap();
        std::fs::write(&file3_realpath, "test").unwrap();
        let file3_mtime = UnixTime::mtime(&file3_realpath.metadata()?);
        let hash3 = hash::digest("test");
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &file3_path,
            4,
            file3_mtime,
            hash3.clone(),
        )?;
        let file3_pathid = tree.resolve(&file3_path)?.unwrap();

        let source_dir_pathid = tree.expect(&source_dir)?;
        clear_dirty(&mut dirty)?;
        let history_start = history_start(&history)?;
        // Perform the directory rename
        cache.rename(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &source_dir,
            &dest_dir,
            /*noreplace=*/ true,
        )?;

        // After directory rename, rename should remove the source
        // directory and all of its subdirectories once their contents
        // are moved to the destination
        assert!(
            cache.metadata(&tree, &source_dir)?.is_none(),
            "Source directory should be removed after rename"
        );
        let dest_metadata = cache.metadata(&tree, &dest_dir)?;
        assert!(
            matches!(dest_metadata, Some(crate::Metadata::Dir(_))),
            "Destination directory should exist after rename"
        );

        // Verify files moved correctly to destination directory
        let moved_file1 = Path::parse("dest_dir/file1.txt")?;
        let moved_file2 = Path::parse("dest_dir/subdir/file2.txt")?;
        let moved_file3 = Path::parse("dest_dir/subdir/file3.txt")?;

        let file1_entry = cache.file_entry(&tree, &moved_file1)?.unwrap();
        assert_eq!(Version::Indexed(hash1.clone()), file1_entry.version);
        assert_eq!(5, file1_entry.size);
        assert_eq!(test_time(), file1_entry.mtime);
        assert!(!file1_entry.is_local());

        let file2_entry = cache.file_entry(&tree, &moved_file2)?.unwrap();
        assert_eq!(Version::Indexed(hash2.clone()), file2_entry.version);
        assert_eq!(5, file2_entry.size);
        assert_eq!(test_time(), file2_entry.mtime);
        assert!(!file2_entry.is_local());

        let file3_entry = cache.file_entry(&tree, &moved_file3)?.unwrap();
        assert_eq!(Version::Indexed(hash3.clone()), file3_entry.version);
        assert_eq!(4, file3_entry.size);
        assert_eq!(file3_mtime, file3_entry.mtime);
        assert!(file3_entry.is_local());

        let moved_file3_realpath = moved_file3.within(cache.datadir());
        assert!(moved_file3_realpath.exists());
        assert_eq!("test", std::fs::read_to_string(&moved_file3_realpath)?);

        // Original file paths should not exist (since source directory tree is removed)
        assert!(cache.file_entry(&tree, &file1_path)?.is_none(),);
        assert!(cache.file_entry(&tree, &file2_path)?.is_none(),);
        assert!(cache.file_entry(&tree, &file3_path)?.is_none(),);
        assert!(!file3_path.within(cache.datadir()).exists());

        // Source directory should also be removed, both in the cache and the filesystem.
        let source_dir = Path::parse("source_dir")?;
        assert!(cache.metadata(&tree, &source_dir)?.is_none(),);
        assert!(!source_dir.within(cache.datadir()).exists());

        // Check that files are marked dirty
        let moved_file1_pathid = tree.resolve(&moved_file1)?.unwrap();
        let moved_file2_pathid = tree.resolve(&moved_file2)?.unwrap();
        let moved_file3_pathid = tree.resolve(&moved_file3)?.unwrap();
        let dest_dir_pathid = tree.expect(&dest_dir)?;
        assert_eq!(
            HashSet::from([
                file1_pathid,
                file2_pathid,
                file3_pathid,
                moved_file1_pathid,
                moved_file2_pathid,
                moved_file3_pathid
            ]),
            dirty_pathids(&dirty)?
        );

        // Directory inode should be swapped - at least for the top
        // directories.
        assert_eq!(
            Inode(source_dir_pathid.as_u64()),
            cache.map_to_inode(dest_dir_pathid)?
        );
        assert_eq!(
            Inode(dest_dir_pathid.as_u64()),
            cache.map_to_inode(source_dir_pathid)?
        );
        assert_eq!(
            source_dir_pathid,
            cache.map_to_pathid(Inode(dest_dir_pathid.as_u64()))?
        );
        assert_eq!(
            dest_dir_pathid,
            cache.map_to_pathid(Inode(source_dir_pathid.as_u64()))?
        );

        assert_unordered::assert_eq_unordered!(
            vec![
                HistoryTableEntry::Rename(file1_path, moved_file1, hash1),
                HistoryTableEntry::Rename(file2_path, moved_file2, hash2),
                HistoryTableEntry::Add(moved_file3),
                HistoryTableEntry::Remove(file3_path, hash3),
            ],
            collect_history_entries(&history, history_start)?
        );

        Ok(())
    }

    #[tokio::test]
    async fn rename_dir_replace() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let source_dir = Path::parse("source_dir")?;
        let dest_dir = Path::parse("dest_dir")?;
        let source_file = Path::parse("source_dir/file.txt")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut dirty = txn.write_dirty()?;
        let mut blobs = txn.write_blobs()?;
        let mut history = txn.write_history()?;

        // Create source directory with a file
        let source_hash = hash::digest("source_file");
        cache.notify_added(
            &mut tree,
            &mut blobs,
            &mut dirty,
            Peer::from("peer"),
            source_file.clone(),
            test_time(),
            11,
            source_hash.clone(),
        )?;

        // Create empty destination directory
        cache.mkdir(&mut tree, &dest_dir)?;

        // Test noreplace=true should fail with AlreadyExists
        assert!(matches!(
            cache.rename(
                &mut tree,
                &mut blobs,
                &mut history,
                &mut dirty,
                &source_dir,
                &dest_dir,
                /*noreplace=*/ true,
            ),
            Err(StorageError::AlreadyExists)
        ));

        // Test noreplace=false should succeed (replacing empty directory)
        cache.rename(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &source_dir,
            &dest_dir,
            /*noreplace=*/ false,
        )?;

        assert!(
            cache.metadata(&tree, &source_dir)?.is_none(),
            "Source directory should be removed after rename"
        );
        let dest_metadata = cache.metadata(&tree, &dest_dir)?;
        assert!(
            matches!(dest_metadata, Some(crate::Metadata::Dir(_))),
            "Destination directory should exist after rename"
        );

        Ok(())
    }

    #[tokio::test]
    async fn rename_dir_nonempty_dest() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let source_dir = Path::parse("source_dir")?;
        let dest_dir = Path::parse("dest_dir")?;
        let source_file = Path::parse("source_dir/file.txt")?;
        let dest_file = Path::parse("dest_dir/otherfile.txt")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut dirty = txn.write_dirty()?;
        let mut blobs = txn.write_blobs()?;
        let mut history = txn.write_history()?;

        // Create source and dest directories with a file
        let source_hash = hash::digest("source_file");
        cache.notify_added(
            &mut tree,
            &mut blobs,
            &mut dirty,
            Peer::from("peer"),
            source_file.clone(),
            test_time(),
            11,
            source_hash.clone(),
        )?;

        let dest_hash = hash::digest("dest_file");
        cache.notify_added(
            &mut tree,
            &mut blobs,
            &mut dirty,
            Peer::from("peer"),
            dest_file.clone(),
            test_time(),
            11,
            dest_hash.clone(),
        )?;

        // Even with noreplace=false, this should fail because dest is
        // nonempty.
        assert!(matches!(
            cache.rename(
                &mut tree,
                &mut blobs,
                &mut history,
                &mut dirty,
                &source_dir,
                &dest_dir,
                /*noreplace=*/ false,
            ),
            Err(StorageError::AlreadyExists)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_datadir() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let txn = fixture.db.begin_read()?;
        let cache = txn.read_cache()?;

        assert_eq!(fixture.datadir.path(), cache.datadir());

        Ok(())
    }

    #[test]
    fn index() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("foo/bar.txt")?;
        let childpath = fixture.datadir.child("foo/bar.txt");

        // Create file with specific content that produces our desired hash
        let contents = "test content for index"; // This will produce a different hash than [0xfa; 32]
        let (size, mtime, hash) = create_test_file(&childpath, contents)?;

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &path,
            size,
            mtime,
            hash.clone(),
        )?;

        assert!(cache.has_local_file(&tree, &path)?);
        let entry = cache.indexed(&tree, &path)?.unwrap();
        assert_eq!(entry.size, size);
        assert_eq!(entry.mtime, mtime);
        assert_eq!(entry.hash, hash);

        Ok(())
    }

    #[test]
    fn replace_file_in_index() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("foo/bar.txt")?;
        let childpath = fixture.datadir.child("foo/bar.txt");

        // Create first file
        let contents1 = "first version";
        let (size1, mtime1, hash1) = create_test_file(&childpath, contents1)?;

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &path,
            size1,
            mtime1,
            hash1,
        )?;

        // Replace with second file
        let contents2 = "second version with different content";
        let (size2, mtime2, hash2) = create_test_file(&childpath, contents2)?;

        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &path,
            size2,
            mtime2,
            hash2.clone(),
        )?;

        // Verify the file was replaced
        assert!(cache.has_local_file(&tree, &path)?);
        let entry = cache.indexed(&tree, &path)?.unwrap();
        assert_eq!(entry.size, size2);
        assert_eq!(entry.mtime, mtime2);
        assert_eq!(entry.hash, hash2);

        Ok(())
    }

    #[test]
    fn is_indexed() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("foo.txt")?;
        let childpath = fixture.datadir.child("foo.txt");

        // Create file
        let contents = "content for is_indexed test";
        let (size, mtime, hash) = create_test_file(&childpath, contents)?;

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &path,
            size,
            mtime,
            hash,
        )?;

        assert!(cache.has_local_file(&tree, &path)?);
        assert!(!cache.has_local_file(&tree, &Path::parse("bar.txt")?)?);
        assert!(!cache.has_local_file(&tree, &Path::parse("other.txt")?)?);

        Ok(())
    }

    #[test]
    fn indexed() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("foo/bar")?;
        let childpath = fixture.datadir.child("foo/bar");

        // Create file
        let contents = "content for indexed test";
        let (size, mtime, hash) = create_test_file(&childpath, contents)?;

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &path,
            size,
            mtime,
            hash.clone(),
        )?;

        let entry = cache.indexed(&tree, &path)?.unwrap();
        assert_eq!(entry.size, size);
        assert_eq!(entry.mtime, mtime);
        assert_eq!(entry.hash, hash);

        Ok(())
    }

    #[test]
    fn remove_file_from_index_recursively() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("foo/bar.txt")?;
        let childpath = fixture.datadir.child("foo/bar.txt");

        // Create file
        let contents = "content for remove_file_from_index_recursively test";
        let (size, mtime, hash) = create_test_file(&childpath, contents)?;

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &path,
            size,
            mtime,
            hash,
        )?;

        cache.remove_from_index_recursively(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &path,
        )?;

        assert!(!cache.has_local_file(&tree, &path)?);

        Ok(())
    }

    #[test]
    fn remove_local_dir_from_index() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let path = Path::parse("foo/bar")?;
        let childpath = fixture.datadir.child("foo/bar");

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        let (size, mtime, hash) = create_test_file(&childpath, "test")?;
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            path.clone(),
            size,
            mtime,
            hash,
        )?;

        let foo = Path::parse("foo")?;
        assert!(matches!(
            cache.metadata(&tree, &foo)?,
            Some(crate::Metadata::Dir(_))
        ));
        std::fs::remove_file(childpath.path())?;
        std::fs::remove_dir_all(childpath.parent().unwrap())?;
        cache.remove_from_index_recursively(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &foo,
        )?;

        assert_eq!(None, cache.metadata(&tree, &foo)?);

        Ok(())
    }

    #[test]
    fn remove_local_and_remote_dir() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let remote = Path::parse("foo/remote")?;
        fixture.add_to_cache(&remote, 4, test_time())?;

        let local = Path::parse("foo/local")?;
        let local_child = fixture.datadir.child("foo/local");

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        let (size, mtime, hash) = create_test_file(&local_child, "test")?;
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            local.clone(),
            size,
            mtime,
            hash,
        )?;

        let foo = Path::parse("foo")?;
        assert!(matches!(
            cache.metadata(&tree, &foo)?,
            Some(crate::Metadata::Dir(_))
        ));
        std::fs::remove_file(local_child.path())?;
        std::fs::remove_dir_all(local_child.parent().unwrap())?;
        cache.remove_from_index_recursively(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &foo,
        )?;

        // Directory is still there, since it contains "remote", but
        // is now marked remote-only.
        assert!(cache.metadata(&tree, &foo)?.is_some());

        // Once all remote files are gone, the remote-only dir is
        // deleted.
        cache.notify_dropped_or_removed(
            &mut tree,
            &mut blobs,
            &mut dirty,
            test_peer(),
            remote,
            &test_hash(),
            false,
        )?;

        assert_eq!(None, cache.metadata(&tree, &foo)?);

        Ok(())
    }

    #[tokio::test]
    async fn index_marks_dirty_and_adds_history() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("foo/bar.txt")?;
        let childpath = fixture.datadir.child("foo/bar.txt");

        // Create file
        let contents = "content for index_marks_dirty_and_adds_history test";
        let (size, mtime, hash) = create_test_file(&childpath, contents)?;

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        clear_dirty(&mut dirty)?;
        let history_start = history_start(&history)?;

        // Add a file
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &path,
            size,
            mtime,
            hash.clone(),
        )?;

        // Verify the file was added to the index
        assert!(cache.has_local_file(&tree, &path)?);

        // Verify the path was marked dirty
        assert!(dirty_paths(&dirty, &tree)?.contains(&path));

        // Verify history entry was added (Add entry)
        let history_entries = collect_history_entries(&history, history_start)?;
        assert_eq!(history_entries.len(), 1);
        match &history_entries[0] {
            HistoryTableEntry::Add(entry_path) => {
                assert_eq!(entry_path, &path);
            }
            _ => panic!("Expected Add history entry"),
        }

        Ok(())
    }

    #[test]
    fn replace_indexed_marks_dirty_and_adds_history() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("foo/bar.txt")?;
        let childpath = fixture.datadir.child("foo/bar.txt");

        // Create initial file
        let contents1 = "initial content";
        let (size1, mtime1, hash1) = create_test_file(&childpath, contents1)?;

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        // Clear any existing dirty entries
        clear_dirty(&mut dirty)?;

        // Add initial file
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &path,
            size1,
            mtime1,
            hash1.clone(),
        )?;

        clear_dirty(&mut dirty)?;

        // Replace the file with new content
        let contents2 = "replaced content with different data";
        let (size2, mtime2, hash2) = create_test_file(&childpath, contents2)?;

        let history_start = history_start(&history)?;
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &path,
            size2,
            mtime2,
            hash2.clone(),
        )?;

        // Verify the file was replaced in the index
        let entry = cache.indexed(&tree, &path)?.unwrap();
        assert_eq!(entry.hash, hash2);

        // Verify the path was marked dirty
        let dirty_paths = dirty_paths(&dirty, &tree)?;
        assert!(dirty_paths.contains(&path));

        assert_eq!(
            vec![HistoryTableEntry::Replace(path.clone(), hash1)],
            collect_history_entries(&history, history_start)?
        );

        Ok(())
    }

    #[test]
    fn remove_from_index_marks_dirty_and_adds_history() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("foo/bar.txt")?;
        let childpath = fixture.datadir.child("foo/bar.txt");

        // Create file
        let contents = "content for remove_from_index_marks_dirty_and_adds_history test";
        let (size, mtime, hash) = create_test_file(&childpath, contents)?;

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        // Add a file first
        let pathid = cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &path,
            size,
            mtime,
            hash.clone(),
        )?;

        let history_start = history_start(&history)?;
        clear_dirty(&mut dirty)?;

        // Remove the file
        let removed =
            cache.remove_from_index(&mut tree, &mut blobs, &mut history, &mut dirty, &path)?;
        assert!(removed);

        // Verify the file was removed from the index
        assert!(!cache.has_local_file(&tree, &path)?);

        // Verify the path was marked dirty
        assert_eq!(HashSet::from([pathid]), dirty_pathids(&dirty)?);

        assert_eq!(
            vec![HistoryTableEntry::Remove(path.clone(), hash)],
            collect_history_entries(&history, history_start)?
        );

        Ok(())
    }

    #[test]
    fn remove_from_index_recursively_marks_dirty_and_adds_history() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        // Add files in a directory
        let path1 = Path::parse("foo/bar1.txt")?;
        let path2 = Path::parse("foo/bar2.txt")?;
        let childpath1 = fixture.datadir.child("foo/bar1.txt");
        let childpath2 = fixture.datadir.child("foo/bar2.txt");

        let contents1 = "content for bar1 in recursive test";
        let contents2 = "content for bar2 in recursive test";
        let (size1, mtime1, hash1) = create_test_file(&childpath1, contents1)?;
        let (size2, mtime2, hash2) = create_test_file(&childpath2, contents2)?;

        let pathid1 = cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &path1,
            size1,
            mtime1,
            hash1.clone(),
        )?;
        let pathid2 = cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &path2,
            size2,
            mtime2,
            hash2.clone(),
        )?;

        clear_dirty(&mut dirty)?;
        let history_start = history_start(&history)?;

        // Remove the directory
        cache.remove_from_index_recursively(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            Path::parse("foo")?,
        )?;

        // Verify the files were removed from the index
        assert!(!cache.has_local_file(&tree, &path1)?);
        assert!(!cache.has_local_file(&tree, &path2)?);

        // Verify the paths were marked dirty
        assert_eq!(HashSet::from([pathid1, pathid2]), dirty_pathids(&dirty)?);

        // Verify history entries were added (2 Add + 2 Remove entries)
        let history_entries = collect_history_entries(&history, history_start)?;
        assert_unordered::assert_eq_unordered!(
            vec![
                HistoryTableEntry::Remove(path1.clone(), hash1.clone()),
                HistoryTableEntry::Remove(path2.clone(), hash2.clone()),
            ],
            history_entries
        );

        Ok(())
    }

    #[test]
    fn preindex_new_then_index() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("foo/bar.txt")?;
        let childpath = fixture.datadir.child("foo/bar.txt");

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        childpath.write_str("test")?;
        let mtime = UnixTime::mtime(&childpath.metadata()?);
        let history_start = history_start(&history)?;
        cache.preindex(&mut tree, &mut blobs, &mut dirty, &path)?;
        assert!(cache.has_local_file(&tree, &path)?);
        assert_eq!(None, cache.indexed(&tree, &path)?);
        assert_eq!(0, collect_history_entries(&history, history_start)?.len());

        let hash = hash::digest("test");
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &path,
            4,     // actual file size
            mtime, // actual file mtime
            hash.clone(),
        )?;
        assert!(cache.has_local_file(&tree, &path)?);
        assert_eq!(
            IndexedFile {
                size: 4,
                mtime: mtime,
                hash: hash.clone(),
            },
            cache.indexed(&tree, &path)?.unwrap()
        );

        assert_eq!(
            vec![HistoryTableEntry::Add(path)],
            collect_history_entries(&history, history_start)?
        );

        Ok(())
    }

    #[test]
    fn preindex_existing_then_reindex() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("foo/bar.txt")?;
        let childpath = fixture.datadir.child("foo/bar.txt");

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        childpath.write_str("initial")?;
        let initial_hash = hash::digest("initial");
        let initial_mtime = UnixTime::mtime(&childpath.metadata()?);
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &path,
            7,
            initial_mtime,
            initial_hash.clone(),
        )?;

        let history_start = history_start(&history)?;
        childpath.write_str("modified")?;
        let preindex_mtime = UnixTime::mtime(&childpath.metadata()?);
        cache.preindex(&mut tree, &mut blobs, &mut dirty, &path)?;
        assert_eq!(
            Some(Version::Modified(Some(initial_hash.clone()))),
            cache.file_entry(&tree, &path)?.map(|e| e.version)
        );
        assert_eq!(0, collect_history_entries(&history, history_start)?.len());

        let reindex_mtime = preindex_mtime;
        let reindex_hash = hash::digest("modified");
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &path,
            8,
            reindex_mtime,
            reindex_hash.clone(),
        )?;
        assert_eq!(
            Some(Version::Indexed(reindex_hash.clone())),
            cache.file_entry(&tree, &path)?.map(|e| e.version)
        );
        assert_eq!(reindex_hash, cache.indexed(&tree, &path)?.unwrap().hash);

        assert_eq!(
            vec![HistoryTableEntry::Replace(path, initial_hash.clone())],
            collect_history_entries(&history, history_start)?
        );

        Ok(())
    }

    #[test]
    fn preindex_existing_then_delete() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("foobar")?;
        let childpath = fixture.datadir.child("foobar");

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        childpath.write_str("hash1")?;
        let initial_hash = hash::digest("hash1");
        let initial_mtime = UnixTime::mtime(&childpath.metadata()?);
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &path,
            5, // actual size of "hash1"
            initial_mtime,
            initial_hash.clone(),
        )?;

        let history_start = history_start(&history)?;
        childpath.write_str("hash2")?;
        cache.preindex(&mut tree, &mut blobs, &mut dirty, &path)?;
        assert_eq!(0, collect_history_entries(&history, history_start)?.len());

        cache
            .unlink(&mut tree, &mut blobs, &mut history, &mut dirty, &path)
            .unwrap();
        assert!(cache.metadata(&tree, &path).unwrap().is_none());
        assert!(!childpath.exists());
        assert_eq!(
            vec![HistoryTableEntry::Remove(path, initial_hash.clone())],
            collect_history_entries(&history, history_start)?
        );

        Ok(())
    }

    #[test]
    fn preindex_special_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("foobar")?;
        let childpath = fixture.datadir.child("foobar");

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        unix::fs::symlink(fixture.datadir.join("doesnotexist"), childpath.path())?;
        let m = childpath.symlink_metadata()?;

        let history_start = history_start(&history)?;
        let pathid = cache
            .preindex(&mut tree, &mut blobs, &mut dirty, &path)
            .unwrap();
        assert_eq!(
            pathid,
            cache
                .preindex(&mut tree, &mut blobs, &mut dirty, &path)
                .unwrap()
        );
        assert!(cache.has_local_file(&tree, &path)?);
        assert_eq!(0, collect_history_entries(&history, history_start)?.len());

        assert_eq!(
            Some(FileEntryKind::SpecialFile),
            cache.file_at_pathid(pathid)?.map(|e| e.kind)
        );

        assert_eq!(
            Some(crate::Metadata::File(FileMetadata {
                size: m.len(),
                mtime: UnixTime::mtime(&m),
                version: Version::Modified(None),
                ctime: Some(UnixTime::from_system_time(m.created().unwrap()).unwrap()),
                mode: m.mode(),
                uid: Some(m.uid()),
                gid: Some(m.gid()),
                blocks: m.len() / 512 + if (m.len() % 512) == 0 { 0 } else { 1 },
            })),
            cache.metadata(&tree, pathid)?
        );
        assert_eq!(None, cache.indexed(&tree, &path)?);

        // Attempting to index even a preindex special file results in
        // an error
        assert!(matches!(
            cache.index(
                &mut tree,
                &mut blobs,
                &mut history,
                &mut dirty,
                &path,
                m.len(),
                UnixTime::mtime(&m),
                hash::digest("doesnotexist"),
            ),
            Err(StorageError::LocalFileMismatch)
        ));

        // Now turn the symlink int a real file, but keep the old
        // preindexed entry; indexing should not be confused by the
        // outdated preindexed entry.
        std::fs::remove_file(childpath.path())?;
        childpath.write_str("test")?;
        let m = childpath.symlink_metadata()?;
        cache
            .index(
                &mut tree,
                &mut blobs,
                &mut history,
                &mut dirty,
                &path,
                m.len(),
                UnixTime::mtime(&m),
                hash::digest("test"),
            )
            .unwrap();
        assert!(cache.indexed(&tree, &path)?.is_some());

        Ok(())
    }

    #[test]
    fn unindex() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("foo/bar.txt")?;
        let childpath = fixture.datadir.child("foo/bar.txt");

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;

        childpath.write_str("test")?;
        let mtime = UnixTime::mtime(&childpath.metadata()?);
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &path,
            4,     // actual file size
            mtime, // actual file mtime
            hash::digest("test"),
        )?;

        let history_start = history_start(&history)?;
        clear_dirty(&mut dirty)?;
        cache.unindex(&mut tree, &mut blobs, &mut history, &mut dirty, &path)?;

        // path is not index inymore, but is still in the index as a
        // local file.
        assert_eq!(None, cache.indexed(&tree, &path)?);
        assert!(cache.has_local_file(&tree, &path)?);
        assert_eq!(HashSet::from([path.clone()]), dirty_paths(&dirty, &tree)?);

        // The file has been reported as having been removed.
        assert_eq!(
            vec![HistoryTableEntry::Remove(
                path.clone(),
                hash::digest("test")
            )],
            collect_history_entries(&history, history_start)?
        );

        Ok(())
    }

    #[tokio::test]
    async fn list_alternatives_not_found() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let missing_path = Path::parse("does_not_exist.txt")?;

        let alternatives = fixture.list_alternatives(&missing_path)?;
        assert!(alternatives.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn list_alternatives_directory() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let dir_path = Path::parse("test_directory")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        cache.mkdir(&mut tree, &dir_path)?;
        assert!(cache.list_alternatives(&tree, &dir_path)?.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn list_alternatives_local_indexed() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let file_path = Path::parse("local_file.txt")?;
        let file_content = "test content for local file";
        let childpath = fixture.datadir.child("local_file.txt");

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut blobs = txn.write_blobs()?;
        let mut history = txn.write_history()?;
        let mut dirty = txn.write_dirty()?;

        // Create and index a local file
        let (size, mtime, hash) = create_test_file(&childpath, file_content)?;
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &file_path,
            size,
            mtime,
            hash.clone(),
        )?;

        assert_eq!(
            cache.list_alternatives(&tree, &file_path)?,
            vec![FileAlternative::Local(Version::Indexed(hash))]
        );

        Ok(())
    }

    #[tokio::test]
    async fn list_alternatives_local_modified() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let file_path = Path::parse("modified_file.txt")?;
        let childpath = fixture.datadir.child("modified_file.txt");

        let (size, mtime, hash) = create_test_file(&childpath, "original content")?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut blobs = txn.write_blobs()?;
        let mut history = txn.write_history()?;
        let mut dirty = txn.write_dirty()?;
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &file_path,
            size,
            mtime,
            hash.clone(),
        )?;

        // Modify the file and preindex it (to mark as modified)
        childpath.write_str("modified content")?;
        cache.preindex(&mut tree, &mut blobs, &mut dirty, &file_path)?;

        assert_eq!(
            cache.list_alternatives(&tree, &file_path)?,
            vec![FileAlternative::Local(Version::Modified(Some(hash)))]
        );

        Ok(())
    }

    #[tokio::test]
    async fn list_alternatives_remote() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let file_path = Path::parse("remote_file.txt")?;
        let peer1 = Peer::from("peer1");
        let peer2 = Peer::from("peer2");
        let hash1 = Hash([1u8; 32]);
        let hash2 = Hash([2u8; 32]);
        let mtime1 = test_time();
        let mtime2 = later_time();

        // Add the same file from two different peers with different hashes
        fixture.add_file_from_peer(peer1, &file_path, 100, mtime1, hash1.clone())?;
        fixture.add_file_from_peer(peer2, &file_path, 200, mtime2, hash2.clone())?;

        assert_unordered::assert_eq_unordered!(
            fixture.list_alternatives(&file_path)?,
            vec![
                FileAlternative::Remote(peer1, hash1.clone(), 100, mtime1),
                FileAlternative::Remote(peer2, hash2.clone(), 200, mtime2)
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn list_alternatives_branched() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let source_path = Path::parse("source.txt")?;
        let branch_path = Path::parse("branch.txt")?;
        let hash = Hash([42u8; 32]);
        let peer = Peer::from("remote_peer");

        // Add a remote file first (not local)
        fixture.add_file_from_peer(peer, &source_path, 100, test_time(), hash.clone())?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut blobs = txn.write_blobs()?;
        let mut history = txn.write_history()?;
        let mut dirty = txn.write_dirty()?;

        cache.branch(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &source_path,
            &branch_path,
        )?;
        assert_eq!(
            cache.list_alternatives(&tree, &branch_path)?,
            vec![FileAlternative::Branched(source_path.clone(), hash.clone())]
        );

        Ok(())
    }

    #[tokio::test]
    async fn select_alternative_no_change_when_same_hash() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let file_path = Path::parse("test_file.txt")?;
        let peer = Peer::from("test_peer");
        let hash = Hash([42u8; 32]);
        let mtime = test_time();
        let size = 100;

        // Add file from peer
        fixture.add_file_from_peer(peer, &file_path, size, mtime, hash.clone())?;

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut history = txn.write_history()?;
        let mut dirty = txn.write_dirty()?;

        let history_start = history_start(&history)?;

        // Selecting the same hash should be a no-op
        cache.select_alternative(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &file_path,
            &hash,
        )?;

        // No history entries should be generated
        assert_eq!(0, collect_history_entries(&history, history_start)?.len());

        // File should still exist with the same version
        let alternatives = cache.list_alternatives(&tree, &file_path)?;
        assert_eq!(alternatives.len(), 1);
        assert!(matches!(
            alternatives[0],
            FileAlternative::Remote(ref p, ref h, s, m)
                if *p == peer && *h == hash && s == size && m == mtime
        ));

        Ok(())
    }

    #[tokio::test]
    async fn select_alternative_switch_remote_versions() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let file_path = Path::parse("versioned_file.txt")?;
        let peer1 = Peer::from("peer1");
        let peer2 = Peer::from("peer2");
        let hash1 = Hash([1u8; 32]);
        let hash2 = Hash([2u8; 32]);
        let mtime1 = test_time();
        let mtime2 = later_time();
        let size1 = 100;
        let size2 = 200;

        // Add same file from two different peers
        fixture.add_file_from_peer(peer1, &file_path, size1, mtime1, hash1.clone())?;
        fixture.add_file_from_peer(peer2, &file_path, size2, mtime2, hash2.clone())?;

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut history = txn.write_history()?;
        let mut dirty = txn.write_dirty()?;

        // Verify we have two alternatives initially
        let alternatives = cache.list_alternatives(&tree, &file_path)?;
        assert_eq!(alternatives.len(), 2);

        let history_start = history_start(&history)?;

        // Select the alternative version (hash2 from peer2)
        cache.select_alternative(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &file_path,
            &hash2,
        )?;

        // Verify the file now points to peer2's version
        let entry = cache.file_entry_or_err(&tree, &file_path)?;
        assert_eq!(entry.version.expect_indexed()?, &hash2);
        assert_eq!(entry.size, size2);
        assert_eq!(entry.mtime, mtime2);

        // History should be empty since this wasn't a local file
        assert_eq!(0, collect_history_entries(&history, history_start)?.len());

        Ok(())
    }

    #[tokio::test]
    async fn select_alternative_from_local_to_remote() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let file_path = Path::parse("local_to_remote.txt")?;
        let peer = Peer::from("remote_peer");
        let local_hash = Hash([1u8; 32]);
        let remote_hash = Hash([2u8; 32]);
        let mtime = test_time();
        let local_content = "local content";
        let childpath = fixture.datadir.child("local_to_remote.txt");

        // Create local file first
        let (size, actual_mtime, _) = create_test_file(&childpath, local_content)?;

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut history = txn.write_history()?;
        let mut dirty = txn.write_dirty()?;

        // Index the local file
        cache.index(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &file_path,
            size,
            actual_mtime,
            local_hash.clone(),
        )?;

        // Verify file is local
        let entry = cache.file_entry_or_err(&tree, &file_path)?;
        assert!(entry.is_local());
        assert!(childpath.exists());

        // Add remote version from peer
        cache.notify_added(
            &mut tree,
            &mut blobs,
            &mut dirty,
            peer,
            file_path.clone(),
            mtime,
            200,
            remote_hash.clone(),
        )?;

        let history_start = history_start(&history)?;

        // Select the remote alternative
        cache.select_alternative(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &file_path,
            &remote_hash,
        )?;

        // Verify the file now points to the remote version
        let entry = cache.file_entry_or_err(&tree, &file_path)?;
        assert!(!entry.is_local());
        assert_eq!(entry.version.expect_indexed()?, &remote_hash);
        assert_eq!(entry.size, 200);

        // Local file should be deleted
        assert!(!childpath.exists());

        // History should contain both replace and drop entries for the local file
        let history_entries = collect_history_entries(&history, history_start)?;
        assert_eq!(
            history_entries,
            vec![
                HistoryTableEntry::Replace(file_path.clone(), local_hash.clone()),
                HistoryTableEntry::Drop(file_path.clone(), remote_hash.clone())
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn select_alternative_unknown_version_error() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let file_path = Path::parse("unknown_hash.txt")?;
        let peer = Peer::from("test_peer");
        let known_hash = Hash([1u8; 32]);
        let unknown_hash = Hash([99u8; 32]);
        let mtime = test_time();

        // Add file from peer with known hash
        fixture.add_file_from_peer(peer, &file_path, 100, mtime, known_hash.clone())?;

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut history = txn.write_history()?;
        let mut dirty = txn.write_dirty()?;

        // Try to select an unknown hash
        let result = cache.select_alternative(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &file_path,
            &unknown_hash,
        );

        assert!(matches!(result, Err(StorageError::UnknownVersion)));

        // Original file should remain unchanged
        let entry = cache.file_entry_or_err(&tree, &file_path)?;
        assert_eq!(entry.version.expect_indexed()?, &known_hash);

        Ok(())
    }

    #[tokio::test]
    async fn select_alternative_nonexistent_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let file_path = Path::parse("does_not_exist.txt")?;
        let hash = Hash([1u8; 32]);

        let txn = fixture.db.begin_write()?;
        let mut cache = txn.write_cache()?;
        let mut tree = txn.write_tree()?;
        let mut blobs = txn.write_blobs()?;
        let mut history = txn.write_history()?;
        let mut dirty = txn.write_dirty()?;

        // Try to select an alternative for a nonexistent file
        let result = cache.select_alternative(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            &file_path,
            &hash,
        );

        assert!(matches!(result, Err(StorageError::NotFound)));

        Ok(())
    }

    #[tokio::test]
    async fn remove_remote_file_removes_remote_dirs() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        let peer1 = Peer::from("peer1");
        let file_path = Path::parse("a/b/c/file.txt")?;
        update::apply(
            &fixture.db,
            peer1,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 4,
                hash: hash::digest("test"),
            },
        )?;

        assert!(fixture.dir_metadata(Path::parse("a/b/c")?).is_ok());

        update::apply(
            &fixture.db,
            peer1,
            Notification::Remove {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                old_hash: hash::digest("test"),
            },
        )?;

        assert!(matches!(
            fixture.dir_metadata(Path::parse("a/b/c")?),
            Err(StorageError::NotFound)
        ));
        assert!(matches!(
            fixture.dir_metadata(Path::parse("a/b")?),
            Err(StorageError::NotFound)
        ));
        assert!(matches!(
            fixture.dir_metadata(Path::parse("a")?),
            Err(StorageError::NotFound)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn remove_remote_file_keeps_dir_created_by_mkdir() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        let peer1 = Peer::from("peer1");
        let file_path = Path::parse("a/b/c/file.txt")?;
        update::apply(
            &fixture.db,
            peer1,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 4,
                hash: hash::digest("test"),
            },
        )?;

        assert!(fixture.dir_metadata(Path::parse("a/b/c")?).is_ok());

        let txn = fixture.db.begin_write()?;
        txn.write_cache()?
            .mkdir(&mut txn.write_tree()?, &Path::parse("a/b")?)?;
        txn.commit()?;

        update::apply(
            &fixture.db,
            peer1,
            Notification::Remove {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                old_hash: hash::digest("test"),
            },
        )?;

        // a/b/c has been removed, but a/b is still there, even though
        // it's empty, because it was created explicitly by mkdir.
        assert!(matches!(
            fixture.dir_metadata(Path::parse("a/b/c")?),
            Err(StorageError::NotFound)
        ));
        assert!(fixture.dir_metadata(Path::parse("a/b")?).is_ok());
        assert!(fixture.dir_metadata(Path::parse("a")?).is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn remove_remote_file_keeps_dirs_containing_local_files() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        let peer1 = Peer::from("peer1");
        let file_path = Path::parse("a/b/c/file.txt")?;
        update::apply(
            &fixture.db,
            peer1,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 4,
                hash: hash::digest("test"),
            },
        )?;

        assert!(fixture.dir_metadata(Path::parse("a/b/c")?).is_ok());

        let txn = fixture.db.begin_write()?;
        let local_file = Path::parse("a/b/local_file")?;
        fixture.datadir.child("a/b/local_file").write_str("test")?;
        txn.write_cache()?.preindex(
            &mut txn.write_tree()?,
            &mut txn.write_blobs()?,
            &mut txn.write_dirty()?,
            &local_file,
        )?;
        txn.commit()?;

        update::apply(
            &fixture.db,
            peer1,
            Notification::Remove {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                old_hash: hash::digest("test"),
            },
        )?;

        // a/b/c has been removed, but a/b is still there, even though
        // it's empty, because it was created explicitly by mkdir.
        assert!(matches!(
            fixture.dir_metadata(Path::parse("a/b/c")?),
            Err(StorageError::NotFound)
        ));
        assert!(fixture.dir_metadata(Path::parse("a/b")?).is_ok());
        assert!(fixture.dir_metadata(Path::parse("a")?).is_ok());

        Ok(())
    }
}
