use super::blob::{BlobExt, BlobInfo, BlobReadOperations, WritableOpenBlob};
use super::db::{ArenaDatabase, ArenaWriteTransaction};
use super::dirty::WritableOpenDirty;
use super::peer::PeersReadOperations;
use super::tree::{TreeExt, TreeReadOperations, WritableOpenTree};
use super::types::{
    CacheTableEntry, CacheTableKey, DirtableEntry, FileAvailability, FileMetadata, FileTableEntry,
};
use super::update;
use crate::arena::history::WritableOpenHistory;
use crate::arena::notifier::{Notification, Progress};
use crate::arena::tree;
use crate::arena::types::DirMetadata;
use crate::global::types::PathAssignment;
use crate::types::Inode;
use crate::utils::holder::Holder;
use crate::{Blob, LocalAvailability};
use crate::{PathId, StorageError};
use realize_types::{Arena, Hash, Path, Peer, UnixTime};
use redb::{ReadableTable, Table};
use std::collections::HashSet;
use std::sync::Arc;

// TreeLoc is necessary to call ArenaCache methods
pub(crate) use super::tree::TreeLoc;

/// Read operations for cache. See also [CacheExt].
pub(crate) trait CacheReadOperations {
    /// Get metadata for a file or directory.
    fn metadata<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<crate::arena::types::Metadata, StorageError>;

    /// Get remote file availability information for the given pathid and version.
    fn file_availability<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
        hash: &Hash,
    ) -> Result<Option<FileAvailability>, StorageError>;

    /// Read directory contents for the given pathid.
    fn readdir<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> impl Iterator<Item = Result<(String, PathId, crate::arena::types::Metadata), StorageError>>;

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
}

/// A cache open for reading with a read transaction.
pub(crate) struct ReadableOpenCache<T, PN, NP>
where
    T: ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
    PN: ReadableTable<PathId, Inode>,
    NP: ReadableTable<Inode, PathId>,
{
    table: T,
    #[allow(dead_code)]
    pathid_to_inode: PN,
    #[allow(dead_code)]
    inode_to_pathid: NP,
    arena: Arena,
}

impl<T, PN, NP> ReadableOpenCache<T, PN, NP>
where
    T: ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
    PN: ReadableTable<PathId, Inode>,
    NP: ReadableTable<Inode, PathId>,
{
    pub(crate) fn new(table: T, pathid_to_inode: PN, inode_to_pathid: NP, arena: Arena) -> Self {
        Self {
            table,
            pathid_to_inode,
            inode_to_pathid,
            arena,
        }
    }
}

impl<T, PN, NP> CacheReadOperations for ReadableOpenCache<T, PN, NP>
where
    T: ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
    PN: ReadableTable<PathId, Inode>,
    NP: ReadableTable<Inode, PathId>,
{
    fn metadata<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<crate::arena::types::Metadata, StorageError> {
        metadata(&self.table, tree.expect(loc)?)?.ok_or(StorageError::NotFound)
    }

    fn file_availability<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
        hash: &Hash,
    ) -> Result<Option<FileAvailability>, StorageError> {
        file_availability(&self.table, tree, loc, self.arena, hash)
    }

    fn readdir<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> impl Iterator<Item = Result<(String, PathId, crate::arena::types::Metadata), StorageError>>
    {
        ReadDirIterator::new(&self.table, tree, loc)
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
}

impl<'a> CacheReadOperations for WritableOpenCache<'a> {
    fn metadata<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<crate::arena::types::Metadata, StorageError> {
        metadata(&self.table, tree.expect(loc)?)?.ok_or(StorageError::NotFound)
    }

    fn file_availability<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
        hash: &Hash,
    ) -> Result<Option<FileAvailability>, StorageError> {
        file_availability(&self.table, tree, loc, self.arena, hash)
    }

    fn readdir<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> impl Iterator<Item = Result<(String, PathId, crate::arena::types::Metadata), StorageError>>
    {
        ReadDirIterator::new(&self.table, tree, loc)
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

    fn lookup<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<(PathId, crate::arena::types::Metadata), StorageError>;

    fn dir_mtime<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<UnixTime, StorageError>;
}

impl<T: CacheReadOperations> CacheExt for T {
    fn file_metadata<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<FileMetadata, StorageError> {
        self.metadata(tree, loc)?.expect_file()
    }

    fn lookup<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<(PathId, crate::arena::types::Metadata), StorageError> {
        let pathid = tree.expect(loc)?;

        Ok((pathid, self.metadata(tree, pathid)?))
    }

    fn dir_mtime<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<UnixTime, StorageError> {
        Ok(self.metadata(tree, loc)?.expect_dir()?.mtime)
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
}

/// A cache open for writing with a write transaction.
pub(crate) struct WritableOpenCache<'a> {
    table: Table<'a, CacheTableKey, Holder<'static, CacheTableEntry>>,
    pathid_to_inode: Table<'a, PathId, Inode>,
    inode_to_pathid: Table<'a, Inode, PathId>,
    pending_catchup_table: Table<'a, (&'static str, PathId), ()>,
    arena: Arena,
}

impl<'a> WritableOpenCache<'a> {
    pub(crate) fn new(
        table: Table<'a, CacheTableKey, Holder<CacheTableEntry>>,
        pathid_to_inode: Table<'a, PathId, Inode>,
        inode_to_pathid: Table<'a, Inode, PathId>,
        pending_catchup_table: Table<'a, (&'static str, PathId), ()>,
        arena: Arena,
    ) -> Self {
        Self {
            table,
            pathid_to_inode,
            inode_to_pathid,
            pending_catchup_table,
            arena,
        }
    }

    /// Create a blob for that file, unless one already exists.
    pub(crate) fn create_blob(
        &mut self,
        txn: &ArenaWriteTransaction,
        pathid: PathId,
    ) -> Result<BlobInfo, StorageError> {
        let file_entry = default_file_entry_or_err(&self.table, pathid)?;
        let mut blobs = txn.write_blobs()?;
        let mut tree = txn.write_tree()?;
        let marks = txn.read_marks()?;
        blobs.create(&mut tree, &marks, pathid, &file_entry.hash, file_entry.size)
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
        let pathid = tree.expect(loc)?;
        let e = default_file_entry_or_err(&self.table, pathid)?;
        log::debug!(
            "[{}]@local Local removal of \"{}\" pathid {pathid} {}",
            self.arena,
            e.path,
            e.hash
        );
        self.rm_default_file_entry(tree, blobs, dirty, pathid)?;
        history.report_removed(&e.path, &e.hash)?;

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
        let source = source.into();
        let dest = dest.into();
        let source_pathid = tree.expect(source.borrow())?;
        let dest_pathid; // delay calling tree.setup until source is checked
        match default_entry(&self.table, source_pathid)?.ok_or(StorageError::NotFound)? {
            CacheTableEntry::Dir(dir_entry) => {
                dest_pathid = tree.setup(dest.borrow())?;
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
                self.rename_files_in_dir(tree, blobs, history, dirty, source_pathid, dest_pathid)?;
                write_dir_mtime(&mut self.table, tree, dest_pathid, dir_entry.mtime)?;
                tree.remove_and_decref(
                    source_pathid,
                    &mut self.table,
                    CacheTableKey::Default(source_pathid),
                )?;
            }
            CacheTableEntry::File(mut entry) => {
                dest_pathid = tree.setup(dest.borrow())?;
                let old_hash = match default_entry(&self.table, dest_pathid)? {
                    None => None,
                    Some(CacheTableEntry::File(entry)) => Some(entry.hash),
                    Some(CacheTableEntry::Dir(_)) => return Err(StorageError::IsADirectory),
                };
                if old_hash.is_some() && noreplace {
                    return Err(StorageError::AlreadyExists);
                }

                entry.branched_from = Some(source_pathid);
                self.write_default_file_entry(tree, blobs, dirty, dest_pathid, &entry)?;
                self.rm_default_file_entry(tree, blobs, dirty, source_pathid)?;

                if let (Some(source_path), Some(dest_path)) =
                    (tree.backtrack(source)?, tree.backtrack(dest)?)
                {
                    history.request_rename(
                        &source_path,
                        &dest_path,
                        &entry.hash,
                        old_hash.as_ref(),
                    )?;
                }
            }
        }
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

    /// Recursively move all entries in `source_dir` to `dest_dir`.
    fn rename_files_in_dir(
        &mut self,
        tree: &mut WritableOpenTree,
        blobs: &mut WritableOpenBlob,
        history: &mut WritableOpenHistory,
        dirty: &mut WritableOpenDirty,
        source_dir: PathId,
        dest_dir: PathId,
    ) -> Result<(), StorageError> {
        for (name, source_entry) in tree
            .readdir(source_dir)
            .map(|res| res.map(|(n, p)| (n.to_string(), p)))
            .collect::<Result<Vec<_>, _>>()?
        {
            match default_entry(&self.table, source_entry)? {
                None => {}
                Some(CacheTableEntry::Dir(dir_entry)) => {
                    let dest_entry = tree.setup((dest_dir, name))?;
                    self.rename_files_in_dir(
                        tree,
                        blobs,
                        history,
                        dirty,
                        source_entry,
                        dest_entry,
                    )?;
                    write_dir_mtime(&mut self.table, tree, dest_entry, dir_entry.mtime)?;
                    tree.remove_and_decref(
                        source_entry,
                        &mut self.table,
                        CacheTableKey::Default(source_entry),
                    )?;
                }
                Some(CacheTableEntry::File(mut file_entry)) => {
                    let dest_entry = tree.setup((dest_dir, name))?;
                    file_entry.branched_from = Some(source_entry);
                    self.write_default_file_entry(tree, blobs, dirty, dest_entry, &file_entry)?;
                    self.rm_default_file_entry(tree, blobs, dirty, source_entry)?;

                    if let (Some(source_path), Some(dest_path)) =
                        (tree.backtrack(source_entry)?, tree.backtrack(dest_entry)?)
                    {
                        history.request_rename(&source_path, &dest_path, &file_entry.hash, None)?;
                    }
                }
            }
        }

        Ok(())
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
        let dest = dest.into();
        let source_pathid = tree.expect(source.borrow())?;
        let mut entry = self.file_at_pathid_or_err(source_pathid)?;
        entry.branched_from = Some(source_pathid);
        let dest_pathid = tree.setup(dest.borrow())?;
        let old_hash = self.file_at_pathid(dest_pathid)?.map(|e| e.hash);
        check_parent_is_dir(&self.table, tree, dest_pathid)?;
        self.write_default_file_entry(tree, blobs, dirty, dest_pathid, &entry)?;

        if let (Some(source_path), Some(dest_path)) =
            (tree.backtrack(source)?, tree.backtrack(dest)?)
        {
            history.request_branch(&source_path, &dest_path, &entry.hash, old_hash.as_ref())?;
        }

        Ok((
            dest_pathid,
            FileMetadata {
                size: entry.size,
                mtime: entry.mtime,
                hash: entry.hash,
            },
        ))
    }

    pub(crate) fn mkdir<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        loc: L,
    ) -> Result<(PathId, DirMetadata), StorageError> {
        let pathid = tree.setup(loc)?;
        if self.table.get(CacheTableKey::Default(pathid))?.is_some() {
            return Err(StorageError::AlreadyExists);
        }
        check_parent_is_dir(&self.table, tree, pathid)?;
        let mtime = UnixTime::now();
        write_dir_mtime(&mut self.table, tree, pathid, mtime)?;

        Ok((
            pathid,
            DirMetadata {
                read_only: false,
                mtime,
            },
        ))
    }

    pub(crate) fn rmdir<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        loc: L,
    ) -> Result<(), StorageError> {
        let pathid = tree.expect(loc)?;
        match pathid_assignment(&self.table, pathid)? {
            Some(PathAssignment::File) => Err(StorageError::NotADirectory),
            None => Err(StorageError::NotFound),
            Some(PathAssignment::Directory) => {
                if !self.readdir(tree, pathid).next().is_none() {
                    return Err(StorageError::DirectoryNotEmpty);
                }
                tree.remove_and_decref(pathid, &mut self.table, CacheTableKey::Default(pathid))?;

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
        let entry = FileTableEntry::new(path.clone(), size, mtime, hash.clone());
        if peer_file_entry(&self.table, file_pathid, Some(peer))?.is_none() {
            log::debug!("[{}]@{peer} Add \"{path}\" {hash} size={size}", self.arena);
            self.write_file_entry(tree, file_pathid, peer, &entry)?;
        }
        Ok(
            if peer_file_entry(&self.table, file_pathid, None)?.is_none() {
                log::debug!("[{}]@local Add \"{path}\" {hash} size={size}", self.arena);
                self.write_default_file_entry(tree, blobs, dirty, file_pathid, &entry)?;
            },
        )
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
        if let Some(file_pathid) = tree.resolve(path)? {
            let entry = FileTableEntry::new(path.clone(), size, mtime, hash.clone());
            if let Some(e) = peer_file_entry(&self.table, file_pathid, Some(peer))?
                && e.hash == *old_hash
            {
                log::debug!(
                    "[{}]@{peer} \"{path}\" {hash} size={size} replaces {old_hash}",
                    self.arena
                );
                self.write_file_entry(tree, file_pathid, peer, &entry)?;
            }
            if let Some(old_entry) = peer_file_entry(&self.table, file_pathid, None)?
                && old_entry.hash == *old_hash
            {
                log::debug!(
                    "[{}]@local \"{path}\" {hash} size={size} replaces {old_hash}",
                    self.arena
                );
                self.write_default_file_entry(tree, blobs, dirty, file_pathid, &entry)?;
            }
        }
        Ok(())
    }

    /// Start catchup for a peer.
    pub(crate) fn catchup_start(&mut self, peer: Peer) -> Result<(), StorageError> {
        for elt in self.table.iter()? {
            let (k, _) = elt?;
            if let CacheTableKey::PeerCopy(pathid, elt_peer) = k.value()
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
        let entry = FileTableEntry::new(path.clone(), size, mtime, hash.clone());
        if let Some(e) = peer_file_entry(&self.table, file_pathid, None)?
            && e.hash != hash
        {
            self.notify_dropped_or_removed(tree, blobs, dirty, peer, &path, &e.hash, false)?;
        }
        self.write_file_entry(tree, file_pathid, peer, &entry)?;
        Ok(
            if !peer_file_entry(&self.table, file_pathid, None)?.is_some() {
                self.write_default_file_entry(tree, blobs, dirty, file_pathid, &entry)?;
            },
        )
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
            CacheTableKey::PeerCopy(file_pathid, peer),
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
            CacheTableKey::Default(pathid),
            Holder::new(&CacheTableEntry::File(new_entry.clone()))?,
        )? {
            // Update the parent directory mtime since we're adding a file to it
            if let Some(parent_pathid) = tree.parent(pathid)? {
                write_dir_mtime(&mut self.table, tree, parent_pathid, UnixTime::now())?;
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
        tree.remove_and_decref(
            pathid,
            &mut self.table,
            CacheTableKey::PeerCopy(pathid, peer),
        )?;

        Ok(())
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
        if tree.remove_and_decref(pathid, &mut self.table, CacheTableKey::Default(pathid))? {
            // Update the parent modification time, as removing an
            // entry modifies it.
            if let Some(parent) = parent {
                write_dir_mtime(&mut self.table, tree, parent, UnixTime::now())?;
            }
        }

        Ok(())
    }

    /// Unlink a remove or a drop from a peer.
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
                && e.hash == *old_hash
            {
                log::debug!(
                    "[{}]@{peer} Remove \"{path}\" pathid {pathid} {old_hash}",
                    self.arena
                );

                self.rm_peer_file_entry(tree, pathid, peer)?;
            }
            if !dropped {
                if let Some(e) = peer_file_entry(&self.table, pathid, None)?
                    && e.hash == *old_hash
                {
                    log::debug!(
                        "[{}]@local Remove \"{path}\" pathid {pathid} {old_hash}",
                        self.arena
                    );
                    self.rm_default_file_entry(tree, blobs, dirty, pathid)?;
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
                if self.file_availability(tree, pathid, &entry.hash)?.is_none() {
                    // If the file has become unavailable because of
                    // this removal, remove the file itself as well.
                    self.rm_default_file_entry(tree, blobs, dirty, pathid)?;
                }
            }
        }
        Ok(())
    }
}

/// Initialize the database. This should be called at startup, in the
/// transaction that crates new tables.
pub(crate) fn init(
    cache_table: &mut redb::Table<'_, CacheTableKey, Holder<CacheTableEntry>>,
    root_pathid: PathId,
) -> Result<(), StorageError> {
    if cache_table
        .get(CacheTableKey::Default(root_pathid))?
        .is_none()
    {
        // Exceptionally not using write_dir_mtime and not going
        // through tree because , arena roots aren't refcounted by
        // tree and are never deleted.
        cache_table.insert(
            CacheTableKey::Default(root_pathid),
            Holder::with_content(CacheTableEntry::Dir(DirtableEntry {
                mtime: UnixTime::now(),
            }))?,
        )?;
    }
    Ok(())
}

/// Lookup a specific name in the given directory pathid.
fn lookup<'b, L: Into<TreeLoc<'b>>>(
    cache_table: &impl ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
    tree: &impl TreeReadOperations,
    loc: L,
) -> Result<(PathId, crate::arena::types::Metadata), StorageError> {
    let pathid = tree.expect(loc)?;
    let metadata = metadata(cache_table, pathid)?.ok_or(StorageError::NotFound)?;

    return Ok((pathid, metadata));
}

/// Get metadata for a file or directory.
fn metadata(
    cache_table: &impl ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
    pathid: PathId,
) -> Result<Option<crate::arena::types::Metadata>, StorageError> {
    if let Some(e) = cache_table.get(CacheTableKey::Default(pathid))? {
        Ok(Some(match e.value().parse()? {
            CacheTableEntry::File(file_entry) => {
                crate::arena::types::Metadata::File(crate::arena::types::FileMetadata {
                    size: file_entry.size,
                    mtime: file_entry.mtime,
                    hash: file_entry.hash,
                })
            }
            CacheTableEntry::Dir(dir_entry) => {
                crate::arena::types::Metadata::Dir(crate::arena::types::DirMetadata {
                    read_only: false, // Arena directories are writable
                    mtime: dir_entry.mtime,
                })
            }
        }))
    } else {
        Ok(None)
    }
}

/// Get file availability information for the given pathid.
fn file_availability<'b, L: Into<TreeLoc<'b>>>(
    cache_table: &impl ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
    tree: &impl TreeReadOperations,
    loc: L,
    arena: Arena,
    hash: &Hash,
) -> Result<Option<FileAvailability>, StorageError> {
    let mut avail = None;
    let pathid = tree.expect(loc)?;
    let mut visited = HashSet::new();
    let mut next = Some(pathid);
    while let Some(pathid) = next
        && avail.is_none()
        && !visited.contains(&pathid)
    {
        visited.insert(pathid); // avoid loops
        next = None;
        for entry in cache_table.range(CacheTableKey::range(pathid))? {
            let entry = entry?;
            match entry.0.value() {
                CacheTableKey::PeerCopy(_, peer) => {
                    let file_entry: FileTableEntry = entry.1.value().parse()?.expect_file()?;
                    if file_entry.hash == *hash {
                        avail
                            .get_or_insert_with(|| FileAvailability {
                                arena,
                                path: file_entry.path,
                                size: file_entry.size,
                                hash: file_entry.hash,
                                peers: vec![],
                            })
                            .peers
                            .push(peer);
                    }
                }
                CacheTableKey::Default(_) => {
                    let file_entry: FileTableEntry = entry.1.value().parse()?.expect_file()?;
                    next = file_entry.branched_from;
                }
                _ => {}
            }
        }
    }
    if avail.is_none() {
        log::warn!("[{arena}] No peer has hash {hash} for {pathid}",);
    }

    Ok(avail)
}

struct ReadDirIterator<'a, 'b, T> {
    table: &'a T,
    iter: tree::ReadDirIterator<'b>,
}

impl<'a, 'b, T> ReadDirIterator<'a, 'b, T>
where
    T: ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
{
    fn new<'l, L: Into<TreeLoc<'l>>>(
        table: &'a T,
        tree: &'b impl TreeReadOperations,
        loc: L,
    ) -> Self {
        ReadDirIterator {
            table,
            iter: match lookup(table, tree, loc) {
                Err(err) => tree::ReadDirIterator::failed(err),
                Ok((pathid, crate::arena::types::Metadata::Dir(_))) => tree.readdir_pathid(pathid),
                Ok(_) => tree::ReadDirIterator::failed(StorageError::NotADirectory),
            },
        }
    }
}

impl<'a, 'b, T> Iterator for ReadDirIterator<'a, 'b, T>
where
    T: ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
{
    type Item = Result<(String, PathId, crate::arena::types::Metadata), StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(entry) = self.iter.next() {
            match entry {
                Err(err) => return Some(Err(err)),
                Ok((name, pathid)) => {
                    match metadata(self.table, pathid) {
                        Err(err) => return Some(Err(err)),
                        Ok(Some(metadata)) => return Some(Ok((name, pathid, metadata))),
                        Ok(None) => {} // not in the cache; skip
                    }
                }
            }
        }
        None
    }
}

/// A per-arena cache of remote files.
///
/// This struct handles all cache operations for a specific arena.
/// It contains the arena's database and root pathid.
pub(crate) struct ArenaCache {
    arena: Arena,
    db: Arc<ArenaDatabase>,
}

impl ArenaCache {
    #[cfg(test)]
    pub fn for_testing_single_arena(
        arena: realize_types::Arena,
        blob_dir: &std::path::Path,
    ) -> anyhow::Result<Arc<Self>> {
        ArenaCache::for_testing(
            arena,
            crate::PathIdAllocator::new(
                crate::GlobalDatabase::new(crate::utils::redb_utils::in_memory()?)?,
                [arena],
            )?,
            blob_dir,
        )
    }

    #[cfg(test)]
    pub fn for_testing(
        arena: realize_types::Arena,
        allocator: Arc<crate::PathIdAllocator>,
        blob_dir: &std::path::Path,
    ) -> anyhow::Result<Arc<Self>> {
        let db = ArenaDatabase::new(
            crate::utils::redb_utils::in_memory()?,
            arena,
            allocator,
            blob_dir,
        )?;

        Ok(ArenaCache::new(arena, Arc::clone(&db))?)
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
        loc: impl Into<CacheLoc>,
    ) -> Result<(Inode, crate::arena::types::Metadata), StorageError> {
        let txn = self.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        let (pathid, metadata) = cache.lookup(&tree, loc.into().into_tree_loc(&cache)?)?;

        Ok((cache.map_to_inode(pathid)?, metadata))
    }

    pub(crate) fn file_metadata(
        &self,
        loc: impl Into<CacheLoc>,
    ) -> Result<FileMetadata, StorageError> {
        let txn = self.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        cache.file_metadata(&tree, loc.into().into_tree_loc(&cache)?)
    }

    pub(crate) fn dir_metadata(
        &self,
        loc: impl Into<CacheLoc>,
    ) -> Result<DirMetadata, StorageError> {
        let txn = self.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        Ok(DirMetadata {
            read_only: false,
            mtime: cache.dir_mtime(&tree, loc.into().into_tree_loc(&cache)?)?,
        })
    }

    pub(crate) fn metadata(
        &self,
        loc: impl Into<CacheLoc>,
    ) -> Result<crate::arena::types::Metadata, StorageError> {
        let txn = self.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        cache.metadata(&tree, loc.into().into_tree_loc(&cache)?)
    }

    pub(crate) fn readdir(
        &self,
        loc: impl Into<CacheLoc>,
    ) -> Result<Vec<(String, Inode, crate::arena::types::Metadata)>, StorageError> {
        let txn = self.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        let mut vec = vec![];
        for res in cache.readdir(&tree, loc.into().into_tree_loc(&cache)?) {
            let (name, pathid, metadata) = res?;
            vec.push((name, cache.map_to_inode(pathid)?, metadata));
        }

        Ok(vec)
    }

    pub(crate) fn peer_progress(&self, peer: Peer) -> Result<Option<Progress>, StorageError> {
        let txn = self.db.begin_read()?;
        let peers = txn.read_peers()?;
        peers.progress(peer)
    }

    pub(crate) fn unlink(&self, loc: impl Into<CacheLoc>) -> Result<(), StorageError> {
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

    pub(crate) fn branch(
        &self,
        source: impl Into<CacheLoc>,
        dest: impl Into<CacheLoc>,
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
        source: impl Into<CacheLoc>,
        dest: impl Into<CacheLoc>,
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
        index_root: Option<&std::path::Path>,
    ) -> Result<(), StorageError> {
        update::apply(&self.db, index_root, peer, notification)
    }

    /// Open a file for reading/writing.
    pub(crate) fn open_file(&self, loc: impl Into<CacheLoc>) -> Result<Blob, StorageError> {
        // Optimistically, try a read transaction to check whether the
        // blob is there. The pathid is kept across transactions,
        // because it is guaranteed to be stable.
        let pathid;
        {
            let txn = self.db.begin_read()?;
            let tree = txn.read_tree()?;
            pathid = tree.expect(loc.into().into_tree_loc(&txn.read_cache()?)?)?;
            let blobs = txn.read_blobs()?;
            if let Some(info) = blobs.get_with_pathid(pathid)? {
                return Blob::open_with_info(&self.db, info);
            }
        }

        // Switch to a write transaction to create the blob. We need to read
        // the file entry again because it might have changed.
        let txn = self.db.begin_write()?;
        let info = {
            let mut cache = txn.write_cache()?;
            cache.create_blob(&txn, pathid)?
        };
        txn.commit()?;

        Ok(Blob::open_with_info(&self.db, info)?)
    }

    pub(crate) fn local_availability(
        &self,
        loc: impl Into<CacheLoc>,
    ) -> Result<LocalAvailability, StorageError> {
        let txn = self.db.begin_read()?;
        let tree = txn.read_tree()?;
        let blobs = txn.read_blobs()?;
        let cache = txn.read_cache()?;
        blobs.local_availability(&tree, loc.into().into_tree_loc(&cache)?)
    }

    /// Create a directory at the given path.
    pub(crate) fn mkdir(
        &self,
        loc: impl Into<CacheLoc>,
    ) -> Result<(Inode, DirMetadata), StorageError> {
        let txn = self.db.begin_write()?;
        let result = {
            let mut tree = txn.write_tree()?;
            let mut cache = txn.write_cache()?;

            let (pathid, m) = cache.mkdir(&mut tree, loc.into().into_tree_loc(&cache)?)?;

            (cache.map_to_inode(pathid)?, m)
        };
        txn.commit()?;

        Ok(result)
    }

    /// Remove an empty directory at the given path.
    pub(crate) fn rmdir(&self, loc: impl Into<CacheLoc>) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let mut cache = txn.write_cache()?;
            cache.rmdir(&mut tree, loc.into().into_tree_loc(&cache)?)?;
        }
        txn.commit()?;

        Ok(())
    }
}

pub(crate) enum CacheLoc {
    PathId(PathId),
    Inode(Inode),
    Path(Path),
    PathIdAndName(PathId, String),
    InodeAndName(Inode, String),
}

impl CacheLoc {
    fn into_tree_loc(
        self,
        cache: &impl CacheReadOperations,
    ) -> Result<TreeLoc<'static>, StorageError> {
        Ok(match self {
            CacheLoc::PathId(pathid) => TreeLoc::PathId(pathid),
            CacheLoc::Path(path) => TreeLoc::Path(path),
            CacheLoc::PathIdAndName(pathid, name) => TreeLoc::PathIdAndName(pathid, name.into()),
            CacheLoc::Inode(inode) => TreeLoc::PathId(cache.map_to_pathid(inode)?),
            CacheLoc::InodeAndName(inode, name) => {
                TreeLoc::PathIdAndName(cache.map_to_pathid(inode)?, name.into())
            }
        })
    }
}
impl From<PathId> for CacheLoc {
    fn from(value: PathId) -> Self {
        CacheLoc::PathId(value)
    }
}

impl From<Inode> for CacheLoc {
    fn from(value: Inode) -> Self {
        CacheLoc::Inode(value)
    }
}

impl From<Path> for CacheLoc {
    fn from(value: Path) -> Self {
        CacheLoc::Path(value)
    }
}

impl From<&Path> for CacheLoc {
    fn from(value: &Path) -> Self {
        CacheLoc::Path(value.clone())
    }
}

impl From<(PathId, &str)> for CacheLoc {
    fn from(value: (PathId, &str)) -> Self {
        CacheLoc::PathIdAndName(value.0, value.1.to_string())
    }
}

impl From<(PathId, &String)> for CacheLoc {
    fn from(value: (PathId, &String)) -> Self {
        CacheLoc::PathIdAndName(value.0, value.1.to_string())
    }
}
impl From<(PathId, String)> for CacheLoc {
    fn from(value: (PathId, String)) -> Self {
        CacheLoc::PathIdAndName(value.0, value.1)
    }
}

impl From<(Inode, &str)> for CacheLoc {
    fn from(value: (Inode, &str)) -> Self {
        CacheLoc::InodeAndName(value.0, value.1.to_string())
    }
}

impl From<(Inode, &String)> for CacheLoc {
    fn from(value: (Inode, &String)) -> Self {
        CacheLoc::InodeAndName(value.0, value.1.to_string())
    }
}
impl From<(Inode, String)> for CacheLoc {
    fn from(value: (Inode, String)) -> Self {
        CacheLoc::InodeAndName(value.0, value.1)
    }
}

/// Get a [FileTableEntry] for a specific peer.
fn peer_file_entry(
    cache_table: &impl redb::ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
    pathid: PathId,
    peer: Option<Peer>,
) -> Result<Option<FileTableEntry>, StorageError> {
    match cache_table.get(
        peer.map(|p| CacheTableKey::PeerCopy(pathid, p))
            .unwrap_or_else(|| CacheTableKey::Default(pathid)),
    )? {
        None => Ok(None),
        Some(e) => Ok(e.value().parse()?.file()),
    }
}

/// Get the default entry for the given pathid.
///
/// Returns None if the file cannot be found or if it is a directory.
fn default_file_entry(
    cache_table: &impl redb::ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
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
    cache_table: &impl redb::ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
    pathid: PathId,
) -> Result<Option<CacheTableEntry>, StorageError> {
    if let Some(entry) = cache_table.get(CacheTableKey::Default(pathid))? {
        return Ok(Some(entry.value().parse()?));
    }

    Ok(None)
}

/// Get the default entry for the given pathid.
///
/// Fail if the file cannot be found or if it is a directory.
fn default_file_entry_or_err(
    cache_table: &impl redb::ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
    pathid: PathId,
) -> Result<FileTableEntry, StorageError> {
    default_entry(cache_table, pathid)?
        .ok_or(StorageError::NotFound)?
        .expect_file()
}

/// Insert or update a directory entry in the cache table.
///
/// The presence of this entry is what marks a directory as existing,
/// both in the cache and in the tree.
fn write_dir_mtime(
    cache_table: &mut redb::Table<'_, CacheTableKey, Holder<CacheTableEntry>>,
    tree: &mut WritableOpenTree,
    pathid: PathId,
    mtime: UnixTime,
) -> Result<(), StorageError> {
    if tree.insert_and_incref(
        pathid,
        cache_table,
        CacheTableKey::Default(pathid),
        Holder::with_content(CacheTableEntry::Dir(DirtableEntry { mtime }))?,
    )? {
        // Update the parent directory mtime since we're adding a directory to it.
        if let Some(parent_pathid) = tree.parent(pathid)? {
            write_dir_mtime(cache_table, tree, parent_pathid, UnixTime::now())?;
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
    cache_table: &impl ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
    pathid: PathId,
) -> Result<Option<PathAssignment>, StorageError> {
    match cache_table.get(CacheTableKey::Default(pathid))? {
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
    cache_table: &impl ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
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
    cache_table: &impl ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FileMetadata;
    use crate::arena::blob::BlobExt;
    use crate::arena::db::ArenaDatabase;
    use crate::arena::dirty::DirtyReadOperations;
    use crate::arena::history::HistoryReadOperations;
    use crate::arena::notifier::Notification;
    use crate::arena::tree::{TreeExt, TreeLoc, TreeReadOperations};
    use crate::arena::types::{DirMetadata, HistoryTableEntry};
    use crate::utils::hash;
    use crate::{PathId, StorageError};
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use realize_types::{Arena, Hash, Path, Peer, UnixTime};
    use std::collections::HashSet;
    use std::sync::Arc;

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

    struct Fixture {
        arena: Arena,
        acache: Arc<ArenaCache>,
        db: Arc<ArenaDatabase>,
        _tempdir: TempDir,
    }
    impl Fixture {
        async fn setup_with_arena(arena: Arena) -> anyhow::Result<Fixture> {
            let _ = env_logger::try_init();
            let tempdir = TempDir::new()?;
            let child = tempdir.child(format!("{arena}-cache.db"));
            let blob_dir = tempdir.child(format!("{arena}/blobs"));
            if let Some(p) = child.parent() {
                std::fs::create_dir_all(p)?;
            }
            let acache = ArenaCache::for_testing_single_arena(arena, blob_dir.path())?;
            let db = Arc::clone(&acache.db);
            Ok(Self {
                arena,
                acache,
                db,
                _tempdir: tempdir,
            })
        }

        fn dir_metadata(&self, loc: impl Into<CacheLoc>) -> anyhow::Result<DirMetadata> {
            Ok(self.acache.dir_metadata(loc)?)
        }

        fn add_to_cache<T>(&self, path: T, size: u64, mtime: UnixTime) -> anyhow::Result<()>
        where
            T: AsRef<Path>,
        {
            let path = path.as_ref();
            self.acache.update(
                test_peer(),
                Notification::Add {
                    arena: self.arena,
                    index: 1,
                    path: path.clone(),
                    mtime: mtime.clone(),
                    size,
                    hash: test_hash(),
                },
                None,
            )?;

            Ok(())
        }

        fn remove_from_cache<T>(&self, path: T) -> anyhow::Result<()>
        where
            T: AsRef<Path>,
        {
            let path = path.as_ref();
            self.acache.update(
                test_peer(),
                Notification::Remove {
                    arena: self.arena,
                    index: 1,
                    path: path.clone(),
                    old_hash: test_hash(),
                },
                None,
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
            txn.write_dirty()?.delete_range(0, 999)?;
            txn.commit()?;

            Ok(())
        }

        fn dirty_pathids(&self) -> Result<HashSet<PathId>, StorageError> {
            let txn = self.db.begin_read()?;
            let dirty = txn.read_dirty()?;

            dirty_pathids_in_txn(&dirty)
        }

        fn dirty_inodes(&self) -> Result<HashSet<Inode>, StorageError> {
            let txn = self.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            let cache = txn.read_cache()?;
            let mut start = 0;
            let mut ret = HashSet::new();
            while let Some((pathid, counter)) = dirty.next_dirty(start)? {
                ret.insert(cache.map_to_inode(pathid)?);
                start = counter + 1;
            }
            Ok(ret)
        }

        fn dirty_paths(&self) -> Result<HashSet<Path>, StorageError> {
            let pathids = self.dirty_pathids()?;
            let txn = self.db.begin_read()?;
            let tree = txn.read_tree()?;

            Ok(pathids
                .into_iter()
                .filter_map(|i| tree.backtrack(i).ok().flatten())
                .collect())
        }

        async fn setup() -> anyhow::Result<Fixture> {
            Self::setup_with_arena(test_arena()).await
        }
    }

    fn dirty_pathids_in_txn(
        dirty: &impl DirtyReadOperations,
    ) -> Result<HashSet<PathId>, StorageError> {
        let mut start = 0;
        let mut ret = HashSet::new();
        while let Some((pathid, counter)) = dirty.next_dirty(start)? {
            ret.insert(pathid);
            start = counter + 1;
        }

        Ok(ret)
    }

    #[tokio::test]
    async fn empty_cache_readdir() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;

        assert!(fixture.acache.readdir(fixture.db.tree().root())?.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn empty_cache_mtime() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;

        assert_ne!(
            UnixTime::ZERO,
            fixture.acache.dir_metadata(fixture.db.tree().root())?.mtime
        );

        Ok(())
    }

    #[tokio::test]
    async fn add_creates_directories() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let acache = &fixture.acache;
        let file_path = Path::parse("a/b/c.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&file_path, 100, mtime)?;

        let (pathid, metadata) = acache.lookup((fixture.db.tree().root(), "a"))?;
        assert!(
            matches!(metadata, crate::arena::types::Metadata::Dir(_)),
            "a"
        );

        let (_, metadata) = acache.lookup((pathid, "b"))?;
        assert!(
            matches!(metadata, crate::arena::types::Metadata::Dir(_)),
            "b"
        );

        Ok(())
    }

    #[tokio::test]
    async fn add_and_remove_mirrored_in_tree() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let file_path = Path::parse("a/b/c.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&file_path, 100, mtime)?;

        let a: PathId;
        let b: PathId;
        let c: PathId;
        {
            let txn = fixture.db.begin_read()?;
            let tree = txn.read_tree()?;
            let cache = txn.read_cache()?;
            assert_eq!(fixture.db.tree().root(), tree.root());

            a = cache.lookup(&tree, (fixture.db.tree().root(), "a"))?.0;
            b = cache.lookup(&tree, (a, "b"))?.0;
            c = cache.lookup(&tree, (b, "c.txt"))?.0;
            assert!(tree.pathid_exists(a)?);
            assert!(tree.pathid_exists(b)?);
            assert!(tree.pathid_exists(c)?);
            assert_eq!(Some(a), tree.lookup_pathid(tree.root(), "a")?);
            assert_eq!(Some(b), tree.lookup_pathid(a, "b")?);
            assert_eq!(Some(c), tree.lookup_pathid(b, "c.txt")?);
        }

        fixture.remove_from_cache(&file_path)?;

        {
            let txn = fixture.db.begin_read()?;
            let tree = txn.read_tree()?;
            let cache = txn.read_cache()?;

            assert!(cache.lookup(&tree, (b, "c.txt")).is_err());

            // The file is gone from tree, since this was the only
            // reference to it.
            assert!(!tree.pathid_exists(c)?);
            assert_eq!(None, tree.lookup_pathid(b, "c.txt")?);

            // The directories were not cleaned up, even though this
            // was the last file. They need to be rmdir'ed explicitly.
            assert!(tree.pathid_exists(a)?);
            assert!(tree.pathid_exists(b)?);
            assert_eq!(Some(a), tree.lookup_pathid(tree.root(), "a")?);
            assert_eq!(Some(b), tree.lookup_pathid(a, "b")?);
        }
        Ok(())
    }

    #[tokio::test]
    async fn add_updates_dir_mtime() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena).await?;
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
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let file_path = Path::parse("a/b/c.txt")?;

        fixture.add_to_cache(&file_path, 100, test_time())?;
        assert_eq!(HashSet::from([file_path.clone()]), fixture.dirty_paths()?);

        Ok(())
    }

    #[tokio::test]
    async fn replace_existing_file() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena).await?;
        let acache = &fixture.acache;
        let peer = test_peer();
        let file_path = Path::parse("file.txt")?;

        acache.update(
            peer,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 100,
                hash: test_hash(),
            },
            None,
        )?;
        acache.update(
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
            None,
        )?;

        let metadata = acache.file_metadata(&file_path)?;
        assert_eq!(metadata.size, 200);
        assert_eq!(metadata.mtime, later_time());

        Ok(())
    }

    #[tokio::test]
    async fn replace_marks_dirty() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena).await?;
        let acache = &fixture.acache;
        let peer = test_peer();
        let file_path = Path::parse("file.txt")?;

        acache.update(
            peer,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 100,
                hash: test_hash(),
            },
            None,
        )?;
        fixture.clear_dirty()?;

        acache.update(
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
            None,
        )?;
        assert_eq!(HashSet::from([file_path.clone()]), fixture.dirty_paths()?);

        Ok(())
    }

    #[tokio::test]
    async fn ignored_replace_does_not_mark_dirty() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena).await?;
        let acache = &fixture.acache;
        let peer = test_peer();
        let file_path = Path::parse("file.txt")?;

        acache.update(
            peer,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
            None,
        )?;
        fixture.clear_dirty()?;

        // Replace is ignored because old_hash != current hash.
        acache.update(
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
            None,
        )?;
        assert_eq!(HashSet::new(), fixture.dirty_paths()?);

        Ok(())
    }

    #[tokio::test]
    async fn ignore_duplicate_add() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let file_path = Path::parse("file.txt")?;

        fixture.add_to_cache(&file_path, 100, test_time())?;
        fixture.add_to_cache(&file_path, 200, test_time())?;

        let acache = &fixture.acache;
        let metadata = acache.file_metadata(&file_path)?;
        assert_eq!(metadata.size, 100);

        Ok(())
    }

    #[tokio::test]
    async fn ignore_replace_with_wrong_hash() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let acache = &fixture.acache;
        let file_path = Path::parse("file.txt")?;
        let peer = test_peer();

        acache.update(
            peer,
            Notification::Add {
                arena: test_arena(),
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
            None,
        )?;
        acache.update(
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
            None,
        )?;

        let metadata = acache.file_metadata(&file_path)?;
        assert_eq!(metadata.size, 100);

        Ok(())
    }

    #[tokio::test]
    async fn remove_removes_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let acache = &fixture.acache;
        let file_path = Path::parse("file.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&file_path, 100, mtime)?;
        let arena_root = fixture.db.tree().root();
        acache.lookup((arena_root, "file.txt"))?;
        fixture.remove_from_cache(&file_path)?;
        assert!(matches!(
            acache.lookup((arena_root, "file.txt")),
            Err(StorageError::NotFound)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn remove_marks_dirty() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let file_path = Path::parse("file.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&file_path, 100, mtime)?;
        let arena_root = fixture.db.tree().root();
        let txn = fixture.db.begin_read()?;
        let cache = txn.read_cache()?;
        let tree = txn.read_tree()?;
        let (pathid, _) = cache.lookup(&tree, (arena_root, "file.txt"))?;

        fixture.clear_dirty()?;
        fixture.remove_from_cache(&file_path)?;
        assert_eq!(HashSet::from([pathid]), fixture.dirty_pathids()?);

        Ok(())
    }

    #[tokio::test]
    async fn remove_updates_dir_mtime() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena).await?;
        let file_path = Path::parse("file.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&file_path, 100, mtime)?;
        let arena_root = fixture.db.tree().root();
        let dir_mtime = fixture.acache.dir_metadata(arena_root)?.mtime;
        fixture.remove_from_cache(&file_path)?;

        assert!(fixture.acache.dir_metadata(arena_root)?.mtime > dir_mtime);

        Ok(())
    }

    #[tokio::test]
    async fn remove_ignores_wrong_old_hash() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena).await?;
        let acache = &fixture.acache;
        let file_path = Path::parse("file.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&file_path, 100, mtime)?;
        acache.update(
            test_peer(),
            Notification::Remove {
                arena,
                index: 1,
                path: file_path.clone(),
                old_hash: Hash([2u8; 32]), // != test_hash()
            },
            None,
        )?;

        // File should still exist because wrong hash was provided
        let metadata = acache.file_metadata(&file_path)?;
        assert_eq!(metadata.size, 100);

        Ok(())
    }

    #[tokio::test]
    async fn remove_applied_to_tracked_version_immediately() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena).await?;
        let acache = &fixture.acache;
        let peer1 = Peer::from("peer1");
        let peer2 = Peer::from("peer2");
        let file_path = Path::parse("file.txt")?;

        acache.update(
            peer1,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
            None,
        )?;
        acache.update(
            peer2,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
            None,
        )?;

        assert!(acache.file_metadata(&file_path).is_ok());

        acache.update(
            peer1,
            Notification::Remove {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                old_hash: Hash([1u8; 32]),
            },
            None,
        )?;

        // The default version has been removed by this notification,
        // even though it's still available in peer2; this is
        // considered outdated.
        assert!(matches!(
            acache.file_metadata(&file_path),
            Err(StorageError::NotFound)
        ));
        assert!(matches!(
            acache.lookup((fixture.db.tree().root(), "file.txt")),
            Err(StorageError::NotFound)
        ));

        // PathId should still be resolvable, because some peers still have it.
        let txn = fixture.db.begin_read()?;
        let tree = txn.read_tree()?;
        assert!(tree.resolve(&file_path)?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn drop_not_applied_to_tracked_version() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena).await?;
        let acache = &fixture.acache;
        let peer1 = Peer::from("peer1");
        let peer2 = Peer::from("peer2");
        let file_path = Path::parse("file.txt")?;

        acache.update(
            peer1,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
            None,
        )?;
        acache.update(
            peer2,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
            None,
        )?;

        assert!(acache.file_metadata(&file_path).is_ok());

        acache.update(
            peer1,
            Notification::Drop {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                old_hash: Hash([1u8; 32]),
            },
            None,
        )?;

        // The default version has not been removed by this notification,
        // since this is a Drop, not a Remove.
        assert!(acache.file_metadata(&file_path).is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn lookup_finds_entry() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena).await?;
        let acache = &fixture.acache;
        let file_path = Path::parse("a/file.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&file_path, 100, mtime)?;

        // Lookup directory
        let (pathid, metadata) = acache.lookup((fixture.db.tree().root(), "a"))?;
        assert!(matches!(metadata, crate::arena::types::Metadata::Dir(_)));

        // Lookup file
        let (pathid, metadata) = acache.lookup((pathid, "file.txt"))?;
        assert!(matches!(metadata, crate::arena::types::Metadata::File(_)));

        let metadata = acache.file_metadata(pathid)?;
        assert_eq!(metadata.mtime, mtime);
        assert_eq!(metadata.size, 100);

        Ok(())
    }

    #[tokio::test]
    async fn lookup_returns_notfound_for_missing_entry() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let acache = &fixture.acache;

        assert!(matches!(
            acache.lookup((fixture.db.tree().root(), "nonexistent")),
            Err(StorageError::NotFound),
        ));

        Ok(())
    }

    #[tokio::test]
    async fn lookup_path_finds_entry() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena).await?;
        let acache = &fixture.acache;
        let path = Path::parse("a/b/c/file.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&path, 100, mtime)?;

        let metadata = acache.file_metadata(&path)?;
        assert_eq!(metadata.mtime, mtime);
        assert_eq!(metadata.size, 100);

        Ok(())
    }

    #[tokio::test]
    async fn readdir_returns_all_entries() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena).await?;
        let acache = &fixture.acache;
        let mtime = test_time();

        fixture.add_to_cache(&Path::parse("dir/file1.txt")?, 100, mtime)?;
        fixture.add_to_cache(&Path::parse("dir/file2.txt")?, 200, mtime)?;
        fixture.add_to_cache(&Path::parse("dir/subdir/file3.txt")?, 300, mtime)?;

        let arena_root = fixture.db.tree().root();
        let entries = acache.readdir(arena_root)?;
        assert_eq!(entries.len(), 1);
        let (name, _, metadata) = &entries[0];
        assert_eq!(name, "dir");
        match metadata {
            crate::arena::types::Metadata::Dir(_) => {}
            _ => panic!("Expected directory metadata"),
        }

        let (pathid, _) = acache.lookup((arena_root, "dir"))?;
        let entries = acache.readdir(pathid)?;
        assert_eq!(entries.len(), 3);

        let mut names: Vec<String> = entries.iter().map(|(name, _, _)| name.clone()).collect();
        names.sort();
        assert_eq!(names, vec!["file1.txt", "file2.txt", "subdir"]);

        // Verify metadata types
        for (name, _, metadata) in entries {
            match (name.as_str(), &metadata) {
                ("file1.txt" | "file2.txt", crate::arena::types::Metadata::File(_)) => {}
                ("subdir", crate::arena::types::Metadata::Dir(_)) => {}
                _ => panic!("Unexpected entry: {} with metadata {:?}", name, metadata),
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn get_file_metadata_tracks_hash_chain() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena).await?;
        let acache = &fixture.acache;

        let peer1 = Peer::from("peer1");
        let peer2 = Peer::from("peer2");
        let file_path = Path::parse("file.txt")?;

        acache.update(
            peer1,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
            None,
        )?;
        acache.update(
            peer2,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
            None,
        )?;
        acache.update(
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
            None,
        )?;

        let (pathid, _) = acache.lookup((fixture.db.tree().root(), "file.txt"))?;
        let metadata = acache.file_metadata(pathid)?;
        assert_eq!(metadata.size, 200);
        assert_eq!(metadata.mtime, later_time());

        Ok(())
    }

    #[tokio::test]
    async fn file_availability_deals_with_conflicting_adds() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let acache = &fixture.acache;

        let a = Peer::from("a");
        let b = Peer::from("b");
        let c = Peer::from("c");
        let arena = test_arena();
        let path = Path::parse("file.txt")?;

        acache.update(
            a,
            Notification::Add {
                arena: arena,
                index: 0,
                path: path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
            None,
        )?;
        acache.update(
            b,
            Notification::Add {
                arena: arena,
                index: 0,
                path: path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
            None,
        )?;
        acache.update(
            c,
            Notification::Add {
                arena: arena,
                index: 0,
                path: path.clone(),
                mtime: later_time(),
                size: 200,
                hash: Hash([2u8; 32]),
            },
            None,
        )?;
        let txn = fixture.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;

        let avail = cache
            .file_availability(&tree, &path, &Hash([1u8; 32]))?
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
                .file_availability(&tree, &path, &Hash([2u8; 32]))?
                .unwrap()
                .peers
        );

        assert_eq!(
            None,
            cache.file_availability(&tree, &path, &Hash([3u8; 32]))?
        );

        Ok(())
    }

    #[tokio::test]
    async fn file_availablility_deals_with_different_versions() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let acache = &fixture.acache;

        let a = Peer::from("a");
        let b = Peer::from("b");
        let c = Peer::from("c");
        let arena = test_arena();
        let path = Path::parse("file.txt")?;

        acache.update(
            a,
            Notification::Add {
                arena: arena,
                index: 0,
                path: path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
            None,
        )?;
        acache.update(
            b,
            Notification::Add {
                arena: arena,
                index: 0,
                path: path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
            None,
        )?;
        acache.update(
            c,
            Notification::Add {
                arena: arena,
                index: 0,
                path: path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
            None,
        )?;
        acache.update(
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
            None,
        )?;
        {
            let txn = fixture.db.begin_read()?;
            let cache = txn.read_cache()?;
            let tree = txn.read_tree()?;
            let entry = cache.file_entry(&tree, &path)?.unwrap();

            //  Replace with old_hash=1 means that hash=2 is the most
            //  recent one. This is the one chosen.
            assert_eq!(Hash([2u8; 32]), entry.hash);
            assert_eq!(200, entry.size);
            assert_eq!(later_time(), entry.mtime);
            assert_eq!(
                vec![b],
                cache
                    .file_availability(&tree, &path, &entry.hash)?
                    .unwrap()
                    .peers
            );
        }

        // We reconnect to c, which has yet another version. Following
        // the hash chain, Hash 3 is now the newest version.
        acache.update(
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
            None,
        )?;
        acache.update(
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
            None,
        )?;
        {
            let txn = fixture.db.begin_read()?;
            let cache = txn.read_cache()?;
            let tree = txn.read_tree()?;
            let entry = cache.file_entry(&txn.read_tree()?, &path)?.unwrap();

            //  Replace with old_hash=1 means that hash=2 is the most
            //  recent one. This is the one chosen.
            assert_eq!(Hash([3u8; 32]), entry.hash);
            assert_eq!(
                vec![c],
                cache
                    .file_availability(&tree, &path, &entry.hash)?
                    .unwrap()
                    .peers
            );
        }

        {
            let txn = fixture.db.begin_read()?;
            let cache = txn.read_cache()?;
            let tree = txn.read_tree()?;
            let entry = cache.file_entry(&txn.read_tree()?, &path)?.unwrap();

            assert_eq!(Hash([3u8; 32]), entry.hash);
            assert_eq!(
                vec![c],
                cache
                    .file_availability(&tree, &path, &entry.hash)?
                    .unwrap()
                    .peers
            );
        }

        // Later on, b joins the party
        acache.update(
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
            None,
        )?;

        {
            let txn = fixture.db.begin_read()?;
            let cache = txn.read_cache()?;
            let tree = txn.read_tree()?;
            let entry = cache.file_entry(&txn.read_tree()?, &path)?.unwrap();

            assert_eq!(Hash([3u8; 32]), entry.hash);
            assert_eq!(
                vec![b, c],
                cache
                    .file_availability(&tree, &path, &entry.hash)?
                    .unwrap()
                    .peers
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn file_availability_when_a_peer_goes_away() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let acache = &fixture.acache;

        let a = Peer::from("a");
        let b = Peer::from("b");
        let c = Peer::from("c");
        let arena = test_arena();
        let path = Path::parse("file.txt")?;

        acache.update(
            a,
            Notification::Add {
                arena: arena,
                index: 0,
                path: path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
            None,
        )?;
        acache.update(
            b,
            Notification::Add {
                arena: arena,
                index: 0,
                path: path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
            None,
        )?;
        acache.update(
            c,
            Notification::Add {
                arena: arena,
                index: 0,
                path: path.clone(),
                mtime: later_time(),
                size: 100,
                hash: Hash([2u8; 32]),
            },
            None,
        )?;
        acache.update(
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
            None,
        )?;
        let pathid: PathId;
        {
            let txn = fixture.db.begin_read()?;
            let cache = txn.read_cache()?;
            let tree = txn.read_tree()?;
            pathid = cache
                .lookup(&tree, (fixture.db.tree().root(), "file.txt"))?
                .0;
            let entry = cache.file_at_pathid(pathid)?.unwrap();

            assert_eq!(Hash([3u8; 32]), entry.hash);
            assert_eq!(
                vec![a],
                cache
                    .file_availability(&tree, pathid, &entry.hash)?
                    .unwrap()
                    .peers
            );
        }

        acache.update(a, Notification::CatchupStart(arena), None)?;
        acache.update(
            a,
            Notification::CatchupComplete {
                arena: arena,
                index: 0,
            },
            None,
        )?;
        // We've lost the single peer that had the tracked version.
        // This is handled as if A had reported that file as deleted.
        assert!(matches!(
            acache.file_metadata(pathid),
            Err(StorageError::NotFound)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn mark_and_delete_peer_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let acache = &fixture.acache;
        let arena = test_arena();
        let peer1 = Peer::from("1");
        let peer2 = Peer::from("2");
        let peer3 = Peer::from("3");
        let file1 = Path::parse("file1")?;
        let file2 = Path::parse("afile2")?;
        let file3 = Path::parse("file3")?;
        let file4 = Path::parse("file4")?;

        let mtime = test_time();

        acache.update(
            peer1,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file1.clone(),
                mtime: mtime.clone(),
                size: 10,
                hash: test_hash(),
            },
            None,
        )?;

        acache.update(
            peer1,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file2.clone(),
                mtime: mtime.clone(),
                size: 10,
                hash: test_hash(),
            },
            None,
        )?;
        acache.update(
            peer2,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file2.clone(),
                mtime: mtime.clone(),
                size: 10,
                hash: test_hash(),
            },
            None,
        )?;

        acache.update(
            peer1,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file1.clone(),
                mtime: mtime.clone(),
                size: 10,
                hash: test_hash(),
            },
            None,
        )?;
        acache.update(
            peer2,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file2.clone(),
                mtime: mtime.clone(),
                size: 10,
                hash: test_hash(),
            },
            None,
        )?;
        acache.update(
            peer3,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file3.clone(),
                mtime: mtime.clone(),
                size: 10,
                hash: test_hash(),
            },
            None,
        )?;

        acache.update(
            peer1,
            Notification::Add {
                arena: arena,
                index: 0,
                path: file4.clone(),
                mtime: mtime.clone(),
                size: 10,
                hash: test_hash(),
            },
            None,
        )?;

        // Simulate a catchup that only reports file2 and file4.
        acache.update(peer1, Notification::CatchupStart(arena), None)?;
        acache.update(
            peer1,
            Notification::Catchup {
                arena: arena,
                path: file2.clone(),
                size: 10,
                mtime: mtime.clone(),
                hash: test_hash(),
            },
            None,
        )?;
        acache.update(
            peer1,
            Notification::Catchup {
                arena: arena,
                path: file4.clone(),
                size: 10,
                mtime: mtime.clone(),
                hash: test_hash(),
            },
            None,
        )?;
        acache.update(
            peer1,
            Notification::CatchupComplete {
                arena: arena,
                index: 0,
            },
            None,
        )?;

        let txn = fixture.db.begin_read()?;
        let cache = txn.read_cache()?;
        let tree = txn.read_tree()?;

        // File1 should have been deleted, since it was only on peer1,
        assert!(cache.file_entry(&tree, &file1)?.is_none());

        assert_eq!(
            vec![peer1, peer2],
            cache
                .file_availability(&tree, &file2, &test_hash())?
                .unwrap()
                .peers
        );

        assert_eq!(
            vec![peer3],
            cache
                .file_availability(&tree, &file3, &test_hash())?
                .unwrap()
                .peers
        );

        assert_eq!(
            vec![peer1],
            cache
                .file_availability(&tree, &file4, &test_hash())?
                .unwrap()
                .peers
        );

        Ok(())
    }

    #[tokio::test]
    async fn blob_deleted_on_file_overwrite() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;

        // Create a file
        fixture.add_to_cache(&file_path, 100, test_time())?;

        // Open the file to create a blob
        acache.open_file(&file_path)?;
        assert!(fixture.has_blob(&file_path)?);

        acache.update(
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
            None,
        )?;

        // The version has changed, so the blob must have been deleted.
        assert!(!fixture.has_blob(&file_path)?);

        Ok(())
    }

    #[tokio::test]
    async fn blob_deleted_on_file_removal() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;
        let file_path = Path::parse("file.txt")?;

        // Create a file and open it to create the blob
        fixture.add_to_cache(&file_path, 100, test_time())?;
        acache.open_file(&file_path)?;
        assert!(fixture.has_blob(&file_path)?);

        fixture.remove_from_cache(&file_path)?;

        // the blob must be gone
        assert!(!fixture.has_blob(&file_path)?);

        Ok(())
    }

    #[tokio::test]
    async fn blob_deleted_on_catchup_removal() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let acache = &fixture.acache;
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;

        // Create a file and open it to create the blob
        fixture.add_to_cache(&file_path, 100, test_time())?;
        acache.open_file(&file_path)?;
        assert!(fixture.has_blob(&file_path)?);

        // Do a catchup that doesn't include this file (simulating file removal)
        acache.update(test_peer(), Notification::CatchupStart(arena), None)?;
        // Note: No Catchup notification for the file, so it will be deleted
        acache.update(
            test_peer(),
            Notification::CatchupComplete { arena, index: 0 },
            None,
        )?;

        // The blob must be gone
        assert!(!fixture.has_blob(&file_path)?);

        Ok(())
    }

    #[tokio::test]
    async fn unlink() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let acache = &fixture.acache;
        let file_path = Path::parse("file.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&file_path, 100, mtime)?;
        let arena_root = fixture.db.tree().root();

        let (inode, _) = acache.lookup((arena_root, "file.txt"))?;
        assert!(acache.file_metadata(inode).is_ok());

        fixture.clear_dirty()?;
        let dir_mtime_before = acache.dir_metadata(arena_root)?.mtime;

        acache.unlink((arena_root, "file.txt"))?;

        assert!(matches!(
            acache.lookup((arena_root, "file.txt")),
            Err(StorageError::NotFound)
        ));
        assert!(matches!(
            acache.file_metadata(inode),
            Err(StorageError::NotFound)
        ));

        assert_eq!(HashSet::from([inode]), fixture.dirty_inodes()?);

        assert!(acache.dir_metadata(arena_root)?.mtime > dir_mtime_before);

        let txn = fixture.db.begin_read()?;
        let history = txn.read_history()?;
        let (_, history_entry) = history.history(0..).last().unwrap().unwrap();
        assert_eq!(
            HistoryTableEntry::Remove(file_path, test_hash()),
            history_entry
        );

        Ok(())
    }

    #[tokio::test]
    async fn branch() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let dir_path = Path::parse("mydir")?;
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
        let source_pathid = tree.expect(&source_path)?;
        let dir_pathid = tree.expect(&dir_path)?;

        dirty.delete_range(0, 999)?; // clear dirty

        cache.branch(
            &mut tree,
            &mut blobs,
            &mut history,
            &mut dirty,
            source_pathid,
            (dir_pathid, "dest"),
        )?;

        let dest_pathid = tree.expect(&dest_path)?;
        assert_eq!(
            Some(dest_pathid),
            dirty.next_dirty(0)?.map(|(pathid, _)| pathid)
        );
        let (_, history_entry) = history.history(0..).last().unwrap().unwrap();
        assert_eq!(
            HistoryTableEntry::Branch(source_path.clone(), dest_path.clone(), hash.clone(), None),
            history_entry
        );

        // Dest file exists and can be downloaded from peer1, using source path
        assert_eq!(
            FileMetadata {
                size: 6,
                mtime: test_time(),
                hash: hash.clone(),
            },
            cache.file_metadata(&tree, dest_pathid)?
        );
        let avail = cache.file_availability(&tree, dest_pathid, &hash)?.unwrap();
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

        // Dest file exists and can now be downloaded normally from peer2
        assert_eq!(
            FileMetadata {
                size: 6,
                mtime: test_time(),
                hash: hash.clone(),
            },
            cache.file_metadata(&tree, dest_pathid)?
        );
        let avail = cache.file_availability(&tree, dest_pathid, &hash)?.unwrap();
        assert_eq!(dest_path, avail.path);
        assert_eq!(hash, avail.hash);
        assert_eq!(vec![peer2], avail.peers);

        Ok(())
    }

    #[tokio::test]
    async fn mkdir_creates_directory() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let dir_path = Path::parse("newdir")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut dirty = txn.write_dirty()?;

        // Clear dirty state
        dirty.delete_range(0, 999)?;

        let (pathid, metadata) = cache.mkdir(&mut tree, &dir_path)?;

        // Verify directory was created
        assert!(tree.pathid_exists(pathid)?);
        assert_eq!(
            Some(pathid),
            tree.lookup_pathid(fixture.db.tree().root(), "newdir")?
        );

        // Verify metadata
        assert!(!metadata.read_only);
        assert!(metadata.mtime > UnixTime::ZERO);

        // Verify directory entry exists in cache by checking dir_mtime
        let mtime = cache.dir_mtime(&tree, pathid)?;
        assert!(mtime > UnixTime::ZERO);

        // Verify parent directory mtime was updated
        let parent_mtime = cache.dir_mtime(&tree, fixture.db.tree().root())?;
        assert!(parent_mtime > UnixTime::ZERO);

        Ok(())
    }

    #[tokio::test]
    async fn mkdir_creates_nested_directories() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;

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
    async fn mkdir_fails_if_directory_already_exists() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let dir_path = Path::parse("existing_dir")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut dirty = txn.write_dirty()?;

        // Clear dirty state
        dirty.delete_range(0, 999)?;

        // Create directory first time
        let (pathid1, _) = cache.mkdir(&mut tree, &dir_path)?;
        assert!(tree.pathid_exists(pathid1)?);

        // Try to create same directory again
        let result = cache.mkdir(&mut tree, &dir_path);
        assert!(matches!(result, Err(StorageError::AlreadyExists)));

        Ok(())
    }

    #[tokio::test]
    async fn rmdir_removes_empty_directory() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let dir_path = Path::parse("empty_dir")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut dirty = txn.write_dirty()?;

        // Clear dirty state
        dirty.delete_range(0, 999)?;

        // Create directory
        let (pathid, _) = cache.mkdir(&mut tree, &dir_path)?;
        assert!(tree.pathid_exists(pathid)?);

        // Remove directory
        cache.rmdir(&mut tree, &dir_path)?;

        // Verify directory is gone
        assert!(!tree.pathid_exists(pathid)?);
        assert_eq!(
            None,
            tree.lookup_pathid(fixture.db.tree().root(), "empty_dir")?
        );

        // Verify cache entry is gone
        assert!(cache.file_at_pathid(pathid)?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn rmdir_fails_if_directory_not_empty() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let dir_path = Path::parse("non_empty_dir")?;
        let file_path = Path::parse("non_empty_dir/file.txt")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;

        // Clear dirty state
        dirty.delete_range(0, 999)?;

        // Create directory
        let (dir_pathid, _) = cache.mkdir(&mut tree, &dir_path)?;

        // Add a file to the directory
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

        // Try to remove non-empty directory
        let result = cache.rmdir(&mut tree, &dir_path);
        assert!(matches!(result, Err(StorageError::DirectoryNotEmpty)));

        // Verify directory still exists
        assert!(tree.pathid_exists(dir_pathid)?);

        Ok(())
    }

    #[tokio::test]
    async fn rmdir_fails_if_not_a_directory() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let file_path = Path::parse("file.txt")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;

        // Clear dirty state
        dirty.delete_range(0, 999)?;

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
        assert!(tree.pathid_exists(file_pathid)?);

        Ok(())
    }

    #[tokio::test]
    async fn rmdir_fails_if_directory_not_found() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let nonexistent_path = Path::parse("nonexistent_dir")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut dirty = txn.write_dirty()?;

        // Clear dirty state
        dirty.delete_range(0, 999)?;

        // Try to remove non-existent directory
        let result = cache.rmdir(&mut tree, &nonexistent_path);
        assert!(matches!(result, Err(StorageError::NotFound)));

        Ok(())
    }

    #[tokio::test]
    async fn rmdir_updates_parent_mtime() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let parent_path = Path::parse("parent")?;
        let child_path = Path::parse("parent/child")?;

        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let mut cache = txn.write_cache()?;
        let mut dirty = txn.write_dirty()?;

        // Clear dirty state
        dirty.delete_range(0, 999)?;

        // Create parent and child directories
        let (parent_pathid, _) = cache.mkdir(&mut tree, &parent_path)?;
        let (child_pathid, _) = cache.mkdir(&mut tree, &child_path)?;

        // Get parent mtime before removal
        let parent_mtime_before = cache.dir_mtime(&tree, parent_pathid)?;

        // Remove child directory
        cache.rmdir(&mut tree, &child_path)?;

        // Verify parent mtime was updated (should be >= since operations can be fast)
        let parent_mtime_after = cache.dir_mtime(&tree, parent_pathid)?;
        assert!(parent_mtime_after >= parent_mtime_before);

        // Verify child is gone
        assert!(!tree.pathid_exists(child_pathid)?);

        Ok(())
    }

    #[tokio::test]
    async fn dir_or_file_metadata() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
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
            crate::arena::types::Metadata::File(meta) => {
                assert_eq!(meta.size, 1024);
                assert_eq!(meta.mtime, test_time());
                assert_eq!(meta.hash, test_hash());
            }
            crate::arena::types::Metadata::Dir(_) => {
                panic!("Expected file metadata, got directory metadata");
            }
        }

        // Test directory metadata
        let dir_metadata = cache.metadata(&tree, dir_pathid)?;
        match dir_metadata {
            crate::arena::types::Metadata::Dir(meta) => {
                assert!(!meta.read_only); // Arena directories are writable
                assert!(meta.mtime >= test_time());
            }
            crate::arena::types::Metadata::File(_) => {
                panic!("Expected directory metadata, got file metadata");
            }
        }

        // Test error cases
        let nonexistent_pathid = PathId(99999);
        let result = cache.metadata(&tree, nonexistent_pathid);
        assert!(matches!(result, Err(StorageError::NotFound)));

        Ok(())
    }

    #[tokio::test]
    async fn dir_or_file_metadata_acache() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
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
        let file_metadata = fixture.acache.metadata(&file_path)?;
        match file_metadata {
            crate::arena::types::Metadata::File(meta) => {
                assert_eq!(meta.size, 1024);
                assert_eq!(meta.mtime, test_time());
                assert_eq!(meta.hash, test_hash());
            }
            crate::arena::types::Metadata::Dir(_) => {
                panic!("Expected file metadata, got directory metadata");
            }
        }

        // Test directory metadata through ArenaCache
        let dir_metadata = fixture.acache.metadata(&dir_path)?;
        match dir_metadata {
            crate::arena::types::Metadata::Dir(meta) => {
                assert!(!meta.read_only); // Arena directories are writable
                assert!(meta.mtime >= test_time());
            }
            crate::arena::types::Metadata::File(_) => {
                panic!("Expected directory metadata, got file metadata");
            }
        }

        // Test error cases
        let nonexistent_pathid = PathId(99999);
        let result = fixture.acache.metadata(nonexistent_pathid);
        assert!(matches!(result, Err(StorageError::NotFound)));

        Ok(())
    }

    #[tokio::test]
    async fn rename_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
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
        dirty.delete_range(0, 999)?;
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
        assert_eq!(tree.resolve(&source_path)?, dest_entry.branched_from);
        assert_eq!(hash, dest_entry.hash);
        assert_eq!(6, dest_entry.size);
        assert_eq!(test_time(), dest_entry.mtime);

        // both dest and source are dirty
        let dest_pathid = tree.expect(&dest_path)?;
        assert_eq!(
            HashSet::from([source_pathid, dest_pathid]),
            dirty_pathids_in_txn(&dirty)?
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
            HistoryTableEntry::Rename(source_path, dest_path, hash, None),
            history_entry
        );

        Ok(())
    }

    #[tokio::test]
    async fn rename_file_replace() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
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
        assert_eq!(tree.resolve(&source_path)?, dest_entry.branched_from);
        assert_eq!(source_hash, dest_entry.hash);
        assert_eq!(6, dest_entry.size);

        // history entry reports old hash
        let (_, history_entry) = history.history(0..).last().unwrap().unwrap();
        assert_eq!(
            HistoryTableEntry::Rename(source_path, dest_path, source_hash, Some(dest_hash)),
            history_entry
        );

        Ok(())
    }

    #[tokio::test]
    async fn rename_dir() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let source_dir = Path::parse("source_dir")?;
        let dest_dir = Path::parse("dest_dir")?;
        let file1_path = Path::parse("source_dir/file1.txt")?;
        let file2_path = Path::parse("source_dir/subdir/file2.txt")?;

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

        let source_dir_pathid = tree.expect(&source_dir)?;
        dirty.delete_range(0, 999)?;

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

        // After directory rename, the updated implementation should remove the source directory
        // and all of its subdirectories once their contents are moved to the destination
        log::debug!("check dir {source_dir:?} {:?}", tree.resolve(&source_dir));
        assert!(
            cache.metadata(&tree, &source_dir).is_err(),
            "Source directory should be removed after rename"
        );
        let dest_metadata = cache.metadata(&tree, &dest_dir)?;
        assert!(
            matches!(dest_metadata, crate::arena::types::Metadata::Dir(_)),
            "Destination directory should exist after rename"
        );

        // Verify files moved correctly to destination directory
        let moved_file1 = Path::parse("dest_dir/file1.txt")?;
        let moved_file2 = Path::parse("dest_dir/subdir/file2.txt")?;

        let file1_entry = cache.file_entry_or_err(&tree, &moved_file1)?;
        assert_eq!(hash1, file1_entry.hash);
        assert_eq!(5, file1_entry.size);
        assert_eq!(test_time(), file1_entry.mtime);

        let file2_entry = cache.file_entry_or_err(&tree, &moved_file2)?;
        assert_eq!(hash2, file2_entry.hash);
        assert_eq!(5, file2_entry.size);
        assert_eq!(test_time(), file2_entry.mtime);

        // Original file paths should not exist (since source directory tree is removed)
        assert!(
            cache.file_entry(&tree, &file1_path)?.is_none(),
            "Original file1 path should not exist after directory rename"
        );
        assert!(
            cache.file_entry(&tree, &file2_path)?.is_none(),
            "Original file2 path should not exist after directory rename"
        );

        // Source subdirectory should also be removed
        let source_subdir = Path::parse("source_dir/subdir")?;
        assert!(
            cache.metadata(&tree, &source_subdir).is_err(),
            "Source subdirectory should be removed after rename"
        );

        // Check that files are marked dirty
        let dest_dir_pathid = tree.expect(&dest_dir)?;
        let dirty_pathids = dirty_pathids_in_txn(&dirty)?;
        assert!(dirty_pathids.contains(&tree.resolve(&file1_path)?.unwrap()));
        assert!(dirty_pathids.contains(&tree.resolve(&file2_path)?.unwrap()));
        assert!(dirty_pathids.contains(&tree.resolve(&moved_file1)?.unwrap()));
        assert!(dirty_pathids.contains(&tree.resolve(&moved_file2)?.unwrap()));

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

        let history_entries: Vec<_> = history
            .history(0..)
            .map(|res| res.map(|(_, val)| val))
            .collect::<Result<Vec<_>, _>>()?;
        assert_unordered::assert_eq_unordered!(
            vec![
                HistoryTableEntry::Rename(file1_path, moved_file1, hash1, None),
                HistoryTableEntry::Rename(file2_path, moved_file2, hash2, None)
            ],
            history_entries
        );

        Ok(())
    }

    #[tokio::test]
    async fn rename_dir_replace() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
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
            cache.metadata(&tree, &source_dir).is_err(),
            "Source directory should be removed after rename"
        );
        let dest_metadata = cache.metadata(&tree, &dest_dir)?;
        assert!(
            matches!(dest_metadata, crate::arena::types::Metadata::Dir(_)),
            "Destination directory should exist after rename"
        );

        Ok(())
    }

    #[tokio::test]
    async fn rename_dir_nonempty_dest() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
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
}
