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
use crate::utils::holder::Holder;
use crate::{Blob, LocalAvailability, PathAssignment};
use crate::{PathId, StorageError};
use realize_types::{Arena, Hash, Path, Peer, UnixTime};
use redb::{ReadableTable, Table};
use std::sync::Arc;

// TreeLoc is necessary to call ArenaCache methods
pub(crate) use super::tree::TreeLoc;

/// Read operations for cache. See also [CacheExt].
pub(crate) trait CacheReadOperations {
    /// Lookup a specific location.
    ///
    /// The location is usually either a path or a (pathid, name)
    /// pair.
    fn lookup<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<(PathId, PathAssignment), StorageError>;

    /// Get directory modification time for the given pathid.
    fn dir_mtime<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<UnixTime, StorageError>;

    /// Get remote file availability information for the given pathid and version.
    fn file_availability(
        &self,
        pathid: PathId,
        hash: &Hash,
    ) -> Result<Option<FileAvailability>, StorageError>;

    /// Read directory contents for the given pathid.
    fn readdir<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> impl Iterator<Item = Result<(String, PathId, PathAssignment), StorageError>>;

    /// Get the default file entry for the given pathid.
    fn get_at_pathid(&self, pathid: PathId) -> Result<Option<FileTableEntry>, StorageError>;

    /// Get the default file entry for the given pathid; fail if the entry
    /// cannot be found or if it is a directory.
    #[allow(dead_code)]
    fn get_at_pathid_or_err(&self, pathid: PathId) -> Result<FileTableEntry, StorageError>;
}

/// A cache open for reading with a read transaction.
pub(crate) struct ReadableOpenCache<T>
where
    T: ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
{
    table: T,
    arena: Arena,
}

impl<T> ReadableOpenCache<T>
where
    T: ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
{
    pub(crate) fn new(table: T, arena: Arena) -> Self {
        Self { table, arena }
    }
}

impl<T> CacheReadOperations for ReadableOpenCache<T>
where
    T: ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
{
    fn lookup<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<(PathId, PathAssignment), StorageError> {
        lookup(&self.table, tree, loc)
    }

    fn dir_mtime<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<UnixTime, StorageError> {
        dir_mtime(&self.table, tree, loc)
    }

    fn file_availability(
        &self,
        pathid: PathId,
        hash: &Hash,
    ) -> Result<Option<FileAvailability>, StorageError> {
        file_availability(&self.table, pathid, self.arena, hash)
    }

    fn readdir<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> impl Iterator<Item = Result<(String, PathId, PathAssignment), StorageError>> {
        ReadDirIterator::new(&self.table, tree, loc)
    }

    fn get_at_pathid(&self, pathid: PathId) -> Result<Option<FileTableEntry>, StorageError> {
        get_default_entry(&self.table, pathid)
    }

    fn get_at_pathid_or_err(&self, pathid: PathId) -> Result<FileTableEntry, StorageError> {
        get_default_entry_or_err(&self.table, pathid)
    }
}

impl<'a> CacheReadOperations for WritableOpenCache<'a> {
    fn lookup<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<(PathId, PathAssignment), StorageError> {
        lookup(&self.table, tree, loc)
    }

    fn dir_mtime<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<UnixTime, StorageError> {
        dir_mtime(&self.table, tree, loc)
    }

    fn file_availability(
        &self,
        pathid: PathId,
        hash: &Hash,
    ) -> Result<Option<FileAvailability>, StorageError> {
        file_availability(&self.table, pathid, self.arena, hash)
    }

    fn readdir<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> impl Iterator<Item = Result<(String, PathId, PathAssignment), StorageError>> {
        ReadDirIterator::new(&self.table, tree, loc)
    }

    fn get_at_pathid(&self, pathid: PathId) -> Result<Option<FileTableEntry>, StorageError> {
        get_default_entry(&self.table, pathid)
    }

    fn get_at_pathid_or_err(&self, pathid: PathId) -> Result<FileTableEntry, StorageError> {
        get_default_entry_or_err(&self.table, pathid)
    }
}

/// Extend [CacheReadOperations] with convenience functions for working
/// with paths and tree locations.
pub(crate) trait CacheExt {
    /// Get a file entry.
    ///
    /// Return None if the file is not found or if it is a directory.
    #[allow(dead_code)]
    fn get<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<Option<FileTableEntry>, StorageError>;

    /// Get a file entry, failing if the file is not found or if it is a directory.
    #[allow(dead_code)]
    fn get_or_err<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<FileTableEntry, StorageError>;

    fn file_metadata<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<FileMetadata, StorageError>;
}

impl<T: CacheReadOperations> CacheExt for T {
    fn file_metadata<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<FileMetadata, StorageError> {
        let FileTableEntry {
            size, mtime, hash, ..
        } = self.get(tree, loc)?.ok_or(StorageError::NotFound)?;

        Ok(FileMetadata { size, mtime, hash })
    }

    fn get<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<Option<FileTableEntry>, StorageError> {
        if let Some(pathid) = tree.resolve(loc)? {
            self.get_at_pathid(pathid)
        } else {
            Ok(None)
        }
    }
    fn get_or_err<'b, L: Into<TreeLoc<'b>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<FileTableEntry, StorageError> {
        self.get_at_pathid_or_err(tree.expect(loc)?)
    }
}

/// A cache open for writing with a write transaction.
pub(crate) struct WritableOpenCache<'a> {
    table: Table<'a, CacheTableKey, Holder<'static, CacheTableEntry>>,
    pending_catchup_table: Table<'a, (&'static str, PathId), ()>,
    arena: Arena,
}

impl<'a> WritableOpenCache<'a> {
    pub(crate) fn new(
        table: Table<'a, CacheTableKey, Holder<CacheTableEntry>>,
        pending_catchup_table: Table<'a, (&'static str, PathId), ()>,
        arena: Arena,
    ) -> Self {
        Self {
            table,
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
        let file_entry = get_default_entry_or_err(&self.table, pathid)?;
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
        let e = get_default_entry_or_err(&self.table, pathid)?;
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
        let mut entry = self.get_at_pathid_or_err(source_pathid)?;
        entry.branched_from = Some(source_pathid);
        let dest_pathid = tree.setup(dest.borrow())?;
        let old_hash = self.get_at_pathid(dest_pathid)?.map(|e| e.hash);
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
        if get_file_entry(&self.table, file_pathid, Some(peer))?.is_none() {
            log::debug!("[{}]@{peer} Add \"{path}\" {hash} size={size}", self.arena);
            self.write_file_entry(tree, file_pathid, peer, &entry)?;
        }
        Ok(
            if get_file_entry(&self.table, file_pathid, None)?.is_none() {
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
            if let Some(e) = get_file_entry(&self.table, file_pathid, Some(peer))?
                && e.hash == *old_hash
            {
                log::debug!(
                    "[{}]@{peer} \"{path}\" {hash} size={size} replaces {old_hash}",
                    self.arena
                );
                self.write_file_entry(tree, file_pathid, peer, &entry)?;
            }
            if let Some(old_entry) = get_file_entry(&self.table, file_pathid, None)?
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
        if let Some(e) = get_file_entry(&self.table, file_pathid, None)?
            && e.hash != hash
        {
            self.notify_dropped_or_removed(tree, blobs, dirty, peer, &path, &e.hash, false)?;
        }
        self.write_file_entry(tree, file_pathid, peer, &entry)?;
        Ok(
            if !get_file_entry(&self.table, file_pathid, None)?.is_some() {
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
            if let Some(e) = get_file_entry(&self.table, pathid, Some(peer))?
                && e.hash == *old_hash
            {
                log::debug!(
                    "[{}]@{peer} Remove \"{path}\" pathid {pathid} {old_hash}",
                    self.arena
                );

                self.rm_peer_file_entry(tree, pathid, peer)?;
            }
            if !dropped {
                if let Some(e) = get_file_entry(&self.table, pathid, None)?
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
            if let Some(entry) = self.get_at_pathid(pathid)? {
                if self.file_availability(pathid, &entry.hash)?.is_none() {
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
) -> Result<(PathId, PathAssignment), StorageError> {
    if let Some(pathid) = tree.resolve(loc)? {
        if let Some(assignment) = pathid_assignment(cache_table, pathid)? {
            return Ok((pathid, assignment));
        }
    }

    Err(StorageError::NotFound)
}

/// Get directory modification time for the given pathid.
fn dir_mtime<'b, L: Into<TreeLoc<'b>>>(
    cache_table: &impl ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
    tree: &impl TreeReadOperations,
    loc: L,
) -> Result<UnixTime, StorageError> {
    let pathid = tree.expect(loc)?;
    let e = cache_table
        .get(CacheTableKey::Default(pathid))?
        .ok_or(StorageError::NotFound)?;

    match e.value().parse()? {
        CacheTableEntry::Dir(dir_entry) => Ok(dir_entry.mtime),
        CacheTableEntry::File(_) => Err(StorageError::NotADirectory),
    }
}

/// Get file availability information for the given pathid.
fn file_availability(
    cache_table: &impl ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
    pathid: PathId,
    arena: Arena,
    hash: &Hash,
) -> Result<Option<FileAvailability>, StorageError> {
    let mut avail = None;
    let mut next = Some(pathid);
    while let Some(pathid) = next
        && avail.is_none()
    {
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
                Ok((pathid, PathAssignment::Directory)) => tree.readdir_pathid(pathid),
                Ok(_) => tree::ReadDirIterator::failed(StorageError::NotADirectory),
            },
        }
    }
}

impl<'a, 'b, T> Iterator for ReadDirIterator<'a, 'b, T>
where
    T: ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
{
    type Item = Result<(String, PathId, PathAssignment), StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(entry) = self.iter.next() {
            match entry {
                Err(err) => return Some(Err(err)),
                Ok((name, pathid)) => {
                    match pathid_assignment(self.table, pathid) {
                        Err(err) => return Some(Err(err)),
                        Ok(Some(assignment)) => return Some(Ok((name, pathid, assignment))),
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

    pub(crate) fn lookup<'b, L: Into<TreeLoc<'b>>>(
        &self,
        loc: L,
    ) -> Result<(PathId, PathAssignment), StorageError> {
        let txn = self.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        cache.lookup(&tree, loc)
    }

    pub(crate) fn expect<'a, L: Into<TreeLoc<'a>>>(&self, loc: L) -> Result<PathId, StorageError> {
        let txn = self.db.begin_read()?;
        let tree = txn.read_tree()?;

        tree.expect(loc)
    }

    pub(crate) fn resolve<'a, L: Into<TreeLoc<'a>>>(
        &self,
        loc: L,
    ) -> Result<Option<PathId>, StorageError> {
        let txn = self.db.begin_read()?;
        let tree = txn.read_tree()?;

        tree.resolve(loc)
    }

    pub(crate) fn file_metadata<'b, L: Into<TreeLoc<'b>>>(
        &self,
        loc: L,
    ) -> Result<FileMetadata, StorageError> {
        let txn = self.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        cache.file_metadata(&tree, loc)
    }

    pub(crate) fn dir_metadata<'b, L: Into<TreeLoc<'b>>>(
        &self,
        loc: L,
    ) -> Result<DirMetadata, StorageError> {
        let txn = self.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        Ok(DirMetadata {
            read_only: false,
            mtime: cache.dir_mtime(&tree, loc)?,
        })
    }

    pub(crate) fn readdir<'b, L: Into<TreeLoc<'b>>>(
        &self,
        loc: L,
    ) -> Result<Vec<(String, PathId, PathAssignment)>, StorageError> {
        let txn = self.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        cache.readdir(&tree, loc).collect()
    }

    pub(crate) fn peer_progress(&self, peer: Peer) -> Result<Option<Progress>, StorageError> {
        let txn = self.db.begin_read()?;
        let peers = txn.read_peers()?;
        peers.progress(peer)
    }

    pub(crate) fn unlink<'b, L: Into<TreeLoc<'b>>>(&self, loc: L) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let mut blobs = txn.write_blobs()?;
            let mut history = txn.write_history()?;
            let mut dirty = txn.write_dirty()?;
            let mut cache = txn.write_cache()?;

            cache.unlink(&mut tree, &mut blobs, &mut history, &mut dirty, loc)?;
        }
        txn.commit()?;

        Ok(())
    }

    pub(crate) fn branch<'l1, 'l2, L1: Into<TreeLoc<'l1>>, L2: Into<TreeLoc<'l2>>>(
        &self,
        source: L1,
        dest: L2,
    ) -> Result<(PathId, FileMetadata), StorageError> {
        let txn = self.db.begin_write()?;
        let result = {
            let mut tree = txn.write_tree()?;
            let mut blobs = txn.write_blobs()?;
            let mut history = txn.write_history()?;
            let mut dirty = txn.write_dirty()?;
            let mut cache = txn.write_cache()?;

            cache.branch(
                &mut tree,
                &mut blobs,
                &mut history,
                &mut dirty,
                source,
                dest,
            )?
        };
        txn.commit()?;

        Ok(result)
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
    pub(crate) fn open_file<'b, L: Into<TreeLoc<'b>>>(&self, loc: L) -> Result<Blob, StorageError> {
        // Optimistically, try a read transaction to check whether the
        // blob is there. The pathid is kept between transaction,
        // since it's guaranteed not to change.
        let pathid;
        {
            let txn = self.db.begin_read()?;
            let tree = txn.read_tree()?;
            pathid = tree.expect(loc)?;
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

    pub(crate) fn local_availability<'b, L: Into<TreeLoc<'b>>>(
        &self,
        loc: L,
    ) -> Result<LocalAvailability, StorageError> {
        let txn = self.db.begin_read()?;
        let tree = txn.read_tree()?;
        let blobs = txn.read_blobs()?;
        blobs.local_availability(&tree, loc)
    }

    /// Create a directory at the given path.
    pub(crate) fn mkdir<'b, L: Into<TreeLoc<'b>>>(
        &self,
        loc: L,
    ) -> Result<(PathId, DirMetadata), StorageError> {
        let txn = self.db.begin_write()?;
        let result = {
            let mut tree = txn.write_tree()?;
            let mut cache = txn.write_cache()?;

            cache.mkdir(&mut tree, loc)
        };
        txn.commit()?;

        result
    }

    /// Remove an empty directory at the given path.
    pub(crate) fn rmdir<'b, L: Into<TreeLoc<'b>>>(&self, loc: L) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let mut cache = txn.write_cache()?;

            cache.rmdir(&mut tree, loc)?;
        }
        txn.commit()?;

        Ok(())
    }
}

/// Get a [FileTableEntry] for a specific peer.
fn get_file_entry(
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
fn get_default_entry(
    cache_table: &impl redb::ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
    pathid: PathId,
) -> Result<Option<FileTableEntry>, StorageError> {
    if let Some(entry) = cache_table.get(CacheTableKey::Default(pathid))? {
        if let CacheTableEntry::File(entry) = entry.value().parse()? {
            return Ok(Some(entry));
        }
    }

    Ok(None)
}

/// Get the default entry for the given pathid.
///
/// Fail if the file cannot be found or if it is a directory.
fn get_default_entry_or_err(
    cache_table: &impl redb::ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
    pathid: PathId,
) -> Result<FileTableEntry, StorageError> {
    cache_table
        .get(CacheTableKey::Default(pathid))?
        .ok_or(StorageError::NotFound)?
        .value()
        .parse()?
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

#[cfg(test)]
mod tests {
    use super::ArenaCache;
    use crate::FileMetadata;
    use crate::arena::arena_cache::CacheExt;
    use crate::arena::arena_cache::CacheReadOperations;
    use crate::arena::blob::BlobExt;
    use crate::arena::db::ArenaDatabase;
    use crate::arena::dirty::DirtyReadOperations;
    use crate::arena::history::HistoryReadOperations;
    use crate::arena::notifier::Notification;
    use crate::arena::tree::TreeExt;
    use crate::arena::tree::TreeLoc;
    use crate::arena::tree::TreeReadOperations;
    use crate::arena::types::DirMetadata;
    use crate::arena::types::HistoryTableEntry;
    use crate::utils::hash;
    use crate::{PathAssignment, PathId, StorageError};
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

        fn dir_metadata<T>(&self, path: T) -> anyhow::Result<DirMetadata>
        where
            T: AsRef<Path>,
        {
            let path = path.as_ref();
            let pathid = self.acache.expect(path)?;
            Ok(self.acache.dir_metadata(pathid)?)
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
            let mut start = 0;
            let mut ret = HashSet::new();
            while let Some((pathid, counter)) = dirty.next_dirty(start)? {
                ret.insert(pathid);
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

        let (pathid, assignment) = acache.lookup((fixture.db.tree().root(), "a"))?;
        assert_eq!(assignment, PathAssignment::Directory, "a");

        let (_, assignment) = acache.lookup((pathid, "b"))?;
        assert_eq!(assignment, PathAssignment::Directory, "b");

        Ok(())
    }

    #[tokio::test]
    async fn add_and_remove_mirrored_in_tree() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let acache = &fixture.acache;
        let file_path = Path::parse("a/b/c.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&file_path, 100, mtime)?;

        let (a, _) = acache.lookup((fixture.db.tree().root(), "a"))?;
        let (b, _) = acache.lookup((a, "b"))?;
        let (c, _) = acache.lookup((b, "c.txt"))?;

        {
            let txn = fixture.db.begin_read()?;
            let tree = txn.read_tree()?;
            assert_eq!(fixture.db.tree().root(), tree.root());
            assert!(tree.pathid_exists(a)?);
            assert!(tree.pathid_exists(b)?);
            assert!(tree.pathid_exists(c)?);
            assert_eq!(Some(a), tree.lookup_pathid(tree.root(), "a")?);
            assert_eq!(Some(b), tree.lookup_pathid(a, "b")?);
            assert_eq!(Some(c), tree.lookup_pathid(b, "c.txt")?);
        }

        fixture.remove_from_cache(&file_path)?;

        assert!(acache.lookup((b, "c.txt")).is_err());
        {
            let txn = fixture.db.begin_read()?;
            let tree = txn.read_tree()?;

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

        let pathid = acache.expect(&file_path)?;
        let metadata = acache.file_metadata(pathid)?;
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
        let pathid = acache.expect(&file_path)?;
        let metadata = acache.file_metadata(pathid)?;
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

        let pathid = acache.expect(&file_path)?;
        let metadata = acache.file_metadata(pathid)?;
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
        let acache = &fixture.acache;
        let file_path = Path::parse("file.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&file_path, 100, mtime)?;
        let arena_root = fixture.db.tree().root();
        let (pathid, _) = acache.lookup((arena_root, "file.txt"))?;

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
        let pathid = acache.expect(&file_path)?;
        let metadata = acache.file_metadata(pathid)?;
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

        let pathid = acache.expect(&file_path)?;
        assert!(acache.file_metadata(pathid).is_ok());

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
            acache.file_metadata(pathid),
            Err(StorageError::NotFound)
        ));
        assert!(matches!(
            acache.lookup((fixture.db.tree().root(), "file.txt")),
            Err(StorageError::NotFound)
        ));

        // PathId should still be resolvable, because some peers still have it.
        assert_eq!(Some(pathid), acache.resolve(&file_path)?);

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

        let pathid = acache.expect(&file_path)?;
        assert!(acache.file_metadata(pathid).is_ok());

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
        assert!(acache.file_metadata(pathid).is_ok());

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
        let (pathid, assignment) = acache.lookup((fixture.db.tree().root(), "a"))?;
        assert_eq!(assignment, PathAssignment::Directory);

        // Lookup file
        let (pathid, assignment) = acache.lookup((pathid, "file.txt"))?;
        assert_eq!(assignment, PathAssignment::File);

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

        let pathid = acache.expect(&path)?;
        let metadata = acache.file_metadata(pathid)?;
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
        assert_unordered::assert_eq_unordered!(
            vec![("dir".to_string(), PathAssignment::Directory),],
            acache
                .readdir(arena_root)?
                .into_iter()
                .map(|(name, _, assignment)| (name, assignment))
                .collect::<Vec<_>>(),
        );

        let (pathid, _) = acache.lookup((arena_root, "dir"))?;
        assert_unordered::assert_eq_unordered!(
            vec![
                ("file1.txt".to_string(), PathAssignment::File),
                ("file2.txt".to_string(), PathAssignment::File),
                ("subdir".to_string(), PathAssignment::Directory),
            ],
            acache
                .readdir(pathid)?
                .into_iter()
                .map(|(name, _, assignment)| (name, assignment))
                .collect::<Vec<_>>(),
        );

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

        let pathid = tree.lookup(fixture.db.tree().root(), "file.txt")?.unwrap();
        let avail = cache.file_availability(pathid, &Hash([1u8; 32]))?.unwrap();
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
                .file_availability(pathid, &Hash([2u8; 32]))?
                .unwrap()
                .peers
        );

        assert_eq!(None, cache.file_availability(pathid, &Hash([3u8; 32]))?);

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
        let pathid = acache.expect(&path)?;
        {
            let txn = fixture.db.begin_read()?;
            let cache = txn.read_cache()?;
            let entry = cache.get_at_pathid(pathid)?.unwrap();

            //  Replace with old_hash=1 means that hash=2 is the most
            //  recent one. This is the one chosen.
            assert_eq!(Hash([2u8; 32]), entry.hash);
            assert_eq!(200, entry.size);
            assert_eq!(later_time(), entry.mtime);
            assert_eq!(
                vec![b],
                cache.file_availability(pathid, &entry.hash)?.unwrap().peers
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
            let entry = cache.get_at_pathid(pathid)?.unwrap();

            //  Replace with old_hash=1 means that hash=2 is the most
            //  recent one. This is the one chosen.
            assert_eq!(Hash([3u8; 32]), entry.hash);
            assert_eq!(
                vec![c],
                cache.file_availability(pathid, &entry.hash)?.unwrap().peers
            );
        }

        {
            let txn = fixture.db.begin_read()?;
            let cache = txn.read_cache()?;
            let entry = cache.get_at_pathid(pathid)?.unwrap();

            assert_eq!(Hash([3u8; 32]), entry.hash);
            assert_eq!(
                vec![c],
                cache.file_availability(pathid, &entry.hash)?.unwrap().peers
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
            let entry = cache.get_at_pathid(pathid)?.unwrap();

            assert_eq!(Hash([3u8; 32]), entry.hash);
            assert_eq!(
                vec![b, c],
                cache.file_availability(pathid, &entry.hash)?.unwrap().peers
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
        let (pathid, _) = acache.lookup((fixture.db.tree().root(), "file.txt"))?;
        {
            let txn = fixture.db.begin_read()?;
            let cache = txn.read_cache()?;
            let entry = cache.get_at_pathid(pathid)?.unwrap();

            assert_eq!(Hash([3u8; 32]), entry.hash);
            assert_eq!(
                vec![a],
                cache.file_availability(pathid, &entry.hash)?.unwrap().peers
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
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;

        // File1 should have been deleted, since it was only on peer1,
        assert!(cache.get(&tree, &file1)?.is_none());

        let file2_pathid = tree.resolve(&file2)?.unwrap();
        assert_eq!(
            vec![peer1, peer2],
            cache
                .file_availability(file2_pathid, &test_hash())?
                .unwrap()
                .peers
        );

        let file3_pathid = tree.resolve(&file3)?.unwrap();
        assert_eq!(
            vec![peer3],
            cache
                .file_availability(file3_pathid, &test_hash())?
                .unwrap()
                .peers
        );

        let file4_pathid = tree.resolve(&file4)?.unwrap();
        assert_eq!(
            vec![peer1],
            cache
                .file_availability(file4_pathid, &test_hash())?
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
        let pathid = acache.expect(&file_path)?;
        acache.open_file(pathid)?;
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
        let pathid = acache.expect(&file_path)?;
        acache.open_file(pathid)?;
        assert!(fixture.has_blob(&file_path)?);

        fixture.remove_from_cache(&file_path)?;

        // the blob must be gone
        assert!(!fixture.has_blob(pathid)?);

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
        let pathid = acache.expect(&file_path)?;
        acache.open_file(pathid)?;
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
        assert!(!fixture.has_blob(pathid)?);

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

        let (pathid, _) = acache.lookup((arena_root, "file.txt"))?;
        assert!(acache.file_metadata(pathid).is_ok());

        fixture.clear_dirty()?;
        let dir_mtime_before = acache.dir_metadata(arena_root)?.mtime;

        acache.unlink((arena_root, "file.txt"))?;

        assert!(matches!(
            acache.lookup((arena_root, "file.txt")),
            Err(StorageError::NotFound)
        ));
        assert!(matches!(
            acache.file_metadata(pathid),
            Err(StorageError::NotFound)
        ));

        assert_eq!(HashSet::from([pathid]), fixture.dirty_pathids()?);

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
        let avail = cache.file_availability(dest_pathid, &hash)?.unwrap();
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
        let avail = cache.file_availability(dest_pathid, &hash)?.unwrap();
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
        assert!(cache.get_at_pathid(pathid)?.is_none());

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
}
