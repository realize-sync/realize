use super::blob::{self, Blobstore};
use super::db::{ArenaDatabase, ArenaReadTransaction, ArenaWriteTransaction};
use super::history::HistoryReadOperations;
use super::index::RealIndex;
use super::mark::PathMarks;
use super::tree::{TreeExt, TreeLoc, TreeReadOperations, WritableOpenTree};
use super::types::{
    CacheTableEntry, CacheTableKey, DirtableEntry, FileAvailability, FileMetadata, FileTableEntry,
    HistoryTableEntry, IndexedFileTableEntry, LocalAvailability, LruQueueId, MarkTableEntry,
    PeerTableEntry,
};
use crate::arena::notifier::{Notification, Progress};
use crate::utils::fs_utils;
use crate::utils::holder::{ByteConversionError, Holder};
use crate::{Blob, InodeAssignment, Mark};
use crate::{Inode, StorageError};
use realize_types::{Arena, ByteRanges, Hash, Path, Peer, UnixTime};
use redb::{ReadableTable, Table};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

/// A per-arena cache of remote files.
///
/// This struct handles all cache operations for a specific arena.
/// It contains the arena's database and root inode.
pub(crate) struct ArenaCache {
    arena: Arena,
    arena_root: Inode,
    db: Arc<ArenaDatabase>,
    blobstore: Arc<Blobstore>,
    uuid: Uuid,
}

impl ArenaCache {
    #[cfg(test)]
    pub fn for_testing_single_arena(
        arena: realize_types::Arena,
        blob_dir: &std::path::Path,
    ) -> anyhow::Result<Arc<Self>> {
        ArenaCache::for_testing(
            arena,
            crate::InodeAllocator::new(
                crate::GlobalDatabase::new(crate::utils::redb_utils::in_memory()?)?,
                [arena],
            )?,
            blob_dir,
        )
    }

    #[cfg(test)]
    pub fn for_testing(
        arena: realize_types::Arena,
        allocator: Arc<crate::InodeAllocator>,
        blob_dir: &std::path::Path,
    ) -> anyhow::Result<Arc<Self>> {
        let db = ArenaDatabase::new(crate::utils::redb_utils::in_memory()?, arena, allocator)?;

        Ok(ArenaCache::new(arena, Arc::clone(&db), blob_dir)?)
    }

    #[cfg(test)]
    pub(crate) fn db(&self) -> Arc<ArenaDatabase> {
        Arc::clone(&self.db)
    }

    /// Create a new ArenaUnrealCacheBlocking from an arena, root inode, database, and blob directory.
    pub(crate) fn new(
        arena: Arena,
        db: Arc<ArenaDatabase>,
        blob_dir: &std::path::Path,
    ) -> Result<Arc<Self>, StorageError> {
        let blobstore = Blobstore::new(Arc::clone(&db), blob_dir)?;
        let uuid = load_or_assign_uuid(&db)?;
        Ok(Arc::new(Self {
            arena,
            arena_root: db.tree().root(),
            db,
            blobstore,
            uuid,
        }))
    }

    pub(crate) fn arena(&self) -> Arena {
        self.arena
    }

    pub(crate) fn arena_root(&self) -> Inode {
        self.arena_root
    }

    pub(crate) fn as_index(self: &Arc<Self>) -> Arc<dyn RealIndex> {
        self.clone()
    }

    pub(crate) fn lookup(
        &self,
        parent_inode: Inode,
        name: &str,
    ) -> Result<(Inode, InodeAssignment), StorageError> {
        let txn = self.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache_table = txn.cache_table()?;
        check_is_dir(&cache_table, parent_inode)?;
        if let Some(inode) = tree.lookup(parent_inode, name)? {
            if let Some(assignment) = inode_assignment(&cache_table, inode)? {
                return Ok((inode, assignment));
            }
        }

        Err(StorageError::NotFound)
    }

    pub(crate) fn expect<'a, L: Into<TreeLoc<'a>>>(&self, loc: L) -> Result<Inode, StorageError> {
        let txn = self.db.begin_read()?;
        let tree = txn.read_tree()?;

        tree.expect(loc)
    }

    pub(crate) fn resolve<'a, L: Into<TreeLoc<'a>>>(
        &self,
        loc: L,
    ) -> Result<Option<Inode>, StorageError> {
        let txn = self.db.begin_read()?;
        let tree = txn.read_tree()?;

        tree.resolve(loc)
    }

    pub(crate) fn file_metadata(&self, inode: Inode) -> Result<FileMetadata, StorageError> {
        let txn = self.db.begin_read()?;
        do_file_metadata(&txn, inode)
    }

    pub(crate) fn dir_mtime(&self, inode: Inode) -> Result<UnixTime, StorageError> {
        let txn = self.db.begin_read()?;
        let cache_table = txn.cache_table()?;
        match cache_table.get(CacheTableKey::Default(inode))? {
            Some(e) => match e.value().parse()? {
                CacheTableEntry::Dir(dir_entry) => Ok(dir_entry.mtime),
                CacheTableEntry::File(_) => Err(StorageError::NotADirectory),
            },
            None => {
                if inode == self.arena_root {
                    // When the filesystem is empty, the root dir might not
                    // have a mtime. This is not an error.
                    return Ok(UnixTime::ZERO);
                }
                Err(StorageError::NotFound)
            }
        }
    }

    pub(crate) fn file_availability(&self, inode: Inode) -> Result<FileAvailability, StorageError> {
        let txn = self.db.begin_read()?;

        do_file_availability(&txn, inode, self.arena)
    }

    pub(crate) fn readdir(
        &self,
        inode: Inode,
    ) -> Result<Vec<(String, Inode, InodeAssignment)>, StorageError> {
        let txn = self.db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache_table = txn.cache_table()?;
        check_is_dir(&cache_table, inode)?;

        // A Vec is not ideal, but redb iterators are bound to the
        // transaction; we must collect the results.
        let mut entries = vec![];
        for entry in tree.readdir(inode) {
            let (name, inode) = entry?;
            if let Some(assignment) = inode_assignment(&cache_table, inode)? {
                entries.push((name, inode, assignment));
            }
        }

        Ok(entries)
    }

    pub(crate) fn peer_progress(&self, peer: Peer) -> Result<Option<Progress>, StorageError> {
        let txn = self.db.begin_read()?;

        do_peer_progress(&txn, peer)
    }

    pub(crate) fn update(
        &self,
        peer: Peer,
        notification: Notification,
        index_root: Option<&std::path::Path>,
    ) -> Result<(), StorageError> {
        log::debug!("notification from {peer}: {notification:?}");
        // UnrealCacheBlocking::update, is responsible for dispatching properly
        assert_eq!(self.arena, notification.arena());

        let txn = self.db.begin_write()?;
        match notification {
            Notification::Add {
                index,
                path,
                mtime,
                size,
                hash,
                ..
            } => {
                do_update_last_seen_notification(&txn, peer, index)?;

                let mut cache_table = txn.cache_table()?;
                let mut tree = txn.write_tree()?;
                let (_, file_inode) = do_create_file(&mut cache_table, &mut tree, &path)?;
                let entry = FileTableEntry::new(path, size, mtime, hash);

                // add peer
                if get_file_entry(&cache_table, file_inode, Some(peer))?.is_none() {
                    self.do_write_file_entry(&mut cache_table, file_inode, peer, &entry)?;
                }

                // add default
                if get_file_entry(&cache_table, file_inode, None)?.is_none() {
                    self.do_write_default_file_entry(
                        &txn,
                        &mut tree,
                        &mut cache_table,
                        file_inode,
                        &entry,
                    )?;
                }
            }
            Notification::Replace {
                index,
                path,
                mtime,
                size,
                hash,
                old_hash,
                ..
            } => {
                do_update_last_seen_notification(&txn, peer, index)?;
                let mut cache_table = txn.cache_table()?;
                let mut tree = txn.write_tree()?;
                let (_, file_inode) = do_create_file(&mut cache_table, &mut tree, &path)?;
                let entry = FileTableEntry::new(path, size, mtime, hash.clone());

                // replace peer
                if let Some(e) = get_file_entry(&cache_table, file_inode, Some(peer))?
                    && e.hash == old_hash
                {
                    self.do_write_file_entry(&mut cache_table, file_inode, peer, &entry)?;
                }

                // replace default
                if let Some(old_entry) = get_file_entry(&cache_table, file_inode, None)?
                    && old_entry.hash == old_hash
                {
                    self.do_write_default_file_entry(
                        &txn,
                        &mut tree,
                        &mut cache_table,
                        file_inode,
                        &entry,
                    )?;
                }

                // replace local
                let entry = txn
                    .index_table()?
                    .get(file_inode)?
                    .map(|v| v.value().parse().ok())
                    .flatten();
                if let Some(mut entry) = entry {
                    if replaces_local_copy(&entry, &old_hash) {
                        // Just remember that a newer version exist in
                        // a remote peer. This information is going to
                        // be used to download that newer version later on.
                        entry.outdated_by = Some(hash.clone());
                        let mut index_table = txn.index_table()?;
                        index_table.insert(file_inode, Holder::with_content(entry)?)?;
                    }
                }
            }
            Notification::Remove {
                index,
                path,
                old_hash,
                ..
            } => {
                do_update_last_seen_notification(&txn, peer, index)?;

                self.do_unlink(&txn, peer, &path, old_hash.clone())?;

                // remove local
                if let Some(index_root) = index_root {
                    let tree = txn.read_tree()?;
                    if let Some(inode) = tree.resolve(&path)? {
                        let index_table = txn.index_table()?;
                        if let Some(entry) = index_table.get(inode)? {
                            let entry = entry.value().parse()?;
                            if replaces_local_copy(&entry, &old_hash) {
                                // This specific version has been removed
                                // remotely. Make sure that the file hasn't
                                // changed since it was indexed and if it hasn't,
                                // remove it locally as well.
                                if file_matches_index(&entry.into(), index_root, &path) {
                                    std::fs::remove_file(&path.within(index_root))?;
                                }
                            }
                        }
                    }
                }
            }
            Notification::Drop {
                index,
                path,
                old_hash,
                ..
            } => {
                do_update_last_seen_notification(&txn, peer, index)?;

                self.do_unlink(&txn, peer, &path, old_hash)?;
            }
            Notification::CatchupStart(_) => {
                do_mark_peer_files(&txn, peer)?;
            }
            Notification::Catchup {
                path,
                mtime,
                size,
                hash,
                ..
            } => {
                let mut cache_table = txn.cache_table()?;
                let mut tree = txn.write_tree()?;
                let (_, file_inode) = do_create_file(&mut cache_table, &mut tree, &path)?;

                do_unmark_peer_file(&txn, peer, file_inode)?;

                let entry = FileTableEntry::new(path.clone(), size, mtime, hash.clone());

                // catchup peer; remove the older version and write a
                // new one
                if let Some(e) = get_file_entry(&cache_table, file_inode, None)?
                    && e.hash != hash
                {
                    self.do_unlink(&txn, peer, &path, e.hash)?;
                }
                self.do_write_file_entry(&mut cache_table, file_inode, peer, &entry)?;

                // catchup default (same as add)
                if !get_file_entry(&cache_table, file_inode, None)?.is_some() {
                    self.do_write_default_file_entry(
                        &txn,
                        &mut tree,
                        &mut cache_table,
                        file_inode,
                        &entry,
                    )?;
                }
            }
            Notification::CatchupComplete { index, .. } => {
                self.do_delete_marked_files(&txn, peer)?;
                do_update_last_seen_notification(&txn, peer, index)?;
            }
            Notification::Connected { uuid, .. } => {
                let mut peer_table = txn.peer_table()?;
                let key = peer.as_str();
                if let Some(entry) = peer_table.get(key)? {
                    if entry.value().parse()?.uuid == uuid {
                        // We're connected to the same store as before; there's nothing to do.
                        return Ok(());
                    }
                }
                peer_table.insert(key, Holder::with_content(PeerTableEntry { uuid })?)?;
                let mut notification_table = txn.notification_table()?;
                notification_table.remove(key)?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Open a file for reading/writing.
    pub(crate) fn open_file(&self, inode: Inode) -> Result<Blob, StorageError> {
        // Optimistically, try a read transaction to check whether the
        // blob is there.
        {
            let txn = self.db.begin_read()?;
            if blob::blob_exists(&txn, inode)? {
                // Delegate to Blobstore
                return self.blobstore.open_blob(&txn, inode);
            }
        }

        // Switch to a write transaction to create the blob. We need to read
        // the file entry again because it might have changed.
        let txn = self.db.begin_write()?;
        let ret = {
            let cache_table = txn.cache_table()?;
            let mark_table = txn.mark_table()?;
            let file_entry = get_default_entry(&cache_table, inode)?;
            let queue = queue_for_blob(&mark_table, &txn.read_tree()?, inode)?;
            let blob = self.blobstore.create_blob(
                &txn,
                &file_entry.hash,
                file_entry.size,
                queue,
                inode,
            )?;
            log::debug!(
                "[{}] assigned blob {} in {queue:?} to file {inode}",
                self.arena,
                blob.inode()
            );

            blob
        };
        txn.commit()?;

        Ok(ret)
    }

    /// Move the blob entry for `path` to `dest` and delete the blob.
    ///
    /// Also enables version tracking on `path` to allow detecting
    /// when `dest` becomes out-of-date.
    ///
    /// Gives up and returns false if `path` doesn't have a verified
    /// blob with version `hash`.
    pub(crate) fn move_blob_if_matches<T>(
        &self,
        txn: &ArenaWriteTransaction,
        path: T,
        hash: &Hash,
        dest: &std::path::Path,
    ) -> Result<bool, StorageError>
    where
        T: AsRef<Path>,
    {
        let path = path.as_ref();
        let tree = txn.read_tree()?;
        let inode = tree.expect(path)?;
        let cache_table = txn.cache_table()?;
        let file_entry = get_default_entry(&cache_table, inode)?;
        if file_entry.hash != *hash {
            return Ok(false);
        }
        if !self
            .blobstore
            .move_blob_if_matches(&txn, inode, hash, dest)?
        {
            return Ok(false);
        }

        log::debug!("Realized [{}]/{path} {hash} as {dest:?}", self.arena);

        Ok(true)
    }

    /// Prepare the database and return the path to write into to move some
    /// file into the blob, replacing any existing ones.
    ///
    /// Gives up and returns None if `hash` doesn't match the entry version.
    pub(crate) fn move_into_blob_if_matches<T>(
        &self,
        txn: &ArenaWriteTransaction,
        path: T,
        hash: &Hash,
        metadata: &std::fs::Metadata,
    ) -> Result<Option<PathBuf>, StorageError>
    where
        T: AsRef<Path>,
    {
        let path = path.as_ref();
        let tree = txn.read_tree()?;
        let inode = match tree.resolve(path)? {
            Some(inode) => inode,
            None => {
                return Ok(None);
            }
        };
        let cache_table = txn.cache_table()?;
        let file_entry = match get_default_entry(&cache_table, inode) {
            Ok(e) => e,
            Err(StorageError::NotFound) => {
                return Ok(None);
            }
            Err(err) => {
                return Err(err);
            }
        };
        if file_entry.hash != *hash || file_entry.size != metadata.len() {
            return Ok(None);
        }
        let mark_table = txn.mark_table()?;
        let cachepath = self.blobstore.move_into_blob(
            &txn,
            inode,
            hash,
            queue_for_blob(&mark_table, &tree, inode)?,
            metadata,
        )?;

        Ok(Some(cachepath))
    }

    // Return the default file entry for a path.
    pub(crate) fn get_file_entry_for_path<T>(
        &self,
        txn: &ArenaReadTransaction,
        path: T,
    ) -> Result<FileTableEntry, StorageError>
    where
        T: AsRef<Path>,
    {
        let path = path.as_ref();
        let tree = txn.read_tree()?;

        let inode = tree.expect(path)?;
        if let Some(e) = get_file_entry(&txn.cache_table()?, inode, None)? {
            return Ok(e);
        }

        Err(StorageError::NotFound)
    }

    /// Write an entry in the file table, overwriting any existing one.
    fn do_write_file_entry(
        &self,
        cache_table: &mut redb::Table<'_, CacheTableKey, Holder<CacheTableEntry>>,
        file_inode: Inode,
        peer: Peer,
        entry: &FileTableEntry,
    ) -> Result<(), StorageError> {
        log::debug!(
            "[{}] new file entry {:?} {file_inode} on {peer} {}",
            self.arena,
            entry.path,
            entry.hash
        );

        cache_table.insert(
            CacheTableKey::PeerCopy(file_inode, peer),
            Holder::new(&CacheTableEntry::File(entry.clone()))?,
        )?;

        Ok(())
    }

    /// This must be executed before updating or removing the default
    /// file entry.
    fn before_default_file_entry_change(
        &self,
        txn: &ArenaWriteTransaction,
        inode: Inode,
        path: &Path,
    ) -> Result<(), StorageError> {
        self.blobstore.delete_blob(&txn, inode)?;

        // This entry is the outside world view of the file, so
        // changes should be reported.
        let mut dirty = txn.write_dirty()?;
        dirty.mark_dirty(path)?;

        Ok(())
    }

    /// Write an entry in the file table, overwriting any existing one.
    fn do_write_default_file_entry(
        &self,
        txn: &ArenaWriteTransaction,
        tree: &mut WritableOpenTree,
        cache_table: &mut redb::Table<'_, CacheTableKey, Holder<CacheTableEntry>>,
        inode: Inode,
        new_entry: &FileTableEntry,
    ) -> Result<(), StorageError> {
        self.before_default_file_entry_change(txn, inode, &new_entry.path)?;
        tree.insert_and_incref(
            inode,
            cache_table,
            CacheTableKey::Default(inode),
            Holder::new(&CacheTableEntry::File(new_entry.clone()))?,
        )?;

        Ok(())
    }

    /// Remove a file entry for a specific peer and update or remove
    /// the corresponding default entry, as necessary.
    fn do_rm_file_entry(
        &self,
        txn: &ArenaWriteTransaction,
        cache_table: &mut redb::Table<'_, CacheTableKey, Holder<CacheTableEntry>>,
        tree: &mut WritableOpenTree,
        parent_inode: Inode,
        inode: Inode,
        peer: Peer,
    ) -> Result<(), StorageError> {
        // Remove the entry
        if cache_table
            .remove(CacheTableKey::PeerCopy(inode, peer))?
            .is_none()
        {
            // nothing was changed
            return Ok(());
        }

        // Update or remove the default entry, if it relied on the now
        // removed peer entry.

        let mut peer_entries = vec![];
        let mut default_entry = None;
        for elt in cache_table.range(CacheTableKey::range(inode))? {
            let (key, value) = elt?;
            match key.value() {
                CacheTableKey::PeerCopy(_, _) => {
                    peer_entries.push(value.value().parse()?.expect_file()?);
                }
                CacheTableKey::Default(_) => {
                    default_entry = Some(value.value().parse()?.expect_file()?);
                }
                _ => {}
            }
        }
        let default_entry = match default_entry {
            Some(e) => e,
            None => {
                // if there's no default entry, there's nothing to do
                return Ok(());
            }
        };

        if peer_entries
            .iter()
            .find(|e| e.hash == default_entry.hash)
            .is_some()
        {
            // The default entry is still valid
            return Ok(());
        }

        // Select a new hash. This selects the most recent one; since
        // version history is lost there's very little else we can do.
        let new_entry = peer_entries.into_iter().max_by_key(|e| e.mtime);
        match new_entry {
            Some(new_entry) => {
                self.do_write_default_file_entry(txn, tree, cache_table, inode, &new_entry)?;
            }
            None => {
                self.before_default_file_entry_change(txn, inode, &default_entry.path)?;
                tree.remove_and_decref(inode, cache_table, CacheTableKey::Default(inode))?;

                // Update the parent modification time, as removing an
                // entry modifies it.
                let parent_dir_entry = DirtableEntry {
                    mtime: UnixTime::now(),
                };
                let parent_dir_holder =
                    Holder::with_content(CacheTableEntry::Dir(parent_dir_entry))?;
                cache_table.insert(CacheTableKey::Default(parent_inode), parent_dir_holder)?;

                return Ok(());
            }
        }

        Ok(())
    }

    fn do_unlink<T>(
        &self,
        txn: &ArenaWriteTransaction,
        peer: Peer,
        path: T,
        old_hash: Hash,
    ) -> Result<(), StorageError>
    where
        T: AsRef<Path>,
    {
        let path = path.as_ref();
        let mut tree = txn.write_tree()?;
        let parent_inode = {
            if let Some(parent_path) = path.parent() {
                tree.resolve(parent_path)?
            } else {
                Some(tree.root())
            }
        };
        if let Some(parent_inode) = parent_inode {
            if let Some(inode) = tree.lookup(parent_inode, path.name())? {
                let mut cache_table = txn.cache_table()?;

                if let Some(e) = get_file_entry(&cache_table, inode, Some(peer))?
                    && e.hash == old_hash
                {
                    self.do_rm_file_entry(
                        txn,
                        &mut cache_table,
                        &mut tree,
                        parent_inode,
                        inode,
                        peer,
                    )?;
                }
            }
        }

        Ok(())
    }

    /// Delete all marked files for a peer.
    fn do_delete_marked_files(
        &self,
        txn: &ArenaWriteTransaction,
        peer: Peer,
    ) -> Result<(), StorageError> {
        let mut pending_catchup_table = txn.pending_catchup_table()?;
        let mut cache_table = txn.cache_table()?;
        let mut tree = txn.write_tree()?;
        let peer_str = peer.as_str();
        for elt in pending_catchup_table
            .extract_from_if((peer_str, Inode::ZERO)..=(peer_str, Inode::MAX), |_, _| {
                true
            })?
        {
            let elt = elt?;
            let (_, inode) = elt.0.value();
            if let Some(parent_inode) = tree.parent(inode)? {
                self.do_rm_file_entry(txn, &mut cache_table, &mut tree, parent_inode, inode, peer)?;
            }
        }
        Ok(())
    }

    pub(crate) fn local_availability(
        &self,
        inode: Inode,
    ) -> Result<LocalAvailability, StorageError> {
        let txn = self.db.begin_read()?;

        blob::local_availability(&txn, inode)
    }

    // TODO: update tests to work on blobstore and remove
    #[allow(dead_code)]
    pub(crate) fn extend_local_availability(
        &self,
        inode: Inode,
        new_range: &ByteRanges,
    ) -> Result<(), StorageError> {
        self.blobstore.extend_local_availability(inode, new_range)
    }

    /// Clean up the cache by removing blobs until the total disk usage is <= target_size.
    ///
    /// This method removes the least recently used blobs first, but skips blobs that are currently open.
    #[allow(dead_code)]
    pub(crate) fn cleanup_cache(&self, target_size: u64) -> Result<(), StorageError> {
        self.blobstore.cleanup_cache(target_size)
    }

    fn get_indexed_file(
        &self,
        tree: &impl TreeReadOperations,
        index_table: &impl ReadableTable<Inode, Holder<'static, FileTableEntry>>,
        path: &Path,
    ) -> Result<Option<IndexedFileTableEntry>, StorageError> {
        if let Some(inode) = tree.resolve(path)? {
            get_indexed_file_inode(index_table, inode)
        } else {
            Ok(None)
        }
    }

    fn index_file(
        &self,
        txn: &ArenaWriteTransaction,
        tree: &mut WritableOpenTree,
        path: &realize_types::Path,
        size: u64,
        mtime: UnixTime,
        hash: Hash,
    ) -> Result<IndexedFileTableEntry, StorageError> {
        let mut index_table = txn.index_table()?;
        let old_hash = tree
            .resolve(path)?
            .map(|inode| {
                index_table
                    .get(inode)
                    .ok()
                    .flatten()
                    .map(|e| e.value().parse().ok())
                    .flatten()
                    .map(|e| e.hash)
            })
            .flatten();
        let same_hash = old_hash.as_ref().map(|h| *h == hash).unwrap_or(false);
        let entry = FileTableEntry::new(path.clone(), size, mtime, hash);
        let inode = tree.setup(path)?;
        tree.insert_and_incref(inode, &mut index_table, inode, Holder::new(&entry)?)?;
        if !same_hash {
            txn.write_dirty()?.mark_dirty(path)?;
            txn.write_history()?.report_added(path, old_hash.as_ref())?;
        }

        Ok(entry.into())
    }

    fn unindex_file(
        &self,
        txn: &ArenaWriteTransaction,
        tree: &mut WritableOpenTree,
        index_table: &mut Table<'_, Inode, Holder<'static, FileTableEntry>>,
        inode: Inode,
    ) -> Result<(), StorageError> {
        let FileTableEntry { path, hash, .. } = match index_table.get(inode)? {
            None => {
                // Nothing to do
                return Ok(());
            }
            Some(existing) => existing.value().parse()?,
        };
        if tree.remove_and_decref(inode, index_table, inode)? {
            txn.write_dirty()?.mark_dirty(&path)?;
            txn.write_history()?.report_removed(&path, &hash)?;
        }

        Ok(())
    }

    fn unindex_dir(
        &self,
        txn: &ArenaWriteTransaction,
        tree: &mut WritableOpenTree,
        index_table: &mut Table<'_, Inode, Holder<'static, FileTableEntry>>,
        inode: Inode,
    ) -> Result<(), StorageError> {
        let mut dirty = txn.write_dirty()?;
        let mut history = txn.write_history()?;
        tree.remove_recursive_and_decref(
            inode,
            index_table,
            |inode| Ok(inode),
            |_, e| {
                let FileTableEntry { path, hash, .. } = e.parse()?;
                dirty.mark_dirty(&path)?;
                history.report_removed(&path, &hash)?;

                Ok(true)
            },
        )?;

        Ok(())
    }
}

/// Choose the appropriate blob queue for the inode, given its mark.
fn queue_for_blob(
    mark_table: &impl ReadableTable<Inode, Holder<'static, MarkTableEntry>>,
    tree: &impl TreeReadOperations,
    inode: Inode,
) -> Result<LruQueueId, StorageError> {
    let mark = resolve_mark(mark_table, tree, inode)?;
    Ok(queue_for_mark(mark))
}

/// Choose the appropriate queue given a file mark.
fn queue_for_mark(mark: Mark) -> LruQueueId {
    match mark {
        Mark::Watch => LruQueueId::WorkingArea,
        Mark::Keep | Mark::Own => LruQueueId::Protected,
    }
}

fn get_indexed_file_inode(
    index_table: &impl ReadableTable<Inode, Holder<'static, FileTableEntry>>,
    inode: Inode,
) -> Result<Option<IndexedFileTableEntry>, StorageError> {
    match index_table.get(inode)? {
        None => Ok(None),
        Some(v) => Ok(Some(v.value().parse()?.into())),
    }
}

impl RealIndex for ArenaCache {
    fn uuid(&self) -> &uuid::Uuid {
        &self.uuid
    }

    fn arena(&self) -> Arena {
        self.arena
    }

    fn watch_history(&self) -> tokio::sync::watch::Receiver<u64> {
        self.db.history().watch()
    }

    fn last_history_index(&self) -> Result<u64, StorageError> {
        self.db.begin_read()?.read_history()?.last_history_index()
    }

    fn get_file(
        &self,
        path: &realize_types::Path,
    ) -> Result<Option<IndexedFileTableEntry>, StorageError> {
        let txn = self.db.begin_read()?;

        self.get_file_txn(&txn, path)
    }

    fn get_file_txn(
        &self,
        txn: &super::db::ArenaReadTransaction,
        path: &realize_types::Path,
    ) -> Result<Option<IndexedFileTableEntry>, StorageError> {
        self.get_indexed_file(&txn.read_tree()?, &txn.index_table()?, path)
    }

    fn get_indexed_file_txn(
        &self,
        txn: &ArenaWriteTransaction,
        root: &std::path::Path,
        path: &realize_types::Path,
        hash: Option<&Hash>,
    ) -> Result<Option<std::path::PathBuf>, StorageError> {
        let index_table = txn.index_table()?;
        let entry = self.get_indexed_file(&txn.read_tree()?, &index_table, path)?;
        match hash {
            Some(hash) => {
                if let Some(entry) = entry
                    && entry.hash == *hash
                    && file_matches_index(&entry, root, path)
                {
                    return Ok(Some(path.within(root)));
                }
            }
            None => {
                if entry.is_none() && fs_utils::metadata_no_symlink_blocking(root, path).is_err() {
                    return Ok(Some(path.within(root)));
                }
            }
        }

        Ok(None)
    }

    fn has_file(&self, path: &realize_types::Path) -> Result<bool, StorageError> {
        // TODO: consider optimizing; parsing the final value can be skipped

        Ok(self.get_file(path)?.is_some())
    }

    fn has_matching_file(
        &self,
        path: &realize_types::Path,
        size: u64,
        mtime: UnixTime,
    ) -> Result<bool, StorageError> {
        Ok(self
            .get_file(path)?
            .map(|e| e.size == size && e.mtime == mtime)
            .unwrap_or(false))
    }

    fn add_file(
        &self,
        path: &realize_types::Path,
        size: u64,
        mtime: UnixTime,
        hash: Hash,
    ) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        self.index_file(&txn, &mut txn.write_tree()?, path, size, mtime, hash)?;
        txn.commit()?;

        Ok(())
    }

    fn add_file_if_matches(
        &self,
        root: &std::path::Path,
        path: &realize_types::Path,
        size: u64,
        mtime: UnixTime,
        hash: Hash,
    ) -> Result<bool, StorageError> {
        let txn = self.db.begin_write()?;
        let matches = {
            let mut tree = txn.write_tree()?;
            let entry = self.index_file(&txn, &mut tree, path, size, mtime, hash)?;

            file_matches_index(&entry, root, path)
        };
        if matches {
            txn.commit()?;
        }

        Ok(matches)
    }

    fn remove_file_if_missing(
        &self,
        root: &std::path::Path,
        path: &realize_types::Path,
    ) -> Result<bool, StorageError> {
        let txn = self.db.begin_write()?;
        if fs_utils::metadata_no_symlink_blocking(root, path).is_err() {
            {
                let path = path.as_ref();
                let mut tree = txn.write_tree()?;
                if let Some(inode) = tree.resolve(path)? {
                    let mut index_table = txn.index_table()?;
                    self.unindex_file(&txn, &mut tree, &mut index_table, inode)?;
                }
            }
            txn.commit()?;
            return Ok(true);
        }

        Ok(false)
    }

    fn all_files(
        &self,
        tx: mpsc::Sender<(realize_types::Path, IndexedFileTableEntry)>,
    ) -> Result<(), StorageError> {
        let txn = self.db.begin_read()?;
        let index_table = txn.index_table()?;
        for (file_path, entry) in index_table
            .iter()?
            .flatten()
            // Skip any entry with errors
            .flat_map(|(_, v)| {
                if let Ok(entry) = v.value().parse() {
                    let file_path = entry.path.clone();
                    return Some((file_path, IndexedFileTableEntry::from(entry)));
                }

                None
            })
        {
            if let Err(_) = tx.blocking_send((file_path, entry)) {
                break;
            }
        }

        Ok(())
    }

    fn history(
        &self,
        range: std::ops::Range<u64>,
        tx: mpsc::Sender<Result<(u64, HistoryTableEntry), StorageError>>,
    ) -> Result<(), StorageError> {
        self.db.begin_read()?.read_history()?.history(range, tx)
    }

    fn remove_file_or_dir(&self, path: &realize_types::Path) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            if let Some(inode) = tree.resolve(path)? {
                let mut index_table = txn.index_table()?;
                self.unindex_file(&txn, &mut tree, &mut index_table, inode)?;
                self.unindex_dir(&txn, &mut tree, &mut index_table, inode)?;
            }
        }
        txn.commit()?;

        Ok(())
    }

    fn drop_file_if_matches(
        &self,
        txn: &ArenaWriteTransaction,
        root: &std::path::Path,
        path: &realize_types::Path,
        hash: &Hash,
    ) -> Result<bool, StorageError> {
        let mut tree = txn.write_tree()?;
        let inode = match tree.resolve(path)? {
            Some(inode) => inode,
            None => {
                return Ok(false);
            }
        };

        let mut index_table = txn.index_table()?;
        if let Some(entry) = get_indexed_file_inode(&index_table, inode)?
            && entry.hash == *hash
            && file_matches_index(&entry.into(), root, path)
        {
            tree.remove_and_decref(inode, &mut index_table, inode)?;
            txn.write_dirty()?.mark_dirty(path)?;
            txn.write_history()?.report_dropped(path, hash)?;
            return Ok(true);
        }

        Ok(false)
    }

    fn update(
        &self,
        _notification: &Notification,
        _root: &std::path::Path,
    ) -> Result<(), StorageError> {
        // This now happens in on the ArenaCache side.
        // TODO:remove this method once ArenaCache and index have been fully merged.
        Ok(())
    }
}

impl PathMarks for ArenaCache {
    fn get_mark(&self, path: &Path) -> Result<Mark, StorageError> {
        let txn = self.db.begin_read()?;

        self.get_mark_txn(&txn, path)
    }

    fn get_mark_txn(&self, txn: &ArenaReadTransaction, path: &Path) -> Result<Mark, StorageError> {
        let mark_table = txn.mark_table()?;
        let tree = txn.read_tree()?;

        Ok(resolve_mark_at_path(&mark_table, &tree, path)?)
    }

    fn set_arena_mark(&self, mark: Mark) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let inode = self.arena_root;

            let mut mark_table = txn.mark_table()?;
            let cache_table = txn.cache_table()?;
            let tree = txn.read_tree()?;
            let old_mark = resolve_mark(&mark_table, &tree, inode)?;
            mark_table.insert(inode, Holder::with_content(MarkTableEntry { mark })?)?;

            if old_mark != mark {
                let index_table = txn.index_table()?;
                mark_dirty_recursive(&txn, &tree, &cache_table, &index_table, inode)?;
            }
        }
        txn.commit()?;

        Ok(())
    }

    fn set_mark(&self, path: &Path, mark: Mark) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut mark_table = txn.mark_table()?;
            let mut tree = txn.write_tree()?;
            let old_mark = resolve_mark_at_path(&mark_table, &tree, path)?;
            let inode = tree.setup(path)?;
            tree.insert_and_incref(
                inode,
                &mut mark_table,
                inode,
                Holder::with_content(MarkTableEntry { mark })?,
            )?;

            if old_mark != mark {
                let cache_table = txn.cache_table()?;
                let index_table = txn.index_table()?;
                mark_dirty_recursive(&txn, &tree, &cache_table, &index_table, inode)?;
            }
        }
        txn.commit()?;

        Ok(())
    }

    fn clear_mark(&self, path: &Path) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let inode = match tree.resolve(path)? {
                Some(inode) => inode,
                None => {
                    // No mark to remove
                    return Ok(());
                }
            };
            let mut mark_table = txn.mark_table()?;
            let old_mark = resolve_mark(&mark_table, &tree, inode)?;
            let removed = tree.remove_and_decref(inode, &mut mark_table, inode)?;
            if removed && old_mark != resolve_mark(&mark_table, &tree, inode)? {
                let cache_table = txn.cache_table()?;
                let index_table = txn.index_table()?;
                mark_dirty_recursive(&txn, &tree, &cache_table, &index_table, inode)?;
            }
        }
        txn.commit()?;

        Ok(())
    }
}

fn resolve_mark_at_path<P: AsRef<Path>>(
    mark_table: &impl ReadableTable<Inode, Holder<'static, MarkTableEntry>>,
    tree: &impl TreeReadOperations,
    path: P,
) -> Result<Mark, StorageError> {
    let last_matching = tree.resolve_partial(path.as_ref())?;

    resolve_mark(mark_table, tree, last_matching)
}

fn resolve_mark(
    mark_table: &impl ReadableTable<Inode, Holder<'static, MarkTableEntry>>,
    tree: &impl TreeReadOperations,
    inode: Inode,
) -> Result<Mark, StorageError> {
    for inode in std::iter::once(Ok(inode)).chain(tree.ancestors(inode)) {
        let inode = inode?;
        if let Some(e) = mark_table.get(inode)? {
            return Ok(e.value().parse()?.mark);
        }
    }

    Ok(Mark::default())
}

/// Mark all indexed or cached files dirty.
///
/// This is typically called after changing a mark on `inode`.
fn mark_dirty_recursive(
    txn: &ArenaWriteTransaction,
    tree: &impl TreeReadOperations,
    cache_table: &redb::Table<'_, CacheTableKey, Holder<CacheTableEntry>>,
    index_table: &redb::Table<'_, Inode, Holder<FileTableEntry>>,
    inode: Inode,
) -> Result<(), StorageError> {
    let mut dirty = txn.write_dirty()?;
    for inode in std::iter::once(Ok(inode)).chain(tree.recurse(inode, |_| true)) {
        let inode = inode?;
        if let Some(entry) = index_table.get(inode)? {
            let path = &entry.value().parse()?.path;
            dirty.mark_dirty(path)?;
        } else if let Some(entry) = cache_table.get(CacheTableKey::Default(inode))? {
            if let CacheTableEntry::File(entry) = entry.value().parse()? {
                dirty.mark_dirty(entry.path)?;
            }
        }
    }

    Ok(())
}

fn do_update_last_seen_notification(
    txn: &ArenaWriteTransaction,
    peer: Peer,
    index: u64,
) -> Result<(), StorageError> {
    let mut notification_table = txn.notification_table()?;
    notification_table.insert(peer.as_str(), index)?;

    Ok(())
}

fn do_peer_progress(
    txn: &ArenaReadTransaction,
    peer: Peer,
) -> Result<Option<Progress>, StorageError> {
    let key = peer.as_str();

    let peer_table = txn.peer_table()?;
    if let Some(entry) = peer_table.get(key)? {
        let PeerTableEntry { uuid, .. } = entry.value().parse()?;

        let notification_table = txn.notification_table()?;
        if let Some(last_seen) = notification_table.get(key)? {
            return Ok(Some(Progress::new(uuid, last_seen.value())));
        }
    }

    Ok(None)
}

/// Get a [FileTableEntry] for a specific peer.
fn get_file_entry(
    cache_table: &impl redb::ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
    inode: Inode,
    peer: Option<Peer>,
) -> Result<Option<FileTableEntry>, StorageError> {
    match cache_table.get(
        peer.map(|p| CacheTableKey::PeerCopy(inode, p))
            .unwrap_or_else(|| CacheTableKey::Default(inode)),
    )? {
        None => Ok(None),
        Some(e) => Ok(e.value().parse()?.file()),
    }
}

fn get_default_entry(
    cache_table: &impl redb::ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
    inode: Inode,
) -> Result<FileTableEntry, StorageError> {
    let entry = cache_table
        .get(CacheTableKey::Default(inode))?
        .ok_or(StorageError::NotFound)?;

    Ok(entry.value().parse()?.expect_file()?)
}

fn do_file_metadata(
    txn: &ArenaReadTransaction,
    inode: Inode,
) -> Result<FileMetadata, StorageError> {
    let entry = get_default_entry(&txn.cache_table()?, inode)?;
    Ok(FileMetadata {
        size: entry.size,
        mtime: entry.mtime,
    })
}

fn do_file_availability(
    txn: &ArenaReadTransaction,
    inode: Inode,
    arena: Arena,
) -> Result<FileAvailability, StorageError> {
    let cache_table = txn.cache_table()?;

    let mut range = cache_table.range(CacheTableKey::range(inode))?;
    let (default_key, default_entry) = range.next().ok_or(StorageError::NotFound)??;
    if !matches!(default_key.value(), CacheTableKey::Default(_)) {
        return Err(StorageError::NotFound);
    }
    let FileTableEntry {
        size,
        mtime,
        path,
        hash,
        ..
    } = default_entry.value().parse()?.expect_file()?;

    let mut peers = vec![];
    for entry in range {
        let entry = entry?;
        if let CacheTableKey::PeerCopy(_, peer) = entry.0.value() {
            let file_entry: FileTableEntry = entry.1.value().parse()?.expect_file()?;
            if file_entry.hash == hash {
                peers.push(peer);
            }
        }
    }
    if peers.is_empty() {
        log::warn!("No peer has hash {hash} for {inode}");
        return Err(StorageError::NotFound);
    }

    Ok(FileAvailability {
        arena: arena,
        path,
        metadata: FileMetadata { size, mtime },
        hash,
        peers,
    })
}

/// Retrieve or create a file entry at the given path.
///
/// Return the parent inode and the file inode.
fn do_create_file<T>(
    cache_table: &mut redb::Table<'_, CacheTableKey, Holder<CacheTableEntry>>,
    tree: &mut WritableOpenTree,
    path: T,
) -> Result<(Inode, Inode), StorageError>
where
    T: AsRef<Path>,
{
    let path = path.as_ref();
    let mut parent_inode = tree.root();
    for component in Path::components(path.parent().as_ref()) {
        parent_inode = setup_dir(cache_table, tree, parent_inode, component)?;
    }

    let file_inode = tree.setup_name(parent_inode, path.name())?;

    // Update the parent directory mtime since we're adding a file to it
    let parent_dir_entry = DirtableEntry {
        mtime: UnixTime::now(),
    };
    let parent_dir_holder = Holder::with_content(CacheTableEntry::Dir(parent_dir_entry))?;
    cache_table.insert(CacheTableKey::Default(parent_inode), parent_dir_holder)?;

    Ok((parent_inode, file_inode))
}

fn setup_dir(
    cache_table: &mut redb::Table<'_, CacheTableKey, Holder<CacheTableEntry>>,
    tree: &mut WritableOpenTree,
    parent_inode: Inode,
    name: &str,
) -> Result<Inode, StorageError> {
    let inode = tree.setup_name(parent_inode, name)?;

    let dir_entry = DirtableEntry {
        mtime: UnixTime::now(),
    };
    let dir_holder = Holder::with_content(CacheTableEntry::Dir(dir_entry))?;
    if cache_table.get(CacheTableKey::Default(inode))?.is_none() {
        // new directory; assign its mtime
        cache_table.insert(CacheTableKey::Default(inode), dir_holder.borrow())?;

        // and update the parent mtime, since the parent content changed
        // Only update if the parent doesn't already have a directory entry
        if cache_table
            .get(CacheTableKey::Default(parent_inode))?
            .is_none()
        {
            let parent_dir_entry = DirtableEntry {
                mtime: UnixTime::now(),
            };
            let parent_dir_holder = Holder::with_content(CacheTableEntry::Dir(parent_dir_entry))?;
            cache_table.insert(CacheTableKey::Default(parent_inode), parent_dir_holder)?;
        }
    }

    Ok(inode)
}

fn do_mark_peer_files(txn: &ArenaWriteTransaction, peer: Peer) -> Result<(), StorageError> {
    let cache_table = txn.cache_table()?;
    let mut pending_catchup_table = txn.pending_catchup_table()?;
    for elt in cache_table.iter()? {
        let (k, _) = elt?;
        if let CacheTableKey::PeerCopy(inode, elt_peer) = k.value()
            && elt_peer == peer
        {
            pending_catchup_table.insert((peer.as_str(), inode), ())?;
        }
    }

    Ok(())
}

fn do_unmark_peer_file(
    txn: &ArenaWriteTransaction,
    peer: Peer,
    inode: Inode,
) -> Result<(), StorageError> {
    let mut pending_catchup_table = txn.pending_catchup_table()?;
    pending_catchup_table.remove((peer.as_str(), inode))?;

    Ok(())
}

fn load_or_assign_uuid(db: &Arc<ArenaDatabase>) -> Result<Uuid, StorageError> {
    let txn = db.begin_write()?;
    let mut settings_table = txn.settings_table()?;
    if let Some(value) = settings_table.get("uuid")? {
        let bytes: uuid::Bytes = value
            .value()
            .try_into()
            .map_err(|_| ByteConversionError::Invalid("uuid"))?;

        Ok(Uuid::from_bytes(bytes))
    } else {
        let uuid = Uuid::now_v7();
        let bytes: &[u8] = uuid.as_bytes();
        settings_table.insert("uuid", &bytes)?;
        drop(settings_table);
        txn.commit()?;

        Ok(uuid)
    }
}

fn file_matches_index(
    entry: &IndexedFileTableEntry,
    root: &std::path::Path,
    path: &realize_types::Path,
) -> bool {
    if let Ok(m) = fs_utils::metadata_no_symlink_blocking(root, path) {
        UnixTime::mtime(&m) == entry.mtime && m.len() == entry.size
    } else {
        false
    }
}

/// Check whether replacing `old_hash` replaces `entry`
fn replaces_local_copy(entry: &FileTableEntry, old_hash: &Hash) -> bool {
    entry.hash == *old_hash
        || entry
            .outdated_by
            .as_ref()
            .map(|h| *h == *old_hash)
            .unwrap_or(false)
}

/// Check whether the given inode exists in the cache and whether it
/// is a file or a directory.
fn inode_assignment(
    cache_table: &impl ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
    inode: Inode,
) -> Result<Option<InodeAssignment>, StorageError> {
    match cache_table.get(CacheTableKey::Default(inode))? {
        Some(e) => match e.value().parse()? {
            CacheTableEntry::Dir(_) => Ok(Some(InodeAssignment::Directory)),
            CacheTableEntry::File(_) => Ok(Some(InodeAssignment::File)),
        },
        None => Ok(None),
    }
}

/// Make sure the given inode exists and is a directory.
fn check_is_dir(
    cache_table: &impl ReadableTable<CacheTableKey, Holder<'static, CacheTableEntry>>,
    inode: Inode,
) -> Result<(), StorageError> {
    match inode_assignment(cache_table, inode)? {
        None => Err(StorageError::NotFound),
        Some(InodeAssignment::File) => Err(StorageError::NotADirectory),
        Some(InodeAssignment::Directory) => Ok(()), // continue
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::sync::Arc;
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};
    use tokio::sync::mpsc;

    use super::ArenaCache;
    use crate::arena::db::ArenaDatabase;
    use crate::arena::dirty::DirtyReadOperations;

    use crate::arena::index::RealIndex;
    use crate::arena::mark::PathMarks;
    use crate::arena::notifier::Notification;
    use crate::arena::tree::{TreeExt, TreeReadOperations};
    use crate::arena::types::HistoryTableEntry;
    use crate::utils::hash;
    use crate::utils::redb_utils;
    use crate::{
        FileMetadata, GlobalDatabase, Inode, InodeAllocator, InodeAssignment, LocalAvailability,
        Mark, StorageError,
    };
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use realize_types::{Arena, Hash, Path, Peer, UnixTime};

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

        fn clear_dirty(&self) -> anyhow::Result<()> {
            let txn = self.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                while dirty.take_next_dirty_path()?.is_some() {}
            }
            txn.commit()?;

            Ok(())
        }

        fn dir_mtime<T>(&self, path: T) -> anyhow::Result<UnixTime>
        where
            T: AsRef<Path>,
        {
            let path = path.as_ref();
            let inode = self.acache.expect(path)?;
            Ok(self.acache.dir_mtime(inode)?)
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

        fn as_real_index(&self) -> Arc<dyn RealIndex> {
            Arc::clone(&self.acache) as Arc<dyn RealIndex>
        }

        /// Check if a path is dirty
        fn is_dirty<T>(&self, path: T) -> anyhow::Result<bool>
        where
            T: AsRef<Path>,
        {
            let path = path.as_ref();
            let txn = self.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            Ok(dirty.is_dirty(path)?)
        }

        async fn setup() -> anyhow::Result<Fixture> {
            Self::setup_with_arena(test_arena()).await
        }
    }

    #[tokio::test]
    async fn add_creates_directories() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let acache = &fixture.acache;
        let file_path = Path::parse("a/b/c.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&file_path, 100, mtime)?;

        let (inode, assignment) = acache.lookup(acache.arena_root(), "a")?;
        assert_eq!(assignment, InodeAssignment::Directory, "a");

        let (_, assignment) = acache.lookup(inode, "b")?;
        assert_eq!(assignment, InodeAssignment::Directory, "b");

        Ok(())
    }

    #[tokio::test]
    async fn add_and_remove_mirrored_in_tree() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let acache = &fixture.acache;
        let file_path = Path::parse("a/b/c.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&file_path, 100, mtime)?;

        let (a, _) = acache.lookup(acache.arena_root(), "a")?;
        let (b, _) = acache.lookup(a, "b")?;
        let (c, _) = acache.lookup(b, "c.txt")?;

        {
            let txn = fixture.db.begin_read()?;
            let tree = txn.read_tree()?;
            assert_eq!(acache.arena_root(), tree.root());
            assert!(tree.inode_exists(a)?);
            assert!(tree.inode_exists(b)?);
            assert!(tree.inode_exists(c)?);
            assert_eq!(Some(a), tree.lookup_inode(tree.root(), "a")?);
            assert_eq!(Some(b), tree.lookup_inode(a, "b")?);
            assert_eq!(Some(c), tree.lookup_inode(b, "c.txt")?);
        }

        fixture.remove_from_cache(&file_path)?;

        assert!(acache.lookup(b, "c.txt").is_err());
        {
            let txn = fixture.db.begin_read()?;
            let tree = txn.read_tree()?;

            // The file is gone from tree, since this was the only
            // reference to it.
            assert!(!tree.inode_exists(c)?);
            assert_eq!(None, tree.lookup_inode(b, "c.txt")?);

            // The directories were cleaned up as well, since this was
            // the last file.
            assert!(!tree.inode_exists(a)?);
            assert!(!tree.inode_exists(b)?);
            assert_eq!(None, tree.lookup_inode(tree.root(), "a")?);
            assert_eq!(None, tree.lookup_inode(a, "b")?);
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
        let dir_mtime = fixture.dir_mtime(&dir)?;

        let path2 = Path::parse("a/b/2.txt")?;
        fixture.add_to_cache(&path2, 100, mtime)?;

        assert!(fixture.dir_mtime(&dir)? > dir_mtime);
        Ok(())
    }

    #[tokio::test]
    async fn add_marks_dirty() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let file_path = Path::parse("a/b/c.txt")?;

        fixture.add_to_cache(&file_path, 100, test_time())?;

        assert!(fixture.is_dirty(&file_path)?);

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

        let inode = acache.expect(&file_path)?;
        let metadata = acache.file_metadata(inode)?;
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
        assert!(fixture.is_dirty(&file_path)?);

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
        assert!(!fixture.is_dirty(&file_path)?);

        Ok(())
    }

    #[tokio::test]
    async fn ignore_duplicate_add() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let file_path = Path::parse("file.txt")?;

        fixture.add_to_cache(&file_path, 100, test_time())?;
        fixture.add_to_cache(&file_path, 200, test_time())?;

        let acache = &fixture.acache;
        let inode = acache.expect(&file_path)?;
        let metadata = acache.file_metadata(inode)?;
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

        let inode = acache.expect(&file_path)?;
        let metadata = acache.file_metadata(inode)?;
        assert_eq!(metadata.size, 100);

        Ok(())
    }

    #[tokio::test]
    async fn unlink_removes_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let acache = &fixture.acache;
        let file_path = Path::parse("file.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&file_path, 100, mtime)?;
        let arena_root = acache.arena_root();
        acache.lookup(arena_root, "file.txt")?;
        fixture.remove_from_cache(&file_path)?;
        assert!(matches!(
            acache.lookup(arena_root, "file.txt"),
            Err(StorageError::NotFound)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn unlink_marks_dirty() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let acache = &fixture.acache;
        let file_path = Path::parse("file.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&file_path, 100, mtime)?;
        let arena_root = acache.arena_root();
        acache.lookup(arena_root, "file.txt")?;

        fixture.clear_dirty()?;
        fixture.remove_from_cache(&file_path)?;
        assert!(fixture.is_dirty(&file_path)?);

        Ok(())
    }

    #[tokio::test]
    async fn unlink_updates_dir_mtime() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena).await?;
        let file_path = Path::parse("file.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&file_path, 100, mtime)?;
        let arena_root = fixture.acache.arena_root();
        let dir_mtime = fixture.acache.dir_mtime(arena_root)?;
        fixture.remove_from_cache(&file_path)?;

        assert!(fixture.acache.dir_mtime(arena_root)? > dir_mtime);

        Ok(())
    }

    #[tokio::test]
    async fn unlink_ignores_wrong_old_hash() -> anyhow::Result<()> {
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
        let inode = acache.expect(&file_path)?;
        let metadata = acache.file_metadata(inode)?;
        assert_eq!(metadata.size, 100);

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
        let (inode, assignment) = acache.lookup(acache.arena_root(), "a")?;
        assert_eq!(assignment, InodeAssignment::Directory);

        // Lookup file
        let (inode, assignment) = acache.lookup(inode, "file.txt")?;
        assert_eq!(assignment, InodeAssignment::File);

        let metadata = acache.file_metadata(inode)?;
        assert_eq!(metadata.mtime, mtime);
        assert_eq!(metadata.size, 100);

        Ok(())
    }

    #[tokio::test]
    async fn lookup_returns_notfound_for_missing_entry() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let acache = &fixture.acache;

        assert!(matches!(
            acache.lookup(acache.arena_root(), "nonexistent"),
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

        let inode = acache.expect(&path)?;
        let metadata = acache.file_metadata(inode)?;
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

        let arena_root = acache.arena_root();
        assert_unordered::assert_eq_unordered!(
            vec![("dir".to_string(), InodeAssignment::Directory),],
            acache
                .readdir(arena_root)?
                .into_iter()
                .map(|(name, _, assignment)| (name, assignment))
                .collect::<Vec<_>>(),
        );

        let (inode, _) = acache.lookup(arena_root, "dir")?;
        assert_unordered::assert_eq_unordered!(
            vec![
                ("file1.txt".to_string(), InodeAssignment::File),
                ("file2.txt".to_string(), InodeAssignment::File),
                ("subdir".to_string(), InodeAssignment::Directory),
            ],
            acache
                .readdir(inode)?
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

        let (inode, _) = acache.lookup(acache.arena_root, "file.txt")?;
        let metadata = acache.file_metadata(inode)?;
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
        let (inode, _) = acache.lookup(acache.arena_root(), "file.txt")?;
        let avail = acache.file_availability(inode)?;
        assert_eq!(arena, avail.arena);
        assert_eq!(path, avail.path);
        // Since they're just independent additions, the cache chooses
        // the first one.
        assert_eq!(Hash([1u8; 32]), avail.hash);
        assert_eq!(
            FileMetadata {
                size: 100,
                mtime: test_time(),
            },
            avail.metadata
        );
        assert_unordered::assert_eq_unordered!(vec![a, b], avail.peers);

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
        let (inode, _) = acache.lookup(acache.arena_root, "file.txt")?;
        let avail = acache.file_availability(inode)?;

        //  Replace with old_hash=1 means that hash=2 is the most
        //  recent one. This is the one chosen.
        assert_eq!(Hash([2u8; 32]), avail.hash);
        assert_eq!(
            FileMetadata {
                size: 200,
                mtime: later_time(),
            },
            avail.metadata
        );
        assert_eq!(vec![b], avail.peers);

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
        let acache = &fixture.acache;
        let avail = acache.file_availability(inode)?;
        assert_eq!(Hash([3u8; 32]), avail.hash);
        assert_eq!(vec![c], avail.peers);

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

        let avail = acache.file_availability(inode)?;
        assert_eq!(Hash([3u8; 32]), avail.hash);
        assert_unordered::assert_eq_unordered!(vec![b, c], avail.peers);

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
        let (inode, _) = acache.lookup(acache.arena_root, "file.txt")?;
        let avail = acache.file_availability(inode)?;
        assert_eq!(vec![a], avail.peers);
        assert_eq!(Hash([3u8; 32]), avail.hash);

        acache.update(a, Notification::CatchupStart(arena), None)?;
        acache.update(
            a,
            Notification::CatchupComplete {
                arena: arena,
                index: 0,
            },
            None,
        )?;
        // All entries from A are now lost! We've lost the single peer
        // that has the selected version.

        // From the two conflicting versions that remain, Hash=2 from
        // C should be chosen, because it has the most recent mtime.
        //
        // (If we kept a history, we would probably go
        // back to Hash=1, but we don't have that kind of information)
        let acache = &fixture.acache;
        let avail = acache.file_availability(inode)?;
        assert_eq!(vec![c], avail.peers);
        assert_eq!(Hash([2u8; 32]), avail.hash);

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

        // File1 should have been deleted, since it was only on peer1,
        assert!(matches!(acache.expect(&file1), Err(StorageError::NotFound)));

        // File2 should still be available, from peer2
        let file2_inode = acache.expect(&file2)?;
        let file2_availability = acache.file_availability(file2_inode)?;
        assert!(file2_availability.peers.contains(&peer2));

        // File3 should still be available, from peer3
        let file3_inode = acache.expect(&file3)?;
        let file3_availability = acache.file_availability(file3_inode)?;
        assert!(file3_availability.peers.contains(&peer3));

        // File4 should still be available, from peer1
        let file4_inode = acache.expect(&file4)?;
        let file4_availability = acache.file_availability(file4_inode)?;
        assert!(file4_availability.peers.contains(&peer1));

        Ok(())
    }

    // Tests for local_availability method
    #[tokio::test]
    async fn local_availability_missing_when_no_blob() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let file_path = Path::parse("test.txt")?;
        let mtime = test_time();

        // Add a file without opening it (so no blob is created)
        fixture.add_to_cache(&file_path, 100, mtime)?;

        let acache = &fixture.acache;
        let inode = acache.expect(&file_path)?;
        let availability = acache.local_availability(inode)?;

        assert!(matches!(availability, LocalAvailability::Missing));

        Ok(())
    }

    #[tokio::test]
    async fn local_availability_missing_when_blob_empty() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let file_path = Path::parse("test.txt")?;
        let mtime = test_time();

        // Add a file and open it to create a blob
        fixture.add_to_cache(&file_path, 100, mtime)?;
        let acache = &fixture.acache;
        let inode = acache.expect(&file_path)?;

        // Open the file to create a blob but don't write anything
        let _blob = fixture.acache.open_file(inode)?;

        let availability = acache.local_availability(inode)?;

        assert!(matches!(availability, LocalAvailability::Missing));

        Ok(())
    }

    #[tokio::test]
    async fn local_availability_partial_when_blob_incomplete() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let file_path = Path::parse("test.txt")?;
        let mtime = test_time();

        // Add a file and open it to create a blob
        fixture.add_to_cache(&file_path, 100, mtime)?;
        let acache = &fixture.acache;
        let inode = acache.expect(&file_path)?;

        // Open the file and write only part of it
        let mut blob = fixture.acache.open_file(inode)?;
        blob.write_all(b"partial data").await?;
        blob.flush_and_sync().await?;
        blob.update_db().await?;

        let availability = acache.local_availability(inode)?;

        match availability {
            LocalAvailability::Partial(size, ranges) => {
                assert_eq!(size, 100);
                assert!(!ranges.is_empty());
                assert!(ranges.bytecount() < 100);
            }
            _ => panic!("Expected Partial availability, got {:?}", availability),
        }

        Ok(())
    }

    #[tokio::test]
    async fn local_availability_complete_when_blob_full_but_unverified() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let file_path = Path::parse("test.txt")?;
        let mtime = test_time();

        // Add a file and open it to create a blob
        fixture.add_to_cache(&file_path, 100, mtime)?;
        let acache = &fixture.acache;
        let inode = acache.expect(&file_path)?;

        // Open the file and write the full content
        let mut blob = fixture.acache.open_file(inode)?;
        let data = vec![b'x'; 100];
        blob.write_all(&data).await?;
        blob.flush_and_sync().await?;
        blob.update_db().await?;

        let availability = acache.local_availability(inode)?;

        assert!(matches!(availability, LocalAvailability::Complete));

        Ok(())
    }

    #[tokio::test]
    async fn local_availability_verified_when_blob_full_and_verified() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let file_path = Path::parse("test.txt")?;
        let mtime = test_time();

        // Add a file and open it to create a blob
        fixture.add_to_cache(&file_path, 100, mtime)?;
        let acache = &fixture.acache;
        let inode = acache.expect(&file_path)?;

        // Open the file and write the full content
        let mut blob = fixture.acache.open_file(inode)?;
        let data = vec![b'x'; 100];
        blob.write_all(&data).await?;
        blob.flush_and_sync().await?;
        blob.update_db().await?;

        // Mark the blob as verified
        blob.mark_verified().await?;

        let availability = acache.local_availability(inode)?;

        assert!(matches!(availability, LocalAvailability::Verified));

        Ok(())
    }

    #[tokio::test]
    async fn local_availability_partial_with_multiple_ranges() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let file_path = Path::parse("test.txt")?;
        let mtime = test_time();

        // Add a file and open it to create a blob
        fixture.add_to_cache(&file_path, 1000, mtime)?;
        let acache = &fixture.acache;
        let inode = acache.expect(&file_path)?;

        // Open the file and write data in non-contiguous ranges
        let mut blob = fixture.acache.open_file(inode)?;

        // Write first range: 0-100
        blob.seek(std::io::SeekFrom::Start(0)).await?;
        blob.write_all(&vec![b'a'; 100]).await?;

        // Write second range: 500-600
        blob.seek(std::io::SeekFrom::Start(500)).await?;
        blob.write_all(&vec![b'b'; 100]).await?;

        // Write third range: 900-1000
        blob.seek(std::io::SeekFrom::Start(900)).await?;
        blob.write_all(&vec![b'c'; 100]).await?;

        blob.flush_and_sync().await?;
        blob.update_db().await?;

        let availability = acache.local_availability(inode)?;

        match availability {
            LocalAvailability::Partial(size, ranges) => {
                assert_eq!(size, 1000);
                assert_eq!(ranges.len(), 3); // Three separate ranges
                assert_eq!(ranges.bytecount(), 300); // Total 300 bytes written
            }
            _ => panic!("Expected Partial availability, got {:?}", availability),
        }

        Ok(())
    }

    #[tokio::test]
    async fn local_availability_handles_zero_size_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let file_path = Path::parse("empty.txt")?;
        let mtime = test_time();

        // Add a zero-size file
        fixture.add_to_cache(&file_path, 0, mtime)?;
        let acache = &fixture.acache;
        let inode = acache.expect(&file_path)?;
        assert!(matches!(
            acache.local_availability(inode)?,
            LocalAvailability::Missing
        ));

        // TODO: should a zero-length file be complete from the very
        // beginning? Will everything work even before a blob is
        // created?

        // Open the file to create a blob; this is all that's needed
        // for availability to switch to Complete.
        fixture.acache.open_file(inode)?;
        assert!(matches!(
            acache.local_availability(inode)?,
            LocalAvailability::Complete
        ));

        Ok(())
    }

    #[tokio::test]
    async fn local_availability_handles_nonexistent_inode() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let acache = &fixture.acache;
        let nonexistent_inode = Inode(999999);
        assert_eq!(
            LocalAvailability::Missing,
            acache.local_availability(nonexistent_inode)?,
        );

        Ok(())
    }

    #[tokio::test]
    async fn local_availability_updates_after_writing() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let file_path = Path::parse("test.txt")?;
        let mtime = test_time();

        // Add a file and open it to create a blob
        fixture.add_to_cache(&file_path, 100, mtime)?;
        let acache = &fixture.acache;
        let inode = acache.expect(&file_path)?;

        // Initially should be missing
        let availability = acache.local_availability(inode)?;
        assert!(matches!(availability, LocalAvailability::Missing));

        // Open and write half the file
        {
            let mut blob = fixture.acache.open_file(inode)?;
            blob.write_all(&vec![b'x'; 50]).await?;
            blob.flush_and_sync().await?;
            blob.update_db().await?;
        }

        // Should now be partial
        let availability = acache.local_availability(inode)?;
        match availability {
            LocalAvailability::Partial(size, ranges) => {
                assert_eq!(size, 100);
                assert_eq!(ranges.bytecount(), 50);
            }
            _ => panic!("Expected Partial availability, got {:?}", availability),
        }

        // Write the rest of the file
        {
            let mut blob = fixture.acache.open_file(inode)?;
            blob.seek(std::io::SeekFrom::Start(50)).await?;
            blob.write_all(&vec![b'y'; 50]).await?;
            blob.flush_and_sync().await?;
            blob.update_db().await?;
        }

        // Should now be complete
        let availability = acache.local_availability(inode)?;
        assert!(matches!(availability, LocalAvailability::Complete));

        Ok(())
    }

    // RealIndex trait tests
    #[tokio::test]
    async fn real_index_reopen_keeps_uuid() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;
        let arena = test_arena();
        let path = tempdir.path().join("index.db");
        let allocator =
            InodeAllocator::new(GlobalDatabase::new(redb_utils::in_memory()?)?, [arena])?;
        let db = ArenaDatabase::new(
            redb::Database::create(&path)?,
            arena,
            Arc::clone(&allocator),
        )?;
        let blob_dir = tempdir.child("blobs");
        let acache = ArenaCache::new(arena, db, blob_dir.path())?;
        let uuid = acache.uuid().clone();

        // Drop the first instance to release the database lock
        drop(acache);

        let db = ArenaDatabase::new(
            redb::Database::create(&path)?,
            arena,
            Arc::clone(&allocator),
        )?;
        let acache = ArenaCache::new(arena, db, blob_dir.path())?;
        assert!(!uuid.is_nil());
        assert_eq!(uuid, acache.uuid().clone());

        Ok(())
    }

    #[tokio::test]
    async fn real_index_add_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = fixture.as_real_index();

        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar.txt")?;
        index.add_file(&path, 100, mtime, Hash([0xfa; 32]))?;

        // Verify the file was added
        assert!(index.has_file(&path)?);
        let entry = index.get_file(&path)?.unwrap();
        assert_eq!(entry.size, 100);
        assert_eq!(entry.mtime, mtime);
        assert_eq!(entry.hash, Hash([0xfa; 32]));

        Ok(())
    }

    #[tokio::test]
    async fn real_index_add_file_if_matches_success() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = fixture.as_real_index();

        // Create a file on disk
        let tempdir = TempDir::new()?;
        let file_path = tempdir.child("foo");
        file_path.write_str("foo")?;

        assert!(index.add_file_if_matches(
            tempdir.path(),
            &realize_types::Path::parse("foo")?,
            3,
            UnixTime::mtime(&file_path.path().metadata()?),
            hash::digest("foo"),
        )?);
        assert!(index.has_file(&realize_types::Path::parse("foo")?)?);

        Ok(())
    }

    #[tokio::test]
    async fn real_index_add_file_if_matches_time_mismatch() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = fixture.as_real_index();

        // Create a file on disk
        let tempdir = TempDir::new()?;
        let file_path = tempdir.child("foo");
        file_path.write_str("foo")?;

        assert!(!index.add_file_if_matches(
            tempdir.path(),
            &realize_types::Path::parse("foo")?,
            3,
            UnixTime::from_secs(1234567890),
            hash::digest("foo"),
        )?);
        assert!(!index.has_file(&realize_types::Path::parse("foo")?)?);

        Ok(())
    }

    #[tokio::test]
    async fn real_index_add_file_if_matches_size_mismatch() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = fixture.as_real_index();

        // Create a file on disk
        let tempdir = TempDir::new()?;
        let file_path = tempdir.child("foo");
        file_path.write_str("foo")?;

        assert!(!index.add_file_if_matches(
            tempdir.path(),
            &realize_types::Path::parse("foo")?,
            2,
            UnixTime::mtime(&file_path.path().metadata()?),
            hash::digest("foo"),
        )?);
        assert!(!index.has_file(&realize_types::Path::parse("foo")?)?);

        Ok(())
    }

    #[tokio::test]
    async fn real_index_add_file_if_matches_missing() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = fixture.as_real_index();

        // Create a file on disk then remove it
        let tempdir = TempDir::new()?;
        let file_path = tempdir.child("foo");
        file_path.write_str("foo")?;
        let mtime = UnixTime::mtime(&file_path.path().metadata()?);
        fs::remove_file(file_path.path())?;

        assert!(!index.add_file_if_matches(
            tempdir.path(),
            &realize_types::Path::parse("foo")?,
            3,
            mtime,
            hash::digest("foo"),
        )?);
        assert!(!index.has_file(&realize_types::Path::parse("foo")?)?);

        Ok(())
    }

    #[tokio::test]
    async fn real_index_replace_file_in_index() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = fixture.as_real_index();

        let mtime1 = UnixTime::from_secs(1234567890);
        let mtime2 = UnixTime::from_secs(1234567891);
        let path = realize_types::Path::parse("foo/bar.txt")?;
        index.add_file(&path, 100, mtime1, Hash([0xfa; 32]))?;
        index.add_file(&path, 200, mtime2, Hash([0x07; 32]))?;

        // Verify the file was replaced
        assert!(index.has_file(&path)?);
        let entry = index.get_file(&path)?.unwrap();
        assert_eq!(entry.size, 200);
        assert_eq!(entry.mtime, mtime2);
        assert_eq!(entry.hash, Hash([0x07; 32]));

        Ok(())
    }

    #[tokio::test]
    async fn real_index_has_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = fixture.as_real_index();

        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo.txt")?;
        index.add_file(&path, 100, mtime, Hash([0xfa; 32]))?;

        assert!(index.has_file(&path)?);
        assert!(!index.has_file(&realize_types::Path::parse("bar.txt")?)?);
        assert!(!index.has_file(&realize_types::Path::parse("other.txt")?)?);

        Ok(())
    }

    #[tokio::test]
    async fn real_index_get_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = fixture.as_real_index();

        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar")?;
        let hash = Hash([0xfa; 32]);
        index.add_file(&path, 100, mtime, hash.clone())?;

        let entry = index.get_file(&path)?.unwrap();
        assert_eq!(entry.size, 100);
        assert_eq!(entry.mtime, mtime);
        assert_eq!(entry.hash, hash);

        Ok(())
    }

    #[tokio::test]
    async fn real_index_has_matching_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = fixture.as_real_index();

        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar")?;
        index.add_file(&path, 100, mtime, Hash([0xfa; 32]))?;

        assert!(index.has_matching_file(&path, 100, mtime)?);
        assert!(!index.has_matching_file(&realize_types::Path::parse("other")?, 100, mtime)?);
        assert!(!index.has_matching_file(&path, 200, mtime)?);
        assert!(!index.has_matching_file(&path, 100, UnixTime::from_secs(1234567891))?);

        Ok(())
    }

    #[tokio::test]
    async fn real_index_remove_file_or_dir() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = fixture.as_real_index();

        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar.txt")?;
        index.add_file(&path, 100, mtime, Hash([0xfa; 32]))?;
        index.remove_file_or_dir(&path)?;

        assert!(!index.has_file(&path)?);

        Ok(())
    }

    #[tokio::test]
    async fn real_index_remove_file_if_missing_success() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = fixture.as_real_index();

        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("bar.txt")?;
        index.add_file(&path, 100, mtime, Hash([0xfa; 32]))?;
        assert!(index.remove_file_if_missing(&std::path::Path::new("/tmp"), &path)?);

        assert!(!index.has_file(&path)?);

        Ok(())
    }

    #[tokio::test]
    async fn real_index_remove_file_if_missing_failure() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = fixture.as_real_index();

        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("bar.txt")?;
        index.add_file(&path, 100, mtime, Hash([0xfa; 32]))?;

        // Create the file on disk
        let tempdir = TempDir::new()?;
        let file_path = tempdir.child("bar.txt");
        file_path.write_str("content")?;

        assert!(!index.remove_file_if_missing(tempdir.path(), &path)?);
        assert!(index.has_file(&path)?);

        Ok(())
    }

    #[tokio::test]
    async fn real_index_remove_dir_from_index() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = fixture.as_real_index();

        let mtime = UnixTime::from_secs(1234567890);

        index.add_file(
            &realize_types::Path::parse("foo/a")?,
            100,
            mtime,
            Hash([1; 32]),
        )?;
        index.add_file(
            &realize_types::Path::parse("foo/b")?,
            100,
            mtime,
            Hash([2; 32]),
        )?;
        index.add_file(
            &realize_types::Path::parse("foo/c")?,
            100,
            mtime,
            Hash([3; 32]),
        )?;
        index.add_file(
            &realize_types::Path::parse("foobar")?,
            100,
            mtime,
            Hash([0x04; 32]),
        )?;

        // Remove the directory
        index.remove_file_or_dir(&realize_types::Path::parse("foo")?)?;

        // Verify all files in the directory were removed
        assert!(!index.has_file(&realize_types::Path::parse("foo/a")?)?);
        assert!(!index.has_file(&realize_types::Path::parse("foo/b")?)?);
        assert!(!index.has_file(&realize_types::Path::parse("foo/c")?)?);

        // But the file outside the directory should remain
        assert!(index.has_file(&realize_types::Path::parse("foobar")?)?);

        Ok(())
    }

    #[tokio::test]
    async fn real_index_all_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = fixture.as_real_index();

        let mtime = UnixTime::from_secs(1234567890);
        let path1 = realize_types::Path::parse("foo/a")?;
        let path2 = realize_types::Path::parse("foo/b")?;
        let path3 = realize_types::Path::parse("bar.txt")?;

        index.add_file(&path1, 100, mtime, Hash([1; 32]))?;
        index.add_file(&path2, 200, mtime, Hash([2; 32]))?;
        index.add_file(&path3, 300, mtime, Hash([3; 32]))?;

        let (tx, mut rx) = mpsc::channel(10);
        let task = tokio::task::spawn_blocking({
            let index = index.clone();

            move || index.all_files(tx)
        });

        let mut files = HashMap::new();
        while let Some((path, entry)) = rx.recv().await {
            files.insert(path, entry);
        }
        task.await??; // make sure there are no errors

        assert_eq!(files.len(), 3);
        assert!(files.contains_key(&path1));
        assert!(files.contains_key(&path2));
        assert!(files.contains_key(&path3));

        Ok(())
    }

    #[tokio::test]
    async fn real_index_history() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = fixture.as_real_index();

        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar.txt")?;
        index.add_file(&path, 100, mtime, Hash([0xfa; 32]))?;

        let (tx, mut rx) = mpsc::channel(10);
        let task = tokio::task::spawn_blocking({
            let index = index.clone();

            move || index.history(1..2, tx)
        });

        let mut entries = Vec::new();
        while let Some(entry) = rx.recv().await {
            entries.push(entry);
        }
        assert_eq!(entries.len(), 1);
        assert!(matches!(entries[0], Ok((1, HistoryTableEntry::Add(_)))));
        task.await??; // make sure there are no errors

        Ok(())
    }

    #[tokio::test]
    async fn real_index_watch_history() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = fixture.as_real_index();

        let mut rx = index.watch_history();
        let initial = *rx.borrow();

        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar.txt")?;
        index.add_file(&path, 100, mtime, Hash([0xfa; 32]))?;

        // Wait for the history to be updated
        let timeout = tokio::time::Duration::from_millis(100);
        let _ = tokio::time::timeout(timeout, rx.changed()).await;

        let updated = *rx.borrow();
        assert!(updated > initial);

        Ok(())
    }

    #[tokio::test]
    async fn real_index_last_history_index() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = fixture.as_real_index();

        let initial = index.last_history_index()?;

        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("foo/bar.txt")?;
        index.add_file(&path, 100, mtime, Hash([0xfa; 32]))?;

        let updated = index.last_history_index()?;
        assert!(updated > initial);

        Ok(())
    }

    #[tokio::test]
    async fn real_index_drop_file_if_matches_success() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = fixture.as_real_index();

        let path = realize_types::Path::parse("test.txt")?;
        let content = "test content";
        let hash = hash::digest(content);

        // Add file to index
        index.add_file(
            &path,
            content.len() as u64,
            UnixTime::from_secs(1234567890),
            hash.clone(),
        )?;

        // Verify file exists in index
        assert!(index.has_file(&path)?);

        // Get the initial history count
        let initial_history_count = index.last_history_index()?;

        // Try to drop the file - this should fail because file doesn't exist on disk
        // but the test is checking that the method returns the correct result
        {
            let txn = fixture.db.begin_write()?;
            let result =
                index.drop_file_if_matches(&txn, std::path::Path::new("/tmp"), &path, &hash)?;
            // The method should return false because file_matches_index will fail
            // since the file doesn't exist on disk at /tmp/test.txt
            assert!(!result);
            txn.commit()?;
        }

        // Verify file still exists in index (since drop failed)
        assert!(index.has_file(&path)?);

        // Verify no new history entry was created
        let final_history_count = index.last_history_index()?;
        assert_eq!(initial_history_count, final_history_count);

        Ok(())
    }

    #[tokio::test]
    async fn real_index_drop_file_if_matches_wrong_hash() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = fixture.as_real_index();

        let path = realize_types::Path::parse("test.txt")?;
        let content = "test content";
        let hash = hash::digest(content);
        let wrong_hash = hash::digest("wrong content");

        // Add file to index
        index.add_file(
            &path,
            content.len() as u64,
            UnixTime::from_secs(1234567890),
            hash.clone(),
        )?;

        // Verify file exists in index
        assert!(index.has_file(&path)?);

        // Get the initial history count
        let initial_history_count = index.last_history_index()?;

        // Try to drop the file with wrong hash
        {
            let txn = fixture.db.begin_write()?;
            let result = index.drop_file_if_matches(
                &txn,
                std::path::Path::new("/tmp"),
                &path,
                &wrong_hash,
            )?;
            assert!(!result);
            txn.commit()?;
        }

        // Verify file still exists in index
        assert!(index.has_file(&path)?);

        // Verify no new history entry was created
        let final_history_count = index.last_history_index()?;
        assert_eq!(initial_history_count, final_history_count);

        Ok(())
    }

    #[tokio::test]
    async fn real_index_drop_file_if_matches_file_not_in_index() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let index = fixture.as_real_index();

        let path = realize_types::Path::parse("test.txt")?;
        let hash = hash::digest("test content");

        // Verify file doesn't exist in index
        assert!(!index.has_file(&path)?);

        // Get the initial history count
        let initial_history_count = index.last_history_index()?;

        // Try to drop the file
        {
            let txn = fixture.db.begin_write()?;
            let result =
                index.drop_file_if_matches(&txn, std::path::Path::new("/tmp"), &path, &hash)?;
            assert!(!result);
            txn.commit()?;
        }

        // Verify file still doesn't exist in index
        assert!(!index.has_file(&path)?);

        // Verify no new history entry was created
        let final_history_count = index.last_history_index()?;
        assert_eq!(initial_history_count, final_history_count);

        Ok(())
    }

    struct MarksFixture {
        arena: Arena,
        db: Arc<ArenaDatabase>,
        acache: Arc<ArenaCache>,
        index: Arc<dyn RealIndex>,
        marks: Arc<dyn PathMarks>,
    }

    impl MarksFixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();

            let arena = Arena::from("test");
            let acache = ArenaCache::for_testing_single_arena(
                arena,
                &std::path::PathBuf::from("/dev/null"),
            )?;
            let index = acache.as_index();
            let marks = acache.clone() as Arc<dyn PathMarks>;

            Ok(Self {
                arena,
                db: acache.db(),
                acache,
                index,
                marks,
            })
        }

        /// Clear all dirty flags in both index and cache
        fn clear_all_dirty(&self) -> anyhow::Result<()> {
            let txn = self.db.begin_write()?;
            {
                let mut dirty = txn.write_dirty()?;
                while dirty.take_next_dirty_path()?.is_some() {}
            }
            txn.commit()?;

            Ok(())
        }

        /// Check if a path is dirty in the index
        fn is_dirty<T>(&self, path: T) -> anyhow::Result<bool>
        where
            T: AsRef<Path>,
        {
            let path = path.as_ref();
            let txn = self.db.begin_read()?;
            let dirty = txn.read_dirty()?;
            Ok(dirty.is_dirty(path)?)
        }

        /// Add a file to the index for testing
        fn add_file_to_index<T>(&self, path: T) -> anyhow::Result<()>
        where
            T: AsRef<Path>,
        {
            let path = path.as_ref();
            Ok(self
                .index
                .add_file(path, 100, UnixTime::from_secs(1234567889), Hash([1; 32]))?)
        }

        /// Add a file to the cache for testing
        fn add_file_to_cache<T>(&self, path: T) -> anyhow::Result<()>
        where
            T: AsRef<Path>,
        {
            let path = path.as_ref();
            use crate::arena::notifier::Notification;
            use realize_types::Peer;

            let test_peer = Peer::from("test-peer");
            let notification = Notification::Add {
                arena: self.arena,
                index: 1,
                path: path.clone(),
                mtime: UnixTime::from_secs(1234567890),
                size: 100,
                hash: Hash([2; 32]),
            };

            self.acache.update(test_peer, notification, None)?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn default_mark() -> anyhow::Result<()> {
        let fixture = MarksFixture::setup().await?;

        // New PathMarks should return default mark (Watch) for any path
        let path = Path::parse("some/file.txt")?;
        fixture.add_file_to_index(&path)?;

        let mark = fixture.marks.get_mark(&path)?;

        assert_eq!(mark, Mark::Watch);

        Ok(())
    }

    #[tokio::test]
    async fn set_and_get_root_mark() -> anyhow::Result<()> {
        let fixture = MarksFixture::setup().await?;

        // Set root mark to Keep
        fixture.marks.set_arena_mark(Mark::Keep)?;

        // Verify root mark is returned for any path
        let path = Path::parse("some/file.txt")?;
        fixture.add_file_to_index(&path)?;

        let mark = fixture.marks.get_mark(&path)?;

        assert_eq!(mark, Mark::Keep);

        // Change root mark to Own
        fixture.marks.set_arena_mark(Mark::Own)?;
        let mark = fixture.marks.get_mark(&path)?;

        assert_eq!(mark, Mark::Own);

        Ok(())
    }

    #[tokio::test]
    async fn set_and_get_path_mark() -> anyhow::Result<()> {
        let fixture = MarksFixture::setup().await?;

        let file_path = Path::parse("dir/file.txt")?;
        fixture.add_file_to_index(&file_path)?;
        let dir_path = Path::parse("dir")?;

        // Set mark on specific file
        fixture.marks.set_mark(&file_path, Mark::Own)?;

        // Verify file has the mark
        let mark = fixture.marks.get_mark(&file_path)?;
        assert_eq!(mark, Mark::Own);

        // Verify other files still get default mark
        let other_file = Path::parse("other/file.txt")?;
        fixture.add_file_to_index(&other_file)?;
        let mark = fixture.marks.get_mark(&other_file)?;
        assert_eq!(mark, Mark::Watch);

        // Set mark on directory
        fixture.marks.set_mark(&dir_path, Mark::Keep)?;

        // Verify directory has the mark
        let mark = fixture.marks.get_mark(&dir_path)?;
        assert_eq!(mark, Mark::Keep);

        // Verify files in directory inherit the mark
        let file_in_dir = Path::parse("dir/another.txt")?;
        fixture.add_file_to_index(&file_in_dir)?;
        let mark = fixture.marks.get_mark(&file_in_dir)?;
        assert_eq!(mark, Mark::Keep);

        // But the specific file still has its own mark
        let mark = fixture.marks.get_mark(&file_path)?;
        assert_eq!(mark, Mark::Own);

        Ok(())
    }

    #[tokio::test]
    async fn hierarchical_mark_inheritance() -> anyhow::Result<()> {
        let fixture = MarksFixture::setup().await?;

        let dir_path = Path::parse("parent/child")?;
        let file_path = Path::parse("parent/child/file.txt")?;
        let root_file = Path::parse("other/file.txt")?;
        let parent_file = Path::parse("parent/file.txt")?;
        let child_file = Path::parse("parent/child/other.txt")?;
        let specific_file = Path::parse("parent/child/file.txt")?;

        for path in [
            &file_path,
            &root_file,
            &parent_file,
            &child_file,
            &specific_file,
        ] {
            fixture.add_file_to_index(path)?;
        }

        fixture.marks.set_arena_mark(Mark::Watch)?;
        fixture.marks.set_mark(&dir_path, Mark::Keep)?;
        fixture.marks.set_mark(&file_path, Mark::Own)?;

        // Test inheritance hierarchy
        assert_eq!(fixture.marks.get_mark(&root_file)?, Mark::Watch);
        assert_eq!(fixture.marks.get_mark(&parent_file)?, Mark::Watch);
        assert_eq!(fixture.marks.get_mark(&child_file)?, Mark::Keep);
        assert_eq!(fixture.marks.get_mark(&specific_file)?, Mark::Own);

        Ok(())
    }

    #[tokio::test]
    async fn hierarchical_mark_inheritance_nonexistent() -> anyhow::Result<()> {
        let fixture = MarksFixture::setup().await?;
        fixture.add_file_to_index(&Path::parse("foo/bar/baz/waldo")?)?;

        fixture.marks.set_arena_mark(Mark::Watch)?;
        fixture.marks.set_mark(&Path::parse("foo")?, Mark::Own)?;
        fixture
            .marks
            .set_mark(&Path::parse("foo/bar/baz")?, Mark::Keep)?;

        // in foo, but not in baz
        assert_eq!(fixture.marks.get_mark(&Path::parse("foo/qux")?)?, Mark::Own);
        assert_eq!(
            fixture.marks.get_mark(&Path::parse("foo/bar/qux")?)?,
            Mark::Own
        );
        // in baz
        assert_eq!(
            fixture
                .marks
                .get_mark(&Path::parse("foo/bar/baz/qux/quuux")?)?,
            Mark::Keep
        );

        // neither in foo, nor in baz
        assert_eq!(fixture.marks.get_mark(&Path::parse("waldo")?)?, Mark::Watch);
        assert_eq!(
            fixture.marks.get_mark(&Path::parse("waldo/fred")?)?,
            Mark::Watch
        );

        Ok(())
    }

    #[tokio::test]
    async fn clear_mark() -> anyhow::Result<()> {
        let fixture = MarksFixture::setup().await?;

        let file_path = Path::parse("dir/file.txt")?;
        fixture.add_file_to_index(&file_path)?;
        let dir_path = Path::parse("dir")?;

        // Set marks
        fixture.marks.set_mark(&file_path, Mark::Own)?;
        fixture.marks.set_mark(&dir_path, Mark::Keep)?;

        // Verify marks are set
        assert_eq!(fixture.marks.get_mark(&file_path)?, Mark::Own);
        assert_eq!(fixture.marks.get_mark(&dir_path)?, Mark::Keep);

        // Clear file mark
        fixture.marks.clear_mark(&file_path)?;

        // File should now inherit from directory
        assert_eq!(fixture.marks.get_mark(&file_path)?, Mark::Keep);

        // Clear directory mark
        fixture.marks.clear_mark(&dir_path)?;

        // Both should now get default mark
        assert_eq!(fixture.marks.get_mark(&file_path)?, Mark::Watch);
        assert_eq!(fixture.marks.get_mark(&dir_path)?, Mark::Watch);

        // Clearing non-existent mark should be no-op
        let otherfile = Path::parse("dir/otherfile.txt")?;
        fixture.add_file_to_index(&otherfile)?;
        fixture.marks.clear_mark(&otherfile)?;

        Ok(())
    }

    #[tokio::test]
    async fn clear_mark_otherwise_nonexistent_file() -> anyhow::Result<()> {
        let fixture = MarksFixture::setup().await?;

        fixture.marks.set_arena_mark(Mark::Keep)?;
        let path = Path::parse("foo/bar")?;
        fixture.marks.set_mark(&path, Mark::Own)?;

        // after this call, the path has no inode anymore, make sure this
        // also doesn't return an error.
        fixture.marks.clear_mark(&path)?;

        assert_eq!(fixture.marks.get_mark(&path)?, Mark::Keep);
        let txn = fixture.db.begin_read()?;
        let tree = txn.read_tree()?;
        assert!(tree.resolve(&path)?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn clear_mark_nonexistent_file() -> anyhow::Result<()> {
        let fixture = MarksFixture::setup().await?;

        // this mainly makes sure that nothing returns "not found"
        // even though the path doesn't exist in the cache, index, or
        // marks.
        fixture.marks.clear_mark(&Path::parse("doesnotexist")?)?;

        Ok(())
    }

    #[tokio::test]
    async fn clear_mark_with_root_mark() -> anyhow::Result<()> {
        let fixture = MarksFixture::setup().await?;

        // Set root mark
        fixture.marks.set_arena_mark(Mark::Keep)?;

        let file_path = Path::parse("dir/file.txt")?;
        fixture.add_file_to_index(&file_path)?;

        // Set specific file mark
        fixture.marks.set_mark(&file_path, Mark::Own)?;
        assert_eq!(fixture.marks.get_mark(&file_path)?, Mark::Own);

        // Clear file mark
        fixture.marks.clear_mark(&file_path)?;

        // File should now inherit from root
        assert_eq!(fixture.marks.get_mark(&file_path)?, Mark::Keep);

        Ok(())
    }

    #[tokio::test]
    async fn all_mark_types() -> anyhow::Result<()> {
        let fixture = MarksFixture::setup().await?;

        let watch_path = Path::parse("watch/file.txt")?;
        fixture.add_file_to_index(&watch_path)?;

        let keep_path = Path::parse("keep/file.txt")?;
        fixture.add_file_to_index(&keep_path)?;

        let own_path = Path::parse("own/file.txt")?;
        fixture.add_file_to_index(&own_path)?;

        // Test all mark types
        fixture.marks.set_mark(&watch_path, Mark::Watch)?;
        fixture.marks.set_mark(&keep_path, Mark::Keep)?;
        fixture.marks.set_mark(&own_path, Mark::Own)?;

        assert_eq!(fixture.marks.get_mark(&watch_path)?, Mark::Watch);
        assert_eq!(fixture.marks.get_mark(&keep_path)?, Mark::Keep);
        assert_eq!(fixture.marks.get_mark(&own_path)?, Mark::Own);

        Ok(())
    }

    #[tokio::test]
    async fn mark_changes() -> anyhow::Result<()> {
        let fixture = MarksFixture::setup().await?;

        let file_path = Path::parse("test/file.txt")?;
        fixture.add_file_to_index(&file_path)?;

        // Set initial mark
        fixture.marks.set_mark(&file_path, Mark::Watch)?;
        assert_eq!(fixture.marks.get_mark(&file_path)?, Mark::Watch);

        // Change mark
        fixture.marks.set_mark(&file_path, Mark::Keep)?;
        assert_eq!(fixture.marks.get_mark(&file_path)?, Mark::Keep);

        // Change again
        fixture.marks.set_mark(&file_path, Mark::Own)?;
        assert_eq!(fixture.marks.get_mark(&file_path)?, Mark::Own);

        // Change back
        fixture.marks.set_mark(&file_path, Mark::Watch)?;
        assert_eq!(fixture.marks.get_mark(&file_path)?, Mark::Watch);

        Ok(())
    }

    #[tokio::test]
    async fn set_mark_marks_dirty_when_effective() -> anyhow::Result<()> {
        let fixture = MarksFixture::setup().await?;

        // Add files to both index and cache
        let in_index = Path::parse("test/in_index.txt")?;
        fixture.add_file_to_index(&in_index)?;

        let in_cache = Path::parse("test/in_cache.txt")?;
        fixture.add_file_to_cache(&in_cache)?;

        fixture.clear_all_dirty()?;

        // Set a mark that changes the effective mark (from Watch to Own)
        fixture.marks.set_mark(&in_index, Mark::Own)?;
        fixture.marks.set_mark(&in_cache, Mark::Own)?;

        // Check that the file is marked dirty
        assert!(fixture.is_dirty(&in_index)?);
        assert!(fixture.is_dirty(&in_cache)?);

        Ok(())
    }

    #[tokio::test]
    async fn set_mark_does_not_mark_dirty_when_not_effective() -> anyhow::Result<()> {
        let fixture = MarksFixture::setup().await?;

        // Add files to both index and cache
        let file_path = Path::parse("test/file.txt")?;
        fixture.add_file_to_index(&file_path)?;

        fixture.clear_all_dirty()?;

        // Set the same mark that's already effective (Watch is default)
        fixture.marks.set_mark(&file_path, Mark::Watch)?;

        // Check that the file is NOT marked dirty since the effective mark didn't change
        assert!(!fixture.is_dirty(&file_path)?);

        Ok(())
    }

    #[tokio::test]
    async fn set_root_mark_marks_all_dirty() -> anyhow::Result<()> {
        let fixture = MarksFixture::setup().await?;

        // Add multiple files to both index and cache
        let index_files = vec![
            Path::parse("dir1/file1.txt")?,
            Path::parse("dir1/file2.txt")?,
            Path::parse("dir2/file3.txt")?,
            Path::parse("file4.txt")?,
        ];
        for file in &index_files {
            fixture.add_file_to_index(file)?;
        }

        let cache_files = vec![
            Path::parse("dir3/file5.txt")?,
            Path::parse("dir3/file6.txt")?,
            Path::parse("dir4/file7.txt")?,
            Path::parse("file8.txt")?,
        ];
        for file in &cache_files {
            fixture.add_file_to_index(file)?;
        }

        fixture.clear_all_dirty()?;

        // Set root mark that changes the effective mark for all files
        fixture.marks.set_arena_mark(Mark::Keep)?;

        // Check that all files are marked dirty
        for file in &index_files {
            assert!(fixture.is_dirty(file)?, "{file} should be dirty");
        }
        for file in &cache_files {
            assert!(fixture.is_dirty(file)?, "{file} should be dirty");
        }

        Ok(())
    }

    #[tokio::test]
    async fn clear_mark_marks_dirty_when_effective() -> anyhow::Result<()> {
        let fixture = MarksFixture::setup().await?;

        // Add files to both index and cache
        let file_path = Path::parse("test/file.txt")?;
        fixture.add_file_to_index(&file_path)?;

        // Set a specific mark first
        fixture.marks.set_mark(&file_path, Mark::Own)?;
        fixture.clear_all_dirty()?;

        // Clear the mark, which should change the effective mark back to default
        fixture.marks.clear_mark(&file_path)?;

        // Check that the file is marked dirty
        assert!(fixture.is_dirty(&file_path)?);

        Ok(())
    }

    #[tokio::test]
    async fn clear_mark_does_not_mark_dirty_when_it_did_not_exist() -> anyhow::Result<()> {
        let fixture = MarksFixture::setup().await?;

        // Add files to both index and cache
        let file_path = Path::parse("test/file.txt")?;
        fixture.add_file_to_index(&file_path)?;

        fixture.clear_all_dirty()?;

        // Clear a mark that doesn't exist (file already has default Watch mark)
        fixture.marks.clear_mark(&file_path)?;

        // Check that the file is NOT marked dirty since the effective mark didn't change
        assert!(!fixture.is_dirty(&file_path)?);

        Ok(())
    }

    #[tokio::test]
    async fn clear_mark_does_not_mark_dirty_when_not_effective() -> anyhow::Result<()> {
        let fixture = MarksFixture::setup().await?;

        // Add files to both index and cache
        let file_path = Path::parse("test/file.txt")?;
        fixture.add_file_to_index(&file_path)?;
        fixture.marks.set_mark(&file_path, Mark::Watch)?; // the default
        fixture.clear_all_dirty()?;

        // Clearing the mark changes nothing, since this mark just sets the default.
        fixture.marks.clear_mark(&file_path)?;

        // Check that the file is NOT marked dirty since the effective mark didn't change
        assert!(!fixture.is_dirty(&file_path)?);

        Ok(())
    }

    #[tokio::test]
    async fn hierarchical_mark_changes_mark_dirty_recursively() -> anyhow::Result<()> {
        let fixture = MarksFixture::setup().await?;

        // Add files in a directory structure
        let dir_file1 = Path::parse("dir/file1.txt")?;
        let dir_file2 = Path::parse("dir/file2.txt")?;
        let dir_subdir_file3 = Path::parse("dir/subdir/file3.txt")?;
        let dir_subdir_file4 = Path::parse("dir/subdir/file4.txt")?;
        let notdir_file5 = Path::parse("other/file5.txt")?;
        let notdir_file6 = Path::parse("other/file6.txt")?;
        for file in vec![&dir_file1, &dir_subdir_file3, &notdir_file5] {
            fixture.add_file_to_index(file)?;
        }
        for file in vec![&dir_file2, &dir_subdir_file4, &notdir_file6] {
            fixture.add_file_to_cache(file)?;
        }
        fixture.clear_all_dirty()?;

        // Set a mark on the directory that changes the effective mark for files in that directory
        let dir_path = Path::parse("dir")?;
        fixture.marks.set_mark(&dir_path, Mark::Keep)?;

        // Check that files in the directory are marked dirty
        assert!(fixture.is_dirty(&dir_file1)?);
        assert!(fixture.is_dirty(&dir_file2)?);
        assert!(fixture.is_dirty(&dir_subdir_file3)?);
        assert!(fixture.is_dirty(&dir_subdir_file4)?);

        // Check that files outside the directory are NOT marked dirty
        assert!(!fixture.is_dirty(&notdir_file5)?);
        assert!(!fixture.is_dirty(&notdir_file6)?);

        Ok(())
    }

    #[tokio::test]
    async fn specific_file_mark_overrides_directory_mark() -> anyhow::Result<()> {
        let fixture = MarksFixture::setup().await?;

        // Add files to both index and cache
        let dir_path = Path::parse("dir")?;
        let file_path = Path::parse("dir/file.txt")?;
        fixture.add_file_to_index(&file_path)?;

        // Set directory mark first
        fixture.marks.set_mark(&dir_path, Mark::Keep)?;
        fixture.clear_all_dirty()?;

        // Set a different mark on the specific file
        fixture.marks.set_mark(&file_path, Mark::Own)?;

        // Check that the file is marked dirty (effective mark changed from Keep to Own)
        assert!(fixture.is_dirty(&file_path)?);

        Ok(())
    }

    #[tokio::test]
    async fn clear_directory_mark_affects_all_files_in_directory() -> anyhow::Result<()> {
        let fixture = MarksFixture::setup().await?;

        // Add files in a directory structure
        let files = vec![
            Path::parse("dir/file1.txt")?,
            Path::parse("dir/file2.txt")?,
            Path::parse("dir/subdir/file3.txt")?,
        ];

        for file in &files {
            fixture.add_file_to_index(file)?;
        }

        // Set directory mark
        let dir_path = Path::parse("dir")?;
        fixture.marks.set_mark(&dir_path, Mark::Keep)?;
        fixture.clear_all_dirty()?;

        // Clear the directory mark
        fixture.marks.clear_mark(&dir_path)?;

        // Check that all files in the directory are marked dirty (effective mark changed from Keep to Watch)
        for file in &files {
            assert!(fixture.is_dirty(file)?, "{file} should be dirty in index");
        }

        Ok(())
    }

    #[tokio::test]
    async fn mark_nonexistent_file() -> anyhow::Result<()> {
        let fixture = MarksFixture::setup().await?;

        // Set marks on paths that don't exist in index or cache
        fixture
            .marks
            .set_mark(&Path::parse("foo/bar")?, Mark::Own)?;
        assert_eq!(Mark::Own, fixture.marks.get_mark(&Path::parse("foo/bar")?)?);
        assert_eq!(
            Mark::Own,
            fixture.marks.get_mark(&Path::parse("foo/bar/baz")?)?,
        );
        assert_eq!(Mark::Watch, fixture.marks.get_mark(&Path::parse("waldo")?)?);

        // The path still doesn't exist in the cache, even though it has a mark
        assert!(matches!(
            fixture
                .acache
                .file_metadata(fixture.acache.expect(Path::parse("foo/bar")?)?),
            Err(StorageError::NotFound)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn root_mark_clearing_marks_all_dirty() -> anyhow::Result<()> {
        let fixture = MarksFixture::setup().await?;

        // Add multiple files to both index and cache
        let files_in_index = vec![
            Path::parse("dir1/file1.txt")?,
            Path::parse("dir2/file2.txt")?,
            Path::parse("file3.txt")?,
        ];
        for file in &files_in_index {
            fixture.add_file_to_index(file)?;
        }
        let files_in_cache = vec![
            Path::parse("dir3/file4.txt")?,
            Path::parse("dir4/file5.txt")?,
            Path::parse("file6.txt")?,
        ];
        for file in &files_in_cache {
            fixture.add_file_to_cache(file)?;
        }

        // Set root mark
        fixture.marks.set_arena_mark(Mark::Keep)?;
        fixture.clear_all_dirty()?;

        // Clear root mark (set to default Watch)
        fixture.marks.set_arena_mark(Mark::Watch)?;

        // Check that all files are marked dirty (effective mark changed from Keep to Watch)
        for file in &files_in_index {
            assert!(fixture.is_dirty(file)?, "{file} should be dirty");
        }
        for file in &files_in_cache {
            assert!(fixture.is_dirty(file)?, "{file} should be dirty");
        }

        Ok(())
    }
}
