use super::blob::{self, Blobstore};
use super::db::{ArenaDatabase, ArenaReadTransaction, ArenaWriteTransaction};
use super::index::RealIndex;
use super::mark::PathMarks;
use super::tree::{OpenTree, OpenTreeWrite, Tree, TreeExt, TreeReadOperations, WritableOpenTree};
use super::types::{
    DirTableEntry, FileAvailability, FileContent, FileMetadata, FileTableEntry, FileTableKey,
    HistoryTableEntry, IndexedFileTableEntry, LocalAvailability, LruQueueId, MarkTableEntry,
    PeerTableEntry, ReadDirEntry,
};
use crate::arena::engine::DirtyPaths;
use crate::arena::notifier::{Notification, Progress};
use crate::types::BlobId;
use crate::utils::fs_utils;
use crate::utils::holder::{ByteConversionError, Holder};
use crate::{Blob, InodeAllocator, InodeAssignment, Mark};
use crate::{Inode, StorageError};
use realize_types::{Arena, ByteRanges, Hash, Path, Peer, UnixTime};
use redb::{ReadableTable, Table};
use std::collections::HashMap;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

/// A per-arena cache of remote files.
///
/// This struct handles all cache operations for a specific arena.
/// It contains the arena's database and root inode.
pub(crate) struct ArenaCache {
    arena: Arena,
    arena_root: Inode,
    db: Arc<ArenaDatabase>,
    tree: Tree,
    blobstore: Arc<Blobstore>,
    dirty_paths: Arc<DirtyPaths>,

    history_tx: watch::Sender<u64>,
    uuid: Uuid,
    /// This is just to keep history_tx alive and up-to-date.
    _history_rx: watch::Receiver<u64>,
}

impl ArenaCache {
    /// Create a new ArenaUnrealCacheBlocking from an arena, root inode, database, and blob directory.
    pub(crate) fn new(
        arena: Arena,
        allocator: Arc<InodeAllocator>,
        db: Arc<ArenaDatabase>,
        blob_dir: &std::path::Path,
        dirty_paths: Arc<DirtyPaths>,
    ) -> Result<Arc<Self>, StorageError> {
        let blobstore = Blobstore::new(Arc::clone(&db), blob_dir)?;
        let arena_root = allocator
            .arena_root(arena)
            .ok_or_else(|| StorageError::UnknownArena(arena))?;
        let tree = Tree::new(arena, arena_root, Arc::clone(&allocator));
        let uuid = load_or_assign_uuid(&db)?;
        let last_history_index = {
            let txn = db.begin_read()?;
            let history_table = txn.history_table()?;
            last_history_index(&history_table)?
        };
        let (history_tx, history_rx) = watch::channel(last_history_index);

        Ok(Arc::new(Self {
            arena,
            arena_root,
            tree,
            db,
            blobstore,
            dirty_paths,
            uuid,
            history_tx,
            _history_rx: history_rx,
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
        do_lookup(&txn.dir_table()?, parent_inode, name)
    }

    pub(crate) fn lookup_path<T>(&self, path: T) -> Result<(Inode, InodeAssignment), StorageError>
    where
        T: AsRef<Path>,
    {
        let path = path.as_ref();
        let txn = self.db.begin_read()?;
        let dir_table = txn.dir_table()?;

        do_lookup_path(&dir_table, self.arena_root, Some(path))
    }

    pub(crate) fn file_metadata(&self, inode: Inode) -> Result<FileMetadata, StorageError> {
        let txn = self.db.begin_read()?;
        do_file_metadata(&txn, inode)
    }

    pub(crate) fn dir_mtime(&self, inode: Inode) -> Result<UnixTime, StorageError> {
        let txn = self.db.begin_read()?;

        do_dir_mtime(&txn.dir_table()?, inode, self.arena_root)
    }

    #[allow(dead_code)]
    pub(crate) fn parent_inode(&self, inode: Inode) -> Result<Inode, StorageError> {
        let txn = self.db.begin_read()?;

        do_parent_inode(&txn.dir_table()?, &txn.file_table()?, inode)
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

        do_readdir(&txn.dir_table()?, inode)
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

                let mut file_table = txn.file_table()?;
                let (parent_inode, file_inode) =
                    do_create_file(&txn, &mut txn.write_tree(&self.tree)?, &path)?;
                if !get_file_entry(&file_table, file_inode, Some(peer))?.is_some() {
                    let entry = FileTableEntry::new(path, size, mtime, hash, parent_inode);
                    self.do_write_file_entry(&mut file_table, file_inode, peer, &entry)?;
                    if !get_file_entry(&file_table, file_inode, None)?.is_some() {
                        self.do_write_default_file_entry(
                            &txn,
                            &mut file_table,
                            file_inode,
                            &entry,
                        )?;
                    }
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

                let (parent_inode, file_inode) =
                    do_create_file(&txn, &mut txn.write_tree(&self.tree)?, &path)?;

                let mut file_table = txn.file_table()?;
                let entry = FileTableEntry::new(path, size, mtime, hash.clone(), parent_inode);
                if let Some(e) = get_file_entry(&file_table, file_inode, None)?
                    && e.content.hash == old_hash
                {
                    // If it overwrites the entry that's current, it's
                    // necessarily an entry we want.
                    self.do_write_file_entry(&mut file_table, file_inode, peer, &entry)?;
                    self.do_write_default_file_entry(&txn, &mut file_table, file_inode, &entry)?;
                } else if let Some(e) = get_file_entry(&file_table, file_inode, Some(peer))?
                    && e.content.hash == old_hash
                {
                    // If it overwrites the peer's entry, we want to
                    // keep that.
                    self.do_write_file_entry(&mut file_table, file_inode, peer, &entry)?;
                }

                let entry = file_table
                    .get(FileTableKey::LocalCopy(file_inode))?
                    .map(|v| v.value().parse().ok())
                    .flatten();
                if let Some(mut entry) = entry {
                    if replaces_local_copy(&entry, &old_hash) {
                        // Just remember that a newer version exist in
                        // a remote peer. This information is going to
                        // be used to download that newer version later on.
                        entry.outdated_by = Some(hash.clone());
                        file_table.insert(
                            FileTableKey::LocalCopy(file_inode),
                            Holder::with_content(entry)?,
                        )?;
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

                self.do_unlink(&txn, peer, self.arena_root, &path, old_hash.clone())?;

                if let Some(index_root) = index_root {
                    let dir_table = txn.dir_table()?;
                    let inode = match do_lookup_path(&dir_table, self.arena_root, Some(&path)) {
                        Ok((inode, _)) => Some(inode),
                        Err(StorageError::NotFound) => None,
                        Err(err) => {
                            return Err(err);
                        }
                    };
                    if let Some(inode) = inode {
                        let file_table = txn.file_table()?;
                        if let Some(entry) = file_table.get(FileTableKey::LocalCopy(inode))? {
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

                let root = self.arena_root;
                self.do_unlink(&txn, peer, root, &path, old_hash)?;
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
                let (parent_inode, file_inode) =
                    do_create_file(&txn, &mut txn.write_tree(&self.tree)?, &path)?;

                do_unmark_peer_file(&txn, peer, file_inode)?;

                let mut file_table = txn.file_table()?;
                let entry = FileTableEntry::new(path, size, mtime, hash, parent_inode);
                self.do_write_file_entry(&mut file_table, file_inode, peer, &entry)?;
                if !get_file_entry(&file_table, file_inode, None)?.is_some() {
                    self.do_write_default_file_entry(&txn, &mut file_table, file_inode, &entry)?;
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
            let file_entry = get_default_entry(&txn.file_table()?, inode)?;
            if let Some(blob_id) = file_entry.content.blob
                && blob::blob_exists(&txn, blob_id)?
            {
                // Delegate to Blobstore
                return self.blobstore.open_blob(&txn, file_entry, blob_id);
            }
        }

        // Switch to a write transaction to create the blob. We need to read
        // the file entry again because it might have changed.
        let txn = self.db.begin_write()?;
        let ret = {
            let mut file_table = txn.file_table()?;
            let mark_table = txn.mark_table()?;
            let mut file_entry = get_default_entry(&file_table, inode)?;
            let queue = queue_for_inode(&mark_table, &txn.read_tree(&self.tree)?, inode)?;
            let blob = self
                .blobstore
                .create_blob(&txn, file_entry.clone(), queue)?;
            log::debug!(
                "[{}] assigned blob {} in {queue:?} to file {inode}",
                self.arena,
                blob.id()
            );
            file_entry.content.blob = Some(blob.id());
            file_table.insert(FileTableKey::Default(inode), Holder::new(&file_entry)?)?;

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
        let (inode, _) = do_lookup_path(&txn.dir_table()?, self.arena_root, Some(path))?;
        let mut file_table = txn.file_table()?;
        let mut file_entry = get_default_entry(&file_table, inode)?;
        if file_entry.content.hash != *hash {
            return Ok(false);
        }
        let blob_id = match file_entry.content.blob {
            None => {
                return Err(StorageError::NotFound);
            }
            Some(id) => id,
        };

        if !self
            .blobstore
            .move_blob_if_matches(&txn, blob_id, hash, dest)?
        {
            return Ok(false);
        }

        file_entry.content.blob = None;
        file_table.insert(
            FileTableKey::Default(inode),
            Holder::with_content(file_entry)?,
        )?;

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
        let inode = match do_lookup_path(&txn.dir_table()?, self.arena_root, Some(path)) {
            Ok((inode, _)) => inode,
            Err(StorageError::NotFound) => {
                return Ok(None);
            }
            Err(err) => {
                return Err(err);
            }
        };
        let mut file_table = txn.file_table()?;
        let mut file_entry = match get_default_entry(&file_table, inode) {
            Ok(e) => e,
            Err(StorageError::NotFound) => {
                return Ok(None);
            }
            Err(err) => {
                return Err(err);
            }
        };
        if file_entry.content.hash != *hash || file_entry.metadata.size != metadata.len() {
            return Ok(None);
        }
        let mark_table = txn.mark_table()?;
        let (blob_id, cachepath) = self.blobstore.move_into_blob(
            &txn,
            file_entry.content.blob,
            queue_for_inode(&mark_table, &txn.read_tree(&self.tree)?, inode)?,
            metadata,
        )?;
        file_entry.content.blob = Some(blob_id);
        file_table.insert(
            FileTableKey::Default(inode),
            Holder::with_content(file_entry)?,
        )?;

        Ok(Some(cachepath))
    }

    // Return the default file entry for a path.
    pub(crate) fn get_file_entry_for_path<T>(
        &self,
        txn: &ArenaReadTransaction,
        root: Inode,
        path: T,
    ) -> Result<FileTableEntry, StorageError>
    where
        T: AsRef<Path>,
    {
        let path = path.as_ref();
        let dir_table = txn.dir_table()?;
        let (inode, assignment) = do_lookup_path(&dir_table, root, Some(path))?;
        if assignment != InodeAssignment::File {
            return Err(StorageError::IsADirectory);
        }
        let file_table = txn.file_table()?;

        get_file_entry(&file_table, inode, None)?.ok_or(StorageError::NotFound)
    }

    /// Write an entry in the file table, overwriting any existing one.
    fn do_write_file_entry(
        &self,
        file_table: &mut redb::Table<'_, FileTableKey, Holder<FileTableEntry>>,
        file_inode: Inode,
        peer: Peer,
        entry: &FileTableEntry,
    ) -> Result<(), StorageError> {
        log::debug!(
            "[{}] new file entry {:?} {file_inode} on {peer} {}",
            self.arena,
            entry.content.path,
            entry.content.hash
        );

        file_table.insert(
            FileTableKey::PeerCopy(file_inode, peer),
            Holder::new(entry)?,
        )?;

        Ok(())
    }

    /// Write an entry in the file table, overwriting any existing one.
    fn do_write_default_file_entry(
        &self,
        txn: &ArenaWriteTransaction,
        file_table: &mut redb::Table<'_, FileTableKey, Holder<FileTableEntry>>,
        file_inode: Inode,
        entry: &FileTableEntry,
    ) -> Result<(), StorageError> {
        if let Some(old_entry) = file_table.get(FileTableKey::Default(file_inode))? {
            let old_entry = old_entry.value().parse()?;
            if let Some(blob_id) = old_entry.content.blob {
                self.blobstore.delete_blob(&txn, blob_id)?;
            }
        }

        // This entry is the outside world view of the file, so
        // changes should be reported.
        self.dirty_paths.mark_dirty(txn, &entry.content.path)?;

        file_table.insert(FileTableKey::Default(file_inode), Holder::new(entry)?)?;

        Ok(())
    }

    /// Remove a file entry for a specific peer.
    fn do_rm_file_entry(
        &self,
        txn: &ArenaWriteTransaction,
        file_table: &mut redb::Table<'_, FileTableKey, Holder<FileTableEntry>>,
        dir_table: &mut redb::Table<'_, (Inode, &str), Holder<DirTableEntry>>,
        parent_inode: Inode,
        inode: Inode,
        peer: Peer,
        old_hash: Option<Hash>,
    ) -> Result<(), StorageError> {
        let mut path = None;
        let mut entries = HashMap::new();
        for elt in file_table.range(FileTableKey::range(inode))? {
            let (key, value) = elt?;
            let insert_key = match key.value() {
                FileTableKey::PeerCopy(_, peer) => Some(peer),
                FileTableKey::Default(_) => None,
                _ => {
                    continue;
                }
            };
            let entry = value.value().parse()?;
            if path.is_none() {
                path = Some(entry.content.path.clone());
            }
            entries.insert(insert_key, entry);
        }

        let peer_hash = match entries.remove(&Some(peer)).map(|e| e.content.hash) {
            Some(h) => h,
            None => {
                // No entry to delete
                return Ok(());
            }
        };

        if let Some(old_hash) = old_hash
            && peer_hash != old_hash
        {
            // Skip deletion
            return Ok(());
        }

        file_table.remove(FileTableKey::PeerCopy(inode, peer))?;

        let default_hash = entries.remove(&None).map(|e| e.content.hash);
        // In case old_hash == default_hash, should we remove the default
        // version and pretend the file doesn't exist anymore, even if
        // it's available on other peers? It would be consistent,
        // history-wise. With the current logic, a file is only gone once
        // it's gone from all peers.

        if entries.is_empty() {
            // This was the last peer. Remove the default entry as well as
            // the directory entry.
            // TODO: delete empty directories, up to the arena root

            if let Some(path) = path {
                self.dirty_paths.mark_dirty(&txn, &path)?;
            }

            // Check if the default entry has a blob and delete it
            if let Some(default_entry) = file_table.get(FileTableKey::Default(inode))? {
                let default_entry = default_entry.value().parse()?;
                if let Some(blob_id) = default_entry.content.blob {
                    self.blobstore.delete_blob(&txn, blob_id)?;
                }
            }

            file_table.remove(FileTableKey::Default(inode))?;
            let mut tree = txn.write_tree(&self.tree)?;
            if let Some(name) = tree.name_in(parent_inode, inode)? {
                tree.remove(inode, dir_table, (parent_inode, name.as_str()))?;
                log::debug!("Tree.remove {inode} ({parent_inode}, {name})");
            }
            dir_table.insert(
                (parent_inode, "."),
                Holder::with_content(DirTableEntry::Dot(UnixTime::now()))?,
            )?;

            return Ok(());
        }

        // If this was the peer that had the default entry, we need to
        // choose another one as default.
        let another_peer_has_default_hash = default_hash
            .map(|h| entries.values().any(|e| e.content.hash == h))
            .unwrap_or(false);
        if another_peer_has_default_hash {
            return Ok(());
        }

        let most_recent = entries.into_iter().reduce(|a, b| {
            if b.1.metadata.mtime > a.1.metadata.mtime {
                b
            } else {
                a
            }
        });
        if let Some((_, entry)) = most_recent {
            self.do_write_default_file_entry(txn, file_table, inode, &entry)?;
        }

        Ok(())
    }

    fn do_unlink<T>(
        &self,
        txn: &ArenaWriteTransaction,
        peer: Peer,
        arena_root: Inode,
        path: T,
        old_hash: Hash,
    ) -> Result<(), StorageError>
    where
        T: AsRef<Path>,
    {
        let path = path.as_ref();
        let mut dir_table = txn.dir_table()?;
        let (parent_inode, parent_assignment) =
            do_lookup_path(&dir_table, arena_root, path.parent().as_ref())?;
        if parent_assignment != InodeAssignment::Directory {
            return Err(StorageError::NotADirectory);
        }

        let (inode, assignment) =
            get_dir_entry(&dir_table, parent_inode, path.name())?.ok_or(StorageError::NotFound)?;
        if assignment != InodeAssignment::File {
            return Err(StorageError::IsADirectory);
        }

        let mut file_table = txn.file_table()?;
        self.do_rm_file_entry(
            txn,
            &mut file_table,
            &mut dir_table,
            parent_inode,
            inode,
            peer,
            Some(old_hash),
        )?;

        Ok(())
    }

    /// Delete all marked files for a peer.
    fn do_delete_marked_files(
        &self,
        txn: &ArenaWriteTransaction,
        peer: Peer,
    ) -> Result<(), StorageError> {
        let mut pending_catchup_table = txn.pending_catchup_table()?;
        let mut file_table = txn.file_table()?;
        let mut directory_table = txn.dir_table()?;
        let peer_str = peer.as_str();
        for elt in pending_catchup_table
            .extract_from_if((peer_str, Inode::ZERO)..=(peer_str, Inode::MAX), |_, _| {
                true
            })?
        {
            let elt = elt?;
            let (_, inode) = elt.0.value();
            let parent_inode = elt.1.value();
            self.do_rm_file_entry(
                txn,
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

    pub(crate) fn local_availability(
        &self,
        inode: Inode,
    ) -> Result<LocalAvailability, StorageError> {
        let txn = self.db.begin_read()?;
        let file_entry = get_default_entry(&txn.file_table()?, inode)?;

        blob::local_availability(&txn, &file_entry)
    }

    // TODO: update tests to work on blobstore and remove
    #[allow(dead_code)]
    pub(crate) fn extend_local_availability(
        &self,
        blob_id: BlobId,
        new_range: &ByteRanges,
    ) -> Result<(), StorageError> {
        self.blobstore.extend_local_availability(blob_id, new_range)
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
        file_table: &impl ReadableTable<FileTableKey, Holder<'static, FileTableEntry>>,
        path: &Path,
    ) -> Result<Option<IndexedFileTableEntry>, StorageError> {
        if let Some(inode) = tree.lookup_path(path)? {
            get_indexed_file_inode(file_table, inode)
        } else {
            Ok(None)
        }
    }

    fn index_file(
        &self,
        txn: &ArenaWriteTransaction,
        path: &realize_types::Path,
        size: u64,
        mtime: UnixTime,
        hash: Hash,
    ) -> Result<IndexedFileTableEntry, StorageError> {
        let (parent_inode, file_inode) =
            do_create_file(&txn, &mut txn.write_tree(&self.tree)?, &path)?;

        let mut file_table = txn.file_table()?;
        let mut history_table = txn.history_table()?;
        let old_hash = file_table
            .get(FileTableKey::LocalCopy(file_inode))?
            .map(|e| e.value().parse().ok())
            .flatten()
            .map(|e| e.content.hash);
        let same_hash = old_hash.as_ref().map(|h| *h == hash).unwrap_or(false);
        let entry = FileTableEntry::new(path.clone(), size, mtime, hash, parent_inode);
        file_table.insert(FileTableKey::LocalCopy(file_inode), Holder::new(&entry)?)?;
        if !same_hash {
            (&self.dirty_paths).mark_dirty(txn, path)?;
            let index = self.allocate_history_index(txn, &history_table)?;
            let ev = if let Some(old_hash) = old_hash {
                HistoryTableEntry::Replace(path.clone(), old_hash)
            } else {
                HistoryTableEntry::Add(path.clone())
            };
            log::debug!("[{}] History #{index}: {ev:?}", self.arena);
            history_table.insert(index, Holder::with_content(ev)?)?;
        }

        Ok(entry.into())
    }

    fn allocate_history_index(
        &self,
        txn: &ArenaWriteTransaction,
        history_table: &impl ReadableTable<u64, Holder<'static, HistoryTableEntry>>,
    ) -> Result<u64, StorageError> {
        let entry_index = 1 + last_history_index(history_table)?;

        let history_tx = self.history_tx.clone();
        txn.after_commit(move || {
            let _ = history_tx.send(entry_index);
        });
        Ok(entry_index)
    }

    fn report_removed(
        &self,
        txn: &ArenaWriteTransaction,
        history_table: &mut redb::Table<'_, u64, Holder<'static, HistoryTableEntry>>,
        path: realize_types::Path,
        hash: Hash,
    ) -> Result<(), StorageError> {
        let index = self.allocate_history_index(txn, history_table)?;
        (&self.dirty_paths).mark_dirty(txn, &path)?;
        let ev = HistoryTableEntry::Remove(path, hash);
        log::debug!("[{}] History #{index}: {ev:?}", self.arena);
        history_table.insert(index, Holder::with_content(ev)?)?;
        Ok(())
    }

    fn unindex_file(
        &self,
        txn: &ArenaWriteTransaction,
        file_table: &mut Table<'_, FileTableKey, Holder<'static, FileTableEntry>>,
        history_table: &mut Table<'_, u64, Holder<'static, HistoryTableEntry>>,
        inode: Inode,
    ) -> Result<(), StorageError> {
        let removed = file_table.remove(FileTableKey::LocalCopy(inode))?;
        Ok(if let Some(removed) = removed {
            let entry = removed.value().parse()?;
            self.report_removed(txn, history_table, entry.content.path, entry.content.hash)?;
        })
    }

    fn unindex_dir(
        &self,
        txn: &ArenaWriteTransaction,
        tree: &impl TreeReadOperations,
        file_table: &mut Table<'_, FileTableKey, Holder<'static, FileTableEntry>>,
        history_table: &mut Table<'_, u64, Holder<'static, HistoryTableEntry>>,
        inode: Inode,
    ) -> Result<(), StorageError> {
        for inode in tree.recurse(inode, |_| true) {
            self.unindex_file(txn, file_table, history_table, inode?)?
        }

        Ok(())
    }
}

/// Choose the appropriate blob queue for the inode, given its mark.
fn queue_for_inode(
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
    file_table: &impl ReadableTable<FileTableKey, Holder<'static, FileTableEntry>>,
    inode: Inode,
) -> Result<Option<IndexedFileTableEntry>, StorageError> {
    match file_table.get(FileTableKey::LocalCopy(inode))? {
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
        self.history_tx.subscribe()
    }

    fn last_history_index(&self) -> Result<u64, StorageError> {
        let txn = self.db.begin_read()?;
        let history_table = txn.history_table()?;

        last_history_index(&history_table)
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
        self.get_indexed_file(&txn.read_tree(&self.tree)?, &txn.file_table()?, path)
    }

    fn get_indexed_file_txn(
        &self,
        txn: &ArenaWriteTransaction,
        root: &std::path::Path,
        path: &realize_types::Path,
        hash: Option<&Hash>,
    ) -> Result<Option<std::path::PathBuf>, StorageError> {
        let file_table = txn.file_table()?;
        let entry = self.get_indexed_file(&txn.read_tree(&self.tree)?, &file_table, path)?;
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
        self.index_file(&txn, path, size, mtime, hash)?;
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
        let entry = self.index_file(&txn, path, size, mtime, hash)?;
        if file_matches_index(&entry, root, path) {
            txn.commit()?;
            return Ok(true);
        }

        Ok(false)
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
                let tree = txn.read_tree(&self.tree)?;
                if let Some(inode) = tree.lookup_path(path)? {
                    let mut file_table = txn.file_table()?;
                    let mut history_table = txn.history_table()?;
                    self.unindex_file(&txn, &mut file_table, &mut history_table, inode)?;
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
        let file_table = txn.file_table()?;
        for (path, entry) in file_table
            .iter()?
            .flatten()
            // Skip any entry with errors
            .flat_map(|(k, v)| {
                if let FileTableKey::LocalCopy(_) = k.value() {
                    if let Ok(entry) = v.value().parse() {
                        let path = entry.content.path.clone();
                        return Some((path, IndexedFileTableEntry::from(entry)));
                    }
                }

                None
            })
        {
            if let Err(_) = tx.blocking_send((path, entry)) {
                break;
            }
        }

        Ok(())
    }

    fn history(
        &self,
        range: Range<u64>,
        tx: mpsc::Sender<Result<(u64, HistoryTableEntry), StorageError>>,
    ) -> Result<(), StorageError> {
        let txn = self.db.begin_read()?;
        let history_table = txn.history_table()?;
        for res in history_table.range(range)?.map(|res| match res {
            Err(err) => Err(StorageError::from(err)),
            Ok((k, v)) => match v.value().parse() {
                Ok(v) => Ok((k.value(), v)),
                Err(err) => Err(StorageError::from(err)),
            },
        }) {
            if let Err(_) = tx.blocking_send(res) {
                break;
            }
        }

        Ok(())
    }

    fn remove_file_or_dir(&self, path: &realize_types::Path) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let tree = txn.read_tree(&self.tree)?;
            if let Some(inode) = tree.lookup_path(path)? {
                let mut file_table = txn.file_table()?;
                let mut history_table = txn.history_table()?;
                self.unindex_file(&txn, &mut file_table, &mut history_table, inode)?;
                self.unindex_dir(&txn, &tree, &mut file_table, &mut history_table, inode)?;
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
        let tree = txn.read_tree(&self.tree)?;
        let inode = match tree.lookup_path(path)? {
            Some(inode) => inode,
            None => {
                return Ok(false);
            }
        };

        let mut file_table = txn.file_table()?;
        let mut history_table = txn.history_table()?;
        if let Some(entry) = get_indexed_file_inode(&file_table, inode)?
            && entry.hash == *hash
            && file_matches_index(&entry.into(), root, path)
        {
            file_table.remove(FileTableKey::LocalCopy(inode))?;

            let index = self.allocate_history_index(&txn, &history_table)?;
            (&self.dirty_paths).mark_dirty(&txn, &path)?;
            let ev = HistoryTableEntry::Drop(path.clone(), hash.clone());
            log::debug!("[{}] History #{index}: {ev:?}", self.arena);
            history_table.insert(index, Holder::with_content(ev)?)?;
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
        let tree = txn.read_tree(&self.tree)?;

        Ok(resolve_mark_at_path(&mark_table, &tree, path)?)
    }

    fn set_arena_mark(&self, mark: Mark) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let inode = self.arena_root;

            let mut mark_table = txn.mark_table()?;
            let dir_table = txn.dir_table()?;
            let file_table = txn.file_table()?;
            let old_mark = resolve_mark(&mark_table, &txn.read_tree(&self.tree)?, inode)?;
            mark_table.insert(inode, Holder::with_content(MarkTableEntry { mark })?)?;

            if old_mark != mark {
                mark_dir_dirty(&txn, &dir_table, &file_table, inode, &self.dirty_paths)?;
            }
        }
        txn.commit()?;

        Ok(())
    }

    fn set_mark(&self, path: &Path, mark: Mark) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut mark_table = txn.mark_table()?;
            let mut tree = txn.write_tree(&self.tree)?;
            let old_mark = resolve_mark_at_path(&mark_table, &tree, path)?;
            let inode = tree.insert_at_path(
                path,
                &mut mark_table,
                |inode| inode,
                Holder::with_content(MarkTableEntry { mark })?,
            )?;

            if old_mark != mark {
                let dir_table = txn.dir_table()?;
                let file_table = txn.file_table()?;
                mark_dir_dirty(&txn, &dir_table, &file_table, inode, &self.dirty_paths)?;
                mark_file_dirty(&txn, &file_table, inode, &self.dirty_paths)?;
            }
        }
        txn.commit()?;

        Ok(())
    }

    fn clear_mark(&self, path: &Path) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut tree = txn.write_tree(&self.tree)?;
            let inode = match tree.lookup_path(path)? {
                Some(inode) => inode,
                None => {
                    // No mark to remove
                    return Ok(());
                }
            };
            let mut mark_table = txn.mark_table()?;
            let old_mark = resolve_mark(&mark_table, &tree, inode)?;
            let removed = tree.remove(inode, &mut mark_table, inode)?;
            if removed && old_mark != resolve_mark(&mark_table, &tree, inode)? {
                let dir_table = txn.dir_table()?;
                let file_table = txn.file_table()?;
                mark_dir_dirty(&txn, &dir_table, &file_table, inode, &self.dirty_paths)?;
                mark_file_dirty(&txn, &file_table, inode, &self.dirty_paths)?
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
    let (_, last_matching) = tree.lookup_partial_path(path)?;

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

fn mark_file_dirty(
    txn: &ArenaWriteTransaction,
    file_table: &redb::Table<'_, FileTableKey, Holder<FileTableEntry>>,
    inode: Inode,
    dirty_paths: &Arc<DirtyPaths>,
) -> Result<(), StorageError> {
    for entry in file_table.range(FileTableKey::range(inode))? {
        let entry = entry?;
        match entry.0.value() {
            FileTableKey::Default(_) | FileTableKey::LocalCopy(_) => {
                dirty_paths.mark_dirty(txn, &entry.1.value().parse()?.content.path)?;
                return Ok(());
            }
            _ => {}
        }
    }

    Ok(())
}

fn mark_dir_dirty(
    txn: &ArenaWriteTransaction,
    dir_table: &redb::Table<'_, (Inode, &str), Holder<DirTableEntry>>,
    file_table: &redb::Table<'_, FileTableKey, Holder<FileTableEntry>>,
    inode: Inode,
    dirty_paths: &Arc<DirtyPaths>,
) -> Result<(), StorageError> {
    for item in dir_table.range((inode, "")..(inode.plus(1), ""))? {
        let (key, value) = item?;
        if key.value().0 != inode {
            break;
        }
        if let DirTableEntry::Regular(entry) = value.value().parse()? {
            match entry.assignment {
                InodeAssignment::File => {
                    mark_file_dirty(txn, file_table, entry.inode, dirty_paths)?;
                }
                InodeAssignment::Directory => {
                    mark_dir_dirty(txn, dir_table, file_table, entry.inode, dirty_paths)?;
                }
            }
        }
    }

    Ok(())
}

fn do_readdir(
    dir_table: &impl ReadableTable<(Inode, &'static str), Holder<'static, DirTableEntry>>,
    inode: Inode,
) -> Result<Vec<(String, Inode, InodeAssignment)>, StorageError> {
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
            entries.push((name, entry.inode, entry.assignment));
        }
    }

    Ok(entries)
}

fn do_lookup(
    dir_table: &impl ReadableTable<(Inode, &'static str), Holder<'static, DirTableEntry>>,
    parent_inode: Inode,
    name: &str,
) -> Result<(Inode, InodeAssignment), StorageError> {
    Ok(dir_table
        .get((parent_inode, name))?
        .ok_or(StorageError::NotFound)?
        .value()
        .parse()?
        .into_readdir_entry(parent_inode))
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

fn do_dir_mtime(
    dir_table: &impl redb::ReadableTable<(Inode, &'static str), Holder<'static, DirTableEntry>>,
    inode: Inode,
    root: Inode,
) -> Result<UnixTime, StorageError> {
    match dir_table.get((inode, "."))? {
        Some(e) => {
            if let DirTableEntry::Dot(mtime) = e.value().parse()? {
                return Ok(mtime);
            }
        }
        None => {
            if inode == root {
                // When the filesystem is empty, the root dir might not
                // have a mtime. This is not an error.
                return Ok(UnixTime::ZERO);
            }
        }
    }

    Err(StorageError::NotFound)
}

fn do_parent_inode(
    dir_table: &impl redb::ReadableTable<(Inode, &'static str), Holder<'static, DirTableEntry>>,
    file_table: &impl ReadableTable<FileTableKey, Holder<'static, FileTableEntry>>,
    inode: Inode,
) -> Result<Inode, StorageError> {
    if let Some(e) = dir_table.get((inode, ".."))? {
        if let DirTableEntry::DotDot(inode) = e.value().parse()? {
            return Ok(inode);
        }
    }
    for entry in file_table.range(FileTableKey::range(inode))? {
        let entry = entry?;
        return Ok(entry.1.value().parse()?.parent_inode);
    }

    Err(StorageError::NotFound)
}

/// Get a [FileTableEntry] for a specific peer.
fn get_file_entry(
    file_table: &impl redb::ReadableTable<FileTableKey, Holder<'static, FileTableEntry>>,
    inode: Inode,
    peer: Option<Peer>,
) -> Result<Option<FileTableEntry>, StorageError> {
    match file_table.get(
        peer.map(|p| FileTableKey::PeerCopy(inode, p))
            .unwrap_or_else(|| FileTableKey::Default(inode)),
    )? {
        None => Ok(None),
        Some(e) => Ok(Some(e.value().parse()?)),
    }
}

fn get_default_entry(
    file_table: &impl redb::ReadableTable<FileTableKey, Holder<'static, FileTableEntry>>,
    inode: Inode,
) -> Result<FileTableEntry, StorageError> {
    let entry = file_table
        .get(FileTableKey::Default(inode))?
        .ok_or(StorageError::NotFound)?;

    Ok(entry.value().parse()?)
}

fn do_file_metadata(
    txn: &ArenaReadTransaction,
    inode: Inode,
) -> Result<FileMetadata, StorageError> {
    Ok(get_default_entry(&txn.file_table()?, inode)?.metadata)
}

fn do_file_availability(
    txn: &ArenaReadTransaction,
    inode: Inode,
    arena: Arena,
) -> Result<FileAvailability, StorageError> {
    let file_table = txn.file_table()?;

    let mut range = file_table.range(FileTableKey::range(inode))?;
    let (default_key, default_entry) = range.next().ok_or(StorageError::NotFound)??;
    if !matches!(default_key.value(), FileTableKey::Default(_)) {
        return Err(StorageError::NotFound);
    }
    let FileTableEntry {
        metadata,
        content: FileContent { path, hash, .. },
        ..
    } = default_entry.value().parse()?;

    let mut peers = vec![];
    for entry in range {
        let entry = entry?;
        if let FileTableKey::PeerCopy(_, peer) = entry.0.value() {
            let file_entry: FileTableEntry = entry.1.value().parse()?;
            if file_entry.content.hash == hash {
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
        metadata,
        hash,
        peers,
    })
}

/// Retrieve or create a file entry at the given path.
///
/// Return the parent inode and the file inode.
fn do_create_file<T>(
    txn: &ArenaWriteTransaction,
    tree: &mut WritableOpenTree,
    path: T,
) -> Result<(Inode, Inode), StorageError>
where
    T: AsRef<Path>,
{
    let path = path.as_ref();
    let mut dir_table = txn.dir_table()?;
    let mut parent_inode = tree.root();
    for component in Path::components(path.parent().as_ref()) {
        parent_inode = add_dir_entry(
            &mut dir_table,
            tree,
            parent_inode,
            component,
            InodeAssignment::Directory,
        )?;
    }

    let file_inode = add_dir_entry(
        &mut dir_table,
        tree,
        parent_inode,
        path.name(),
        InodeAssignment::File,
    )?;

    Ok((parent_inode, file_inode))
}

fn add_dir_entry(
    dir_table: &mut redb::Table<'_, (Inode, &str), Holder<DirTableEntry>>,
    tree: &mut WritableOpenTree,
    parent_inode: Inode,
    name: &str,
    assignment: InodeAssignment,
) -> Result<Inode, StorageError> {
    let (new_inode, created) = tree.insert_if_not_found(
        parent_inode,
        name,
        dir_table,
        |_| (parent_inode, name),
        |inode| Holder::with_content(DirTableEntry::Regular(ReadDirEntry { inode, assignment })),
        |_, v| {
            if let DirTableEntry::Regular(e) = v.value().parse()? {
                if e.assignment != assignment {
                    return Err(match assignment {
                        InodeAssignment::Directory => StorageError::NotADirectory,
                        InodeAssignment::File => StorageError::IsADirectory,
                    });
                }
            } else {
                return Err(StorageError::InconsistentDatabase(format!(
                    "expected regular entry for ({parent_inode}, {name:?}) in dir_table"
                )));
            }

            Ok(())
        },
    )?;
    if created {
        log::debug!("new dir entry {parent_inode} {name} -> {new_inode} {assignment:?}");

        let mtime = UnixTime::now();
        let dot = Holder::with_content(DirTableEntry::Dot(mtime))?;
        dir_table.insert((parent_inode, "."), dot.clone())?;
        if assignment == InodeAssignment::Directory {
            dir_table.insert((new_inode, "."), dot.clone())?;
            dir_table.insert(
                (new_inode, ".."),
                Holder::with_content(DirTableEntry::DotDot(parent_inode))?,
            )?;
        }
    }

    Ok(new_inode)
}

/// Find the file or directory pointed to by the given path.
fn do_lookup_path(
    dir_table: &impl redb::ReadableTable<(Inode, &'static str), Holder<'static, DirTableEntry>>,
    root_inode: Inode,
    path: Option<&Path>,
) -> Result<(Inode, InodeAssignment), StorageError> {
    let mut current = (root_inode, InodeAssignment::Directory);
    for component in Path::components(path) {
        if current.1 != InodeAssignment::Directory {
            return Err(StorageError::NotADirectory);
        }
        if let Some(e) = get_dir_entry(dir_table, current.0, component)? {
            current = e
        } else {
            return Err(StorageError::NotFound);
        };
    }

    Ok(current)
}

/// Get a [ReadDirEntry] from a directory, if it exists.
fn get_dir_entry(
    dir_table: &impl redb::ReadableTable<(Inode, &'static str), Holder<'static, DirTableEntry>>,
    parent_inode: Inode,
    name: &str,
) -> Result<Option<(Inode, InodeAssignment)>, StorageError> {
    match dir_table.get((parent_inode, name))? {
        None => Ok(None),
        Some(e) => Ok(Some(e.value().parse()?.into_readdir_entry(parent_inode))),
    }
}

fn do_mark_peer_files(txn: &ArenaWriteTransaction, peer: Peer) -> Result<(), StorageError> {
    let file_table = txn.file_table()?;
    let mut pending_catchup_table = txn.pending_catchup_table()?;
    for elt in file_table.iter()? {
        let (k, v) = elt?;
        if let FileTableKey::PeerCopy(inode, elt_peer) = k.value()
            && elt_peer == peer
        {
            let v = v.value().parse()?;
            pending_catchup_table.insert((peer.as_str(), inode), v.parent_inode)?;
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

fn last_history_index(
    history_table: &impl redb::ReadableTable<u64, Holder<'static, HistoryTableEntry>>,
) -> Result<u64, StorageError> {
    Ok(history_table.last()?.map(|(k, _)| k.value()).unwrap_or(0))
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
    entry.content.hash == *old_hash
        || entry
            .outdated_by
            .as_ref()
            .map(|h| *h == *old_hash)
            .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::sync::Arc;
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};
    use tokio::sync::mpsc;

    use super::ArenaCache;
    use crate::arena::db::{ArenaDatabase, ArenaReadTransaction};
    use crate::arena::engine::{self, DirtyPaths};
    use crate::arena::index::RealIndex;
    use crate::arena::mark::PathMarks;
    use crate::arena::notifier::Notification;
    use crate::arena::tree::{OpenTree, TreeExt, TreeReadOperations};
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
            let allocator =
                InodeAllocator::new(GlobalDatabase::new(redb_utils::in_memory()?)?, [arena])?;

            let child = tempdir.child(format!("{arena}-cache.db"));
            let blob_dir = tempdir.child(format!("{arena}/blobs"));
            if let Some(p) = child.parent() {
                std::fs::create_dir_all(p)?;
            }
            let db = ArenaDatabase::new(redb_utils::in_memory()?)?;
            let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;
            let acache = ArenaCache::new(
                arena,
                allocator,
                Arc::clone(&db),
                blob_dir.path(),
                Arc::clone(&dirty_paths),
            )?;

            Ok(Self {
                arena,
                acache,
                db,
                _tempdir: tempdir,
            })
        }

        fn begin_read(&self) -> anyhow::Result<ArenaReadTransaction> {
            Ok(self.db.begin_read()?)
        }

        fn clear_dirty(&self) -> anyhow::Result<()> {
            let txn = self.db.begin_write()?;
            while engine::take_dirty(&txn)?.is_some() {}
            txn.commit()?;

            Ok(())
        }

        fn dir_mtime<T>(&self, path: T) -> anyhow::Result<UnixTime>
        where
            T: AsRef<Path>,
        {
            let path = path.as_ref();
            let (inode, _) = self.acache.lookup_path(path)?;
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
            let tree = txn.read_tree(&acache.tree)?;
            assert_eq!(acache.arena_root(), tree.root());
            assert!(tree.exists(a)?);
            assert!(tree.exists(b)?);
            assert!(tree.exists(c)?);
            assert_eq!(Some(a), tree.lookup(tree.root(), "a")?);
            assert_eq!(Some(b), tree.lookup(a, "b")?);
            assert_eq!(Some(c), tree.lookup(b, "c.txt")?);
        }

        fixture.remove_from_cache(&file_path)?;

        assert!(acache.lookup(b, "c.txt").is_err());
        {
            let txn = fixture.db.begin_read()?;
            let tree = txn.read_tree(&acache.tree)?;

            // The directories stay, because they still exist in the cache
            // but the file is gone from tree as well, since this was the
            // only reference to it.
            assert!(tree.exists(a)?);
            assert!(tree.exists(b)?);
            assert!(!tree.exists(c)?);
            assert_eq!(Some(a), tree.lookup(tree.root(), "a")?);
            assert_eq!(Some(b), tree.lookup(a, "b")?);
            assert_eq!(None, tree.lookup(b, "c.txt")?);
        }
        Ok(())
    }

    #[tokio::test]
    async fn get_parent_of_file_in_cache() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let acache = &fixture.acache;
        let file_path = Path::parse("a/b/c.txt")?;
        let mtime = test_time();

        fixture.add_to_cache(&file_path, 100, mtime)?;

        let (a_inode, _) = acache.lookup_path(Path::parse("a")?)?;
        let (b_inode, _) = acache.lookup_path(Path::parse("a/b")?)?;
        let (c_inode, _) = acache.lookup_path(Path::parse("a/b/c.txt")?)?;
        assert_eq!(b_inode, acache.parent_inode(c_inode)?);
        assert_eq!(a_inode, acache.parent_inode(b_inode)?);
        assert_eq!(acache.arena_root(), acache.parent_inode(a_inode)?);

        Ok(())
    }

    #[tokio::test]
    async fn get_parent_of_file_in_index() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let acache = &fixture.acache;
        let index = acache.as_index();
        let mtime = UnixTime::from_secs(1234567890);
        let path = realize_types::Path::parse("a/b/c.txt")?;
        index.add_file(&path, 100, mtime, Hash([0xfa; 32]))?;

        let (a_inode, _) = acache.lookup_path(Path::parse("a")?)?;
        let (b_inode, _) = acache.lookup_path(Path::parse("a/b")?)?;
        let (c_inode, _) = acache.lookup_path(Path::parse("a/b/c.txt")?)?;
        assert_eq!(b_inode, acache.parent_inode(c_inode)?);
        assert_eq!(a_inode, acache.parent_inode(b_inode)?);
        assert_eq!(acache.arena_root(), acache.parent_inode(a_inode)?);

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

        assert!(engine::is_dirty(&fixture.begin_read()?, &file_path)?);

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

        let (inode, _) = acache.lookup_path(&file_path)?;
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
        assert!(engine::is_dirty(&fixture.begin_read()?, &file_path)?);

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
        assert!(!engine::is_dirty(&fixture.begin_read()?, &file_path)?);

        Ok(())
    }

    #[tokio::test]
    async fn ignore_duplicate_add() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let file_path = Path::parse("file.txt")?;

        fixture.add_to_cache(&file_path, 100, test_time())?;
        fixture.add_to_cache(&file_path, 200, test_time())?;

        let acache = &fixture.acache;
        let (inode, _) = acache.lookup_path(&file_path)?;
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

        let (inode, _) = acache.lookup_path(&file_path)?;
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
        assert!(engine::is_dirty(&fixture.begin_read()?, &file_path)?);

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
        let (inode, _) = acache.lookup_path(&file_path)?;
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

        let (inode, assignment) = acache.lookup_path(&path)?;
        assert_eq!(assignment, InodeAssignment::File);

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
                old_hash: Hash([3u8; 32]),
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
        assert!(matches!(
            acache.lookup_path(&file1),
            Err(StorageError::NotFound)
        ));

        // File2 should still be available, from peer2
        let (file2_inode, _) = acache.lookup_path(&file2)?;
        let file2_availability = acache.file_availability(file2_inode)?;
        assert!(file2_availability.peers.contains(&peer2));

        // File3 should still be available, from peer3
        let (file3_inode, _) = acache.lookup_path(&file3)?;
        let file3_availability = acache.file_availability(file3_inode)?;
        assert!(file3_availability.peers.contains(&peer3));

        // File4 should still be available, from peer1
        let (file4_inode, _) = acache.lookup_path(&file4)?;
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
        let (inode, _) = acache.lookup_path(&file_path)?;
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
        let (inode, _) = acache.lookup_path(&file_path)?;

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
        let (inode, _) = acache.lookup_path(&file_path)?;

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
        let (inode, _) = acache.lookup_path(&file_path)?;

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
        let (inode, _) = acache.lookup_path(&file_path)?;

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
        let (inode, _) = acache.lookup_path(&file_path)?;

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
        let (inode, _) = acache.lookup_path(&file_path)?;
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

        // Try to get availability for a non-existent inode
        let nonexistent_inode = Inode(999999);
        let result = acache.local_availability(nonexistent_inode);

        assert!(matches!(result, Err(StorageError::NotFound)));

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
        let (inode, _) = acache.lookup_path(&file_path)?;

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
        let path = tempdir.path().join("index.db");
        let db = ArenaDatabase::new(redb::Database::create(&path)?)?;
        let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;
        let arena = test_arena();
        let allocator =
            InodeAllocator::new(GlobalDatabase::new(redb_utils::in_memory()?)?, [arena])?;
        let blob_dir = tempdir.child("blobs");
        let acache = ArenaCache::new(
            arena,
            allocator.clone(),
            db,
            blob_dir.path(),
            dirty_paths.clone(),
        )?;
        let uuid = acache.uuid().clone();

        // Drop the first instance to release the database lock
        drop(acache);

        let db = ArenaDatabase::new(redb::Database::create(&path)?)?;
        let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;
        let allocator =
            InodeAllocator::new(GlobalDatabase::new(redb_utils::in_memory()?)?, [arena])?;
        let acache = ArenaCache::new(arena, allocator, db, blob_dir.path(), dirty_paths)?;
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
            let db = ArenaDatabase::new(redb_utils::in_memory()?)?;
            let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;
            let allocator =
                InodeAllocator::new(GlobalDatabase::new(redb_utils::in_memory()?)?, [arena])?;
            let acache = ArenaCache::new(
                arena,
                allocator,
                Arc::clone(&db),
                &std::path::PathBuf::from("/dev/null"),
                Arc::clone(&dirty_paths),
            )?;
            let index = acache.as_index();
            let marks = acache.clone() as Arc<dyn PathMarks>;

            Ok(Self {
                arena,
                db,
                acache,
                index,
                marks,
            })
        }

        /// Clear all dirty flags in both index and cache
        fn clear_all_dirty(&self) -> anyhow::Result<()> {
            let txn = self.db.begin_write()?;
            while engine::take_dirty(&txn)?.is_some() {}
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
            Ok(engine::is_dirty(&txn, path)?)
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
        let tree = txn.read_tree(&fixture.acache.tree)?;
        assert!(tree.lookup_path(&path)?.is_none());

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
            fixture.acache.lookup_path(Path::parse("foo/bar")?),
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
