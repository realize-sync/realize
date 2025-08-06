use super::blob::{self, Blobstore};
use super::db::{ArenaDatabase, ArenaReadTransaction, ArenaWriteTransaction};
use super::types::{LocalAvailability, LruQueueId};
use crate::arena::engine::DirtyPaths;
use crate::arena::notifier::{Notification, Progress};
use crate::global::types::{
    DirTableEntry, FileAvailability, FileContent, FileMetadata, FileTableEntry, InodeAssignment,
    PeerTableEntry, ReadDirEntry,
};
use crate::types::BlobId;
use crate::utils::holder::Holder;
use crate::{Blob, InodeAllocator};
use crate::{Inode, StorageError};
use realize_types::{Arena, ByteRanges, Hash, Path, Peer, UnixTime};
use redb::ReadableTable;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

/// A per-arena cache of remote files.
///
/// This struct handles all cache operations for a specific arena.
/// It contains the arena's database and root inode.
pub(crate) struct ArenaCache {
    arena: Arena,
    arena_root: Inode,
    db: Arc<ArenaDatabase>,
    allocator: Arc<InodeAllocator>,
    blobstore: Arc<Blobstore>,
    dirty_paths: Arc<DirtyPaths>,
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
        Ok(Arc::new(Self {
            arena,
            arena_root,
            allocator,
            db,
            blobstore,
            dirty_paths,
        }))
    }

    pub(crate) fn arena(&self) -> Arena {
        self.arena
    }

    pub(crate) fn arena_root(&self) -> Inode {
        self.arena_root
    }

    pub(crate) fn lookup(
        &self,
        parent_inode: Inode,
        name: &str,
    ) -> Result<ReadDirEntry, StorageError> {
        let txn = self.db.begin_read()?;
        do_lookup(&txn.cache_directory_table()?, parent_inode, name)
    }

    pub(crate) fn lookup_path<T>(&self, path: T) -> Result<(Inode, InodeAssignment), StorageError>
    where
        T: AsRef<Path>,
    {
        let path = path.as_ref();
        let txn = self.db.begin_read()?;
        let dir_table = txn.cache_directory_table()?;

        do_lookup_path(&dir_table, self.arena_root, Some(path))
    }

    pub(crate) fn file_metadata(&self, inode: Inode) -> Result<FileMetadata, StorageError> {
        let txn = self.db.begin_read()?;
        do_file_metadata(&txn, inode)
    }

    pub(crate) fn dir_mtime(&self, inode: Inode) -> Result<UnixTime, StorageError> {
        let txn = self.db.begin_read()?;

        do_dir_mtime(&txn.cache_directory_table()?, inode, self.arena_root)
    }

    pub(crate) fn file_availability(&self, inode: Inode) -> Result<FileAvailability, StorageError> {
        let txn = self.db.begin_read()?;

        do_file_availability(&txn, inode, self.arena)
    }

    pub(crate) fn readdir(
        &self,
        inode: Inode,
    ) -> Result<Vec<(String, ReadDirEntry)>, StorageError> {
        let txn = self.db.begin_read()?;

        do_readdir(&txn.cache_directory_table()?, inode)
    }

    pub(crate) fn peer_progress(&self, peer: Peer) -> Result<Option<Progress>, StorageError> {
        let txn = self.db.begin_read()?;

        do_peer_progress(&txn, peer)
    }

    pub(crate) fn update(
        &self,
        peer: Peer,
        notification: Notification,
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

                let mut file_table = txn.cache_file_table()?;
                let (parent_inode, file_inode) =
                    do_create_file(&txn, self.arena_root, &path, &|| self.allocate_inode(&txn))?;
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
                    do_create_file(&txn, self.arena_root, &path, &|| self.allocate_inode(&txn))?;

                let mut file_table = txn.cache_file_table()?;
                let entry = FileTableEntry::new(path, size, mtime, hash, parent_inode);
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
            }
            Notification::Remove {
                index,
                path,
                old_hash,
                ..
            }
            | Notification::Drop {
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
                    do_create_file(&txn, self.arena_root, &path, &|| self.allocate_inode(&txn))?;

                do_unmark_peer_file(&txn, peer, file_inode)?;

                let mut file_table = txn.cache_file_table()?;
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
                let mut peer_table = txn.cache_peer_table()?;
                let key = peer.as_str();
                if let Some(entry) = peer_table.get(key)? {
                    if entry.value().parse()?.uuid == uuid {
                        // We're connected to the same store as before; there's nothing to do.
                        return Ok(());
                    }
                }
                peer_table.insert(key, Holder::with_content(PeerTableEntry { uuid })?)?;
                let mut notification_table = txn.cache_notification_table()?;
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
            let file_entry = get_default_entry(&txn.cache_file_table()?, inode)?;
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
            let mut file_table = txn.cache_file_table()?;
            let mut file_entry = get_default_entry(&file_table, inode)?;
            let blob = self
                .blobstore
                .create_blob(inode, &txn, file_entry.clone())?;

            file_entry.content.blob = Some(blob.id());
            file_table.insert((inode, ""), Holder::new(&file_entry)?)?;

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
        let (inode, _) =
            do_lookup_path(&txn.cache_directory_table()?, self.arena_root, Some(path))?;
        let mut file_table = txn.cache_file_table()?;
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
        file_table.insert((inode, ""), Holder::with_content(file_entry)?)?;

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
    ) -> Result<Option<PathBuf>, StorageError>
    where
        T: AsRef<Path>,
    {
        let path = path.as_ref();
        let inode = match do_lookup_path(&txn.cache_directory_table()?, self.arena_root, Some(path))
        {
            Ok((inode, _)) => inode,
            Err(StorageError::NotFound) => {
                return Ok(None);
            }
            Err(err) => {
                return Err(err);
            }
        };
        let mut file_table = txn.cache_file_table()?;
        let mut file_entry = get_default_entry(&file_table, inode)?;
        if file_entry.content.hash != *hash {
            return Ok(None);
        }
        let (blob_id, cachepath) = self.blobstore.move_into_blob(
            &txn,
            file_entry.content.blob,
            file_entry.metadata.size,
        )?;
        file_entry.content.blob = Some(blob_id);
        file_table.insert((inode, ""), Holder::with_content(file_entry)?)?;

        Ok(Some(cachepath))
    }

    /// Write an entry in the file table, overwriting any existing one.
    fn do_write_file_entry(
        &self,
        file_table: &mut redb::Table<'_, (Inode, &str), Holder<FileTableEntry>>,
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
        let key = peer.as_str();

        file_table.insert((file_inode, key), Holder::new(entry)?)?;

        Ok(())
    }

    /// Write an entry in the file table, overwriting any existing one.
    fn do_write_default_file_entry(
        &self,
        txn: &ArenaWriteTransaction,
        file_table: &mut redb::Table<'_, (Inode, &str), Holder<FileTableEntry>>,
        file_inode: Inode,
        entry: &FileTableEntry,
    ) -> Result<(), StorageError> {
        let key = "";
        if let Some(old_entry) = file_table.get((file_inode, ""))? {
            let old_entry = old_entry.value().parse()?;
            if let Some(blob_id) = old_entry.content.blob {
                self.blobstore.delete_blob(&txn, blob_id)?;
            }
        }

        // This entry is the outside world view of the file, so
        // changes should be reported.
        self.dirty_paths.mark_dirty(txn, &entry.content.path)?;

        file_table.insert((file_inode, key), Holder::new(entry)?)?;

        Ok(())
    }

    /// Remove a file entry for a specific peer.
    fn do_rm_file_entry(
        &self,
        txn: &ArenaWriteTransaction,
        file_table: &mut redb::Table<'_, (Inode, &str), Holder<FileTableEntry>>,
        dir_table: &mut redb::Table<'_, (Inode, &str), Holder<DirTableEntry>>,
        parent_inode: Inode,
        inode: Inode,
        peer: Peer,
        old_hash: Option<Hash>,
    ) -> Result<(), StorageError> {
        let peer_str = peer.as_str();

        let mut path = None;
        let mut entries = HashMap::new();
        for elt in file_table.range((inode, "")..(inode.plus(1), ""))? {
            let (key, value) = elt?;
            let key = key.value().1;
            let entry = value.value().parse()?;
            if path.is_none() {
                path = Some(entry.content.path.clone());
            }
            entries.insert(key.to_string(), entry);
        }

        let peer_hash = match entries.remove(peer_str).map(|e| e.content.hash) {
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

        file_table.remove((inode, peer_str))?;

        let default_hash = entries.remove("").map(|e| e.content.hash);
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
            if let Some(default_entry) = file_table.get((inode, ""))? {
                let default_entry = default_entry.value().parse()?;
                if let Some(blob_id) = default_entry.content.blob {
                    self.blobstore.delete_blob(&txn, blob_id)?;
                }
            }

            file_table.remove((inode, ""))?;
            dir_table.retain_in(
                (parent_inode, "")..(parent_inode.plus(1), ""),
                |_, v| match v.parse() {
                    Ok(DirTableEntry::Regular(v)) => v.inode != inode,
                    _ => true,
                },
            )?;
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
        let mut dir_table = txn.cache_directory_table()?;
        let (parent_inode, parent_assignment) =
            do_lookup_path(&dir_table, arena_root, path.parent().as_ref())?;
        if parent_assignment != InodeAssignment::Directory {
            return Err(StorageError::NotADirectory);
        }

        let dir_entry =
            get_dir_entry(&dir_table, parent_inode, path.name())?.ok_or(StorageError::NotFound)?;
        if dir_entry.assignment != InodeAssignment::File {
            return Err(StorageError::IsADirectory);
        }

        let inode = dir_entry.inode;
        let mut file_table = txn.cache_file_table()?;
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
        let mut pending_catchup_table = txn.cache_pending_catchup_table()?;
        let mut file_table = txn.cache_file_table()?;
        let mut directory_table = txn.cache_directory_table()?;
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
        let file_entry = get_default_entry(&txn.cache_file_table()?, inode)?;

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
        self.blobstore
            .cleanup_cache(LruQueueId::WorkingArea, target_size)
    }

    /// Allocate a new inode, extending the range if necessary.
    fn allocate_inode(&self, txn: &ArenaWriteTransaction) -> Result<Inode, StorageError> {
        self.allocator
            .allocate_arena_inode(&mut txn.cache_current_inode_range_table()?, self.arena)
    }
}

/// Mark files within the cache dirty.
///
/// If the inode is a file in the cache, it is marked dirty.
//
/// If the inode is a directory in the cache, all the files within it
/// and its subdirectories are marked dirty.
///
/// If the inode is not in the cache, the function does nothing.
pub(crate) fn mark_dirty_recursive(
    txn: &ArenaWriteTransaction,
    arena_root: Inode,
    path: Option<&Path>,
    dirty_paths: &Arc<DirtyPaths>,
) -> Result<(), StorageError> {
    let dir_table = txn.cache_directory_table()?;
    match do_lookup_path(&dir_table, arena_root, path) {
        Ok((inode, _)) => {
            let file_table = txn.cache_file_table()?;
            mark_file_dirty(txn, &file_table, inode, dirty_paths)?;
            mark_dir_dirty(txn, &dir_table, &file_table, inode, dirty_paths)?;

            Ok(())
        }
        Err(StorageError::NotFound) => Ok(()),
        Err(err) => Err(err),
    }
}

fn mark_file_dirty(
    txn: &ArenaWriteTransaction,
    file_table: &redb::Table<'_, (Inode, &str), Holder<FileTableEntry>>,
    inode: Inode,
    dirty_paths: &Arc<DirtyPaths>,
) -> Result<(), StorageError> {
    if let Some(entry) = file_table.get((inode, ""))? {
        dirty_paths.mark_dirty(txn, &entry.value().parse()?.content.path)?;
    }

    Ok(())
}

fn mark_dir_dirty(
    txn: &ArenaWriteTransaction,
    dir_table: &redb::Table<'_, (Inode, &str), Holder<DirTableEntry>>,
    file_table: &redb::Table<'_, (Inode, &str), Holder<FileTableEntry>>,
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

pub(crate) fn do_readdir(
    dir_table: &impl ReadableTable<(Inode, &'static str), Holder<'static, DirTableEntry>>,
    inode: Inode,
) -> Result<Vec<(String, ReadDirEntry)>, StorageError> {
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

pub(crate) fn do_lookup(
    dir_table: &impl ReadableTable<(Inode, &'static str), Holder<'static, DirTableEntry>>,
    parent_inode: Inode,
    name: &str,
) -> Result<ReadDirEntry, StorageError> {
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
    let mut notification_table = txn.cache_notification_table()?;
    notification_table.insert(peer.as_str(), index)?;

    Ok(())
}

fn do_peer_progress(
    txn: &ArenaReadTransaction,
    peer: Peer,
) -> Result<Option<Progress>, StorageError> {
    let key = peer.as_str();

    let peer_table = txn.cache_peer_table()?;
    if let Some(entry) = peer_table.get(key)? {
        let PeerTableEntry { uuid, .. } = entry.value().parse()?;

        let notification_table = txn.cache_notification_table()?;
        if let Some(last_seen) = notification_table.get(key)? {
            return Ok(Some(Progress::new(uuid, last_seen.value())));
        }
    }

    Ok(None)
}

pub(crate) fn do_dir_mtime(
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

// Return the default file entry for a path.
pub(crate) fn get_file_entry_for_path<T>(
    txn: &ArenaReadTransaction,
    root: Inode,
    path: T,
) -> Result<FileTableEntry, StorageError>
where
    T: AsRef<Path>,
{
    let path = path.as_ref();
    let dir_table = txn.cache_directory_table()?;
    let (inode, assignment) = do_lookup_path(&dir_table, root, Some(path))?;
    if assignment != InodeAssignment::File {
        return Err(StorageError::IsADirectory);
    }
    let file_table = txn.cache_file_table()?;

    get_file_entry(&file_table, inode, None)?.ok_or(StorageError::NotFound)
}

/// Get a [FileTableEntry] for a specific peer.
fn get_file_entry(
    file_table: &impl redb::ReadableTable<(Inode, &'static str), Holder<'static, FileTableEntry>>,
    inode: Inode,
    peer: Option<Peer>,
) -> Result<Option<FileTableEntry>, StorageError> {
    match file_table.get((inode, peer.map(|p| p.as_str()).unwrap_or("")))? {
        None => Ok(None),
        Some(e) => Ok(Some(e.value().parse()?)),
    }
}

fn get_default_entry(
    file_table: &impl redb::ReadableTable<(Inode, &'static str), Holder<'static, FileTableEntry>>,
    inode: Inode,
) -> Result<FileTableEntry, StorageError> {
    let entry = file_table.get((inode, ""))?.ok_or(StorageError::NotFound)?;

    Ok(entry.value().parse()?)
}

fn do_file_metadata(
    txn: &ArenaReadTransaction,
    inode: Inode,
) -> Result<FileMetadata, StorageError> {
    Ok(get_default_entry(&txn.cache_file_table()?, inode)?.metadata)
}

fn do_file_availability(
    txn: &ArenaReadTransaction,
    inode: Inode,
    arena: Arena,
) -> Result<FileAvailability, StorageError> {
    let file_table = txn.cache_file_table()?;

    let mut range = file_table.range((inode, "")..(inode.plus(1), ""))?;
    let (default_key, default_entry) = range.next().ok_or(StorageError::NotFound)??;
    if default_key.value().1 != "" {
        log::warn!("File table entry without a default peer: {inode}");
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
        let file_entry: FileTableEntry = entry.1.value().parse()?;
        if file_entry.content.hash == hash {
            let peer = Peer::from(entry.0.value().1);
            peers.push(peer);
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

/// Write an entry in the file table, overwriting any existing one.

/// Retrieve or create a file entry at the given path.
///
/// Return the parent inode and the file inode.
fn do_create_file<T>(
    txn: &ArenaWriteTransaction,
    arena_root: Inode,
    path: T,
    alloc_inode: &impl Fn() -> Result<Inode, StorageError>,
) -> Result<(Inode, Inode), StorageError>
where
    T: AsRef<Path>,
{
    let path = path.as_ref();
    let mut dir_table = txn.cache_directory_table()?;
    let filename = path.name();
    let parent_inode = do_mkdirs(
        &mut dir_table,
        arena_root,
        path.parent().as_ref(),
        alloc_inode,
    )?;
    let dir_entry = get_dir_entry(&dir_table, parent_inode, filename)?;
    let file_inode = match dir_entry {
        None => {
            let new_inode = (alloc_inode)()?;
            add_dir_entry(
                &mut dir_table,
                parent_inode,
                new_inode,
                filename,
                InodeAssignment::File,
            )?;

            new_inode
        }
        Some(dir_entry) => {
            if dir_entry.assignment != InodeAssignment::File {
                return Err(StorageError::IsADirectory);
            }

            dir_entry.inode
        }
    };

    Ok((parent_inode, file_inode))
}

/// Make sure that the given path is a directory; create it if necessary.
///
/// Returns the inode of the directory pointed to by the path.
pub(crate) fn do_mkdirs(
    dir_table: &mut redb::Table<'_, (Inode, &str), Holder<DirTableEntry>>,
    root_inode: Inode,
    path: Option<&Path>,
    alloc_inode: &impl Fn() -> Result<Inode, StorageError>,
) -> Result<Inode, StorageError> {
    let mut current = root_inode;
    for component in Path::components(path) {
        current = if let Some(entry) = get_dir_entry(dir_table, current, component)? {
            if entry.assignment != InodeAssignment::Directory {
                return Err(StorageError::NotADirectory);
            }
            log::debug!("found {component} in {current} -> {entry:?}");

            entry.inode
        } else {
            log::debug!("add {component} in {current}");
            let new_inode = (alloc_inode)()?;
            add_dir_entry(
                dir_table,
                current,
                new_inode,
                component,
                InodeAssignment::Directory,
            )?;

            new_inode
        };
    }

    Ok(current)
}

/// Find the file or directory pointed to by the given path.
pub(crate) fn do_lookup_path(
    dir_table: &impl redb::ReadableTable<(Inode, &'static str), Holder<'static, DirTableEntry>>,
    root_inode: Inode,
    path: Option<&Path>,
) -> Result<(Inode, InodeAssignment), StorageError> {
    let mut current = (root_inode, InodeAssignment::Directory);
    for component in Path::components(path) {
        if current.1 != InodeAssignment::Directory {
            return Err(StorageError::NotADirectory);
        }
        if let Some(entry) = get_dir_entry(dir_table, current.0, component)? {
            current = (entry.inode, entry.assignment);
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
) -> Result<Option<ReadDirEntry>, StorageError> {
    match dir_table.get((parent_inode, name))? {
        None => Ok(None),
        Some(e) => Ok(Some(e.value().parse()?.into_readdir_entry(parent_inode))),
    }
}

/// Add an entry to the given directory.
pub(crate) fn add_dir_entry(
    dir_table: &mut redb::Table<'_, (Inode, &str), Holder<DirTableEntry>>,
    parent_inode: Inode,
    new_inode: Inode,
    name: &str,
    assignment: InodeAssignment,
) -> Result<(), StorageError> {
    log::debug!("new dir entry {parent_inode} {name} -> {new_inode} {assignment:?}");
    dir_table.insert(
        (parent_inode, name),
        Holder::with_content(DirTableEntry::Regular(ReadDirEntry {
            inode: new_inode,
            assignment,
        }))?,
    )?;
    let mtime = UnixTime::now();
    let dot = Holder::with_content(DirTableEntry::Dot(mtime))?;
    dir_table.insert((parent_inode, "."), dot.clone())?;
    if assignment == InodeAssignment::Directory {
        dir_table.insert((new_inode, "."), dot.clone())?;
    }

    Ok(())
}

fn do_mark_peer_files(txn: &ArenaWriteTransaction, peer: Peer) -> Result<(), StorageError> {
    let file_table = txn.cache_file_table()?;
    let mut pending_catchup_table = txn.cache_pending_catchup_table()?;
    let peer_str = peer.as_str();
    for elt in file_table.iter()? {
        let (k, v) = elt?;
        let k = k.value();
        if k.1 != peer_str {
            continue;
        }
        let v = v.value().parse()?;
        let inode = k.0;
        pending_catchup_table.insert((peer_str, inode), v.parent_inode)?;
    }

    Ok(())
}

fn do_unmark_peer_file(
    txn: &ArenaWriteTransaction,
    peer: Peer,
    inode: Inode,
) -> Result<(), StorageError> {
    let mut pending_catchup_table = txn.cache_pending_catchup_table()?;
    pending_catchup_table.remove((peer.as_str(), inode))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::arena::db::{ArenaDatabase, ArenaReadTransaction, ArenaWriteTransaction};
    use crate::arena::engine;
    use crate::arena::notifier::Notification;
    use crate::global::types::{FileMetadata, InodeAssignment};
    use crate::utils::redb_utils;
    use crate::{
        DirtyPaths, GlobalDatabase, Inode, InodeAllocator, LocalAvailability, StorageError,
    };
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use realize_types::{Arena, Hash, Path, Peer, UnixTime};
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};

    use super::mark_dirty_recursive;

    use super::ArenaCache;

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
        dirty_paths: Arc<DirtyPaths>,
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
                dirty_paths,
                _tempdir: tempdir,
            })
        }

        fn arena_root(&self) -> Inode {
            self.acache.arena_root()
        }

        fn begin_read(&self) -> anyhow::Result<ArenaReadTransaction> {
            Ok(self.db.begin_read()?)
        }

        fn begin_write(&self) -> anyhow::Result<ArenaWriteTransaction> {
            Ok(self.db.begin_write()?)
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

        fn add_file<T>(&self, path: T, size: u64, mtime: UnixTime) -> anyhow::Result<()>
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
            )?;

            Ok(())
        }

        fn remove_file<T>(&self, path: T) -> anyhow::Result<()>
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
            )?;

            Ok(())
        }
    }

    #[tokio::test]
    async fn add_creates_directories() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let acache = &fixture.acache;
        let file_path = Path::parse("a/b/c.txt")?;
        let mtime = test_time();

        fixture.add_file(&file_path, 100, mtime)?;

        let entry = acache.lookup(acache.arena_root(), "a")?;
        assert_eq!(entry.assignment, InodeAssignment::Directory, "a");

        let entry = acache.lookup(entry.inode, "b")?;
        assert_eq!(entry.assignment, InodeAssignment::Directory, "b");

        Ok(())
    }

    #[tokio::test]
    async fn add_updates_dir_mtime() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena).await?;
        let mtime = test_time();
        let path1 = Path::parse("a/b/1.txt")?;
        fixture.add_file(&path1, 100, mtime)?;
        let dir = Path::parse("a/b")?;
        let dir_mtime = fixture.dir_mtime(&dir)?;

        let path2 = Path::parse("a/b/2.txt")?;
        fixture.add_file(&path2, 100, mtime)?;

        assert!(fixture.dir_mtime(&dir)? > dir_mtime);
        Ok(())
    }

    #[tokio::test]
    async fn add_marks_dirty() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let file_path = Path::parse("a/b/c.txt")?;

        fixture.add_file(&file_path, 100, test_time())?;

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
        )?;
        assert!(!engine::is_dirty(&fixture.begin_read()?, &file_path)?);

        Ok(())
    }

    #[tokio::test]
    async fn ignore_duplicate_add() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let file_path = Path::parse("file.txt")?;

        fixture.add_file(&file_path, 100, test_time())?;
        fixture.add_file(&file_path, 200, test_time())?;

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

        fixture.add_file(&file_path, 100, mtime)?;
        let arena_root = acache.arena_root();
        acache.lookup(arena_root, "file.txt")?;
        fixture.remove_file(&file_path)?;
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

        fixture.add_file(&file_path, 100, mtime)?;
        let arena_root = acache.arena_root();
        acache.lookup(arena_root, "file.txt")?;

        fixture.clear_dirty()?;
        fixture.remove_file(&file_path)?;
        assert!(engine::is_dirty(&fixture.begin_read()?, &file_path)?);

        Ok(())
    }

    #[tokio::test]
    async fn unlink_updates_dir_mtime() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena).await?;
        let file_path = Path::parse("file.txt")?;
        let mtime = test_time();

        fixture.add_file(&file_path, 100, mtime)?;
        let arena_root = fixture.acache.arena_root();
        let dir_mtime = fixture.acache.dir_mtime(arena_root)?;
        fixture.remove_file(&file_path)?;

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

        fixture.add_file(&file_path, 100, mtime)?;
        acache.update(
            test_peer(),
            Notification::Remove {
                arena,
                index: 1,
                path: file_path.clone(),
                old_hash: Hash([2u8; 32]), // != test_hash()
            },
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

        fixture.add_file(&file_path, 100, mtime)?;

        // Lookup directory
        let dir_entry = acache.lookup(acache.arena_root(), "a")?;
        assert_eq!(dir_entry.assignment, InodeAssignment::Directory);

        // Lookup file
        let file_entry = acache.lookup(dir_entry.inode, "file.txt")?;
        assert_eq!(file_entry.assignment, InodeAssignment::File);

        let metadata = acache.file_metadata(file_entry.inode)?;
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

        fixture.add_file(&path, 100, mtime)?;

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

        fixture.add_file(&Path::parse("dir/file1.txt")?, 100, mtime)?;
        fixture.add_file(&Path::parse("dir/file2.txt")?, 200, mtime)?;
        fixture.add_file(&Path::parse("dir/subdir/file3.txt")?, 300, mtime)?;

        let arena_root = acache.arena_root();
        assert_unordered::assert_eq_unordered!(
            vec![("dir".to_string(), InodeAssignment::Directory),],
            acache
                .readdir(arena_root)?
                .into_iter()
                .map(|(name, entry)| (name, entry.assignment))
                .collect::<Vec<_>>(),
        );

        let dir_entry = acache.lookup(arena_root, "dir")?;
        assert_unordered::assert_eq_unordered!(
            vec![
                ("file1.txt".to_string(), InodeAssignment::File),
                ("file2.txt".to_string(), InodeAssignment::File),
                ("subdir".to_string(), InodeAssignment::Directory),
            ],
            acache
                .readdir(dir_entry.inode)?
                .into_iter()
                .map(|(name, entry)| (name, entry.assignment))
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
        )?;

        let file_entry = acache.lookup(acache.arena_root, "file.txt")?;
        let metadata = acache.file_metadata(file_entry.inode)?;
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
        )?;
        let inode = acache.lookup(acache.arena_root(), "file.txt")?.inode;
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
        )?;
        let inode = acache.lookup(acache.arena_root, "file.txt")?.inode;
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
        )?;
        let inode = acache.lookup(acache.arena_root, "file.txt")?.inode;
        let avail = acache.file_availability(inode)?;
        assert_eq!(vec![a], avail.peers);
        assert_eq!(Hash([3u8; 32]), avail.hash);

        acache.update(a, Notification::CatchupStart(arena))?;
        acache.update(
            a,
            Notification::CatchupComplete {
                arena: arena,
                index: 0,
            },
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
        )?;

        // Simulate a catchup that only reports file2 and file4.
        acache.update(peer1, Notification::CatchupStart(arena))?;
        acache.update(
            peer1,
            Notification::Catchup {
                arena: arena,
                path: file2.clone(),
                size: 10,
                mtime: mtime.clone(),
                hash: test_hash(),
            },
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
        )?;
        acache.update(
            peer1,
            Notification::CatchupComplete {
                arena: arena,
                index: 0,
            },
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

    #[tokio::test]
    async fn mark_dirty_recursive_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let mtime = test_time();

        // Add a single file
        let file_path = Path::parse("foo/bar.txt")?;
        fixture.add_file(&file_path, 100, mtime)?;
        fixture.clear_dirty()?;

        // Mark the file dirty recursively
        {
            let txn = fixture.begin_write()?;
            mark_dirty_recursive(
                &txn,
                fixture.arena_root(),
                Some(&file_path),
                &fixture.dirty_paths,
            )?;
            txn.commit()?;
        }

        // Check that the file is marked dirty
        {
            let txn = fixture.begin_read()?;
            assert!(engine::is_dirty(&txn, &file_path)?);
        }

        Ok(())
    }

    #[tokio::test]
    async fn mark_dirty_recursive_directory() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let mtime = test_time();

        // Add files in a directory structure
        let a = Path::parse("foo/a.txt")?;
        let b = Path::parse("foo/b.txt")?;
        let c = Path::parse("foo/subdir/c.txt")?;
        let d = Path::parse("foo/subdir/d.txt")?;
        let e = Path::parse("bar/e.txt")?;

        for file in vec![&a, &b, &c, &d, &e] {
            fixture.add_file(file, 100, mtime)?;
        }
        fixture.clear_dirty()?;

        // Mark the foo directory dirty recursively
        {
            let txn = fixture.begin_write()?;
            mark_dirty_recursive(
                &txn,
                fixture.arena_root(),
                Some(&Path::parse("foo")?),
                &fixture.dirty_paths,
            )?;
            txn.commit()?;
        }

        // Check that all files under foo are marked dirty
        {
            let txn = fixture.begin_read()?;
            assert!(engine::is_dirty(&txn, &a)?);
            assert!(engine::is_dirty(&txn, &b)?);
            assert!(engine::is_dirty(&txn, &c)?);
            assert!(engine::is_dirty(&txn, &d)?);

            // Files outside foo should not be dirty
            assert!(!engine::is_dirty(&txn, &e)?);
        }

        Ok(())
    }

    #[tokio::test]
    async fn mark_dirty_recursive_nested_directory() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let mtime = test_time();

        // Add files in a deeply nested structure
        let a = Path::parse("foo/a.txt")?;
        let b = Path::parse("foo/subdir/b.txt")?;
        let c = Path::parse("foo/subdir/deep/c.txt")?;
        let d = Path::parse("foo/subdir/deep/very/d.txt")?;
        let e = Path::parse("foo/other/e.txt")?;

        for file in vec![&a, &b, &c, &d, &e] {
            fixture.add_file(file, 100, mtime)?;
        }
        fixture.clear_dirty()?;

        // Mark the subdir directory dirty recursively
        {
            let txn = fixture.begin_write()?;
            mark_dirty_recursive(
                &txn,
                fixture.arena_root(),
                Some(&Path::parse("foo/subdir")?),
                &fixture.dirty_paths,
            )?;
            txn.commit()?;
        }

        // Check that only files under subdir are marked dirty
        {
            let txn = fixture.begin_read()?;
            // Files under subdir should be dirty
            assert!(engine::is_dirty(&txn, &b)?);
            assert!(engine::is_dirty(&txn, &c)?);
            assert!(engine::is_dirty(&txn, &d)?);

            // Files outside subdir should not be dirty
            assert!(!engine::is_dirty(&txn, &a)?);
            assert!(!engine::is_dirty(&txn, &e)?);
        }

        Ok(())
    }

    #[tokio::test]
    async fn mark_dirty_recursive_nonexistent_inode() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let mtime = test_time();

        // Add some files
        for file in vec!["foo/a.txt", "bar/b.txt"] {
            let path = Path::parse(file)?;
            fixture.add_file(&path, 100, mtime)?;
        }
        fixture.clear_dirty()?;

        // Mark a non-existent inode dirty recursively
        {
            let acache = &fixture.acache;
            let txn = acache.db.begin_write()?;
            // Use a very high inode number that shouldn't exist
            let nonexistent_inode = Inode(999999);
            mark_dirty_recursive(
                &txn,
                nonexistent_inode,
                Some(&Path::parse("foo")?),
                &fixture.dirty_paths,
            )?;
            txn.commit()?;
        }

        // Check that no files are marked dirty (function should do nothing)
        {
            let txn = fixture.begin_write()?;
            assert_eq!(None, engine::take_dirty(&txn)?);
        }

        Ok(())
    }

    #[tokio::test]
    async fn mark_dirty_recursive_nonexistent_path() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let mtime = test_time();

        // Add some files
        for file in vec!["foo/a.txt", "bar/b.txt"] {
            let path = Path::parse(file)?;
            fixture.add_file(&path, 100, mtime)?;
        }
        fixture.clear_dirty()?;

        {
            let acache = &fixture.acache;
            let txn = acache.db.begin_write()?;
            mark_dirty_recursive(
                &txn,
                fixture.arena_root(),
                Some(&Path::parse("doesnotexist")?),
                &fixture.dirty_paths,
            )?;
            txn.commit()?;
        }

        // Check that no files are marked dirty (function should do nothing)
        {
            let txn = fixture.begin_write()?;
            assert_eq!(None, engine::take_dirty(&txn)?);
        }

        Ok(())
    }

    #[tokio::test]
    async fn mark_dirty_recursive_arena_root() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;

        // Add files in different directories
        let files = vec![
            Path::parse("foo/a.txt")?,
            Path::parse("foo/b.txt")?,
            Path::parse("bar/c.txt")?,
            Path::parse("baz/d.txt")?,
        ];

        let mtime = test_time();
        for file in &files {
            fixture.add_file(file, 100, mtime)?;
        }
        fixture.clear_dirty()?;

        // Mark the whole dirty
        {
            let txn = fixture.begin_write()?;
            mark_dirty_recursive(&txn, fixture.arena_root(), None, &fixture.dirty_paths)?;
            txn.commit()?;
        }

        // Check that all files in the arena are marked dirty
        {
            let txn = fixture.begin_read()?;
            for file in files {
                assert!(engine::is_dirty(&txn, &file)?, "{file} should be dirty",);
            }
        }

        Ok(())
    }

    // Tests for local_availability method
    #[tokio::test]
    async fn local_availability_missing_when_no_blob() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena()).await?;
        let file_path = Path::parse("test.txt")?;
        let mtime = test_time();

        // Add a file without opening it (so no blob is created)
        fixture.add_file(&file_path, 100, mtime)?;

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
        fixture.add_file(&file_path, 100, mtime)?;
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
        fixture.add_file(&file_path, 100, mtime)?;
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
        fixture.add_file(&file_path, 100, mtime)?;
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
        fixture.add_file(&file_path, 100, mtime)?;
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
        fixture.add_file(&file_path, 1000, mtime)?;
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
        fixture.add_file(&file_path, 0, mtime)?;
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
        fixture.add_file(&file_path, 100, mtime)?;
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
}
