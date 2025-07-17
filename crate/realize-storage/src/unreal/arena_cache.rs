use super::blob::OpenBlob;
use super::types::{
    BlobId, BlobTableEntry, DirTableEntry, FileAvailability, FileContent, FileMetadata,
    FileTableEntry, InodeAssignment, PeerTableEntry, ReadDirEntry,
};
use crate::real::notifier::{Notification, Progress};
use crate::utils::holder::Holder;
use crate::{Inode, StorageError};
use realize_types::{Arena, ByteRanges, Hash, Path, Peer, UnixTime};
use redb::{Database, ReadTransaction, ReadableTable, TableDefinition, WriteTransaction};
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;

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
pub(crate) const DIRECTORY_TABLE: TableDefinition<(Inode, &str), Holder<DirTableEntry>> =
    TableDefinition::new("directory");

/// Track peer files.
///
/// Each known peer file has an entry in this table, keyed with the
/// file inode and the peer name. More than one peer might have the
/// same entry.
///
/// An inode available in no peers should be remove from all
/// directories.
///
/// Key: (inode, peer)
/// Value: FileTableEntry
const FILE_TABLE: TableDefinition<(Inode, &str), Holder<FileTableEntry>> =
    TableDefinition::new("file");

/// Track peer files that might have been deleted remotely.
///
/// When a peer starts catchup of an arena, all its files are added to
/// this table. Calls to catchup for that peer and arena removes the
/// corresponding entry in the table. At the end of catchup, files
/// still in this table are deleted.
///
/// Key: (peer, file inode)
/// Value: parent dir inode
const PENDING_CATCHUP_TABLE: TableDefinition<(&str, Inode), Inode> =
    TableDefinition::new("pending_catchup");

/// Track Peer UUIDs.
///
/// This table tracks the store UUID for each peer.
///
/// Key: &str (Peer)
/// Value: PeerTableEntry
const PEER_TABLE: TableDefinition<&str, Holder<PeerTableEntry>> = TableDefinition::new("peer");

/// Track last seen notification index.
///
/// This table tracks the last seen notification index for each peer.
///
/// Key: &str (Peer)
/// Value: last seen index
const NOTIFICATION_TABLE: TableDefinition<&str, u64> = TableDefinition::new("notification");

/// Track current inode range for each arena.
///
/// The current inode is the last inode that was allocated for the
/// arena.
///
/// Key: ()
/// Value: (Inode, Inode) (last inode allocated, end of range)
pub(crate) const CURRENT_INODE_RANGE_TABLE: TableDefinition<(), (Inode, Inode)> =
    TableDefinition::new("current_inode_range");

/// Track blobs.
///
/// Key: inode
/// Value: BlobTableEntry
const BLOB_TABLE: TableDefinition<BlobId, Holder<BlobTableEntry>> = TableDefinition::new("blob");

/// A per-arena cache of remote files.
///
/// This struct handles all cache operations for a specific arena.
/// It contains the arena's database and root inode.
pub(crate) struct ArenaCache {
    arena: Arena,
    arena_root: Inode,
    db: Database,
    blob_dir: PathBuf,
}

impl ArenaCache {
    /// Create a new ArenaUnrealCacheBlocking from an arena, root inode, database, and blob directory.
    pub(crate) fn new(
        arena: Arena,
        arena_root: Inode,
        db: Database,
        blob_dir: PathBuf,
    ) -> Result<Self, StorageError> {
        // Ensure the database has the required tables
        {
            let txn = db.begin_write()?;
            txn.open_table(DIRECTORY_TABLE)?;
            txn.open_table(FILE_TABLE)?;
            txn.open_table(PENDING_CATCHUP_TABLE)?;
            txn.open_table(PEER_TABLE)?;
            txn.open_table(NOTIFICATION_TABLE)?;
            txn.open_table(CURRENT_INODE_RANGE_TABLE)?;
            txn.open_table(BLOB_TABLE)?;

            // ARENA_TABLE and INODE_RANGE_ALLOCATION_TABLE are only used in UnrealCacheBlocking
            txn.commit()?;
        }

        if !blob_dir.exists() {
            std::fs::create_dir_all(&blob_dir)?;
        }

        Ok(Self {
            arena,
            arena_root,
            db,
            blob_dir,
        })
    }

    pub(crate) fn arena(&self) -> Arena {
        self.arena
    }

    pub(crate) fn lookup(
        &self,
        parent_inode: Inode,
        name: &str,
    ) -> Result<ReadDirEntry, StorageError> {
        let txn = self.db.begin_read()?;
        do_lookup(txn, parent_inode, name)
    }

    pub(crate) fn lookup_path(
        &self,
        path: &Path,
    ) -> Result<(Inode, InodeAssignment), StorageError> {
        let txn = self.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;

        do_lookup_path(&dir_table, self.arena_root, &Some(path.clone()))
    }

    pub(crate) fn file_metadata(&self, inode: Inode) -> Result<FileMetadata, StorageError> {
        let txn = self.db.begin_read()?;
        do_file_metadata(&txn, inode)
    }

    pub(crate) fn dir_mtime(&self, inode: Inode) -> Result<UnixTime, StorageError> {
        let txn = self.db.begin_read()?;

        do_dir_mtime(&txn, inode, self.arena_root)
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

        do_readdir(&txn, inode)
    }

    pub(crate) fn peer_progress(&self, peer: Peer) -> Result<Option<Progress>, StorageError> {
        let txn = self.db.begin_read()?;

        do_peer_progress(&txn, peer)
    }

    pub(crate) fn update(
        &self,
        peer: Peer,
        notification: Notification,
        alloc_inode_range: impl Fn() -> Result<(Inode, Inode), StorageError>,
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

                let mut file_table = txn.open_table(FILE_TABLE)?;
                let (parent_inode, file_inode) =
                    do_create_file(&txn, self.arena_root, &path, &alloc_inode_range)?;
                if !get_file_entry(&file_table, file_inode, Some(peer))?.is_some() {
                    let entry = FileTableEntry::new(path, size, mtime, hash, parent_inode);
                    let mut blob_table = txn.open_table(BLOB_TABLE)?;

                    if !get_file_entry(&file_table, file_inode, None)?.is_some() {
                        self.do_write_file_entry(
                            &mut file_table,
                            file_inode,
                            None,
                            &entry,
                            Some(&mut blob_table),
                        )?;
                    }
                    self.do_write_file_entry(
                        &mut file_table,
                        file_inode,
                        Some(peer),
                        &entry,
                        None,
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

                let (parent_inode, file_inode) =
                    do_create_file(&txn, self.arena_root, &path, &alloc_inode_range)?;

                let mut file_table = txn.open_table(FILE_TABLE)?;
                let entry = FileTableEntry::new(path, size, mtime, hash, parent_inode);
                let mut blob_table = txn.open_table(BLOB_TABLE)?;
                if let Some(e) = get_file_entry(&file_table, file_inode, None)?
                    && e.content.hash == old_hash
                {
                    // If it overwrites the entry that's current, it's
                    // necessarily an entry we want.
                    self.do_write_file_entry(
                        &mut file_table,
                        file_inode,
                        None,
                        &entry,
                        Some(&mut blob_table),
                    )?;
                    self.do_write_file_entry(
                        &mut file_table,
                        file_inode,
                        Some(peer),
                        &entry,
                        None,
                    )?;
                } else if let Some(e) = get_file_entry(&file_table, file_inode, Some(peer))?
                    && e.content.hash == old_hash
                {
                    // If it overwrites the peer's entry, we want to
                    // keep that.
                    self.do_write_file_entry(
                        &mut file_table,
                        file_inode,
                        Some(peer),
                        &entry,
                        None,
                    )?;
                }
            }
            Notification::Remove {
                index,
                path,
                old_hash,
                ..
            } => {
                do_update_last_seen_notification(&txn, peer, index)?;

                let root = self.arena_root;
                let mut blob_table = txn.open_table(BLOB_TABLE)?;
                self.do_unlink(&txn, peer, root, &path, old_hash, Some(&mut blob_table))?;
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
                    do_create_file(&txn, self.arena_root, &path, &alloc_inode_range)?;

                do_unmark_peer_file(&txn, peer, file_inode)?;

                let mut file_table = txn.open_table(FILE_TABLE)?;
                let entry = FileTableEntry::new(path, size, mtime, hash, parent_inode);
                let mut blob_table = txn.open_table(BLOB_TABLE)?;
                if !get_file_entry(&file_table, file_inode, None)?.is_some() {
                    self.do_write_file_entry(
                        &mut file_table,
                        file_inode,
                        None,
                        &entry,
                        Some(&mut blob_table),
                    )?;
                }
                self.do_write_file_entry(&mut file_table, file_inode, Some(peer), &entry, None)?;
            }
            Notification::CatchupComplete { index, .. } => {
                self.do_delete_marked_files(&txn, peer)?;
                do_update_last_seen_notification(&txn, peer, index)?;
            }
            Notification::Connected { uuid, .. } => {
                let mut peer_table = txn.open_table(PEER_TABLE)?;
                let key = peer.as_str();
                if let Some(entry) = peer_table.get(key)? {
                    if entry.value().parse()?.uuid == uuid {
                        // We're connected to the same store as before; there's nothing to do.
                        return Ok(());
                    }
                }
                peer_table.insert(key, Holder::with_content(PeerTableEntry { uuid })?)?;
                let mut notification_table = txn.open_table(NOTIFICATION_TABLE)?;
                notification_table.remove(key)?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Open a file for reading/writing.
    pub(crate) fn open_file(&self, inode: Inode) -> Result<OpenBlob, StorageError> {
        // Optimistically, try a read transaction to check whether the
        // blob is there.
        {
            let txn = self.db.begin_read()?;
            let file_entry = get_default_entry(&txn.open_table(FILE_TABLE)?, inode)?;
            if let Some(blob_id) = file_entry.content.blob {
                let blob_entry = get_blob_entry(&txn.open_table(BLOB_TABLE)?, blob_id)?;
                let file = self.open_blob_file(blob_id, file_entry.metadata.size, false)?;
                return Ok(OpenBlob {
                    blob_id,
                    file_entry,
                    blob_entry,
                    file,
                });
            }
        }

        // Switch to a write transaction to create the blob. We need to read
        // the file entry again because it might have changed.
        let txn = self.db.begin_write()?;
        let ret = {
            let mut file_table = txn.open_table(FILE_TABLE)?;
            let mut file_entry = get_default_entry(&file_table, inode)?;
            if let Some(blob_id) = file_entry.content.blob {
                let blob_entry = get_blob_entry(&txn.open_table(BLOB_TABLE)?, blob_id)?;
                let file = self.open_blob_file(blob_id, file_entry.metadata.size, false)?;
                return Ok(OpenBlob {
                    blob_id,
                    file_entry,
                    blob_entry,
                    file,
                });
            }

            let mut blob_table = txn.open_table(BLOB_TABLE)?;
            let blob_id = blob_table
                .last()?
                .map(|(k, _)| k.value())
                .unwrap_or(BlobId(1));
            log::debug!(
                "assigned blob {blob_id} to file {inode} {}",
                file_entry.content.hash
            );
            let file = self.open_blob_file(blob_id, file_entry.metadata.size, true)?;
            let blob_entry = BlobTableEntry {
                written_areas: ByteRanges::new(),
            };
            blob_table.insert(blob_id, Holder::new(&blob_entry)?)?;
            file_entry.content.blob = Some(blob_id);
            file_table.insert((inode, ""), Holder::new(&file_entry)?)?;

            OpenBlob {
                blob_id,
                file_entry,
                blob_entry,
                file,
            }
        };
        txn.commit()?;

        Ok(ret)
    }

    /// Open or create a file for the blob and make sure it has the
    /// right size.
    fn open_blob_file(
        &self,
        blob_id: BlobId,
        file_size: u64,
        new_file: bool,
    ) -> Result<std::fs::File, StorageError> {
        let path = self.blob_path(blob_id);
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(new_file)
            .create(true)
            .open(path)?;
        let file_meta = file.metadata()?;
        if file_size != file_meta.len() {
            file.set_len(file_size)?;
            file.flush()?;
        }

        Ok(file)
    }

    /// Return the path of the file for the given blob.
    fn blob_path(&self, blob_id: BlobId) -> PathBuf {
        self.blob_dir.join(blob_id.to_string())
    }

    /// Delete a blob and its associated file.
    fn delete_blob(&self, blob_id: BlobId) -> Result<(), StorageError> {
        let blob_path = self.blob_path(blob_id);
        if blob_path.exists() {
            std::fs::remove_file(&blob_path)?;
        }
        Ok(())
    }

    /// Write an entry in the file table, overwriting any existing one.
    fn do_write_file_entry(
        &self,
        file_table: &mut redb::Table<'_, (Inode, &str), Holder<FileTableEntry>>,
        file_inode: Inode,
        peer: Option<Peer>,
        entry: &FileTableEntry,
        blob_table: Option<&mut redb::Table<'_, BlobId, Holder<BlobTableEntry>>>,
    ) -> Result<(), StorageError> {
        let key = match peer {
            None => "",
            Some(peer) => {
                log::debug!(
                    "new file entry {file_inode} on {peer} {}",
                    entry.content.hash
                );
                peer.as_str()
            }
        };

        // If this is overwriting the default entry (no peer), check if the old entry had a blob
        if peer.is_none() {
            if let Some(old_entry) = file_table.get((file_inode, ""))? {
                let old_entry = old_entry.value().parse()?;
                if let Some(blob_id) = old_entry.content.blob {
                    self.delete_blob(blob_id)?;
                    if let Some(blob_table) = blob_table {
                        blob_table.remove(blob_id)?;
                    }
                }
            }
        }

        file_table.insert((file_inode, key), Holder::new(entry)?)?;

        Ok(())
    }

    /// Remove a file entry for a specific peer.
    fn do_rm_file_entry(
        &self,
        file_table: &mut redb::Table<'_, (Inode, &str), Holder<FileTableEntry>>,
        dir_table: &mut redb::Table<'_, (Inode, &str), Holder<DirTableEntry>>,
        parent_inode: Inode,
        inode: Inode,
        peer: Peer,
        old_hash: Option<Hash>,
        blob_table: Option<&mut redb::Table<'_, BlobId, Holder<BlobTableEntry>>>,
    ) -> Result<(), StorageError> {
        let peer_str = peer.as_str();

        let mut entries = HashMap::new();
        for elt in file_table.range((inode, "")..(inode.plus(1), ""))? {
            let (key, value) = elt?;
            let key = key.value().1;
            let entry = value.value().parse()?;
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

            // Check if the default entry has a blob and delete it
            if let Some(default_entry) = file_table.get((inode, ""))? {
                let default_entry = default_entry.value().parse()?;
                if let Some(blob_id) = default_entry.content.blob {
                    self.delete_blob(blob_id)?;
                    if let Some(blob_table) = blob_table {
                        blob_table.remove(blob_id)?;
                    }
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
            self.do_write_file_entry(file_table, inode, None, &entry, blob_table)?;
        }

        Ok(())
    }

    fn do_unlink(
        &self,
        txn: &WriteTransaction,
        peer: Peer,
        arena_root: Inode,
        path: &Path,
        old_hash: Hash,
        blob_table: Option<&mut redb::Table<'_, BlobId, Holder<BlobTableEntry>>>,
    ) -> Result<(), StorageError> {
        let mut dir_table = txn.open_table(DIRECTORY_TABLE)?;
        let (parent_inode, parent_assignment) =
            do_lookup_path(&dir_table, arena_root, &path.parent())?;
        if parent_assignment != InodeAssignment::Directory {
            return Err(StorageError::NotADirectory);
        }

        let dir_entry =
            get_dir_entry(&dir_table, parent_inode, path.name())?.ok_or(StorageError::NotFound)?;
        if dir_entry.assignment != InodeAssignment::File {
            return Err(StorageError::IsADirectory);
        }

        let inode = dir_entry.inode;
        let mut file_table = txn.open_table(FILE_TABLE)?;
        self.do_rm_file_entry(
            &mut file_table,
            &mut dir_table,
            parent_inode,
            inode,
            peer,
            Some(old_hash),
            blob_table,
        )?;

        Ok(())
    }

    /// Delete all marked files for a peer.
    fn do_delete_marked_files(
        &self,
        txn: &WriteTransaction,
        peer: Peer,
    ) -> Result<(), StorageError> {
        let mut pending_catchup_table = txn.open_table(PENDING_CATCHUP_TABLE)?;
        let mut file_table = txn.open_table(FILE_TABLE)?;
        let mut directory_table = txn.open_table(DIRECTORY_TABLE)?;
        let mut blob_table = txn.open_table(BLOB_TABLE)?;
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
                &mut file_table,
                &mut directory_table,
                parent_inode,
                inode,
                peer,
                None,
                Some(&mut blob_table),
            )?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn local_availability(&self, blob_id: BlobId) -> Result<ByteRanges, StorageError> {
        let txn = self.db.begin_read()?;
        let blob_table = txn.open_table(BLOB_TABLE)?;

        Ok(get_blob_entry(&blob_table, blob_id)?.written_areas)
    }

    pub(crate) fn extend_local_availability(
        &self,
        blob_id: BlobId,
        new_range: &ByteRanges,
    ) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut blob_table = txn.open_table(BLOB_TABLE)?;
            let mut blob_entry = get_blob_entry(&blob_table, blob_id)?;
            blob_entry.written_areas = blob_entry.written_areas.union(&new_range);
            log::debug!(
                "{blob_id} extended by {new_range}; available: {}",
                blob_entry.written_areas
            );

            blob_table.insert(blob_id, Holder::with_content(blob_entry)?)?;
        }
        txn.commit()?;

        Ok(())
    }
}

pub(crate) fn do_readdir(
    txn: &ReadTransaction,
    inode: Inode,
) -> Result<Vec<(String, ReadDirEntry)>, StorageError> {
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

pub(crate) fn do_lookup(
    txn: ReadTransaction,
    parent_inode: Inode,
    name: &str,
) -> Result<ReadDirEntry, StorageError> {
    let dir_table = txn.open_table(DIRECTORY_TABLE)?;

    Ok(dir_table
        .get((parent_inode, name))?
        .ok_or(StorageError::NotFound)?
        .value()
        .parse()?
        .into_readdir_entry(parent_inode))
}

fn do_update_last_seen_notification(
    txn: &WriteTransaction,
    peer: Peer,
    index: u64,
) -> Result<(), StorageError> {
    let mut notification_table = txn.open_table(NOTIFICATION_TABLE)?;
    notification_table.insert(peer.as_str(), index)?;

    Ok(())
}

fn do_peer_progress(txn: &ReadTransaction, peer: Peer) -> Result<Option<Progress>, StorageError> {
    let key = peer.as_str();

    let peer_table = txn.open_table(PEER_TABLE)?;
    if let Some(entry) = peer_table.get(key)? {
        let PeerTableEntry { uuid, .. } = entry.value().parse()?;

        let notification_table = txn.open_table(NOTIFICATION_TABLE)?;
        if let Some(last_seen) = notification_table.get(key)? {
            return Ok(Some(Progress::new(uuid, last_seen.value())));
        }
    }

    Ok(None)
}

pub(crate) fn do_dir_mtime(
    txn: &ReadTransaction,
    inode: Inode,
    root: Inode,
) -> Result<UnixTime, StorageError> {
    let dir_table = txn.open_table(DIRECTORY_TABLE)?;
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

/// Get a [FileTableEntry] for a specific peer.
fn get_file_entry(
    file_table: &redb::Table<'_, (Inode, &str), Holder<FileTableEntry>>,
    inode: Inode,
    peer: Option<Peer>,
) -> Result<Option<FileTableEntry>, StorageError> {
    match file_table.get((inode, peer.map(|p| p.as_str()).unwrap_or("")))? {
        None => Ok(None),
        Some(e) => Ok(Some(e.value().parse()?)),
    }
}

fn get_blob_entry(
    blob_table: &impl redb::ReadableTable<BlobId, Holder<'static, BlobTableEntry>>,
    blob_id: BlobId,
) -> Result<BlobTableEntry, StorageError> {
    let entry = blob_table
        .get(blob_id)?
        .ok_or(StorageError::NotFound)?
        .value()
        .parse()?;

    Ok(entry)
}

fn get_default_entry(
    file_table: &impl redb::ReadableTable<(Inode, &'static str), Holder<'static, FileTableEntry>>,
    inode: Inode,
) -> Result<FileTableEntry, StorageError> {
    let entry = file_table.get((inode, ""))?.ok_or(StorageError::NotFound)?;

    Ok(entry.value().parse()?)
}

fn do_file_metadata(txn: &ReadTransaction, inode: Inode) -> Result<FileMetadata, StorageError> {
    Ok(get_default_entry(&txn.open_table(FILE_TABLE)?, inode)?.metadata)
}

fn do_file_availability(
    txn: &ReadTransaction,
    inode: Inode,
    arena: Arena,
) -> Result<FileAvailability, StorageError> {
    let file_table = txn.open_table(FILE_TABLE)?;

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
fn do_create_file(
    txn: &WriteTransaction,
    arena_root: Inode,
    path: &Path,
    alloc_inode_range: &impl Fn() -> Result<(Inode, Inode), StorageError>,
) -> Result<(Inode, Inode), StorageError> {
    let mut dir_table = txn.open_table(DIRECTORY_TABLE)?;
    let filename = path.name();
    let parent_inode = do_mkdirs(
        txn,
        &mut dir_table,
        arena_root,
        &path.parent(),
        alloc_inode_range,
    )?;
    let dir_entry = get_dir_entry(&dir_table, parent_inode, filename)?;
    let file_inode = match dir_entry {
        None => {
            let new_inode = alloc_inode(txn, alloc_inode_range)?;
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

    log::debug!("new dir entry {parent_inode} {filename} {file_inode}");
    Ok((parent_inode, file_inode))
}

/// Make sure that the given path is a directory; create it if necessary.
///
/// Returns the inode of the directory pointed to by the path.
pub(crate) fn do_mkdirs(
    txn: &WriteTransaction,
    dir_table: &mut redb::Table<'_, (Inode, &str), Holder<DirTableEntry>>,
    root_inode: Inode,
    path: &Option<Path>,
    alloc_inode_range: &impl Fn() -> Result<(Inode, Inode), StorageError>,
) -> Result<Inode, StorageError> {
    log::debug!("mkdirs {root_inode} {path:?}");
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
            let new_inode = alloc_inode(txn, alloc_inode_range)?;
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
fn do_lookup_path(
    dir_table: &impl redb::ReadableTable<(Inode, &'static str), Holder<'static, DirTableEntry>>,
    root_inode: Inode,
    path: &Option<Path>,
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

/// Allocate a new inode for an arena.
///
/// This function allocates a new inode from the arena's current range.
/// If the arena has no range or the current range is exhausted,
/// it allocates a new range.
///
/// Returns the allocated inode.
pub(crate) fn alloc_inode(
    txn: &WriteTransaction,
    alloc_inode_range: impl Fn() -> Result<(Inode, Inode), StorageError>,
) -> Result<Inode, StorageError> {
    let mut current_range_table = txn.open_table(CURRENT_INODE_RANGE_TABLE)?;
    // Read the current range directly from the write transaction
    let current_range = match current_range_table.get(())? {
        Some(value) => {
            let (current, end) = value.value();
            Some((current, end))
        }
        None => None,
    };
    match current_range {
        Some((current, end)) if (current.plus(1)) < end => {
            // We have inodes available in the current range
            let inode = current.plus(1);
            current_range_table.insert((), (inode, end))?;
            Ok(inode)
        }
        _ => {
            // Need to allocate a new range
            let (start, end) = alloc_inode_range()?;
            let inode = start;
            current_range_table.insert((), (start, end))?;
            Ok(inode)
        }
    }
}

fn do_mark_peer_files(txn: &WriteTransaction, peer: Peer) -> Result<(), StorageError> {
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
        let inode = k.0;
        pending_catchup_table.insert((peer_str, inode), v.parent_inode)?;
    }

    Ok(())
}

fn do_unmark_peer_file(
    txn: &WriteTransaction,
    peer: Peer,
    inode: Inode,
) -> Result<(), StorageError> {
    let mut pending_catchup_table = txn.open_table(PENDING_CATCHUP_TABLE)?;
    pending_catchup_table.remove((peer.as_str(), inode))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::MetadataExt as _;

    use super::super::cache::UnrealCacheBlocking;
    use super::*;
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use realize_types::ByteRange;
    use realize_types::{Arena, Path, Peer};

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
        cache: UnrealCacheBlocking,
        tempdir: TempDir,
    }
    impl Fixture {
        fn setup_with_arena(arena: Arena) -> anyhow::Result<Fixture> {
            let _ = env_logger::try_init();
            let tempdir = TempDir::new()?;
            let path = tempdir.path().join("unreal.db");
            let mut cache = UnrealCacheBlocking::open(&path)?;

            let child = tempdir.child(format!("{arena}-cache.db"));
            let blob_dir = tempdir.child(format!("{arena}/blobs"));
            if let Some(p) = child.parent() {
                std::fs::create_dir_all(p)?;
            }
            cache.add_arena(
                arena,
                Database::create(child.path())?,
                blob_dir.to_path_buf(),
            )?;

            Ok(Self {
                arena: arena,
                cache,
                tempdir,
            })
        }

        fn arena_cache(&self) -> anyhow::Result<&ArenaCache> {
            Ok(self.cache.arena_cache(self.arena)?)
        }

        fn parent_dir_mtime(&self, arena: Arena, path: &Path) -> anyhow::Result<UnixTime> {
            let arena_root = self.cache.arena_root(arena).expect("arena was added");
            match path.parent() {
                None => Ok(self.cache.dir_mtime(arena_root)?),
                Some(path) => {
                    let (inode, _) = self.cache.lookup_path(arena, &path)?;

                    Ok(self.cache.dir_mtime(inode)?)
                }
            }
        }

        fn add_file(&self, path: &Path, size: u64, mtime: &UnixTime) -> anyhow::Result<()> {
            self.cache.update(
                test_peer(),
                Notification::Add {
                    arena: test_arena(),
                    index: 1,
                    path: path.clone(),
                    mtime: mtime.clone(),
                    size,
                    hash: test_hash(),
                },
            )?;

            Ok(())
        }

        fn remove_file(&self, path: &Path) -> anyhow::Result<()> {
            self.cache.update(
                test_peer(),
                Notification::Remove {
                    arena: test_arena(),
                    index: 1,
                    path: path.clone(),
                    old_hash: test_hash(),
                },
            )?;

            Ok(())
        }

        /// Check if a blob file exists for the given blob ID in the test arena.
        fn blob_file_exists(&self, arena: Arena, blob_id: BlobId) -> bool {
            self.blob_path(arena, blob_id).exists()
        }

        /// Return the path to a blob file for test use.
        fn blob_path(&self, arena: Arena, blob_id: BlobId) -> std::path::PathBuf {
            self.tempdir
                .child(format!("{arena}/blobs/{blob_id}"))
                .to_path_buf()
        }
    }

    #[test]
    fn add_creates_directories() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let cache = &fixture.cache;
        let file_path = Path::parse("a/b/c.txt")?;
        let mtime = test_time();

        fixture.add_file(&file_path, 100, &mtime)?;

        let entry = cache.lookup(UnrealCacheBlocking::ROOT_DIR, "test_arena")?;
        assert_eq!(entry.assignment, InodeAssignment::Directory, "test_arena");

        let entry = cache.lookup(entry.inode, "a")?;
        assert_eq!(entry.assignment, InodeAssignment::Directory, "a");

        let entry = cache.lookup(entry.inode, "b")?;
        assert_eq!(entry.assignment, InodeAssignment::Directory, "b");

        Ok(())
    }

    #[test]
    fn add_updates_dir_mtime() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        let mtime = test_time();
        let path1 = Path::parse("a/b/1.txt")?;
        fixture.add_file(&path1, 100, &mtime)?;
        let dir_mtime = fixture.parent_dir_mtime(arena, &path1)?;

        let path2 = Path::parse("a/b/2.txt")?;
        fixture.add_file(&path2, 100, &mtime)?;

        assert!(fixture.parent_dir_mtime(arena, &path2)? > dir_mtime);
        Ok(())
    }

    #[test]
    fn replace_existing_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let cache = &fixture.cache;
        let peer = test_peer();
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;

        cache.update(
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
        cache.update(
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

        let acache = fixture.arena_cache()?;
        let (inode, _) = acache.lookup_path(&file_path)?;
        let metadata = acache.file_metadata(inode)?;
        assert_eq!(metadata.size, 200);
        assert_eq!(metadata.mtime, later_time());

        Ok(())
    }

    #[test]
    fn ignore_duplicate_add() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let file_path = Path::parse("file.txt")?;

        fixture.add_file(&file_path, 100, &test_time())?;
        fixture.add_file(&file_path, 200, &test_time())?;

        let acache = fixture.arena_cache()?;
        let (inode, _) = acache.lookup_path(&file_path)?;
        let metadata = acache.file_metadata(inode)?;
        assert_eq!(metadata.size, 100);

        Ok(())
    }

    #[test]
    fn ignore_replace_with_wrong_hash() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let cache = &fixture.cache;
        let file_path = Path::parse("file.txt")?;
        let peer = test_peer();

        cache.update(
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
        cache.update(
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

        let acache = fixture.arena_cache()?;
        let (inode, _) = acache.lookup_path(&file_path)?;
        let metadata = acache.file_metadata(inode)?;
        assert_eq!(metadata.size, 100);

        Ok(())
    }

    #[test]
    fn unlink_removes_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let cache = &fixture.cache;
        let file_path = Path::parse("file.txt")?;
        let mtime = test_time();

        fixture.add_file(&file_path, 100, &mtime)?;
        let arena_root = cache.arena_root(test_arena())?;
        cache.lookup(arena_root, "file.txt")?;
        fixture.remove_file(&file_path)?;
        assert!(matches!(
            cache.lookup(arena_root, "file.txt"),
            Err(StorageError::NotFound)
        ));

        Ok(())
    }

    #[test]
    fn unlink_updates_dir_mtime() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        let file_path = Path::parse("file.txt")?;
        let mtime = test_time();

        fixture.add_file(&file_path, 100, &mtime)?;
        let dir_mtime = fixture.parent_dir_mtime(arena, &file_path)?;
        fixture.remove_file(&file_path)?;

        assert!(fixture.parent_dir_mtime(arena, &file_path)? > dir_mtime);

        Ok(())
    }

    #[test]
    fn unlink_ignores_wrong_old_hash() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let cache = &fixture.cache;
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;
        let mtime = test_time();

        fixture.add_file(&file_path, 100, &mtime)?;
        cache.update(
            test_peer(),
            Notification::Remove {
                arena: arena,
                index: 1,
                path: file_path.clone(),
                old_hash: Hash([2u8; 32]), // != test_hash()
            },
        )?;

        // File should still exist because wrong hash was provided
        let acache = fixture.arena_cache()?;
        let (inode, _) = acache.lookup_path(&file_path)?;
        let metadata = acache.file_metadata(inode)?;
        assert_eq!(metadata.size, 100);

        Ok(())
    }

    #[test]
    fn lookup_finds_entry() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let cache = &fixture.cache;
        let arena = test_arena();
        let file_path = Path::parse("a/file.txt")?;
        let mtime = test_time();

        fixture.add_file(&file_path, 100, &mtime)?;

        // Lookup directory
        let dir_entry = cache.lookup(cache.arena_root(arena)?, "a")?;
        assert_eq!(dir_entry.assignment, InodeAssignment::Directory);

        // Lookup file
        let file_entry = cache.lookup(dir_entry.inode, "file.txt")?;
        assert_eq!(file_entry.assignment, InodeAssignment::File);

        let acache = fixture.arena_cache()?;
        let metadata = acache.file_metadata(file_entry.inode)?;
        assert_eq!(metadata.mtime, mtime);
        assert_eq!(metadata.size, 100);

        Ok(())
    }

    #[test]
    fn lookup_returns_notfound_for_missing_entry() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let cache = &fixture.cache;

        assert!(matches!(
            cache.lookup(cache.arena_root(test_arena())?, "nonexistent"),
            Err(StorageError::NotFound),
        ));

        Ok(())
    }

    #[test]
    fn lookup_path_finds_entry() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let cache = &fixture.cache;
        let arena = test_arena();
        let path = Path::parse("a/b/c/file.txt")?;
        let mtime = test_time();

        fixture.add_file(&path, 100, &mtime)?;

        let (inode, assignment) = cache.lookup_path(arena, &path)?;
        assert_eq!(assignment, InodeAssignment::File);

        let acache = fixture.arena_cache()?;
        let metadata = acache.file_metadata(inode)?;
        assert_eq!(metadata.mtime, mtime);
        assert_eq!(metadata.size, 100);

        Ok(())
    }

    #[test]
    fn readdir_returns_all_entries() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let cache = &fixture.cache;
        let arena = test_arena();
        let mtime = test_time();

        fixture.add_file(&Path::parse("dir/file1.txt")?, 100, &mtime)?;
        fixture.add_file(&Path::parse("dir/file2.txt")?, 200, &mtime)?;
        fixture.add_file(&Path::parse("dir/subdir/file3.txt")?, 300, &mtime)?;

        assert_unordered::assert_eq_unordered!(
            vec![(arena.to_string(), InodeAssignment::Directory),],
            cache
                .readdir(UnrealCacheBlocking::ROOT_DIR)?
                .into_iter()
                .map(|(name, entry)| (name, entry.assignment))
                .collect::<Vec<_>>(),
        );

        let arena_root = cache.arena_root(arena)?;
        assert_unordered::assert_eq_unordered!(
            vec![("dir".to_string(), InodeAssignment::Directory),],
            cache
                .readdir(arena_root)?
                .into_iter()
                .map(|(name, entry)| (name, entry.assignment))
                .collect::<Vec<_>>(),
        );

        let dir_entry = cache.lookup(arena_root, "dir")?;
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
    fn get_file_metadata_tracks_hash_chain() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let cache = &fixture.cache;

        let peer1 = Peer::from("peer1");
        let peer2 = Peer::from("peer2");
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;

        cache.update(
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
        cache.update(
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
        cache.update(
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

        let acache = fixture.arena_cache()?;
        let file_entry = acache.lookup(acache.arena_root, "file.txt")?;
        let metadata = acache.file_metadata(file_entry.inode)?;
        assert_eq!(metadata.size, 200);
        assert_eq!(metadata.mtime, later_time());

        Ok(())
    }

    #[test]
    fn file_availability_deals_with_conflicting_adds() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let cache = &fixture.cache;

        let a = Peer::from("a");
        let b = Peer::from("b");
        let c = Peer::from("c");
        let arena = test_arena();
        let path = Path::parse("file.txt")?;

        cache.update(
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
        cache.update(
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
        cache.update(
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
        let acache = fixture.arena_cache()?;
        let inode = acache.lookup(acache.arena_root, "file.txt")?.inode;
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

    #[test]
    fn file_availablility_deals_with_different_versions() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let cache = &fixture.cache;

        let a = Peer::from("a");
        let b = Peer::from("b");
        let c = Peer::from("c");
        let arena = test_arena();
        let path = Path::parse("file.txt")?;

        cache.update(
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
        cache.update(
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
        cache.update(
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
        cache.update(
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
        let acache = fixture.arena_cache()?;
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
        cache.update(
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
        cache.update(
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
        let acache = fixture.arena_cache()?;
        let avail = acache.file_availability(inode)?;
        assert_eq!(Hash([3u8; 32]), avail.hash);
        assert_eq!(vec![c], avail.peers);

        // Later on, b joins the party
        cache.update(
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

        let acache = fixture.arena_cache()?;
        let avail = acache.file_availability(inode)?;
        assert_eq!(Hash([3u8; 32]), avail.hash);
        assert_unordered::assert_eq_unordered!(vec![b, c], avail.peers);

        Ok(())
    }

    #[test]
    fn file_availability_when_a_peer_goes_away() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let cache = &fixture.cache;

        let a = Peer::from("a");
        let b = Peer::from("b");
        let c = Peer::from("c");
        let arena = test_arena();
        let path = Path::parse("file.txt")?;

        cache.update(
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
        cache.update(
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
        cache.update(
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
        cache.update(
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
        let acache = fixture.arena_cache()?;
        let inode = acache.lookup(acache.arena_root, "file.txt")?.inode;
        let avail = acache.file_availability(inode)?;
        assert_eq!(vec![a], avail.peers);
        assert_eq!(Hash([3u8; 32]), avail.hash);

        cache.update(a, Notification::CatchupStart(arena))?;
        cache.update(
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
        let acache = fixture.arena_cache()?;
        let avail = acache.file_availability(inode)?;
        assert_eq!(vec![c], avail.peers);
        assert_eq!(Hash([2u8; 32]), avail.hash);

        Ok(())
    }

    #[test]
    fn mark_and_delete_peer_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
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

        cache.update(
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

        cache.update(
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
        cache.update(
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

        cache.update(
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
        cache.update(
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
        cache.update(
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

        cache.update(
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
        cache.update(peer1, Notification::CatchupStart(arena))?;
        cache.update(
            peer1,
            Notification::Catchup {
                arena: arena,
                path: file2.clone(),
                size: 10,
                mtime: mtime.clone(),
                hash: test_hash(),
            },
        )?;
        cache.update(
            peer1,
            Notification::Catchup {
                arena: arena,
                path: file4.clone(),
                size: 10,
                mtime: mtime.clone(),
                hash: test_hash(),
            },
        )?;
        cache.update(
            peer1,
            Notification::CatchupComplete {
                arena: arena,
                index: 0,
            },
        )?;

        // File1 should have been deleted, since it was only on peer1,
        assert!(matches!(
            cache.lookup_path(arena, &file1),
            Err(StorageError::NotFound)
        ));

        // File2 should still be available, from peer2
        let acache = fixture.arena_cache()?;
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

    #[test]
    fn open_file_creates_sparse_file() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        let acache = fixture.arena_cache()?;
        let file_path = Path::parse("foobar")?;

        fixture.add_file(&file_path, 10000, &test_time())?;
        let inode = acache.lookup(acache.arena_root, "foobar")?.inode;

        let blob_id = {
            let blob = acache.open_file(inode)?;
            assert_eq!(BlobId(1), blob.blob_id);

            let m = fixture.blob_path(arena, blob.blob_id).metadata()?;

            // File should have the right size
            assert_eq!(10000, m.len());

            // File should be sparse
            assert_eq!(0, m.blocks());

            // Range empty for now
            assert_eq!(ByteRanges::new(), blob.blob_entry.written_areas);

            blob.blob_id
        };

        // If called a second time, it should return a handle on the same file.
        let blob = acache.open_file(inode)?;
        assert_eq!(blob_id, blob.blob_id);
        assert_eq!(ByteRanges::new(), blob.blob_entry.written_areas);

        Ok(())
    }

    #[test]
    fn blob_deleted_on_file_overwrite() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let cache = &fixture.cache;
        let acache = fixture.arena_cache()?;
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;

        // Create a file
        fixture.add_file(&file_path, 100, &test_time())?;

        // Open the file to create a blob
        let (inode, _) = acache.lookup_path(&file_path)?;
        let blob_id = acache.open_file(inode)?.blob_id;

        // Verify the blob file was created
        assert!(fixture.blob_file_exists(arena, blob_id));

        // Overwrite the file with a new version
        cache.update(
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

        // Verify the blob file has been deleted
        assert!(!fixture.blob_file_exists(arena, blob_id));

        Ok(())
    }

    #[test]
    fn blob_deleted_on_file_removal() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let acache = fixture.arena_cache()?;
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;

        // Create a file
        fixture.add_file(&file_path, 100, &test_time())?;

        // Open the file to create a blob
        let (inode, _) = acache.lookup_path(&file_path)?;
        let blob_id = acache.open_file(inode)?.blob_id;

        // Verify the blob file was created
        assert!(fixture.blob_file_exists(arena, blob_id));

        // Remove the file
        fixture.remove_file(&file_path)?;

        // Verify the blob file has been deleted
        assert!(!fixture.blob_file_exists(arena, blob_id));

        Ok(())
    }

    #[test]
    fn blob_deleted_on_catchup_removal() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(test_arena())?;
        let cache = &fixture.cache;
        let acache = fixture.arena_cache()?;
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;

        // Create a file
        fixture.add_file(&file_path, 100, &test_time())?;

        // Open the file to create a blob
        let (inode, _) = acache.lookup_path(&file_path)?;
        let blob_id = acache.open_file(inode)?.blob_id;

        // Verify the blob file was created
        assert!(fixture.blob_file_exists(arena, blob_id));

        // Do a catchup that doesn't include this file (simulating file removal)
        cache.update(test_peer(), Notification::CatchupStart(arena))?;
        // Note: No Catchup notification for the file, so it will be deleted
        cache.update(
            test_peer(),
            Notification::CatchupComplete {
                arena: arena,
                index: 0,
            },
        )?;

        // Verify the blob file has been deleted
        assert!(!fixture.blob_file_exists(arena, blob_id));

        Ok(())
    }

    #[test]
    fn blob_update_extends_range() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(arena)?;
        let acache = fixture.arena_cache()?;
        let file_path = Path::parse("file.txt")?;

        // Create a file
        fixture.add_file(&file_path, 1000, &test_time())?;

        // Open the file to create a blob
        let (inode, _) = acache.lookup_path(&file_path)?;
        let blob_id = acache.open_file(inode)?.blob_id;

        // Initially, the blob should have empty written areas
        let initial_ranges = acache.local_availability(blob_id)?;
        assert!(initial_ranges.is_empty());

        // Update the blob with some written areas
        let written_areas = ByteRanges::from_ranges(vec![
            ByteRange::new(0, 100),
            ByteRange::new(200, 300),
            ByteRange::new(500, 600),
        ]);

        acache.extend_local_availability(blob_id, &written_areas)?;

        // Verify the written areas were updated
        let retrieved_ranges = acache.local_availability(blob_id)?;
        assert_eq!(retrieved_ranges, written_areas);

        acache.extend_local_availability(
            blob_id,
            &ByteRanges::from_ranges(vec![ByteRange::new(50, 210), ByteRange::new(200, 400)]),
        )?;

        // Verify the ranges were updated again
        let final_ranges = acache.local_availability(blob_id)?;
        assert_eq!(
            final_ranges,
            ByteRanges::from_ranges(vec![ByteRange::new(0, 400), ByteRange::new(500, 600)])
        );

        // Test that getting ranges for a non-existent blob returns NotFound
        assert!(matches!(
            acache.local_availability(BlobId(99999)),
            Err(StorageError::NotFound)
        ));

        // Test that updating ranges for a non-existent blob returns NotFound
        assert!(matches!(
            acache.extend_local_availability(BlobId(99999), &ByteRanges::new()),
            Err(StorageError::NotFound)
        ));

        Ok(())
    }
}
