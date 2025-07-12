//! The Unreal cache - a partial local cache of remote files.
//!
//! See `spec/unreal.md` for details.

use super::types::{
    DirTableEntry, FileAvailability, FileContent, FileMetadata, FileTableEntry, InodeAssignment,
    PeerTableEntry, ReadDirEntry,
};
use crate::StorageError;
use crate::config::StorageConfig;
use crate::real::notifier::{Notification, Progress};
use crate::utils::holder::Holder;
use realize_types::{Arena, Hash, Path, Peer, UnixTime};
use redb::{Database, ReadTransaction, ReadableTable, TableDefinition, WriteTransaction};
use std::collections::HashMap;
use std::path;
use std::sync::Arc;
use tokio::task;

/// Size of allocated ranges.
const RANGE_SIZE: u64 = 10000;

/// Maps arena to their root directory inode.
///
/// Arenas in this table can also be accessed as subdirectories of the
/// root directory (1).
///
/// Key: arena name
/// Value: root inode of arena
const ARENA_TABLE: TableDefinition<&str, u64> = TableDefinition::new("arena");

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
const DIRECTORY_TABLE: TableDefinition<(u64, &str), Holder<DirTableEntry>> =
    TableDefinition::new("directory");

/// Track peer files.
///
/// Each known peer file has an entry in this table, keyed with the
/// file inode and the peer name. More than one peer might have the
/// same entry.
///
/// An inode available in no peers should be remove from all
/// directories. This is handled by [do_rm_file_entry].
///
/// Key: (inode, peer)
/// Value: FileEntry
const FILE_TABLE: TableDefinition<(u64, &str), Holder<FileTableEntry>> =
    TableDefinition::new("file");

/// Track peer files that might have been deleted remotely.
///
/// When a peer starts catchup of an arena, all its files are added to
/// this table. Calls to catchup for that peer and arena removes the
/// corresponding entry in the table. At the end of catchup, files
/// still in this table are deleted.
///
/// This is handled by [do_mark_peer_files], [do_delete_marked_files]
/// and [do_unmark_peer_file].
///
/// Key: (peer, arena, file inode)
/// Value: parent dir inode
const PENDING_CATCHUP_TABLE: TableDefinition<(&str, &str, u64), u64> =
    TableDefinition::new("pending_catchup");

/// Track Peer UUIDs.
///
/// This table tracks the store UUID for each peer and arena.
///
/// Key: (&str, &str) (Peer, Arena)
/// Value: PeerTableEntry
const PEER_TABLE: TableDefinition<(&str, &str), Holder<PeerTableEntry>> =
    TableDefinition::new("peer");

/// Track last seen notification index.
///
/// Key: (&str, &str) (Peer, Arena)
/// Value: last seen index
const NOTIFICATION_TABLE: TableDefinition<(&str, &str), u64> = TableDefinition::new("notification");

/// Track inode range allocation for arenas.
///
/// This table allocates increasing ranges of inodes to arenas.
/// Key: u64 (inode - end of range)
/// Value: &str (arena name, "" for global inodes)
///
/// To find which arena a given inode N belongs to, lookup the range [N..];
/// the first element returned is the end of the current range to which N belongs.
const INODE_RANGE_ALLOCATION_TABLE: TableDefinition<u64, &str> =
    TableDefinition::new("inode_range_allocation");

/// Track current inode range for each arena.
///
/// The current inode is the last inode that was allocated for the
/// arena.
///
/// Key: &str (arena name)
/// Value: (u64, u64) (last inode allocated, end of range)
const CURRENT_INODE_RANGE_TABLE: TableDefinition<&str, (u64, u64)> =
    TableDefinition::new("current_inode_range");

/// A cache of remote files.
pub struct UnrealCacheBlocking {
    db: Database,
    arena_map: HashMap<Arena, u64>,
}

impl UnrealCacheBlocking {
    /// Inode of the root dir.
    pub const ROOT_DIR: u64 = 1;

    /// Create a new UnrealCache from a redb database.
    pub fn new(db: Database) -> Result<Self, StorageError> {
        {
            let txn = db.begin_write()?;
            txn.open_table(ARENA_TABLE)?;
            txn.open_table(DIRECTORY_TABLE)?;
            txn.open_table(FILE_TABLE)?;
            txn.open_table(PENDING_CATCHUP_TABLE)?;
            txn.open_table(PEER_TABLE)?;
            txn.open_table(NOTIFICATION_TABLE)?;
            txn.open_table(INODE_RANGE_ALLOCATION_TABLE)?;
            txn.open_table(CURRENT_INODE_RANGE_TABLE)?;
            txn.commit()?;
        }

        let arena_map = {
            let txn = db.begin_read()?;
            do_read_arena_map(&txn)?
        };

        Ok(Self { db, arena_map })
    }

    /// Open or create an UnrealCache at the given path.
    pub fn open(path: &path::Path) -> Result<Self, StorageError> {
        Self::new(Database::create(path)?)
    }

    /// Lists arenas available in this database
    pub fn arenas(&self) -> impl Iterator<Item = &Arena> {
        self.arena_map.keys()
    }

    /// Returns the inode of an arena.
    ///
    /// Will return [UnrealCacheError::UnknownArena] unless the arena
    /// is available in the cache.
    pub fn arena_root(&self, arena: &Arena) -> Result<u64, StorageError> {
        self.arena_map
            .get(arena)
            .copied()
            .ok_or_else(|| StorageError::UnknownArena(arena.clone()))
    }

    /// Add an arena to the database.
    pub fn add_arena(&mut self, arena: &Arena) -> anyhow::Result<()> {
        if self.arena_map.contains_key(arena) {
            return Ok(());
        }

        for existing in self.arena_map.keys() {
            check_arena_compatibility(arena, existing)?;
        }

        let inode: u64;
        {
            let txn = self.db.begin_write()?;
            inode = do_add_arena(&txn, arena)?;
            txn.commit()?;
        }

        self.arena_map.insert(arena.clone(), inode);

        Ok(())
    }

    /// Transform this cache into an async cache.
    pub fn into_async(self) -> UnrealCacheAsync {
        UnrealCacheAsync::new(self)
    }

    /// Lookup a directory entry.
    pub fn lookup(&self, parent_inode: u64, name: &str) -> Result<ReadDirEntry, StorageError> {
        let txn = self.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;

        Ok(dir_table
            .get((parent_inode, name))?
            .ok_or(StorageError::NotFound)?
            .value()
            .parse()?
            .into_readdir_entry(parent_inode))
    }

    /// Lookup the inode and type of the file or directory pointed to by a path.
    pub fn lookup_path(
        &self,
        parent_inode: u64,
        path: &Path,
    ) -> Result<(u64, InodeAssignment), StorageError> {
        let txn = self.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;

        do_lookup_path(&dir_table, parent_inode, &Some(path.clone()))
    }

    /// Return the best metadata for the file.
    pub fn file_metadata(&self, inode: u64) -> Result<FileMetadata, StorageError> {
        let txn = self.db.begin_read()?;

        do_file_availability(&txn, inode).map(|a| a.metadata)
    }

    /// Return the mtime of the directory.
    pub fn dir_mtime(&self, inode: u64) -> Result<UnixTime, StorageError> {
        let txn = self.db.begin_read()?;

        do_dir_mtime(&txn, inode)
    }

    /// Return valid peer file entries for the file.
    ///
    /// The returned vector might be empty if the file isn't available in any peer.
    pub fn file_availability(&self, inode: u64) -> Result<FileAvailability, StorageError> {
        let txn = self.db.begin_read()?;

        do_file_availability(&txn, inode)
    }

    pub fn readdir(&self, inode: u64) -> Result<Vec<(String, ReadDirEntry)>, StorageError> {
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
            if let DirTableEntry::Regular(entry) = value.value().parse()? {
                entries.push((name, entry.clone()));
            }
        }

        Ok(entries)
    }

    /// Return a [Progress] instance that represents how up-to-date
    /// the information in the cache is for that peer and arena.
    ///
    /// This should be passed to the peer when subscribing.
    pub fn peer_progress(
        &self,
        peer: &Peer,
        arena: &Arena,
    ) -> Result<Option<Progress>, StorageError> {
        let txn = self.db.begin_read()?;

        do_peer_progress(&txn, peer, arena)
    }

    pub fn update(&self, peer: &Peer, notification: Notification) -> Result<(), StorageError> {
        log::debug!("notification from {peer}: {notification:?}");
        let txn = self.db.begin_write()?;
        match notification {
            Notification::Add {
                arena,
                index,
                path,
                mtime,
                size,
                hash,
            } => {
                do_update_last_seen_notification(&txn, peer, &arena, index)?;

                let mut file_table = txn.open_table(FILE_TABLE)?;
                let (parent_inode, file_inode) =
                    do_create_file(&txn, self.arena_root(&arena)?, &arena, &path)?;
                if !get_file_entry(&file_table, file_inode, Some(peer))?.is_some() {
                    let entry = FileTableEntry::new(arena, path, size, mtime, hash, parent_inode);

                    if !get_file_entry(&file_table, file_inode, None)?.is_some() {
                        do_write_file_entry(&mut file_table, file_inode, None, &entry)?;
                    }
                    do_write_file_entry(&mut file_table, file_inode, Some(peer), &entry)?;
                }
            }
            Notification::Replace {
                arena,
                index,
                path,
                mtime,
                size,
                hash,
                old_hash,
            } => {
                do_update_last_seen_notification(&txn, peer, &arena, index)?;

                let (parent_inode, file_inode) =
                    do_create_file(&txn, self.arena_root(&arena)?, &arena, &path)?;

                let mut file_table = txn.open_table(FILE_TABLE)?;
                let entry = FileTableEntry::new(arena, path, size, mtime, hash, parent_inode);
                if let Some(e) = get_file_entry(&file_table, file_inode, None)?
                    && e.content.hash == old_hash
                {
                    // If it overwrites the entry that's current, it's
                    // necessarily an entry we want.
                    do_write_file_entry(&mut file_table, file_inode, None, &entry)?;
                    do_write_file_entry(&mut file_table, file_inode, Some(peer), &entry)?;
                } else if let Some(e) = get_file_entry(&file_table, file_inode, Some(peer))?
                    && e.content.hash == old_hash
                {
                    // If it overwrites the peer's entry, we want to
                    // keep that.
                    do_write_file_entry(&mut file_table, file_inode, Some(peer), &entry)?;
                }
            }
            Notification::Remove {
                arena,
                index,
                path,
                old_hash,
            } => {
                do_update_last_seen_notification(&txn, peer, &arena, index)?;

                let root = self.arena_root(&arena)?;
                do_unlink(&txn, peer, root, &path, old_hash)?;
            }
            Notification::CatchupStart(arena) => {
                do_mark_peer_files(&txn, peer, &arena)?;
            }
            Notification::Catchup {
                arena,
                path,
                mtime,
                size,
                hash,
            } => {
                let (parent_inode, file_inode) =
                    do_create_file(&txn, self.arena_root(&arena)?, &arena, &path)?;

                do_unmark_peer_file(&txn, peer, &arena, file_inode)?;

                let mut file_table = txn.open_table(FILE_TABLE)?;
                let entry = FileTableEntry::new(arena, path, size, mtime, hash, parent_inode);
                if !get_file_entry(&file_table, file_inode, None)?.is_some() {
                    do_write_file_entry(&mut file_table, file_inode, None, &entry)?;
                }
                do_write_file_entry(&mut file_table, file_inode, Some(peer), &entry)?;
            }
            Notification::CatchupComplete { arena, index } => {
                do_delete_marked_files(&txn, peer, &arena)?;
                do_update_last_seen_notification(&txn, peer, &arena, index)?;
            }
            Notification::Connected { arena, uuid } => {
                let mut peer_table = txn.open_table(PEER_TABLE)?;
                let key = (peer.as_str(), arena.as_str());
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
}

#[derive(Clone)]
pub struct UnrealCacheAsync {
    inner: Arc<UnrealCacheBlocking>,
}

impl UnrealCacheAsync {
    /// Inode of the root dir.
    pub const ROOT_DIR: u64 = UnrealCacheBlocking::ROOT_DIR;

    /// Create and configure a cache from configuration.
    pub fn from_config(config: &StorageConfig) -> anyhow::Result<Option<Self>> {
        match &config.cache {
            None => Ok(None),
            Some(cache_config) => {
                let mut cache = UnrealCacheBlocking::open(&cache_config.db)?;
                for arena in config.arenas.keys() {
                    cache.add_arena(arena)?;
                }
                Ok(Some(cache.into_async()))
            }
        }
    }

    /// Create a new cache from a blocking one.
    pub fn new(inner: UnrealCacheBlocking) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Create a new cache with the database at the given path.
    pub async fn open<T>(arenas: T, path: &path::Path) -> Result<Self, anyhow::Error>
    where
        T: IntoIterator<Item = Arena> + Send + 'static,
    {
        let path = path.to_path_buf();

        task::spawn_blocking(move || {
            let mut cache = UnrealCacheBlocking::open(&path)?;
            for arena in arenas.into_iter() {
                cache.add_arena(&arena)?;
            }

            Ok::<_, anyhow::Error>(Self::new(cache))
        })
        .await?
    }

    /// Create a new cache with the database at the given path.
    pub async fn with_db<T>(arenas: T, db: redb::Database) -> Result<Self, anyhow::Error>
    where
        T: IntoIterator<Item = Arena> + Send + 'static,
    {
        task::spawn_blocking(move || {
            let mut cache = UnrealCacheBlocking::new(db)?;
            for arena in arenas.into_iter() {
                cache.add_arena(&arena)?;
            }

            Ok::<_, anyhow::Error>(Self::new(cache))
        })
        .await?
    }

    /// Return a reference on the blocking cache.
    pub fn blocking(&self) -> Arc<UnrealCacheBlocking> {
        Arc::clone(&self.inner)
    }

    pub fn arenas(&self) -> impl Iterator<Item = &Arena> {
        self.inner.arenas()
    }

    pub fn arena_root(&self, arena: &Arena) -> Result<u64, StorageError> {
        self.inner.arena_root(arena)
    }

    pub async fn lookup(
        &self,
        parent_inode: u64,
        name: &str,
    ) -> Result<ReadDirEntry, StorageError> {
        let name = name.to_string();
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || inner.lookup(parent_inode, &name)).await?
    }

    pub async fn lookup_path(
        &self,
        parent_inode: u64,
        path: &Path,
    ) -> Result<(u64, InodeAssignment), StorageError> {
        let path = path.clone();
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || inner.lookup_path(parent_inode, &path)).await?
    }

    pub async fn file_metadata(&self, inode: u64) -> Result<FileMetadata, StorageError> {
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || inner.file_metadata(inode)).await?
    }

    pub async fn file_availability(&self, inode: u64) -> Result<FileAvailability, StorageError> {
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || inner.file_availability(inode)).await?
    }

    pub async fn dir_mtime(&self, inode: u64) -> Result<UnixTime, StorageError> {
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || inner.dir_mtime(inode)).await?
    }

    pub async fn readdir(&self, inode: u64) -> Result<Vec<(String, ReadDirEntry)>, StorageError> {
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || inner.readdir(inode)).await?
    }

    /// Return a [Progress] instance that represents how up-to-date
    /// the information in the cache is for that peer and arena.
    ///
    /// This should be passed to the peer when subscribing.
    pub async fn peer_progress(
        &self,
        peer: &Peer,
        arena: &Arena,
    ) -> Result<Option<Progress>, StorageError> {
        let peer = peer.clone();
        let arena = arena.clone();
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || inner.peer_progress(&peer, &arena)).await?
    }

    /// Update the cache by applying a notification coming from the given peer.
    pub async fn update(
        &self,
        peer: &Peer,
        notification: Notification,
    ) -> Result<(), StorageError> {
        let peer = peer.clone();
        let inner = Arc::clone(&self.inner);

        task::spawn_blocking(move || inner.update(&peer, notification)).await?
    }
}

fn do_update_last_seen_notification(
    txn: &WriteTransaction,
    peer: &Peer,
    arena: &Arena,
    index: u64,
) -> Result<(), StorageError> {
    let mut notification_table = txn.open_table(NOTIFICATION_TABLE)?;
    notification_table.insert((peer.as_str(), arena.as_str()), index)?;

    Ok(())
}

fn do_peer_progress(
    txn: &ReadTransaction,
    peer: &Peer,
    arena: &Arena,
) -> Result<Option<Progress>, StorageError> {
    let key = (peer.as_str(), arena.as_str());

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

fn do_dir_mtime(txn: &ReadTransaction, inode: u64) -> Result<UnixTime, StorageError> {
    let dir_table = txn.open_table(DIRECTORY_TABLE)?;
    match dir_table.get((inode, "."))? {
        Some(e) => {
            if let DirTableEntry::Dot(mtime) = e.value().parse()? {
                return Ok(mtime);
            }
        }
        None => {
            if inode == UnrealCacheBlocking::ROOT_DIR {
                // When the filesystem is empty, the root dir might not
                // have a mtime. This is not an error.
                return Ok(UnixTime::ZERO);
            }
        }
    }

    Err(StorageError::NotFound)
}

fn do_read_arena_map(txn: &ReadTransaction) -> Result<HashMap<Arena, u64>, StorageError> {
    let mut map = HashMap::new();
    let arena_table = txn.open_table(ARENA_TABLE)?;
    for elt in arena_table.iter()? {
        let (k, v) = elt?;
        let arena = Arena::from(k.value());
        let inode = v.value();
        map.insert(arena, inode);
    }

    Ok(map)
}

fn check_arena_compatibility(arena: &Arena, existing: &Arena) -> anyhow::Result<()> {
    fn is_path_prefix(prefix: &str, arena: &str) -> bool {
        if let Some(rest) = arena.strip_prefix(prefix) {
            rest.starts_with("/")
        } else {
            false
        }
    }
    if is_path_prefix(arena.as_str(), existing.as_str())
        || is_path_prefix(existing.as_str(), arena.as_str())
    {
        return Err(anyhow::anyhow!(
            "arena {arena} incompatible with existing arena {existing}"
        ));
    }

    Ok(())
}

fn do_add_arena(txn: &WriteTransaction, arena: &Arena) -> anyhow::Result<u64> {
    let mut arena_table = txn.open_table(ARENA_TABLE)?;
    let arena_str = arena.as_str();
    if let Some(v) = arena_table.get(arena_str)? {
        return Ok(v.value());
    }
    let mut dir_table = txn.open_table(DIRECTORY_TABLE)?;
    let arena_path = Path::parse(arena.as_str())?;
    let parent_inode = do_mkdirs(
        txn,
        &mut dir_table,
        UnrealCacheBlocking::ROOT_DIR,
        &arena_path.parent(),
        None,
    )?;
    let inode = add_dir_entry(
        txn,
        &mut dir_table,
        parent_inode,
        arena_path.name(),
        InodeAssignment::Directory,
        Some(arena),
    )?;
    arena_table.insert(arena.as_str(), inode)?;

    log::debug!("Arena root {arena}: {inode}");

    Ok(inode)
}

/// Implement [UnrealCache::unlink] within a transaction.
fn do_unlink(
    txn: &WriteTransaction,
    peer: &Peer,
    arena_root: u64,
    path: &Path,
    old_hash: Hash,
) -> Result<(), StorageError> {
    let mut dir_table = txn.open_table(DIRECTORY_TABLE)?;
    let (parent_inode, parent_assignment) = do_lookup_path(&dir_table, arena_root, &path.parent())?;
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
    do_rm_file_entry(
        &mut file_table,
        &mut dir_table,
        parent_inode,
        inode,
        peer,
        Some(old_hash),
    )?;

    Ok(())
}

/// Get a [FileEntry] for a specific peer.
fn get_file_entry(
    file_table: &redb::Table<'_, (u64, &str), Holder<FileTableEntry>>,
    inode: u64,
    peer: Option<&Peer>,
) -> Result<Option<FileTableEntry>, StorageError> {
    match file_table.get((inode, peer.map(|p| p.as_str()).unwrap_or("")))? {
        None => Ok(None),
        Some(e) => Ok(Some(e.value().parse()?)),
    }
}

fn do_file_availability(
    txn: &ReadTransaction,
    inode: u64,
) -> Result<FileAvailability, StorageError> {
    let file_table = txn.open_table(FILE_TABLE)?;

    let mut range = file_table.range((inode, "")..(inode + 1, ""))?;
    let (default_key, default_entry) = range.next().ok_or(StorageError::NotFound)??;
    if default_key.value().1 != "" {
        log::warn!("File table entry without a default peer: {inode}");
        return Err(StorageError::NotFound);
    }
    let FileTableEntry {
        metadata,
        content: FileContent {
            arena, path, hash, ..
        },
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
        arena,
        path,
        metadata,
        hash,
        peers,
    })
}

/// Write an entry in the file table, overwriting any existing one.
fn do_write_file_entry(
    file_table: &mut redb::Table<'_, (u64, &str), Holder<FileTableEntry>>,
    file_inode: u64,
    peer: Option<&Peer>,
    entry: &FileTableEntry,
) -> Result<(), StorageError> {
    let key = peer.map(|p| p.as_str()).unwrap_or("");
    log::debug!("new file entry {file_inode} {key} {}", entry.content.hash);

    file_table.insert((file_inode, key), Holder::new(entry)?)?;

    Ok(())
}

/// Retrieve or create a file entry at the given path.
///
/// Return the parent inode and the file inode.
fn do_create_file(
    txn: &WriteTransaction,
    arena_root: u64,
    arena: &Arena,
    path: &Path,
) -> Result<(u64, u64), StorageError> {
    let mut dir_table = txn.open_table(DIRECTORY_TABLE)?;
    let filename = path.name();
    let parent_inode = do_mkdirs(txn, &mut dir_table, arena_root, &path.parent(), Some(arena))?;
    let dir_entry = get_dir_entry(&dir_table, parent_inode, filename)?;
    let file_inode = match dir_entry {
        None => add_dir_entry(
            txn,
            &mut dir_table,
            parent_inode,
            filename,
            InodeAssignment::File,
            Some(arena),
        )?,
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
fn do_mkdirs(
    txn: &WriteTransaction,
    dir_table: &mut redb::Table<'_, (u64, &str), Holder<DirTableEntry>>,
    root_inode: u64,
    path: &Option<Path>,
    arena: Option<&Arena>,
) -> Result<u64, StorageError> {
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
            add_dir_entry(
                txn,
                dir_table,
                current,
                component,
                InodeAssignment::Directory,
                arena,
            )?
        };
        log::debug!("current={current}");
    }

    Ok(current)
}

/// Find the file or directory pointed to by the given path.
fn do_lookup_path(
    dir_table: &impl redb::ReadableTable<(u64, &'static str), Holder<'static, DirTableEntry>>,
    root_inode: u64,
    path: &Option<Path>,
) -> Result<(u64, InodeAssignment), StorageError> {
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
    dir_table: &impl redb::ReadableTable<(u64, &'static str), Holder<'static, DirTableEntry>>,
    parent_inode: u64,
    name: &str,
) -> Result<Option<ReadDirEntry>, StorageError> {
    match dir_table.get((parent_inode, name))? {
        None => Ok(None),
        Some(e) => Ok(Some(e.value().parse()?.into_readdir_entry(parent_inode))),
    }
}

/// Add an entry to the given directory.
fn add_dir_entry(
    txn: &WriteTransaction,
    dir_table: &mut redb::Table<'_, (u64, &str), Holder<DirTableEntry>>,
    parent_inode: u64,
    name: &str,
    assignment: InodeAssignment,
    arena: Option<&Arena>,
) -> Result<u64, StorageError> {
    let new_inode = alloc_inode(txn, arena)?;

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

    Ok(new_inode)
}

/// Allocate a new inode for an arena.
///
/// This function allocates a new inode from the arena's current range.
/// If the arena has no range or the current range is exhausted,
/// it allocates a new range.
///
/// Returns the allocated inode.
fn alloc_inode(txn: &WriteTransaction, arena: Option<&Arena>) -> Result<u64, StorageError> {
    let arena_key = arena.map(|a| a.as_str()).unwrap_or("");
    let mut current_range_table = txn.open_table(CURRENT_INODE_RANGE_TABLE)?;
    // Read the current range directly from the write transaction
    let current_range = match current_range_table.get(arena_key)? {
        Some(value) => {
            let (current, end) = value.value();
            Some((current, end))
        }
        None => None,
    };
    match current_range {
        Some((current, end)) if current < end => {
            // We have inodes available in the current range
            let inode = current + 1;
            log::debug!("Allocated inode for {arena:?}: inode");
            current_range_table.insert(arena_key, (inode, end))?;
            Ok(inode)
        }
        _ => {
            // Need to allocate a new range
            let (start, end) = alloc_inode_range(txn, arena, RANGE_SIZE)?;
            let inode = start;
            log::debug!("Allocated new range for {arena:?}: [{start}, {end}), used up {start}");
            current_range_table.insert(arena_key, (start, end))?;
            Ok(inode)
        }
    }
}

/// Allocate a new range of inodes to an arena.
///
/// This function allocates a range of `range_size` inodes to the given arena.
/// It finds the last allocated range and allocates the next available range.
///
/// Returns the allocated range as (start, end) where end is exclusive.
fn alloc_inode_range(
    txn: &WriteTransaction,
    arena: Option<&Arena>,
    range_size: u64,
) -> Result<(u64, u64), StorageError> {
    let arena_key = arena.map(|a| a.as_str()).unwrap_or("");
    let mut range_table = txn.open_table(INODE_RANGE_ALLOCATION_TABLE)?;

    let last_end = range_table.last()?.map(|(key, _)| key.value()).unwrap_or(1);

    let new_range_start = last_end + 1;
    let new_range_end = new_range_start + range_size;
    range_table.insert(new_range_end - 1, arena_key)?;

    Ok((new_range_start, new_range_end))
}

fn do_mark_peer_files(
    txn: &WriteTransaction,
    peer: &Peer,
    arena: &Arena,
) -> Result<(), StorageError> {
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
        if v.content.arena != *arena {
            continue;
        }
        let inode = k.0;
        pending_catchup_table.insert((peer_str, arena.as_str(), inode), v.parent_inode)?;
    }

    Ok(())
}

fn do_unmark_peer_file(
    txn: &WriteTransaction,
    peer: &Peer,
    arena: &Arena,
    inode: u64,
) -> Result<(), StorageError> {
    let mut pending_catchup_table = txn.open_table(PENDING_CATCHUP_TABLE)?;
    pending_catchup_table.remove((peer.as_str(), arena.as_str(), inode))?;

    Ok(())
}

fn do_delete_marked_files(
    txn: &WriteTransaction,
    peer: &Peer,
    arena: &Arena,
) -> Result<(), StorageError> {
    let mut pending_catchup_table = txn.open_table(PENDING_CATCHUP_TABLE)?;
    let mut file_table = txn.open_table(FILE_TABLE)?;
    let mut directory_table = txn.open_table(DIRECTORY_TABLE)?;
    let peer_str = peer.as_str();
    let arena_str = arena.as_str();
    for elt in pending_catchup_table.extract_from_if(
        (peer_str, arena_str, 0)..(peer_str, arena_str, u64::MAX),
        |_, _| true,
    )? {
        let elt = elt?;
        let (_, _, inode) = elt.0.value();
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
    file_table: &mut redb::Table<'_, (u64, &str), Holder<FileTableEntry>>,
    dir_table: &mut redb::Table<'_, (u64, &str), Holder<DirTableEntry>>,
    parent_inode: u64,
    inode: u64,
    peer: &Peer,
    old_hash: Option<Hash>,
) -> Result<(), StorageError> {
    let peer_str = peer.as_str();

    let mut entries = HashMap::new();
    for elt in file_table.range((inode, "")..(inode + 1, ""))? {
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

        file_table.remove((inode, ""))?;
        dir_table.retain_in((parent_inode, "")..(parent_inode + 1, ""), |_, v| {
            match v.parse() {
                Ok(DirTableEntry::Regular(v)) => v.inode != inode,
                _ => true,
            }
        })?;
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
        do_write_file_entry(file_table, inode, None, &entry)?;
    }

    Ok(())
}

/// Lookup the arena assigned to `inode`, fail if it has no assigned
/// arenas yet.
///
/// None is for global nodes, such as the root node and intermediate
/// directories created for arenas with compound names, such as
/// "documents/others". Note that inodes of arena roots are assigned
/// to their own arena.
#[allow(dead_code)] // Will be used in the next change
fn lookup_arena(txn: &redb::ReadTransaction, inode: u64) -> Result<Option<Arena>, StorageError> {
    if inode == UnrealCacheBlocking::ROOT_DIR {
        return Ok(None);
    }

    let range_table = txn.open_table(INODE_RANGE_ALLOCATION_TABLE)?;
    for entry in range_table.range(inode..)? {
        let (_, arena_str) = entry?;
        let arena_str = arena_str.value();
        if arena_str == "" {
            return Ok(None);
        } else {
            return Ok(Some(Arena::from(arena_str)));
        }
    }

    Err(StorageError::NotFound)
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_fs::TempDir;
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

        fn setup_with_arena(arena: &Arena) -> anyhow::Result<Fixture> {
            let mut fixture = Self::setup()?;
            fixture.cache.add_arena(arena)?;

            Ok(fixture)
        }

        fn parent_dir_mtime(&self, arena: &Arena, path: &Path) -> anyhow::Result<UnixTime> {
            let arena_root = self.cache.arena_root(arena).expect("arena was added");
            match path.parent() {
                None => Ok(self.cache.dir_mtime(arena_root)?),
                Some(path) => {
                    let (inode, _) = self.cache.lookup_path(arena_root, &path)?;

                    Ok(self.cache.dir_mtime(inode)?)
                }
            }
        }

        fn add_file(&self, path: &Path, size: u64, mtime: &UnixTime) -> anyhow::Result<()> {
            self.cache.update(
                &test_peer(),
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
                &test_peer(),
                Notification::Remove {
                    arena: test_arena(),
                    index: 1,
                    path: path.clone(),
                    old_hash: test_hash(),
                },
            )?;

            Ok(())
        }
    }

    /// Get the current inode range for an arena.
    ///
    /// Returns the current inode and end of range for the arena.
    /// If no range exists, returns None.
    fn get_current_inode_range(
        txn: &ReadTransaction,
        arena: &Arena,
    ) -> Result<Option<(u64, u64)>, StorageError> {
        let range_table = txn.open_table(CURRENT_INODE_RANGE_TABLE)?;

        match range_table.get(arena.as_str())? {
            Some(value) => {
                let (current, end) = value.value();
                Ok(Some((current, end)))
            }
            None => Ok(None),
        }
    }

    #[test]
    fn open_creates_tables() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(&test_arena())?;
        let cache = &fixture.cache;

        let txn = cache.db.begin_read()?;
        assert!(txn.open_table(DIRECTORY_TABLE).is_ok());
        assert!(txn.open_table(FILE_TABLE).is_ok());
        Ok(())
    }

    #[test]
    fn add_creates_directories() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(&test_arena())?;
        let cache = &fixture.cache;
        let file_path = Path::parse("a/b/c.txt")?;
        let mtime = test_time();

        fixture.add_file(&file_path, 100, &mtime)?;

        let txn = cache.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;
        let entry = dir_table
            .get((UnrealCacheBlocking::ROOT_DIR, "test_arena"))?
            .unwrap();
        let entry = match entry.value().parse()? {
            DirTableEntry::Dot(_) => panic!("Unexpected dot entry"),
            DirTableEntry::Regular(e) => e,
        };

        let entry = dir_table.get((entry.inode, "a"))?.unwrap();
        let entry = match entry.value().parse()? {
            DirTableEntry::Dot(_) => panic!("Unexpected dot entry"),
            DirTableEntry::Regular(e) => e,
        };
        assert_eq!(entry.assignment, InodeAssignment::Directory);

        let entry = dir_table.get((entry.inode, "b"))?.unwrap();
        let entry = match entry.value().parse()? {
            DirTableEntry::Dot(_) => panic!("Unexpected dot entry"),
            DirTableEntry::Regular(e) => e,
        };
        assert_eq!(entry.assignment, InodeAssignment::Directory);

        Ok(())
    }

    #[test]
    fn add_update_dir_mtime() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(&arena)?;
        let mtime = test_time();
        let path1 = Path::parse("a/b/1.txt")?;
        fixture.add_file(&path1, 100, &mtime)?;
        let dir_mtime = fixture.parent_dir_mtime(&arena, &path1)?;

        let path2 = Path::parse("a/b/2.txt")?;
        fixture.add_file(&path2, 100, &mtime)?;

        assert!(fixture.parent_dir_mtime(&arena, &path2)? > dir_mtime);
        Ok(())
    }

    #[test]
    fn replace_existing_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(&test_arena())?;
        let cache = &fixture.cache;
        let peer = test_peer();
        let file_path = Path::parse("file.txt")?;

        cache.update(
            &peer,
            Notification::Add {
                arena: test_arena(),
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 100,
                hash: test_hash(),
            },
        )?;
        cache.update(
            &peer,
            Notification::Replace {
                arena: test_arena(),
                index: 0,
                path: file_path.clone(),
                mtime: later_time(),
                size: 200,
                hash: Hash([2u8; 32]),
                old_hash: test_hash(),
            },
        )?;

        let txn = cache.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;
        let dir_entry = dir_table
            .get((UnrealCacheBlocking::ROOT_DIR, "test_arena"))?
            .unwrap();
        let dir_entry = match dir_entry.value().parse()? {
            DirTableEntry::Dot(_) => panic!("Unexpected dot entry"),
            DirTableEntry::Regular(e) => e,
        };
        let dir_entry = dir_table.get((dir_entry.inode, "file.txt"))?.unwrap();
        let dir_entry = match dir_entry.value().parse()? {
            DirTableEntry::Dot(_) => panic!("Unexpected dot entry"),
            DirTableEntry::Regular(e) => e,
        };

        let file_table = txn.open_table(FILE_TABLE)?;
        let entry = file_table
            .get((dir_entry.inode, peer.as_str()))?
            .unwrap()
            .value()
            .parse()?;
        assert_eq!(entry.metadata.size, 200);
        assert_eq!(entry.metadata.mtime, later_time());

        Ok(())
    }

    #[test]
    fn ignore_duplicate_add() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(&test_arena())?;
        let cache = &fixture.cache;
        let file_path = Path::parse("file.txt")?;

        fixture.add_file(&file_path, 100, &test_time())?;
        fixture.add_file(&file_path, 200, &test_time())?;

        let (inode, _) = cache.lookup_path(cache.arena_root(&test_arena())?, &file_path)?;
        let metadata = cache.file_metadata(inode)?;
        assert_eq!(metadata.size, 100);

        Ok(())
    }

    #[test]
    fn ignore_replace_with_wrong_hash() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(&test_arena())?;
        let cache = &fixture.cache;
        let file_path = Path::parse("file.txt")?;
        let peer = test_peer();

        cache.update(
            &peer,
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
            &peer,
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

        let (inode, _) = cache.lookup_path(cache.arena_root(&test_arena())?, &file_path)?;
        let metadata = cache.file_metadata(inode)?;
        assert_eq!(metadata.size, 100);

        Ok(())
    }

    #[test]
    fn unlink_removes_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(&test_arena())?;
        let cache = &fixture.cache;
        let file_path = Path::parse("file.txt")?;
        let mtime = test_time();

        fixture.add_file(&file_path, 100, &mtime)?;
        fixture.remove_file(&file_path)?;

        let txn = cache.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;
        assert!(
            dir_table
                .get((UnrealCacheBlocking::ROOT_DIR, "file.txt"))?
                .is_none()
        );

        Ok(())
    }

    #[test]
    fn unlink_update_dir_mtime() -> anyhow::Result<()> {
        let arena = test_arena();
        let fixture = Fixture::setup_with_arena(&arena)?;
        let file_path = Path::parse("file.txt")?;
        let mtime = test_time();

        fixture.add_file(&file_path, 100, &mtime)?;
        let dir_mtime = fixture.parent_dir_mtime(&arena, &file_path)?;
        fixture.remove_file(&file_path)?;

        assert!(fixture.parent_dir_mtime(&arena, &file_path)? > dir_mtime);

        Ok(())
    }

    #[test]
    fn unlink_ignores_wrong_old_hash() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(&test_arena())?;
        let cache = &fixture.cache;
        let file_path = Path::parse("file.txt")?;
        let mtime = test_time();

        fixture.add_file(&file_path, 100, &mtime)?;
        cache.update(
            &test_peer(),
            Notification::Remove {
                arena: test_arena(),
                index: 1,
                path: file_path.clone(),
                old_hash: Hash([2u8; 32]), // != test_hash()
            },
        )?;

        let txn = cache.db.begin_read()?;
        let dir_table = txn.open_table(DIRECTORY_TABLE)?;
        let dir_entry = dir_table
            .get((UnrealCacheBlocking::ROOT_DIR, "test_arena"))?
            .unwrap();
        let dir_entry = match dir_entry.value().parse()? {
            DirTableEntry::Dot(_) => panic!("Unexpected dot entry"),
            DirTableEntry::Regular(e) => e,
        };
        assert!(dir_table.get((dir_entry.inode, "file.txt"))?.is_some());

        Ok(())
    }

    #[test]
    fn lookup_finds_entry() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(&test_arena())?;
        let cache = &fixture.cache;
        let arena = test_arena();
        let file_path = Path::parse("a/file.txt")?;
        let mtime = test_time();

        fixture.add_file(&file_path, 100, &mtime)?;

        // Lookup directory
        let dir_entry = cache.lookup(cache.arena_root(&arena)?, "a")?;
        assert_eq!(dir_entry.assignment, InodeAssignment::Directory);

        // Lookup file
        let file_entry = cache.lookup(dir_entry.inode, "file.txt")?;
        assert_eq!(file_entry.assignment, InodeAssignment::File);

        let metadata = cache.file_metadata(file_entry.inode)?;
        assert_eq!(metadata.mtime, mtime);
        assert_eq!(metadata.size, 100);

        Ok(())
    }

    #[test]
    fn lookup_returns_notfound_for_missing_entry() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(&test_arena())?;
        let cache = &fixture.cache;

        assert!(matches!(
            cache.lookup(UnrealCacheBlocking::ROOT_DIR, "nonexistent"),
            Err(StorageError::NotFound),
        ));

        Ok(())
    }

    #[test]
    fn lookup_path_finds_entry() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(&test_arena())?;
        let cache = &fixture.cache;
        let arena = test_arena();
        let path = Path::parse("a/b/c/file.txt")?;
        let mtime = test_time();

        fixture.add_file(&path, 100, &mtime)?;

        let (inode, assignment) = cache.lookup_path(cache.arena_root(&arena)?, &path)?;
        assert_eq!(assignment, InodeAssignment::File);

        let metadata = cache.file_metadata(inode)?;
        assert_eq!(metadata.mtime, mtime);
        assert_eq!(metadata.size, 100);

        Ok(())
    }

    #[test]
    fn readdir_returns_all_entries() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(&test_arena())?;
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

        let dir_entry = cache.lookup(UnrealCacheBlocking::ROOT_DIR, arena.as_str())?;
        assert_unordered::assert_eq_unordered!(
            vec![("dir".to_string(), InodeAssignment::Directory),],
            cache
                .readdir(dir_entry.inode)?
                .into_iter()
                .map(|(name, entry)| (name, entry.assignment))
                .collect::<Vec<_>>(),
        );

        let dir_entry = cache.lookup(dir_entry.inode, "dir")?;
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
        let fixture = Fixture::setup_with_arena(&test_arena())?;
        let cache = &fixture.cache;

        let peer1 = Peer::from("peer1");
        let peer2 = Peer::from("peer2");
        let arena = test_arena();
        let file_path = Path::parse("file.txt")?;

        cache.update(
            &peer1,
            Notification::Add {
                arena: arena.clone(),
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
        )?;
        cache.update(
            &peer2,
            Notification::Add {
                arena: arena.clone(),
                index: 0,
                path: file_path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
        )?;
        cache.update(
            &peer2,
            Notification::Replace {
                arena: arena.clone(),
                index: 0,
                path: file_path.clone(),
                mtime: later_time(),
                size: 200,
                hash: Hash([2u8; 32]),
                old_hash: Hash([1u8; 32]),
            },
        )?;

        let file_entry = cache.lookup(cache.arena_root(&arena)?, "file.txt")?;
        let metadata = cache.file_metadata(file_entry.inode)?;
        assert_eq!(metadata.size, 200);
        assert_eq!(metadata.mtime, later_time());

        Ok(())
    }

    #[test]
    fn file_available_with_conflicting_adds() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(&test_arena())?;
        let cache = &fixture.cache;

        let a = Peer::from("a");
        let b = Peer::from("b");
        let c = Peer::from("c");
        let arena = test_arena();
        let path = Path::parse("file.txt")?;

        cache.update(
            &a,
            Notification::Add {
                arena: arena.clone(),
                index: 0,
                path: path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
        )?;
        cache.update(
            &b,
            Notification::Add {
                arena: arena.clone(),
                index: 0,
                path: path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
        )?;
        cache.update(
            &c,
            Notification::Add {
                arena: arena.clone(),
                index: 0,
                path: path.clone(),
                mtime: later_time(),
                size: 200,
                hash: Hash([2u8; 32]),
            },
        )?;
        let parent_inode = cache.arena_root(&arena)?;
        let inode = cache.lookup(parent_inode, "file.txt")?.inode;
        let avail = cache.file_availability(inode)?;
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
        assert_unordered::assert_eq_unordered!(vec![a.clone(), b.clone()], avail.peers);

        Ok(())
    }

    #[test]
    fn file_available_with_different_versions() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(&test_arena())?;
        let cache = &fixture.cache;

        let a = Peer::from("a");
        let b = Peer::from("b");
        let c = Peer::from("c");
        let arena = test_arena();
        let path = Path::parse("file.txt")?;

        cache.update(
            &a,
            Notification::Add {
                arena: arena.clone(),
                index: 0,
                path: path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
        )?;
        cache.update(
            &b,
            Notification::Add {
                arena: arena.clone(),
                index: 0,
                path: path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
        )?;
        cache.update(
            &c,
            Notification::Add {
                arena: arena.clone(),
                index: 0,
                path: path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
        )?;
        cache.update(
            &b,
            Notification::Replace {
                arena: arena.clone(),
                index: 0,
                path: path.clone(),
                mtime: later_time(),
                size: 200,
                hash: Hash([2u8; 32]),
                old_hash: Hash([1u8; 32]),
            },
        )?;
        let inode = cache.lookup(cache.arena_root(&arena)?, "file.txt")?.inode;
        let avail = cache.file_availability(inode)?;

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
        assert_eq!(vec![b.clone()], avail.peers);

        // We reconnect to c, which has yet another version. Following
        // the hash chain, Hash 3 is now the newest version.
        cache.update(
            &c,
            Notification::Replace {
                arena: arena.clone(),
                index: 0,
                path: path.clone(),
                mtime: later_time(),
                size: 300,
                hash: Hash([3u8; 32]),
                old_hash: Hash([1u8; 32]),
            },
        )?;
        cache.update(
            &c,
            Notification::Replace {
                arena: arena.clone(),
                index: 0,
                path: path.clone(),
                mtime: later_time(),
                size: 300,
                hash: Hash([3u8; 32]),
                old_hash: Hash([2u8; 32]),
            },
        )?;
        let avail = cache.file_availability(inode)?;
        assert_eq!(Hash([3u8; 32]), avail.hash);
        assert_eq!(vec![c.clone()], avail.peers);

        // Later on, b joins the party
        cache.update(
            &b,
            Notification::Replace {
                arena: arena.clone(),
                index: 0,
                path: path.clone(),
                mtime: later_time(),
                size: 300,
                hash: Hash([3u8; 32]),
                old_hash: Hash([3u8; 32]),
            },
        )?;

        let avail = cache.file_availability(inode)?;
        assert_eq!(Hash([3u8; 32]), avail.hash);
        assert_unordered::assert_eq_unordered!(vec![b.clone(), c.clone()], avail.peers);

        Ok(())
    }

    #[test]
    fn file_available_goes_away() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(&test_arena())?;
        let cache = &fixture.cache;

        let a = Peer::from("a");
        let b = Peer::from("b");
        let c = Peer::from("c");
        let arena = test_arena();
        let path = Path::parse("file.txt")?;

        cache.update(
            &a,
            Notification::Add {
                arena: arena.clone(),
                index: 0,
                path: path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
        )?;
        cache.update(
            &b,
            Notification::Add {
                arena: arena.clone(),
                index: 0,
                path: path.clone(),
                mtime: test_time(),
                size: 100,
                hash: Hash([1u8; 32]),
            },
        )?;
        cache.update(
            &c,
            Notification::Add {
                arena: arena.clone(),
                index: 0,
                path: path.clone(),
                mtime: later_time(),
                size: 100,
                hash: Hash([2u8; 32]),
            },
        )?;
        cache.update(
            &a,
            Notification::Replace {
                arena: arena.clone(),
                index: 0,
                path: path.clone(),
                mtime: later_time(),
                size: 200,
                hash: Hash([3u8; 32]),
                old_hash: Hash([1u8; 32]),
            },
        )?;
        let inode = cache.lookup(cache.arena_root(&arena)?, "file.txt")?.inode;
        let avail = cache.file_availability(inode)?;
        assert_eq!(Hash([3u8; 32]), avail.hash);
        assert_eq!(vec![a.clone()], avail.peers);

        cache.update(&a, Notification::CatchupStart(arena.clone()))?;
        cache.update(
            &a,
            Notification::CatchupComplete {
                arena: arena.clone(),
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
        let avail = cache.file_availability(inode)?;
        assert_eq!(Hash([2u8; 32]), avail.hash);
        assert_eq!(vec![c.clone()], avail.peers);

        Ok(())
    }

    #[test]
    fn mark_and_delete_peer_files() -> anyhow::Result<()> {
        let fixture = Fixture::setup_with_arena(&test_arena())?;
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
            &peer1,
            Notification::Add {
                arena: arena.clone(),
                index: 0,
                path: file1.clone(),
                mtime: mtime.clone(),
                size: 10,
                hash: test_hash(),
            },
        )?;

        cache.update(
            &peer1,
            Notification::Add {
                arena: arena.clone(),
                index: 0,
                path: file2.clone(),
                mtime: mtime.clone(),
                size: 10,
                hash: test_hash(),
            },
        )?;
        cache.update(
            &peer2,
            Notification::Add {
                arena: arena.clone(),
                index: 0,
                path: file2.clone(),
                mtime: mtime.clone(),
                size: 10,
                hash: test_hash(),
            },
        )?;

        cache.update(
            &peer1,
            Notification::Add {
                arena: arena.clone(),
                index: 0,
                path: file1.clone(),
                mtime: mtime.clone(),
                size: 10,
                hash: test_hash(),
            },
        )?;
        cache.update(
            &peer2,
            Notification::Add {
                arena: arena.clone(),
                index: 0,
                path: file2.clone(),
                mtime: mtime.clone(),
                size: 10,
                hash: test_hash(),
            },
        )?;
        cache.update(
            &peer3,
            Notification::Add {
                arena: arena.clone(),
                index: 0,
                path: file3.clone(),
                mtime: mtime.clone(),
                size: 10,
                hash: test_hash(),
            },
        )?;

        cache.update(
            &peer1,
            Notification::Add {
                arena: arena.clone(),
                index: 0,
                path: file4.clone(),
                mtime: mtime.clone(),
                size: 10,
                hash: test_hash(),
            },
        )?;

        let arena_root = cache.arena_root(&arena)?;
        let file1_inode = cache.lookup(arena_root, file1.name())?.inode;

        // Simulate a catchup that only reports file2 and file4.
        cache.update(&peer1, Notification::CatchupStart(arena.clone()))?;
        cache.update(
            &peer1,
            Notification::Catchup {
                arena: arena.clone(),
                path: file2.clone(),
                size: 10,
                mtime: mtime.clone(),
                hash: test_hash(),
            },
        )?;
        cache.update(
            &peer1,
            Notification::Catchup {
                arena: arena.clone(),
                path: file4.clone(),
                size: 10,
                mtime: mtime.clone(),
                hash: test_hash(),
            },
        )?;
        cache.update(
            &peer1,
            Notification::CatchupComplete {
                arena: arena.clone(),
                index: 0,
            },
        )?;

        // File1 should have been deleted, since it was only on peer1,
        assert!(matches!(
            cache.lookup(UnrealCacheBlocking::ROOT_DIR, file1.name()),
            Err(StorageError::NotFound)
        ));
        // File2 and 3 should still be available, from other peers
        let file2_inode = cache.lookup(arena_root, file2.name())?.inode;
        let file3_inode = cache.lookup(arena_root, file3.name())?.inode;

        // File4 should still be available, from peer1
        let file4_inode = cache.lookup(arena_root, file4.name())?.inode;

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

    #[test]
    fn test_alloc_inode_range() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let cache = &fixture.cache;

        // Test allocating a range
        {
            let txn = cache.db.begin_write()?;
            let (start, end) = alloc_inode_range(&txn, Some(&Arena::from("arena1")), 10000)?;
            txn.commit()?;

            assert_eq!(2, start);
            assert_eq!(10002, end);
        }

        // Test allocating a second range
        {
            let txn = cache.db.begin_write()?;
            let (start, end) = alloc_inode_range(&txn, Some(&Arena::from("arena2")), 5000)?;
            txn.commit()?;

            assert_eq!(10002, start);
            assert_eq!(15002, end);
        }

        // Verify the ranges are stored correctly
        {
            let txn = cache.db.begin_read()?;
            let range_table = txn.open_table(INODE_RANGE_ALLOCATION_TABLE)?;

            // Check that both ranges are stored
            let range = range_table.get(10001)?.expect("First range should exist");
            assert_eq!("arena1", range.value());

            let range = range_table.get(15001)?.expect("Second range should exist");
            assert_eq!("arena2", range.value());
        }

        Ok(())
    }

    #[test]
    fn test_alloc_inode() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let cache = &fixture.cache;

        let arena = test_arena();

        // First allocation should create a new range
        {
            let txn = cache.db.begin_write()?;
            let inode = alloc_inode(&txn, Some(&arena))?;
            txn.commit()?;

            // Should get the first inode from the new range
            assert_eq!(2, inode);
        }

        // Second allocation should use the same range
        {
            let txn = cache.db.begin_write()?;
            let inode = alloc_inode(&txn, Some(&arena))?;
            txn.commit()?;

            // Should get the second inode from the same range
            assert_eq!(3, inode);
        }

        // Verify the current range is updated correctly
        {
            let txn = cache.db.begin_read()?;
            let range = get_current_inode_range(&txn, &arena)?;
            assert!(range.is_some());
            let (current, end) = range.unwrap();
            // Current should be 3 (last allocated inode), end should be 10001
            assert_eq!(3, current);
            assert_eq!(10002, end);
        }

        // Allocate many more inodes to exhaust the range
        {
            let txn = cache.db.begin_write()?;
            for _ in 0..9998 {
                alloc_inode(&txn, Some(&arena))?;
            }
            txn.commit()?;
        }

        // Next allocation should create a new range
        {
            let txn = cache.db.begin_write()?;
            let inode = alloc_inode(&txn, Some(&arena))?;
            txn.commit()?;

            // Should get the first inode from the new range
            assert_eq!(inode, 10002);
        }

        Ok(())
    }

    #[test]
    fn test_multiple_arenas_inode_allocation() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup()?;
        let arena1 = Arena::from("arena1");
        let arena2 = Arena::from("arena2");

        // Add the arenas to the cache. This allocates inodes for both arenas.
        fixture.cache.add_arena(&arena1)?;
        fixture.cache.add_arena(&arena2)?;

        // Verify both arenas have their own ranges
        {
            let txn = fixture.cache.db.begin_read()?;

            let range1 = get_current_inode_range(&txn, &arena1)?;
            assert!(range1.is_some());
            let (current1, end1) = range1.unwrap();
            assert_eq!(current1, 2); // Next inode to allocate
            assert_eq!(end1, 10002);

            let range2 = get_current_inode_range(&txn, &arena2)?;
            assert!(range2.is_some());
            let (current2, end2) = range2.unwrap();
            assert_eq!(current2, 10002); // Next inode to allocate
            assert_eq!(end2, 20002);
        }

        Ok(())
    }

    #[test]
    fn test_lookup_arena() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup()?;

        let arena1 = Arena::from("arenas/arena1");
        let arena2 = Arena::from("arenas/arena2");
        fixture.cache.add_arena(&arena1)?;
        fixture.cache.add_arena(&arena2)?;

        let cache = &fixture.cache;
        cache.update(
            &test_peer(),
            Notification::Add {
                arena: arena1.clone(),
                index: 1,
                path: Path::parse("foo/bar.txt")?,
                mtime: UnixTime::from_secs(1234567890),
                size: 10,
                hash: test_hash(),
            },
        )?;

        let (arenas_inode, _) = cache.lookup_path(1, &Path::parse("arenas")?)?;
        let (arena1_inode, _) = cache.lookup_path(1, &Path::parse("arenas/arena1")?)?;
        let (arena2_inode, _) = cache.lookup_path(1, &Path::parse("arenas/arena2")?)?;
        let (foo_inode, _) = cache.lookup_path(1, &Path::parse("arenas/arena1/foo")?)?;
        let (bar_txt_inode, _) =
            cache.lookup_path(1, &Path::parse("arenas/arena1/foo/bar.txt")?)?;

        let txn = cache.db.begin_read()?;
        assert_eq!(None, lookup_arena(&txn, 1)?);
        assert_eq!(None, lookup_arena(&txn, arenas_inode)?);
        assert_eq!(Some(arena1.clone()), lookup_arena(&txn, arena1_inode)?);
        assert_eq!(Some(arena2.clone()), lookup_arena(&txn, arena2_inode)?);
        assert_eq!(Some(arena1.clone()), lookup_arena(&txn, foo_inode)?);
        assert_eq!(Some(arena1.clone()), lookup_arena(&txn, bar_txt_inode)?);

        // Unused, but allocated inodes work
        assert_eq!(Some(arena1.clone()), lookup_arena(&txn, bar_txt_inode + 1)?);
        assert_eq!(Some(arena2.clone()), lookup_arena(&txn, arena2_inode + 1)?);

        // Unallocated inodes are rejected
        assert!(matches!(
            lookup_arena(&txn, 100000),
            Err(StorageError::NotFound),
        ));

        Ok(())
    }
}
