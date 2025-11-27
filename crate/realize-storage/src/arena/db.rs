use super::blob::{BlobReadOperations, Blobs, ReadableOpenBlob, WritableOpenBlob};
use super::cache::{self, Cache, WritableOpenCache};
use super::dirty::{Dirty, DirtyReadOperations, ReadableOpenDirty, WritableOpenDirty};
use super::history::{History, HistoryReadOperations, ReadableOpenHistory, WritableOpenHistory};
use super::mark::{MarkReadOperations, ReadableOpenMark, WritableOpenMark};
use super::peer::{PeersReadOperations, ReadableOpenPeers, WritableOpenPeers};
use super::settings::{Settings, WritableOpenSettings};
use super::tree::{ReadableOpenTree, Tree, TreeReadOperations, WritableOpenTree};
use super::types::{BlobId, Layer};
use super::types::{
    BlobTableEntry, CacheTableEntry, FailedJobTableEntry, HistoryTableEntry, MarkTableEntry,
    PeerTableEntry, QueueTableEntry,
};
use crate::StorageError;
use crate::arena::types::SettingsTableEntry;
use crate::error::SanityCheck;
use crate::types::{PartialInode, PathId};
use crate::utils::fs_utils;
use crate::utils::holder::Holder;
use realize_types::{Arena, Path, PathSet};
use redb::TableDefinition;
use std::cell::RefCell;
use std::os::unix::fs::MetadataExt;
use std::panic::Location;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

/// Local file history.
///
/// Key: u64 (monotonically increasing index value)
/// Value: HistoryTableEntry
const HISTORY_TABLE: TableDefinition<u64, Holder<HistoryTableEntry>> =
    TableDefinition::new("history");

/// Database settings.
///
/// Key: ()
/// Value: Settings
const SETTINGS_TABLE: TableDefinition<(), Holder<SettingsTableEntry>> =
    TableDefinition::new("settings");

/// Tree branches and leaves, associated to pathids.
///
/// Key: (pathid, name)
/// Value: pathid
pub(crate) const TREE_TABLE: TableDefinition<(PathId, &str), PathId> = TableDefinition::new("tree");

/// Refcount for tree nodes
///
/// Key: pathid
/// Value: u32 (refcount)
pub(crate) const TREE_REFCOUNT_TABLE: TableDefinition<PathId, u32> =
    TableDefinition::new("tree_refcount");

/// Track peer files.
///
/// Each known peer file has an entry in this table, keyed with the
/// file pathid and the peer name. More than one peer might have the
/// same entry.
///
/// An pathid available in no peers should be remove from all
/// directories.
///
/// Key: (PartialPathId, Layer) (layer, pathid)
/// Value: CacheTableEntry
const CACHE_TABLE: TableDefinition<(PathId, Layer), Holder<CacheTableEntry>> =
    TableDefinition::new("cache");

/// Track peer files that might have been deleted remotely.
///
/// When a peer starts catchup of an arena, all its files are added to
/// this table. Calls to catchup for that peer and arena removes the
/// corresponding entry in the table. At the end of catchup, files
/// still in this table are deleted.
///
/// Key: (peer, file pathid)
/// Value: ()
const PENDING_CATCHUP_TABLE: TableDefinition<(&str, PathId), ()> =
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

/// Track blobs.
///
/// Key: BlodId
/// Value: BlobTableEntry
const BLOB_TABLE: TableDefinition<BlobId, Holder<BlobTableEntry>> = TableDefinition::new("blob");

/// Track the next blob ID to be allocated.
///
/// Key: () (unit key)
/// Value: PartialPathId (next ID to allocate)

/// Track LRU queue for blobs.
///
/// Key: u16 (LRU Queue ID)
/// Value: QueueTableEntry
const BLOB_LRU_QUEUE_TABLE: TableDefinition<u16, Holder<QueueTableEntry>> =
    TableDefinition::new("blob_lru_queue");

/// Track current pathid range for each arena.
///
/// The current pathid is the last pathid that was allocated for the
/// arena.
///
/// Key: ()
/// Value: (PartialPathId, PartialPathId) (last pathid allocated, end of range)
pub(crate) const PATHID_RANGE_TABLE: TableDefinition<(), (PathId, PathId)> =
    TableDefinition::new("pathid_range");

/// Mark table for storing file marks within an Arena.
///
/// Key: &str (path)
/// Value: Holder<MarkTableEntry>
const MARK_TABLE: TableDefinition<PathId, Holder<MarkTableEntry>> = TableDefinition::new("mark");

/// Path marked dirty, indexed by path.
///
/// The path can be in the index, in the cache or both.
///
/// Each entry in this table has a corresponding entry in DIRTY_LOG_TABLE.
///
/// Key: &str (path)
/// Value: dirty counter (key of DIRTY_LOG_TABLE)
const DIRTY_TABLE: TableDefinition<PathId, u64> = TableDefinition::new("dirty");

/// Path marked dirty, indexed by an increasing counter.
///
/// Key: u64 (increasing counter)
/// Value: &str (path)
const DIRTY_LOG_TABLE: TableDefinition<u64, PathId> = TableDefinition::new("dirty_log");

/// Highest counter value for DIRTY_LOG_TABLE.
///
/// Can only be cleared if DIRTY_TABLE, DIRTY_LOG_TABLE and JOB_TABLE
/// are empty and there is no active stream of Jobs.
const DIRTY_COUNTER_TABLE: TableDefinition<(), u64> = TableDefinition::new("dirty_counter");

/// Stores job failures.
///
/// Key: u64 (key of the corresponding DIRTY_LOG_TABLE entry)
/// Value: FailedJobTableEntry
const FAILED_JOB_TABLE: TableDefinition<u64, Holder<FailedJobTableEntry>> =
    TableDefinition::new("failed_job");

/// Maps partial inode value to [PartialPathId].
///
/// In most cases, pathids are converted to inodes directly. In some cases, however
/// a mapping is required which is what this table and its reverse provide.
const INODE_TO_PATHID_TABLE: TableDefinition<PartialInode, PathId> =
    TableDefinition::new("inode_to_pathid");

/// Maps [PartialPathId] to a partial inode value (that is, without
/// ipath prefix); the reverse of [INODE_TO_PATHID_TABLE];
const PATHID_TO_INODE_TABLE: TableDefinition<PathId, PartialInode> =
    TableDefinition::new("pathid_to_inode");

pub(crate) struct ArenaDatabase {
    db: redb::Database,
    uuid: Uuid,
    arena: Arena,
    subsystems: Subsystems,
    tag: Tag,

    /// Directory that contains the blobs, in "blobs" and the
    /// database, unless the database is an in-memory one.
    workdir: PathBuf,
}

struct Subsystems {
    tree: Tree,
    dirty: Dirty,
    history: History,
    blobs: Blobs,
    cache: Cache,
    settings: Settings,
}

impl ArenaDatabase {
    const WORKDIR_NAME: &str = ".realize";
    const BLOBDIR_NAME: &str = "blobs";
    const DB_NAME: &str = "arena.db";
    /// Special file used to execute write tests. It must be possible
    /// to safely write it to the data and blob directories. That file
    /// should ideally be excluded.
    const TEST_FILENAME: &str = ".realize_writetest";

    #[cfg(test)]
    pub fn for_testing(
        arena: realize_types::Arena,
        datadir: impl AsRef<std::path::Path>,
    ) -> Result<Arc<Self>, StorageError> {
        let datadir = datadir.as_ref();
        let workdir = datadir.join(Self::WORKDIR_NAME);
        std::fs::create_dir_all(&workdir)?;

        Self::new(
            crate::utils::redb_utils::in_memory()?,
            arena,
            &workdir,
            datadir,
            PathSet::from([
                Path::parse(Self::WORKDIR_NAME)?,
                Path::parse(Self::TEST_FILENAME)?,
            ]),
        )
    }

    pub(crate) fn open(
        arena: Arena,
        datadir: impl AsRef<std::path::Path>,
    ) -> Result<Arc<Self>, StorageError> {
        let datadir = datadir.as_ref();
        let workdir = datadir.join(Self::WORKDIR_NAME);
        let dbpath = workdir.join(Self::DB_NAME);

        sanity_check_dirs(arena, datadir, &workdir).fail_if_error()?;

        Self::new(
            redb::Database::create(dbpath)?,
            arena,
            workdir,
            datadir,
            PathSet::from([
                Path::parse(Self::WORKDIR_NAME)?,
                Path::parse(Self::TEST_FILENAME)?,
            ]),
        )
    }

    pub(crate) fn new(
        db: redb::Database,
        arena: Arena,
        workdir: impl AsRef<std::path::Path>,
        datadir: impl AsRef<std::path::Path>,
        exclude: PathSet,
    ) -> Result<Arc<Self>, StorageError> {
        let tree = Tree::new(arena);
        let cache: Cache;
        let dirty: Dirty;
        let history: History;
        let blobs: Blobs;
        let settings: Settings;
        let uuid: Uuid;
        let tag: Tag;
        let txn = db.begin_write()?;
        {
            // Create tables so they can safely be queried in read
            // transactions in an empty database.
            let history_table = txn.open_table(HISTORY_TABLE)?;
            let mut settings_table = txn.open_table(SETTINGS_TABLE)?;
            txn.open_table(TREE_TABLE)?;
            txn.open_table(TREE_REFCOUNT_TABLE)?;
            let mut cache_table = txn.open_table(CACHE_TABLE)?;
            txn.open_table(PENDING_CATCHUP_TABLE)?;
            txn.open_table(PEER_TABLE)?;
            txn.open_table(NOTIFICATION_TABLE)?;
            txn.open_table(PATHID_RANGE_TABLE)?;
            txn.open_table(BLOB_TABLE)?;
            let blob_lru_queue_table = txn.open_table(BLOB_LRU_QUEUE_TABLE)?;
            txn.open_table(MARK_TABLE)?;
            txn.open_table(DIRTY_TABLE)?;
            let dirty_log_table = txn.open_table(DIRTY_LOG_TABLE)?;
            txn.open_table(DIRTY_COUNTER_TABLE)?;
            txn.open_table(FAILED_JOB_TABLE)?;
            txn.open_table(PATHID_TO_INODE_TABLE)?;
            txn.open_table(INODE_TO_PATHID_TABLE)?;

            dirty = Dirty::setup(&dirty_log_table)?;
            history = History::setup(&history_table)?;
            settings = Settings::setup(&mut settings_table)?;
            uuid = settings.borrow().uuid;
            tag = Tag::new(uuid, arena);
            blobs = Blobs::setup(
                &workdir.as_ref().join(ArenaDatabase::BLOBDIR_NAME),
                &blob_lru_queue_table,
            )?;
            cache = Cache::setup(&mut cache_table, tree.root(), datadir.as_ref(), exclude)?;
        }
        txn.commit()?;

        Ok(Arc::new(Self {
            db,
            arena,
            uuid,
            workdir: workdir.as_ref().to_path_buf(),
            subsystems: Subsystems {
                tree,
                dirty,
                history,
                blobs,
                cache,
                settings,
            },
            tag,
        }))
    }

    pub fn uuid(&self) -> &Uuid {
        &self.uuid
    }

    #[allow(dead_code)]
    pub fn arena(&self) -> Arena {
        self.arena
    }

    pub fn tag(&self) -> Tag {
        self.tag
    }

    /// Return handle on the Settings subsystem.
    #[allow(dead_code)]
    pub fn settings(&self) -> &Settings {
        &self.subsystems.settings
    }

    /// Return handle on the Tree subsystem.
    #[allow(dead_code)]
    pub fn tree(&self) -> &Tree {
        &self.subsystems.tree
    }

    /// Return handle on the Dirty subsystem.
    #[allow(dead_code)]
    pub fn dirty(&self) -> &Dirty {
        &self.subsystems.dirty
    }

    /// Return handle on the History subsystem.
    #[allow(dead_code)]
    pub fn history(&self) -> &History {
        &self.subsystems.history
    }

    /// Return handle on the Blobs subsystem.
    pub fn blobs(&self) -> &Blobs {
        &self.subsystems.blobs
    }

    /// Return handle on the Cache subsystem.
    pub fn cache(&self) -> &Cache {
        &self.subsystems.cache
    }

    /// Return the directory that stores the data files.
    pub(crate) fn datadir(&self) -> &std::path::Path {
        self.subsystems.cache.datadir()
    }

    /// Return the directory that stores the database and blobs.
    pub(crate) fn workdir(&self) -> &std::path::Path {
        &self.workdir
    }

    pub fn begin_write(&self) -> Result<ArenaWriteTransaction<'_>, StorageError> {
        Ok(ArenaWriteTransaction {
            inner: self.db.begin_write()?,
            tag: self.tag,
            subsystems: &self.subsystems,
            before_commit: BeforeCommit::new(),
            after_commit: AfterCommit::new(),
        })
    }

    pub fn begin_read(&self) -> Result<ArenaReadTransaction<'_>, StorageError> {
        Ok(ArenaReadTransaction {
            inner: self.db.begin_read()?,
            subsystems: &self.subsystems,
        })
    }

    /// Check the database and directory setup and reports the result
    /// as a series of log entries.
    #[allow(dead_code)] // for later
    pub fn sanity_checks(&self) -> SanityCheckResult {
        sanity_check_dirs(
            self.arena,
            self.subsystems.cache.datadir(),
            self.subsystems.blobs.blob_dir(),
        )
    }
}

/// Result of [ArenaDatabase::sanity_checks] and [sanity_check_dirs].
pub struct SanityCheckResult {
    arena: Arena,
    issues: Vec<(log::Level, StorageError)>,
}

impl SanityCheckResult {
    pub fn new(arena: Arena) -> Self {
        Self {
            arena,
            issues: vec![],
        }
    }

    /// Build sanity check results using a callback , capturing any
    /// error returned by the callback.
    fn build(
        arena: Arena,
        cb: impl FnOnce(&mut SanityCheckResult) -> Result<(), StorageError>,
    ) -> Self {
        let mut result = SanityCheckResult::new(arena);
        if let Err(err) = (cb)(&mut result) {
            result.issues.push((log::Level::Error, err));
        }

        result
    }

    /// Register a new issue at the given level.
    pub fn add(&mut self, level: log::Level, path: &std::path::Path, check: SanityCheck) {
        self.issues.push((
            level,
            StorageError::SanityCheckFailed(self.arena, path.to_path_buf(), check),
        ));
    }

    /// Register an error.
    pub fn err(&mut self, path: &std::path::Path, check: SanityCheck) {
        self.add(log::Level::Error, path, check)
    }

    /// Register a warning
    pub fn warn(&mut self, path: &std::path::Path, check: SanityCheck) {
        self.add(log::Level::Warn, path, check)
    }

    /// Check whether there are messages to report
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.issues.is_empty()
    }

    /// Check whether there are errors
    #[allow(dead_code)]
    pub fn has_errors(&self) -> bool {
        self.issues.iter().any(|(l, _)| *l <= log::Level::Error)
    }

    /// Log all messages
    pub fn log_all(&self) {
        for (level, err) in &self.issues {
            log::log!(*level, "[{}] {err}", self.arena);
        }
    }

    /// Fail if the result has an error, log everything else.
    pub fn fail_if_error(self) -> Result<(), StorageError> {
        self.log_all();
        if let Some((_, err)) = self
            .issues
            .into_iter()
            .find(|(level, _)| *level <= log::Level::Error)
        {
            return Err(err);
        }

        Ok(())
    }
}

/// Perform sanity checks on datadir and report the result.
fn sanity_check_dirs(
    arena: Arena,
    datadir: &std::path::Path,
    workdir: &std::path::Path,
) -> SanityCheckResult {
    SanityCheckResult::build(arena, |result| {
        if !datadir.exists() {
            result.err(datadir, SanityCheck::Exists);
            return Ok(());
        }
        if !fs_utils::is_readable_dir(datadir) {
            result.err(datadir, SanityCheck::ReadableDir);
            return Ok(());
        }
        if !fs_utils::is_writable_dir(datadir, ArenaDatabase::TEST_FILENAME) {
            result.warn(datadir, SanityCheck::WritableDir);
        }

        let blobdir = workdir.join(ArenaDatabase::BLOBDIR_NAME);
        let _ = std::fs::create_dir_all(&blobdir);
        if !fs_utils::is_readable_dir(&blobdir) {
            result.err(&blobdir, SanityCheck::ReadableDir);
            return Ok(());
        }

        if !fs_utils::is_writable_dir(&blobdir, ArenaDatabase::TEST_FILENAME) {
            result.err(&blobdir, SanityCheck::WritableDir);
        }

        if let (Ok(m1), Ok(m2)) = (datadir.metadata(), blobdir.metadata())
            && m1.dev() != m2.dev()
        {
            result.warn(&blobdir, SanityCheck::SameDevice);
        }
        Ok(())
    })
}

/// A short tag that represents the database for logging.
#[derive(Clone, Copy)]
pub struct Tag {
    inner: internment::Intern<String>,
}
impl Tag {
    fn new(uuid: Uuid, arena: Arena) -> Self {
        let bytes = uuid.as_bytes();

        Self {
            inner: internment::Intern::new(format!("{:02x?}{:02x?}/{arena}", bytes[14], bytes[15])),
        }
    }
}
impl std::fmt::Display for Tag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.inner)
    }
}

pub struct ArenaWriteTransaction<'db> {
    inner: redb::WriteTransaction,
    tag: Tag,
    subsystems: &'db Subsystems,

    /// Callbacks to be run after the transaction is committed.
    ///
    /// Using a RefCell to avoid issues, as transactions are pretty
    /// much always borrowed immutably, with the tables it would be
    /// impractical to have pass around mutable references to
    /// transactions.
    after_commit: AfterCommit,

    /// Callbacks to be run before the transaction is committed.
    ///
    /// These callbacks can interrupt the commit by returning an
    /// error.
    before_commit: BeforeCommit,
}

impl<'db> ArenaWriteTransaction<'db> {
    /// Commit the changes.
    ///
    /// If the transaction is successfully committed, functions
    /// registered by after_commit are run, and these may fail.
    pub fn commit(self) -> Result<(), StorageError> {
        self.before_commit.run_all(&self)?;
        self.inner.commit()?;
        self.after_commit.run_all();
        Ok(())
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn read_cache(&self) -> Result<impl cache::CacheReadOperations, StorageError> {
        Ok(cache::ReadableOpenCache::new(
            self.inner
                .open_table(CACHE_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.inner.open_table(PATHID_TO_INODE_TABLE)?,
            self.inner.open_table(INODE_TO_PATHID_TABLE)?,
            &self.subsystems.cache,
        ))
    }

    #[track_caller]
    pub(crate) fn write_cache(&self) -> Result<cache::WritableOpenCache<'_>, StorageError> {
        Ok(WritableOpenCache::new(
            self.tag,
            self.inner
                .open_table(CACHE_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.inner.open_table(PATHID_TO_INODE_TABLE)?,
            self.inner.open_table(INODE_TO_PATHID_TABLE)?,
            self.inner.open_table(PENDING_CATCHUP_TABLE)?,
            &self.subsystems.cache,
        ))
    }

    #[track_caller]
    #[allow(dead_code)]
    pub(crate) fn write_settings(&self) -> Result<WritableOpenSettings<'_>, StorageError> {
        Ok(WritableOpenSettings::new(
            self.inner
                .open_table(SETTINGS_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            &self.subsystems.settings,
            &self.after_commit,
        ))
    }

    #[track_caller]
    #[allow(dead_code)]
    pub(crate) fn read_peers(&self) -> Result<impl PeersReadOperations, StorageError> {
        Ok(ReadableOpenPeers::new(
            self.inner
                .open_table(PEER_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.inner.open_table(NOTIFICATION_TABLE)?,
        ))
    }

    #[track_caller]
    pub(crate) fn write_peers(&self) -> Result<WritableOpenPeers<'_>, StorageError> {
        Ok(WritableOpenPeers::new(
            self.inner
                .open_table(PEER_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.inner.open_table(NOTIFICATION_TABLE)?,
        ))
    }

    pub fn after_commit(&self, cb: impl FnOnce() -> () + Send + 'static) {
        self.after_commit.add(cb)
    }

    #[track_caller]
    pub(crate) fn read_tree(&self) -> Result<impl TreeReadOperations, StorageError> {
        Ok(ReadableOpenTree::new(
            self.inner
                .open_table(TREE_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            &self.subsystems.tree,
        ))
    }

    #[track_caller]
    pub(crate) fn write_tree(&self) -> Result<WritableOpenTree<'_>, StorageError> {
        Ok(WritableOpenTree::new(
            self.tag,
            &self.before_commit,
            self.inner
                .open_table(TREE_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.inner.open_table(TREE_REFCOUNT_TABLE)?,
            self.inner.open_table(PATHID_RANGE_TABLE)?,
            &self.subsystems.tree,
        ))
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn read_dirty(&self) -> Result<impl DirtyReadOperations, StorageError> {
        Ok(ReadableOpenDirty::new(
            self.inner
                .open_table(DIRTY_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.inner.open_table(DIRTY_LOG_TABLE)?,
            self.inner.open_table(FAILED_JOB_TABLE)?,
        ))
    }

    #[track_caller]
    pub(crate) fn write_dirty(&self) -> Result<WritableOpenDirty<'_>, StorageError> {
        Ok(WritableOpenDirty::new(
            self.tag,
            &self.after_commit,
            &self.subsystems.dirty,
            self.inner
                .open_table(DIRTY_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.inner.open_table(DIRTY_LOG_TABLE)?,
            self.inner.open_table(FAILED_JOB_TABLE)?,
            self.inner.open_table(DIRTY_COUNTER_TABLE)?,
        ))
    }
    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn read_history(&self) -> Result<impl HistoryReadOperations, StorageError> {
        Ok(ReadableOpenHistory::new(
            self.inner
                .open_table(HISTORY_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
        ))
    }

    #[track_caller]
    pub(crate) fn write_history(&self) -> Result<WritableOpenHistory<'_>, StorageError> {
        Ok(WritableOpenHistory::new(
            self.tag,
            &self.after_commit,
            &self.subsystems.history,
            self.inner
                .open_table(HISTORY_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
        ))
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn read_marks(&self) -> Result<impl MarkReadOperations, StorageError> {
        Ok(ReadableOpenMark::new(
            self.inner
                .open_table(MARK_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
        ))
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn write_marks(&self) -> Result<WritableOpenMark<'_>, StorageError> {
        Ok(WritableOpenMark::new(
            self.inner
                .open_table(MARK_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
        ))
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn read_blobs(&self) -> Result<impl BlobReadOperations, StorageError> {
        Ok(ReadableOpenBlob::new(
            self.inner
                .open_table(BLOB_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.inner.open_table(BLOB_LRU_QUEUE_TABLE)?,
        ))
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn write_blobs(&self) -> Result<WritableOpenBlob<'_>, StorageError> {
        Ok(WritableOpenBlob::new(
            self.tag,
            &self.subsystems.blobs,
            &self.before_commit,
            self.inner
                .open_table(BLOB_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.inner.open_table(BLOB_LRU_QUEUE_TABLE)?,
            &self.subsystems.blobs,
        ))
    }
}

pub struct ArenaReadTransaction<'db> {
    inner: redb::ReadTransaction,
    subsystems: &'db Subsystems,
}

impl<'db> ArenaReadTransaction<'db> {
    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn read_tree(&self) -> Result<impl TreeReadOperations, StorageError> {
        Ok(ReadableOpenTree::new(
            self.inner
                .open_table(TREE_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            &self.subsystems.tree,
        ))
    }

    #[track_caller]
    pub(crate) fn read_dirty(&self) -> Result<impl DirtyReadOperations, StorageError> {
        Ok(ReadableOpenDirty::new(
            self.inner
                .open_table(DIRTY_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.inner.open_table(DIRTY_LOG_TABLE)?,
            self.inner.open_table(FAILED_JOB_TABLE)?,
        ))
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn read_history(&self) -> Result<impl HistoryReadOperations, StorageError> {
        Ok(ReadableOpenHistory::new(
            self.inner
                .open_table(HISTORY_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
        ))
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn read_marks(&self) -> Result<impl MarkReadOperations, StorageError> {
        Ok(ReadableOpenMark::new(
            self.inner
                .open_table(MARK_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
        ))
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn read_blobs(&self) -> Result<impl BlobReadOperations, StorageError> {
        Ok(ReadableOpenBlob::new(
            self.inner
                .open_table(BLOB_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.inner.open_table(BLOB_LRU_QUEUE_TABLE)?,
        ))
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn read_cache(&self) -> Result<impl cache::CacheReadOperations, StorageError> {
        Ok(cache::ReadableOpenCache::new(
            self.inner
                .open_table(CACHE_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.inner.open_table(PATHID_TO_INODE_TABLE)?,
            self.inner.open_table(INODE_TO_PATHID_TABLE)?,
            &self.subsystems.cache,
        ))
    }

    #[allow(dead_code)]
    #[track_caller]
    pub(crate) fn read_peers(&self) -> Result<impl PeersReadOperations, StorageError> {
        Ok(ReadableOpenPeers::new(
            self.inner
                .open_table(PEER_TABLE)
                .map_err(|e| StorageError::open_table(e, Location::caller()))?,
            self.inner.open_table(NOTIFICATION_TABLE)?,
        ))
    }
}

/// Callbacks that run after a transaction is committed.
///
/// These callbacks cannot fail and are run after the transaction has been
/// successfully committed to the database.
pub(crate) struct AfterCommit {
    inner: RefCell<Vec<Box<dyn FnOnce() -> () + Send + 'static>>>,
}

impl AfterCommit {
    /// Create a new empty after-commit callback collection.
    pub(crate) fn new() -> Self {
        Self {
            inner: RefCell::new(vec![]),
        }
    }

    /// Add a callback to be run after the transaction is committed.
    pub(crate) fn add(&self, cb: impl FnOnce() -> () + Send + 'static) {
        self.inner.borrow_mut().push(Box::new(cb));
    }

    /// Run all registered callbacks.
    ///
    /// This consumes the callbacks, so they can only be run once.
    pub(crate) fn run_all(self) {
        for cb in self.inner.into_inner() {
            cb();
        }
    }
}

/// Callbacks that run before a transaction is committed.
///
/// These callbacks can interrupt the commit by returning an error.
pub(crate) struct BeforeCommit {
    inner: RefCell<
        Vec<Box<dyn FnOnce(&ArenaWriteTransaction) -> Result<(), StorageError> + Send + 'static>>,
    >,
}

impl BeforeCommit {
    /// Create a new empty before-commit callback collection.
    pub(crate) fn new() -> Self {
        Self {
            inner: RefCell::new(vec![]),
        }
    }

    /// Add a callback to be run before the transaction is committed.
    pub(crate) fn add(
        &self,
        cb: impl FnOnce(&ArenaWriteTransaction) -> Result<(), StorageError> + Send + 'static,
    ) {
        self.inner.borrow_mut().push(Box::new(cb));
    }

    /// Run all registered callbacks in order.
    ///
    /// Returns an error if any callback fails, which will interrupt the commit.
    pub(crate) fn run_all(&self, txn: &ArenaWriteTransaction) -> Result<(), StorageError> {
        while let cbs = self.inner.take()
            && !cbs.is_empty()
        {
            for cb in cbs {
                cb(txn)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;
    use assert_fs::{
        TempDir,
        prelude::{FileWriteStr, PathChild, PathCreateDir},
    };
    use realize_types::Arena;
    use redb::{ReadOnlyTable, ReadableTable};

    const TEST_TABLE: TableDefinition<&str, &str> = TableDefinition::new("test");

    struct Fixture {
        db: Arc<ArenaDatabase>,
        _tempdir: TempDir,
    }

    impl Fixture {
        fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();
            let tempdir = TempDir::new()?;
            let db = ArenaDatabase::for_testing(Arena::from("myarena"), tempdir.path())?;

            Ok(Self {
                db,
                _tempdir: tempdir,
            })
        }
    }

    #[test]
    fn create_databases() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        // Make sure the tables can be opened in a read transaction.
        let txn = fixture.db.begin_read()?;
        txn.read_tree()?;
        txn.read_blobs()?;
        txn.read_marks()?;
        txn.read_dirty()?;
        txn.read_history()?;

        Ok(())
    }

    fn test_table_content(
        test_table: ReadOnlyTable<&str, &str>,
    ) -> Result<Vec<(String, String)>, anyhow::Error> {
        let result = test_table
            .iter()?
            .map(|r| r.and_then(|(k, v)| Ok((k.value().to_string(), v.value().to_string()))))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(result)
    }

    #[test]
    fn before_commit() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let txn = fixture.db.begin_write()?;
        {
            let mut test_table = txn.inner.open_table(TEST_TABLE)?;
            txn.before_commit.add(|txn| {
                let mut test_table = txn.inner.open_table(TEST_TABLE)?;
                test_table.insert("2", "before_commit")?;
                Ok(())
            });
            test_table.insert("1", "normal")?;
        }
        txn.commit()?;

        let txn = fixture.db.begin_read()?;
        let test_table = txn.inner.open_table(TEST_TABLE)?;
        let result = test_table_content(test_table)?;
        assert_eq!(
            vec![
                ("1".to_string(), "normal".to_string()),
                ("2".to_string(), "before_commit".to_string())
            ],
            result
        );

        Ok(())
    }

    #[test]
    fn before_commit_registers_another() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let txn = fixture.db.begin_write()?;
        {
            let mut test_table = txn.inner.open_table(TEST_TABLE)?;
            txn.before_commit.add(|txn| {
                let mut test_table = txn.inner.open_table(TEST_TABLE)?;
                test_table.insert("2", "before_commit 1")?;

                txn.before_commit.add(|txn| {
                    let mut test_table = txn.inner.open_table(TEST_TABLE)?;
                    test_table.insert("3", "before_commit 2")?;

                    Ok(())
                });
                Ok(())
            });
            test_table.insert("1", "normal")?;
        }
        txn.commit()?;

        let txn = fixture.db.begin_read()?;
        let test_table = txn.inner.open_table(TEST_TABLE)?;
        let result = test_table_content(test_table)?;
        assert_eq!(
            vec![
                ("1".to_string(), "normal".to_string()),
                ("2".to_string(), "before_commit 1".to_string()),
                ("3".to_string(), "before_commit 2".to_string())
            ],
            result
        );

        Ok(())
    }

    #[test]
    fn reopen_keeps_uuid() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        let tempdir = TempDir::new()?;
        let dbpath = tempdir.join("myarena.db");
        let blob_dir = tempdir.join("blobs");
        let datadir = tempdir.join("data");
        let arena = Arena::from("myarena");
        let db = ArenaDatabase::new(
            redb::Database::create(&dbpath)?,
            arena,
            &blob_dir,
            &datadir,
            PathSet::new(),
        )?;
        let uuid = db.uuid().clone();
        assert!(!uuid.is_nil());
        assert_eq!(db.settings().borrow().uuid, *db.uuid());

        drop(db);

        let db = ArenaDatabase::new(
            redb::Database::create(&dbpath)?,
            arena,
            &blob_dir,
            &datadir,
            PathSet::new(),
        )?;
        assert_eq!(uuid, *db.uuid());
        assert_eq!(db.settings().borrow().uuid, *db.uuid());

        Ok(())
    }

    #[test]
    fn check_create_workdir() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        let tempdir = TempDir::new()?;

        let datadir = tempdir.child("datadir");
        datadir.create_dir_all()?;
        let blobdir = tempdir.child(".realize/blobs");

        let arena = Arena::from("myarena");
        let result = super::sanity_check_dirs(arena, datadir.path(), blobdir.path());
        result.log_all();
        assert!(result.is_empty());
        assert!(blobdir.exists());

        Ok(())
    }

    #[test]
    fn check_rejects_missing_datadir() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        let tempdir = TempDir::new()?;

        let datadir = tempdir.child("datadir");
        let blobdir = tempdir.child(".realize/blobs");
        blobdir.create_dir_all()?;

        let result =
            super::sanity_check_dirs(Arena::from("myarena"), datadir.path(), blobdir.path());
        result.log_all();
        assert!(result.has_errors());

        Ok(())
    }

    #[test]
    fn check_accepts_all() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        let tempdir = TempDir::new()?;

        let datadir = tempdir.child("datadir");
        datadir.create_dir_all()?;
        let blobdir = tempdir.child(".realize/blobs");
        blobdir.create_dir_all()?;

        let result =
            super::sanity_check_dirs(Arena::from("myarena"), datadir.path(), blobdir.path());
        result.log_all();
        assert!(result.is_empty());

        Ok(())
    }

    #[test]
    fn check_rejects_nondirectory_datadir() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        let tempdir = TempDir::new()?;

        let datadir = tempdir.child("datadir");
        let blobdir = tempdir.child(".realize/blobs");

        // Create a file instead of directory
        datadir.write_str("not a directory")?;
        blobdir.create_dir_all()?;

        let result =
            super::sanity_check_dirs(Arena::from("myarena"), datadir.path(), blobdir.path());
        result.log_all();
        assert!(result.has_errors());

        Ok(())
    }

    #[test]
    fn check_rejects_datadir_not_accessible() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        let tempdir = TempDir::new()?;

        let datadir = tempdir.child("datadir");
        let blobdir = tempdir.child(".realize/blobs");

        datadir.create_dir_all()?;
        blobdir.create_dir_all()?;

        let mut perms = datadir.path().metadata()?.permissions();
        perms.set_mode(0o000); // No permissions
        std::fs::set_permissions(datadir.path(), perms)?;

        let result =
            super::sanity_check_dirs(Arena::from("myarena"), datadir.path(), blobdir.path());
        result.log_all();
        assert!(result.has_errors());

        Ok(())
    }

    #[test]
    fn check_warns_datadir_not_writable() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        let tempdir = TempDir::new()?;

        let datadir = tempdir.child("datadir");
        let blobdir = tempdir.child(".realize/blobs");

        datadir.create_dir_all()?;
        blobdir.create_dir_all()?;

        let mut perms = datadir.path().metadata()?.permissions();
        perms.set_mode(0o444); // Read-only
        std::fs::set_permissions(datadir.path(), perms)?;

        let result =
            super::sanity_check_dirs(Arena::from("myarena"), datadir.path(), blobdir.path());
        result.log_all();
        assert!(!result.is_empty());
        assert!(!result.has_errors());

        Ok(())
    }

    #[test]
    fn check_rejects_blobdir_not_writable() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        let tempdir = TempDir::new()?;

        let datadir = tempdir.child("datadir");
        datadir.create_dir_all()?;
        let blobdir = tempdir.child(".realize/blobs");
        blobdir.create_dir_all()?;

        let mut perms = blobdir.path().metadata()?.permissions();
        perms.set_mode(0o444); // Read-only
        std::fs::set_permissions(blobdir.path(), perms)?;

        let result =
            super::sanity_check_dirs(Arena::from("myarena"), datadir.path(), blobdir.path());
        result.log_all();
        assert!(result.has_errors());

        Ok(())
    }
}
