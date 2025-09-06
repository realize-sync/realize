use std::sync::Arc;

use bimap::BiMap;
use realize_types::Arena;
use redb::ReadableTable;

use crate::{GlobalDatabase, PathId, StorageError};

use super::db::{GlobalReadTransaction, GlobalWriteTransaction};

/// Allocate pathid ranges and assign them to arenas.
pub(crate) struct PathIdAllocator {
    db: Arc<GlobalDatabase>,
    arena_roots: BiMap<Arena, PathId>,
}

impl PathIdAllocator {
    pub(crate) const ROOT_INODE: PathId = PathId(1);

    /// Create a new allocator, backed by the given global database.
    ///
    /// An arena root is allocated for all arenas in `arenas` and
    /// stored in the database for next time..
    pub(crate) fn new<T>(db: Arc<GlobalDatabase>, arenas: T) -> Result<Arc<Self>, StorageError>
    where
        T: IntoIterator<Item = Arena>,
    {
        let mut this = Self {
            db,
            arena_roots: BiMap::new(),
        };
        for arena in arenas.into_iter() {
            this.add_arena(arena)?;
        }

        Ok(Arc::new(this))
    }

    /// Return the root pathid of the given arena.
    ///
    /// The arena must have been added to this allocator.
    pub(crate) fn arena_root(&self, arena: Arena) -> Option<PathId> {
        self.arena_roots.get_by_left(&arena).copied()
    }

    /// Check whether [PathId] is an arena root.
    pub(crate) fn is_arena_root(&self, pathid: PathId) -> bool {
        self.arena_roots.get_by_right(&pathid).is_some()
    }

    /// Allocate an pathid for an arena.
    ///
    /// `current_range_table` must be an opened table within the arena database.
    pub(crate) fn allocate_arena_pathid(
        &self,
        current_range_table: &mut redb::Table<'_, (), (PathId, PathId)>,
        arena: Arena,
    ) -> Result<PathId, StorageError> {
        self.allocate_pathid(current_range_table, || {
            self.allocate_pathid_range(arena, 10000)
        })
    }

    /// Allocate a global pathid.
    pub(crate) fn allocate_global_pathid(
        &self,
        txn: &GlobalWriteTransaction,
    ) -> Result<PathId, StorageError> {
        self.allocate_pathid(&mut txn.current_pathid_range_table()?, || {
            do_alloc_pathid_range(&txn, Self::ROOT_INODE, 100)
        })
    }

    /// Maps pathids to arenas.
    ///
    /// The root pathid of an arena is mapped to the arena, even though
    /// these pathids are allocated from the global range.
    pub(crate) fn arena_for_pathid(
        &self,
        txn: &GlobalReadTransaction,
        pathid: PathId,
    ) -> Result<Option<Arena>, StorageError> {
        if pathid == Self::ROOT_INODE {
            return Ok(None);
        }
        if let Some(arena) = self.arena_roots.get_by_right(&pathid) {
            return Ok(Some(*arena));
        }

        let range_table = txn.pathid_range_allocation_table()?;
        for entry in range_table.range(pathid..)? {
            let (_, root) = entry?;
            let root = root.value();
            if root == Self::ROOT_INODE {
                return Ok(None);
            } else if let Some(arena) = self.arena_roots.get_by_right(&root) {
                return Ok(Some(*arena));
            }
        }

        Err(StorageError::NotFound)
    }

    /// Retrieve or allocate the arena root for the given arena.
    fn add_arena(&mut self, arena: Arena) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut arena_table = txn.arena_table()?;
            let pathid = if let Some(v) = arena_table.get(arena.as_str())? {
                v.value()
            } else {
                let pathid = self.allocate_global_pathid(&txn)?;
                arena_table.insert(arena.as_str(), pathid)?;

                pathid
            };
            self.arena_roots.insert(arena, pathid);
            log::debug!("[{arena}]: Root pathid {pathid}");
        }
        txn.commit()?;
        Ok(())
    }

    /// Allocate an pathid, using the given table and range allocation
    /// function.
    fn allocate_pathid(
        &self,
        current_range_table: &mut redb::Table<'_, (), (PathId, PathId)>,
        alloc_pathid_range: impl FnOnce() -> Result<(PathId, PathId), StorageError>,
    ) -> Result<PathId, StorageError> {
        let current_range = match current_range_table.get(())? {
            Some(value) => {
                let (current, end) = value.value();
                Some((current, end))
            }
            None => None,
        };
        match current_range {
            Some((current, end)) if (current.plus(1)) < end => {
                // We have pathids available in the current range
                let pathid = current.plus(1);
                current_range_table.insert((), (pathid, end))?;
                Ok(pathid)
            }
            _ => {
                // Need to allocate a new range
                let (start, end) = alloc_pathid_range()?;
                let pathid = start;
                current_range_table.insert((), (start, end))?;
                Ok(pathid)
            }
        }
    }

    fn allocate_pathid_range(
        &self,
        arena: Arena,
        range_size: u64,
    ) -> Result<(PathId, PathId), StorageError> {
        let arena_root = self
            .arena_root(arena)
            .ok_or_else(|| StorageError::UnknownArena(arena))?;
        let txn = self.db.begin_write()?;
        let ret = do_alloc_pathid_range(&txn, arena_root, range_size)?;

        log::debug!("[{arena}] Allocated pathid range: {ret:?}");
        // If the transaction inside the arena cache fails to commit,
        // the range is lost. TODO: find a way of avoiding such
        // issues.
        txn.commit()?;

        Ok(ret)
    }
}

fn do_alloc_pathid_range(
    txn: &GlobalWriteTransaction,
    assigned_root: PathId,
    range_size: u64,
) -> Result<(PathId, PathId), StorageError> {
    let mut range_table = txn.pathid_range_allocation_table()?;

    let last_end = range_table
        .last()?
        .map(|(key, _)| key.value())
        .unwrap_or(PathId(1));

    let new_range_start = last_end.plus(1);
    let new_range_end = new_range_start.plus(range_size);
    range_table.insert(new_range_end.minus(1), assigned_root)?;

    Ok((new_range_start, new_range_end))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::utils::redb_utils;

    use super::*;

    struct Fixture {
        db: Arc<GlobalDatabase>,
        allocator: Arc<PathIdAllocator>,
        arena_dbs: HashMap<Arena, Arc<GlobalDatabase>>,
    }

    impl Fixture {
        fn setup<T>(arenas: T) -> anyhow::Result<Self>
        where
            T: IntoIterator<Item = Arena>,
        {
            let _ = env_logger::try_init();
            let db = GlobalDatabase::new(redb_utils::in_memory()?)?;
            let arenas = arenas.into_iter().collect::<Vec<_>>();
            let allocator = PathIdAllocator::new(Arc::clone(&db), arenas.clone())?;

            let mut arena_dbs = HashMap::new();
            for arena in arenas {
                arena_dbs.insert(arena, GlobalDatabase::new(redb_utils::in_memory()?)?);
            }

            Ok(Self {
                allocator,
                db,
                arena_dbs,
            })
        }

        fn arena_db(&self, arena: Arena) -> &Arc<GlobalDatabase> {
            self.arena_dbs.get(&arena).unwrap()
        }

        fn allocate_arena_pathid(&self, arena: Arena) -> Result<PathId, StorageError> {
            let txn = self.arena_db(arena).begin_write()?;
            let pathid = self
                .allocator
                .allocate_arena_pathid(&mut txn.current_pathid_range_table()?, arena)?;
            txn.commit()?;

            Ok(pathid)
        }

        fn allocate_global_pathid(&self) -> Result<PathId, StorageError> {
            let txn = self.db.begin_write()?;
            let pathid = self.allocator.allocate_global_pathid(&txn)?;
            txn.commit()?;

            Ok(pathid)
        }

        fn arena_for_pathid(&self, pathid: PathId) -> Result<Option<Arena>, StorageError> {
            let txn = self.db.begin_read()?;
            self.allocator.arena_for_pathid(&txn, pathid)
        }
    }

    #[test]
    fn assign_arena_roots() -> anyhow::Result<()> {
        let a = Arena::from("a");
        let b = Arena::from("b");
        let fixture = Fixture::setup([a, b])?;

        // Allocation starts at 2, since 1 is the root pathid
        assert_eq!(Some(PathId(2)), fixture.allocator.arena_root(a));
        assert_eq!(Some(PathId(3)), fixture.allocator.arena_root(b));
        assert!(
            fixture
                .allocator
                .arena_root(Arena::from("notadded"))
                .is_none()
        );

        Ok(())
    }

    #[test]
    fn test_allocate_global_pathid() -> anyhow::Result<()> {
        let fixture = Fixture::setup([])?;
        let txn = fixture.db.begin_write()?;

        let pathid1 = fixture.allocator.allocate_global_pathid(&txn)?;
        let pathid2 = fixture.allocator.allocate_global_pathid(&txn)?;

        // First allocation should be 2 (since 1 is ROOT_INODE)
        assert_eq!(PathId(2), pathid1);
        // Second allocation should be 3
        assert_eq!(PathId(3), pathid2);

        txn.commit()?;
        Ok(())
    }

    #[test]
    fn test_allocate_pathid() -> anyhow::Result<()> {
        let a = Arena::from("a");
        let b = Arena::from("b");
        let c = Arena::from("c");
        let fixture = Fixture::setup([a, b, c])?;

        // After the arena root, allocate global pathids incrementally.
        assert_eq!(Some(PathId(2)), fixture.allocator.arena_root(a));
        assert_eq!(Some(PathId(3)), fixture.allocator.arena_root(b));
        assert_eq!(Some(PathId(4)), fixture.allocator.arena_root(c));
        assert_eq!(PathId(5), fixture.allocate_global_pathid()?);
        assert_eq!(PathId(6), fixture.allocate_global_pathid()?);

        // 1 is root, so everything starts at 2, 100 is allocated for
        // global pathids, 10000 for each arena
        assert_eq!(PathId(102), fixture.allocate_arena_pathid(a)?);
        assert_eq!(PathId(103), fixture.allocate_arena_pathid(a)?);

        assert_eq!(PathId(10102), fixture.allocate_arena_pathid(b)?);
        assert_eq!(PathId(10103), fixture.allocate_arena_pathid(b)?);

        assert_eq!(PathId(20102), fixture.allocate_arena_pathid(c)?);
        assert_eq!(PathId(20103), fixture.allocate_arena_pathid(c)?);
        Ok(())
    }

    #[test]
    fn test_allocate_arena_pathid_unknown_arena() -> anyhow::Result<()> {
        let fixture = Fixture::setup([])?;

        let arena = Arena::from("unknown");
        let db = GlobalDatabase::new(redb_utils::in_memory()?)?;
        let txn = db.begin_write()?;
        let res = fixture
            .allocator
            .allocate_arena_pathid(&mut txn.current_pathid_range_table()?, arena);
        assert!(matches!(res, Err(StorageError::UnknownArena(a)) if a == arena));

        Ok(())
    }

    #[test]
    fn test_arena_for_pathid_root() -> anyhow::Result<()> {
        let fixture = Fixture::setup([])?;
        let txn = fixture.db.begin_read()?;

        let result = fixture
            .allocator
            .arena_for_pathid(&txn, PathIdAllocator::ROOT_INODE)?;
        assert_eq!(None, result);

        Ok(())
    }

    #[test]
    fn test_arena_for_pathid_arena_root() -> anyhow::Result<()> {
        let arena = Arena::from("test");
        let fixture = Fixture::setup([arena])?;
        let txn = fixture.db.begin_read()?;

        let arena_root = fixture.allocator.arena_root(arena).unwrap();
        let result = fixture.allocator.arena_for_pathid(&txn, arena_root)?;

        assert_eq!(Some(arena), result);

        Ok(())
    }

    #[test]
    fn test_arena_for_pathid_not_found() -> anyhow::Result<()> {
        let fixture = Fixture::setup([])?;
        let txn = fixture.db.begin_read()?;

        let result = fixture.allocator.arena_for_pathid(&txn, PathId(999));

        assert!(result.is_err());
        match result {
            Err(StorageError::NotFound) => {}
            _ => panic!("Expected NotFound error"),
        }

        Ok(())
    }

    #[test]
    fn test_arena_for_pathid_global() -> anyhow::Result<()> {
        let fixture = Fixture::setup([])?;

        assert_eq!(
            None,
            fixture.arena_for_pathid(fixture.allocate_global_pathid()?)?
        );
        assert_eq!(
            None,
            fixture.arena_for_pathid(fixture.allocate_global_pathid()?)?
        );

        Ok(())
    }

    #[test]
    fn test_arena_for_pathid_arenas() -> anyhow::Result<()> {
        let a = Arena::from("a");
        let b = Arena::from("b");
        let c = Arena::from("c");
        let fixture = Fixture::setup([a, b, c])?;

        let first_in_a = fixture.allocate_arena_pathid(a)?;
        let first_in_b = fixture.allocate_arena_pathid(b)?;
        let first_in_c = fixture.allocate_arena_pathid(c)?;

        // first in range
        assert_eq!(Some(a), fixture.arena_for_pathid(first_in_a)?);
        assert_eq!(Some(b), fixture.arena_for_pathid(first_in_b)?);
        assert_eq!(Some(c), fixture.arena_for_pathid(first_in_c)?);

        // inside the range
        assert_eq!(Some(a), fixture.arena_for_pathid(first_in_a.plus(100))?);
        assert_eq!(Some(b), fixture.arena_for_pathid(first_in_b.plus(100))?);
        assert_eq!(Some(c), fixture.arena_for_pathid(first_in_c.plus(100))?);

        // last in range
        assert_eq!(None, fixture.arena_for_pathid(first_in_a.minus(1))?);
        assert_eq!(Some(a), fixture.arena_for_pathid(first_in_b.minus(1))?);
        assert_eq!(Some(b), fixture.arena_for_pathid(first_in_c.minus(1))?);

        Ok(())
    }

    #[test]
    fn test_arena_allocate_to_exhaustion() -> anyhow::Result<()> {
        let a = Arena::from("a");
        let b = Arena::from("b");
        let fixture = Fixture::setup([a, b])?;

        let first_in_a = fixture.allocate_arena_pathid(a)?;
        let first_in_b = fixture.allocate_arena_pathid(b)?;
        let last_in_a = first_in_b.minus(1);

        {
            let txn = fixture.arena_db(a).begin_write()?;
            {
                let mut table = txn.current_pathid_range_table()?;
                let allocator = &fixture.allocator;
                while allocator.allocate_arena_pathid(&mut table, a)? < last_in_a {}
            }
            txn.commit()?;
        }

        let first_in_new_range = fixture.allocate_arena_pathid(a)?;
        assert_eq!(first_in_b.plus(10000), first_in_new_range);
        let last_in_b = first_in_new_range.minus(1);

        assert_eq!(Some(a), fixture.arena_for_pathid(first_in_a)?);
        assert_eq!(Some(a), fixture.arena_for_pathid(last_in_a)?);
        assert_eq!(Some(b), fixture.arena_for_pathid(first_in_b)?);
        assert_eq!(Some(b), fixture.arena_for_pathid(last_in_b)?);
        assert_eq!(Some(a), fixture.arena_for_pathid(first_in_new_range)?);
        assert_eq!(
            Some(a),
            fixture.arena_for_pathid(first_in_new_range.plus(100))?
        );

        Ok(())
    }
}
