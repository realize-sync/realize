use super::db::{GlobalReadTransaction, GlobalWriteTransaction};
use crate::types::{PartialPathId, PathIdPrefix};
use crate::{GlobalDatabase, PathId, StorageError};
use bimap::BiMap;
use realize_types::Arena;
use redb::ReadableTable;
use std::sync::Arc;

/// Allocate pathid ranges and assign them to arenas.
pub(crate) struct PathIdAllocator {
    db: Arc<GlobalDatabase>,
    prefixes: BiMap<Arena, PathIdPrefix>,
}

impl PathIdAllocator {
    pub(crate) const ROOT_INODE: PathId = PathId::ROOT;

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
            prefixes: BiMap::new(),
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
        self.prefixes
            .get_by_left(&arena)
            .map(|p| PartialPathId::ROOT.with(*p))
    }

    /// Check whether [PathId] is an arena root.
    pub(crate) fn is_arena_root(&self, pathid: PathId) -> bool {
        self.prefixes.get_by_right(&pathid.prefix()).is_some()
    }

    /// Allocate an pathid for an arena.
    ///
    /// `current_range_table` must be an opened table within the arena database.
    pub(crate) fn allocate_arena_pathid(
        &self,
        current_range_table: &mut redb::Table<'_, (), (PathId, PathId)>,
        arena: Arena,
    ) -> Result<PathId, StorageError> {
        self.allocate_pathid(current_range_table, || self.pathid_range_for_arena(arena))
    }

    /// Allocate a global pathid.
    pub(crate) fn allocate_global_pathid(
        &self,
        txn: &GlobalWriteTransaction,
    ) -> Result<PathId, StorageError> {
        self.allocate_pathid(&mut txn.current_pathid_range_table()?, || {
            Ok(pathid_range_with_prefix(PathIdPrefix::ZERO))
        })
    }

    /// Maps pathids to arenas.
    ///
    /// The root pathid of an arena is mapped to the arena, even though
    /// these pathids are allocated from the global range.
    pub(crate) fn arena_for_pathid(
        &self,
        _txn: &GlobalReadTransaction,
        pathid: PathId,
    ) -> Result<Option<Arena>, StorageError> {
        let prefix = pathid.prefix();
        if prefix == PathIdPrefix::ZERO {
            return Ok(None);
        }
        if let Some(arena) = self.prefixes.get_by_right(&prefix) {
            return Ok(Some(*arena));
        }
        Err(StorageError::NotFound)
    }

    /// Retrieve or allocate the arena root for the given arena.
    fn add_arena(&mut self, arena: Arena) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut arena_table = txn.arena_table()?;
            let prefix = PathIdPrefix::from_u8(
                self.prefixes
                    .right_values()
                    .map(|p| p.as_u8())
                    .max()
                    .unwrap_or(0)
                    + 1,
            );
            self.prefixes.insert(arena, prefix);
            let root = PartialPathId::ROOT.with(prefix);
            arena_table.insert(arena.as_str(), root)?;
            log::debug!("[{arena}]: prefix {prefix} root {root}");
        }
        txn.commit()?;
        Ok(())
    }

    /// Allocate a newpathid, using the given table and range
    /// allocation function.
    fn allocate_pathid(
        &self,
        current_range_table: &mut redb::Table<'_, (), (PathId, PathId)>,
        alloc_pathid_range: impl FnOnce() -> Result<(PathId, PathId), StorageError>,
    ) -> Result<PathId, StorageError> {
        let (current, end) = match current_range_table.get(())? {
            Some(value) => value.value(),
            None => alloc_pathid_range()?,
        };
        if current < end {
            let pathid = current.plus(1);
            current_range_table.insert((), (pathid, end))?;
            return Ok(pathid);
        }

        Err(StorageError::CannotAllocatePathId)
    }

    fn pathid_range_for_arena(&self, arena: Arena) -> Result<(PathId, PathId), StorageError> {
        let prefix = *self
            .prefixes
            .get_by_left(&arena)
            .ok_or_else(|| StorageError::UnknownArena(arena))?;

        Ok(pathid_range_with_prefix(prefix))
    }
}

fn pathid_range_with_prefix(prefix: PathIdPrefix) -> (PathId, PathId) {
    (
        PartialPathId::ROOT.with(prefix),
        PartialPathId::MAX.with(prefix),
    )
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

        assert_eq!(
            Some(PartialPathId::ROOT.with(PathIdPrefix::from_u8(1))),
            fixture.allocator.arena_root(a)
        );
        assert_eq!(
            Some(PartialPathId::ROOT.with(PathIdPrefix::from_u8(2))),
            fixture.allocator.arena_root(b)
        );
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

        let a_prefix = PathIdPrefix::from_u8(1);
        let b_prefix = PathIdPrefix::from_u8(2);
        let c_prefix = PathIdPrefix::from_u8(3);

        assert_eq!(
            Some(PartialPathId::ROOT.with(a_prefix)),
            fixture.allocator.arena_root(a)
        );
        assert_eq!(
            Some(PartialPathId::ROOT.with(b_prefix)),
            fixture.allocator.arena_root(b)
        );
        assert_eq!(
            Some(PartialPathId::ROOT.with(c_prefix)),
            fixture.allocator.arena_root(c)
        );

        // 1 is root, so everything starts at 2
        assert_eq!(
            PartialPathId(2).with(a_prefix),
            fixture.allocate_arena_pathid(a)?
        );
        assert_eq!(
            PartialPathId(3).with(a_prefix),
            fixture.allocate_arena_pathid(a)?
        );
        assert_eq!(
            PartialPathId(4).with(a_prefix),
            fixture.allocate_arena_pathid(a)?
        );

        assert_eq!(
            PartialPathId(2).with(b_prefix),
            fixture.allocate_arena_pathid(b)?
        );
        assert_eq!(
            PartialPathId(3).with(b_prefix),
            fixture.allocate_arena_pathid(b)?
        );
        assert_eq!(
            PartialPathId(4).with(b_prefix),
            fixture.allocate_arena_pathid(b)?
        );

        assert_eq!(
            PartialPathId(2).with(c_prefix),
            fixture.allocate_arena_pathid(c)?
        );
        assert_eq!(
            PartialPathId(3).with(c_prefix),
            fixture.allocate_arena_pathid(c)?
        );
        assert_eq!(
            PartialPathId(4).with(c_prefix),
            fixture.allocate_arena_pathid(c)?
        );

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

        let result = fixture
            .allocator
            .arena_for_pathid(&txn, PartialPathId(999).with(PathIdPrefix::from_u8(99)));

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
}
