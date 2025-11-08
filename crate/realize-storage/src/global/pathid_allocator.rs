use super::db::GlobalWriteTransaction;
use crate::types::{InodePrefix, PartialInode, PathId};
use crate::{Inode, StorageError};
use redb::ReadableTable;

/// Allocate a new pathid in [GlobalDatabase].
pub(crate) fn allocal_global_inode(txn: &GlobalWriteTransaction) -> Result<Inode, StorageError> {
    Ok(PartialInode::from(allocate(&mut txn.pathid_range_table()?)?).to_inode(InodePrefix::ZERO))
}

/// Allocate a new pathid, using the given table and prefix.
/// allocation function.
pub(crate) fn allocate(
    current_range_table: &mut redb::Table<'_, (), (PathId, PathId)>,
) -> Result<PathId, StorageError> {
    let (current, end) = match current_range_table.get(())? {
        Some(value) => value.value(),
        None => (PathId::ROOT, PathId::MAX),
    };
    if current < end {
        let pathid = current.plus(1);
        current_range_table.insert((), (pathid, end))?;
        return Ok(pathid);
    }

    Err(StorageError::PathIdSpaceExhausted)
}

#[cfg(test)]
mod tests {
    use realize_types::Arena;

    use super::*;
    use crate::global::db::GlobalDatabase;
    use crate::utils::redb_utils;
    use std::collections::HashMap;
    use std::sync::Arc;

    struct Fixture {
        db: Arc<GlobalDatabase>,
        arena_dbs: HashMap<Arena, Arc<GlobalDatabase>>,
    }

    impl Fixture {
        fn setup<T>(arenas: T) -> anyhow::Result<Self>
        where
            T: IntoIterator<Item = Arena>,
        {
            let _ = env_logger::try_init();
            let db = GlobalDatabase::new(redb_utils::in_memory()?)?;
            let mut arena_dbs = HashMap::new();
            for arena in arenas.into_iter() {
                arena_dbs.insert(arena, GlobalDatabase::new(redb_utils::in_memory()?)?);
            }

            Ok(Self { db, arena_dbs })
        }

        fn arena_db(&self, arena: Arena) -> &Arc<GlobalDatabase> {
            self.arena_dbs.get(&arena).unwrap()
        }

        fn allocate_arena_pathid(&self, arena: Arena) -> Result<PathId, StorageError> {
            let txn = self.arena_db(arena).begin_write()?;
            let pathid = super::allocate(&mut txn.pathid_range_table()?)?;
            txn.commit()?;

            Ok(pathid)
        }
    }

    #[test]
    fn allocate_pathid_global() -> anyhow::Result<()> {
        let fixture = Fixture::setup([])?;
        let txn = fixture.db.begin_write()?;

        let inode1 = allocal_global_inode(&txn)?;
        let inode2 = allocal_global_inode(&txn)?;

        // First allocation should be 2 (since 1 is ROOT_INODE)
        assert_eq!(Inode(2), inode1);
        // Second allocation should be 3
        assert_eq!(Inode(3), inode2);

        txn.commit()?;
        Ok(())
    }

    #[test]
    fn allocate_in_arena() -> anyhow::Result<()> {
        let a = Arena::from("a");
        let b = Arena::from("b");
        let c = Arena::from("c");
        let fixture = Fixture::setup([a, b, c])?;

        // 1 is root, so everything starts at 2
        assert_eq!(PathId(2), fixture.allocate_arena_pathid(a)?);
        assert_eq!(PathId(3), fixture.allocate_arena_pathid(a)?);
        assert_eq!(PathId(4), fixture.allocate_arena_pathid(a)?);

        assert_eq!(PathId(2), fixture.allocate_arena_pathid(b)?);
        assert_eq!(PathId(3), fixture.allocate_arena_pathid(b)?);
        assert_eq!(PathId(4), fixture.allocate_arena_pathid(b)?);

        assert_eq!(PathId(2), fixture.allocate_arena_pathid(c)?);
        assert_eq!(PathId(3), fixture.allocate_arena_pathid(c)?);
        assert_eq!(PathId(4), fixture.allocate_arena_pathid(c)?);

        Ok(())
    }

    #[test]
    fn pathid_exhaustion() -> anyhow::Result<()> {
        let arena = Arena::from("arena");
        let fixture = Fixture::setup([arena])?;
        let txn = fixture.arena_db(arena).begin_write()?;
        {
            let mut table = txn.pathid_range_table()?;
            table.insert((), (PathId::MAX.minus(2), PathId::MAX))?;
            assert_eq!(PathId::MAX.minus(1), allocate(&mut table)?);
            assert_eq!(PathId::MAX, allocate(&mut table)?);
            assert!(matches!(
                allocate(&mut table),
                Err(StorageError::PathIdSpaceExhausted)
            ));
        }
        txn.commit()?;

        Ok(())
    }
}
