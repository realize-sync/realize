#![allow(dead_code)] // work in progress

use crate::StorageError;
use realize_types::Path;
use redb::{ReadTransaction, ReadableTable, TableDefinition, WriteTransaction};

/// Stores paths marked dirty.
///
/// The path can be in the index, in the cache or both.
///
/// Key: &str (path)
/// Value: Holder<DecisionTableEntry>
const DIRTY_TABLE: TableDefinition<&str, ()> = TableDefinition::new("engine.dirty");

/// Mark a path dirty.
///
/// This does nothing if the path is already dirty.
pub(crate) fn mark_dirty(txn: &WriteTransaction, path: &Path) -> Result<(), StorageError> {
    let mut dirty_table = txn.open_table(DIRTY_TABLE)?;
    if dirty_table.get(path.as_str())?.is_none() {
        dirty_table.insert(path.as_str(), ())?;
    }

    Ok(())
}

/// Check the dirty mark on a path.
pub(crate) fn is_dirty(txn: &ReadTransaction, path: &Path) -> Result<bool, StorageError> {
    let dirty_table = txn.open_table(DIRTY_TABLE)?;

    Ok(dirty_table.get(path.as_str())?.is_some())
}

/// Clear the dirty mark on a path.
///
/// This does nothing if the path has no dirty mark.
pub(crate) fn clear_dirty(txn: &WriteTransaction, path: &Path) -> Result<(), StorageError> {
    let mut dirty_table = txn.open_table(DIRTY_TABLE)?;
    dirty_table.remove(path.as_str())?;

    Ok(())
}

/// Get a dirty path from the table for processing.
///
/// The returned path is removed from the dirty table when the
/// transaction is committed.
pub(crate) fn take_dirty(txn: &WriteTransaction) -> Result<Option<Path>, StorageError> {
    let mut dirty_table = txn.open_table(DIRTY_TABLE)?;
    loop {
        // This loop skips invalid paths.
        match dirty_table.pop_first()? {
            None => {
                return Ok(None);
            }
            Some((k, _)) => {
                if let Ok(path) = Path::parse(k.value()) {
                    return Ok(Some(path));
                }
                // otherwise, pop another
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::utils::redb_utils;

    struct Fixture {
        db: Arc<redb::Database>,
        txn: Option<WriteTransaction>,
    }
    impl Fixture {
        fn setup() -> anyhow::Result<Fixture> {
            let _ = env_logger::try_init();
            let db = redb_utils::in_memory()?;
            let txn = db.begin_write()?;
            Ok(Self { db, txn: Some(txn) })
        }

        fn txn(&self) -> anyhow::Result<&WriteTransaction> {
            self.txn.as_ref().ok_or(anyhow::anyhow!("no transaction"))
        }

        fn commit(&mut self) -> anyhow::Result<()> {
            self.txn
                .take()
                .ok_or(anyhow::anyhow!("no transaction"))?
                .commit()?;

            Ok(())
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = self.txn.take().map(|v| {
                let _ = v.abort();
            });
        }
    }

    #[test]
    fn mark_and_take() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path1 = Path::parse("path1")?;
        let path2 = Path::parse("path2")?;

        mark_dirty(fixture.txn()?, &path1)?;
        mark_dirty(fixture.txn()?, &path2)?;

        assert_eq!(Some(path1), take_dirty(fixture.txn()?)?);
        assert_eq!(Some(path2), take_dirty(fixture.txn()?)?);
        assert_eq!(None, take_dirty(fixture.txn()?)?);
        assert_eq!(None, take_dirty(fixture.txn()?)?);

        Ok(())
    }

    #[test]
    fn check_dirty() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup()?;
        let path1 = Path::parse("path1")?;
        let path2 = Path::parse("path2")?;

        mark_dirty(fixture.txn()?, &path2)?;

        fixture.commit()?;

        let txn = fixture.db.begin_read()?;
        assert_eq!(false, is_dirty(&txn, &path1)?);
        assert_eq!(true, is_dirty(&txn, &path2)?);

        Ok(())
    }

    #[test]
    fn clear_mark() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path1 = Path::parse("path1")?;
        let path2 = Path::parse("path2")?;

        mark_dirty(fixture.txn()?, &path1)?;
        clear_dirty(fixture.txn()?, &path1)?;
        mark_dirty(fixture.txn()?, &path2)?;

        assert_eq!(Some(path2), take_dirty(fixture.txn()?)?);
        assert_eq!(None, take_dirty(fixture.txn()?)?);

        Ok(())
    }

    #[test]
    fn skip_invalid_path() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        fixture.txn()?.open_table(DIRTY_TABLE)?.insert("///", ())?;
        let path = Path::parse("path")?;

        mark_dirty(fixture.txn()?, &path)?;

        assert_eq!(Some(path), take_dirty(fixture.txn()?)?);
        assert_eq!(None, take_dirty(fixture.txn()?)?);

        Ok(())
    }
}
