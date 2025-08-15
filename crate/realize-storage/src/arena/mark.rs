#![allow(dead_code)] // work in progress

use std::sync::Arc;

use super::db::ArenaDatabase;
use super::dirty::WritableOpenDirty;
use super::tree::{TreeExt, TreeLoc, TreeReadOperations, WritableOpenTree};
use super::types::Mark;
use super::types::MarkTableEntry;
use crate::utils::holder::Holder;
use crate::{Inode, StorageError};
use redb::{ReadableTable, Table};

pub(crate) fn get<'a, L: Into<TreeLoc<'a>>>(
    db: &Arc<ArenaDatabase>,
    loc: L,
) -> Result<Mark, StorageError> {
    let txn = db.begin_read()?;

    txn.read_marks()?.get(&txn.read_tree()?, loc)
}
pub(crate) fn set<'a, L: Into<TreeLoc<'a>>>(
    db: &Arc<ArenaDatabase>,
    loc: L,
    mark: Mark,
) -> Result<(), StorageError> {
    let txn = db.begin_write()?;
    {
        let mut marks = txn.write_marks()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;

        marks.set(&mut tree, &mut dirty, loc, mark)?;
    }
    txn.commit()?;
    Ok(())
}
pub(crate) fn clear<'a, L: Into<TreeLoc<'a>>>(
    db: &Arc<ArenaDatabase>,
    loc: L,
) -> Result<(), StorageError> {
    let txn = db.begin_write()?;
    {
        let mut marks = txn.write_marks()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;

        marks.clear(&mut tree, &mut dirty, loc)?;
    }
    txn.commit()?;
    Ok(())
}

pub(crate) fn set_arena_mark(db: &Arc<ArenaDatabase>, mark: Mark) -> Result<(), StorageError> {
    let txn = db.begin_write()?;
    {
        let mut marks = txn.write_marks()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let root = tree.root();

        marks.set(&mut tree, &mut dirty, root, mark)?;
    }
    txn.commit()?;
    Ok(())
}
pub(crate) fn clear_arena_mark<'a, L: Into<TreeLoc<'a>>>(
    db: &Arc<ArenaDatabase>,
) -> Result<(), StorageError> {
    let txn = db.begin_write()?;
    {
        let mut marks = txn.write_marks()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let root = tree.root();

        marks.clear(&mut tree, &mut dirty, root)?;
    }
    txn.commit()?;
    Ok(())
}

pub(crate) struct ReadableOpenMark<T>
where
    T: ReadableTable<Inode, Holder<'static, MarkTableEntry>>,
{
    table: T,
}

impl<T> ReadableOpenMark<T>
where
    T: ReadableTable<Inode, Holder<'static, MarkTableEntry>>,
{
    pub(crate) fn new(table: T) -> Self {
        Self { table }
    }
}

pub(crate) struct WritableOpenMark<'a> {
    table: Table<'a, Inode, Holder<'static, MarkTableEntry>>,
}

impl<'a> WritableOpenMark<'a> {
    pub(crate) fn new(table: Table<'a, Inode, Holder<MarkTableEntry>>) -> Self {
        Self { table }
    }
}

/// Read operations for marks. See also [MarkExt].
pub(crate) trait MarkReadOperations {
    /// Get the mark at the given inode.
    ///
    /// [MarkExt::get] is usually more convenient, as it accepts paths as well as inodes.
    fn get_at_inode(
        &self,
        tree: &impl TreeReadOperations,
        inode: Inode,
    ) -> Result<Mark, StorageError>;
}

impl<T> MarkReadOperations for ReadableOpenMark<T>
where
    T: ReadableTable<Inode, Holder<'static, MarkTableEntry>>,
{
    fn get_at_inode(
        &self,
        tree: &impl TreeReadOperations,
        inode: Inode,
    ) -> Result<Mark, StorageError> {
        get_at_inode(&self.table, tree, inode)
    }
}

impl<'a> MarkReadOperations for WritableOpenMark<'a> {
    fn get_at_inode(
        &self,
        tree: &impl TreeReadOperations,
        inode: Inode,
    ) -> Result<Mark, StorageError> {
        get_at_inode(&self.table, tree, inode)
    }
}
/// Extend [MarkReadOperations] with convenience functions for working
/// with [Path].
pub(crate) trait MarkExt {
    /// Get the mark at the given location.
    ///
    /// If the location is a path, the path doesn't need to exist. The
    /// mark that's returned will be the mark of the last matching
    /// inode in the path - or that of the area root.
    fn get<'a, L: Into<TreeLoc<'a>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<Mark, StorageError>;
}
impl<T: MarkReadOperations> MarkExt for T {
    fn get<'a, L: Into<TreeLoc<'a>>>(
        &self,
        tree: &impl TreeReadOperations,
        loc: L,
    ) -> Result<Mark, StorageError> {
        self.get_at_inode(tree, tree.resolve_partial(loc)?)
    }
}

impl<'a> WritableOpenMark<'a> {
    /// Set a mark at the given location.
    ///
    /// If the location is a path, the path will be created if it doesn't already exist.
    pub(crate) fn set<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        dirty: &mut WritableOpenDirty,
        loc: L,
        mark: Mark,
    ) -> Result<(), StorageError> {
        let inode = tree.setup(loc)?;
        let old_mark = self.get_at_inode(tree, inode)?;
        tree.insert_and_incref(
            inode,
            &mut self.table,
            inode,
            Holder::with_content(MarkTableEntry { mark })?,
        )?;
        if old_mark != mark {
            dirty.mark_dirty_recursive(tree, inode)?;
        }

        Ok(())
    }

    /// Clear a mark at the given location.
    ///
    /// If the location is a path, the path doesn't need to exist.
    pub(crate) fn clear<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        tree: &mut WritableOpenTree,
        dirty: &mut WritableOpenDirty,
        loc: L,
    ) -> Result<(), StorageError> {
        let inode = match tree.resolve(loc)? {
            Some(inode) => inode,
            None => {
                // No mark to remove
                return Ok(());
            }
        };
        let old_mark = self.get_at_inode(tree, inode)?;
        let removed = tree.remove_and_decref(inode, &mut self.table, inode)?;
        if removed && old_mark != self.get_at_inode(tree, inode)? {
            dirty.mark_dirty_recursive(tree, inode)?;
        }

        Ok(())
    }
}

fn get_at_inode(
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

#[cfg(test)]
mod tests {
    use realize_types::Arena;

    use super::*;
    use crate::arena::{db::ArenaDatabase, dirty::DirtyReadOperations};
    use realize_types::Path;
    use std::sync::Arc;

    struct Fixture {
        db: Arc<ArenaDatabase>,
    }

    impl Fixture {
        fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();
            let arena = Arena::from("myarena");
            let db = ArenaDatabase::for_testing_single_arena(arena)?;

            Ok(Self { db })
        }
    }

    #[test]
    fn read_txn_with_mark_compiles() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_read()?;
        let mark = txn.read_marks()?;
        let tree = txn.read_tree()?;
        assert_eq!(Mark::default(), mark.get(&tree, tree.root())?);

        Ok(())
    }

    #[test]
    fn write_txn_with_mark_compiles() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mark = txn.read_marks()?;
        let tree = txn.read_tree()?;
        assert_eq!(Mark::default(), mark.get(&tree, tree.root())?);

        Ok(())
    }

    #[test]
    fn write_mark_compiles() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut mark = txn.write_marks()?;
        let mut tree = txn.write_tree()?;
        mark.clear(&mut tree, &mut txn.write_dirty()?, Path::parse("foo/bar")?)?;

        Ok(())
    }

    #[test]
    fn default_mark() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mark = txn.write_marks()?;
        let tree = txn.write_tree()?;

        assert_eq!(Mark::Watch, mark.get(&tree, tree.root())?);
        assert_eq!(Mark::Watch, mark.get(&tree, Path::parse("some/file.txt")?)?);

        Ok(())
    }

    #[test]
    fn set_and_get_root_mark() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut mark = txn.write_marks()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let root = tree.root();
        assert_eq!(Mark::Watch, mark.get(&tree, root)?);
        mark.set(&mut tree, &mut dirty, root, Mark::Keep)?;

        assert_eq!(Mark::Keep, mark.get(&tree, root)?);

        mark.set(&mut tree, &mut dirty, root, Mark::Own)?;
        assert_eq!(Mark::Own, mark.get(&tree, root)?);

        Ok(())
    }

    #[test]
    fn set_and_get_on_paths() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut mark = txn.write_marks()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;

        assert_eq!(Mark::Watch, mark.get(&tree, Path::parse("foo/bar")?)?);
        assert_eq!(Mark::Watch, mark.get(&tree, Path::parse("foo/qux")?)?);

        mark.set(&mut tree, &mut dirty, Path::parse("foo/bar")?, Mark::Keep)?;
        mark.set(&mut tree, &mut dirty, Path::parse("foo/qux")?, Mark::Own)?;

        assert_eq!(Mark::Keep, mark.get(&tree, Path::parse("foo/bar")?)?);
        assert_eq!(Mark::Own, mark.get(&tree, Path::parse("foo/qux")?)?);

        // replace
        mark.set(&mut tree, &mut dirty, Path::parse("foo/bar")?, Mark::Watch)?;
        assert_eq!(Mark::Watch, mark.get(&tree, Path::parse("foo/bar")?)?);

        Ok(())
    }

    #[test]
    fn set_and_get_on_hierarchical() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut mark = txn.write_marks()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let root = tree.root();

        mark.set(&mut tree, &mut dirty, root, Mark::Keep)?;
        mark.set(&mut tree, &mut dirty, Path::parse("foo")?, Mark::Own)?;
        mark.set(&mut tree, &mut dirty, Path::parse("foo/bar")?, Mark::Watch)?;
        mark.set(&mut tree, &mut dirty, Path::parse("baz")?, Mark::Own)?;

        // from root
        assert_eq!(Mark::Keep, mark.get(&tree, Path::parse("qux")?)?);

        // from foo
        assert_eq!(Mark::Own, mark.get(&tree, Path::parse("foo")?)?);
        assert_eq!(Mark::Own, mark.get(&tree, Path::parse("foo/baz")?)?);

        // from foo/bar
        assert_eq!(Mark::Watch, mark.get(&tree, Path::parse("foo/bar")?)?);
        assert_eq!(Mark::Watch, mark.get(&tree, Path::parse("foo/bar/baz")?)?);

        // from baz
        assert_eq!(Mark::Own, mark.get(&tree, Path::parse("baz")?)?);
        assert_eq!(Mark::Own, mark.get(&tree, Path::parse("baz/waldo")?)?);

        Ok(())
    }

    #[test]
    fn clear() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut mark = txn.write_marks()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let root = tree.root();

        mark.set(&mut tree, &mut dirty, root, Mark::Keep)?;
        mark.set(&mut tree, &mut dirty, Path::parse("foo")?, Mark::Own)?;
        mark.set(&mut tree, &mut dirty, Path::parse("foo/bar")?, Mark::Watch)?;

        // from foo/bar
        assert_eq!(Mark::Watch, mark.get(&tree, Path::parse("foo/bar/baz")?)?);

        mark.clear(&mut tree, &mut dirty, Path::parse("foo/bar")?)?;

        // from foo
        assert_eq!(Mark::Own, mark.get(&tree, Path::parse("foo/bar/baz")?)?);

        mark.clear(&mut tree, &mut dirty, Path::parse("foo")?)?;

        // from root
        assert_eq!(Mark::Keep, mark.get(&tree, Path::parse("foo/bar/baz")?)?);

        mark.clear(&mut tree, &mut dirty, root)?;

        // default
        assert_eq!(Mark::Watch, mark.get(&tree, Path::parse("foo/bar/baz")?)?);

        Ok(())
    }

    #[test]
    fn set_marks_dirty() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut mark = txn.write_marks()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let root = tree.root();

        tree.setup(Path::parse("foo/bar/baz")?)?;
        tree.setup(Path::parse("foo/qux/quux")?)?;
        tree.setup(Path::parse("waldo")?)?;

        mark.set(
            &mut tree,
            &mut dirty,
            Path::parse("foo/qux/quux")?,
            Mark::Keep,
        )?;

        assert!(dirty.is_dirty(&Path::parse("foo/qux/quux")?)?);
        assert!(!dirty.is_dirty(&Path::parse("foo/qux")?)?);
        assert!(!dirty.is_dirty(&Path::parse("foo")?)?);

        mark.set(&mut tree, &mut dirty, Path::parse("foo")?, Mark::Own)?;
        assert!(dirty.is_dirty(&Path::parse("foo")?)?);
        assert!(dirty.is_dirty(&Path::parse("foo/bar")?)?);
        assert!(dirty.is_dirty(&Path::parse("foo/bar/baz")?)?);
        assert!(dirty.is_dirty(&Path::parse("foo/qux")?)?);
        assert!(!dirty.is_dirty(&Path::parse("waldo")?)?);
        assert!(!dirty.is_dirty(&Path::parse("fred")?)?); // doesn't exist

        mark.set(&mut tree, &mut dirty, root, Mark::Keep)?;
        assert!(dirty.is_dirty(&Path::parse("waldo")?)?);
        assert!(!dirty.is_dirty(&Path::parse("fred")?)?); // doesn't exist

        Ok(())
    }

    #[test]
    fn clear_marks_dirty() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut mark = txn.write_marks()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;

        tree.setup(Path::parse("foo/bar/baz")?)?;
        tree.setup(Path::parse("foo/qux/quux")?)?;

        mark.set(&mut tree, &mut dirty, Path::parse("foo/qux")?, Mark::Keep)?;
        dirty.delete_range(0, 999)?;
        assert!(!dirty.is_dirty(&Path::parse("foo/qux")?)?);
        assert!(!dirty.is_dirty(&Path::parse("foo/qux/qux")?)?);

        mark.clear(&mut tree, &mut dirty, Path::parse("foo/qux")?)?;
        assert!(dirty.is_dirty(&Path::parse("foo/qux")?)?);
        assert!(dirty.is_dirty(&Path::parse("foo/qux/quux")?)?);

        assert!(!dirty.is_dirty(&Path::parse("foo/bar/baz")?)?);
        assert!(!dirty.is_dirty(&Path::parse("foo/bar")?)?);
        assert!(!dirty.is_dirty(&Path::parse("foo")?)?);

        Ok(())
    }

    #[test]
    fn clear_marks_dirty_root() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut mark = txn.write_marks()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;
        let root = tree.root();

        tree.setup(Path::parse("foo/bar/baz")?)?;
        tree.setup(Path::parse("foo/qux/quux")?)?;

        mark.set(&mut tree, &mut dirty, root, Mark::Keep)?;
        dirty.delete_range(0, 999)?;

        assert!(!dirty.is_dirty(&Path::parse("foo/bar/baz")?)?);
        assert!(!dirty.is_dirty(&Path::parse("foo/qux/quux")?)?);

        mark.clear(&mut tree, &mut dirty, root)?;
        assert!(dirty.is_dirty(&Path::parse("foo/bar/baz")?)?);
        assert!(dirty.is_dirty(&Path::parse("foo/bar")?)?);
        assert!(dirty.is_dirty(&Path::parse("foo")?)?);
        assert!(dirty.is_dirty(&Path::parse("foo/qux/quux")?)?);
        assert!(dirty.is_dirty(&Path::parse("foo/qux")?)?);

        Ok(())
    }

    #[test]
    fn clear_doesnt_mark_dirty_if_ineffective() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut mark = txn.write_marks()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;

        tree.setup(Path::parse("foo/bar/baz")?)?;
        tree.setup(Path::parse("foo/qux/quux")?)?;

        mark.clear(&mut tree, &mut dirty, Path::parse("foo/qux")?)?;
        assert!(!dirty.is_dirty(&Path::parse("foo/qux/quux")?)?);
        assert!(!dirty.is_dirty(&Path::parse("foo/bar/baz")?)?);

        Ok(())
    }

    #[test]
    fn set_doesnt_mark_dirty_if_ineffective() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut mark = txn.write_marks()?;
        let mut tree = txn.write_tree()?;
        let mut dirty = txn.write_dirty()?;

        tree.setup(Path::parse("foo/bar/baz")?)?;
        tree.setup(Path::parse("foo/qux/quux")?)?;

        mark.set(
            &mut tree,
            &mut dirty,
            Path::parse("foo/qux")?,
            Mark::default(),
        )?;
        assert!(!dirty.is_dirty(&Path::parse("foo/qux/quux")?)?);
        assert!(!dirty.is_dirty(&Path::parse("foo/bar/baz")?)?);

        Ok(())
    }
}
