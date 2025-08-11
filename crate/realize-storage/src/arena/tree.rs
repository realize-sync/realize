#![allow(dead_code)]

use super::db::{ArenaReadTransaction, ArenaWriteTransaction};
use crate::InodeAllocator;
use crate::{Inode, StorageError};
use realize_types::{Arena, Path};
use redb::{ReadableTable, Table};
use std::collections::VecDeque;
use std::sync::Arc;

/// A tree stored on a [ArenaDatabase].
///
/// This type and the associated types allow managing a tree on a
/// database.
///
/// It must first be open for reading, given a read transaction:
///
/// ```ignore
/// let tree: Tree = ...;
/// let txn = db.begin_read()?;
/// let readable_tree = txn.read_tree(&tree);
/// let inode = readable_tree.lookup(tree.root(), "mydir")?;
/// ```
///
/// or a write transaction:
///
/// ```ignore
/// let txn = db.begin_write()?;
/// let readable_tree = txn.read_tree(&tree);
/// let inode = readable_tree.lookup(tree.root(), "mydir")?;
/// ```
///
/// It can also be open for writing, given a write transaction:
///
/// ```ignore
/// let txn = db.begin_write()?;
/// let writable_tree = txn.write_tree(&tree);
/// readable_tree.add(path)?;
/// ```
pub(crate) struct Tree {
    arena: Arena,
    root: Inode,
    allocator: Arc<InodeAllocator>,
}

impl Tree {
    pub(crate) fn new(arena: Arena, root: Inode, allocator: Arc<InodeAllocator>) -> Self {
        Self {
            arena,
            root,
            allocator,
        }
    }
}

/// An extension for [ArenaReadTransaction] and
/// [ArenaWriteTransaction] to open a tree for reading.
pub(crate) trait OpenTree {
    fn read_tree(&self, tree: &Tree) -> Result<impl TreeReadOperations, StorageError>;
}

/// An extension for [ArenaWriteTransaction] to open a tree for
/// writing.
pub(crate) trait OpenTreeWrite {
    fn write_tree<'a>(&'a self, tree: &'a Tree) -> Result<WritableOpenTree<'a>, StorageError>;
}

impl OpenTree for ArenaReadTransaction {
    fn read_tree(&self, tree: &Tree) -> Result<impl TreeReadOperations, StorageError> {
        Ok(ReadableOpenTree {
            table: self.tree_table()?,
            root: tree.root,
        })
    }
}
impl OpenTree for ArenaWriteTransaction {
    fn read_tree(&self, tree: &Tree) -> Result<impl TreeReadOperations, StorageError> {
        Ok(ReadableOpenTree {
            table: self.tree_table()?,
            root: tree.root,
        })
    }
}
impl OpenTreeWrite for ArenaWriteTransaction {
    fn write_tree<'a>(&'a self, tree: &'a Tree) -> Result<WritableOpenTree<'a>, StorageError> {
        Ok(WritableOpenTree {
            table: self.tree_table()?,
            refcount_table: self.tree_refcount_table()?,
            current_inode_range_table: self.current_inode_range_table()?,
            tree,
        })
    }
}

struct ReadableOpenTree<T>
where
    T: ReadableTable<(Inode, &'static str), Inode>,
{
    table: T,
    root: Inode,
}

/// Operations supported on a tree open for read with [OpenTree::read_tree].
///
/// See also [TreeExt] for path-based operations.
pub(crate) trait TreeReadOperations {
    /// Returns the tree root inode.
    fn root(&self) -> Inode;

    /// Lookup a specific name in the given node.
    fn lookup(&self, inode: Inode, name: &str) -> Result<Option<Inode>, StorageError>;

    /// List names under the given inode.
    fn readdir(&self, inode: Inode) -> ReadDirIterator;

    /// Follow the inodes back up to the root and build a path.
    fn backtrack(&self, inode: Inode) -> Result<Path, StorageError>;

    /// Check whether the given inode exists
    fn exists(&self, inode: Inode) -> Result<bool, StorageError>;

    /// Return the parent of the given inode, or None if not found.
    ///
    /// The parent of the root inode is None. All other existing inodes
    /// have a parent.
    fn parent(&self, inode: Inode) -> Result<Option<Inode>, StorageError>;
}

impl<T> TreeReadOperations for ReadableOpenTree<T>
where
    T: ReadableTable<(Inode, &'static str), Inode>,
{
    fn root(&self) -> Inode {
        self.root
    }
    fn lookup(&self, inode: Inode, name: &str) -> Result<Option<Inode>, StorageError> {
        lookup(&self.table, inode, name)
    }
    fn readdir(&self, inode: Inode) -> ReadDirIterator<'_> {
        readdir(&self.table, inode)
    }
    fn backtrack(&self, inode: Inode) -> Result<Path, StorageError> {
        backtrack(&self.table, self.root, inode)
    }
    fn exists(&self, inode: Inode) -> Result<bool, StorageError> {
        exists(&self.table, self.root, inode)
    }
    fn parent(&self, inode: Inode) -> Result<Option<Inode>, StorageError> {
        parent(&self.table, inode)
    }
}

impl<'a> TreeReadOperations for WritableOpenTree<'a> {
    fn root(&self) -> Inode {
        self.tree.root
    }
    fn lookup(&self, inode: Inode, name: &str) -> Result<Option<Inode>, StorageError> {
        lookup(&self.table, inode, name)
    }
    fn readdir(&self, inode: Inode) -> ReadDirIterator<'_> {
        readdir(&self.table, inode)
    }
    fn backtrack(&self, inode: Inode) -> Result<Path, StorageError> {
        backtrack(&self.table, self.tree.root, inode)
    }
    fn exists(&self, inode: Inode) -> Result<bool, StorageError> {
        exists(&self.table, self.tree.root, inode)
    }
    fn parent(&self, inode: Inode) -> Result<Option<Inode>, StorageError> {
        parent(&self.table, inode)
    }
}

/// Extend [TreeReadOperations] with convenience functions for working
/// with [Path].
pub(crate) trait TreeExt {
    /// Lookup the given path.
    ///
    /// Returns the inode of the leaf or None if one of the components
    /// of the path cannot be found.
    fn lookup_path<P: AsRef<Path>>(&self, path: P) -> Result<Option<Inode>, StorageError>;

    /// Lookup the given path, fail with [StorageError::NotFound] if
    /// it doesn't exist.
    fn expect_path<P: AsRef<Path>>(&self, path: P) -> Result<Inode, StorageError>;

    /// Read the content of the given directory.
    fn readdir_path<P: AsRef<Path>>(&self, path: P) -> ReadDirIterator<'_>;

    /// Checks whether the given path exists.
    fn path_exists<P: AsRef<Path>>(&self, path: P) -> Result<bool, StorageError>;

    /// Goes through whole tree, starting at inode and return it as an
    /// iterator (depth-first).
    ///
    /// Only enters the node for which `enter` returns true.
    fn recurse_path<P, F>(
        &self,
        path: P,
        enter: F,
    ) -> impl Iterator<Item = Result<Inode, StorageError>>
    where
        P: AsRef<Path>,
        F: FnMut(Inode) -> bool;

    /// Goes through whole tree, starting at inode and return it as an
    /// iterator (depth-first).
    ///
    /// Only enters the node for which `enter` returns true.
    fn recurse<F>(
        &self,
        inode: Inode,
        enter: F,
    ) -> impl Iterator<Item = Result<Inode, StorageError>>
    where
        F: FnMut(Inode) -> bool;
}

impl<T: TreeReadOperations> TreeExt for T {
    fn expect_path<P: AsRef<Path>>(&self, path: P) -> Result<Inode, StorageError> {
        self.lookup_path(path)?.ok_or(StorageError::NotFound)
    }

    fn lookup_path<P: AsRef<Path>>(&self, path: P) -> Result<Option<Inode>, StorageError> {
        let path = path.as_ref();
        let mut current = self.root();
        for component in Path::components(Some(path)) {
            if let Some(e) = self.lookup(current, component)? {
                current = e
            } else {
                log::debug!("not found: {component} in {current}");
                return Ok(None);
            };
        }

        Ok(Some(current))
    }

    fn readdir_path<P: AsRef<Path>>(&self, path: P) -> ReadDirIterator<'_> {
        match self.expect_path(path) {
            Err(err) => ReadDirIterator::failed(err),
            Ok(inode) => self.readdir(inode),
        }
    }

    fn path_exists<P: AsRef<Path>>(&self, path: P) -> Result<bool, StorageError> {
        Ok(self.lookup_path(path)?.is_some())
    }

    fn recurse_path<P, F>(
        &self,
        path: P,
        enter: F,
    ) -> impl Iterator<Item = Result<Inode, StorageError>>
    where
        P: AsRef<Path>,
        F: FnMut(Inode) -> bool,
    {
        RecurseIterator {
            tree: self,
            enter,
            stack: VecDeque::from([self.readdir_path(path)]),
        }
    }

    fn recurse<F>(
        &self,
        inode: Inode,
        enter: F,
    ) -> impl Iterator<Item = Result<Inode, StorageError>>
    where
        F: FnMut(Inode) -> bool,
    {
        RecurseIterator {
            tree: self,
            enter,
            stack: VecDeque::from([self.readdir(inode)]),
        }
    }
}

/// Implements [TreeExt::recurse].
struct RecurseIterator<'a, T, F>
where
    T: TreeReadOperations,
    F: FnMut(Inode) -> bool,
{
    tree: &'a T,
    enter: F,
    stack: VecDeque<ReadDirIterator<'a>>,
}

impl<'a, T, F> Iterator for RecurseIterator<'a, T, F>
where
    T: TreeReadOperations,
    F: FnMut(Inode) -> bool,
{
    type Item = Result<Inode, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(iter) = self.stack.back_mut() {
            while let Some(elt) = iter.next() {
                match elt {
                    Err(err) => {
                        return Some(Err(err));
                    }
                    Ok((_, inode)) => {
                        if (self.enter)(inode) {
                            self.stack.push_back(self.tree.readdir(inode));
                        }
                        return Some(Ok(inode));
                    }
                }
            }
            self.stack.pop_back();
        }

        None
    }
}

/// A tree open for write with [OpenTree::write_tree].
pub(crate) struct WritableOpenTree<'a> {
    table: Table<'a, (Inode, &'static str), Inode>,
    refcount_table: Table<'a, Inode, u32>,
    current_inode_range_table: redb::Table<'a, (), (Inode, Inode)>,
    tree: &'a Tree,
}

impl<'a> WritableOpenTree<'a> {
    /// Make [path] a valid path on the tree, create it if necessary.
    ///
    /// Returns a possibly empty vector of [Inode] containting the
    /// inodes of the parent directories (excluding the root inode),
    /// as well as the leaf inode.
    ///
    /// This increments the reference count on the leaf. To decrement
    /// the reference count and allow the path to be cleaned up, call
    /// [WritableOpenTree::decref].
    pub(crate) fn add<P: AsRef<Path>>(
        &mut self,
        path: P,
    ) -> Result<(Vec<Inode>, Inode), StorageError> {
        let path = path.as_ref();
        let leaf_name = path.name();
        let branches = self.make_branches(self.tree.root, path.parent().as_ref())?;
        let parent_inode = branches.last().map(|i| *i).unwrap_or(self.tree.root);
        let leaf = match get_inode(&self.table, parent_inode, leaf_name)? {
            Some(inode) => inode,
            None => {
                let new_inode = self.allocate_inode()?;
                self.add_inode(parent_inode, new_inode, leaf_name)?;

                new_inode
            }
        };
        self.incref(leaf)?;

        Ok((branches, leaf))
    }

    /// Increment reference count on the leaf node of the given path.
    ///
    /// This is called automatically by `add` for the leaf node, but
    /// you might want to call it if you have more than one reference
    /// or make reference to intermediate nodes.
    pub(crate) fn incref_path<P: AsRef<Path>>(&mut self, path: P) -> Result<(), StorageError> {
        self.incref(self.expect_path(path)?)
    }

    /// Increment reference count on the given inode.
    ///
    /// This is called automatically by `add` for the leaf node, but
    /// you might want to call it if you have more than one reference
    /// or make reference to intermediate nodes.
    pub(crate) fn incref(&mut self, inode: Inode) -> Result<(), StorageError> {
        let mut refcount = self
            .refcount_table
            .get(inode)?
            .map(|v| v.value())
            .unwrap_or(0);
        refcount += 1;
        self.refcount_table.insert(inode, refcount)?;

        Ok(())
    }

    /// Decrement reference count on the leaf node of the given path..
    ///
    /// This might result in the deletion of the node and its parent
    /// if the refcount reaches 0.
    pub(crate) fn decref_path<P: AsRef<Path>>(&mut self, path: P) -> Result<(), StorageError> {
        self.decref(self.expect_path(path)?)
    }

    /// Decrement reference count on the given inode.
    ///
    /// This might result in the deletion of the node and its parent
    /// if the refcount reaches 0.
    pub(crate) fn decref(&mut self, inode: Inode) -> Result<(), StorageError> {
        let mut refcount = self
            .refcount_table
            .get(inode)?
            .ok_or(StorageError::NotFound)?
            .value();
        refcount = refcount.saturating_sub(1);

        if refcount > 0 {
            self.refcount_table.insert(inode, refcount)?;
        } else {
            self.refcount_table.remove(inode)?;
            let parent = self.table.remove((inode, ".."))?.map(|v| v.value());
            if let Some(parent) = parent {
                self.table
                    .retain_in(inode_range(parent), |_, v| v != inode)?;
                self.decref(parent)?;
            }
        }

        Ok(())
    }

    fn allocate_inode(&mut self) -> Result<Inode, StorageError> {
        self.tree
            .allocator
            .allocate_arena_inode(&mut self.current_inode_range_table, self.tree.arena)
    }

    /// Make sure that the given path exists; create it if necessary.
    ///
    /// Returns the inode of the branch pointed to by the path.
    fn make_branches(
        &mut self,
        start: Inode,
        path: Option<&Path>,
    ) -> Result<Vec<Inode>, StorageError> {
        let mut path_inodes = Vec::new();
        let mut current = start;
        for component in Path::components(path) {
            let inode = if let Some(inode) = get_inode(&self.table, current, component)? {
                log::debug!("found {component} in {current} -> {inode}");

                inode
            } else {
                log::debug!("add {component} in {current}");
                let new_inode = self.allocate_inode()?;
                self.add_inode(current, new_inode, component)?;

                new_inode
            };
            path_inodes.push(inode);
            current = inode;
        }

        Ok(path_inodes)
    }

    fn add_inode(
        &mut self,
        parent_inode: Inode,
        new_inode: Inode,
        name: &str,
    ) -> Result<(), StorageError> {
        log::debug!("new tree node {parent_inode} {name} -> {new_inode}");
        self.table.insert((parent_inode, name), new_inode)?;
        self.table.insert((new_inode, ".."), parent_inode)?;
        self.incref(parent_inode)?;

        Ok(())
    }
}

fn lookup(
    tree_table: &impl ReadableTable<(Inode, &'static str), Inode>,
    inode: Inode,
    name: &str,
) -> Result<Option<Inode>, StorageError> {
    Ok(tree_table.get((inode, name))?.map(|v| v.value()))
}

fn exists(
    tree_table: &impl ReadableTable<(Inode, &'static str), Inode>,
    root_inode: Inode,
    inode: Inode,
) -> Result<bool, StorageError> {
    if root_inode == inode {
        return Ok(true);
    }

    Ok(tree_table.get((inode, ".."))?.is_some())
}

fn backtrack(
    tree_table: &impl ReadableTable<(Inode, &'static str), Inode>,
    root_inode: Inode,
    inode: Inode,
) -> Result<Path, StorageError> {
    let mut components = VecDeque::new();
    let mut current = inode;
    'outer: while let Some(parent) = parent(tree_table, current)? {
        for v in tree_table.range(inode_range(parent))? {
            let v = v?;
            let (key, value) = v;
            if value.value() == current {
                let (_, name) = key.value();
                components.push_front(name.to_string());

                current = parent;
                continue 'outer;
            }
        }
        return Err(StorageError::InconsistentDatabase(format!(
            "{current} not in its parent {parent}"
        )));
    }
    if current != root_inode {
        return Err(StorageError::NotFound);
    }

    Ok(Path::parse(components.make_contiguous().join("/"))?)
}

fn parent(
    tree_table: &impl ReadableTable<(Inode, &'static str), Inode>,
    inode: Inode,
) -> Result<Option<Inode>, StorageError> {
    Ok(tree_table.get((inode, ".."))?.map(|v| v.value()))
}

fn readdir(
    tree_table: &impl ReadableTable<(Inode, &'static str), Inode>,
    inode: Inode,
) -> ReadDirIterator<'_> {
    let range = tree_table
        .range(inode_range(inode))
        .map_err(|e| StorageError::from(e));

    ReadDirIterator { iter: Some(range) }
}

/// Iterator returned by [TreeReadOperations::readdir]
pub(crate) struct ReadDirIterator<'a> {
    iter: Option<Result<redb::Range<'a, (Inode, &'static str), Inode>, StorageError>>,
}

impl<'a> ReadDirIterator<'a> {
    /// Builds an iterator that will only return that error.
    pub(crate) fn failed(err: StorageError) -> Self {
        Self {
            iter: Some(Err(err)),
        }
    }
}

impl<'a> Iterator for ReadDirIterator<'a> {
    type Item = Result<(String, Inode), StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.iter {
            None => None,
            Some(Err(_)) => Some(Err(self.iter.take().unwrap().err().unwrap())),
            Some(Ok(iter)) => {
                while let Some(v) = iter.next() {
                    match v {
                        Err(err) => {
                            return Some(Err(err.into()));
                        }
                        Ok(v) => {
                            let name = v.0.value().1;
                            if name != "." && name != ".." {
                                let inode = v.1.value();
                                return Some(Ok((name.to_string(), inode)));
                            }
                        }
                    }
                }

                None
            }
        }
    }
}

fn get_inode(
    table: &impl redb::ReadableTable<(Inode, &'static str), Inode>,
    parent_inode: Inode,
    name: &str,
) -> Result<Option<Inode>, StorageError> {
    match table.get((parent_inode, name))? {
        None => Ok(None),
        Some(e) => Ok(Some(e.value())),
    }
}

/// Builds a range that covers all entries for the given inode.
fn inode_range(inode: Inode) -> std::ops::Range<(Inode, &'static str)> {
    (inode, "")..(inode.plus(1), "")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::GlobalDatabase;
    use crate::arena::db::ArenaDatabase;
    use crate::utils::redb_utils;

    use super::*;

    struct Fixture {
        db: Arc<ArenaDatabase>,
        tree: Tree,
    }

    impl Fixture {
        fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();
            let db = ArenaDatabase::new(redb_utils::in_memory()?)?;
            let arena = Arena::from("myarena");
            let allocator =
                InodeAllocator::new(GlobalDatabase::new(redb_utils::in_memory()?)?, [arena])?;
            let tree = Tree::new(arena, Inode(1), allocator);

            Ok(Self { db, tree })
        }
    }

    #[test]
    fn read_txn_with_tree_compiles() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_read()?;
        let tree = txn.read_tree(&fixture.tree)?;
        assert_eq!(None, tree.lookup(Inode(1), "test")?,);

        Ok(())
    }

    #[test]
    fn write_txn_with_tree_compiles() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let tree = txn.read_tree(&fixture.tree)?;
        assert_eq!(None, tree.lookup(Inode(1), "test")?);

        Ok(())
    }

    #[test]
    fn write_tree_compiles() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree(&fixture.tree)?;
        let _ = tree.add(&Path::parse("test")?);

        Ok(())
    }

    #[test]
    fn add_depth0() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree(&fixture.tree)?;
        let (parents, node) = tree.add(&Path::parse("test")?)?;
        assert!(parents.is_empty());

        assert_eq!(Some(node), tree.lookup(Inode(1), "test")?);

        Ok(())
    }

    #[test]
    fn add_depth1() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree(&fixture.tree)?;
        let (parents, _) = tree.add(&Path::parse("foo/bar")?)?;
        assert_eq!(1, parents.len());

        Ok(())
    }

    #[test]
    fn add_same() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree(&fixture.tree)?;
        let first = tree.add(&Path::parse("foo/bar")?)?;
        let second = tree.add(&Path::parse("foo/bar")?)?;
        assert_eq!(first, second);

        Ok(())
    }

    #[test]
    fn add_deeper() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree(&fixture.tree)?;
        let (bar_parents, bar_inode) = tree.add(&Path::parse("foo/bar")?)?;
        let (baz_parents, _) = tree.add(&Path::parse("foo/bar/baz")?)?;
        assert_eq!(1, bar_parents.len());
        assert_eq!(2, baz_parents.len());
        assert_eq!(vec![bar_parents[0], bar_inode], baz_parents);
        Ok(())
    }

    #[test]
    fn lookup_path() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree(&fixture.tree)?;
        let (baz_parents, baz_inode) = tree.add(&Path::parse("foo/bar/baz")?)?;
        let (_, qux_inode) = tree.add(&Path::parse("foo/bar/baz/qux")?)?;

        assert_eq!(2, baz_parents.len());
        assert_eq!(
            Some(baz_parents[0]),
            tree.lookup_path(&Path::parse("foo")?)?
        );
        assert_eq!(
            Some(baz_parents[1]),
            tree.lookup_path(&Path::parse("foo/bar")?)?
        );
        assert_eq!(
            Some(baz_inode),
            tree.lookup_path(&Path::parse("foo/bar/baz")?)?
        );

        assert_eq!(
            Some(qux_inode),
            tree.lookup_path(&Path::parse("foo/bar/baz/qux")?)?
        );

        Ok(())
    }

    #[test]
    fn lookup_path_not_found() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree(&fixture.tree)?;
        tree.add(&Path::parse("foo/bar")?)?;

        assert_eq!(
            None,
            tree.lookup_path(Path::parse("foo/bar/notfound1/notfound2")?)?
        );
        assert_eq!(None, tree.lookup_path(Path::parse("foo/bar/notfound")?)?);
        assert_eq!(None, tree.lookup_path(Path::parse("foo/notfound")?)?);
        assert_eq!(None, tree.lookup_path(Path::parse("notfound")?)?);

        Ok(())
    }

    #[test]
    fn readdir() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree(&fixture.tree)?;
        let (_, foo_inode) = tree.add(&Path::parse("foo")?)?;
        let (_, bar_inode) = tree.add(&Path::parse("foo/bar")?)?;
        let (_, baz_inode) = tree.add(&Path::parse("foo/bar/baz")?)?;
        let (_, qux_inode) = tree.add(&Path::parse("foo/bar/qux")?)?;
        let (_, quux_inode) = tree.add(&Path::parse("foo/bar/quux")?)?;

        assert_eq!(
            Some(vec![("foo".to_string(), foo_inode)]),
            tree.readdir(tree.root())
                .collect::<Result<Vec<_>, _>>()
                .ok()
        );
        assert_eq!(
            Some(vec![("bar".to_string(), bar_inode)]),
            tree.readdir(foo_inode).collect::<Result<Vec<_>, _>>().ok()
        );

        assert_eq!(
            Some(vec![
                ("baz".to_string(), baz_inode),
                ("quux".to_string(), quux_inode),
                ("qux".to_string(), qux_inode),
            ]),
            tree.readdir(bar_inode).collect::<Result<Vec<_>, _>>().ok()
        );

        Ok(())
    }

    #[test]
    fn backtrack() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree(&fixture.tree)?;
        let (_, foo_inode) = tree.add(&Path::parse("foo")?)?;
        let (_, bar_inode) = tree.add(&Path::parse("foo/bar")?)?;
        let (_, baz_inode) = tree.add(&Path::parse("foo/bar/baz")?)?;

        assert_eq!(Path::parse("foo/bar/baz")?, tree.backtrack(baz_inode)?);
        assert_eq!(Path::parse("foo/bar")?, tree.backtrack(bar_inode)?);
        assert_eq!(Path::parse("foo")?, tree.backtrack(foo_inode)?);
        // Root cannot be turned into a path, as empty paths are invalid.
        assert!(tree.backtrack(tree.root()).is_err());

        assert!(matches!(
            tree.backtrack(Inode(999)),
            Err(StorageError::NotFound)
        ));

        Ok(())
    }

    #[test]
    fn exists() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree(&fixture.tree)?;
        assert!(tree.exists(tree.root())?);
        assert!(!tree.exists(Inode(999))?);

        let (_, foo_inode) = tree.add(&Path::parse("foo")?)?;
        let (_, bar_inode) = tree.add(&Path::parse("foo/bar")?)?;
        assert!(tree.exists(foo_inode)?);
        assert!(tree.exists(bar_inode)?);

        Ok(())
    }

    #[test]
    fn parent() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree(&fixture.tree)?;
        assert!(tree.exists(tree.root())?);
        assert!(!tree.exists(Inode(999))?);

        let (_, foo) = tree.add(&Path::parse("foo")?)?;
        let (_, bar) = tree.add(&Path::parse("foo/bar")?)?;
        let (_, baz) = tree.add(&Path::parse("foo/bar/baz")?)?;
        assert_eq!(Some(tree.root()), tree.parent(foo)?);
        assert_eq!(Some(foo), tree.parent(bar)?);
        assert_eq!(Some(bar), tree.parent(baz)?);
        assert_eq!(None, tree.parent(Inode(999))?);
        assert_eq!(None, tree.parent(tree.root())?);

        Ok(())
    }

    #[test]
    fn refcount() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree(&fixture.tree)?;
        let (bar_parents, bar) = tree.add(&Path::parse("foo/bar")?)?;
        let foo = bar_parents[0];
        let (_, baz) = tree.add(&Path::parse("foo/bar/baz")?)?;
        let (_, qux) = tree.add(&Path::parse("foo/qux")?)?;

        // holding a reference on bar, baz and qux

        // decref qux deletes it, but not foo
        tree.decref(qux)?;
        assert!(tree.exists(foo)?);
        assert!(tree.exists(bar)?);
        assert!(tree.exists(baz)?);
        assert!(!tree.exists(qux)?);

        // decref bar does not delete it, because it contains baz
        tree.decref(bar)?;
        assert!(tree.exists(foo)?);
        assert!(tree.exists(bar)?);
        assert!(tree.exists(baz)?);

        // decref baz deletes bar and baz, but not foo
        tree.decref(baz)?;
        assert!(!tree.exists(foo)?);
        assert!(!tree.exists(bar)?);
        assert!(!tree.exists(baz)?);

        Ok(())
    }

    #[test]
    fn incref() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree(&fixture.tree)?;
        let (parents, baz) = tree.add(&Path::parse("foo/bar/baz")?)?;
        let bar = parents[1];

        // holding a reference on baz
        tree.incref(bar)?;
        tree.incref(bar)?;
        tree.incref(bar)?;

        // decref baz deletes baz, but not bar
        tree.decref(baz)?;
        assert!(tree.exists(bar)?);
        assert!(!tree.exists(baz)?);

        // now that baz is gone, it still takes 3 decref to delete bar
        tree.decref(bar)?;
        assert!(tree.exists(bar)?);
        tree.decref(bar)?;
        assert!(tree.exists(bar)?);
        tree.decref(bar)?;
        assert!(!tree.exists(bar)?);

        Ok(())
    }

    #[test]
    fn recurse() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree(&fixture.tree)?;

        let (_, foo) = tree.add(Path::parse("foo")?)?;
        let (_, bar) = tree.add(Path::parse("foo/bar")?)?;
        let (_, baz) = tree.add(Path::parse("foo/bar/baz")?)?;
        let (_, qux) = tree.add(Path::parse("foo/bar/baz/qux")?)?;
        let (_, corge) = tree.add(Path::parse("foo/bar/corge")?)?;
        let (_, graply) = tree.add(Path::parse("foo/bar/corge/graply")?)?;
        let (_, grault) = tree.add(Path::parse("foo/bar/corge/grault")?)?;
        let (_, quux) = tree.add(Path::parse("foo/bar/quux")?)?;
        let (_, waldo) = tree.add(Path::parse("foo/bar/waldo")?)?;
        let (_, fred) = tree.add(Path::parse("foo/fred")?)?;
        tree.add(Path::parse("xyzzy")?)?; // not in foo, ignored

        assert_eq!(
            vec![bar, baz, qux, corge, graply, grault, quux, waldo, fred],
            tree.recurse(foo, |_| true).collect::<Result<Vec<_>, _>>()?
        );

        // don't enter corge or baz
        assert_eq!(
            vec![bar, baz, corge, quux, waldo, fred],
            tree.recurse(foo, |inode| inode != baz && inode != corge)
                .collect::<Result<Vec<_>, _>>()?
        );

        // don't enter anything; list foo's children
        assert_eq!(
            vec![bar, fred],
            tree.recurse(foo, |_| false)
                .collect::<Result<Vec<_>, _>>()?
        );

        Ok(())
    }

    #[test]
    fn recurse_path() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree(&fixture.tree)?;

        let (_, bar) = tree.add(Path::parse("foo/bar")?)?;
        let (_, baz) = tree.add(Path::parse("foo/bar/baz")?)?;
        let (_, qux) = tree.add(Path::parse("foo/bar/baz/qux")?)?;

        assert_eq!(
            vec![bar, baz, qux],
            tree.recurse_path(Path::parse("foo")?, |_| true)
                .collect::<Result<Vec<_>, _>>()?
        );

        assert_eq!(
            vec![baz, qux],
            tree.recurse_path(Path::parse("foo/bar")?, |_| true)
                .collect::<Result<Vec<_>, _>>()?
        );

        assert!(matches!(
            tree.recurse_path(Path::parse("doesnotexist")?, |_| true)
                .collect::<Result<Vec<_>, _>>(),
            Err(StorageError::NotFound)
        ));

        Ok(())
    }
}
