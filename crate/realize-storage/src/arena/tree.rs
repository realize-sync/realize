#![allow(dead_code)]

use super::db::BeforeCommit;
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
/// let readable_tree = txn.read_tree();
/// let inode = readable_tree.lookup(tree.root(), "mydir")?;
/// ```
///
/// or a write transaction:
///
/// ```ignore
/// let txn = db.begin_write()?;
/// let readable_tree = txn.read_tree();
/// let inode = readable_tree.lookup(tree.root(), "mydir")?;
/// ```
///
/// It can also be open for writing, given a write transaction:
///
/// ```ignore
/// let txn = db.begin_write()?;
/// let writable_tree = txn.write_tree();
/// readable_tree.add(path)?;
/// ```
pub(crate) struct Tree {
    arena: Arena,
    root: Inode,
    allocator: Arc<InodeAllocator>,
}

impl Tree {
    pub(crate) fn new(arena: Arena, allocator: Arc<InodeAllocator>) -> Result<Self, StorageError> {
        let root = allocator
            .arena_root(arena)
            .ok_or_else(|| StorageError::UnknownArena(arena))?;
        Ok(Self {
            arena,
            root,
            allocator,
        })
    }

    pub(crate) fn root(&self) -> Inode {
        self.root
    }
}

pub(crate) struct ReadableOpenTree<T>
where
    T: ReadableTable<(Inode, &'static str), Inode>,
{
    table: T,
    root: Inode,
}

impl<T> ReadableOpenTree<T>
where
    T: ReadableTable<(Inode, &'static str), Inode>,
{
    pub(crate) fn new(table: T, tree: &Tree) -> Self {
        Self {
            table,
            root: tree.root,
        }
    }
}

/// Operations supported on a tree open for read with [OpenTree::read_tree].
///
/// See also [TreeExt] for path-based operations.
pub(crate) trait TreeReadOperations {
    /// Returns the tree root inode.
    fn root(&self) -> Inode;

    /// Lookup a specific name in the given node.
    fn lookup_inode(&self, inode: Inode, name: &str) -> Result<Option<Inode>, StorageError>;

    /// List names under the given inode.
    fn readdir_inode(&self, inode: Inode) -> ReadDirIterator;

    /// Check whether the given inode exists
    fn inode_exists(&self, inode: Inode) -> Result<bool, StorageError>;

    /// Return the parent of the given inode, or None if not found.
    ///
    /// The parent of the root inode is None. All other existing inodes
    /// have a parent.
    ///
    /// See also [TreeExt::ancestors]
    fn parent(&self, inode: Inode) -> Result<Option<Inode>, StorageError>;

    /// Return the name {inode} can be found in in {parent_inode}.
    fn name_in(&self, parent_inode: Inode, inode: Inode) -> Result<Option<String>, StorageError>;
}

impl<T> TreeReadOperations for ReadableOpenTree<T>
where
    T: ReadableTable<(Inode, &'static str), Inode>,
{
    fn root(&self) -> Inode {
        self.root
    }
    fn lookup_inode(&self, inode: Inode, name: &str) -> Result<Option<Inode>, StorageError> {
        lookup(&self.table, inode, name)
    }
    fn readdir_inode(&self, inode: Inode) -> ReadDirIterator<'_> {
        readdir(&self.table, inode)
    }
    fn inode_exists(&self, inode: Inode) -> Result<bool, StorageError> {
        exists(&self.table, self.root, inode)
    }
    fn parent(&self, inode: Inode) -> Result<Option<Inode>, StorageError> {
        parent(&self.table, inode)
    }
    fn name_in(&self, parent_inode: Inode, inode: Inode) -> Result<Option<String>, StorageError> {
        name_in(&self.table, parent_inode, inode)
    }
}

impl<'a> TreeReadOperations for WritableOpenTree<'a> {
    fn root(&self) -> Inode {
        self.tree.root
    }
    fn lookup_inode(&self, inode: Inode, name: &str) -> Result<Option<Inode>, StorageError> {
        lookup(&self.table, inode, name)
    }
    fn readdir_inode(&self, inode: Inode) -> ReadDirIterator<'_> {
        readdir(&self.table, inode)
    }
    fn inode_exists(&self, inode: Inode) -> Result<bool, StorageError> {
        exists(&self.table, self.tree.root, inode)
    }
    fn parent(&self, inode: Inode) -> Result<Option<Inode>, StorageError> {
        parent(&self.table, inode)
    }
    fn name_in(&self, parent_inode: Inode, inode: Inode) -> Result<Option<String>, StorageError> {
        name_in(&self.table, parent_inode, inode)
    }
}

/// A location within the tree, which can be expressed as an inode or
/// a path.
///
/// Such locations are usually created automatically using into().
pub(crate) enum TreeLoc<'a> {
    Inode(Inode),
    PathRef(&'a Path),
    Path(Path),
}

impl<'a> TreeLoc<'a> {
    /// Return another location that might borrow from the original
    /// one.
    ///
    /// This is cheaper than a clone() for `TreeLoc::Path`.
    pub(crate) fn borrow(&self) -> TreeLoc<'_> {
        match self {
            TreeLoc::Inode(inode) => TreeLoc::Inode(*inode),
            TreeLoc::PathRef(path) => TreeLoc::PathRef(*path),
            TreeLoc::Path(path) => TreeLoc::PathRef(path),
        }
    }

    pub(crate) fn into_owned(self) -> TreeLoc<'static> {
        match self {
            TreeLoc::Inode(inode) => TreeLoc::Inode(inode),
            TreeLoc::PathRef(path) => TreeLoc::Path(path.clone()),
            TreeLoc::Path(path) => TreeLoc::Path(path),
        }
    }
}

impl From<Inode> for TreeLoc<'static> {
    fn from(value: Inode) -> Self {
        TreeLoc::Inode(value)
    }
}

impl From<Path> for TreeLoc<'static> {
    fn from(value: Path) -> Self {
        TreeLoc::Path(value)
    }
}

impl<'a> From<&'a Path> for TreeLoc<'a> {
    fn from(value: &'a Path) -> Self {
        TreeLoc::PathRef(value)
    }
}

/// Extend [TreeReadOperations] with convenience functions for working
/// with [Path].
pub(crate) trait TreeExt {
    /// Resolve the given tree location to an inode.
    fn resolve<'a, L: Into<TreeLoc<'a>>>(&self, loc: L) -> Result<Option<Inode>, StorageError>;

    /// Resolve the given tree location to an inode or return [Storage::NotFound].
    fn expect<'a, L: Into<TreeLoc<'a>>>(&self, loc: L) -> Result<Inode, StorageError>;

    /// Lookup the given path and return the most specific inode matching the
    /// path - which might just be the root if nothing matches.
    fn resolve_partial<'a, L: Into<TreeLoc<'a>>>(&self, path: L) -> Result<Inode, StorageError>;

    /// Lookup a specific entry in the given directory.
    fn lookup<'a, L: Into<TreeLoc<'a>>>(
        &self,
        loc: L,
        name: &str,
    ) -> Result<Option<Inode>, StorageError>;

    /// Read the content of the given directory.
    fn readdir<'a, L: Into<TreeLoc<'a>>>(&self, loc: L) -> ReadDirIterator<'_>;

    /// Checks whether a given location exists in the tree.
    fn exists<'a, L: Into<TreeLoc<'a>>>(&self, loc: L) -> Result<bool, StorageError>;

    /// Goes through whole tree, starting at inode and return it as an
    /// iterator (depth-first).
    ///
    /// Only enters the node for which `enter` returns true.
    fn recurse<'a, L, F>(
        &self,
        loc: L,
        enter: F,
    ) -> impl Iterator<Item = Result<Inode, StorageError>>
    where
        L: Into<TreeLoc<'a>>,
        F: FnMut(Inode) -> bool;

    /// Follow the inodes back up to the root and build a path.
    fn backtrack<'b, L: Into<TreeLoc<'b>>>(&self, loc: L) -> Result<Path, StorageError>;

    /// Return an iterator that returns the parent of inode and it
    /// parent until the root.
    fn ancestors(&self, inode: Inode) -> impl Iterator<Item = Result<Inode, StorageError>>;
}

impl<T: TreeReadOperations> TreeExt for T {
    fn expect<'a, L: Into<TreeLoc<'a>>>(&self, loc: L) -> Result<Inode, StorageError> {
        self.resolve(loc)?.ok_or(StorageError::NotFound)
    }

    fn resolve<'a, L: Into<TreeLoc<'a>>>(&self, loc: L) -> Result<Option<Inode>, StorageError> {
        let loc = loc.into();
        match loc {
            TreeLoc::Inode(inode) => Ok(Some(inode)),
            TreeLoc::PathRef(path) => resolve_path(self, path),
            TreeLoc::Path(path) => resolve_path(self, &path),
        }
    }

    fn resolve_partial<'a, L: Into<TreeLoc<'a>>>(&self, loc: L) -> Result<Inode, StorageError> {
        let loc = loc.into();
        match loc {
            TreeLoc::Inode(inode) => Ok(inode),
            TreeLoc::PathRef(path) => resolve_path_partial(self, path),
            TreeLoc::Path(path) => resolve_path_partial(self, &path),
        }
    }

    fn lookup<'a, L: Into<TreeLoc<'a>>>(
        &self,
        loc: L,
        name: &str,
    ) -> Result<Option<Inode>, StorageError> {
        self.lookup_inode(self.expect(loc)?, name)
    }

    fn readdir<'a, L: Into<TreeLoc<'a>>>(&self, loc: L) -> ReadDirIterator<'_> {
        match self.expect(loc) {
            Err(err) => ReadDirIterator::failed(err),
            Ok(inode) => self.readdir_inode(inode),
        }
    }

    fn exists<'a, L: Into<TreeLoc<'a>>>(&self, loc: L) -> Result<bool, StorageError> {
        Ok(match loc.into() {
            TreeLoc::Inode(inode) => self.inode_exists(inode)?,
            TreeLoc::Path(path) => resolve_path(self, &path)?.is_some(),
            TreeLoc::PathRef(path) => resolve_path(self, path)?.is_some(),
        })
    }

    fn recurse<'a, L, F>(
        &self,
        loc: L,
        enter: F,
    ) -> impl Iterator<Item = Result<Inode, StorageError>>
    where
        L: Into<TreeLoc<'a>>,
        F: FnMut(Inode) -> bool,
    {
        RecurseIterator {
            tree: self,
            enter,
            stack: VecDeque::from([self.readdir(loc)]),
        }
    }

    fn backtrack<'b, L: Into<TreeLoc<'b>>>(&self, loc: L) -> Result<Path, StorageError> {
        match loc.into() {
            TreeLoc::PathRef(path) => Ok(path.clone()),
            TreeLoc::Path(path) => Ok(path),
            TreeLoc::Inode(inode) => {
                let mut components = VecDeque::new();
                let mut current = inode;
                while let Some(parent) = self.parent(current)? {
                    if let Some(name) = self.name_in(parent, current)? {
                        components.push_front(name);
                    } else {
                        return Err(StorageError::InconsistentDatabase(format!(
                            "{current} not in its parent {parent}"
                        )));
                    }
                    current = parent;
                }
                if current != self.root() {
                    return Err(StorageError::NotFound);
                }

                Ok(Path::parse(components.make_contiguous().join("/"))?)
            }
        }
    }

    fn ancestors(&self, inode: Inode) -> impl Iterator<Item = Result<Inode, StorageError>> {
        Ancestors {
            tree: self,
            current: Some(inode),
        }
    }
}

struct Ancestors<'a, T>
where
    T: TreeReadOperations,
{
    tree: &'a T,
    current: Option<Inode>,
}

impl<'a, T> Iterator for Ancestors<'a, T>
where
    T: TreeReadOperations,
{
    type Item = Result<Inode, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current.take() {
            None => None,
            Some(inode) => match self.tree.parent(inode) {
                Err(err) => Some(Err(err)),
                Ok(Some(inode)) => {
                    self.current = Some(inode);

                    Some(Ok(inode))
                }
                Ok(None) => None,
            },
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
                            self.stack.push_back(self.tree.readdir_inode(inode));
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
    before_commit: &'a BeforeCommit,
    table: Table<'a, (Inode, &'static str), Inode>,
    refcount_table: Table<'a, Inode, u32>,
    current_inode_range_table: redb::Table<'a, (), (Inode, Inode)>,
    tree: &'a Tree,
}

impl<'a> WritableOpenTree<'a> {
    pub(crate) fn new(
        before_commit: &'a BeforeCommit,
        tree_table: Table<'a, (Inode, &'static str), Inode>,
        refcount_table: Table<'a, Inode, u32>,
        current_inode_range_table: redb::Table<'a, (), (Inode, Inode)>,
        tree: &'a Tree,
    ) -> Self {
        Self {
            before_commit,
            table: tree_table,
            refcount_table,
            current_inode_range_table,
            tree,
        }
    }

    /// Insert the given entry into the table and increment the
    /// reference count of the inode at `loc` if an entry was inserted
    /// (as opposed to an old entry being replaced).
    ///
    /// This function, together with the `remove` functions below are
    /// meant to help apply the rule "a reference is an entry in
    /// another table", so the reference count is incremented when an
    /// entry is really added and decremented when it is really
    /// removed.
    ///
    /// Return true if no entry existed before this call.
    pub(crate) fn insert_and_incref<'b, 'k, 'v, K, V, L>(
        &mut self,
        loc: L,
        table: &mut Table<'_, K, V>,
        key: impl std::borrow::Borrow<K::SelfType<'k>>,
        value: impl std::borrow::Borrow<V::SelfType<'v>>,
    ) -> Result<bool, StorageError>
    where
        L: Into<TreeLoc<'b>>,
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        let inode = match loc.into() {
            TreeLoc::Inode(inode) => inode,
            TreeLoc::PathRef(path) => self.setup(path)?,
            TreeLoc::Path(path) => self.setup(path)?,
        };
        if table.insert(key, value)?.is_none() {
            self.incref(inode)?;
            return Ok(true);
        }

        Ok(false)
    }

    /// Remove the entry `key` in `table`, releasing any reference
    /// held for `inode`.
    ///
    /// The entry must have been created by an `insert` method in
    /// [WritableOpenTree]. Using this method to remove guarantees
    /// reference counting is correct in that a strong reference is
    /// held to the name/inode pair in the tree as long as the entry
    /// `key` in `table` exists.
    ///
    /// Return true if an entry existed before this call and was
    /// removed.
    pub(crate) fn remove_and_decref<'b, 'k, K, V, L>(
        &mut self,
        loc: L,
        table: &mut Table<'_, K, V>,
        key: impl std::borrow::Borrow<K::SelfType<'k>>,
    ) -> Result<bool, StorageError>
    where
        L: Into<TreeLoc<'b>>,
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        if let Some(inode) = self.resolve(loc)? {
            if table.remove(key)?.is_some() {
                self.decref(inode)?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Remove the given inode and all of its children from the given table.
    ///
    /// The entries are removed from `table` if `pred` returns true
    /// and their reference count is decreased in the tree.
    ///
    /// `keygen` generates keys for `table` from an inode.
    pub fn remove_recursive_and_decref_checked<'b, 'k, K, V, FK, KB, F, L>(
        &mut self,
        start: L,
        table: &mut Table<'_, K, V>,
        keygen: FK,
        mut pred: F,
    ) -> Result<(), StorageError>
    where
        L: Into<TreeLoc<'b>>,
        K: redb::Key + 'static,
        V: redb::Value + 'static,
        FK: Fn(Inode) -> KB,
        KB: std::borrow::Borrow<K::SelfType<'k>>,
        F: for<'f> FnMut(Inode, V::SelfType<'f>) -> Result<bool, StorageError>,
    {
        let mut decrement = vec![];
        let start = match self.resolve(start)? {
            Some(inode) => inode,
            None => {
                return Ok(());
            }
        };
        for inode in std::iter::once(Ok(start)).chain(self.recurse(start, |_| true)) {
            let inode = inode?;
            let remove = match table.get((keygen)(inode))? {
                None => false,
                Some(v) => (pred)(inode, v.value())?,
            };
            if remove {
                if table.remove((keygen)(inode))?.is_some() {
                    decrement.push(inode);
                }
            }
        }
        for inode in decrement {
            self.decref(inode)?;
        }

        Ok(())
    }

    /// Remove the given inode and all of its children from the given table.
    ///
    /// The entries are removed from `table` if `pred` returns true
    /// and their reference count is decreased in the tree.
    ///
    /// `keygen` generates keys for `table` from an inode.
    pub fn remove_recursive_and_decref<'b, 'k, K, V, FK, KB, L>(
        &mut self,
        start: L,
        table: &mut Table<'_, K, V>,
        keygen: FK,
    ) -> Result<(), StorageError>
    where
        L: Into<TreeLoc<'b>>,
        K: redb::Key + 'static,
        V: redb::Value + 'static,
        FK: Fn(Inode) -> KB,
        KB: std::borrow::Borrow<K::SelfType<'k>>,
    {
        let mut decrement = vec![];
        let start = match self.resolve(start)? {
            Some(inode) => inode,
            None => {
                return Ok(());
            }
        };
        for inode in std::iter::once(Ok(start)).chain(self.recurse(start, |_| true)) {
            let inode = inode?;
            if table.remove((keygen)(inode))?.is_some() {
                decrement.push(inode);
            }
        }
        for inode in decrement {
            self.decref(inode)?;
        }

        Ok(())
    }

    /// Get or create a mapping for all parts of the given path.
    ///
    /// A new inode allocated by this function has a reference count
    /// of 0 and is only valid until the end of the transaction until
    /// its reference count is incremented by one of the insert
    /// methods on [WritableOpenTree].
    pub(crate) fn setup<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        loc: L,
    ) -> Result<Inode, StorageError> {
        match loc.into() {
            TreeLoc::Inode(inode) => Ok(inode),
            TreeLoc::Path(path) => self.setup_path(&path),
            TreeLoc::PathRef(path) => self.setup_path(path),
        }
    }

    fn setup_path(&mut self, path: &Path) -> Result<Inode, StorageError> {
        let (inode, added) = self.add_path(path.as_ref())?;
        if added {
            self.before_commit.add(move |txn| {
                // Check refcount and delete the entry if it reaches
                // 0. Note that the allocated inode is lost, so it's
                // not a no-op.
                txn.write_tree()?.check_refcount(inode)
            });
        }

        Ok(inode)
    }

    /// Get or create a mapping for `name` in `parent_inode`
    ///
    /// A new inode allocated by this function has a reference count
    /// of 0 and is only valid until the end of the transaction until
    /// its reference count is incremented by one of the insert
    /// methods on [WritableOpenTree].
    pub(crate) fn setup_name(
        &mut self,
        parent_inode: Inode,
        name: &str,
    ) -> Result<Inode, StorageError> {
        let (inode, added) = self.add_name(parent_inode, name)?;

        if added {
            self.before_commit.add(move |txn| {
                // Check refcount and delete the entry if it reaches
                // 0. Note that the allocated inode is lost, so it's
                // not a no-op.
                txn.write_tree()?.check_refcount(inode)
            });
        }
        Ok(inode)
    }

    /// Create a mapping for `path` but don't increment reference
    /// count.
    ///
    /// Caller must make sure of incrementing reference count or call
    /// setup instead.
    ///
    /// Return (inode, added), with added true if the leaf inode is new.
    fn add_path(&mut self, path: &Path) -> Result<(Inode, bool), StorageError> {
        let path = path.as_ref();
        let mut current = (self.tree.root, false);
        for component in Path::components(Some(path)) {
            current = self.add_name(current.0, component)?;
        }

        Ok(current)
    }

    /// Create a mapping for `name` in `parent_inode` but don't increment reference count.
    ///
    /// Caller must make sure of incrementing reference count.
    ///
    /// Return (inode, added), with added true if the inode is new.
    fn add_name(&mut self, parent_inode: Inode, name: &str) -> Result<(Inode, bool), StorageError> {
        match get_inode(&self.table, parent_inode, name)? {
            Some(inode) => {
                log::debug!("found {name} in {parent_inode} -> {inode}");

                Ok((inode, false))
            }
            None => {
                let new_inode = self.allocate_inode()?;
                self.add_inode(parent_inode, new_inode, name)?;
                log::debug!("add {name} in {parent_inode} -> {new_inode}");

                Ok((new_inode, true))
            }
        }
    }

    /// Increment reference count on the given inode.
    ///
    /// This is called automatically by the insert methods in this
    /// class. Incrementing a reference should be tied to insertion in
    /// another table and removing to removal from another table.
    fn incref(&mut self, inode: Inode) -> Result<(), StorageError> {
        let mut refcount = self
            .refcount_table
            .get(inode)?
            .map(|v| v.value())
            .unwrap_or(0);
        if refcount == 0 {
            log::debug!("Inode {inode} got its first reference");
        }

        refcount += 1;
        self.refcount_table.insert(inode, refcount)?;

        Ok(())
    }

    /// Decrement reference count on the given inode.
    ///
    /// This is called automatically by the remove methods in this
    /// class. Incrementing a reference should be tied to insertion in
    /// another table and removing to removal from another table.
    fn decref(&mut self, inode: Inode) -> Result<(), StorageError> {
        let mut refcount = self
            .refcount_table
            .get(inode)?
            .map(|v| v.value())
            .unwrap_or(0);
        refcount = refcount.saturating_sub(1);

        if refcount > 0 {
            self.refcount_table.insert(inode, refcount)?;
        } else {
            self.remove_mapping(inode)?;
        }

        Ok(())
    }

    fn check_refcount(&mut self, inode: Inode) -> Result<(), StorageError> {
        let refcount = self
            .refcount_table
            .get(inode)?
            .map(|v| v.value())
            .unwrap_or(0);
        if refcount == 0 {
            log::warn!("Refcount of {inode} was never increased; cleaning up.");
            self.remove_mapping(inode)?;
        }

        Ok(())
    }

    /// Remove mapping of `inode` from its parent.
    ///
    /// This must only be called after checking the inode refcount.
    fn remove_mapping(&mut self, inode: Inode) -> Result<(), StorageError> {
        self.refcount_table.remove(inode)?;
        let parent = self.table.remove((inode, ".."))?.map(|v| v.value());
        if let Some(parent) = parent {
            self.table
                .retain_in(inode_range(parent), |_, v| v != inode)?;
            self.decref(parent)?;
        }

        Ok(())
    }

    fn allocate_inode(&mut self) -> Result<Inode, StorageError> {
        self.tree
            .allocator
            .allocate_arena_inode(&mut self.current_inode_range_table, self.tree.arena)
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

fn name_in(
    tree_table: &impl ReadableTable<(Inode, &'static str), Inode>,
    parent_inode: Inode,
    inode: Inode,
) -> Result<Option<String>, StorageError> {
    for v in tree_table.range(inode_range(parent_inode))? {
        let v = v?;
        let (key, value) = v;
        if value.value() == inode {
            let (_, name) = key.value();
            if name == ".." || name == "." {
                return Ok(None);
            }
            return Ok(Some(name.to_string()));
        }
    }

    Ok(None)
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

fn resolve_path(
    tree: &impl TreeReadOperations,
    path: &Path,
) -> Result<Option<Inode>, StorageError> {
    let mut current = tree.root();
    for component in Path::components(Some(path)) {
        if let Some(e) = tree.lookup_inode(current, component)? {
            current = e
        } else {
            log::debug!("not found: {component} in {current}");
            return Ok(None);
        };
    }

    Ok(Some(current))
}

fn resolve_path_partial(
    tree: &impl TreeReadOperations,
    path: &Path,
) -> Result<Inode, StorageError> {
    let path = path.as_ref();
    let mut current = tree.root();
    for component in Path::components(Some(path)) {
        if let Some(e) = tree.lookup_inode(current, component)? {
            current = e
        } else {
            log::debug!("not found: {component} in {current}");
            break;
        };
    }

    Ok(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arena::db::ArenaDatabase;
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
    fn read_txn_with_tree_compiles() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_read()?;
        let tree = txn.read_tree()?;
        assert_eq!(None, tree.lookup_inode(tree.root(), "test")?,);

        Ok(())
    }

    #[test]
    fn write_txn_with_tree_compiles() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let tree = txn.read_tree()?;
        assert_eq!(None, tree.lookup_inode(tree.root(), "test")?);

        Ok(())
    }

    #[test]
    fn write_tree_compiles() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let _ = tree.setup(&Path::parse("test")?);

        Ok(())
    }

    #[test]
    fn add_depth0() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let node = tree.setup(&Path::parse("test")?)?;

        assert_eq!(Some(node), tree.lookup_inode(tree.root(), "test")?);

        Ok(())
    }

    #[test]
    fn add_depth1() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let bar = tree.setup(&Path::parse("foo/bar")?)?;

        let foo = tree.lookup_inode(tree.root(), "foo")?.unwrap();
        assert_eq!(bar, tree.lookup_inode(foo, "bar")?.unwrap());

        Ok(())
    }

    #[test]
    fn add_same() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let first = tree.setup(&Path::parse("foo/bar")?)?;
        let second = tree.setup(&Path::parse("foo/bar")?)?;
        assert_eq!(first, second);

        Ok(())
    }

    #[test]
    fn add_deeper() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let bar = tree.setup(&Path::parse("foo/bar")?)?;
        let baz = tree.setup(&Path::parse("foo/bar/baz")?)?;

        let foo = tree.lookup_inode(tree.root(), "foo")?.unwrap();
        assert_eq!(bar, tree.lookup_inode(foo, "bar")?.unwrap());
        assert_eq!(baz, tree.lookup_inode(bar, "baz")?.unwrap());

        Ok(())
    }

    #[test]
    fn lookup() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let baz = tree.setup(&Path::parse("foo/bar/baz")?)?;
        let qux = tree.setup(&Path::parse("foo/bar/baz/qux")?)?;

        let foo = tree.lookup_inode(tree.root(), "foo")?.unwrap();
        let bar = tree.lookup_inode(foo, "bar")?.unwrap();
        assert_eq!(baz, tree.lookup_inode(bar, "baz")?.unwrap());
        assert_eq!(qux, tree.lookup_inode(baz, "qux")?.unwrap());

        Ok(())
    }

    #[test]
    fn lookup_path() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let baz = tree.setup(&Path::parse("foo/bar/baz")?)?;
        let qux = tree.setup(&Path::parse("foo/bar/baz/qux")?)?;
        let foo = tree.lookup_inode(tree.root(), "foo")?.unwrap();
        let bar = tree.lookup_inode(foo, "bar")?.unwrap();

        assert_eq!(foo, tree.resolve(Path::parse("foo")?)?.unwrap());
        assert_eq!(bar, tree.resolve(Path::parse("foo/bar")?)?.unwrap());
        assert_eq!(baz, tree.resolve(Path::parse("foo/bar/baz")?)?.unwrap());
        assert_eq!(qux, tree.resolve(Path::parse("foo/bar/baz/qux")?)?.unwrap());

        Ok(())
    }

    #[test]
    fn lookup_path_not_found() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        tree.setup(&Path::parse("foo/bar")?)?;

        assert_eq!(
            None,
            tree.resolve(Path::parse("foo/bar/notfound1/notfound2")?)?
        );
        assert_eq!(None, tree.resolve(Path::parse("foo/bar/notfound")?)?);
        assert_eq!(None, tree.resolve(Path::parse("foo/notfound")?)?);
        assert_eq!(None, tree.resolve(Path::parse("notfound")?)?);

        Ok(())
    }

    #[test]
    fn lookup_partial_path() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let baz = tree.setup(&Path::parse("foo/bar/baz")?)?;
        tree.setup(&Path::parse("foo/bar/baz/qux")?)?;
        let foo = tree.lookup_inode(tree.root(), "foo")?.unwrap();
        let bar = tree.lookup_inode(foo, "bar")?.unwrap();

        assert_eq!(baz, tree.resolve_partial(Path::parse("foo/bar/baz")?)?);
        assert_eq!(baz, tree.resolve_partial(Path::parse("foo/bar/baz/quux")?)?);
        assert_eq!(bar, tree.resolve_partial(Path::parse("foo/bar/burgle")?)?);
        assert_eq!(
            bar,
            tree.resolve_partial(Path::parse("foo/bar/waldo/fred")?)?
        );
        assert_eq!(
            tree.root(),
            tree.resolve_partial(Path::parse("waldo/fred")?)?
        );

        Ok(())
    }

    #[test]
    fn readdir() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let foo = tree.setup(&Path::parse("foo")?)?;
        let bar = tree.setup(&Path::parse("foo/bar")?)?;
        let baz = tree.setup(&Path::parse("foo/bar/baz")?)?;
        let qux = tree.setup(&Path::parse("foo/bar/qux")?)?;
        let quux = tree.setup(&Path::parse("foo/bar/quux")?)?;

        assert_eq!(
            Some(vec![("foo".to_string(), foo)]),
            tree.readdir_inode(tree.root())
                .collect::<Result<Vec<_>, _>>()
                .ok()
        );
        assert_eq!(
            Some(vec![("bar".to_string(), bar)]),
            tree.readdir_inode(foo).collect::<Result<Vec<_>, _>>().ok()
        );

        assert_eq!(
            Some(vec![
                ("baz".to_string(), baz),
                ("quux".to_string(), quux),
                ("qux".to_string(), qux),
            ]),
            tree.readdir_inode(bar).collect::<Result<Vec<_>, _>>().ok()
        );

        Ok(())
    }

    #[test]
    fn backtrack() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let foo = tree.setup(&Path::parse("foo")?)?;
        let bar = tree.setup(&Path::parse("foo/bar")?)?;
        let baz = tree.setup(&Path::parse("foo/bar/baz")?)?;

        assert_eq!(Path::parse("foo/bar/baz")?, tree.backtrack(baz)?);
        assert_eq!(Path::parse("foo/bar")?, tree.backtrack(bar)?);
        assert_eq!(Path::parse("foo")?, tree.backtrack(foo)?);
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
        let mut tree = txn.write_tree()?;
        assert!(tree.inode_exists(tree.root())?);
        assert!(!tree.inode_exists(Inode(999))?);

        let foo = tree.setup(&Path::parse("foo")?)?;
        let bar = tree.setup(&Path::parse("foo/bar")?)?;
        assert!(tree.inode_exists(foo)?);
        assert!(tree.inode_exists(bar)?);

        Ok(())
    }

    #[test]
    fn parent() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        assert!(tree.inode_exists(tree.root())?);
        assert!(!tree.inode_exists(Inode(999))?);

        let foo = tree.setup(&Path::parse("foo")?)?;
        let bar = tree.setup(&Path::parse("foo/bar")?)?;
        let baz = tree.setup(&Path::parse("foo/bar/baz")?)?;
        assert_eq!(Some(tree.root()), tree.parent(foo)?);
        assert_eq!(Some(foo), tree.parent(bar)?);
        assert_eq!(Some(bar), tree.parent(baz)?);
        assert_eq!(None, tree.parent(Inode(999))?);
        assert_eq!(None, tree.parent(tree.root())?);

        Ok(())
    }

    #[test]
    fn ancestors() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        assert!(tree.inode_exists(tree.root())?);
        assert!(!tree.inode_exists(Inode(999))?);

        let foo = tree.setup(&Path::parse("foo")?)?;
        let bar = tree.setup(&Path::parse("foo/bar")?)?;
        let baz = tree.setup(&Path::parse("foo/bar/baz")?)?;
        assert_eq!(
            vec![bar, foo, tree.root()],
            tree.ancestors(baz).collect::<Result<Vec<_>, _>>()?
        );
        assert_eq!(
            vec![foo, tree.root()],
            tree.ancestors(bar).collect::<Result<Vec<_>, _>>()?
        );
        assert_eq!(
            vec![tree.root()],
            tree.ancestors(foo).collect::<Result<Vec<_>, _>>()?
        );
        assert!(tree.ancestors(tree.root()).next().is_none());

        Ok(())
    }

    #[test]
    fn name_in() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        assert!(tree.inode_exists(tree.root())?);
        assert!(!tree.inode_exists(Inode(999))?);

        let foo = tree.setup(&Path::parse("foo")?)?;
        let bar = tree.setup(&Path::parse("foo/bar")?)?;
        let baz = tree.setup(&Path::parse("foo/bar/baz")?)?;
        assert_eq!(Some("foo".to_string()), tree.name_in(tree.root(), foo)?);
        assert_eq!(Some("bar".to_string()), tree.name_in(foo, bar)?);
        assert_eq!(Some("baz".to_string()), tree.name_in(bar, baz)?);
        assert_eq!(None, tree.name_in(foo, baz)?);
        assert_eq!(None, tree.name_in(foo, tree.root())?);

        Ok(())
    }

    #[test]
    fn refcount() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let bar = tree.setup(&Path::parse("foo/bar")?)?;
        let foo = tree.lookup_inode(tree.root(), "foo")?.unwrap();
        let baz = tree.setup(&Path::parse("foo/bar/baz")?)?;
        let qux = tree.setup(&Path::parse("foo/qux")?)?;

        tree.incref(bar)?;
        tree.incref(baz)?;
        tree.incref(qux)?;

        // decref qux deletes it, but not foo
        tree.decref(qux)?;
        assert!(tree.inode_exists(foo)?);
        assert!(tree.inode_exists(bar)?);
        assert!(tree.inode_exists(baz)?);
        assert!(!tree.inode_exists(qux)?);

        // decref bar does not delete it, because it contains baz
        tree.decref(bar)?;
        assert!(tree.inode_exists(foo)?);
        assert!(tree.inode_exists(bar)?);
        assert!(tree.inode_exists(baz)?);

        // decref baz deletes bar and baz, but not foo
        tree.decref(baz)?;
        assert!(!tree.inode_exists(foo)?);
        assert!(!tree.inode_exists(bar)?);
        assert!(!tree.inode_exists(baz)?);

        Ok(())
    }

    #[test]
    fn setup() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let bar: Inode;
        let foo: Inode;
        let qux: Inode;
        let baz: Inode;
        {
            let mut tree = txn.write_tree()?;
            bar = tree.setup(&Path::parse("foo/bar")?)?;
            qux = tree.setup(&Path::parse("baz/qux")?)?;
            foo = tree.lookup(tree.root(), "foo")?.unwrap();
            baz = tree.lookup(tree.root(), "baz")?.unwrap();

            // refcount is incremented for qux, but not bar, so foo
            // and bar will be removed before committing the
            // transaction.
            tree.incref(qux)?;
        }
        txn.commit()?;

        let txn = fixture.db.begin_read()?;
        let tree = txn.read_tree()?;
        assert!(tree.exists(qux)?);
        assert!(tree.exists(baz)?);

        assert!(!tree.exists(bar)?);
        assert!(!tree.exists(foo)?);

        Ok(())
    }

    #[test]
    fn setup_name() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let bar: Inode;
        let foo: Inode;
        let qux: Inode;
        let baz: Inode;
        {
            let mut tree = txn.write_tree()?;
            foo = tree.setup_name(tree.root(), "foo")?;
            bar = tree.setup_name(foo, "bar")?;
            baz = tree.setup_name(tree.root(), "baz")?;
            qux = tree.setup_name(baz, "qux")?;

            // refcount is incremented for qux, but not bar, so foo
            // and bar will be removed before committing the
            // transaction.
            tree.incref(qux)?;
        }
        txn.commit()?;

        let txn = fixture.db.begin_read()?;
        let tree = txn.read_tree()?;
        assert!(tree.exists(qux)?);
        assert!(tree.exists(baz)?);

        assert!(!tree.exists(bar)?);
        assert!(!tree.exists(foo)?);

        Ok(())
    }

    #[test]
    fn incref() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let baz = tree.setup(Path::parse("foo/bar/baz")?)?;
        let bar = tree.expect(Path::parse("foo/bar")?)?;

        tree.incref(baz)?;
        tree.incref(bar)?;
        tree.incref(bar)?;
        tree.incref(bar)?;

        // decref baz deletes baz, but not bar
        tree.decref(baz)?;
        assert!(tree.inode_exists(bar)?);
        assert!(!tree.inode_exists(baz)?);

        // now that baz is gone, it still takes 3 decref to delete bar
        tree.decref(bar)?;
        assert!(tree.inode_exists(bar)?);
        tree.decref(bar)?;
        assert!(tree.inode_exists(bar)?);
        tree.decref(bar)?;
        assert!(!tree.inode_exists(bar)?);

        Ok(())
    }

    #[test]
    fn recurse() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;

        let foo = tree.setup(Path::parse("foo")?)?;
        let bar = tree.setup(Path::parse("foo/bar")?)?;
        let baz = tree.setup(Path::parse("foo/bar/baz")?)?;
        let qux = tree.setup(Path::parse("foo/bar/baz/qux")?)?;
        let corge = tree.setup(Path::parse("foo/bar/corge")?)?;
        let graply = tree.setup(Path::parse("foo/bar/corge/graply")?)?;
        let grault = tree.setup(Path::parse("foo/bar/corge/grault")?)?;
        let quux = tree.setup(Path::parse("foo/bar/quux")?)?;
        let waldo = tree.setup(Path::parse("foo/bar/waldo")?)?;
        let fred = tree.setup(Path::parse("foo/fred")?)?;
        tree.setup(Path::parse("xyzzy")?)?; // not in foo, ignored

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
        let mut tree = txn.write_tree()?;

        let bar = tree.setup(Path::parse("foo/bar")?)?;
        let baz = tree.setup(Path::parse("foo/bar/baz")?)?;
        let qux = tree.setup(Path::parse("foo/bar/baz/qux")?)?;

        assert_eq!(
            vec![bar, baz, qux],
            tree.recurse(Path::parse("foo")?, |_| true)
                .collect::<Result<Vec<_>, _>>()?
        );

        assert_eq!(
            vec![baz, qux],
            tree.recurse(Path::parse("foo/bar")?, |_| true)
                .collect::<Result<Vec<_>, _>>()?
        );

        assert!(matches!(
            tree.recurse(Path::parse("doesnotexist")?, |_| true)
                .collect::<Result<Vec<_>, _>>(),
            Err(StorageError::NotFound)
        ));

        Ok(())
    }
}
