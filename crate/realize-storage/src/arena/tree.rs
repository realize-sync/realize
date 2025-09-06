#![allow(dead_code)]

use super::db::BeforeCommit;
use crate::PathIdAllocator;
use crate::{PathId, StorageError};
use realize_types::{Arena, Path};
use redb::{ReadableTable, Table};
use std::borrow::{Borrow, Cow};
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
/// let pathid = readable_tree.lookup(tree.root(), "mydir")?;
/// ```
///
/// or a write transaction:
///
/// ```ignore
/// let txn = db.begin_write()?;
/// let readable_tree = txn.read_tree();
/// let pathid = readable_tree.lookup(tree.root(), "mydir")?;
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
    root: PathId,
    allocator: Arc<PathIdAllocator>,
}

impl Tree {
    pub(crate) fn new(arena: Arena, allocator: Arc<PathIdAllocator>) -> Result<Self, StorageError> {
        let root = allocator
            .arena_root(arena)
            .ok_or_else(|| StorageError::UnknownArena(arena))?;
        Ok(Self {
            arena,
            root,
            allocator,
        })
    }

    pub(crate) fn root(&self) -> PathId {
        self.root
    }
}

pub(crate) struct ReadableOpenTree<T>
where
    T: ReadableTable<(PathId, &'static str), PathId>,
{
    table: T,
    root: PathId,
    arena: Arena,
}

impl<T> ReadableOpenTree<T>
where
    T: ReadableTable<(PathId, &'static str), PathId>,
{
    pub(crate) fn new(table: T, tree: &Tree) -> Self {
        Self {
            table,
            root: tree.root,
            arena: tree.arena,
        }
    }
}

/// Operations supported on a tree open for read with [OpenTree::read_tree].
///
/// See also [TreeExt] for path-based operations.
pub(crate) trait TreeReadOperations {
    /// Returns the tree root pathid.
    fn root(&self) -> PathId;

    /// Lookup a specific name in the given node.
    fn lookup_pathid(&self, pathid: PathId, name: &str) -> Result<Option<PathId>, StorageError>;

    /// List names under the given pathid.
    fn readdir_pathid(&self, pathid: PathId) -> ReadDirIterator<'_>;

    /// Check whether the given pathid exists
    fn pathid_exists(&self, pathid: PathId) -> Result<bool, StorageError>;

    /// Return the parent of the given pathid, or None if not found.
    ///
    /// The parent of the root pathid is None. All other existing pathids
    /// have a parent.
    ///
    /// See also [TreeExt::ancestors]
    fn parent(&self, pathid: PathId) -> Result<Option<PathId>, StorageError>;

    /// Return the name {pathid} can be found in in {parent_pathid}.
    fn name_in(
        &self,
        parent_pathid: PathId,
        pathid: PathId,
    ) -> Result<Option<String>, StorageError>;

    /// Returns the arena this tree belongs to.
    fn arena(&self) -> Arena;
}

impl<T> TreeReadOperations for ReadableOpenTree<T>
where
    T: ReadableTable<(PathId, &'static str), PathId>,
{
    fn root(&self) -> PathId {
        self.root
    }
    fn lookup_pathid(&self, pathid: PathId, name: &str) -> Result<Option<PathId>, StorageError> {
        lookup(&self.table, pathid, name)
    }
    fn readdir_pathid(&self, pathid: PathId) -> ReadDirIterator<'_> {
        readdir(&self.table, pathid)
    }
    fn pathid_exists(&self, pathid: PathId) -> Result<bool, StorageError> {
        exists(&self.table, self.root, pathid)
    }
    fn parent(&self, pathid: PathId) -> Result<Option<PathId>, StorageError> {
        parent(&self.table, pathid)
    }
    fn name_in(
        &self,
        parent_pathid: PathId,
        pathid: PathId,
    ) -> Result<Option<String>, StorageError> {
        name_in(&self.table, parent_pathid, pathid)
    }
    fn arena(&self) -> Arena {
        self.arena
    }
}

impl<'a> TreeReadOperations for WritableOpenTree<'a> {
    fn root(&self) -> PathId {
        self.tree.root
    }
    fn lookup_pathid(&self, pathid: PathId, name: &str) -> Result<Option<PathId>, StorageError> {
        lookup(&self.table, pathid, name)
    }
    fn readdir_pathid(&self, pathid: PathId) -> ReadDirIterator<'_> {
        readdir(&self.table, pathid)
    }
    fn pathid_exists(&self, pathid: PathId) -> Result<bool, StorageError> {
        exists(&self.table, self.tree.root, pathid)
    }
    fn parent(&self, pathid: PathId) -> Result<Option<PathId>, StorageError> {
        parent(&self.table, pathid)
    }
    fn name_in(
        &self,
        parent_pathid: PathId,
        pathid: PathId,
    ) -> Result<Option<String>, StorageError> {
        name_in(&self.table, parent_pathid, pathid)
    }
    fn arena(&self) -> Arena {
        self.tree.arena
    }
}

/// A location within the tree, which can be expressed as an pathid or
/// a path.
///
/// Such locations are usually created automatically using into().
pub(crate) enum TreeLoc<'a> {
    PathId(PathId),
    PathRef(&'a Path),
    Path(Path),
    PathIdAndName(PathId, Cow<'a, str>),
}

impl<'a> TreeLoc<'a> {
    /// Return another location that might borrow from the original
    /// one.
    ///
    /// This is cheaper than a clone() for `TreeLoc::Path`.
    pub(crate) fn borrow(&self) -> TreeLoc<'_> {
        match self {
            TreeLoc::PathId(pathid) => TreeLoc::PathId(*pathid),
            TreeLoc::PathRef(path) => TreeLoc::PathRef(*path),
            TreeLoc::Path(path) => TreeLoc::PathRef(path),
            TreeLoc::PathIdAndName(pathid, str) => {
                TreeLoc::PathIdAndName(*pathid, Cow::Borrowed(str.borrow()))
            }
        }
    }

    pub(crate) fn into_owned(self) -> TreeLoc<'static> {
        match self {
            TreeLoc::PathId(pathid) => TreeLoc::PathId(pathid),
            TreeLoc::PathRef(path) => TreeLoc::Path(path.clone()),
            TreeLoc::Path(path) => TreeLoc::Path(path),
            TreeLoc::PathIdAndName(pathid, str) => {
                TreeLoc::PathIdAndName(pathid, Cow::Owned(str.into_owned()))
            }
        }
    }
}

impl From<PathId> for TreeLoc<'static> {
    fn from(value: PathId) -> Self {
        TreeLoc::PathId(value)
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

impl<'a> From<(PathId, &'a str)> for TreeLoc<'a> {
    fn from(value: (PathId, &'a str)) -> Self {
        TreeLoc::PathIdAndName(value.0, Cow::from(value.1))
    }
}

impl<'a> From<(PathId, &'a String)> for TreeLoc<'a> {
    fn from(value: (PathId, &'a String)) -> Self {
        TreeLoc::PathIdAndName(value.0, Cow::from(value.1))
    }
}
impl From<(PathId, String)> for TreeLoc<'static> {
    fn from(value: (PathId, String)) -> Self {
        TreeLoc::PathIdAndName(value.0, Cow::from(value.1))
    }
}

/// Extend [TreeReadOperations] with convenience functions for working
/// with [Path].
pub(crate) trait TreeExt {
    /// Resolve the given tree location to an pathid.
    fn resolve<'a, L: Into<TreeLoc<'a>>>(&self, loc: L) -> Result<Option<PathId>, StorageError>;

    /// Resolve the given tree location to an pathid or return [Storage::NotFound].
    fn expect<'a, L: Into<TreeLoc<'a>>>(&self, loc: L) -> Result<PathId, StorageError>;

    /// Lookup the given path and return the most specific pathid matching the
    /// path - which might just be the root if nothing matches.
    fn resolve_partial<'a, L: Into<TreeLoc<'a>>>(&self, path: L) -> Result<PathId, StorageError>;

    /// Lookup a specific entry in the given directory.
    fn lookup<'a, L: Into<TreeLoc<'a>>>(
        &self,
        loc: L,
        name: &str,
    ) -> Result<Option<PathId>, StorageError>;

    /// Read the content of the given directory.
    fn readdir<'a, L: Into<TreeLoc<'a>>>(&self, loc: L) -> ReadDirIterator<'_>;

    /// Checks whether a given location exists in the tree.
    fn exists<'a, L: Into<TreeLoc<'a>>>(&self, loc: L) -> Result<bool, StorageError>;

    /// Goes through whole tree, starting at pathid and return it as an
    /// iterator (depth-first).
    ///
    /// Only enters the node for which `enter` returns true.
    fn recurse<'a, L, F>(
        &self,
        loc: L,
        enter: F,
    ) -> impl Iterator<Item = Result<PathId, StorageError>>
    where
        L: Into<TreeLoc<'a>>,
        F: FnMut(PathId) -> bool;

    /// Follow the pathids back up to the root and build a path.
    ///
    /// Not all pathids can be turned into a path, even valid ones.
    fn backtrack<'b, L: Into<TreeLoc<'b>>>(&self, loc: L) -> Result<Option<Path>, StorageError>;

    /// Return an iterator that returns the parent of pathid and it
    /// parent until the root.
    fn ancestors(&self, pathid: PathId) -> impl Iterator<Item = Result<PathId, StorageError>>;
}

impl<T: TreeReadOperations> TreeExt for T {
    fn expect<'a, L: Into<TreeLoc<'a>>>(&self, loc: L) -> Result<PathId, StorageError> {
        self.resolve(loc)?.ok_or(StorageError::NotFound)
    }

    fn resolve<'a, L: Into<TreeLoc<'a>>>(&self, loc: L) -> Result<Option<PathId>, StorageError> {
        let loc = loc.into();
        match loc {
            TreeLoc::PathId(pathid) => Ok(Some(pathid)),
            TreeLoc::PathRef(path) => resolve_path(self, path),
            TreeLoc::Path(path) => resolve_path(self, &path),
            TreeLoc::PathIdAndName(pathid, name) => self.lookup_pathid(pathid, name.as_ref()),
        }
    }

    fn resolve_partial<'a, L: Into<TreeLoc<'a>>>(&self, loc: L) -> Result<PathId, StorageError> {
        let loc = loc.into();
        match loc {
            TreeLoc::PathId(pathid) => Ok(pathid),
            TreeLoc::PathRef(path) => resolve_path_partial(self, path),
            TreeLoc::Path(path) => resolve_path_partial(self, &path),
            TreeLoc::PathIdAndName(pathid, name) => {
                match self.lookup_pathid(pathid, name.as_ref())? {
                    None => Ok(pathid),
                    Some(pathid) => Ok(pathid),
                }
            }
        }
    }

    fn lookup<'a, L: Into<TreeLoc<'a>>>(
        &self,
        loc: L,
        name: &str,
    ) -> Result<Option<PathId>, StorageError> {
        self.lookup_pathid(self.expect(loc)?, name)
    }

    fn readdir<'a, L: Into<TreeLoc<'a>>>(&self, loc: L) -> ReadDirIterator<'_> {
        match self.expect(loc) {
            Err(err) => ReadDirIterator::failed(err),
            Ok(pathid) => self.readdir_pathid(pathid),
        }
    }

    fn exists<'a, L: Into<TreeLoc<'a>>>(&self, loc: L) -> Result<bool, StorageError> {
        Ok(match loc.into() {
            TreeLoc::PathId(pathid) => self.pathid_exists(pathid)?,
            TreeLoc::Path(path) => resolve_path(self, &path)?.is_some(),
            TreeLoc::PathRef(path) => resolve_path(self, path)?.is_some(),
            TreeLoc::PathIdAndName(pathid, cow) => self.lookup(pathid, cow.as_ref())?.is_some(),
        })
    }

    fn recurse<'a, L, F>(
        &self,
        loc: L,
        enter: F,
    ) -> impl Iterator<Item = Result<PathId, StorageError>>
    where
        L: Into<TreeLoc<'a>>,
        F: FnMut(PathId) -> bool,
    {
        RecurseIterator {
            tree: self,
            enter,
            stack: VecDeque::from([self.readdir(loc)]),
        }
    }

    fn backtrack<'b, L: Into<TreeLoc<'b>>>(&self, loc: L) -> Result<Option<Path>, StorageError> {
        match loc.into() {
            TreeLoc::PathRef(path) => Ok(Some(path.clone())),
            TreeLoc::Path(path) => Ok(Some(path)),
            TreeLoc::PathIdAndName(pathid, name) => match self.backtrack(pathid)? {
                None => Ok(Some(Path::parse(name)?)),
                Some(path) => Ok(Some(Path::parse(&format!("{}/{}", path, name))?)),
            },
            TreeLoc::PathId(pathid) => {
                let mut components = VecDeque::new();
                let mut current = pathid;
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
                if current != self.root() || components.is_empty() {
                    return Ok(None);
                }

                Ok(Some(Path::parse(components.make_contiguous().join("/"))?))
            }
        }
    }

    fn ancestors(&self, pathid: PathId) -> impl Iterator<Item = Result<PathId, StorageError>> {
        Ancestors {
            tree: self,
            current: Some(pathid),
        }
    }
}

struct Ancestors<'a, T>
where
    T: TreeReadOperations,
{
    tree: &'a T,
    current: Option<PathId>,
}

impl<'a, T> Iterator for Ancestors<'a, T>
where
    T: TreeReadOperations,
{
    type Item = Result<PathId, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current.take() {
            None => None,
            Some(pathid) => match self.tree.parent(pathid) {
                Err(err) => Some(Err(err)),
                Ok(Some(pathid)) => {
                    self.current = Some(pathid);

                    Some(Ok(pathid))
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
    F: FnMut(PathId) -> bool,
{
    tree: &'a T,
    enter: F,
    stack: VecDeque<ReadDirIterator<'a>>,
}

impl<'a, T, F> Iterator for RecurseIterator<'a, T, F>
where
    T: TreeReadOperations,
    F: FnMut(PathId) -> bool,
{
    type Item = Result<PathId, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(iter) = self.stack.back_mut() {
            while let Some(elt) = iter.next() {
                match elt {
                    Err(err) => {
                        return Some(Err(err));
                    }
                    Ok((_, pathid)) => {
                        if (self.enter)(pathid) {
                            self.stack.push_back(self.tree.readdir_pathid(pathid));
                        }
                        return Some(Ok(pathid));
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
    table: Table<'a, (PathId, &'static str), PathId>,
    refcount_table: Table<'a, PathId, u32>,
    current_pathid_range_table: redb::Table<'a, (), (PathId, PathId)>,
    tree: &'a Tree,
}

impl<'a> WritableOpenTree<'a> {
    pub(crate) fn new(
        before_commit: &'a BeforeCommit,
        tree_table: Table<'a, (PathId, &'static str), PathId>,
        refcount_table: Table<'a, PathId, u32>,
        current_pathid_range_table: redb::Table<'a, (), (PathId, PathId)>,
        tree: &'a Tree,
    ) -> Self {
        Self {
            before_commit,
            table: tree_table,
            refcount_table,
            current_pathid_range_table,
            tree,
        }
    }

    /// Insert the given entry into the table and increment the
    /// reference count of the pathid at `loc` if an entry was inserted
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
        let pathid = match loc.into() {
            TreeLoc::PathId(pathid) => pathid,
            TreeLoc::PathRef(path) => self.setup(path)?,
            TreeLoc::Path(path) => self.setup(path)?,
            TreeLoc::PathIdAndName(pathid, name) => self.setup_name(pathid, name.as_ref())?,
        };
        if table.insert(key, value)?.is_none() {
            self.incref(pathid)?;
            return Ok(true);
        }

        Ok(false)
    }

    /// Remove the entry `key` in `table`, releasing any reference
    /// held for `pathid`.
    ///
    /// The entry must have been created by an `insert` method in
    /// [WritableOpenTree]. Using this method to remove guarantees
    /// reference counting is correct in that a strong reference is
    /// held to the name/pathid pair in the tree as long as the entry
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
        if let Some(pathid) = self.resolve(loc)? {
            if table.remove(key)?.is_some() {
                self.decref(pathid)?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Remove the given pathid and all of its children from the given table.
    ///
    /// The entries are removed from `table` if `pred` returns true
    /// and their reference count is decreased in the tree.
    ///
    /// `keygen` generates keys for `table` from an pathid.
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
        FK: Fn(PathId) -> KB,
        KB: std::borrow::Borrow<K::SelfType<'k>>,
        F: for<'f> FnMut(PathId, V::SelfType<'f>) -> Result<bool, StorageError>,
    {
        let mut decrement = vec![];
        let start = match self.resolve(start)? {
            Some(pathid) => pathid,
            None => {
                return Ok(());
            }
        };
        for pathid in std::iter::once(Ok(start)).chain(self.recurse(start, |_| true)) {
            let pathid = pathid?;
            let remove = match table.get((keygen)(pathid))? {
                None => false,
                Some(v) => (pred)(pathid, v.value())?,
            };
            if remove {
                if table.remove((keygen)(pathid))?.is_some() {
                    decrement.push(pathid);
                }
            }
        }
        for pathid in decrement {
            self.decref(pathid)?;
        }

        Ok(())
    }

    /// Remove the given pathid and all of its children from the given table.
    ///
    /// The entries are removed from `table` if `pred` returns true
    /// and their reference count is decreased in the tree.
    ///
    /// `keygen` generates keys for `table` from an pathid.
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
        FK: Fn(PathId) -> KB,
        KB: std::borrow::Borrow<K::SelfType<'k>>,
    {
        let mut decrement = vec![];
        let start = match self.resolve(start)? {
            Some(pathid) => pathid,
            None => {
                return Ok(());
            }
        };
        for pathid in std::iter::once(Ok(start)).chain(self.recurse(start, |_| true)) {
            let pathid = pathid?;
            if table.remove((keygen)(pathid))?.is_some() {
                decrement.push(pathid);
            }
        }
        for pathid in decrement {
            self.decref(pathid)?;
        }

        Ok(())
    }

    /// Get or create a mapping for all parts of the given path.
    ///
    /// A new pathid allocated by this function has a reference count
    /// of 0 and is only valid until the end of the transaction until
    /// its reference count is incremented by one of the insert
    /// methods on [WritableOpenTree].
    pub(crate) fn setup<'b, L: Into<TreeLoc<'b>>>(
        &mut self,
        loc: L,
    ) -> Result<PathId, StorageError> {
        match loc.into() {
            TreeLoc::PathId(pathid) => Ok(pathid),
            TreeLoc::Path(path) => self.setup_path(&path),
            TreeLoc::PathRef(path) => self.setup_path(path),
            TreeLoc::PathIdAndName(pathid, name) => self.setup_name(pathid, name.as_ref()),
        }
    }

    fn setup_path(&mut self, path: &Path) -> Result<PathId, StorageError> {
        let (pathid, added) = self.add_path(path.as_ref())?;
        if added {
            log::debug!("[{}] \"{path}\" = pathid {pathid}", self.tree.arena);
            self.before_commit.add(move |txn| {
                // Check refcount and delete the entry if it reaches
                // 0. Note that the allocated pathid is lost, so it's
                // not a no-op.
                txn.write_tree()?.check_refcount(pathid)
            });
        }

        Ok(pathid)
    }

    /// Get or create a mapping for `name` in `parent_pathid`
    ///
    /// A new pathid allocated by this function has a reference count
    /// of 0 and is only valid until the end of the transaction until
    /// its reference count is incremented by one of the insert
    /// methods on [WritableOpenTree].
    pub(crate) fn setup_name(
        &mut self,
        parent_pathid: PathId,
        name: &str,
    ) -> Result<PathId, StorageError> {
        let (pathid, added) = self.add_name(parent_pathid, name)?;

        if added {
            log::debug!(
                "[{}] \"{name}\" = pathid {pathid}, in parent pathid {parent_pathid} ",
                self.tree.arena
            );
            self.before_commit.add(move |txn| {
                // Check refcount and delete the entry if it reaches
                // 0. Note that the allocated pathid is lost, so it's
                // not a no-op.
                txn.write_tree()?.check_refcount(pathid)
            });
        }
        Ok(pathid)
    }

    /// Create a mapping for `path` but don't increment reference
    /// count.
    ///
    /// Caller must make sure of incrementing reference count or call
    /// setup instead.
    ///
    /// Return (pathid, added), with added true if the leaf pathid is new.
    fn add_path(&mut self, path: &Path) -> Result<(PathId, bool), StorageError> {
        let path = path.as_ref();
        let mut current = (self.tree.root, false);
        for component in Path::components(Some(path)) {
            current = self.add_name(current.0, component)?;
        }

        Ok(current)
    }

    /// Create a mapping for `name` in `parent_pathid` but don't increment reference count.
    ///
    /// Caller must make sure of incrementing reference count.
    ///
    /// Return (pathid, added), with added true if the pathid is new.
    fn add_name(
        &mut self,
        parent_pathid: PathId,
        name: &str,
    ) -> Result<(PathId, bool), StorageError> {
        match get_pathid(&self.table, parent_pathid, name)? {
            Some(pathid) => Ok((pathid, false)),
            None => {
                let new_pathid = self.allocate_pathid()?;
                self.add_pathid(parent_pathid, new_pathid, name)?;

                Ok((new_pathid, true))
            }
        }
    }

    /// Increment reference count on the given pathid.
    ///
    /// This is called automatically by the insert methods in this
    /// class. Incrementing a reference should be tied to insertion in
    /// another table and removing to removal from another table.
    fn incref(&mut self, pathid: PathId) -> Result<(), StorageError> {
        let mut refcount = self
            .refcount_table
            .get(pathid)?
            .map(|v| v.value())
            .unwrap_or(0);
        if refcount == 0 {
            log::trace!(
                "[{}] PathId {pathid} got its first reference",
                self.tree.arena
            );
        }

        refcount += 1;
        self.refcount_table.insert(pathid, refcount)?;

        Ok(())
    }

    /// Decrement reference count on the given pathid.
    ///
    /// This is called automatically by the remove methods in this
    /// class. Incrementing a reference should be tied to insertion in
    /// another table and removing to removal from another table.
    fn decref(&mut self, pathid: PathId) -> Result<(), StorageError> {
        let mut refcount = self
            .refcount_table
            .get(pathid)?
            .map(|v| v.value())
            .unwrap_or(0);
        refcount = refcount.saturating_sub(1);

        if refcount > 0 {
            self.refcount_table.insert(pathid, refcount)?;
        } else {
            self.remove_mapping(pathid)?;
        }

        Ok(())
    }

    fn check_refcount(&mut self, pathid: PathId) -> Result<(), StorageError> {
        let refcount = self
            .refcount_table
            .get(pathid)?
            .map(|v| v.value())
            .unwrap_or(0);
        if refcount == 0 {
            log::warn!(
                "[{}] Refcount of {pathid} was never increased; Cleaning up.",
                self.tree.arena
            );
            self.remove_mapping(pathid)?;
        }

        Ok(())
    }

    /// Remove mapping of `pathid` from its parent.
    ///
    /// This must only be called after checking the pathid refcount.
    fn remove_mapping(&mut self, pathid: PathId) -> Result<(), StorageError> {
        log::trace!(
            "[{}] PathId {pathid} lost its last reference; Cleaning up.",
            self.tree.arena
        );
        self.refcount_table.remove(pathid)?;
        let parent = self.table.remove((pathid, ".."))?.map(|v| v.value());
        if let Some(parent) = parent {
            self.table
                .retain_in(pathid_range(parent), |_, v| v != pathid)?;
            self.decref(parent)?;
        }

        Ok(())
    }

    fn allocate_pathid(&mut self) -> Result<PathId, StorageError> {
        self.tree
            .allocator
            .allocate_arena_pathid(&mut self.current_pathid_range_table, self.tree.arena)
    }

    fn add_pathid(
        &mut self,
        parent_pathid: PathId,
        new_pathid: PathId,
        name: &str,
    ) -> Result<(), StorageError> {
        self.table.insert((parent_pathid, name), new_pathid)?;
        self.table.insert((new_pathid, ".."), parent_pathid)?;
        self.incref(parent_pathid)?;

        Ok(())
    }
}

fn lookup(
    tree_table: &impl ReadableTable<(PathId, &'static str), PathId>,
    pathid: PathId,
    name: &str,
) -> Result<Option<PathId>, StorageError> {
    Ok(tree_table.get((pathid, name))?.map(|v| v.value()))
}

fn exists(
    tree_table: &impl ReadableTable<(PathId, &'static str), PathId>,
    root_pathid: PathId,
    pathid: PathId,
) -> Result<bool, StorageError> {
    if root_pathid == pathid {
        return Ok(true);
    }

    Ok(tree_table.get((pathid, ".."))?.is_some())
}

fn name_in(
    tree_table: &impl ReadableTable<(PathId, &'static str), PathId>,
    parent_pathid: PathId,
    pathid: PathId,
) -> Result<Option<String>, StorageError> {
    for v in tree_table.range(pathid_range(parent_pathid))? {
        let v = v?;
        let (key, value) = v;
        if value.value() == pathid {
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
    tree_table: &impl ReadableTable<(PathId, &'static str), PathId>,
    pathid: PathId,
) -> Result<Option<PathId>, StorageError> {
    Ok(tree_table.get((pathid, ".."))?.map(|v| v.value()))
}

fn readdir(
    tree_table: &impl ReadableTable<(PathId, &'static str), PathId>,
    pathid: PathId,
) -> ReadDirIterator<'_> {
    let range = tree_table
        .range(pathid_range(pathid))
        .map_err(|e| StorageError::from(e));

    ReadDirIterator { iter: Some(range) }
}

/// Iterator returned by [TreeReadOperations::readdir]
pub(crate) struct ReadDirIterator<'a> {
    iter: Option<Result<redb::Range<'a, (PathId, &'static str), PathId>, StorageError>>,
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
    type Item = Result<(String, PathId), StorageError>;

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
                                let pathid = v.1.value();
                                return Some(Ok((name.to_string(), pathid)));
                            }
                        }
                    }
                }

                None
            }
        }
    }
}

fn get_pathid(
    table: &impl redb::ReadableTable<(PathId, &'static str), PathId>,
    parent_pathid: PathId,
    name: &str,
) -> Result<Option<PathId>, StorageError> {
    match table.get((parent_pathid, name))? {
        None => Ok(None),
        Some(e) => Ok(Some(e.value())),
    }
}

/// Builds a range that covers all entries for the given pathid.
fn pathid_range(pathid: PathId) -> std::ops::Range<(PathId, &'static str)> {
    (pathid, "")..(pathid.plus(1), "")
}

fn resolve_path(
    tree: &impl TreeReadOperations,
    path: &Path,
) -> Result<Option<PathId>, StorageError> {
    let mut current = tree.root();
    for component in Path::components(Some(path)) {
        if let Some(e) = tree.lookup_pathid(current, component)? {
            current = e
        } else {
            return Ok(None);
        };
    }

    Ok(Some(current))
}

fn resolve_path_partial(
    tree: &impl TreeReadOperations,
    path: &Path,
) -> Result<PathId, StorageError> {
    let path = path.as_ref();
    let mut current = tree.root();
    for component in Path::components(Some(path)) {
        if let Some(e) = tree.lookup_pathid(current, component)? {
            current = e
        } else {
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
            let db =
                ArenaDatabase::for_testing_single_arena(arena, std::path::Path::new("/dev/null"))?;

            Ok(Self { db })
        }
    }

    #[test]
    fn read_txn_with_tree_compiles() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_read()?;
        let tree = txn.read_tree()?;
        assert_eq!(None, tree.lookup_pathid(tree.root(), "test")?,);

        Ok(())
    }

    #[test]
    fn write_txn_with_tree_compiles() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let tree = txn.read_tree()?;
        assert_eq!(None, tree.lookup_pathid(tree.root(), "test")?);

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

        assert_eq!(Some(node), tree.lookup_pathid(tree.root(), "test")?);

        Ok(())
    }

    #[test]
    fn add_depth1() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let bar = tree.setup(&Path::parse("foo/bar")?)?;

        let foo = tree.lookup_pathid(tree.root(), "foo")?.unwrap();
        assert_eq!(bar, tree.lookup_pathid(foo, "bar")?.unwrap());

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

        let foo = tree.lookup_pathid(tree.root(), "foo")?.unwrap();
        assert_eq!(bar, tree.lookup_pathid(foo, "bar")?.unwrap());
        assert_eq!(baz, tree.lookup_pathid(bar, "baz")?.unwrap());

        Ok(())
    }

    #[test]
    fn lookup() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let baz = tree.setup(&Path::parse("foo/bar/baz")?)?;
        let qux = tree.setup(&Path::parse("foo/bar/baz/qux")?)?;

        let foo = tree.lookup_pathid(tree.root(), "foo")?.unwrap();
        let bar = tree.lookup_pathid(foo, "bar")?.unwrap();
        assert_eq!(baz, tree.lookup_pathid(bar, "baz")?.unwrap());
        assert_eq!(qux, tree.lookup_pathid(baz, "qux")?.unwrap());

        Ok(())
    }

    #[test]
    fn lookup_path() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        let baz = tree.setup(&Path::parse("foo/bar/baz")?)?;
        let qux = tree.setup(&Path::parse("foo/bar/baz/qux")?)?;
        let foo = tree.lookup_pathid(tree.root(), "foo")?.unwrap();
        let bar = tree.lookup_pathid(foo, "bar")?.unwrap();

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
        let foo = tree.lookup_pathid(tree.root(), "foo")?.unwrap();
        let bar = tree.lookup_pathid(foo, "bar")?.unwrap();

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
            tree.readdir_pathid(tree.root())
                .collect::<Result<Vec<_>, _>>()
                .ok()
        );
        assert_eq!(
            Some(vec![("bar".to_string(), bar)]),
            tree.readdir_pathid(foo).collect::<Result<Vec<_>, _>>().ok()
        );

        assert_eq!(
            Some(vec![
                ("baz".to_string(), baz),
                ("quux".to_string(), quux),
                ("qux".to_string(), qux),
            ]),
            tree.readdir_pathid(bar).collect::<Result<Vec<_>, _>>().ok()
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

        assert_eq!(Path::parse("foo/bar/baz")?, tree.backtrack(baz)?.unwrap());
        assert_eq!(Path::parse("foo/bar")?, tree.backtrack(bar)?.unwrap());
        assert_eq!(Path::parse("foo")?, tree.backtrack(foo)?.unwrap());

        // Roots cannot be turned into a path, as empty paths are invalid.
        assert!(tree.backtrack(tree.root())?.is_none());
        assert!(tree.backtrack(PathId(1))?.is_none());

        // Invalid pathids are reported as None, not NotFound
        assert!(tree.backtrack(PathId(999))?.is_none());

        Ok(())
    }

    #[test]
    fn exists() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        assert!(tree.pathid_exists(tree.root())?);
        assert!(!tree.pathid_exists(PathId(999))?);

        let foo = tree.setup(&Path::parse("foo")?)?;
        let bar = tree.setup(&Path::parse("foo/bar")?)?;
        assert!(tree.pathid_exists(foo)?);
        assert!(tree.pathid_exists(bar)?);

        Ok(())
    }

    #[test]
    fn parent() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        assert!(tree.pathid_exists(tree.root())?);
        assert!(!tree.pathid_exists(PathId(999))?);

        let foo = tree.setup(&Path::parse("foo")?)?;
        let bar = tree.setup(&Path::parse("foo/bar")?)?;
        let baz = tree.setup(&Path::parse("foo/bar/baz")?)?;
        assert_eq!(Some(tree.root()), tree.parent(foo)?);
        assert_eq!(Some(foo), tree.parent(bar)?);
        assert_eq!(Some(bar), tree.parent(baz)?);
        assert_eq!(None, tree.parent(PathId(999))?);
        assert_eq!(None, tree.parent(tree.root())?);

        Ok(())
    }

    #[test]
    fn ancestors() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let mut tree = txn.write_tree()?;
        assert!(tree.pathid_exists(tree.root())?);
        assert!(!tree.pathid_exists(PathId(999))?);

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
        assert!(tree.pathid_exists(tree.root())?);
        assert!(!tree.pathid_exists(PathId(999))?);

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
        let foo = tree.lookup_pathid(tree.root(), "foo")?.unwrap();
        let baz = tree.setup(&Path::parse("foo/bar/baz")?)?;
        let qux = tree.setup(&Path::parse("foo/qux")?)?;

        tree.incref(bar)?;
        tree.incref(baz)?;
        tree.incref(qux)?;

        // decref qux deletes it, but not foo
        tree.decref(qux)?;
        assert!(tree.pathid_exists(foo)?);
        assert!(tree.pathid_exists(bar)?);
        assert!(tree.pathid_exists(baz)?);
        assert!(!tree.pathid_exists(qux)?);

        // decref bar does not delete it, because it contains baz
        tree.decref(bar)?;
        assert!(tree.pathid_exists(foo)?);
        assert!(tree.pathid_exists(bar)?);
        assert!(tree.pathid_exists(baz)?);

        // decref baz deletes bar and baz, but not foo
        tree.decref(baz)?;
        assert!(!tree.pathid_exists(foo)?);
        assert!(!tree.pathid_exists(bar)?);
        assert!(!tree.pathid_exists(baz)?);

        Ok(())
    }

    #[test]
    fn setup() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let txn = fixture.db.begin_write()?;
        let bar: PathId;
        let foo: PathId;
        let qux: PathId;
        let baz: PathId;
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
        let bar: PathId;
        let foo: PathId;
        let qux: PathId;
        let baz: PathId;
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
        assert!(tree.pathid_exists(bar)?);
        assert!(!tree.pathid_exists(baz)?);

        // now that baz is gone, it still takes 3 decref to delete bar
        tree.decref(bar)?;
        assert!(tree.pathid_exists(bar)?);
        tree.decref(bar)?;
        assert!(tree.pathid_exists(bar)?);
        tree.decref(bar)?;
        assert!(!tree.pathid_exists(bar)?);

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
            tree.recurse(foo, |pathid| pathid != baz && pathid != corge)
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
