use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

/// A set of path and associated data shared between peers.
///
/// This types is an identifier. An arena is identified by its name,
/// which must be unique and known to all peers that want to share
/// data.
#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Arena {
    name: String,
}
impl From<String> for Arena {
    fn from(value: String) -> Self {
        Self { name: value }
    }
}
impl From<&str> for Arena {
    fn from(value: &str) -> Self {
        Self {
            name: value.to_string(),
        }
    }
}
impl std::fmt::Display for Arena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name)
    }
}
impl Arena {
    pub fn as_str(&self) -> &str {
        &self.name
    }
    pub fn into_string(self) -> String {
        self.name
    }
}

/// Local location of a shared [Arena].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalArena {
    arena: Arena,
    path: PathBuf,
}

impl LocalArena {
    /// Set local location of the given arena to the given path.
    pub fn new(arena: &Arena, path: &Path) -> Self {
        Self {
            arena: arena.clone(),
            path: path.to_path_buf(),
        }
    }

    /// LocalArena ID, as found in Service calls.
    pub fn arena(&self) -> &Arena {
        &self.arena
    }

    /// Local directory path that correspond to the ID.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Local definition of multiple [Arena]s.
#[derive(Clone, Debug)]
pub struct LocalArenas {
    map: Arc<HashMap<Arena, Arc<LocalArena>>>,
}

impl LocalArenas {
    /// Create a [LocalArenas] from an iterator of [LocalArena].
    pub fn new<T>(arenas: T) -> Self
    where
        T: IntoIterator<Item = LocalArena>,
    {
        let map = arenas
            .into_iter()
            .map(|e| (e.arena.clone(), Arc::new(e)))
            .collect();
        Self { map: Arc::new(map) }
    }

    /// Define a single local arena.
    pub fn single(id: &Arena, path: &Path) -> Self {
        let dir = Arc::new(LocalArena::new(id, path));
        let mut map = HashMap::new();
        map.insert(id.clone(), dir);
        Self { map: Arc::new(map) }
    }

    /// Get the [LocalArena] for the give arena.
    pub fn get(&self, arena: &Arena) -> Option<&Arc<LocalArena>> {
        self.map.get(arena)
    }

    /// Get the local path assigned to the given arena.
    pub fn get_path(&self, arena: &Arena) -> Option<&Path> {
        self.map.get(arena).map(|e| e.path())
    }
}
