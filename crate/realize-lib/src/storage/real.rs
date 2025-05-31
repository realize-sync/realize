use std::{
    collections::HashMap,
    ffi::OsString,
    io,
    path::{self},
    sync::Arc,
};

use tokio::fs;

use crate::model::{self, Arena, LocalArena};

/// Resolve model paths into OS paths and back.
#[allow(async_fn_in_trait)]
pub trait PathResolver {
    /// Resolve a model path into an OS path.
    ///
    /// The OS path might not exist or some error might prevent it
    /// from being accessible.
    async fn resolve(&self, path: &model::Path) -> Result<path::PathBuf, io::Error>;

    /// Resolve an OS path into a model path.
    ///
    /// The OS path might not be part of the model, so reverse
    /// sometimes return None.
    fn reverse(&self, path: &path::Path) -> Option<model::Path>;
}

/// Local storage, for all [Arena]s.
#[derive(Clone, Debug)]
pub struct LocalStorage {
    map: Arc<HashMap<Arena, Arc<LocalArena>>>,
}

impl LocalStorage {
    /// Create a [LocalStorage] from an iterator of [LocalArena].
    pub fn new<T>(arenas: T) -> Self
    where
        T: IntoIterator<Item = LocalArena>,
    {
        let map = arenas
            .into_iter()
            .map(|e| (e.arena().clone(), Arc::new(e)))
            .collect();
        Self { map: Arc::new(map) }
    }

    /// Define a single local arena.
    pub fn single(arena: &Arena, path: &path::Path) -> Self {
        let dir = Arc::new(LocalArena::new(arena, path));
        let mut map = HashMap::new();
        map.insert(arena.clone(), dir);
        Self { map: Arc::new(map) }
    }

    /// Get a [LocalArena] if one exists for the given arena.
    pub fn get(&self, arena: &Arena) -> Option<&Arc<LocalArena>> {
        self.map.get(arena)
    }

    /// Build a path resolver for a read-only view of the arena.
    pub fn readonly_arena(&self, arena: &Arena) -> Option<ReadOnlyArenaStorage> {
        self.map
            .get(arena)
            .map(|local_arena| ReadOnlyArenaStorage::new(Arc::clone(local_arena)))
    }

    /// Build a path resolver for a writable view of the arena.
    pub fn writable_arena(&self, arena: &Arena) -> Option<WritableArenaStorage> {
        self.map
            .get(arena)
            .map(|local_arena| WritableArenaStorage::new(Arc::clone(local_arena)))
    }
}

pub struct ReadOnlyArenaStorage {
    arena: Arc<LocalArena>,
}

impl ReadOnlyArenaStorage {
    fn new(arena: Arc<LocalArena>) -> Self {
        Self { arena }
    }
}

impl PathResolver for ReadOnlyArenaStorage {
    async fn resolve(&self, path: &model::Path) -> Result<path::PathBuf, io::Error> {
        let full_path = to_full_path(self.arena.path(), path).ok_or(not_found())?;
        fs::metadata(&full_path).await?; // Make sure it exists

        Ok(full_path)
    }

    fn reverse(&self, actual: &path::Path) -> Option<model::Path> {
        if is_partial(actual) {
            return None;
        }
        let relative = pathdiff::diff_paths(actual, self.arena.path());
        if let Some(relative) = relative {
            if let Ok(path) = model::Path::from_real_path(&relative) {
                return Some(path);
            }
        }

        None
    }
}

pub struct WritableArenaStorage {
    arena: Arc<LocalArena>,
}

impl WritableArenaStorage {
    fn new(arena: Arc<LocalArena>) -> Self {
        Self { arena }
    }
}

impl PathResolver for WritableArenaStorage {
    async fn resolve(&self, path: &model::Path) -> Result<path::PathBuf, io::Error> {
        let full_path = to_full_path(self.arena.path(), path).ok_or(not_found())?;

        let partial_path = to_partial(&full_path);
        if fs::metadata(&partial_path).await.is_ok() {
            return Ok(partial_path);
        }

        fs::metadata(&full_path).await?; // Make sure it exists

        Ok(full_path)
    }

    fn reverse(&self, actual: &path::Path) -> Option<model::Path> {
        let relative = pathdiff::diff_paths(actual, self.arena.path());
        if let Some(mut relative) = relative {
            if let Some(non_partial) = non_partial_name(&relative) {
                relative.set_file_name(&OsString::from(non_partial.to_string()));
            }
            if let Ok(p) = model::Path::from_real_path(&relative) {
                return Some(p);
            }
        }

        None
    }
}

/// Check whether a path is partial
fn is_partial(path: &path::Path) -> bool {
    non_partial_name(path).is_some()
}

/// Convert a path to its partial equivalent.
fn to_partial(path: &path::Path) -> path::PathBuf {
    let mut path = path.to_path_buf();

    if let Some(filename) = path.file_name() {
        let mut part_filename = OsString::from(".");
        part_filename.push(filename);
        part_filename.push(OsString::from(".part"));
        path.set_file_name(part_filename);
    }

    path
}

/// Extract the non-partial name from a partial path.
///
/// Returns None unless given a valid partial path.
fn non_partial_name(path: &path::Path) -> Option<&str> {
    if let Some(filename) = path.file_name() {
        if let Some(name) = filename.to_str() {
            if name.starts_with(".") && name.ends_with(".part") {
                return Some(&name[1..name.len() - 5]);
            }
        }
    }

    None
}

/// Build a full path from a root and relative model path.
///
/// This function refuses to return partial paths, even if the model
/// path is trying to point to a partial path, as partial paths never
/// exist in this view of the Arena.
fn to_full_path(root: &path::Path, relative: &model::Path) -> Option<path::PathBuf> {
    let full_path = relative.within(root);
    if is_partial(&full_path) {
        return None;
    }

    Some(full_path)
}

fn not_found() -> std::io::Error {
    std::io::Error::from(std::io::ErrorKind::NotFound)
}

#[cfg(test)]
mod tests {
    use assert_fs::{
        prelude::{FileWriteStr as _, PathChild as _},
        TempDir,
    };

    use super::*;

    #[tokio::test]
    async fn resolve_in_readonly_arena() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let arena = &Arena::from("test");
        let storage = LocalStorage::single(arena, temp.path())
            .readonly_arena(arena)
            .ok_or(not_found())?;

        let simple_file = &model::Path::parse("foo.txt")?;
        assert!(matches!(
            storage.resolve(simple_file).await,
            Err(std::io::Error { .. })
        ));

        temp.child(".foo.txt.part").write_str("")?;
        assert!(matches!(
            storage.resolve(simple_file).await,
            Err(std::io::Error { .. })
        ));

        temp.child("foo.txt").write_str("")?;
        assert_eq!(
            temp.child("foo.txt").path().to_path_buf(),
            storage.resolve(simple_file).await?,
        );

        let file_in_subdir = &model::Path::parse("foo/bar.txt")?;
        assert!(matches!(
            storage.resolve(file_in_subdir).await,
            Err(std::io::Error { .. })
        ));

        temp.child("foo/.bar.txt.part").write_str("")?;
        assert!(matches!(
            storage.resolve(file_in_subdir).await,
            Err(std::io::Error { .. })
        ));

        temp.child("foo/bar.txt").write_str("")?;
        assert_eq!(
            temp.child("foo/bar.txt").path().to_path_buf(),
            storage.resolve(file_in_subdir).await?,
        );

        let hidden_file = &model::Path::parse(".foo/.bar.txt")?;
        assert!(matches!(
            storage.resolve(hidden_file).await,
            Err(std::io::Error { .. })
        ));

        temp.child(".foo/.bar.txt").write_str("")?;
        assert_eq!(
            temp.child(".foo/.bar.txt").path().to_path_buf(),
            storage.resolve(hidden_file).await?,
        );

        Ok(())
    }

    #[tokio::test]
    async fn ignore_model_path_with_partial_name_in_readonly_area() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let arena = &Arena::from("test");
        let storage = LocalStorage::single(arena, temp.path())
            .readonly_arena(arena)
            .ok_or(not_found())?;

        let simple_file = &model::Path::parse(".foo.txt.part")?;
        assert!(matches!(
            storage.resolve(simple_file).await,
            Err(std::io::Error { .. })
        ));

        temp.child(".foo.txt").write_str("")?;
        assert!(matches!(
            storage.resolve(simple_file).await,
            Err(std::io::Error { .. })
        ));

        let file_in_subdir = &model::Path::parse("foo/.bar.txt.part")?;
        assert!(matches!(
            storage.resolve(file_in_subdir).await,
            Err(std::io::Error { .. })
        ));

        temp.child("foo/.bar.txt.part").write_str("")?;
        assert!(matches!(
            storage.resolve(file_in_subdir).await,
            Err(std::io::Error { .. })
        ));

        Ok(())
    }

    #[tokio::test]
    async fn resolve_in_writable_arena() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let arena = &Arena::from("test");
        let storage = LocalStorage::single(arena, temp.path())
            .writable_arena(arena)
            .ok_or(not_found())?;

        let simple_file = &model::Path::parse("foo.txt")?;
        assert!(matches!(
            storage.resolve(simple_file).await,
            Err(std::io::Error { .. })
        ));

        temp.child("foo.txt").write_str("")?;
        assert_eq!(
            temp.child("foo.txt").path().to_path_buf(),
            storage.resolve(simple_file).await?,
        );

        temp.child(".foo.txt.part").write_str("")?;
        assert_eq!(
            temp.child(".foo.txt.part").path().to_path_buf(),
            storage.resolve(simple_file).await?,
        );

        let file_in_subdir = &model::Path::parse("foo/bar.txt")?;
        assert!(matches!(
            storage.resolve(file_in_subdir).await,
            Err(std::io::Error { .. })
        ));

        temp.child("foo/bar.txt").write_str("")?;
        assert_eq!(
            temp.child("foo/bar.txt").path().to_path_buf(),
            storage.resolve(file_in_subdir).await?,
        );

        temp.child("foo/.bar.txt.part").write_str("")?;
        assert_eq!(
            temp.child("foo/.bar.txt.part").path().to_path_buf(),
            storage.resolve(file_in_subdir).await?,
        );

        let hidden_file = &model::Path::parse(".foo/.bar.txt")?;
        assert!(matches!(
            storage.resolve(hidden_file).await,
            Err(std::io::Error { .. })
        ));

        temp.child(".foo/.bar.txt").write_str("")?;
        assert_eq!(
            temp.child(".foo/.bar.txt").path().to_path_buf(),
            storage.resolve(hidden_file).await?,
        );

        temp.child(".foo/..bar.txt.part").write_str("")?;
        assert_eq!(
            temp.child(".foo/..bar.txt.part").path().to_path_buf(),
            storage.resolve(hidden_file).await?,
        );

        Ok(())
    }

    #[tokio::test]
    async fn ignore_model_path_with_partial_name_in_writable_arena() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let arena = &Arena::from("test");
        let storage = LocalStorage::single(arena, temp.path())
            .writable_arena(arena)
            .ok_or(not_found())?;

        let simple_file = &model::Path::parse(".foo.txt.part")?;
        assert!(matches!(
            storage.resolve(simple_file).await,
            Err(std::io::Error { .. })
        ));

        temp.child(".foo.txt").write_str("")?;
        assert!(matches!(
            storage.resolve(simple_file).await,
            Err(std::io::Error { .. })
        ));

        let file_in_subdir = &model::Path::parse("foo/.bar.txt.part")?;
        assert!(matches!(
            storage.resolve(file_in_subdir).await,
            Err(std::io::Error { .. })
        ));

        temp.child("foo/.bar.txt.part").write_str("")?;
        assert!(matches!(
            storage.resolve(file_in_subdir).await,
            Err(std::io::Error { .. })
        ));

        Ok(())
    }

    #[test]
    fn reverse_in_readonly_arena() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let arena = &Arena::from("test");
        let storage = LocalStorage::single(arena, temp.path())
            .readonly_arena(arena)
            .ok_or(not_found())?;

        assert_eq!(
            Some(model::Path::parse("foo/bar")?),
            storage.reverse(temp.child("foo/bar").path())
        );

        assert_eq!(None, storage.reverse(temp.child("foo/.bar.part").path()));
        assert_eq!(None, storage.reverse(temp.child("../foo/bar").path()));
        assert_eq!(
            None,
            storage.reverse(path::Path::new("/some/other/dir/foo/bar"))
        );

        Ok(())
    }

    #[test]
    fn reverse_in_writable_arena() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let arena = &Arena::from("test");
        let storage = LocalStorage::single(arena, temp.path())
            .writable_arena(arena)
            .ok_or(not_found())?;

        assert_eq!(
            Some(model::Path::parse("foo/bar")?),
            storage.reverse(temp.child("foo/bar").path())
        );

        assert_eq!(
            Some(model::Path::parse("foo/bar")?),
            storage.reverse(temp.child("foo/.bar.part").path())
        );

        assert_eq!(None, storage.reverse(temp.child("../foo/bar").path()));

        assert_eq!(
            None,
            storage.reverse(path::Path::new("/some/other/dir/foo/bar"))
        );

        Ok(())
    }
}
