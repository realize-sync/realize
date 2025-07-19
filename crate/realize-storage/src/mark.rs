#![allow(dead_code)] // work in progress

use realize_types::Path;
use std::collections::HashMap;

use crate::config::ArenaConfig;

/// A mark that can be applied to files and directories in an arena.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum Mark {
    /// Files marked as "own" belong in the real. They should be moved into the arena root as regular files.
    Own,
    /// Files marked as "watch" belong in the unreal. They should be left in the cache and are subject to normal LRU rules.
    Watch,
    /// Files marked as "keep" belong in the unreal. They should be left in the cache and are unconditionally kept.
    Keep,
}

impl Default for Mark {
    fn default() -> Self {
        Mark::Watch
    }
}

/// Tracks marks hierarchically by paths in an arena.
///
/// Paths can be files or directories, and marks are inherited from the most specific
/// path that has a mark set (file > directory > arena default).
pub struct PathMarks {
    /// The default mark for the entire arena
    default_mark: Mark,
    /// Path-specific marks, mapping from path to mark
    path_marks: HashMap<Path, Mark>,
}

impl PathMarks {
    /// Create a new PathMarks with the given default mark.
    pub fn new(default_mark: Mark) -> Self {
        Self {
            default_mark,
            path_marks: HashMap::new(),
        }
    }

    /// Create a PathMarks from an ArenaConfig.
    pub fn from_config(config: &ArenaConfig) -> Self {
        Self::new(config.mark)
    }

    /// Get the mark for a specific path.
    ///
    /// Returns the most specific mark available:
    /// 1. File mark if the path itself has a mark set
    /// 2. Directory mark if a parent directory has a mark set
    /// 3. Default arena mark otherwise
    pub fn for_path(&self, path: &Path) -> Mark {
        // First check if the path itself has a mark
        if let Some(mark) = self.path_marks.get(path) {
            return *mark;
        }

        // Then check if any parent directory has a mark
        let mut current = path.clone();
        while let Some(parent) = current.parent() {
            if let Some(mark) = self.path_marks.get(&parent) {
                return *mark;
            }
            current = parent;
        }

        // Fall back to the default mark
        self.default_mark
    }

    /// Set a mark for a specific path.
    pub fn set_mark(&mut self, path: &Path, mark: Mark) {
        self.path_marks.insert(path.clone(), mark);
    }

    /// Unset a mark for a specific path.
    pub fn unset_mark(&mut self, path: &Path) {
        self.path_marks.remove(path);
    }

    /// Get a reference to the path marks map for testing.
    #[cfg(test)]
    pub fn path_marks(&self) -> &HashMap<Path, Mark> {
        &self.path_marks
    }
}

impl Default for PathMarks {
    fn default() -> Self {
        Self::new(Mark::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_for_path_file_mark() {
        let mut path_marks = PathMarks::new(Mark::Watch);
        let file_path = Path::parse("dir/file.txt").unwrap();

        // Set a specific mark for the file
        path_marks.set_mark(&file_path, Mark::Own);

        // Should return the file's mark
        assert_eq!(path_marks.for_path(&file_path), Mark::Own);
    }

    #[test]
    fn test_for_path_directory_mark() {
        let mut path_marks = PathMarks::new(Mark::Watch);
        let dir_path = Path::parse("dir").unwrap();
        let file_path = Path::parse("dir/file.txt").unwrap();

        // Set a mark for the directory
        path_marks.set_mark(&dir_path, Mark::Keep);

        // Should return the directory's mark for files in that directory
        assert_eq!(path_marks.for_path(&file_path), Mark::Keep);
    }

    #[test]
    fn test_for_path_arena_default() {
        let path_marks = PathMarks::new(Mark::Own);
        let file_path = Path::parse("dir/file.txt").unwrap();

        // Should return the arena default mark when no specific marks are set
        assert_eq!(path_marks.for_path(&file_path), Mark::Own);
    }

    #[test]
    fn test_for_path_inheritance_hierarchy() {
        let mut path_marks = PathMarks::new(Mark::Watch);
        let root_dir = Path::parse("root").unwrap();
        let sub_dir = Path::parse("root/subdir").unwrap();
        let file_path = Path::parse("root/subdir/file.txt").unwrap();

        // Set different marks at different levels
        path_marks.set_mark(&root_dir, Mark::Keep);
        path_marks.set_mark(&sub_dir, Mark::Own);

        // File should inherit from its immediate parent (subdir)
        assert_eq!(path_marks.for_path(&file_path), Mark::Own);

        // Remove the subdir mark, should inherit from root
        path_marks.unset_mark(&sub_dir);
        assert_eq!(path_marks.for_path(&file_path), Mark::Keep);

        // Remove the root mark, should use arena default
        path_marks.unset_mark(&root_dir);
        assert_eq!(path_marks.for_path(&file_path), Mark::Watch);
    }

    #[test]
    fn test_set_and_unset_mark() {
        let mut path_marks = PathMarks::new(Mark::Watch);
        let file_path = Path::parse("test.txt").unwrap();

        // Initially no specific mark
        assert_eq!(path_marks.for_path(&file_path), Mark::Watch);
        assert_eq!(path_marks.path_marks().len(), 0);

        // Set a mark
        path_marks.set_mark(&file_path, Mark::Own);
        assert_eq!(path_marks.for_path(&file_path), Mark::Own);
        assert_eq!(path_marks.path_marks().len(), 1);

        // Unset the mark
        path_marks.unset_mark(&file_path);
        assert_eq!(path_marks.for_path(&file_path), Mark::Watch);
        assert_eq!(path_marks.path_marks().len(), 0);
    }

    #[test]
    fn test_from_config() {
        let mut config = ArenaConfig::new(
            std::path::PathBuf::from("/doesnotexist/arena/root"),
            std::path::PathBuf::from("/doesnotexist/arena/db"),
            std::path::PathBuf::from("/doesnotexist/arena/blobs"),
        );
        config.mark = Mark::Keep;

        let path_marks = PathMarks::from_config(&config);
        assert_eq!(path_marks.default_mark, Mark::Keep);

        // Test with different default mark
        config.mark = Mark::Own;
        let path_marks = PathMarks::from_config(&config);
        assert_eq!(path_marks.default_mark, Mark::Own);
    }
}
