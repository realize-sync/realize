#![allow(dead_code)] // work in progress

use super::db::{ArenaDatabase, ArenaReadTransaction, ArenaWriteTransaction};
use super::types::{Mark, MarkTableEntry};
use crate::arena::arena_cache;
use crate::arena::index;
use crate::utils::holder::Holder;
use crate::{DirtyPaths, Inode, StorageError};
use realize_types::Path;
use std::sync::Arc;

/// Tracks marks hierarchically by paths in an arena.
///
/// Paths can be files or directories, and marks are inherited from the most specific
/// path that has a mark set (file > directory > arena default).
///
/// When a mark changes, affected paths in the index and arena cache
/// are marked dirty. Changing the root mark will mark all files in
/// the cache and index dirty.
pub struct PathMarks {
    db: Arc<ArenaDatabase>,
    arena_root: Inode,
    dirty_paths: Arc<DirtyPaths>,
}

impl PathMarks {
    /// Create a new PathMarks with the given default mark.
    pub fn new(
        db: Arc<ArenaDatabase>,
        arena_root: Inode,
        dirty_paths: Arc<DirtyPaths>,
    ) -> Result<Self, StorageError> {
        // Ensure the database has the required mark table
        {
            let txn = db.begin_write()?;
            txn.mark_table()?;
            txn.commit()?;
        }

        Ok(Self {
            db,
            arena_root,
            dirty_paths,
        })
    }

    /// Get the mark for a specific path.
    ///
    /// The mark might have been set on the given path, one of its
    /// parents or it can be the root mark.
    pub fn get_mark(&self, path: &Path) -> Result<Mark, StorageError> {
        let txn = self.db.begin_read()?;
        let mark_table = txn.mark_table()?;

        do_get_mark(&mark_table, Some(path))
    }

    /// Set the default mark for the arena.
    pub fn set_root_mark(&self, mark: Mark) -> Result<(), StorageError> {
        self.set_mark_or_root(None, mark)
    }

    /// Set a mark for a specific path, which can be a file or a directory.
    pub fn set_mark(&self, path: &Path, mark: Mark) -> Result<(), StorageError> {
        self.set_mark_or_root(Some(path), mark)
    }

    /// Unset a mark for a specific path.
    pub fn clear_mark(&self, path: &Path) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut mark_table = txn.mark_table()?;

            let before = match mark_table.remove(path.as_str())? {
                None => {
                    // No changes
                    return Ok(());
                }
                Some(e) => e.value().parse()?.mark,
            };
            let after = do_get_mark(&mark_table, Some(path))?;

            if before != after {
                // Clearing the mark had an impact, mark affected files.
                self.mark_dirty(&txn, Some(path))?;
            }
        }

        txn.commit()?;
        Ok(())
    }

    fn set_mark_or_root(
        &self,
        path_or_root: Option<&Path>,
        mark: Mark,
    ) -> Result<(), StorageError> {
        let txn = self.db.begin_write()?;
        {
            let mut mark_table = txn.mark_table()?;
            let before = do_get_mark(&mark_table, path_or_root)?;
            mark_table.insert(
                path_or_root.map(|p| p.as_str()).unwrap_or(""),
                Holder::with_content(MarkTableEntry { mark })?,
            )?;

            if before != mark {
                // Setting the mark had an impact, mark affected files.
                self.mark_dirty(&txn, path_or_root)?;
            }
        }

        txn.commit()?;
        Ok(())
    }

    /// Mark a paths as dirty in the engine, indirectly through the
    /// index and cache.
    ///
    /// If `path_or_root` is None, all files in the index and cache are marked dirty.
    ///
    /// If `path_or_root` is a file, that file is marked dirty in the cache, index or both.
    ///
    /// If `path_or_root` is a directory, all the files it contains,
    /// directly or indirectly, in the cache or the index, are all
    /// marked dirty.
    ///
    /// If `path_or_root` doesn't exist, neither in the cache nor in
    /// the index, this function does nothing.
    fn mark_dirty(
        &self,
        txn: &ArenaWriteTransaction,
        path_or_root: Option<&Path>,
    ) -> Result<(), StorageError> {
        if let Some(path) = path_or_root {
            index::mark_dirty_recursive(txn, &path, &self.dirty_paths)?;
        } else {
            index::make_all_dirty(txn, &self.dirty_paths)?;
        }

        arena_cache::mark_dirty_recursive(txn, self.arena_root, path_or_root, &self.dirty_paths)?;

        Ok(())
    }
}

pub(crate) fn get_mark(txn: &ArenaReadTransaction, path: &Path) -> Result<Mark, StorageError> {
    let mark_table = txn.mark_table()?;
    do_get_mark(&mark_table, Some(path))
}

fn do_get_mark(
    mark_table: &impl redb::ReadableTable<&'static str, Holder<'static, MarkTableEntry>>,
    path: Option<&Path>,
) -> Result<Mark, StorageError> {
    let mut current = path.cloned();

    loop {
        let key = current.as_ref().map(|v| v.as_str()).unwrap_or("");
        if let Some(entry) = mark_table.get(key)? {
            return Ok(entry.value().parse()?.mark);
        }
        match current {
            None => {
                return Ok(Mark::default());
            }
            Some(p) => {
                current = p.parent();
            }
        }
    }
}

/// Entry in the mark
#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::arena::arena_cache::ArenaCache;
    use crate::arena::engine;
    use crate::arena::index::RealIndexBlocking;
    use crate::utils::redb_utils;
    use realize_types::{Arena, Hash, UnixTime};

    struct Fixture {
        arena: Arena,
        db: Arc<ArenaDatabase>,
        acache: ArenaCache,
        index: RealIndexBlocking,
        marks: PathMarks,
        dirty_paths: Arc<DirtyPaths>,
    }

    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();

            let arena = Arena::from("test");
            let db = ArenaDatabase::new(redb_utils::in_memory()?)?;
            let dirty_paths = DirtyPaths::new(Arc::clone(&db)).await?;
            let arena_root = Inode(300);
            let acache = ArenaCache::new(
                arena,
                arena_root,
                Arc::clone(&db),
                PathBuf::from("/dev/null"),
                Arc::clone(&dirty_paths),
            )?;
            let index = RealIndexBlocking::new(arena, Arc::clone(&db), Arc::clone(&dirty_paths))?;
            let marks = PathMarks::new(Arc::clone(&db), arena_root, Arc::clone(&dirty_paths))?;

            Ok(Self {
                arena,
                db,
                acache,
                index,
                marks,
                dirty_paths,
            })
        }

        /// Clear all dirty flags in both index and cache
        fn clear_all_dirty(&self) -> anyhow::Result<()> {
            let txn = self.db.begin_write()?;
            while engine::take_dirty(&txn)?.is_some() {}
            txn.commit()?;

            Ok(())
        }

        /// Check if a path is dirty in the index
        fn is_dirty(&self, path: &Path) -> anyhow::Result<bool> {
            let txn = self.db.begin_read()?;
            Ok(engine::is_dirty(&txn, path)?)
        }

        /// Add a file to the index for testing
        fn add_file_to_index(&self, path: &Path) -> anyhow::Result<()> {
            Ok(self
                .index
                .add_file(path, 100, &UnixTime::from_secs(1234567889), Hash([1; 32]))?)
        }

        /// Add a file to the cache for testing
        fn add_file_to_cache(&self, path: &Path) -> anyhow::Result<()> {
            use crate::arena::notifier::Notification;
            use realize_types::Peer;

            let test_peer = Peer::from("test-peer");
            let notification = Notification::Add {
                arena: self.arena,
                index: 1,
                path: path.clone(),
                mtime: UnixTime::from_secs(1234567890),
                size: 100,
                hash: Hash([2; 32]),
            };

            self.acache
                .update(test_peer, notification, || Ok((Inode(1000), Inode(2000))))?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn default_mark() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        // New PathMarks should return default mark (Watch) for any path
        let path = Path::parse("some/file.txt")?;
        let mark = fixture.marks.get_mark(&path)?;

        assert_eq!(mark, Mark::Watch);

        Ok(())
    }

    #[tokio::test]
    async fn set_and_get_root_mark() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        // Set root mark to Keep
        fixture.marks.set_root_mark(Mark::Keep)?;

        // Verify root mark is returned for any path
        let path = Path::parse("some/file.txt")?;
        let mark = fixture.marks.get_mark(&path)?;

        assert_eq!(mark, Mark::Keep);

        // Change root mark to Own
        fixture.marks.set_root_mark(Mark::Own)?;
        let mark = fixture.marks.get_mark(&path)?;

        assert_eq!(mark, Mark::Own);

        Ok(())
    }

    #[tokio::test]
    async fn set_and_get_path_mark() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let file_path = Path::parse("dir/file.txt")?;
        let dir_path = Path::parse("dir")?;

        // Set mark on specific file
        fixture.marks.set_mark(&file_path, Mark::Own)?;

        // Verify file has the mark
        let mark = fixture.marks.get_mark(&file_path)?;
        assert_eq!(mark, Mark::Own);

        // Verify other files still get default mark
        let other_file = Path::parse("other/file.txt")?;
        let mark = fixture.marks.get_mark(&other_file)?;
        assert_eq!(mark, Mark::Watch);

        // Set mark on directory
        fixture.marks.set_mark(&dir_path, Mark::Keep)?;

        // Verify directory has the mark
        let mark = fixture.marks.get_mark(&dir_path)?;
        assert_eq!(mark, Mark::Keep);

        // Verify files in directory inherit the mark
        let file_in_dir = Path::parse("dir/another.txt")?;
        let mark = fixture.marks.get_mark(&file_in_dir)?;
        assert_eq!(mark, Mark::Keep);

        // But the specific file still has its own mark
        let mark = fixture.marks.get_mark(&file_path)?;
        assert_eq!(mark, Mark::Own);

        Ok(())
    }

    #[tokio::test]
    async fn hierarchical_mark_inheritance() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        // Set root mark
        fixture.marks.set_root_mark(Mark::Watch)?;

        // Set directory mark
        let dir_path = Path::parse("parent/child")?;
        fixture.marks.set_mark(&dir_path, Mark::Keep)?;

        // Set specific file mark
        let file_path = Path::parse("parent/child/file.txt")?;
        fixture.marks.set_mark(&file_path, Mark::Own)?;

        // Test inheritance hierarchy
        let root_file = Path::parse("other/file.txt")?;
        assert_eq!(fixture.marks.get_mark(&root_file)?, Mark::Watch);

        let parent_file = Path::parse("parent/file.txt")?;
        assert_eq!(fixture.marks.get_mark(&parent_file)?, Mark::Watch);

        let child_file = Path::parse("parent/child/other.txt")?;
        assert_eq!(fixture.marks.get_mark(&child_file)?, Mark::Keep);

        let specific_file = Path::parse("parent/child/file.txt")?;
        assert_eq!(fixture.marks.get_mark(&specific_file)?, Mark::Own);

        Ok(())
    }

    #[tokio::test]
    async fn clear_mark() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let file_path = Path::parse("dir/file.txt")?;
        let dir_path = Path::parse("dir")?;

        // Set marks
        fixture.marks.set_mark(&file_path, Mark::Own)?;
        fixture.marks.set_mark(&dir_path, Mark::Keep)?;

        // Verify marks are set
        assert_eq!(fixture.marks.get_mark(&file_path)?, Mark::Own);
        assert_eq!(fixture.marks.get_mark(&dir_path)?, Mark::Keep);

        // Clear file mark
        fixture.marks.clear_mark(&file_path)?;

        // File should now inherit from directory
        assert_eq!(fixture.marks.get_mark(&file_path)?, Mark::Keep);

        // Clear directory mark
        fixture.marks.clear_mark(&dir_path)?;

        // Both should now get default mark
        assert_eq!(fixture.marks.get_mark(&file_path)?, Mark::Watch);
        assert_eq!(fixture.marks.get_mark(&dir_path)?, Mark::Watch);

        // Clearing non-existent mark should be no-op
        fixture
            .marks
            .clear_mark(&Path::parse("nonexistent/file.txt")?)?;

        Ok(())
    }

    #[tokio::test]
    async fn clear_mark_with_root_mark() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        // Set root mark
        fixture.marks.set_root_mark(Mark::Keep)?;

        let file_path = Path::parse("dir/file.txt")?;

        // Set specific file mark
        fixture.marks.set_mark(&file_path, Mark::Own)?;
        assert_eq!(fixture.marks.get_mark(&file_path)?, Mark::Own);

        // Clear file mark
        fixture.marks.clear_mark(&file_path)?;

        // File should now inherit from root
        assert_eq!(fixture.marks.get_mark(&file_path)?, Mark::Keep);

        Ok(())
    }

    #[tokio::test]
    async fn mark_persistence() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let file_path = Path::parse("test/file.txt")?;
        let dir_path = Path::parse("test")?;

        // Set marks
        fixture.marks.set_root_mark(Mark::Keep)?;
        fixture.marks.set_mark(&dir_path, Mark::Watch)?;
        fixture.marks.set_mark(&file_path, Mark::Own)?;

        // Create new PathMarks instance with same database
        let new_marks = PathMarks::new(
            Arc::clone(&fixture.db),
            fixture.marks.arena_root,
            Arc::clone(&fixture.dirty_paths),
        )?;

        // Verify marks persist
        assert_eq!(new_marks.get_mark(&file_path)?, Mark::Own);
        assert_eq!(new_marks.get_mark(&dir_path)?, Mark::Watch);
        assert_eq!(
            new_marks.get_mark(&Path::parse("other/file.txt")?)?,
            Mark::Keep
        );

        Ok(())
    }

    #[tokio::test]
    async fn all_mark_types() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let watch_path = Path::parse("watch/file.txt")?;
        let keep_path = Path::parse("keep/file.txt")?;
        let own_path = Path::parse("own/file.txt")?;

        // Test all mark types
        fixture.marks.set_mark(&watch_path, Mark::Watch)?;
        fixture.marks.set_mark(&keep_path, Mark::Keep)?;
        fixture.marks.set_mark(&own_path, Mark::Own)?;

        assert_eq!(fixture.marks.get_mark(&watch_path)?, Mark::Watch);
        assert_eq!(fixture.marks.get_mark(&keep_path)?, Mark::Keep);
        assert_eq!(fixture.marks.get_mark(&own_path)?, Mark::Own);

        Ok(())
    }

    #[tokio::test]
    async fn mark_changes() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let file_path = Path::parse("test/file.txt")?;

        // Set initial mark
        fixture.marks.set_mark(&file_path, Mark::Watch)?;
        assert_eq!(fixture.marks.get_mark(&file_path)?, Mark::Watch);

        // Change mark
        fixture.marks.set_mark(&file_path, Mark::Keep)?;
        assert_eq!(fixture.marks.get_mark(&file_path)?, Mark::Keep);

        // Change again
        fixture.marks.set_mark(&file_path, Mark::Own)?;
        assert_eq!(fixture.marks.get_mark(&file_path)?, Mark::Own);

        // Change back
        fixture.marks.set_mark(&file_path, Mark::Watch)?;
        assert_eq!(fixture.marks.get_mark(&file_path)?, Mark::Watch);

        Ok(())
    }

    // New dirty flag tests
    #[tokio::test]
    async fn set_mark_marks_dirty_when_effective() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        // Add files to both index and cache
        let in_index = Path::parse("test/in_index.txt")?;
        fixture.add_file_to_index(&in_index)?;

        let in_cache = Path::parse("test/in_cache.txt")?;
        fixture.add_file_to_cache(&in_cache)?;

        fixture.clear_all_dirty()?;

        // Set a mark that changes the effective mark (from Watch to Own)
        fixture.marks.set_mark(&in_index, Mark::Own)?;
        fixture.marks.set_mark(&in_cache, Mark::Own)?;

        // Check that the file is marked dirty
        assert!(fixture.is_dirty(&in_index)?);
        assert!(fixture.is_dirty(&in_cache)?);

        Ok(())
    }

    #[tokio::test]
    async fn set_mark_does_not_mark_dirty_when_not_effective() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        // Add files to both index and cache
        let file_path = Path::parse("test/file.txt")?;
        fixture.add_file_to_index(&file_path)?;

        fixture.clear_all_dirty()?;

        // Set the same mark that's already effective (Watch is default)
        fixture.marks.set_mark(&file_path, Mark::Watch)?;

        // Check that the file is NOT marked dirty since the effective mark didn't change
        assert!(!fixture.is_dirty(&file_path)?);

        Ok(())
    }

    #[tokio::test]
    async fn set_root_mark_marks_all_dirty() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        // Add multiple files to both index and cache
        let index_files = vec![
            Path::parse("dir1/file1.txt")?,
            Path::parse("dir1/file2.txt")?,
            Path::parse("dir2/file3.txt")?,
            Path::parse("file4.txt")?,
        ];
        for file in &index_files {
            fixture.add_file_to_index(file)?;
        }

        let cache_files = vec![
            Path::parse("dir3/file5.txt")?,
            Path::parse("dir3/file6.txt")?,
            Path::parse("dir4/file7.txt")?,
            Path::parse("file8.txt")?,
        ];
        for file in &cache_files {
            fixture.add_file_to_index(file)?;
        }

        fixture.clear_all_dirty()?;

        // Set root mark that changes the effective mark for all files
        fixture.marks.set_root_mark(Mark::Keep)?;

        // Check that all files are marked dirty
        for file in &index_files {
            assert!(fixture.is_dirty(file)?, "{file} should be dirty");
        }
        for file in &cache_files {
            assert!(fixture.is_dirty(file)?, "{file} should be dirty");
        }

        Ok(())
    }

    #[tokio::test]
    async fn clear_mark_marks_dirty_when_effective() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        // Add files to both index and cache
        let file_path = Path::parse("test/file.txt")?;
        fixture.add_file_to_index(&file_path)?;

        // Set a specific mark first
        fixture.marks.set_mark(&file_path, Mark::Own)?;
        fixture.clear_all_dirty()?;

        // Clear the mark, which should change the effective mark back to default
        fixture.marks.clear_mark(&file_path)?;

        // Check that the file is marked dirty
        assert!(fixture.is_dirty(&file_path)?);

        Ok(())
    }

    #[tokio::test]
    async fn clear_mark_does_not_mark_dirty_when_it_did_not_exist() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        // Add files to both index and cache
        let file_path = Path::parse("test/file.txt")?;
        fixture.add_file_to_index(&file_path)?;

        fixture.clear_all_dirty()?;

        // Clear a mark that doesn't exist (file already has default Watch mark)
        fixture.marks.clear_mark(&file_path)?;

        // Check that the file is NOT marked dirty since the effective mark didn't change
        assert!(!fixture.is_dirty(&file_path)?);

        Ok(())
    }

    #[tokio::test]
    async fn clear_mark_does_not_mark_dirty_when_not_effective() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        // Add files to both index and cache
        let file_path = Path::parse("test/file.txt")?;
        fixture.add_file_to_index(&file_path)?;
        fixture.marks.set_mark(&file_path, Mark::Watch)?; // the default
        fixture.clear_all_dirty()?;

        // Clearing the mark changes nothing, since this mark just sets the default.
        fixture.marks.clear_mark(&file_path)?;

        // Check that the file is NOT marked dirty since the effective mark didn't change
        assert!(!fixture.is_dirty(&file_path)?);

        Ok(())
    }

    #[tokio::test]
    async fn hierarchical_mark_changes_mark_dirty_recursively() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        // Add files in a directory structure
        let dir_file1 = Path::parse("dir/file1.txt")?;
        let dir_file2 = Path::parse("dir/file2.txt")?;
        let dir_subdir_file3 = Path::parse("dir/subdir/file3.txt")?;
        let dir_subdir_file4 = Path::parse("dir/subdir/file4.txt")?;
        let notdir_file5 = Path::parse("other/file5.txt")?;
        let notdir_file6 = Path::parse("other/file6.txt")?;
        for file in vec![&dir_file1, &dir_subdir_file3, &notdir_file5] {
            fixture.add_file_to_index(file)?;
        }
        for file in vec![&dir_file2, &dir_subdir_file4, &notdir_file6] {
            fixture.add_file_to_cache(file)?;
        }
        fixture.clear_all_dirty()?;

        // Set a mark on the directory that changes the effective mark for files in that directory
        let dir_path = Path::parse("dir")?;
        fixture.marks.set_mark(&dir_path, Mark::Keep)?;

        // Check that files in the directory are marked dirty
        assert!(fixture.is_dirty(&dir_file1)?);
        assert!(fixture.is_dirty(&dir_file2)?);
        assert!(fixture.is_dirty(&dir_subdir_file3)?);
        assert!(fixture.is_dirty(&dir_subdir_file4)?);

        // Check that files outside the directory are NOT marked dirty
        assert!(!fixture.is_dirty(&notdir_file5)?);
        assert!(!fixture.is_dirty(&notdir_file6)?);

        Ok(())
    }

    #[tokio::test]
    async fn specific_file_mark_overrides_directory_mark() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        // Add files to both index and cache
        let dir_path = Path::parse("dir")?;
        let file_path = Path::parse("dir/file.txt")?;
        fixture.add_file_to_index(&file_path)?;

        // Set directory mark first
        fixture.marks.set_mark(&dir_path, Mark::Keep)?;
        fixture.clear_all_dirty()?;

        // Set a different mark on the specific file
        fixture.marks.set_mark(&file_path, Mark::Own)?;

        // Check that the file is marked dirty (effective mark changed from Keep to Own)
        assert!(fixture.is_dirty(&file_path)?);

        Ok(())
    }

    #[tokio::test]
    async fn clear_directory_mark_affects_all_files_in_directory() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        // Add files in a directory structure
        let files = vec![
            Path::parse("dir/file1.txt")?,
            Path::parse("dir/file2.txt")?,
            Path::parse("dir/subdir/file3.txt")?,
        ];

        for file in &files {
            fixture.add_file_to_index(file)?;
        }

        // Set directory mark
        let dir_path = Path::parse("dir")?;
        fixture.marks.set_mark(&dir_path, Mark::Keep)?;
        fixture.clear_all_dirty()?;

        // Clear the directory mark
        fixture.marks.clear_mark(&dir_path)?;

        // Check that all files in the directory are marked dirty (effective mark changed from Keep to Watch)
        for file in &files {
            assert!(fixture.is_dirty(file)?, "{file} should be dirty in index");
        }

        Ok(())
    }

    #[tokio::test]
    async fn nonexistent_files_not_marked_dirty() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        // Set marks on paths that don't exist in index or cache
        let nonexistent_path = Path::parse("nonexistent/file.txt")?;
        fixture.marks.set_mark(&nonexistent_path, Mark::Own)?;

        // Check that no files are marked dirty (since the path doesn't exist)
        let txn = fixture.db.begin_write()?;
        assert!(engine::take_dirty(&txn)?.is_none());
        txn.commit()?;

        Ok(())
    }

    #[tokio::test]
    async fn root_mark_clearing_marks_all_dirty() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        // Add multiple files to both index and cache
        let files_in_index = vec![
            Path::parse("dir1/file1.txt")?,
            Path::parse("dir2/file2.txt")?,
            Path::parse("file3.txt")?,
        ];
        for file in &files_in_index {
            fixture.add_file_to_index(file)?;
        }
        let files_in_cache = vec![
            Path::parse("dir3/file4.txt")?,
            Path::parse("dir4/file5.txt")?,
            Path::parse("file6.txt")?,
        ];
        for file in &files_in_cache {
            fixture.add_file_to_cache(file)?;
        }

        // Set root mark
        fixture.marks.set_root_mark(Mark::Keep)?;
        fixture.clear_all_dirty()?;

        // Clear root mark (set to default Watch)
        fixture.marks.set_root_mark(Mark::Watch)?;

        // Check that all files are marked dirty (effective mark changed from Keep to Watch)
        for file in &files_in_index {
            assert!(fixture.is_dirty(file)?, "{file} should be dirty");
        }
        for file in &files_in_cache {
            assert!(fixture.is_dirty(file)?, "{file} should be dirty");
        }

        Ok(())
    }
}
