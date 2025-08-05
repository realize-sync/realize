use realize_types::Path;
use std::fs::Metadata;
use std::io::{self, ErrorKind};

/// Find a file at the given path within the root directory, but
/// reject if any symlink is encountered (blocking version).
///
/// This function traverses the path components and checks each intermediate directory
/// to ensure no symlinks are present. Only succeeds if the final path exists and
/// is not a symlink.
///
/// Returns the metadata of the final file/directory if found and no symlinks are encountered.
/// Returns an error if any component in the path is a symlink or if the path doesn't exist.
pub fn metadata_no_symlink_blocking(
    root: &std::path::Path,
    path: &Path,
) -> Result<Metadata, io::Error> {
    let mut current_path = root.to_path_buf();
    for component in Path::components(path.parent().as_ref()) {
        current_path = current_path.join(component);

        let m = std::fs::symlink_metadata(&current_path)?;
        let t = m.file_type();

        if t.is_symlink() {
            return Err(io::Error::new(
                ErrorKind::NotADirectory,
                format!("rejected symlink {current_path:?}"),
            ));
        }

        if !t.is_dir() {
            return Err(io::Error::new(
                ErrorKind::NotADirectory,
                current_path.display().to_string(),
            ));
        }
    }

    let current_path = path.within(root);
    let m = std::fs::symlink_metadata(&current_path)?;
    if m.file_type().is_symlink() {
        return Err(io::Error::new(
            ErrorKind::NotFound,
            format!("rejected symlink {current_path:?}"),
        ));
    }

    Ok(m)
}

/// Find a file at the given path within the root directory, but
/// reject if any symlink is encountered (async version).
///
/// This function traverses the path components and checks each intermediate directory
/// to ensure no symlinks are present. Only succeeds if the final path exists and
/// is not a symlink.
///
/// Returns the metadata of the final file/directory if found and no symlinks are encountered.
/// Returns an error if any component in the path is a symlink or if the path doesn't exist.
pub async fn metadata_no_symlink(
    root: &std::path::Path,
    path: &Path,
) -> Result<Metadata, io::Error> {
    let root = root.to_path_buf();
    let path = path.clone();
    tokio::task::spawn_blocking(move || metadata_no_symlink_blocking(&root, &path)).await?
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_fs::TempDir;
    use tokio::fs;

    #[tokio::test]
    async fn test_find_no_symlink_success() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let root = temp_dir.path().to_path_buf();

        // Create directory structure: root/a/b/c
        let dir_a = root.join("a");
        let dir_b = dir_a.join("b");
        let file_c = dir_b.join("c");

        fs::create_dir_all(&dir_b).await?;
        fs::write(&file_c, "test content").await?;

        let path = Path::parse("a/b/c")?;
        let metadata = metadata_no_symlink(&root, &path).await?;

        assert!(metadata.is_file());
        Ok(())
    }

    #[tokio::test]
    async fn test_find_no_symlink_directory() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let root = temp_dir.path().to_path_buf();

        // Create directory structure: root/a/b/c
        let dir_a = root.join("a");
        let dir_b = dir_a.join("b");
        let dir_c = dir_b.join("c");

        fs::create_dir_all(&dir_c).await?;

        let path = Path::parse("a/b/c")?;
        let metadata = metadata_no_symlink(&root, &path).await?;

        assert!(metadata.is_dir());
        Ok(())
    }

    #[tokio::test]
    async fn test_find_no_symlink_symlink_in_path() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let root = temp_dir.path().to_path_buf();

        // Create directory structure: root/a/b
        let dir_a = root.join("a");
        let dir_b = dir_a.join("b");
        fs::create_dir_all(&dir_b).await?;

        // Create a symlink at root/a/c that points to b
        let symlink_c = dir_a.join("c");
        fs::symlink(&dir_b, &symlink_c).await?;

        // Create a file at root/a/c/d
        let file_d = symlink_c.join("d");
        fs::write(&file_d, "test content").await?;

        let path = Path::parse("a/c/d")?;
        let result = metadata_no_symlink(&root, &path).await;

        assert!(matches!(result, Err(e) if e.kind() == ErrorKind::NotADirectory));
        Ok(())
    }

    #[tokio::test]
    async fn test_find_no_symlink_symlink_at_root() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let root = temp_dir.path().to_path_buf();

        // Create a symlink at root/a that points to a directory
        let target_dir = root.join("target");
        fs::create_dir_all(&target_dir).await?;

        let symlink_a = root.join("a");
        fs::symlink(&target_dir, &symlink_a).await?;

        // Create a file at root/a/b
        let file_b = symlink_a.join("b");
        fs::write(&file_b, "test content").await?;

        let path = Path::parse("a/b")?;
        let result = metadata_no_symlink(&root, &path).await;

        assert!(matches!(result, Err(e) if e.kind() == ErrorKind::NotADirectory));
        Ok(())
    }

    #[tokio::test]
    async fn test_find_no_symlink_path_not_found() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let root = temp_dir.path().to_path_buf();

        let path = Path::parse("nonexistent/file")?;
        let result = metadata_no_symlink(&root, &path).await;

        assert!(matches!(result, Err(e) if e.kind() == ErrorKind::NotFound));
        Ok(())
    }

    #[tokio::test]
    async fn test_find_no_symlink_file_instead_of_directory() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let root = temp_dir.path().to_path_buf();

        // Create a file at root/a
        let file_a = root.join("a");
        fs::write(&file_a, "test content").await?;

        let path = Path::parse("a/b")?;
        let result = metadata_no_symlink(&root, &path).await;

        assert!(matches!(result, Err(e) if e.kind() == ErrorKind::NotADirectory));
        Ok(())
    }

    #[tokio::test]
    async fn test_find_no_symlink_empty_path() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let _root = temp_dir.path().to_path_buf();

        // Empty paths are invalid in realize_types::Path
        let path_result = Path::parse("");
        assert!(path_result.is_err());

        // If we could create an empty path, it should fail in find_no_symlink
        // But since Path::parse("") fails, we can't test this scenario
        Ok(())
    }
}
