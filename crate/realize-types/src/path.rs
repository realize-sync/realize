use std::path::{self};

/// A path within an Arena.
///
/// Arena paths are simple nonempty relative paths, with directories
/// separated by / and without '.' or '..' or empty parts. Directory
/// and file names must be valid unicode and not contain a colon.
#[derive(
    Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Deserialize, serde::Serialize,
)]
pub struct Path(String);

impl Path {
    /// Build a path from a string.
    ///
    /// If parsing works, the path is guaranteed to be acceptable.
    pub fn parse(str: impl Into<String>) -> Result<Path, PathError> {
        let str = str.into();
        check_path(&str)?;

        Ok(Path(str))
    }

    /// Append a path to this one and return the result.
    pub fn join(&self, str: &str) -> Result<Path, PathError> {
        check_path(str)?;

        let mut fullpath = self.0.clone();
        fullpath.push('/');
        fullpath.push_str(str);
        Ok(Path(fullpath))
    }

    /// Build a path from a real path.
    ///
    /// Not all real paths can be transformed. They must be relative,
    /// non-empty paths containing only valid unicode strings.
    pub fn from_real_path<T: AsRef<path::Path>>(path: T) -> Result<Path, PathError> {
        let path = path.as_ref();
        for component in path.components() {
            match component {
                std::path::Component::Normal(_) => {}
                _ => {
                    return Err(PathError::InvalidPath);
                }
            }
        }

        Path::parse(path.to_str().ok_or(PathError::InvalidPath)?)
    }

    /// Build a path from the given path, relative to the given root.
    ///
    /// For this to work, the given path must be inside the given root.
    pub fn from_real_path_in<T, U>(path: T, root: U) -> Option<Path>
    where
        T: AsRef<path::Path>,
        U: AsRef<path::Path>,
    {
        path.as_ref()
            .strip_prefix(root.as_ref())
            .ok()
            .map(|p| Path::from_real_path(p).ok())
            .flatten()
    }

    /// The name part of the path, without any parent element.
    ///
    /// This is the last and possibly only element of a
    /// slash-separated path.
    pub fn name(&self) -> &str {
        self.0.rsplit('/').next().unwrap_or(&self.0)
    }

    /// The final extension, with a dot, or the empty string.
    pub fn ext(&self) -> &str {
        let n = self.name();
        match n.rfind('.') {
            Some(pos) => &n[pos..],
            None => "",
        }
    }

    /// The parent of the path.
    ///
    /// Return a non-empty parent path or None.
    pub fn parent(&self) -> Option<Path> {
        self.0
            .rfind('/')
            .map(|slash| Path(self.0[0..slash].to_string()))
    }

    /// Split a possibly empty path into zero or more components.
    pub fn components(path: Option<&Self>) -> impl Iterator<Item = &str> {
        if let Some(path) = path {
            path.0.split('/')
        } else {
            // An empty iterator of the same type as above.
            let mut ret = "".split('/');
            ret.next();

            ret
        }
    }

    /// Return this path as a real path.
    pub fn as_real_path(&self) -> &path::Path {
        path::Path::new(&self.0)
    }

    /// Create a [std::path::PathBuf] from this path.
    pub fn to_path_buf(&self) -> path::PathBuf {
        self.as_real_path().to_path_buf()
    }

    /// Build a real path within the given path.
    pub fn within<T: AsRef<path::Path>>(&self, path: T) -> path::PathBuf {
        path.as_ref().join(self.as_real_path())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Return true if the other path is the current path or a parent
    /// of the current path.
    ///
    /// Note that it works at the level of paths, not names, so
    /// `Path::parse("foobar")?` does not start with
    /// `Path::parts("foo")?` even though "foobar" start with
    /// "foo".
    pub fn starts_with<T: AsRef<Path>>(&self, other: T) -> bool {
        if let Some(rest) = self.0.strip_prefix(other.as_ref().as_str()) {
            return rest == "" || rest.starts_with('/');
        }

        false
    }

    /// Returns true if any member of `paths` starts with or is the
    /// same as this path.
    pub fn matches_any(&self, paths: &Vec<Path>) -> bool {
        paths.iter().any(|e| self.starts_with(e))
    }
}

fn check_path(str: &str) -> Result<(), PathError> {
    if str.is_empty()
        || str.find(':').is_some()
        || str
            .split('/')
            .any(|s| s.is_empty() || s == "." || s == "..")
    {
        return Err(PathError::InvalidPath);
    }
    Ok(())
}

impl AsRef<Path> for Path {
    fn as_ref(&self) -> &Path {
        self
    }
}
impl From<Path> for path::PathBuf {
    fn from(val: Path) -> Self {
        val.as_real_path().to_path_buf()
    }
}

impl std::fmt::Display for Path {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Errors returned by Path functions
#[derive(Debug, thiserror::Error)]
pub enum PathError {
    #[error("Invalid path. Paths must be valid unicode and not contain ., .. or :")]
    InvalidPath,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt as _;

    #[test]
    fn parse_valid_paths() -> anyhow::Result<()> {
        Path::parse("foobar")?;
        Path::parse("foo/bar")?;
        Path::parse(".foo/.bar.txt")?;
        Path::parse("a/b/c/d")?;

        Ok(())
    }

    #[test]
    fn parse_invalid_paths() -> anyhow::Result<()> {
        assert!(matches!(Path::parse(""), Err(PathError::InvalidPath),));
        assert!(matches!(Path::parse("/"), Err(PathError::InvalidPath),));
        assert!(matches!(Path::parse("/foo"), Err(PathError::InvalidPath),));
        assert!(matches!(Path::parse("foo/"), Err(PathError::InvalidPath),));
        assert!(matches!(
            Path::parse("foo//bar"),
            Err(PathError::InvalidPath),
        ));
        assert!(matches!(Path::parse("c:foo"), Err(PathError::InvalidPath),));
        assert!(matches!(Path::parse("c:/foo"), Err(PathError::InvalidPath),));
        assert!(matches!(
            Path::parse("foo/../bar"),
            Err(PathError::InvalidPath),
        ));
        assert!(matches!(
            Path::parse("foo/./bar"),
            Err(PathError::InvalidPath),
        ));

        Ok(())
    }

    #[test]
    fn from_real_path_valid() -> anyhow::Result<()> {
        assert_eq!(
            Path::parse("foobar")?,
            Path::from_real_path(path::Path::new("foobar"))?
        );
        assert_eq!(
            Path::parse("foo/bar")?,
            Path::from_real_path(path::Path::new("foo/bar"))?
        );
        assert_eq!(
            Path::parse(".foo/.bar.txt")?,
            Path::from_real_path(path::Path::new(".foo/.bar.txt"))?
        );
        assert_eq!(
            Path::parse("a/b/c/d")?,
            Path::from_real_path(path::Path::new("a/b/c/d"))?
        );

        Ok(())
    }

    #[test]
    fn from_real_path_invalid() -> anyhow::Result<()> {
        assert!(matches!(
            Path::from_real_path(path::Path::new("foo/./bar")),
            Err(PathError::InvalidPath),
        ));

        assert!(matches!(
            Path::from_real_path(path::Path::new("/foo")),
            Err(PathError::InvalidPath),
        ));

        assert!(matches!(
            Path::from_real_path(path::Path::new("//foo")),
            Err(PathError::InvalidPath),
        ));

        assert!(matches!(
            Path::from_real_path(path::Path::new(OsStr::from_bytes(&[
                0x66, 0x6f, 0x80, 0x6f
            ]))),
            Err(PathError::InvalidPath),
        ));

        Ok(())
    }

    #[test]
    fn name() -> anyhow::Result<()> {
        assert_eq!("foobar", Path::parse("foobar")?.name());
        assert_eq!("bar", Path::parse("foo/bar")?.name());

        Ok(())
    }

    #[test]
    fn ext() -> anyhow::Result<()> {
        assert_eq!("", Path::parse("foobar")?.ext());
        assert_eq!(".txt", Path::parse("foobar.txt")?.ext());
        assert_eq!(".txt", Path::parse(".foobar.txt")?.ext());
        assert_eq!(".gz", Path::parse("foobar.tar.gz")?.ext());
        assert_eq!(".txt", Path::parse("foo.app/bar.txt")?.ext());

        Ok(())
    }

    #[test]
    fn parent() -> anyhow::Result<()> {
        assert_eq!(
            Some(Path::parse("a/b/c")?),
            Path::parse("a/b/c/d")?.parent()
        );
        assert_eq!(Some(Path::parse("a/b")?), Path::parse("a/b/c")?.parent());
        assert_eq!(Some(Path::parse("a")?), Path::parse("a/b")?.parent());
        assert_eq!(None, Path::parse("a")?.parent());

        Ok(())
    }

    #[test]
    fn components() -> anyhow::Result<()> {
        assert_eq!(
            Path::components(Some(&Path::parse("a/b/c")?)).collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
        assert_eq!(
            Path::components(Path::parse("a/b/c")?.parent().as_ref()).collect::<Vec<_>>(),
            vec!["a", "b"]
        );

        assert_eq!(
            Path::components(Some(&Path::parse("file")?)).collect::<Vec<_>>(),
            vec!["file"]
        );

        assert_eq!(
            Path::components(Path::parse("file")?.parent().as_ref()).collect::<Vec<_>>(),
            Vec::<&str>::new()
        );

        Ok(())
    }

    #[test]
    fn as_real_path() -> anyhow::Result<()> {
        assert_eq!(
            path::Path::new("foo/bar"),
            Path::parse("foo/bar")?.as_real_path()
        );
        assert_eq!(path::Path::new("bar"), Path::parse("bar")?.as_real_path());

        Ok(())
    }

    #[test]
    fn within() -> anyhow::Result<()> {
        assert_eq!(
            path::Path::new("/tmp/foo/bar"),
            Path::parse("foo/bar")?.within(path::Path::new("/tmp"))
        );

        Ok(())
    }

    #[test]
    fn into_path_buf() -> anyhow::Result<()> {
        let buf: path::PathBuf = Path::parse("foo/bar")?.into();
        assert_eq!(path::PathBuf::from("foo/bar"), buf);

        Ok(())
    }

    #[test]
    fn starts_with_file_or_dir() -> anyhow::Result<()> {
        assert!(Path::parse("test/foo")?.starts_with(&Path::parse("test")?));
        assert!(Path::parse("test/foo")?.starts_with(&Path::parse("test/foo")?));
        assert!(!Path::parse("test/foo")?.starts_with(&Path::parse("test/foo/bar")?));
        assert!(!Path::parse("test/foo")?.starts_with(&Path::parse("test/foobar")?));

        assert!(!Path::parse("test/foo")?.starts_with(&Path::parse("t")?));
        assert!(!Path::parse("test/foobar")?.starts_with(&Path::parse("test/foo")?));

        Ok(())
    }

    #[test]
    fn join() -> anyhow::Result<()> {
        assert_eq!(Path::parse("foo/bar")?, Path::parse("foo")?.join("bar")?);
        assert_eq!(
            Path::parse("foo/bar/baz")?,
            Path::parse("foo")?.join("bar/baz")?
        );
        assert_eq!(
            Path::parse("foo/bar/baz")?,
            Path::parse("foo/bar")?.join("baz")?
        );
        assert!(matches!(
            Path::parse("foo")?.join("/bar"),
            Err(PathError::InvalidPath),
        ));
        assert!(matches!(
            Path::parse("foo")?.join("bar//baz"),
            Err(PathError::InvalidPath),
        ));
        assert!(matches!(
            Path::parse("foo/bar")?.join("../baz"),
            Err(PathError::InvalidPath),
        ));

        Ok(())
    }
}
