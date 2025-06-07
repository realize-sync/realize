use std::path::{self};

use crate::errors::RealizeError;

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
    pub fn parse(str: impl Into<String>) -> Result<Path, RealizeError> {
        let str = str.into();
        if str.is_empty()
            || str.find(':').is_some()
            || str
                .split('/')
                .any(|s| s.is_empty() || s == "." || s == "..")
        {
            return Err(RealizeError::InvalidPath);
        }

        Ok(Path(str))
    }

    /// Build a path from a real path.
    ///
    /// Not all real paths can be transformed. They must be relative,
    /// non-empty paths containing only valid unicode strings.
    pub fn from_real_path(path: &path::Path) -> Result<Path, RealizeError> {
        for component in path.components() {
            match component {
                std::path::Component::Normal(_) => {}
                _ => {
                    return Err(RealizeError::InvalidPath);
                }
            }
        }

        Path::parse(path.to_str().ok_or(RealizeError::InvalidPath)?)
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
        if let Some(slash) = self.0.rfind('/') {
            Some(Path(self.0[0..slash].to_string()))
        } else {
            None
        }
    }

    /// Return this path as a real path.
    pub fn as_real_path(&self) -> &path::Path {
        path::Path::new(&self.0)
    }

    /// Create a [PathBuf] from this path.
    pub fn to_path_buf(&self) -> path::PathBuf {
        self.as_real_path().to_path_buf()
    }

    /// Build a real path within the given path.
    pub fn within(&self, path: &path::Path) -> path::PathBuf {
        path.join(self.as_real_path())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Into<path::PathBuf> for Path {
    fn into(self) -> path::PathBuf {
        self.as_real_path().to_path_buf()
    }
}

impl std::fmt::Display for Path {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsStr, os::unix::ffi::OsStrExt as _};

    use super::*;

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
        assert!(matches!(Path::parse(""), Err(RealizeError::InvalidPath),));
        assert!(matches!(Path::parse("/"), Err(RealizeError::InvalidPath),));
        assert!(matches!(
            Path::parse("/foo"),
            Err(RealizeError::InvalidPath),
        ));
        assert!(matches!(
            Path::parse("foo/"),
            Err(RealizeError::InvalidPath),
        ));
        assert!(matches!(
            Path::parse("foo//bar"),
            Err(RealizeError::InvalidPath),
        ));
        assert!(matches!(
            Path::parse("c:foo"),
            Err(RealizeError::InvalidPath),
        ));
        assert!(matches!(
            Path::parse("c:/foo"),
            Err(RealizeError::InvalidPath),
        ));
        assert!(matches!(
            Path::parse("foo/../bar"),
            Err(RealizeError::InvalidPath),
        ));
        assert!(matches!(
            Path::parse("foo/./bar"),
            Err(RealizeError::InvalidPath),
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
            Err(RealizeError::InvalidPath),
        ));

        assert!(matches!(
            Path::from_real_path(path::Path::new("/foo")),
            Err(RealizeError::InvalidPath),
        ));

        assert!(matches!(
            Path::from_real_path(path::Path::new("//foo")),
            Err(RealizeError::InvalidPath),
        ));

        assert!(matches!(
            Path::from_real_path(path::Path::new(OsStr::from_bytes(&[
                0x66, 0x6f, 0x80, 0x6f
            ]))),
            Err(RealizeError::InvalidPath),
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
}
