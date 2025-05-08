use crate::model::error::{RealizeError, Result};
use crate::model::service::{ByteRange, DirectoryId, RealizeService, SyncedFile, SyncedFileState};
use std::collections::HashMap;
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use walkdir::WalkDir;

pub struct RealizeServer {
    /// Maps directory IDs to local paths
    dirs: HashMap<DirectoryId, Rc<Directory>>,
}

impl RealizeServer {
    pub fn new<T>(dirs: T) -> Self
    where
        T: IntoIterator<Item = Directory>,
    {
        Self {
            dirs: dirs
                .into_iter()
                .map(|dir| (dir.id.clone(), Rc::new(dir)))
                .collect(),
        }
    }

    pub fn for_dir(id: &DirectoryId, path: &Path) -> Self {
        RealizeServer::new(vec![Directory::new(id, path)])
    }

    fn find_directory(&self, dir_id: &DirectoryId) -> Result<&Rc<Directory>> {
        self.dirs.get(dir_id).ok_or_else(|| {
            RealizeError::BadRequest(format!("Unknown directory ID: '{:?}'", dir_id))
        })
    }
}

impl RealizeService for RealizeServer {
    fn list(&self, dir_id: DirectoryId) -> Result<Vec<SyncedFile>> {
        let dir = self.find_directory(&dir_id)?;
        let mut files = Vec::new();
        for entry in WalkDir::new(dir.path()).into_iter().flatten() {
            if let Some((state, logical)) = LogicalPath::from_actual(dir, entry.path()) {
                // TODO: What if a file exists for both the partial
                // and final form of the logical path? Only one entry
                // should be returned and its state should be final.
                files.push(SyncedFile {
                    path: logical.relative_path().to_path_buf(),
                    size: entry.metadata()?.size(),
                    state,
                });
            }
        }

        Ok(files)
    }

    fn send(
        &self,
        dir_id: DirectoryId,
        relative_path: PathBuf,
        range: ByteRange,
        data: Vec<u8>,
    ) -> Result<()> {
        let dir = self.find_directory(&dir_id)?;
        let logical = LogicalPath::new(dir, &relative_path);
        let path = logical.partial_path();
        if let Ok((state, actual)) = logical.find() {
            // File already exists
            if state == SyncedFileState::Final {
                fs::rename(actual, &path)?;
            }
        } else {
            // Open will need to create the file. Ensure parent
            // directory exists beforehand.
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
        }
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&path)?;
        file.seek(SeekFrom::Start(range.0))?;
        file.write_all(&data)?;

        Ok(())
    }

    fn finish(&self, dir_id: DirectoryId, relative_path: PathBuf) -> Result<()> {
        let dir = self.find_directory(&dir_id)?;
        let logical = LogicalPath::new(dir, &relative_path);
        if let (SyncedFileState::Partial, real_path) = logical.find()? {
            fs::rename(real_path, logical.final_path())?;
        }
        Ok(())
    }
}

// A directory, stored in RealizeServer and in LogicalPath.
pub struct Directory {
    id: DirectoryId,
    path: PathBuf,
}

impl Directory {
    /// Create a directory with the given id and path.
    pub fn new(id: &DirectoryId, path: &Path) -> Self {
        Self {
            id: id.clone(),
            path: path.to_path_buf(),
        }
    }

    /// Directory ID, as found in Service calls.
    pub fn id(&self) -> &DirectoryId {
        &self.id
    }

    /// Local directory path that correspond to the ID.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// A logical path is a relative path inside of a [Directory].
//
/// The corresponding actual file might be in
/// [SyncedFileState::Partial] or [SyncedFileState::Final] form. Check
/// with [LogicalPath::find].
struct LogicalPath(Rc<Directory>, PathBuf);

impl LogicalPath {
    /// Create a new logical path, without checking it.
    fn new(dir: &Rc<Directory>, path: &Path) -> Self {
        // TODO: check that path is relative and doesn't use any ..
        Self(Rc::clone(dir), path.to_path_buf())
    }

    /// Create a logical path from an actual partial or final path.
    fn from_actual(dir: &Rc<Directory>, actual: &Path) -> Option<(SyncedFileState, Self)> {
        if !actual.is_file() {
            return None;
        }

        let relative = pathdiff::diff_paths(actual, dir.path());
        if let Some(mut relative) = relative {
            if let Some(filename) = actual.file_name() {
                let name_bytes = filename.to_string_lossy();
                if name_bytes.starts_with(".") && name_bytes.ends_with(".part") {
                    let stripped_name =
                        OsString::from(name_bytes[1..name_bytes.len() - 5].to_string());
                    relative.set_file_name(&stripped_name);
                    return Some((SyncedFileState::Partial, LogicalPath::new(dir, &relative)));
                }
                return Some((SyncedFileState::Final, LogicalPath::new(dir, &relative)));
            }
        }

        None
    }

    // Look for a file for the logical path.
    //
    // The path can be found in final or partial found. Return both
    // the [SyncedFileState] and the actual path of the file that was
    // found.
    //
    // File with an I/O error of kind [std::io::ErrorKind::NotFound]
    // if no file exists for the logical path.
    fn find(&self) -> Result<(SyncedFileState, PathBuf)> {
        let fpath = self.final_path();
        if fpath.exists() {
            return Ok((SyncedFileState::Final, fpath));
        }
        let partial = self.partial_path();
        if partial.exists() {
            return Ok((SyncedFileState::Partial, partial));
        }

        Err(std::io::Error::from(std::io::ErrorKind::NotFound).into())
    }

    /// Return the final form of the logical path.
    fn final_path(&self) -> PathBuf {
        self.0.path().join(&self.1)
    }

    /// Return the partial form of the logical path.
    fn partial_path(&self) -> PathBuf {
        let mut partial = self.0.path().to_path_buf();
        partial.push(&self.1);

        let mut part_filename = OsString::from(".");
        if let Some(fname) = self.1.file_name() {
            part_filename.push(OsString::from(fname.to_string_lossy().to_string()));
        }
        part_filename.push(OsString::from(".part"));
        partial.set_file_name(part_filename);

        partial
    }

    /// Return the relative path of the logical path.
    fn relative_path(&self) -> &Path {
        &self.1
    }
}

impl From<walkdir::Error> for RealizeError {
    fn from(err: walkdir::Error) -> Self {
        if err.io_error().is_some() {
            // We know err contains an io error; unwrap will succeed.
            err.into_io_error().unwrap().into()
        } else {
            anyhow::Error::new(err).into()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_fs::prelude::*;
    use assert_fs::TempDir;
    use assert_unordered::assert_eq_unordered;
    use std::fs;

    fn setup_server_with_dir() -> anyhow::Result<(RealizeServer, TempDir, Rc<Directory>)> {
        let temp = TempDir::new()?;
        let dir = Rc::new(Directory::new(&DirectoryId::from("testdir"), temp.path()));

        Ok((RealizeServer::for_dir(dir.id(), dir.path()), temp, dir))
    }

    #[test]
    fn test_final_and_partial_paths() -> anyhow::Result<()> {
        let dir = Rc::new(Directory::new(
            &DirectoryId::from("testdir"),
            &PathBuf::from("/doesnotexist/testdir"),
        ));

        let file1 = LogicalPath::new(&dir, &PathBuf::from("file1.txt"));
        assert_eq!(
            PathBuf::from("/doesnotexist/testdir/file1.txt"),
            file1.final_path()
        );
        assert_eq!(
            PathBuf::from("/doesnotexist/testdir/.file1.txt.part"),
            file1.partial_path()
        );

        let file2 = LogicalPath::new(&dir, &PathBuf::from("subdir/file2.txt"));
        assert_eq!(
            PathBuf::from("/doesnotexist/testdir/subdir/file2.txt"),
            file2.final_path()
        );
        assert_eq!(
            PathBuf::from("/doesnotexist/testdir/subdir/.file2.txt.part"),
            file2.partial_path()
        );

        Ok(())
    }

    #[test]
    fn test_find_logical_path() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let dir = Rc::new(Directory::new(&DirectoryId::from("testdir"), temp.path()));

        temp.child("foo.txt").write_str("test")?;
        assert_eq!(
            (SyncedFileState::Final, temp.child("foo.txt").to_path_buf()),
            LogicalPath::new(&dir, &PathBuf::from("foo.txt")).find()?
        );

        temp.child("subdir/foo2.txt").write_str("test")?;
        assert_eq!(
            (
                SyncedFileState::Final,
                temp.child("subdir/foo2.txt").to_path_buf()
            ),
            LogicalPath::new(&dir, &PathBuf::from("subdir/foo2.txt")).find()?
        );

        temp.child(".bar.txt.part").write_str("test")?;
        assert_eq!(
            (
                SyncedFileState::Partial,
                temp.child(".bar.txt.part").to_path_buf()
            ),
            LogicalPath::new(&dir, &PathBuf::from("bar.txt")).find()?
        );

        temp.child("subdir/.bar2.txt.part").write_str("test")?;
        assert_eq!(
            (
                SyncedFileState::Partial,
                temp.child("subdir/.bar2.txt.part").to_path_buf()
            ),
            LogicalPath::new(&dir, &PathBuf::from("subdir/bar2.txt")).find()?
        );

        temp.child("foo3.txt").write_str("test")?;
        temp.child(".foo3.txt.part").write_str("test")?;
        assert_eq!(
            (SyncedFileState::Final, temp.child("foo3.txt").to_path_buf()),
            LogicalPath::new(&dir, &PathBuf::from("foo3.txt")).find()?
        );

        assert!(matches!(
            LogicalPath::new(&dir, &PathBuf::from("notfound.txt")).find(),
            Err(RealizeError::Io(err)) if err.kind()== std::io::ErrorKind::NotFound,
        ));

        Ok(())
    }

    #[test]
    fn test_list_empty() -> anyhow::Result<()> {
        let (server, _temp, dir) = setup_server_with_dir()?;
        let files = server.list(dir.id().clone()).unwrap();
        assert!(files.is_empty());

        Ok(())
    }

    #[test]
    fn test_list_files_and_partial() -> anyhow::Result<()> {
        let (server, temp, dir) = setup_server_with_dir()?;

        fs::create_dir_all(temp.child("subdir"))?;

        temp.child("foo.txt").write_str("hello")?;
        temp.child("subdir/foo2.txt").write_str("hello")?;
        temp.child(".bar.txt.part").write_str("partial")?;
        temp.child("subdir/.bar2.txt.part").write_str("partial")?;

        let files = server.list(dir.id().clone())?;
        assert_eq_unordered!(
            files,
            vec![
                SyncedFile {
                    path: PathBuf::from("foo.txt"),
                    size: 5,
                    state: SyncedFileState::Final
                },
                SyncedFile {
                    path: PathBuf::from("subdir/foo2.txt"),
                    size: 5,
                    state: SyncedFileState::Final
                },
                SyncedFile {
                    path: PathBuf::from("bar.txt"),
                    size: 7,
                    state: SyncedFileState::Partial
                },
                SyncedFile {
                    path: PathBuf::from("subdir/bar2.txt"),
                    size: 7,
                    state: SyncedFileState::Partial
                },
            ]
        );

        Ok(())
    }

    #[test]
    fn test_send_wrong_order() -> anyhow::Result<()> {
        let (server, _temp, dir) = setup_server_with_dir()?;

        let fpath = LogicalPath::new(&dir, &PathBuf::from("wrong_order.txt"));

        server.send(
            dir.id().clone(),
            fpath.relative_path().to_path_buf(),
            (5, 10),
            b"fghij".to_vec(),
        )?;
        assert!(fpath.partial_path().exists());
        assert!(!fpath.final_path().exists());
        assert_eq!(
            std::fs::read_to_string(fpath.partial_path())?,
            "\0\0\0\0\0fghij"
        );

        server.send(
            dir.id().clone(),
            fpath.relative_path().to_path_buf(),
            (0, 5),
            b"abcde".to_vec(),
        )?;
        assert!(fpath.partial_path().exists());
        assert!(!fpath.final_path().exists());
        assert_eq!(std::fs::read_to_string(fpath.partial_path())?, "abcdefghij");

        Ok(())
    }

    #[test]
    fn test_finish_partial() -> anyhow::Result<()> {
        let (server, _temp, dir) = setup_server_with_dir()?;

        let fpath = LogicalPath::new(&dir, &PathBuf::from("finish_partial.txt"));
        fs::write(fpath.partial_path(), "abcde")?;

        server.finish(dir.id().clone(), fpath.relative_path().to_path_buf())?;

        assert!(fpath.final_path().exists());
        assert!(!fpath.partial_path().exists());
        assert_eq!(std::fs::read_to_string(fpath.final_path())?, "abcde");

        Ok(())
    }

    #[test]
    fn test_finish_final() -> anyhow::Result<()> {
        let (server, _temp, dir) = setup_server_with_dir()?;

        let fpath = LogicalPath::new(&dir, &PathBuf::from("finish_partial.txt"));
        fs::write(fpath.final_path(), "abcde")?;

        server.finish(dir.id().clone(), fpath.relative_path().to_path_buf())?;

        assert!(fpath.final_path().exists());
        assert!(!fpath.partial_path().exists());
        assert_eq!(std::fs::read_to_string(fpath.final_path())?, "abcde");

        Ok(())
    }
}
