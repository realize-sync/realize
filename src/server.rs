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

    fn find_directory(&self, dir_id: &DirectoryId) -> Result<&Rc<Directory>> {
        self.dirs
            .get(dir_id)
            .ok_or_else(|| RealizeError::BadRequest(format!("Unknown directory ID: '{}'", dir_id)))
    }
}

impl RealizeService for RealizeServer {
    fn list(&self, dir_id: DirectoryId) -> Result<Vec<SyncedFile>> {
        let dir = self.find_directory(&dir_id)?;
        let mut files = Vec::new();
        for entry in WalkDir::new(dir.path()).into_iter().flatten() {
            if let Some((state, logical)) = LogicalPath::from_actual(&dir, entry.path()) {
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
        let mut file = OpenOptions::new().create(true).write(true).open(&path)?;
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
        return None;
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

        return Err(std::io::Error::from(std::io::ErrorKind::NotFound).into());
    }

    /// Return the final form of the logical path.
    fn final_path(&self) -> PathBuf {
        self.0.path().join(self.1.to_path_buf())
    }

    /// Return the partial form of the logical path.
    fn partial_path(&self) -> PathBuf {
        let mut partial = self.0.path().to_path_buf();
        if let Some(p) = self.1.parent() {
            partial.push(p);
        }
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
        if let Some(_) = err.io_error() {
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
    use std::collections::HashMap;
    use std::fs;
    use std::io::Read;

    fn setup_server_with_dir() -> (RealizeServer, TempDir, DirectoryId) {
        let temp = TempDir::new().unwrap();
        let dir_id = "testdir".to_string();
        let mut dirs = HashMap::new();
        dirs.insert(dir_id.clone(), temp.path().to_path_buf());
        (RealizeServer::new(dirs), temp, dir_id)
    }

    #[test]
    fn test_list_empty() {
        let (server, _temp, dir_id) = setup_server_with_dir();
        let files = server.list(dir_id).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn test_list_files_and_partial() {
        let (server, temp, dir_id) = setup_server_with_dir();
        let file = temp.child("foo.txt");
        file.write_str("hello").unwrap();
        let part_file = temp.child("bar.txt.part");
        part_file.write_str("partial").unwrap();
        let files = server.list(dir_id).unwrap();
        let mut found_foo = false;
        let mut found_bar = false;
        for f in files {
            if f.path == PathBuf::from("foo.txt") {
                assert!(!f.is_partial);
                found_foo = true;
            }
            if f.path == PathBuf::from("bar.txt") {
                assert!(f.is_partial);
                found_bar = true;
            }
            // Path must not start with '.' or end with '.part'
            assert!(!f.path.to_string_lossy().starts_with("."));
            assert!(!f.path.to_string_lossy().ends_with(".part"));
        }
        assert!(found_foo);
        assert!(found_bar);
    }

    #[test]
    fn test_send_and_finish_partial() {
        let (server, temp, dir_id) = setup_server_with_dir();
        let data = b"abcde".to_vec();
        let path = PathBuf::from("file1.txt");
        // Send to partial file
        server
            .send(dir_id.clone(), path.clone(), (0, 5), data.clone())
            .unwrap();
        let part_path = temp.path().join("file1.txt.part");
        assert!(part_path.exists());
        let mut buf = Vec::new();
        fs::File::open(&part_path)
            .unwrap()
            .read_to_end(&mut buf)
            .unwrap();
        assert_eq!(buf, data);
        // Finish (rename to non-partial)
        server.finish(dir_id.clone(), path.clone()).unwrap();
        let final_path = temp.path().join("file1.txt");
        assert!(final_path.exists());
        assert!(!part_path.exists());
        let mut buf2 = Vec::new();
        fs::File::open(&final_path)
            .unwrap()
            .read_to_end(&mut buf2)
            .unwrap();
        assert_eq!(buf2, data);
    }

    #[test]
    fn test_send_and_finish_on_existing_nonpartial() {
        let (server, temp, dir_id) = setup_server_with_dir();
        let orig_data = b"orig";
        let path = PathBuf::from("file2.txt");
        let final_path = temp.path().join("file2.txt");
        fs::write(&final_path, orig_data).unwrap();
        // Send new data (should write to .part)
        let new_data = b"newdata".to_vec();
        server
            .send(dir_id.clone(), path.clone(), (0, 7), new_data.clone())
            .unwrap();
        let part_path = temp.path().join("file2.txt.part");
        assert!(part_path.exists());
        // Finish (should overwrite non-partial)
        server.finish(dir_id.clone(), path.clone()).unwrap();
        assert!(final_path.exists());
        let mut buf = Vec::new();
        fs::File::open(&final_path)
            .unwrap()
            .read_to_end(&mut buf)
            .unwrap();
        assert_eq!(buf, new_data);
    }
}
