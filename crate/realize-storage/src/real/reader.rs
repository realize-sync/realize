use std::io::{self, SeekFrom};
use std::pin::Pin;
use std::task::{Context, Poll};

use super::index::RealIndexAsync;
use crate::StorageError;
use realize_types::{self, Hash, UnixTime};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

/// A handle on a filesystem file, with a known hash.
pub struct Reader {
    index: RealIndexAsync,
    path: realize_types::Path,
    file: File,
}

impl Reader {
    pub(crate) async fn open(
        index: &RealIndexAsync,
        root: &std::path::Path,
        path: &realize_types::Path,
    ) -> Result<Self, StorageError> {
        // The file must exist in the index
        index.get_file(path).await?.ok_or(StorageError::NotFound)?;

        let realpath = path.within(root);
        let file = File::open(realpath).await?;
        Ok(Self {
            index: index.clone(),
            path: path.clone(),
            file,
        })
    }

    // Get metadata for the file.
    //
    // The hash might not be available if the file has just been
    // updated locally.
    pub async fn metadata(&self) -> Result<(u64, Option<Hash>), StorageError> {
        let (m, entry) = tokio::join!(self.file.metadata(), self.index.get_file(&self.path));
        let m = m?;

        // The index and file might not be consistent if the file has
        // been recently updated (maybe even while reading!).
        let hash = if let Ok(Some(entry)) = entry
            && entry.size == m.len()
            && entry.mtime == UnixTime::mtime(&m)
        {
            Some(entry.hash)
        } else {
            None
        };

        Ok((m.len(), hash))
    }
}

impl AsyncRead for Reader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.file).poll_read(cx, buf)
    }
}

impl AsyncSeek for Reader {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        Pin::new(&mut self.file).start_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Pin::new(&mut self.file).poll_complete(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::hash;
    use assert_fs::TempDir;
    use assert_fs::fixture::ChildPath;
    use assert_fs::prelude::*;
    use realize_types::Arena;
    use tokio::fs;
    use tokio::io::AsyncReadExt as _;

    fn test_arena() -> Arena {
        Arena::from("arena")
    }

    struct Fixture {
        index: RealIndexAsync,
        root: ChildPath,
        _tempdir: TempDir,
    }

    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();

            let tempdir = TempDir::new()?;
            let root = tempdir.child("root");
            root.create_dir_all()?;

            let index =
                RealIndexAsync::open(test_arena(), tempdir.child("index.db").path()).await?;

            Ok(Self {
                index,
                root,
                _tempdir: tempdir,
            })
        }

        async fn add_file(
            &self,
            path_str: &str,
            content: &str,
        ) -> anyhow::Result<(realize_types::Path, Hash)> {
            let path = realize_types::Path::parse(path_str)?;
            let hash = hash::digest(content);
            let child = self.root.child(path_str);
            child.write_str(content)?;
            let m = fs::metadata(child.path()).await?;
            self.index
                .add_file(
                    &path,
                    content.len() as u64,
                    &UnixTime::mtime(&m),
                    hash.clone(),
                )
                .await?;

            Ok((path, hash))
        }
    }

    #[tokio::test]
    async fn read_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let root = &fixture.root;
        let (path, _) = fixture.add_file("foo/bar.txt", "foobar").await?;

        let mut reader = Reader::open(&fixture.index, root.path(), &path).await?;
        let mut str = String::new();
        reader.read_to_string(&mut str).await?;
        assert_eq!("foobar", str.as_str());

        Ok(())
    }

    #[tokio::test]
    async fn read_file_metadata() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let root = &fixture.root;
        let (path, hash) = fixture.add_file("foo/bar.txt", "foobar").await?;

        let reader = Reader::open(&fixture.index, root.path(), &path).await?;
        assert_eq!((6, Some(hash)), reader.metadata().await?);

        Ok(())
    }

    #[tokio::test]
    async fn read_file_modified_after_open() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let root = &fixture.root;
        let (path, hash) = fixture.add_file("foo/bar.txt", "foobar").await?;

        let reader = Reader::open(&fixture.index, root.path(), &path).await?;

        assert_eq!((6, Some(hash)), reader.metadata().await?);

        let (_, new_hash) = fixture.add_file("foo/bar.txt", "new data").await?;

        assert_eq!((8, Some(new_hash)), reader.metadata().await?);

        Ok(())
    }

    #[tokio::test]
    async fn read_file_index_inconsistent_with_fs() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let root = &fixture.root;
        let (path, _) = fixture.add_file("foo/bar.txt", "foobar").await?;
        root.child("foo/bar.txt").write_str("new data")?;

        let reader = Reader::open(&fixture.index, root.path(), &path).await?;

        assert_eq!((8, None), reader.metadata().await?);

        Ok(())
    }

    #[tokio::test]
    async fn file_missing() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let root = &fixture.root;
        assert!(matches!(
            Reader::open(
                &fixture.index,
                root.path(),
                &realize_types::Path::parse("doesnotexist")?
            )
            .await,
            Err(StorageError::NotFound)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn file_missing_in_index() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let root = &fixture.root;
        root.child("fs_only").write_str("that's not enough")?;

        assert!(matches!(
            Reader::open(
                &fixture.index,
                root.path(),
                &realize_types::Path::parse("fs_only")?
            )
            .await,
            Err(StorageError::NotFound)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn file_missing_on_filesystem() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let root = &fixture.root;

        let (path, _) = fixture.add_file("foo/bar.txt", "foobar").await?;
        fs::remove_file(root.child("foo/bar.txt").path()).await?;

        assert!(matches!(
            Reader::open(&fixture.index, root.path(), &path).await,
            Err(StorageError::Io(e)) if e.kind() == io::ErrorKind::NotFound
        ));

        Ok(())
    }
}
