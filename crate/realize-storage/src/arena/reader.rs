use super::cache::CacheExt;
use super::db::ArenaDatabase;
use crate::StorageError;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncSeekExt};
use tokio::task;

pub(crate) async fn open(
    db: &Arc<ArenaDatabase>,
    path: &realize_types::Path,
) -> Result<impl AsyncRead + AsyncSeekExt + use<>, StorageError> {
    let db = Arc::clone(db);
    let path = path.clone();

    let fh = task::spawn_blocking(move || {
        let txn = db.begin_read()?;
        let cache = txn.read_cache()?;
        let tree = txn.read_tree()?;

        let realpath = if cache.indexed(&tree, &path)?.is_some() {
            path.within(db.cache().datadir())
        } else {
            return Err(StorageError::NotFound);
        };

        Ok::<_, StorageError>(std::fs::File::open(realpath)?)
    })
    .await??;

    Ok(tokio::fs::File::from_std(fh))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arena::db::ArenaDatabase;
    use crate::arena::index;
    use crate::utils::hash;
    use assert_fs::TempDir;
    use assert_fs::fixture::ChildPath;
    use assert_fs::prelude::*;
    use realize_types::{Arena, Hash, UnixTime};
    use tokio::fs;
    use tokio::io::AsyncReadExt;

    fn test_arena() -> Arena {
        Arena::from("arena")
    }

    struct Fixture {
        db: Arc<ArenaDatabase>,
        root: ChildPath,
        _tempdir: TempDir,
    }

    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();

            let tempdir = TempDir::new()?;
            let root = tempdir.child("root");
            root.create_dir_all()?;

            let arena = test_arena();
            let db = ArenaDatabase::for_testing(arena, root.path())?;

            Ok(Self {
                db,
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
            index::add_file_async(
                &self.db,
                &path,
                content.len() as u64,
                UnixTime::mtime(&m),
                hash.clone(),
            )
            .await?;

            Ok((path, hash))
        }
    }

    #[tokio::test]
    async fn read_file() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let (path, _) = fixture.add_file("foo/bar.txt", "foobar").await?;
        let mut reader = super::open(&fixture.db, &path).await?;
        let mut str = String::new();
        reader.read_to_string(&mut str).await?;
        assert_eq!("foobar", str.as_str());

        Ok(())
    }

    #[tokio::test]
    async fn file_missing() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        assert!(matches!(
            super::open(&fixture.db, &realize_types::Path::parse("doesnotexist")?).await,
            Err(StorageError::NotFound)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn file_missing_in_index() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let root = &fixture.root;
        root.child("fs_only").write_str("that's not enough")?;

        let path = realize_types::Path::parse("fs_only")?;
        assert!(matches!(
            super::open(&fixture.db, &path,).await,
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
            super::open(&fixture.db, &path).await,
            Err(StorageError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound
        ));

        Ok(())
    }
}
