use std::borrow::Cow;
use std::sync::Arc;

use realize_types::Hash;

use crate::arena::cache::{CacheExt, CacheReadOperations};
use crate::arena::db::ArenaDatabase;
use crate::arena::fs::ArenaFsLoc;
use crate::arena::mark::MarkExt;
use crate::arena::tree::{TreeExt, TreeReadOperations};
use crate::arena::types::FileAlternative;
use crate::config::BytesOrPercent;
use crate::{CacheStatus, FileRealm, Mark, StorageError, Version};

const XATTR_MARK: &str = "realize.mark";
const XATTR_STATUS: &str = "realize.status";
const XATTR_VERSION: &str = "realize.version";
const XATTR_VERSIONS: &str = "realize.versions";
const XATTR_QUOTA_MAX: &str = "realize.quota.max";
const XATTR_QUOTA_LEAVE: &str = "realize.quota.leave";

pub(crate) fn list(
    db: &Arc<ArenaDatabase>,
    loc: impl Into<ArenaFsLoc>,
) -> Result<Vec<&'static str>, StorageError> {
    let txn = db.begin_read()?;
    let tree = txn.read_tree()?;
    let cache = txn.read_cache()?;
    let pathid = tree.expect(loc.into().into_tree_loc(&cache)?)?;
    if pathid == tree.root() {
        return Ok(vec![XATTR_QUOTA_MAX, XATTR_QUOTA_LEAVE]);
    }
    match cache
        .metadata(&tree, pathid)?
        .ok_or(StorageError::NotFound)?
    {
        crate::Metadata::Dir(_) => Ok(vec![XATTR_MARK]),
        crate::Metadata::File(_) => Ok(vec![
            XATTR_MARK,
            XATTR_STATUS,
            XATTR_VERSION,
            XATTR_VERSIONS,
        ]),
    }
}

pub(crate) fn get(
    db: &Arc<ArenaDatabase>,
    loc: impl Into<ArenaFsLoc>,
    xattr: &str,
) -> Result<String, StorageError> {
    if xattr == XATTR_MARK {
        let txn = db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        let marks = txn.read_marks()?;
        let loc = loc.into().into_tree_loc(&cache)?;

        let (mark, direct) = marks.get_full(&tree, loc)?;
        if direct {
            return Ok(mark.to_string());
        }
        return Ok(format!("{} (derived)", mark));
    }

    if xattr == XATTR_STATUS {
        let txn = db.begin_read()?;
        let tree = txn.read_tree()?;
        let blobs = txn.read_blobs()?;
        let cache = txn.read_cache()?;
        let loc = loc.into().into_tree_loc(&cache)?;
        return Ok(match cache.file_realm(&tree, &blobs, loc)? {
            FileRealm::Local(_) => "local 100%".to_string(),
            FileRealm::Remote(CacheStatus::Missing) => "remote 0%".to_string(),
            FileRealm::Remote(CacheStatus::Complete) => "remote 100%".to_string(),
            FileRealm::Remote(CacheStatus::Verified) => "remote 100% verified".to_string(),
            FileRealm::Remote(CacheStatus::Partial(size, available_ranges)) => {
                format!(
                    "remote {:0.0}%",
                    (available_ranges.bytecount() as f64) / (size as f64) * 100.0
                )
            }
        });
    }

    if xattr == XATTR_VERSION {
        let txn = db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        let m = cache.file_metadata(&tree, loc.into().into_tree_loc(&cache)?)?;
        return Ok(match m.version {
            Version::Modified(_) => "modified".to_string(),
            Version::Indexed(hash) => hash.to_string(),
        });
    }

    if xattr == XATTR_VERSIONS {
        let txn = db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        let loc = loc.into().into_tree_loc(&cache)?;

        let alternatives = cache.list_alternatives(&tree, loc)?;
        return Ok(format_versions(&alternatives));
    }

    if xattr == XATTR_QUOTA_MAX || xattr == XATTR_QUOTA_LEAVE {
        let txn = db.begin_read()?;
        let tree = txn.read_tree()?;
        let cache = txn.read_cache()?;
        let pathid = tree.expect(loc.into().into_tree_loc(&cache)?)?;
        if pathid != tree.root() {
            return Err(StorageError::NoSuchAttribute);
        }
        let disk_usage = db.settings().borrow().disk_usage.clone();
        let val = if xattr == XATTR_QUOTA_MAX {
            disk_usage.max
        } else {
            disk_usage.leave
        };
        return Ok(match val {
            None => "".to_string(),
            Some(bop) => bop.to_string(),
        });
    }

    Err(StorageError::NoSuchAttribute)
}

pub(crate) fn set(
    db: &Arc<ArenaDatabase>,
    loc: impl Into<ArenaFsLoc>,
    name: &str,
    value: Cow<'_, str>,
) -> Result<(), StorageError> {
    if name == XATTR_MARK {
        let mark = if value.is_empty() {
            None
        } else {
            Some(Mark::parse(value.as_ref()).ok_or(StorageError::InvalidAttributeValue)?)
        };

        let txn = db.begin_write()?;
        {
            let mut marks = txn.write_marks()?;
            let mut tree = txn.write_tree()?;
            let mut dirty = txn.write_dirty()?;
            let cache = txn.read_cache()?;
            let loc = loc.into().into_tree_loc(&cache)?;
            match mark {
                None => marks.clear(&mut tree, &mut dirty, loc)?,
                Some(mark) => marks.set(&mut tree, &mut dirty, loc, mark)?,
            }
        }
        txn.commit()?;
        return Ok(());
    }

    if name == XATTR_VERSION {
        let hash = Hash::from_base64(value.as_ref()).ok_or(StorageError::InvalidAttributeValue)?;

        let txn = db.begin_write()?;
        {
            let mut tree = txn.write_tree()?;
            let mut blobs = txn.write_blobs()?;
            let mut history = txn.write_history()?;
            let mut dirty = txn.write_dirty()?;
            let mut cache = txn.write_cache()?;
            let loc = loc.into().into_tree_loc(&cache)?;

            cache.select_alternative(
                &mut tree,
                &mut blobs,
                &mut history,
                &mut dirty,
                loc,
                &hash,
            )?;
        }
        txn.commit()?;
        return Ok(());
    }

    if name == XATTR_QUOTA_MAX || name == XATTR_QUOTA_LEAVE {
        let value = if value.is_empty() {
            None
        } else {
            Some(
                BytesOrPercent::parse(value.as_ref())
                    .map_err(|_| StorageError::InvalidAttributeValue)?,
            )
        };

        let txn = db.begin_write()?;
        {
            let tree = txn.read_tree()?;
            let cache = txn.read_cache()?;
            let pathid = tree.expect(loc.into().into_tree_loc(&cache)?)?;
            if pathid != tree.root() {
                return Err(StorageError::NoSuchAttribute);
            }

            let mut settings = txn.write_settings()?;
            let mut disk_usage = settings.load()?.disk_usage;
            if name == XATTR_QUOTA_MAX {
                disk_usage.max = value;
            } else {
                disk_usage.leave = value;
            }
            settings.configure_disk_usage(&disk_usage)?;
        }
        txn.commit()?;
        return Ok(());
    }

    Err(StorageError::NoSuchAttribute)
}

/// Format a list of FileAlternatives as a single string with
/// newline-separated entries as output for the xattr
/// realize.versions.
///
/// Each line represents one alternative, ending with a newline.
/// Empty input produces empty string.
fn format_versions(alts: &[FileAlternative]) -> String {
    if alts.is_empty() {
        return String::new();
    }

    let mut result = String::new();
    for alt in alts {
        result.push_str(&format_alternative(alt));
        result.push('\n');
    }

    result
}

/// Format a single FileAlternative for display.
fn format_alternative(alt: &FileAlternative) -> String {
    match alt {
        FileAlternative::Local(version) => match version {
            Version::Modified(Some(hash)) => format!("local {}", hash),
            Version::Modified(None) => "local modified".to_string(),
            Version::Indexed(hash) => format!("local {}", hash),
        },
        FileAlternative::Branched(path, hash) => {
            format!("branched {} {}", path, hash)
        }
        FileAlternative::Remote(peer, hash, size, mtime) => {
            format!("{} {} {} {}", peer, hash, size, mtime.display())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arena::index;
    use crate::config::{BytesOrPercent, DiskUsageConfig};
    use crate::{Blob, Notification, arena::update, utils::hash};
    use assert_fs::TempDir;
    use assert_fs::fixture::ChildPath;
    use assert_fs::prelude::{FileWriteStr, PathChild, PathCreateDir};
    use realize_types::{Arena, Hash, Path, Peer, UnixTime};

    struct Fixture {
        db: Arc<ArenaDatabase>,
        datadir: ChildPath,
        _tempdir: TempDir,
    }

    impl Fixture {
        fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();
            let tempdir = TempDir::new()?;
            let datadir = tempdir.child("data");
            datadir.create_dir_all()?;
            let blobdir = tempdir.child("blobs");
            blobdir.create_dir_all()?;
            let db =
                ArenaDatabase::for_testing(Arena::from("myarena"), blobdir.path(), datadir.path())?;

            Ok(Self {
                db,
                datadir,
                _tempdir: tempdir,
            })
        }

        fn add_to_cache<T>(&self, path: T, peer: Peer, content: &str) -> anyhow::Result<()>
        where
            T: AsRef<Path>,
        {
            let path = path.as_ref();
            update::apply(
                &self.db,
                peer,
                Notification::Add {
                    arena: self.db.arena(),
                    index: 1,
                    path: path.clone(),
                    mtime: UnixTime::from_secs(1234567890),
                    size: content.len() as u64,
                    hash: hash::digest(content),
                },
            )?;

            Ok(())
        }

        fn add_to_index<T>(&self, path: T, content: &str) -> anyhow::Result<()>
        where
            T: AsRef<Path>,
        {
            let child = self.datadir.child(path.as_ref().to_string());
            child.write_str(content)?;
            index::add_file(
                &self.db,
                path.as_ref(),
                content.len() as u64,
                UnixTime::mtime(&child.metadata()?),
                hash::digest(content),
            )?;

            Ok(())
        }

        fn mkdir<T>(&self, path: T) -> anyhow::Result<()>
        where
            T: AsRef<Path>,
        {
            let txn = self.db.begin_write()?;
            {
                let mut tree = txn.write_tree()?;
                let mut cache = txn.write_cache()?;
                cache.mkdir(&mut tree, path.as_ref())?;
            }
            txn.commit()?;

            Ok(())
        }
    }

    #[test]
    fn list_remote_file_xattr() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("remote")?;
        fixture.add_to_cache(&path, Peer::from("peer"), "test")?;

        assert_unordered::assert_eq_unordered!(
            vec![
                "realize.mark",
                "realize.status",
                "realize.version",
                "realize.versions"
            ],
            super::list(&fixture.db, &path)?
        );

        Ok(())
    }

    #[test]
    fn list_local_file_xattr() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("local")?;
        fixture.add_to_index(&path, "test")?;

        assert_unordered::assert_eq_unordered!(
            vec![
                "realize.mark",
                "realize.status",
                "realize.version",
                "realize.versions"
            ],
            super::list(&fixture.db, &path)?
        );

        Ok(())
    }

    #[test]
    fn list_dir_xattr() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("dir")?;
        fixture.mkdir(&path)?;

        assert_eq!(vec!["realize.mark"], super::list(&fixture.db, &path)?);

        Ok(())
    }

    #[test]
    fn list_root_xattr() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        assert_unordered::assert_eq_unordered!(
            vec!["realize.quota.max", "realize.quota.leave"],
            super::list(&fixture.db, Path::root())?
        );

        Ok(())
    }

    #[test]
    fn no_such_attribute() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let file = Path::parse("file")?;
        fixture.add_to_cache(&file, Peer::from("peer"), "test")?;

        assert!(matches!(
            super::get(&fixture.db, &file, "realize.doesnotexist"),
            Err(StorageError::NoSuchAttribute)
        ));
        assert!(matches!(
            super::set(&fixture.db, &file, "realize.doesnotexist", "value".into()),
            Err(StorageError::NoSuchAttribute)
        ));

        Ok(())
    }

    #[test]
    fn get_and_set_mark() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let dir = Path::parse("dir")?;
        let file = dir.join("file")?;
        fixture.add_to_cache(&file, Peer::from("peer"), "test")?;

        assert_eq!(
            "watch (derived)",
            super::get(&fixture.db, Path::root(), "realize.mark")?.as_str(),
        );
        assert_eq!(
            "watch (derived)",
            super::get(&fixture.db, &dir, "realize.mark")?.as_str(),
        );
        assert_eq!(
            "watch (derived)",
            super::get(&fixture.db, &file, "realize.mark")?.as_str(),
        );

        super::set(&fixture.db, &dir, "realize.mark", "own".into())?;

        assert_eq!(
            "watch (derived)",
            super::get(&fixture.db, Path::root(), "realize.mark")?.as_str(),
        );
        assert_eq!(
            "own",
            super::get(&fixture.db, &dir, "realize.mark")?.as_str(),
        );
        assert_eq!(
            "own (derived)",
            super::get(&fixture.db, &file, "realize.mark")?.as_str(),
        );

        super::set(&fixture.db, Path::root(), "realize.mark", "keep".into())?;

        assert_eq!(
            "keep",
            super::get(&fixture.db, Path::root(), "realize.mark")?.as_str(),
        );
        assert_eq!(
            "own",
            super::get(&fixture.db, &dir, "realize.mark")?.as_str(),
        );
        assert_eq!(
            "own (derived)",
            super::get(&fixture.db, &file, "realize.mark")?.as_str(),
        );

        super::set(&fixture.db, &dir, "realize.mark", "".into())?;

        assert_eq!(
            "keep",
            super::get(&fixture.db, Path::root(), "realize.mark")?.as_str(),
        );
        assert_eq!(
            "keep (derived)",
            super::get(&fixture.db, &dir, "realize.mark")?.as_str(),
        );
        assert_eq!(
            "keep (derived)",
            super::get(&fixture.db, &file, "realize.mark")?.as_str(),
        );

        Ok(())
    }

    #[test]
    fn get_and_set_version() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let file = Path::parse("file")?;
        fixture.add_to_cache(&file, Peer::from("peer1"), "one")?;
        fixture.add_to_cache(&file, Peer::from("peer2"), "two")?;
        fixture.add_to_cache(&file, Peer::from("peer3"), "three")?;
        fixture.add_to_index(&file, "local")?;

        assert_eq!(
            hash::digest("local").to_string(),
            super::get(&fixture.db, &file, "realize.version")?
        );

        assert_eq!(
            "local phJYEP8TihveNo6aOJCxLxq34AAOhSYayisOMnod+Kc
peer1 0BwGywlga8Syzt9CZqOiDh5ja0Z8t3qdvtWVCU4C6kY 3 2009-02-13T23:31:30.000
peer2 elKyUaNCihtklnhRFXBH0ikZzGEMaxOOmKJSgLFTSyE 3 2009-02-13T23:31:30.000
peer3 SEvM9qZv1GZ4MupKxpAmzX5v8x5LdB0KG9x0CvykQWw 5 2009-02-13T23:31:30.000
",
            super::get(&fixture.db, &file, "realize.versions")?
        );

        super::set(
            &fixture.db,
            &file,
            "realize.version",
            hash::digest("two").to_string().into(),
        )?;

        assert_eq!(
            hash::digest("two").to_string(),
            super::get(&fixture.db, &file, "realize.version")?
        );

        assert_eq!(
            "peer1 0BwGywlga8Syzt9CZqOiDh5ja0Z8t3qdvtWVCU4C6kY 3 2009-02-13T23:31:30.000
peer2 elKyUaNCihtklnhRFXBH0ikZzGEMaxOOmKJSgLFTSyE 3 2009-02-13T23:31:30.000
peer3 SEvM9qZv1GZ4MupKxpAmzX5v8x5LdB0KG9x0CvykQWw 5 2009-02-13T23:31:30.000
",
            super::get(&fixture.db, &file, "realize.versions")?
        );

        Ok(())
    }

    #[test]
    fn get_and_set_version_not_supported() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let dir = Path::parse("dir")?;
        fixture.mkdir(&dir)?;
        let root = Path::root();

        for path in [&dir, &root] {
            let ret = super::get(&fixture.db, path, "realize.version");
            assert!(
                matches!(ret, Err(StorageError::IsADirectory)),
                "{ret:?} on {path:?}"
            );

            let ret = super::get(&fixture.db, path, "realize.versions");
            assert_eq!("", ret.unwrap(), "on {path:?}");

            let ret = super::set(
                &fixture.db,
                path,
                "realize.version",
                hash::digest("test").to_string().into(),
            );
            assert!(
                matches!(ret, Err(StorageError::IsADirectory)),
                "{ret:?} on {path:?}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn get_remote_file_status() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("remote")?;
        fixture.add_to_cache(&path, Peer::from("peer"), "test")?;

        assert_eq!(
            "remote 0%",
            super::get(&fixture.db, &path, "realize.status")?
        );

        let txn = fixture.db.begin_write()?;
        {
            let marks = txn.read_marks()?;
            let mut blobs = txn.write_blobs()?;
            let mut tree = txn.write_tree()?;
            let mut cache = txn.write_cache()?;

            cache.create_blob(&mut tree, &mut blobs, &marks, &path)?;
        }
        txn.commit()?;

        let mut blob = Blob::open(&fixture.db, &path).unwrap();
        blob.update(0, b"te").await.unwrap();
        blob.update_db().await.unwrap();

        assert_eq!(
            "remote 50%",
            super::get(&fixture.db, &path, "realize.status")?
        );

        Ok(())
    }

    #[test]
    fn get_local_file_status() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let path = Path::parse("local")?;
        fixture.add_to_index(&path, "test")?;

        assert_eq!(
            "local 100%",
            super::get(&fixture.db, &path, "realize.status")?
        );

        Ok(())
    }

    #[test]
    fn get_status_not_supported() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let dir = Path::parse("dir")?;
        fixture.mkdir(&dir)?;
        let root = Path::root();

        for path in [&dir, &root] {
            let ret = super::get(&fixture.db, path, "realize.status");
            assert!(
                matches!(ret, Err(StorageError::IsADirectory)),
                "{ret:?} on {path:?}"
            );
        }

        Ok(())
    }

    #[test]
    fn get_and_set_quota() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        assert_eq!(
            "",
            super::get(&fixture.db, Path::root(), "realize.quota.max")?
        );
        assert_eq!(
            "",
            super::get(&fixture.db, Path::root(), "realize.quota.leave")?
        );

        super::set(
            &fixture.db,
            Path::root(),
            "realize.quota.leave",
            "15%".into(),
        )?;
        super::set(&fixture.db, Path::root(), "realize.quota.max", "20M".into())?;
        assert_eq!(
            DiskUsageConfig {
                max: Some(BytesOrPercent::Bytes(20 * 1024 * 1024)),
                leave: Some(BytesOrPercent::Percent(15))
            },
            fixture.db.settings().borrow().disk_usage
        );

        super::set(&fixture.db, Path::root(), "realize.quota.max", "".into())?;
        super::set(&fixture.db, Path::root(), "realize.quota.leave", "".into())?;
        assert_eq!(
            DiskUsageConfig::default(),
            fixture.db.settings().borrow().disk_usage
        );

        Ok(())
    }

    #[test]
    fn format_local_indexed() {
        let hash = Hash([1; 32]);
        let alt = FileAlternative::Local(Version::Indexed(hash));
        assert_eq!(
            format_alternative(&alt),
            "local AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE"
        );
    }

    #[test]
    fn format_local_modified_with_hash() {
        let hash = Hash([2; 32]);
        let alt = FileAlternative::Local(Version::Modified(Some(hash)));
        assert_eq!(
            format_alternative(&alt),
            "local AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI"
        );
    }

    #[test]
    fn format_local_modified_no_hash() {
        let alt = FileAlternative::Local(Version::Modified(None));
        assert_eq!(format_alternative(&alt), "local modified");
    }

    #[test]
    fn format_branched() {
        let path = Path::parse("some/path").unwrap();
        let hash = Hash([3; 32]);
        let alt = FileAlternative::Branched(path, hash);
        assert_eq!(
            format_alternative(&alt),
            "branched some/path AwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM"
        );
    }

    #[test]
    fn format_remote() {
        let peer = Peer::from("peer1");
        let hash = Hash([4; 32]);
        let size = 100;
        let mtime = UnixTime::new(1234567890, 333999111); // 2009-02-13T23:31:30.333
        let expected = format!("peer1 {} 100 2009-02-13T23:31:30.333", hash);
        let alt = FileAlternative::Remote(peer, hash, size, mtime);
        assert_eq!(format_alternative(&alt), expected);
    }

    #[test]
    fn format_versions_empty() {
        assert_eq!(format_versions(&[]), "");
    }

    #[test]
    fn format_versions_single() {
        let hash = Hash([1; 32]);
        let alt = FileAlternative::Local(Version::Indexed(hash));
        assert_eq!(
            format_versions(&[alt]),
            "local AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE\n"
        );
    }

    #[test]
    fn format_versions_multiple() {
        let local_alt = FileAlternative::Local(Version::Indexed(Hash([1; 32])));
        let remote_alt = FileAlternative::Remote(
            Peer::from("peer1"),
            Hash([2; 32]),
            200,
            UnixTime::new(1640995200, 0), // 2022-01-01T00:00:00.000
        );

        let result = format_versions(&[local_alt, remote_alt]);
        let expected = "local AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE\npeer1 AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI 200 2022-01-01T00:00:00.000\n";
        assert_eq!(result, expected);
    }
}
