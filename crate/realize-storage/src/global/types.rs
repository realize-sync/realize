use crate::Inode;
use crate::types::InodePrefix;
use crate::utils::holder::{ByteConversionError, ByteConvertible, NamedType};
use capnp::message::ReaderOptions;
use capnp::serialize_packed;
use realize_types::UnixTime;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::PathBuf;

#[allow(dead_code)]
#[allow(unknown_lints)]
#[allow(clippy::uninlined_format_args)]
#[allow(clippy::extra_unused_type_parameters)]
mod cache_capnp {
    include!(concat!(env!("OUT_DIR"), "/global/cache_capnp.rs"));
}

/// The type of an pathid.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum PathAssignment {
    /// The pathid of a file, look it up in the file table.
    File,
    /// The pathid of a directory, look it up in the directory table.
    ///
    /// Note that an empty directory won't have any entries in
    /// the directory table.
    Directory,
}

/// An entry in the path table.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PathTableEntry {
    /// Directory modification time
    pub(crate) mtime: UnixTime,

    /// Inode of subdirs or arena.
    pub(crate) subdirs: BTreeMap<String, Inode>,
}

impl PathTableEntry {
    /// Create a new entry with current time as mtime
    pub(crate) fn new() -> Self {
        PathTableEntry {
            mtime: UnixTime::now(),
            subdirs: BTreeMap::new(),
        }
    }
}

impl NamedType for PathTableEntry {
    fn typename() -> &'static str {
        "PathTableEntry"
    }
}

impl ByteConvertible<PathTableEntry> for PathTableEntry {
    fn from_bytes(data: &[u8]) -> Result<PathTableEntry, ByteConversionError> {
        let message_reader = serialize_packed::read_message(&mut &data[..], ReaderOptions::new())?;
        let msg: cache_capnp::path_table_entry::Reader =
            message_reader.get_root::<cache_capnp::path_table_entry::Reader>()?;

        let mtime_reader = msg.get_mtime()?;
        let mtime = UnixTime::new(mtime_reader.get_secs(), mtime_reader.get_nsecs());
        let mut subdirs = BTreeMap::new();
        for subdir in msg.get_subdirs()?.iter() {
            subdirs.insert(subdir.get_name()?.to_string()?, Inode(subdir.get_inode()));
        }
        return Ok(PathTableEntry { mtime, subdirs });
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let mut builder: cache_capnp::path_table_entry::Builder =
            message.init_root::<cache_capnp::path_table_entry::Builder>();

        let mut mtime_builder = builder.reborrow().init_mtime();
        mtime_builder.set_secs(self.mtime.as_secs());
        mtime_builder.set_nsecs(self.mtime.subsec_nanos());
        let mut subdir_list = builder.init_subdirs(self.subdirs.len() as u32);
        for (i, (name, inode)) in self.subdirs.iter().enumerate() {
            let mut entry = subdir_list.reborrow().get(i as u32);
            entry.set_name(name);
            entry.set_inode(inode.as_u64());
        }

        let mut buffer: Vec<u8> = Vec::new();
        serialize_packed::write_message(&mut buffer, &message)?;

        Ok(buffer)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ArenaTableEntry {
    pub(crate) prefix: InodePrefix,
    pub(crate) datadir: PathBuf,
}

impl NamedType for ArenaTableEntry {
    fn typename() -> &'static str {
        "ArenaTableEntry"
    }
}

impl ByteConvertible<ArenaTableEntry> for ArenaTableEntry {
    fn from_bytes(data: &[u8]) -> Result<ArenaTableEntry, ByteConversionError> {
        let message_reader = serialize_packed::read_message(&mut &data[..], ReaderOptions::new())?;
        let msg: cache_capnp::arena_table_entry::Reader =
            message_reader.get_root::<cache_capnp::arena_table_entry::Reader>()?;

        let prefix = InodePrefix::from_u8(msg.get_prefix());
        let datadir = PathBuf::from(OsString::from_vec(msg.get_datadir()?.into()));

        return Ok(ArenaTableEntry { prefix, datadir });
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let mut builder: cache_capnp::arena_table_entry::Builder =
            message.init_root::<cache_capnp::arena_table_entry::Builder>();

        builder.set_prefix(self.prefix.as_u8());
        builder.set_datadir(&self.datadir.as_os_str().as_bytes());

        let mut buffer: Vec<u8> = Vec::new();
        serialize_packed::write_message(&mut buffer, &message)?;

        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn convert_path_table_entry() -> anyhow::Result<()> {
        let entry = PathTableEntry {
            mtime: UnixTime::now(),
            subdirs: BTreeMap::from([
                ("foo".to_string(), Inode(12)),
                ("bar".to_string(), Inode(21)),
            ]),
        };
        assert_eq!(
            entry,
            PathTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );
        Ok(())
    }

    #[test]
    fn convert_arena_table_entry() -> anyhow::Result<()> {
        let entry = ArenaTableEntry {
            prefix: InodePrefix::from_u8(9),
            datadir: PathBuf::from("/datadir"),
        };
        assert_eq!(
            entry,
            ArenaTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );
        Ok(())
    }
}
