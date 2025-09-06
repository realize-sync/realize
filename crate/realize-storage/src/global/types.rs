use crate::types::PathId;
use crate::utils::holder::{ByteConversionError, ByteConvertible, NamedType};
use capnp::message::ReaderOptions;
use capnp::serialize_packed;
use realize_types::UnixTime;

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
pub struct PathTableEntry {
    /// Directory or arena pathid
    pub pathid: PathId,
    /// The modification time when the directory was created.
    pub mtime: UnixTime,
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

        let mtime = msg.get_mtime()?;
        Ok(PathTableEntry {
            pathid: PathId(msg.get_pathid()),
            mtime: UnixTime::new(mtime.get_secs(), mtime.get_nsecs()),
        })
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let mut builder: cache_capnp::path_table_entry::Builder =
            message.init_root::<cache_capnp::path_table_entry::Builder>();

        builder.set_pathid(self.pathid.as_u64());
        let mut mtime = builder.init_mtime();
        mtime.set_secs(self.mtime.as_secs());
        mtime.set_nsecs(self.mtime.subsec_nanos());

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
            pathid: PathId(442),
            mtime: UnixTime::from_secs(1234567890),
        };

        assert_eq!(
            entry,
            PathTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );
        Ok(())
    }
}
