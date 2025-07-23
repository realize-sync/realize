use crate::{
    global::cache_capnp,
    utils::holder::{ByteConversionError, ByteConvertible, NamedType},
};
use capnp::message::ReaderOptions;
use capnp::serialize_packed;
use realize_types::{self, ByteRanges};
use redb::{Key, TypeName, Value};

#[derive(Debug, Clone, PartialEq)]
pub struct BlobTableEntry {
    pub written_areas: realize_types::ByteRanges,
}

impl NamedType for BlobTableEntry {
    fn typename() -> &'static str {
        "BlobTableEntry"
    }
}

impl ByteConvertible<BlobTableEntry> for BlobTableEntry {
    fn from_bytes(data: &[u8]) -> Result<BlobTableEntry, ByteConversionError> {
        let message_reader = serialize_packed::read_message(&mut &data[..], ReaderOptions::new())?;
        let reader: cache_capnp::blob_table_entry::Reader =
            message_reader.get_root::<cache_capnp::blob_table_entry::Reader>()?;

        Ok(BlobTableEntry {
            written_areas: parse_byte_ranges(reader.get_written_areas()?)?,
        })
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let builder: cache_capnp::blob_table_entry::Builder =
            message.init_root::<cache_capnp::blob_table_entry::Builder>();

        fill_byte_ranges(&self.written_areas, builder.init_written_areas());

        let mut buffer: Vec<u8> = Vec::new();
        serialize_packed::write_message(&mut buffer, &message)?;

        Ok(buffer)
    }
}

fn parse_byte_ranges(
    msg: cache_capnp::byte_ranges::Reader<'_>,
) -> Result<ByteRanges, capnp::Error> {
    let ranges_reader = msg.get_ranges()?;
    let mut ranges = Vec::new();
    for i in 0..ranges_reader.len() {
        let range = ranges_reader.get(i);
        ranges.push(realize_types::ByteRange::new(
            range.get_start(),
            range.get_end(),
        ));
    }

    Ok(realize_types::ByteRanges::from_ranges(ranges))
}

fn fill_byte_ranges(ranges: &ByteRanges, builder: cache_capnp::byte_ranges::Builder) {
    let ranges: Vec<_> = ranges.iter().collect();
    let mut ranges_builder = builder.init_ranges(ranges.len() as u32);
    for (i, range) in ranges.iter().enumerate() {
        let mut range_builder = ranges_builder.reborrow().get(i as u32);
        range_builder.set_start(range.start);
        range_builder.set_end(range.end);
    }
}

/// A wrapper around u64 representing an blobId.
///
/// This type can be used as a key or value in redb database schemas.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobId(pub u64);

impl BlobId {
    pub const ZERO: BlobId = BlobId(0);
    pub const MAX: BlobId = BlobId(u64::MAX);

    /// Create a new BlobId from a u64 value.
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// Get the underlying u64 value.
    pub fn value(&self) -> u64 {
        self.0
    }

    pub fn plus(&self, val: u64) -> BlobId {
        BlobId(self.0 + val)
    }

    pub fn minus(&self, val: u64) -> BlobId {
        BlobId(self.0 - val)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn is_invalid(&self) -> bool {
        self.0 == 0
    }

    pub fn as_optional(blob_id: u64) -> Option<BlobId> {
        if blob_id == 0 {
            None
        } else {
            Some(BlobId(blob_id))
        }
    }

    pub fn from_optional(blob_id: Option<BlobId>) -> u64 {
        blob_id.map(|b| b.0).unwrap_or(0)
    }
}

impl From<u64> for BlobId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<BlobId> for u64 {
    fn from(value: BlobId) -> Self {
        value.0
    }
}

impl AsRef<u64> for BlobId {
    fn as_ref(&self) -> &u64 {
        &self.0
    }
}

impl AsMut<u64> for BlobId {
    fn as_mut(&mut self) -> &mut u64 {
        &mut self.0
    }
}

impl std::fmt::Display for BlobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl std::fmt::Debug for BlobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BlobId")
            .field(&format!("{:016x}", &self.0))
            .finish()
    }
}

impl Key for BlobId {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        let value1 = u64::from_le_bytes(data1.try_into().unwrap_or([0; 8]));
        let value2 = u64::from_le_bytes(data2.try_into().unwrap_or([0; 8]));
        value1.cmp(&value2)
    }
}

impl Value for BlobId {
    type SelfType<'a> = BlobId;
    type AsBytes<'a>
        = [u8; 8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(8)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> BlobId
    where
        Self: 'a,
    {
        BlobId(<u64>::from_le_bytes(data.try_into().unwrap_or([0; 8])))
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> [u8; 8]
    where
        Self: 'a,
        Self: 'b,
    {
        value.0.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::new("BlobId")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn convert_blob_table_entry() -> anyhow::Result<()> {
        let entry = BlobTableEntry {
            written_areas: realize_types::ByteRanges::from_ranges(vec![
                realize_types::ByteRange::new(0, 1024),
                realize_types::ByteRange::new(2048, 4096),
            ]),
        };

        assert_eq!(
            entry,
            BlobTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }

    #[test]
    fn test_blobid_display() {
        let blobid = BlobId::new(0xd903);
        assert_eq!(blobid.to_string(), "000000000000d903");
    }
    #[test]
    fn test_blobid_redb_key() {
        let blobid1 = BlobId::new(100);
        let blobid2 = BlobId::new(200);
        let blobid3 = BlobId::new(100);

        let data1 = BlobId::as_bytes(&blobid1);
        let data2 = BlobId::as_bytes(&blobid2);
        let data3 = BlobId::as_bytes(&blobid3);

        assert_eq!(BlobId::compare(&data1, &data2), std::cmp::Ordering::Less);
        assert_eq!(BlobId::compare(&data2, &data1), std::cmp::Ordering::Greater);
        assert_eq!(BlobId::compare(&data1, &data3), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_blobid_redb_value() {
        let original = BlobId::new(12345);
        let bytes = BlobId::as_bytes(&original);
        let restored = BlobId::from_bytes(&bytes);

        assert_eq!(original, restored);
    }

    #[test]
    fn test_blobid_redb_value_edge_cases() {
        // Test zero
        let zero = BlobId::new(0);
        let zero_bytes = BlobId::as_bytes(&zero);
        let zero_restored = BlobId::from_bytes(&zero_bytes);
        assert_eq!(zero, zero_restored);

        // Test maximum u64 value
        let max = BlobId::new(u64::MAX);
        let max_bytes = BlobId::as_bytes(&max);
        let max_restored = BlobId::from_bytes(&max_bytes);
        assert_eq!(max, max_restored);
    }

    #[test]
    fn test_blobid_redb_value_invalid_data() {
        // Test with insufficient data (should handle gracefully)
        let short_data = &[1, 2, 3]; // Less than 8 bytes
        let restored = BlobId::from_bytes(short_data);
        // Should default to 0 or handle gracefully
        assert_eq!(restored.value(), 0);

        // Test with exactly 8 bytes
        let valid_data = &[1, 0, 0, 0, 0, 0, 0, 0]; // Little endian 1
        let restored = BlobId::from_bytes(valid_data);
        assert_eq!(restored.value(), 1);
    }
}
