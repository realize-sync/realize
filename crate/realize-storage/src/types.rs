use redb::{Key, TypeName, Value};

/// A prefix that identifies a subset [Inode]s.
///
/// The inode subset is used to identify the arena an inode belongs
/// to. An [Inode] always has a prefix and a arena-specific value, a
/// [PartialInode] has only an arena-specific value.
///
/// The special prefix [InodePrefix::ZERO] is used to identify inodes
/// that are not part of any arenas.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InodePrefix(u8);

impl InodePrefix {
    /// Number of bits, out of 64, available for the prefix.
    pub const NUMBITS: u32 = 64 - PathId::NUMBITS;

    pub const ZERO: InodePrefix = InodePrefix(0);

    pub fn from_u8(val: u8) -> InodePrefix {
        InodePrefix(val)
    }

    pub fn as_u64(&self) -> u64 {
        (self.0 as u64) << 56
    }

    pub fn as_u8(&self) -> u8 {
        self.0
    }
}

impl From<u64> for InodePrefix {
    fn from(value: u64) -> Self {
        InodePrefix((value >> PathId::NUMBITS) as u8)
    }
}

impl std::fmt::Display for InodePrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:x}+", self.0)
    }
}

impl std::fmt::Debug for InodePrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PathIdPrefix({:x})", self.0)
    }
}

/// An arena-specific number that identifies a path.
///
/// A [PathId] is a unique (within its arena) and stable way of
/// identifying a path that exists whithin an arena.
///
/// In general [PathIds] from different arenas can't be compared. The
/// single exception to this rule being [PathId::ROOT], which
/// identifies the root of any arena.
///
/// [PathId]s are converted to [PartialInode] using
/// [crate::arena::CacheReadOperations] then associated with the
/// arena's [InodePrefix] to form an [Inode].
///
/// This type can be used as a key or value in redb database schemas.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PathId(pub u64);

impl PathId {
    /// Number of usable bits, from a u64, in a PathId.
    pub const NUMBITS: u32 = 56;

    /// Usable portion of a u64 for a `PathId`, must be in sync with [PathId::NUMBITS].
    pub const MASK: u64 = 0x00ffffffffffffff;

    /// An invalid path id.
    pub const ZERO: PathId = PathId(0);

    /// Special path id used to identify the root.
    pub const ROOT: PathId = PathId(1);

    /// Maximum allowed pathid value.
    pub const MAX: PathId = PathId(PathId::MASK);

    /// Return true if this is the special path id 1, which identifies
    /// a root.
    pub fn is_arena_root(&self) -> bool {
        *self == PathId::ROOT
    }

    /// Get the underlying u64 value.
    pub fn value(&self) -> u64 {
        self.0
    }

    pub fn plus(&self, val: u64) -> PathId {
        PathId(self.0 + val)
    }

    pub fn minus(&self, val: u64) -> PathId {
        PathId(self.0 - val)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    /// Return a hexadecimal representation for pathids.
    ///
    /// This representation uses leading 0, so the lexicographical
    /// order is the same as the numeric order.
    pub fn hex(&self) -> String {
        format!("{:016x}", self.0)
    }

    pub fn as_optional(pathid: u64) -> Option<PathId> {
        if pathid == 0 {
            None
        } else {
            Some(PathId(pathid))
        }
    }

    pub fn from_optional(pathid: Option<PathId>) -> u64 {
        pathid.map(|i| i.0).unwrap_or(0)
    }
}

impl From<PartialInode> for PathId {
    fn from(value: PartialInode) -> Self {
        PathId(value.as_u64())
    }
}

impl From<&PartialInode> for PathId {
    fn from(value: &PartialInode) -> Self {
        PathId(value.as_u64())
    }
}

impl std::fmt::Display for PathId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "_+{:x}", self.0)
    }
}

impl std::fmt::Debug for PathId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PathId({:x})", self.0)
    }
}

impl Key for PathId {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        let value1 = u64::from_le_bytes(data1.try_into().unwrap_or([0; 8]));
        let value2 = u64::from_le_bytes(data2.try_into().unwrap_or([0; 8]));
        value1.cmp(&value2)
    }
}

impl Value for PathId {
    type SelfType<'a> = PathId;
    type AsBytes<'a>
        = [u8; 8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(8)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> PathId
    where
        Self: 'a,
    {
        PathId(<u64>::from_le_bytes(data.try_into().unwrap_or([0; 8])))
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> [u8; 8]
    where
        Self: 'a,
        Self: 'b,
    {
        value.0.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::new("PathId")
    }
}

/// A newtype wrapper around u64 representing an Inode number.
///
/// This type can be used as a key or value in redb database schemas.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Inode(pub u64);

impl Inode {
    pub const ZERO: Inode = Inode(0);
    pub const MAX: Inode = Inode(u64::MAX);
    pub const ROOT: Inode = Inode(1);

    /// Create a new Inode from a u64 value.
    pub fn new(prefix: InodePrefix, partial: PartialInode) -> Self {
        Self(prefix.as_u64() | (partial.as_u64() & PathId::MASK))
    }

    /// Check whether the corresponding [PartialInode] is a root.
    pub fn is_arena_root(&self) -> bool {
        self.partial() == PartialInode::ROOT
    }

    /// Return the [PartialInode] that's part of this inode.
    pub fn partial(&self) -> PartialInode {
        PartialInode(self.0 & PathId::MASK)
    }

    /// Return the inode prefix
    pub fn prefix(&self) -> InodePrefix {
        InodePrefix((self.0 >> 56) as u8)
    }

    /// Get the underlying u64 value.
    pub fn value(&self) -> u64 {
        self.0
    }

    pub fn plus(&self, val: u64) -> Inode {
        Inode(self.0 + val)
    }

    pub fn minus(&self, val: u64) -> Inode {
        Inode(self.0 - val)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    /// Return a hexadecimal representation for Inodes.
    ///
    /// This representation uses leading 0, so the lexicographical
    /// order is the same as the numeric order.
    pub fn hex(&self) -> String {
        format!("{:016x}", self.0)
    }
}

impl std::fmt::Display for Inode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "0x{:x}", self.0)
    }
}
impl std::fmt::Debug for Inode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Inode(0x{:x})", self.0)
    }
}

impl Key for Inode {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        let value1 = u64::from_le_bytes(data1.try_into().unwrap_or([0; 8]));
        let value2 = u64::from_le_bytes(data2.try_into().unwrap_or([0; 8]));
        value1.cmp(&value2)
    }
}

impl Value for Inode {
    type SelfType<'a> = Inode;
    type AsBytes<'a>
        = [u8; 8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(8)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Inode
    where
        Self: 'a,
    {
        Inode(<u64>::from_le_bytes(data.try_into().unwrap_or([0; 8])))
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> [u8; 8]
    where
        Self: 'a,
        Self: 'b,
    {
        value.0.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::new("Inode")
    }
}

/// A newtype wrapper around u64 representing an PartialInode number.
///
/// This type can be used as a key or value in redb database schemas.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartialInode(pub u64);

impl PartialInode {
    pub const ZERO: PartialInode = PartialInode(0);
    pub const MAX: PartialInode = PartialInode(u64::MAX);
    pub const ROOT: PartialInode = PartialInode(1);

    pub fn to_inode(&self, prefix: InodePrefix) -> Inode {
        Inode::new(prefix, *self)
    }

    pub fn is_arena_root(&self) -> bool {
        *self == PartialInode::ROOT
    }

    pub fn plus(&self, val: u64) -> PartialInode {
        PartialInode(self.0 + val)
    }

    pub fn minus(&self, val: u64) -> PartialInode {
        PartialInode(self.0 - val)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    /// Return a hexadecimal representation for PartialInodes.
    ///
    /// This representation uses leading 0, so the lexicographical
    /// order is the same as the numeric order.
    pub fn hex(&self) -> String {
        format!("{:016x}", self.0)
    }
}

impl From<PathId> for PartialInode {
    fn from(value: PathId) -> Self {
        PartialInode(value.as_u64())
    }
}

impl From<&PathId> for PartialInode {
    fn from(value: &PathId) -> Self {
        PartialInode(value.as_u64())
    }
}

impl std::fmt::Display for PartialInode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "0x{:x}", self.0)
    }
}
impl std::fmt::Debug for PartialInode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PartialInode(0x{:x})", self.0)
    }
}

impl Key for PartialInode {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        let value1 = u64::from_le_bytes(data1.try_into().unwrap_or([0; 8]));
        let value2 = u64::from_le_bytes(data2.try_into().unwrap_or([0; 8]));
        value1.cmp(&value2)
    }
}

impl Value for PartialInode {
    type SelfType<'a> = PartialInode;
    type AsBytes<'a>
        = [u8; 8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(8)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> PartialInode
    where
        Self: 'a,
    {
        PartialInode(<u64>::from_le_bytes(data.try_into().unwrap_or([0; 8])))
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> [u8; 8]
    where
        Self: 'a,
        Self: 'b,
    {
        value.0.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::new("PartialInode")
    }
}

/// A unique ID for a job within an arena.
///
/// This type can be used as a key or value in redb database schemas.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct JobId(pub u64);

impl JobId {
    pub const ZERO: JobId = JobId(0);
    pub const MAX: JobId = JobId(u64::MAX);

    /// Create a new JobId from a u64 value.
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// Get the underlying u64 value.
    pub fn value(&self) -> u64 {
        self.0
    }

    pub fn plus(&self, val: u64) -> JobId {
        JobId(self.0 + val)
    }

    pub fn minus(&self, val: u64) -> JobId {
        JobId(self.0 - val)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn is_invalid(&self) -> bool {
        self.0 == 0
    }

    pub fn as_optional(job_id: u64) -> Option<JobId> {
        if job_id == 0 {
            None
        } else {
            Some(JobId(job_id))
        }
    }

    pub fn from_optional(job_id: Option<JobId>) -> u64 {
        job_id.map(|b| b.0).unwrap_or(0)
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_and_full_inode() {
        let prefix = InodePrefix(0x12);
        let partial = PartialInode(0x133);
        let full = partial.to_inode(prefix);
        assert_eq!(0x1200000000000133, full.as_u64());
        assert_eq!(partial, full.partial());
        assert_eq!(prefix, full.prefix());
    }

    #[test]
    fn pathid_prefix() {
        let prefix = InodePrefix(0x12);
        assert_eq!(0x12, prefix.as_u8());
        assert_eq!(0x1200000000000000, prefix.as_u64());
        assert_eq!("PathIdPrefix(12)", format!("{:?}", prefix));
        assert_eq!("12+", format!("{}", prefix));
    }

    #[test]
    fn pathid_display() {
        assert_eq!("2b+", format!("{}", InodePrefix(43)));
        assert_eq!("PathIdPrefix(2b)", format!("{:?}", InodePrefix(43)));
        assert_eq!("_+19d", format!("{}", PathId(413)));
        assert_eq!("PathId(19d)", format!("{:?}", PathId(413)));
    }

    #[test]
    fn pathid_redb_key() {
        let pathid1 = PathId(100);
        let pathid2 = PathId(200);
        let pathid3 = PathId(100);

        let data1 = PathId::as_bytes(&pathid1);
        let data2 = PathId::as_bytes(&pathid2);
        let data3 = PathId::as_bytes(&pathid3);

        assert_eq!(PathId::compare(&data1, &data2), std::cmp::Ordering::Less);
        assert_eq!(PathId::compare(&data2, &data1), std::cmp::Ordering::Greater);
        assert_eq!(PathId::compare(&data1, &data3), std::cmp::Ordering::Equal);
    }

    #[test]
    fn pathid_redb_value() {
        let original = PathId(12345);
        let bytes = PathId::as_bytes(&original);
        let restored = PathId::from_bytes(&bytes);

        assert_eq!(original, restored);
    }

    #[test]
    fn pathid_redb_value_edge_cases() {
        // Test zero
        let zero = PathId(0);
        let zero_bytes = PathId::as_bytes(&zero);
        let zero_restored = PathId::from_bytes(&zero_bytes);
        assert_eq!(zero, zero_restored);

        // Test maximum u64 value
        let max = PathId(u64::MAX);
        let max_bytes = PathId::as_bytes(&max);
        let max_restored = PathId::from_bytes(&max_bytes);
        assert_eq!(max, max_restored);
    }

    #[test]
    fn pathid_redb_value_invalid_data() {
        // Test with insufficient data (should handle gracefully)
        let short_data = &[1, 2, 3]; // Less than 8 bytes
        let restored = PathId::from_bytes(short_data);
        // Should default to 0 or handle gracefully
        assert_eq!(restored.as_u64(), 0);

        // Test with exactly 8 bytes
        let valid_data = &[1, 0, 0, 0, 0, 0, 0, 0]; // Little endian 1
        let restored = PathId::from_bytes(valid_data);
        assert_eq!(restored.as_u64(), 1);
    }
}
