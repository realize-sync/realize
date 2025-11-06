use redb::{Key, TypeName, Value};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PathIdPrefix(u8);

impl PathIdPrefix {
    pub(crate) const MASK: u64 = 0x00ffffffffffffff;
    pub const ZERO: PathIdPrefix = PathIdPrefix(0);

    pub fn from_u8(val: u8) -> PathIdPrefix {
        PathIdPrefix(val)
    }

    pub fn as_u64(&self) -> u64 {
        (self.0 as u64) << 56
    }

    pub fn as_u8(&self) -> u8 {
        self.0
    }

    pub fn and(&self, partial: PartialPathId) -> PathId {
        PathId::new(*self, partial)
    }
}

impl From<u64> for PathIdPrefix {
    fn from(value: u64) -> Self {
        PathIdPrefix((value >> 56) as u8)
    }
}

impl From<PathId> for PathIdPrefix {
    fn from(pathid: PathId) -> Self {
        pathid.prefix()
    }
}

impl std::fmt::Display for PathIdPrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:x}+", self.0)
    }
}

impl std::fmt::Debug for PathIdPrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PathIdPrefix({:x})", self.0)
    }
}

/// A global pathid number, the combination of [PathId] and [PathIdPrefix].
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PathId(pub u64);

impl PathId {
    pub const ZERO: PathId = PathId(0);
    pub const ROOT: PathId = PathId(1);
    pub const MAX: PathId = PathId(u64::MAX);

    pub fn new(prefix: PathIdPrefix, local: PartialPathId) -> Self {
        PathId(prefix.as_u64() | (local.as_u64() & PathIdPrefix::MASK))
    }

    pub fn partial(&self) -> PartialPathId {
        PartialPathId(self.0 & PathIdPrefix::MASK)
    }

    pub fn prefix(&self) -> PathIdPrefix {
        PathIdPrefix((self.0 >> 56) as u8)
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

impl From<Inode> for PathId {
    fn from(value: Inode) -> Self {
        PathId(value.as_u64())
    }
}

impl From<&Inode> for PathId {
    fn from(value: &Inode) -> Self {
        PathId(value.as_u64())
    }
}

impl From<u64> for PathId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<PathId> for u64 {
    fn from(pathid: PathId) -> Self {
        pathid.0
    }
}

impl AsRef<u64> for PathId {
    fn as_ref(&self) -> &u64 {
        &self.0
    }
}

impl AsMut<u64> for PathId {
    fn as_mut(&mut self) -> &mut u64 {
        &mut self.0
    }
}

impl std::fmt::Display for PathId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{:x}", self.prefix(), self.0 & PathIdPrefix::MASK,)
    }
}

impl std::fmt::Debug for PathId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PathId({})", self)
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

/// A local pathid number.
///
/// Combine it with a [PathIdPrefix] to make it a globl [PathId]
///
/// This type can be used as a key or value in redb database schemas.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartialPathId(pub u64);

impl PartialPathId {
    pub const ZERO: PartialPathId = PartialPathId(0);
    pub const ROOT: PartialPathId = PartialPathId(1);
    pub const MAX: PartialPathId = PartialPathId(PathIdPrefix::MASK);

    /// Create a new PathId from a u64 value.
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// Add a prefix to this local id, make it a [PathId].
    pub fn with(&self, prefix: PathIdPrefix) -> PathId {
        PathId::new(prefix, *self)
    }

    /// Get the underlying u64 value.
    pub fn value(&self) -> u64 {
        self.0
    }

    pub fn plus(&self, val: u64) -> PartialPathId {
        PartialPathId(self.0 + val)
    }

    pub fn minus(&self, val: u64) -> PartialPathId {
        PartialPathId(self.0 - val)
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

    pub fn as_optional(pathid: u64) -> Option<PartialPathId> {
        if pathid == 0 {
            None
        } else {
            Some(PartialPathId(pathid))
        }
    }

    pub fn from_optional(pathid: Option<PartialPathId>) -> u64 {
        pathid.map(|i| i.0).unwrap_or(0)
    }
}

impl From<PartialInode> for PartialPathId {
    fn from(value: PartialInode) -> Self {
        PartialPathId(value.as_u64())
    }
}

impl From<u64> for PartialPathId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<PathId> for PartialPathId {
    fn from(pathid: PathId) -> Self {
        pathid.partial()
    }
}

impl From<PartialPathId> for u64 {
    fn from(pathid: PartialPathId) -> Self {
        pathid.0
    }
}

impl AsRef<u64> for PartialPathId {
    fn as_ref(&self) -> &u64 {
        &self.0
    }
}

impl AsMut<u64> for PartialPathId {
    fn as_mut(&mut self) -> &mut u64 {
        &mut self.0
    }
}

impl std::fmt::Display for PartialPathId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "_+{:x}", self.0)
    }
}

impl std::fmt::Debug for PartialPathId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PartialPathId({:x})", self.0)
    }
}

impl Key for PartialPathId {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        let value1 = u64::from_le_bytes(data1.try_into().unwrap_or([0; 8]));
        let value2 = u64::from_le_bytes(data2.try_into().unwrap_or([0; 8]));
        value1.cmp(&value2)
    }
}

impl Value for PartialPathId {
    type SelfType<'a> = PartialPathId;
    type AsBytes<'a>
        = [u8; 8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(8)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> PartialPathId
    where
        Self: 'a,
    {
        PartialPathId(<u64>::from_le_bytes(data.try_into().unwrap_or([0; 8])))
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> [u8; 8]
    where
        Self: 'a,
        Self: 'b,
    {
        value.0.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::new("PartialPathId")
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
    pub fn new(prefix: PathIdPrefix, partial: PartialInode) -> Self {
        Self(prefix.as_u64() | (partial.as_u64() & PathIdPrefix::MASK))
    }

    pub fn partial(&self) -> PartialInode {
        PartialInode(self.0 & PathIdPrefix::MASK)
    }

    pub fn prefix(&self) -> PathIdPrefix {
        PathIdPrefix((self.0 >> 56) as u8)
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

impl From<PathId> for Inode {
    fn from(value: PathId) -> Self {
        Inode(value.as_u64())
    }
}

impl From<&PathId> for Inode {
    fn from(value: &PathId) -> Self {
        Inode(value.as_u64())
    }
}

impl From<u64> for Inode {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<Inode> for u64 {
    fn from(inode: Inode) -> Self {
        inode.0
    }
}

impl AsRef<u64> for Inode {
    fn as_ref(&self) -> &u64 {
        &self.0
    }
}

impl AsMut<u64> for Inode {
    fn as_mut(&mut self) -> &mut u64 {
        &mut self.0
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

    pub fn with(&self, prefix: PathIdPrefix) -> Inode {
        Inode::new(prefix, *self)
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

impl From<PartialPathId> for PartialInode {
    fn from(value: PartialPathId) -> Self {
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

impl From<u64> for JobId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<JobId> for u64 {
    fn from(value: JobId) -> Self {
        value.0
    }
}

impl AsRef<u64> for JobId {
    fn as_ref(&self) -> &u64 {
        &self.0
    }
}

impl AsMut<u64> for JobId {
    fn as_mut(&mut self) -> &mut u64 {
        &mut self.0
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
    fn partial_and_full_pathid() {
        let prefix = PathIdPrefix(0x12);
        let partial = PartialPathId(0x133);
        let full = partial.with(prefix);
        assert_eq!(0x1200000000000133, full.as_u64());
        assert_eq!(full, prefix.and(partial));
        assert_eq!(partial, full.partial());
        assert_eq!(prefix, full.prefix());
    }

    #[test]
    fn partial_and_full_inode() {
        let prefix = PathIdPrefix(0x12);
        let partial = PartialInode(0x133);
        let full = partial.with(prefix);
        assert_eq!(0x1200000000000133, full.as_u64());
        assert_eq!(partial, full.partial());
        assert_eq!(prefix, full.prefix());
    }

    #[test]
    fn pathid_prefix() {
        let prefix = PathIdPrefix(0x12);
        assert_eq!(0x12, prefix.as_u8());
        assert_eq!(0x1200000000000000, prefix.as_u64());
        assert_eq!("PathIdPrefix(12)", format!("{:?}", prefix));
        assert_eq!("12+", format!("{}", prefix));
    }

    #[test]
    fn pathid_display() {
        assert_eq!(
            "PathId(2b+19d)",
            format!("{:?}", PathId::new(PathIdPrefix(43), PartialPathId(413)))
        );
        assert_eq!(
            "2b+19d",
            format!("{}", PathId::new(PathIdPrefix(43), PartialPathId(413)))
        );
        assert_eq!("2b+", format!("{}", PathIdPrefix(43)));
        assert_eq!("PathIdPrefix(2b)", format!("{:?}", PathIdPrefix(43)));
        assert_eq!("_+19d", format!("{}", PartialPathId(413)));
        assert_eq!("PartialPathId(19d)", format!("{:?}", PartialPathId(413)));
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

    #[test]
    fn partial_pathid_redb_key() {
        let pathid1 = PartialPathId(100);
        let pathid2 = PartialPathId(200);
        let pathid3 = PartialPathId(100);

        let data1 = PartialPathId::as_bytes(&pathid1);
        let data2 = PartialPathId::as_bytes(&pathid2);
        let data3 = PartialPathId::as_bytes(&pathid3);

        assert_eq!(
            PartialPathId::compare(&data1, &data2),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            PartialPathId::compare(&data2, &data1),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            PartialPathId::compare(&data1, &data3),
            std::cmp::Ordering::Equal
        );
    }

    #[test]
    fn partial_pathid_redb_value() {
        let original = PartialPathId(12345);
        let bytes = PartialPathId::as_bytes(&original);
        let restored = PartialPathId::from_bytes(&bytes);

        assert_eq!(original, restored);
    }

    #[test]
    fn partial_pathid_redb_value_edge_cases() {
        // Test zero
        let zero = PartialPathId(0);
        let zero_bytes = PartialPathId::as_bytes(&zero);
        let zero_restored = PartialPathId::from_bytes(&zero_bytes);
        assert_eq!(zero, zero_restored);

        // Test maximum u64 value
        let max = PartialPathId(u64::MAX);
        let max_bytes = PartialPathId::as_bytes(&max);
        let max_restored = PartialPathId::from_bytes(&max_bytes);
        assert_eq!(max, max_restored);
    }

    #[test]
    fn partial_pathid_redb_value_invalid_data() {
        // Test with insufficient data (should handle gracefully)
        let short_data = &[1, 2, 3]; // Less than 8 bytes
        let restored = PartialPathId::from_bytes(short_data);
        // Should default to 0 or handle gracefully
        assert_eq!(restored.as_u64(), 0);

        // Test with exactly 8 bytes
        let valid_data = &[1, 0, 0, 0, 0, 0, 0, 0]; // Little endian 1
        let restored = PartialPathId::from_bytes(valid_data);
        assert_eq!(restored.as_u64(), 1);
    }
}
