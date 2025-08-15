use redb::{Key, TypeName, Value};

/// A newtype wrapper around u64 representing an inode number.
///
/// This type can be used as a key or value in redb database schemas.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Inode(pub u64);

impl Inode {
    pub const ZERO: Inode = Inode(0);
    pub const MAX: Inode = Inode(u64::MAX);

    /// Create a new Inode from a u64 value.
    pub fn new(value: u64) -> Self {
        Self(value)
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

    /// Return a hexadecimal representation for inodes.
    ///
    /// This representation uses leading 0, so the lexicographical
    /// order is the same as the numeric order.
    pub fn hex(&self) -> String {
        format!("{:016x}", self.0)
    }

    pub fn as_optional(inode: u64) -> Option<Inode> {
        if inode == 0 { None } else { Some(Inode(inode)) }
    }

    pub fn from_optional(inode: Option<Inode>) -> u64 {
        inode.map(|i| i.0).unwrap_or(0)
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
        write!(f, "{}", self.0)
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
    fn test_inode_display() {
        let inode = Inode::new(55555);
        assert_eq!(inode.to_string(), "55555");
    }
    #[test]
    fn test_inode_redb_key() {
        let inode1 = Inode::new(100);
        let inode2 = Inode::new(200);
        let inode3 = Inode::new(100);

        let data1 = Inode::as_bytes(&inode1);
        let data2 = Inode::as_bytes(&inode2);
        let data3 = Inode::as_bytes(&inode3);

        assert_eq!(Inode::compare(&data1, &data2), std::cmp::Ordering::Less);
        assert_eq!(Inode::compare(&data2, &data1), std::cmp::Ordering::Greater);
        assert_eq!(Inode::compare(&data1, &data3), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_inode_redb_value() {
        let original = Inode::new(12345);
        let bytes = Inode::as_bytes(&original);
        let restored = Inode::from_bytes(&bytes);

        assert_eq!(original, restored);
    }

    #[test]
    fn test_inode_redb_value_edge_cases() {
        // Test zero
        let zero = Inode::new(0);
        let zero_bytes = Inode::as_bytes(&zero);
        let zero_restored = Inode::from_bytes(&zero_bytes);
        assert_eq!(zero, zero_restored);

        // Test maximum u64 value
        let max = Inode::new(u64::MAX);
        let max_bytes = Inode::as_bytes(&max);
        let max_restored = Inode::from_bytes(&max_bytes);
        assert_eq!(max, max_restored);
    }

    #[test]
    fn test_inode_redb_value_invalid_data() {
        // Test with insufficient data (should handle gracefully)
        let short_data = &[1, 2, 3]; // Less than 8 bytes
        let restored = Inode::from_bytes(short_data);
        // Should default to 0 or handle gracefully
        assert_eq!(restored.value(), 0);

        // Test with exactly 8 bytes
        let valid_data = &[1, 0, 0, 0, 0, 0, 0, 0]; // Little endian 1
        let restored = Inode::from_bytes(valid_data);
        assert_eq!(restored.value(), 1);
    }
}
