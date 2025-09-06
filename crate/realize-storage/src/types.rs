use redb::{Key, TypeName, Value};

/// A newtype wrapper around u64 representing an pathid number.
///
/// This type can be used as a key or value in redb database schemas.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PathId(pub u64);

impl PathId {
    pub const ZERO: PathId = PathId(0);
    pub const MAX: PathId = PathId(u64::MAX);

    /// Create a new PathId from a u64 value.
    pub fn new(value: u64) -> Self {
        Self(value)
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
        if pathid == 0 { None } else { Some(PathId(pathid)) }
    }

    pub fn from_optional(pathid: Option<PathId>) -> u64 {
        pathid.map(|i| i.0).unwrap_or(0)
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
        write!(f, "{}", self.0)
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
    fn test_pathid_display() {
        let pathid = PathId::new(55555);
        assert_eq!(pathid.to_string(), "55555");
    }
    #[test]
    fn test_pathid_redb_key() {
        let pathid1 = PathId::new(100);
        let pathid2 = PathId::new(200);
        let pathid3 = PathId::new(100);

        let data1 = PathId::as_bytes(&pathid1);
        let data2 = PathId::as_bytes(&pathid2);
        let data3 = PathId::as_bytes(&pathid3);

        assert_eq!(PathId::compare(&data1, &data2), std::cmp::Ordering::Less);
        assert_eq!(PathId::compare(&data2, &data1), std::cmp::Ordering::Greater);
        assert_eq!(PathId::compare(&data1, &data3), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_pathid_redb_value() {
        let original = PathId::new(12345);
        let bytes = PathId::as_bytes(&original);
        let restored = PathId::from_bytes(&bytes);

        assert_eq!(original, restored);
    }

    #[test]
    fn test_pathid_redb_value_edge_cases() {
        // Test zero
        let zero = PathId::new(0);
        let zero_bytes = PathId::as_bytes(&zero);
        let zero_restored = PathId::from_bytes(&zero_bytes);
        assert_eq!(zero, zero_restored);

        // Test maximum u64 value
        let max = PathId::new(u64::MAX);
        let max_bytes = PathId::as_bytes(&max);
        let max_restored = PathId::from_bytes(&max_bytes);
        assert_eq!(max, max_restored);
    }

    #[test]
    fn test_pathid_redb_value_invalid_data() {
        // Test with insufficient data (should handle gracefully)
        let short_data = &[1, 2, 3]; // Less than 8 bytes
        let restored = PathId::from_bytes(short_data);
        // Should default to 0 or handle gracefully
        assert_eq!(restored.value(), 0);

        // Test with exactly 8 bytes
        let valid_data = &[1, 0, 0, 0, 0, 0, 0, 0]; // Little endian 1
        let restored = PathId::from_bytes(valid_data);
        assert_eq!(restored.value(), 1);
    }
}
