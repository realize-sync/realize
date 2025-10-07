use base64::Engine as _;

/// Hash of a file content or byte range.
///
/// It is normally created by functions from
/// `realize_storage::utils::hash`
#[derive(Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Hash(pub [u8; 32]);

impl Hash {
    fn base64(&self) -> String {
        base64::prelude::BASE64_STANDARD_NO_PAD.encode(self.0)
    }

    /// Parse a hash from a base64 string.
    ///
    /// Accepts both standard base64 (with padding) and base64 without padding.
    /// The decoded result must be exactly 32 bytes.
    pub fn from_base64(s: &str) -> Option<Hash> {
        // Try decoding with standard base64 first (with padding)
        let bytes = base64::prelude::BASE64_STANDARD_NO_PAD.decode(s).ok()?;

        if bytes.len() != 32 {
            return None;
        }

        let mut array = [0u8; 32];
        array.copy_from_slice(&bytes);
        Some(Hash(array))
    }

    /// Hash that indicates the absence of any data.
    pub fn zero() -> Self {
        Self([0u8; 32])
    }

    /// True if the hash is [Hash::zero]
    fn is_zero(&self) -> bool {
        for v in self.0 {
            if v != 0 {
                return false;
            }
        }

        true
    }

    /// True if `other` contains this hash.
    pub fn matches(&self, other: Option<&Hash>) -> bool {
        if let Some(other) = other {
            return *self == *other;
        }
        false
    }
}

impl std::fmt::Debug for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_zero() {
            f.write_str("None")
        } else {
            f.write_str(&self.base64())
        }
    }
}

/// A rsync-type signature created from a partial file range.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Signature(pub Vec<u8>);

/// A rsync-type delta that can be applied to existing data to modify
/// a file.
///
/// This is the result of comparing some range of data on the source
/// instance and the destination instance. This is created based on
/// local data and a remote [Signature]. The resulting delta can only
/// be used on the data the [Signature] was created from.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Delta(pub Vec<u8>);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_from_base64_valid_no_padding() {
        // Create a test hash
        let original = Hash([0x42; 32]);
        let base64_str = original.to_string();

        // Parse it back
        let parsed = Hash::from_base64(&base64_str).unwrap();
        assert_eq!(original, parsed);
    }

    #[test]
    fn test_hash_from_base64_valid_with_padding() {
        let original = Hash([0x42; 32]);
        let base64_str = base64::prelude::BASE64_STANDARD.encode(original.0);

        // padding is not supported
        let parsed = Hash::from_base64(&base64_str);
        assert_eq!(parsed, None);
    }

    #[test]
    fn test_hash_from_base64_invalid_length_short() {
        let short_base64 = "dGVzdA"; // "test" in base64, only 4 bytes
        let result = Hash::from_base64(short_base64);
        assert_eq!(result, None);
    }

    #[test]
    fn test_hash_from_base64_invalid_length_long() {
        // Create a 33-byte array and encode it
        let long_bytes = [0x42u8; 33];
        let long_base64 = base64::prelude::BASE64_STANDARD_NO_PAD.encode(long_bytes);
        let result = Hash::from_base64(&long_base64);
        assert_eq!(result, None);
    }

    #[test]
    fn test_hash_from_base64_invalid_characters() {
        let invalid_base64 = "invalid@#$%characters!";
        let result = Hash::from_base64(invalid_base64);
        assert_eq!(result, None);
    }

    #[test]
    fn test_hash_zero_and_display() {
        let zero_hash = Hash::zero();
        assert_eq!(zero_hash.to_string(), "None");
        assert!(zero_hash.is_zero());
    }

    #[test]
    fn test_hash_display_round_trip() {
        // Test various hash patterns
        let patterns = [
            [0x00; 32], // zero hash
            [0xFF; 32], // all ones
            [0x42; 32], // repeated pattern
        ];

        for pattern in patterns {
            let hash = Hash(pattern);
            if !hash.is_zero() {
                let string_repr = hash.to_string();
                let parsed = Hash::from_base64(&string_repr).unwrap();
                assert_eq!(hash, parsed);
            }
        }
    }
}
