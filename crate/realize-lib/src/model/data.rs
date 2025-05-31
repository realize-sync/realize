use base64::Engine as _;

/// Hash for a range within a [SyncedFile].
///
/// It is normally created by functions from [crate::utils::hash]
#[derive(Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Hash(pub [u8; 32]);

impl Hash {
    fn base64(&self) -> String {
        base64::prelude::BASE64_STANDARD_NO_PAD.encode(&self.0)
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
