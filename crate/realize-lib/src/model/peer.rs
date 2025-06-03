use std::fmt;

/// Identifier for a network peer in the configuration.
#[derive(
    Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, serde::Deserialize, serde::Serialize,
)]
#[serde(transparent)]
pub struct Peer(String);

impl From<String> for Peer {
    fn from(value: String) -> Self {
        Peer(value)
    }
}

impl From<&str> for Peer {
    fn from(value: &str) -> Self {
        Peer(value.to_string())
    }
}

impl fmt::Display for Peer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl Peer {
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn into_string(self) -> String {
        self.0
    }
}
