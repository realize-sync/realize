use internment::Intern;
use std::fmt;

/// Identifier for a network peer in the configuration.
///
/// This type is meant to be passed around by reference. The string is
/// interned and the type itself made cheap to copy.
#[derive(
    Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, serde::Deserialize, serde::Serialize,
)]
#[serde(transparent)]
pub struct Peer(Intern<String>);

impl From<String> for Peer {
    fn from(value: String) -> Self {
        Peer(Intern::new(value))
    }
}

impl From<&str> for Peer {
    fn from(value: &str) -> Self {
        Peer(Intern::from_ref(value))
    }
}

impl fmt::Display for Peer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl Peer {
    pub fn as_str(&self) -> &'static str {
        self.0.as_ref()
    }
    pub fn into_string(self) -> String {
        self.0.to_string()
    }
}
