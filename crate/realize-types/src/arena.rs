use internment::Intern;
use std::fmt;

/// A set of path and associated data shared between peers.
///
/// This types is an identifier. An arena is identified by its name,
/// which must be unique and known to all peers that want to share
/// data.
///
/// This type is meant to be passed around by reference. The string is
/// interned and the type itself made cheap to copy.
#[derive(
    Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, serde::Deserialize, serde::Serialize,
)]
#[serde(transparent)]
pub struct Arena(Intern<String>);

impl From<String> for Arena {
    fn from(value: String) -> Self {
        Arena(Intern::new(value))
    }
}

impl From<&str> for Arena {
    fn from(value: &str) -> Self {
        Arena(Intern::from_ref(value))
    }
}

impl fmt::Display for Arena {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl Arena {
    pub fn as_str(&self) -> &'static str {
        self.0.as_ref()
    }
    pub fn into_string(self) -> String {
        self.0.to_string()
    }
}
