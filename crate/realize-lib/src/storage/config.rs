use std::path::PathBuf;

/// Define an Arena available locally.
///
/// An arena is identified by [crate::model::Arena].
#[derive(Clone, serde::Deserialize, Debug)]
pub struct ArenaConfig {
    /// Local path to the directory where files for that arena are
    /// stored.
    ///
    /// That directory must be writable by the current user.
    pub path: PathBuf,
}
