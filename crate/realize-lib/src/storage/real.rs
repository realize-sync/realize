use crate::model::{Arena, Path, UnixTime};

#[cfg(target_os = "linux")]
mod history;
#[allow(dead_code)]
#[allow(unknown_lints)]
#[allow(clippy::uninlined_format_args)]
#[allow(clippy::extra_unused_type_parameters)]
mod real_capnp {
    include!(concat!(env!("OUT_DIR"), "/real_capnp.rs"));
}
mod store;

pub use store::{
    Options, PathResolver, PathType, RealStore, RealStoreError, RsyncOperation, StoreSubscribe,
    SyncedFile,
};

/// Report something happening in arenas of the local file system.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Notification {
    /// Report that a file has been written to, added, moved in or
    /// modified.
    Link {
        arena: Arena,
        path: Path,
        size: u64,
        mtime: UnixTime,
    },

    /// Report that a file has been deleted or moved out of the given
    /// path.
    Unlink {
        arena: Arena,
        path: Path,
        mtime: UnixTime,
    },

    /// Report that the area needs catching up. After that
    /// notification, all existing files are reported as
    /// [Notification::Catchup].
    ///
    /// Catchup ends with [Notification::Ready]
    CatchingUp { arena: Arena },

    /// Report an already existing file at the time notifications were
    /// setup. Catchup notifications only ever sent at the beginning,
    /// before History reports itself ready.
    Catchup {
        arena: Arena,
        path: Path,
        size: u64,
        mtime: UnixTime,
    },

    /// Report that the arena is done catching up. After that
    /// [Notification::Link] and [Notification::Unlink] are sent and
    /// no [Notification::Catcheup].
    Ready { arena: Arena },
}

impl Notification {
    pub fn arena(&self) -> &Arena {
        use Notification::*;
        match self {
            CatchingUp { arena, .. } => arena,
            Ready { arena, .. } => arena,
            Link { arena, .. } => arena,
            Unlink { arena, .. } => arena,
            Catchup { arena, .. } => arena,
        }
    }
    pub fn path(&self) -> Option<&Path> {
        use Notification::*;
        match self {
            CatchingUp { .. } => None,
            Ready { .. } => None,
            Link { path, .. } => Some(path),
            Unlink { path, .. } => Some(path),
            Catchup { path, .. } => Some(path),
        }
    }

    pub fn is_catchup(&self) -> bool {
        use Notification::*;
        match self {
            Link { .. } => false,
            Unlink { .. } => false,
            CatchingUp { .. } => true,
            Catchup { .. } => true,
            Ready { .. } => true,
        }
    }
}
