//! FUSE error layer - error types and conversion functions
//!
//! This module contains error types, error conversions between different error types,
//! and utility functions for handling errors in the FUSE interface.

use crate::rpc::HouseholdOperationError;
use nix::libc::{self, c_int};
use realize_storage::StorageError;

/// Intermediate error type to catch and convert to libc errno to
/// report errors to fuser.
#[derive(Debug, thiserror::Error)]
pub(crate) enum FuseError {
    #[error(transparent)]
    Cache(#[from] StorageError),

    #[error(transparent)]
    Rpc(#[from] HouseholdOperationError),

    #[error("invalid UTF-8 string")]
    Utf8,

    #[error("I/O error")]
    Io(#[from] std::io::Error),

    #[error("errno {0}")]
    Errno(c_int),

    #[error("tokio runtime error {0}")]
    Join(#[from] tokio::task::JoinError),
}

impl FuseError {
    /// Return a libc error code to represent this error, fuse-side.
    pub(crate) fn errno(&self) -> c_int {
        match &self {
            FuseError::Cache(err) => io_errno(err.io_kind()),
            FuseError::Utf8 => libc::EINVAL,
            FuseError::Io(ioerr) => io_errno(ioerr.kind()),
            FuseError::Errno(errno) => *errno,
            FuseError::Rpc(err) => io_errno(err.io_kind()),
            FuseError::Join(_) => libc::EIO,
        }
    }

    /// Convert into a libc error code.
    pub(crate) fn log_and_convert(self) -> c_int {
        let errno = self.errno();

        log::debug!("FUSE operation error: {self:?} -> {errno}");

        errno
    }
}

impl From<nix::errno::Errno> for FuseError {
    fn from(value: nix::errno::Errno) -> Self {
        FuseError::Errno(value as c_int)
    }
}

/// Convert a Rust [std::io::ErrorKind] into a libc error code.
fn io_errno(kind: std::io::ErrorKind) -> c_int {
    match kind {
        std::io::ErrorKind::NotFound => libc::ENOENT,
        std::io::ErrorKind::PermissionDenied => libc::EACCES,
        std::io::ErrorKind::ConnectionRefused => libc::ECONNREFUSED,
        std::io::ErrorKind::ConnectionReset => libc::ECONNRESET,
        std::io::ErrorKind::HostUnreachable => libc::EHOSTUNREACH,
        std::io::ErrorKind::NetworkUnreachable => libc::ENETUNREACH,
        std::io::ErrorKind::ConnectionAborted => libc::ECONNABORTED,
        std::io::ErrorKind::NotConnected => libc::ENOTCONN,
        std::io::ErrorKind::AddrInUse => libc::EADDRINUSE,
        std::io::ErrorKind::AddrNotAvailable => libc::EADDRNOTAVAIL,
        std::io::ErrorKind::NetworkDown => libc::ENETDOWN,
        std::io::ErrorKind::BrokenPipe => libc::EPIPE,
        std::io::ErrorKind::AlreadyExists => libc::EEXIST,
        std::io::ErrorKind::WouldBlock => libc::EAGAIN,
        std::io::ErrorKind::NotADirectory => libc::ENOTDIR,
        std::io::ErrorKind::IsADirectory => libc::EISDIR,
        std::io::ErrorKind::DirectoryNotEmpty => libc::ENOTEMPTY,
        std::io::ErrorKind::ReadOnlyFilesystem => libc::EROFS,
        std::io::ErrorKind::StaleNetworkFileHandle => libc::ESTALE,
        std::io::ErrorKind::InvalidInput => libc::EINVAL,
        std::io::ErrorKind::InvalidData => libc::EINVAL,
        std::io::ErrorKind::TimedOut => libc::ETIMEDOUT,
        std::io::ErrorKind::WriteZero => libc::EIO,
        std::io::ErrorKind::StorageFull => libc::ENOSPC,
        std::io::ErrorKind::NotSeekable => libc::ESPIPE,
        std::io::ErrorKind::QuotaExceeded => libc::EDQUOT,
        std::io::ErrorKind::FileTooLarge => libc::EFBIG,
        std::io::ErrorKind::ResourceBusy => libc::EBUSY,
        std::io::ErrorKind::ExecutableFileBusy => libc::ETXTBSY,
        std::io::ErrorKind::Deadlock => libc::EDEADLK,
        std::io::ErrorKind::CrossesDevices => libc::EXDEV,
        std::io::ErrorKind::TooManyLinks => libc::EMLINK,
        std::io::ErrorKind::InvalidFilename => libc::EINVAL,
        std::io::ErrorKind::ArgumentListTooLong => libc::E2BIG,
        std::io::ErrorKind::Interrupted => libc::EINTR,
        std::io::ErrorKind::Unsupported => libc::ENOSYS,
        std::io::ErrorKind::OutOfMemory => libc::ENOMEM,
        _ => libc::EIO,
    }
}
