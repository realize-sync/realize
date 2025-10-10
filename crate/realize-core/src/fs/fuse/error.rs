//! FUSE error layer - error types and conversion functions
//!
//! This module contains error types, error conversions between different error types,
//! and utility functions for handling errors in the FUSE interface.

use crate::rpc::HouseholdOperationError;
use nix::libc;
use realize_storage::StorageError;

/// Intermediate error type to catch and convert to libc errno to
/// report errors to fuser.
#[derive(Debug, Clone)]
pub(crate) struct FuseError {
    errno: libc::c_int,
    message: Option<String>,
}

impl std::error::Error for FuseError {}
impl std::fmt::Display for FuseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(msg) = &self.message {
            write!(f, "errno {}: {}", self.errno, msg)
        } else {
            write!(f, "errno {}", self.errno)
        }
    }
}

impl FuseError {
    pub(crate) fn utf8() -> FuseError {
        FuseError {
            errno: libc::EINVAL,
            message: Some("invalid UTF-8 string".to_string()),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn errno(&self) -> libc::c_int {
        self.errno
    }

    /// Convert into a libc error code.
    pub(crate) fn log_and_convert(self) -> libc::c_int {
        log::debug!("FUSE operation error: {self}");

        self.errno
    }
}

impl From<StorageError> for FuseError {
    fn from(err: StorageError) -> Self {
        FuseError {
            errno: io_errno(err.io_kind()),
            message: Some(err.to_string()),
        }
    }
}

impl From<HouseholdOperationError> for FuseError {
    fn from(err: HouseholdOperationError) -> Self {
        FuseError {
            errno: io_errno(err.io_kind()),
            message: Some(err.to_string()),
        }
    }
}

impl From<std::io::Error> for FuseError {
    fn from(ioerr: std::io::Error) -> Self {
        FuseError {
            errno: io_errno(ioerr.kind()),
            message: Some(ioerr.to_string()),
        }
    }
}

impl From<tokio::task::JoinError> for FuseError {
    fn from(err: tokio::task::JoinError) -> Self {
        FuseError {
            errno: libc::EIO,
            message: Some(format!("tokio runtime error {}", err)),
        }
    }
}

impl From<nix::errno::Errno> for FuseError {
    fn from(value: nix::errno::Errno) -> Self {
        FuseError {
            errno: value as libc::c_int,
            message: None,
        }
    }
}

/// Convert a Rust [std::io::ErrorKind] into a libc error code.
fn io_errno(kind: std::io::ErrorKind) -> libc::c_int {
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

#[cfg(test)]
mod tests {
    use nix::errno::Errno;

    use super::*;

    #[test]
    fn test_fuse_error_is_clonable() {
        let error1 = FuseError::utf8();
        let error2 = error1.clone(); // This should compile without issues
        assert_eq!(error1.errno(), error2.errno());
        assert_eq!(error1.to_string(), error2.to_string());

        let error3 = FuseError::from(Errno::EINVAL);
        let error4 = error3.clone();
        assert_eq!(error3.errno(), error4.errno());
        assert_eq!(error3.to_string(), error4.to_string());
    }

    #[test]
    fn test_fuse_error_preserves_errno_and_message() {
        let error = FuseError::from(Errno::ENOENT);
        assert_eq!(error.errno(), libc::ENOENT);
        assert_eq!(error.to_string(), "errno 2");

        let utf8_error = FuseError::utf8();
        assert_eq!(utf8_error.errno(), libc::EINVAL);
        assert_eq!(utf8_error.to_string(), "errno 22: invalid UTF-8 string");
    }
}
