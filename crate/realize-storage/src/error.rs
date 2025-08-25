use crate::utils::holder::ByteConversionError;
use realize_types::{self, Arena};
use redb::TableError;
use std::panic::Location;
use tokio::task::JoinError;

/// Error returned types in this crate.
///
/// This type exists mainly so that errors can be converted when
/// needed to OS I/O errors.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("redb error: {0}")]
    Database(Box<redb::Error>), // a box to keep size in check

    #[error("{0}, from {1}")]
    OpenTable(redb::TableError, &'static Location<'static>),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("bincode error: {0}")]
    ByteConversion(#[from] ByteConversionError),

    #[error{"data not available at this time"}]
    Unavailable,

    #[error{"not found"}]
    NotFound,

    #[error{"not a directory"}]
    NotADirectory,

    #[error{"is a directory"}]
    IsADirectory,

    #[error(transparent)]
    JoinError(#[from] JoinError),

    #[error{"invalid rsync signature"}]
    InvalidRsyncSignature,

    #[error("unknown arena: {0}")]
    UnknownArena(Arena),

    #[error("arena {0} has no local storage")]
    NoLocalStorage(Arena),

    #[error("database is inconsistent. This is a bug. {0}")]
    InconsistentDatabase(String),
}

impl StorageError {
    /// Create an error with location.
    pub(crate) fn open_table(
        err: TableError,
        location: &'static Location<'static>,
    ) -> StorageError {
        StorageError::OpenTable(err, location)
    }

    fn from_redb(err: redb::Error) -> StorageError {
        if let redb::Error::Io(_) = &err {
            match err {
                redb::Error::Io(err) => StorageError::Io(err),
                _ => unreachable!(),
            }
        } else {
            StorageError::Database(Box::new(err))
        }
    }
}

impl From<nix::errno::Errno> for StorageError {
    fn from(errno: nix::errno::Errno) -> Self {
        let ioerr: std::io::Error = errno.into();

        StorageError::from(ioerr)
    }
}
impl From<redb::Error> for StorageError {
    fn from(value: redb::Error) -> Self {
        StorageError::from_redb(value)
    }
}

impl From<redb::TableError> for StorageError {
    fn from(value: redb::TableError) -> Self {
        StorageError::from_redb(value.into())
    }
}

impl From<redb::StorageError> for StorageError {
    fn from(value: redb::StorageError) -> Self {
        StorageError::from_redb(value.into())
    }
}

impl From<redb::TransactionError> for StorageError {
    fn from(value: redb::TransactionError) -> Self {
        StorageError::from_redb(value.into())
    }
}

impl From<redb::DatabaseError> for StorageError {
    fn from(value: redb::DatabaseError) -> Self {
        StorageError::from_redb(value.into())
    }
}
impl From<redb::CommitError> for StorageError {
    fn from(value: redb::CommitError) -> Self {
        StorageError::from_redb(value.into())
    }
}

impl From<realize_types::PathError> for StorageError {
    fn from(_: realize_types::PathError) -> Self {
        StorageError::from(ByteConversionError::Invalid("path"))
    }
}

impl From<fast_rsync::DiffError> for StorageError {
    fn from(err: fast_rsync::DiffError) -> Self {
        match err {
            fast_rsync::DiffError::InvalidSignature => StorageError::InvalidRsyncSignature,
            fast_rsync::DiffError::Io(err) => StorageError::from(err),
        }
    }
}

impl From<fast_rsync::SignatureParseError> for StorageError {
    fn from(_: fast_rsync::SignatureParseError) -> Self {
        StorageError::InvalidRsyncSignature
    }
}

impl Into<std::io::Error> for StorageError {
    fn into(self) -> std::io::Error {
        use std::io::ErrorKind;
        match self {
            StorageError::Database(_) => std::io::Error::new(ErrorKind::Other, "database error"),
            StorageError::OpenTable(_, _) => {
                std::io::Error::new(ErrorKind::Other, "failed to open table")
            }
            StorageError::Io(ioerr) => ioerr,
            StorageError::ByteConversion(ByteConversionError::Path(_)) => {
                std::io::Error::new(ErrorKind::InvalidInput, "invalid path")
            }
            StorageError::ByteConversion(_) => {
                std::io::Error::new(ErrorKind::Other, "byte conversion error")
            }
            StorageError::Unavailable => {
                std::io::Error::new(ErrorKind::Other, "data not available")
            }
            StorageError::NotFound => std::io::Error::new(ErrorKind::NotFound, "not found"),
            StorageError::NotADirectory => {
                std::io::Error::new(ErrorKind::NotADirectory, "not a directory")
            }
            StorageError::IsADirectory => {
                std::io::Error::new(ErrorKind::IsADirectory, "is a directory")
            }
            StorageError::JoinError(_) => std::io::Error::new(ErrorKind::Other, "join error"),
            StorageError::InvalidRsyncSignature => {
                std::io::Error::new(ErrorKind::InvalidInput, "invalid rsync signature")
            }
            StorageError::UnknownArena(_) => {
                std::io::Error::new(ErrorKind::NotFound, "unknown arena")
            }
            StorageError::NoLocalStorage(_) => {
                std::io::Error::new(ErrorKind::NotFound, "no local storage")
            }
            StorageError::InconsistentDatabase(_) => {
                std::io::Error::new(ErrorKind::Other, "inconsistent database")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;

    #[test]
    fn test_storage_error_into_io_error() {
        // Test various StorageError variants
        let db_error = StorageError::Database(Box::new(redb::Error::Io(std::io::Error::new(
            ErrorKind::Other,
            "test",
        ))));
        let io_error: std::io::Error = db_error.into();
        assert_eq!(io_error.kind(), ErrorKind::Other);
        assert_eq!(io_error.to_string(), "database error");

        let not_found = StorageError::NotFound;
        let io_error: std::io::Error = not_found.into();
        assert_eq!(io_error.kind(), ErrorKind::NotFound);
        assert_eq!(io_error.to_string(), "not found");

        let not_a_dir = StorageError::NotADirectory;
        let io_error: std::io::Error = not_a_dir.into();
        assert_eq!(io_error.kind(), ErrorKind::NotADirectory);
        assert_eq!(io_error.to_string(), "not a directory");

        let is_a_dir = StorageError::IsADirectory;
        let io_error: std::io::Error = is_a_dir.into();
        assert_eq!(io_error.kind(), ErrorKind::IsADirectory);
        assert_eq!(io_error.to_string(), "is a directory");

        let invalid_input = StorageError::InvalidRsyncSignature;
        let io_error: std::io::Error = invalid_input.into();
        assert_eq!(io_error.kind(), ErrorKind::InvalidInput);
        assert_eq!(io_error.to_string(), "invalid rsync signature");
    }
}
