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

    pub fn io_kind(&self) -> std::io::ErrorKind {
        use std::io::ErrorKind::*;
        match self {
            StorageError::Database(_) => Other,
            StorageError::OpenTable(_, _) => Other,
            StorageError::Io(ioerr) => ioerr.kind(),
            StorageError::ByteConversion(_) => InvalidInput,
            StorageError::Unavailable => Other,
            StorageError::NotFound => NotFound,
            StorageError::NotADirectory => NotADirectory,
            StorageError::IsADirectory => IsADirectory,
            StorageError::JoinError(_) => Other,
            StorageError::InvalidRsyncSignature => InvalidInput,
            StorageError::UnknownArena(_) => NotFound,
            StorageError::NoLocalStorage(_) => NotFound,
            StorageError::InconsistentDatabase(_) => Other,
        }
    }

    pub fn as_ioerr(&self) -> std::io::Error {
        std::io::Error::from(self.io_kind())
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
        self.as_ioerr()
    }
}

impl Into<std::io::Error> for &StorageError {
    fn into(self) -> std::io::Error {
        self.as_ioerr()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;

    #[test]
    fn test_storage_error_into_io_error() {
        let not_found = StorageError::NotFound;
        let io_error: std::io::Error = not_found.into();
        assert_eq!(io_error.kind(), ErrorKind::NotFound);

        let not_a_dir = StorageError::NotADirectory;
        let io_error: std::io::Error = not_a_dir.into();
        assert_eq!(io_error.kind(), ErrorKind::NotADirectory);

        let is_a_dir = StorageError::IsADirectory;
        let io_error: std::io::Error = is_a_dir.into();
        assert_eq!(io_error.kind(), ErrorKind::IsADirectory);

        let invalid_input = StorageError::InvalidRsyncSignature;
        let io_error: std::io::Error = invalid_input.into();
        assert_eq!(io_error.kind(), ErrorKind::InvalidInput);
    }
}
