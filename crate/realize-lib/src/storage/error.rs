use crate::model::{self, Arena};
use crate::utils::holder::ByteConversionError;
use tokio::task::JoinError;

/// Error returned by the [UnrealCache].
///
/// This type exists mainly so that errors can be converted when
/// needed to OS I/O errors.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("redb error {0}")]
    Database(Box<redb::Error>), // a box to keep size in check

    #[error("I/O error {0}")]
    Io(#[from] std::io::Error),

    #[error("bincode error {0}")]
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

    #[error("unknown arena: {0}")]
    UnknownArena(Arena),
}

impl StorageError {
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

impl From<model::PathError> for StorageError {
    fn from(_: model::PathError) -> Self {
        StorageError::from(ByteConversionError::Invalid("path"))
    }
}
