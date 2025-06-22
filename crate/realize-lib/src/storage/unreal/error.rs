use tokio::task::JoinError;

use crate::{model::Arena, utils::holder::ByteConversionError};

/// Error returned by the [UnrealCache].
///
/// This type exists mainly so that errors can be converted when
/// needed to OS I/O errors.
#[derive(Debug, thiserror::Error)]
pub enum UnrealError {
    #[error("redb error {0}")]
    Database(redb::Error),

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

impl UnrealError {
    fn from_redb(err: redb::Error) -> UnrealError {
        if let redb::Error::Io(_) = &err {
            match err {
                redb::Error::Io(err) => UnrealError::Io(err),
                _ => unreachable!(),
            }
        } else {
            UnrealError::Database(err)
        }
    }
}

impl From<redb::Error> for UnrealError {
    fn from(value: redb::Error) -> Self {
        UnrealError::from_redb(value)
    }
}

impl From<redb::TableError> for UnrealError {
    fn from(value: redb::TableError) -> Self {
        UnrealError::from_redb(value.into())
    }
}

impl From<redb::StorageError> for UnrealError {
    fn from(value: redb::StorageError) -> Self {
        UnrealError::from_redb(value.into())
    }
}

impl From<redb::TransactionError> for UnrealError {
    fn from(value: redb::TransactionError) -> Self {
        UnrealError::from_redb(value.into())
    }
}

impl From<redb::DatabaseError> for UnrealError {
    fn from(value: redb::DatabaseError) -> Self {
        UnrealError::from_redb(value.into())
    }
}
impl From<redb::CommitError> for UnrealError {
    fn from(value: redb::CommitError) -> Self {
        UnrealError::from_redb(value.into())
    }
}
