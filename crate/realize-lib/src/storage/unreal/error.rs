use tokio::task::JoinError;

use crate::model::Arena;

/// Error returned by the [UnrealCache].
///
/// This type exists mainly so that errors can be converted when
/// needed to OS I/O errors.
#[derive(Debug, thiserror::Error)]
pub enum UnrealCacheError {
    #[error("redb error {0}")]
    Database(redb::Error),

    #[error("I/O error {0}")]
    Io(#[from] std::io::Error),

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

impl UnrealCacheError {
    fn from_redb(err: redb::Error) -> UnrealCacheError {
        if let redb::Error::Io(_) = &err {
            match err {
                redb::Error::Io(err) => UnrealCacheError::Io(err),
                _ => unreachable!(),
            }
        } else {
            UnrealCacheError::Database(err)
        }
    }
}

impl From<redb::Error> for UnrealCacheError {
    fn from(value: redb::Error) -> Self {
        UnrealCacheError::from_redb(value)
    }
}

impl From<redb::TableError> for UnrealCacheError {
    fn from(value: redb::TableError) -> Self {
        UnrealCacheError::from_redb(value.into())
    }
}

impl From<redb::StorageError> for UnrealCacheError {
    fn from(value: redb::StorageError) -> Self {
        UnrealCacheError::from_redb(value.into())
    }
}

impl From<redb::TransactionError> for UnrealCacheError {
    fn from(value: redb::TransactionError) -> Self {
        UnrealCacheError::from_redb(value.into())
    }
}

impl From<redb::DatabaseError> for UnrealCacheError {
    fn from(value: redb::DatabaseError) -> Self {
        UnrealCacheError::from_redb(value.into())
    }
}
impl From<redb::CommitError> for UnrealCacheError {
    fn from(value: redb::CommitError) -> Self {
        UnrealCacheError::from_redb(value.into())
    }
}
