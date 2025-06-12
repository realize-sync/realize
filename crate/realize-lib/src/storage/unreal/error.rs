use tokio::task::JoinError;

/// Error returned by the [UnrealCache].
///
/// This type exists mainly so that errors can be converted when
/// needed to OS I/O errors.
#[derive(Debug, thiserror::Error)]
pub enum UnrealCacheError {
    #[error("redb error {0}")]
    DatabaseError(#[from] redb::Error),

    #[error{"not found"}]
    NotFound,

    #[error{"not a directory"}]
    NotADirectory,

    #[error{"is a directory"}]
    IsADirectory,

    #[error(transparent)]
    JoinError(#[from] JoinError),
}

impl From<redb::TableError> for UnrealCacheError {
    fn from(value: redb::TableError) -> Self {
        UnrealCacheError::DatabaseError(value.into())
    }
}

impl From<redb::StorageError> for UnrealCacheError {
    fn from(value: redb::StorageError) -> Self {
        UnrealCacheError::DatabaseError(value.into())
    }
}

impl From<redb::TransactionError> for UnrealCacheError {
    fn from(value: redb::TransactionError) -> Self {
        UnrealCacheError::DatabaseError(value.into())
    }
}

impl From<redb::DatabaseError> for UnrealCacheError {
    fn from(value: redb::DatabaseError) -> Self {
        UnrealCacheError::DatabaseError(value.into())
    }
}
impl From<redb::CommitError> for UnrealCacheError {
    fn from(value: redb::CommitError) -> Self {
        UnrealCacheError::DatabaseError(value.into())
    }
}
