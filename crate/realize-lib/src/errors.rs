/// Errors returned by functions in Realize.
#[derive(Debug, thiserror::Error)]
pub enum RealizeError {
    #[error("Invalid path. Paths must be valid unicode and not contain ., .. or :")]
    InvalidPath,
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
