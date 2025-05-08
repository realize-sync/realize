use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RealizeError {
    /// Returned by the RealizeService when given an invalid request.
    #[error("Bad request: {0}")]
    BadRequest(String),

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, RealizeError>;
