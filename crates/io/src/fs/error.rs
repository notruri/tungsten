use thiserror::Error;

/// Errors returned by filesystem-backed chunk storage.
#[derive(Debug, Error)]
pub enum FilesystemError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid chunk key: {0}")]
    InvalidKey(String),
    #[error("invalid chunk state: {0}")]
    InvalidState(String),
}
