use thiserror::Error;
use tungsten_core::CoreError;

/// Errors emitted by writer backends.
#[derive(Debug, Error)]
pub enum WriterError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid stream index: {0}")]
    InvalidStream(usize),
    #[error("writer state error: {0}")]
    State(String),
}

impl From<WriterError> for CoreError {
    fn from(value: WriterError) -> Self {
        match value {
            WriterError::Io(error) => CoreError::Io(error),
            WriterError::InvalidStream(index) => {
                CoreError::State(format!("invalid writer stream index: {index}"))
            }
            WriterError::State(message) => CoreError::State(message),
        }
    }
}
