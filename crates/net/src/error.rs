use thiserror::Error;
use tungsten_core::{CoreError, DownloadId};
use tungsten_io::WriterError;

#[derive(Debug, Error)]
pub enum NetError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("state error: {0}")]
    State(String),
    #[error("backend error: {0}")]
    Backend(String),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("download not found: {0}")]
    DownloadNotFound(DownloadId),
}

impl From<NetError> for CoreError {
    fn from(value: NetError) -> Self {
        match value {
            NetError::Io(error) => CoreError::Io(error),
            NetError::Http(error) => CoreError::Backend(error.to_string()),
            NetError::State(message) => CoreError::State(message),
            NetError::Backend(message) => CoreError::Backend(message),
            NetError::InvalidRequest(message) => CoreError::InvalidRequest(message),
            NetError::DownloadNotFound(id) => {
                CoreError::DownloadNotFound(tungsten_core::DownloadId(id.0))
            }
        }
    }
}

impl From<CoreError> for NetError {
    fn from(value: CoreError) -> Self {
        match value {
            CoreError::Io(error) => NetError::Io(error),
            CoreError::State(message) => NetError::State(message),
            CoreError::Backend(message) => NetError::Backend(message),
            CoreError::InvalidRequest(message) => NetError::InvalidRequest(message),
            CoreError::DownloadNotFound(id) => NetError::DownloadNotFound(DownloadId(id.0)),
        }
    }
}

impl From<WriterError> for NetError {
    fn from(value: WriterError) -> Self {
        match value {
            WriterError::Io(error) => NetError::Io(error),
            WriterError::InvalidStream(index) => {
                NetError::State(format!("invalid writer stream index: {index}"))
            }
            WriterError::State(message) => NetError::State(message),
        }
    }
}
