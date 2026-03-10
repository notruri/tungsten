use thiserror::Error;

use crate::model::DownloadId;

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
