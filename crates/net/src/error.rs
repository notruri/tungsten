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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn net_error_converts_to_core_error() {
        let backend = CoreError::from(NetError::Backend("broken".to_string()));
        let download = CoreError::from(NetError::DownloadNotFound(DownloadId(7)));

        match backend {
            CoreError::Backend(message) => assert_eq!(message, "broken"),
            other => panic!("expected backend error, got {other:?}"),
        }
        match download {
            CoreError::DownloadNotFound(id) => assert_eq!(id.0, 7),
            other => panic!("expected download not found, got {other:?}"),
        }
    }

    #[test]
    fn core_error_converts_to_net_error() {
        let state = NetError::from(CoreError::State("bad".to_string()));

        match state {
            NetError::State(message) => assert_eq!(message, "bad"),
            other => panic!("expected state error, got {other:?}"),
        }
    }

    #[test]
    fn writer_error_invalid_stream_becomes_state_error() {
        let error = NetError::from(WriterError::InvalidStream(3));

        match error {
            NetError::State(message) => assert!(message.contains("invalid writer stream index: 3")),
            other => panic!("expected state error, got {other:?}"),
        }
    }
}
