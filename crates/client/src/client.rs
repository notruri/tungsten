use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;

use interprocess::local_socket::{GenericFilePath, GenericNamespaced, Name, Stream, prelude::*};
use thiserror::Error;
use tracing::warn;
use tungsten_config::app_socket_path;
use tungsten_ipc::{
    DEFAULT_SOCKET_NAME, Event, EventMessage, IpcError, MessageId, RemoteError, Request,
    RequestMessage, Response, ResponseMessage, read_frame, write_frame,
};

pub use tungsten_core::*;
pub use tungsten_ipc::{BackendConfig, ThemePreference};

#[derive(Debug)]
pub struct Client {
    next_id: AtomicU64,
    socket_name: Name<'static>,
}

impl Client {
    pub fn new() -> Result<Self, ClientError> {
        Ok(Self {
            next_id: AtomicU64::new(1),
            socket_name: resolve_socket_name()?,
        })
    }

    pub fn enqueue(&self, request: DownloadRequest) -> Result<DownloadId, ClientError> {
        match self.send_request(Request::Enqueue { request })? {
            Response::Enqueued { download_id } => Ok(download_id),
            response => Err(unexpected_response("enqueued", &response)),
        }
    }

    pub fn pause(&self, download_id: DownloadId) -> Result<(), ClientError> {
        self.send_ack(Request::Pause { download_id })
    }

    pub fn resume(&self, download_id: DownloadId) -> Result<(), ClientError> {
        self.send_ack(Request::Resume { download_id })
    }

    pub fn cancel(&self, download_id: DownloadId) -> Result<(), ClientError> {
        self.send_ack(Request::Cancel { download_id })
    }

    pub fn remove(&self, download_id: DownloadId) -> Result<(), ClientError> {
        self.send_ack(Request::Remove { download_id })
    }

    pub fn set_connections(&self, connections: usize) -> Result<(), ClientError> {
        self.send_ack(Request::SetConnections { connections })
    }

    pub fn set_download_limit(&self, download_limit_kbps: u64) -> Result<(), ClientError> {
        self.send_ack(Request::SetDownloadLimit {
            download_limit_kbps,
        })
    }

    pub fn set_speed_limit(
        &self,
        download_id: DownloadId,
        speed_limit_kbps: Option<u64>,
    ) -> Result<(), ClientError> {
        self.send_ack(Request::SetSpeedLimit {
            download_id,
            speed_limit_kbps,
        })
    }

    pub fn set_fallback_filename(
        &self,
        fallback_filename: impl Into<String>,
    ) -> Result<(), ClientError> {
        self.send_ack(Request::SetFallbackFilename {
            fallback_filename: fallback_filename.into(),
        })
    }

    pub fn set_max_parallel(&self, max_parallel: usize) -> Result<(), ClientError> {
        self.send_ack(Request::SetMaxParallel { max_parallel })
    }

    pub fn get_config(&self) -> Result<BackendConfig, ClientError> {
        match self.send_request(Request::GetConfig)? {
            Response::Config { config } => Ok(config),
            response => Err(unexpected_response("config", &response)),
        }
    }

    pub fn set_config(&self, config: BackendConfig) -> Result<(), ClientError> {
        self.send_ack(Request::SetConfig { config })
    }

    pub fn set_temp_root(&self, temp_root: PathBuf) -> Result<(), ClientError> {
        self.send_ack(Request::SetTempRoot { temp_root })
    }

    pub fn snapshot(&self) -> Result<Vec<DownloadRecord>, ClientError> {
        match self.send_request(Request::Snapshot)? {
            Response::Snapshot { downloads } => Ok(downloads),
            response => Err(unexpected_response("snapshot", &response)),
        }
    }

    pub fn subscribe(&self) -> Result<mpsc::Receiver<QueueEvent>, ClientError> {
        let request = RequestMessage {
            id: self.next_message_id(),
            request: Request::Subscribe,
        };
        let mut stream = connect(&self.socket_name)?;
        write_frame(&mut stream, &request)?;

        let response: ResponseMessage = read_frame(&mut stream)?;
        if response.id != request.id {
            return Err(ClientError::MismatchedResponseId {
                expected: request.id,
                received: response.id,
            });
        }

        match response.response {
            Response::Subscribed => {}
            Response::Error(error) => return Err(ClientError::Remote(error)),
            response => return Err(unexpected_response("subscribed", &response)),
        }

        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            loop {
                let event = match read_frame::<_, EventMessage>(&mut stream) {
                    Ok(event) => event,
                    Err(IpcError::Io(error)) if error.kind() == ErrorKind::UnexpectedEof => break,
                    Err(error) => {
                        warn!(error = %error, "queue subscription ended");
                        break;
                    }
                };

                let queue_event = match event.event {
                    Event::Queue { event } => QueueEvent::from(event),
                };

                if tx.send(queue_event).is_err() {
                    break;
                }
            }
        });

        Ok(rx)
    }

    fn send_ack(&self, request: Request) -> Result<(), ClientError> {
        match self.send_request(request)? {
            Response::Ack => Ok(()),
            response => Err(unexpected_response("ack", &response)),
        }
    }

    fn send_request(&self, request: Request) -> Result<Response, ClientError> {
        let message = RequestMessage {
            id: self.next_message_id(),
            request,
        };
        let mut stream = connect(&self.socket_name)?;

        write_frame(&mut stream, &message)?;
        let response: ResponseMessage = read_frame(&mut stream)?;
        if response.id != message.id {
            return Err(ClientError::MismatchedResponseId {
                expected: message.id,
                received: response.id,
            });
        }

        match response.response {
            Response::Error(error) => Err(ClientError::Remote(error)),
            response => Ok(response),
        }
    }

    fn next_message_id(&self) -> MessageId {
        MessageId(self.next_id.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("ipc error: {0}")]
    Ipc(#[from] IpcError),
    #[error("remote error ({kind:?}): {message}", kind = .0.kind, message = .0.message)]
    Remote(RemoteError),
    #[error("response id mismatch: expected {expected:?}, received {received:?}")]
    MismatchedResponseId {
        expected: MessageId,
        received: MessageId,
    },
    #[error("unexpected response: expected {expected}, received {actual}")]
    UnexpectedResponse {
        expected: &'static str,
        actual: &'static str,
    },
    #[error("{0}")]
    Path(String),
}

fn unexpected_response(expected: &'static str, response: &Response) -> ClientError {
    ClientError::UnexpectedResponse {
        expected,
        actual: response_kind(response),
    }
}

fn response_kind(response: &Response) -> &'static str {
    match response {
        Response::Pong => "pong",
        Response::Ack => "ack",
        Response::Config { .. } => "config",
        Response::Snapshot { .. } => "snapshot",
        Response::Enqueued { .. } => "enqueued",
        Response::Subscribed => "subscribed",
        Response::Error(_) => "error",
    }
}

fn connect(name: &Name<'_>) -> Result<Stream, ClientError> {
    Stream::connect(name.borrow()).map_err(|error| ClientError::Ipc(IpcError::Io(error)))
}

fn resolve_socket_name() -> Result<Name<'static>, ClientError> {
    if GenericNamespaced::is_supported() {
        return DEFAULT_SOCKET_NAME
            .to_ns_name::<GenericNamespaced>()
            .map(Name::into_owned)
            .map_err(|error| ClientError::Path(error.to_string()));
    }

    let socket_path = app_socket_path().map_err(|error| ClientError::Path(error.to_string()))?;
    socket_path
        .to_string_lossy()
        .as_ref()
        .to_fs_name::<GenericFilePath>()
        .map(Name::into_owned)
        .map_err(|error| ClientError::Path(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_kind_matches_variants() {
        assert_eq!(response_kind(&Response::Ack), "ack");
        assert_eq!(response_kind(&Response::Pong), "pong");
        assert_eq!(response_kind(&Response::Subscribed), "subscribed");
    }

    #[test]
    fn socket_name_resolves() {
        let name = resolve_socket_name().expect("socket name should resolve");
        assert!(!format!("{name:?}").is_empty());
    }
}
