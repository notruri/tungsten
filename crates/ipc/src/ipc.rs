use std::io::{Read, Write};
use std::path::PathBuf;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tungsten_core::{
    CoreError, DEFAULT_DOWNLOAD_FILE_NAME, DownloadId, DownloadRecord, DownloadRequest, QueueEvent,
};

pub const DEFAULT_SOCKET_NAME: &str = "tungsten";
pub const MAX_FRAME_SIZE: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId(pub u64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestMessage {
    pub id: MessageId,
    pub request: Request,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseMessage {
    pub id: MessageId,
    pub response: Response,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMessage {
    pub event: Event,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThemePreference {
    #[default]
    System,
    Light,
    Dark,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppConfig {
    pub download_root: PathBuf,
    pub temp_dir: PathBuf,
    pub fallback_filename: String,
    pub max_parallel: usize,
    pub connections: usize,
    pub download_limit_kbps: u64,
    pub minimize_to_tray: bool,
    pub theme: ThemePreference,
}

impl AppConfig {
    pub fn normalize(mut self) -> Self {
        if self.temp_dir.as_os_str().is_empty() {
            self.temp_dir = self.download_root.join("tmp");
        }

        self.fallback_filename = self.fallback_filename.trim().to_string();
        if self.fallback_filename.is_empty() {
            self.fallback_filename = DEFAULT_DOWNLOAD_FILE_NAME.to_string();
        }

        self.max_parallel = self.max_parallel.max(1);
        self.connections = self.connections.max(1);
        self
    }

    pub fn validate(&self) -> Result<(), CoreError> {
        if self.download_root.as_os_str().is_empty() {
            return Err(CoreError::InvalidRequest(
                "download root must not be empty".to_string(),
            ));
        }
        if self.temp_dir.as_os_str().is_empty() {
            return Err(CoreError::InvalidRequest(
                "temp dir must not be empty".to_string(),
            ));
        }

        let fallback = self.fallback_filename.trim();
        if fallback.is_empty() {
            return Err(CoreError::InvalidRequest(
                "fallback filename must not be empty".to_string(),
            ));
        }
        if fallback.contains('/') || fallback.contains('\\') {
            return Err(CoreError::InvalidRequest(
                "fallback filename must not contain path separators".to_string(),
            ));
        }

        if self.max_parallel == 0 {
            return Err(CoreError::InvalidRequest(
                "max_parallel must be at least 1".to_string(),
            ));
        }
        if self.connections == 0 {
            return Err(CoreError::InvalidRequest(
                "connections must be at least 1".to_string(),
            ));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    Ping,
    GetConfig,
    SetConfig {
        config: AppConfig,
    },
    Snapshot,
    Subscribe,
    Enqueue {
        request: DownloadRequest,
    },
    Pause {
        download_id: DownloadId,
    },
    Resume {
        download_id: DownloadId,
    },
    Cancel {
        download_id: DownloadId,
    },
    Remove {
        download_id: DownloadId,
    },
    SetConnections {
        connections: usize,
    },
    SetDownloadLimit {
        download_limit_kbps: u64,
    },
    SetSpeedLimit {
        download_id: DownloadId,
        speed_limit_kbps: Option<u64>,
    },
    SetFallbackFilename {
        fallback_filename: String,
    },
    SetMaxParallel {
        max_parallel: usize,
    },
    SetTempRoot {
        temp_root: PathBuf,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    Pong,
    Ack,
    Config { config: AppConfig },
    Snapshot { downloads: Vec<DownloadRecord> },
    Enqueued { download_id: DownloadId },
    Subscribed,
    Error(RemoteError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    Queue { event: QueueNotification },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QueueNotification {
    Added { record: DownloadRecord },
    Updated { record: DownloadRecord },
    Removed { download_id: DownloadId },
    Batch { events: Vec<QueueNotification> },
}

impl From<QueueEvent> for QueueNotification {
    fn from(value: QueueEvent) -> Self {
        match value {
            QueueEvent::Added(record) => Self::Added { record },
            QueueEvent::Updated(record) => Self::Updated { record },
            QueueEvent::Removed(download_id) => Self::Removed { download_id },
            QueueEvent::Batch(events) => Self::Batch {
                events: events.into_iter().map(Self::from).collect(),
            },
        }
    }
}

impl From<QueueNotification> for QueueEvent {
    fn from(value: QueueNotification) -> Self {
        match value {
            QueueNotification::Added { record } => Self::Added(record),
            QueueNotification::Updated { record } => Self::Updated(record),
            QueueNotification::Removed { download_id } => Self::Removed(download_id),
            QueueNotification::Batch { events } => {
                Self::Batch(events.into_iter().map(Self::from).collect())
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteErrorKind {
    Io,
    State,
    Backend,
    InvalidRequest,
    DownloadNotFound,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteError {
    pub kind: RemoteErrorKind,
    pub message: String,
}

impl From<CoreError> for RemoteError {
    fn from(value: CoreError) -> Self {
        match value {
            CoreError::Io(error) => Self {
                kind: RemoteErrorKind::Io,
                message: error.to_string(),
            },
            CoreError::State(message) => Self {
                kind: RemoteErrorKind::State,
                message,
            },
            CoreError::Backend(message) => Self {
                kind: RemoteErrorKind::Backend,
                message,
            },
            CoreError::InvalidRequest(message) => Self {
                kind: RemoteErrorKind::InvalidRequest,
                message,
            },
            CoreError::DownloadNotFound(download_id) => Self {
                kind: RemoteErrorKind::DownloadNotFound,
                message: download_id.to_string(),
            },
        }
    }
}

#[derive(Debug, Error)]
pub enum IpcError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("frame size {size} exceeds max {max}")]
    FrameTooLarge { size: usize, max: usize },
    #[error("frame length {length} exceeds max {max}")]
    FrameLengthTooLarge { length: u32, max: usize },
}

pub fn write_frame<W, T>(writer: &mut W, value: &T) -> Result<(), IpcError>
where
    W: Write,
    T: Serialize,
{
    let payload = serde_json::to_vec(value)?;
    if payload.len() > MAX_FRAME_SIZE {
        return Err(IpcError::FrameTooLarge {
            size: payload.len(),
            max: MAX_FRAME_SIZE,
        });
    }

    let length = payload.len() as u32;
    writer.write_all(&length.to_le_bytes())?;
    writer.write_all(&payload)?;
    writer.flush()?;
    Ok(())
}

pub fn read_frame<R, T>(reader: &mut R) -> Result<T, IpcError>
where
    R: Read,
    T: DeserializeOwned,
{
    let mut length_bytes = [0u8; 4];
    reader.read_exact(&mut length_bytes)?;

    let length = u32::from_le_bytes(length_bytes);
    if length as usize > MAX_FRAME_SIZE {
        return Err(IpcError::FrameLengthTooLarge {
            length,
            max: MAX_FRAME_SIZE,
        });
    }

    let mut payload = vec![0u8; length as usize];
    reader.read_exact(&mut payload)?;
    Ok(serde_json::from_slice(&payload)?)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tungsten_core::{ConflictPolicy, IntegrityRule};

    use super::*;

    #[test]
    fn queue_notification_round_trips_queue_event() {
        let record = DownloadRecord {
            id: DownloadId(7),
            request: DownloadRequest::new(
                "https://example.com/file.bin".to_string(),
                PathBuf::from("downloads/file.bin"),
                ConflictPolicy::AutoRename,
                IntegrityRule::None,
            ),
            destination: Some(PathBuf::from("downloads/file.bin")),
            supports_resume: true,
            status: tungsten_core::DownloadStatus::Queued,
            progress: tungsten_core::ProgressSnapshot::default(),
            error: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let event = QueueEvent::Batch(vec![
            QueueEvent::Added(record.clone()),
            QueueEvent::Removed(record.id),
        ]);

        let notification = QueueNotification::from(event.clone());
        let restored = QueueEvent::from(notification);

        match restored {
            QueueEvent::Batch(events) => {
                assert_eq!(events.len(), 2);
                match &events[0] {
                    QueueEvent::Added(restored_record) => {
                        assert_eq!(restored_record.id, record.id);
                    }
                    _ => panic!("expected added event"),
                }
                match &events[1] {
                    QueueEvent::Removed(download_id) => {
                        assert_eq!(*download_id, record.id);
                    }
                    _ => panic!("expected removed event"),
                }
            }
            _ => panic!("expected batch event"),
        }
    }

    #[test]
    fn request_frame_round_trips() {
        let request = RequestMessage {
            id: MessageId(3),
            request: Request::SetTempRoot {
                temp_root: PathBuf::from("downloads/tmp"),
            },
        };
        let mut bytes = Vec::new();

        write_frame(&mut bytes, &request).expect("request should encode");
        let restored: RequestMessage =
            read_frame(&mut bytes.as_slice()).expect("request should decode");

        assert_eq!(restored.id, request.id);
        match restored.request {
            Request::SetTempRoot { temp_root } => {
                assert_eq!(temp_root, PathBuf::from("downloads/tmp"));
            }
            _ => panic!("expected temp root request"),
        }
    }

    #[test]
    fn oversized_frame_is_rejected() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&((MAX_FRAME_SIZE as u32) + 1).to_le_bytes());
        bytes.extend_from_slice(&[0u8; 4]);

        let error = read_frame::<_, RequestMessage>(&mut bytes.as_slice())
            .expect_err("oversized frame should fail");

        assert!(matches!(error, IpcError::FrameLengthTooLarge { .. }));
    }
}
