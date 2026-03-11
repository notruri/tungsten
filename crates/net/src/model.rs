use std::fmt::{Display, Formatter};
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Stable identifier for a queued download.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DownloadId(pub u64);

impl Display for DownloadId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// User-facing request to add a download into the queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadRequest {
    pub url: String,
    pub destination: PathBuf,
    pub conflict: ConflictPolicy,
    pub integrity: IntegrityRule,
}

impl DownloadRequest {
    pub fn new(
        url: String,
        destination: impl Into<PathBuf>,
        conflict: ConflictPolicy,
        integrity: IntegrityRule,
    ) -> Self {
        Self {
            url,
            destination: destination.into(),
            conflict,
            integrity,
        }
    }

    pub fn validate(&self) -> Result<(), crate::error::NetError> {
        if self.url.trim().is_empty() {
            return Err(crate::error::NetError::InvalidRequest(
                "url is required".to_string(),
            ));
        }

        if !(self.url.starts_with("http://") || self.url.starts_with("https://")) {
            return Err(crate::error::NetError::InvalidRequest(
                "url must start with http:// or https://".to_string(),
            ));
        }

        if self.destination.as_os_str().is_empty() {
            return Err(crate::error::NetError::InvalidRequest(
                "destination is required".to_string(),
            ));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConflictPolicy {
    AutoRename,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IntegrityRule {
    None,
    Sha256(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DownloadStatus {
    Queued,
    Running,
    Paused,
    Verifying,
    Completed,
    Failed,
    Cancelled,
}

/// User-facing progress telemetry for one download.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProgressSnapshot {
    pub downloaded: u64,
    pub total: Option<u64>,
    pub speed_bps: Option<u64>,
    pub eta_seconds: Option<u64>,
}

/// Public queue record returned to the GUI and event subscribers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadRecord {
    pub id: DownloadId,
    pub request: DownloadRequest,
    pub destination: Option<PathBuf>,
    pub supports_resume: bool,
    pub status: DownloadStatus,
    pub progress: ProgressSnapshot,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl DownloadRecord {
    pub(crate) fn now() -> DateTime<Utc> {
        Utc::now()
    }
}

/// Queue event stream consumed by the GUI.
#[derive(Debug, Clone)]
pub enum QueueEvent {
    Added(DownloadRecord),
    Updated(DownloadRecord),
    Removed(DownloadId),
}
