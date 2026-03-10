use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::NetError;
use crate::model::{DownloadId, DownloadRecord, DownloadRequest, DownloadStatus, ProgressSnapshot};
use crate::transfer::TempLayout;

/// Persisted queue snapshot stored by disk-backed implementations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PersistedQueue {
    pub next_id: u64,
    pub downloads: Vec<PersistedDownload>,
}

/// Internal persisted representation of one queued download.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedDownload {
    pub id: DownloadId,
    pub request: DownloadRequest,
    #[serde(default)]
    pub destination: Option<PathBuf>,
    #[serde(default)]
    pub loaded_from_store: bool,
    pub temp_path: PathBuf,
    #[serde(default)]
    pub temp_layout: TempLayout,
    pub supports_resume: bool,
    pub status: DownloadStatus,
    pub progress: ProgressSnapshot,
    pub error: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub created_at: u64,
    pub updated_at: u64,
}

impl PersistedDownload {
    /// Returns the user-facing record shape used by snapshots and events.
    pub fn to_record(&self) -> DownloadRecord {
        DownloadRecord {
            id: self.id,
            request: self.request.clone(),
            destination: self.destination.clone(),
            supports_resume: self.supports_resume,
            status: self.status.clone(),
            progress: self.progress.clone(),
            error: self.error.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }

    pub(crate) fn touch(&mut self) {
        self.updated_at = DownloadRecord::now_epoch();
    }
}

/// Persistence boundary used by the queue service.
pub trait QueueStore: Send + Sync {
    fn load_queue(&self) -> Result<PersistedQueue, NetError>;
    fn save_queue(&self, state: &PersistedQueue) -> Result<(), NetError>;
}
