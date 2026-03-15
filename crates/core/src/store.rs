use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::CoreError;
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
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
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
        self.updated_at = Utc::now();
    }
}

/// Persistence boundary used by the queue service.
pub trait QueueStore: Send + Sync {
    fn load_queue(&self) -> Result<PersistedQueue, CoreError>;
    fn save_queue(&self, state: &PersistedQueue) -> Result<(), CoreError>;
}

#[cfg(test)]
mod tests {
    use chrono::Duration;

    use super::*;
    use crate::model::{ConflictPolicy, IntegrityRule};

    fn download() -> PersistedDownload {
        let now = Utc::now();
        PersistedDownload {
            id: DownloadId(7),
            request: DownloadRequest::new(
                "https://example.com/file.bin".to_string(),
                "file.bin",
                ConflictPolicy::AutoRename,
                IntegrityRule::Sha256("abc".to_string()),
            )
            .speed_limit_kbps(Some(128)),
            destination: Some("out.bin".into()),
            loaded_from_store: true,
            temp_path: "out.bin.part".into(),
            temp_layout: TempLayout::Single,
            supports_resume: true,
            status: DownloadStatus::Running,
            progress: ProgressSnapshot {
                downloaded: 32,
                total: Some(64),
                speed_bps: Some(16),
                eta_seconds: Some(2),
            },
            error: Some("boom".to_string()),
            etag: Some("etag".to_string()),
            last_modified: Some("now".to_string()),
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn persisted_download_to_record_copies_user_visible_fields() {
        let download = download();

        let record = download.to_record();

        assert_eq!(record.id, download.id);
        assert_eq!(record.request.url, download.request.url);
        assert_eq!(record.destination, download.destination);
        assert_eq!(record.supports_resume, download.supports_resume);
        assert_eq!(record.status, download.status);
        assert_eq!(record.progress.downloaded, download.progress.downloaded);
        assert_eq!(record.error, download.error);
        assert_eq!(record.created_at, download.created_at);
        assert_eq!(record.updated_at, download.updated_at);
    }

    #[test]
    fn persisted_download_touch_updates_timestamp() {
        let mut download = download();
        let previous = download.updated_at - Duration::seconds(5);
        download.updated_at = previous;

        download.touch();

        assert!(download.updated_at > previous);
    }
}
