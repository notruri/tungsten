use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::CoreError;
use crate::model::{DownloadRequest, ProgressSnapshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlSignal {
    Run,
    Pause,
    Cancel,
}

/// Metadata discovered before transfer starts.
#[derive(Debug, Clone, Default)]
pub struct ProbeInfo {
    pub total_size: Option<u64>,
    pub accept_ranges: bool,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub file_name: Option<String>,
}

/// Internal transfer task constructed by the queue lifecycle.
#[derive(Debug, Clone)]
pub struct TransferTask {
    pub request: DownloadRequest,
    pub temp_path: PathBuf,
    pub temp_layout: TempLayout,
    pub existing_size: u64,
    pub etag: Option<String>,
    pub resume_speed_bps: Option<u64>,
}

/// Internal progress update emitted by transfer implementations.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TransferUpdate {
    pub progress: ProgressSnapshot,
    #[serde(default)]
    pub temp_layout: TempLayout,
}

impl TransferUpdate {
    pub fn from_progress(progress: ProgressSnapshot) -> Self {
        Self {
            progress,
            temp_layout: TempLayout::Single,
        }
    }
}

#[derive(Debug, Clone)]
pub enum TransferOutcome {
    Completed(TransferUpdate),
    Paused(TransferUpdate),
    Cancelled(TransferUpdate),
}

/// Internal temp-file layout used to resume downloads safely.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum TempLayout {
    #[default]
    Single,
    Multipart(MultipartState),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartState {
    pub total_size: u64,
    pub parts: Vec<MultipartPart>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartPart {
    pub index: usize,
    pub start: u64,
    pub end: u64,
    pub path: PathBuf,
}

/// Transport boundary used by the queue orchestrator.
pub trait Transfer: Send + Sync {
    fn probe(&self, request: &DownloadRequest) -> Result<ProbeInfo, CoreError>;

    fn download(
        &self,
        task: &TransferTask,
        probe: Option<ProbeInfo>,
        on_update: &mut dyn FnMut(TransferUpdate) -> Result<(), CoreError>,
        control: &dyn Fn() -> ControlSignal,
    ) -> Result<TransferOutcome, CoreError>;

    fn set_connections(&self, _connections: usize) {}
}
