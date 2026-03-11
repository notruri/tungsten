mod multipart;
mod single;
pub(crate) mod temp;

#[cfg(test)]
mod tests;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use reqwest::blocking::Client;
use reqwest::header::{ACCEPT_RANGES, CONTENT_DISPOSITION, ETAG, LAST_MODIFIED};
use serde::{Deserialize, Serialize};

use crate::error::NetError;
use crate::model::{DownloadRequest, ProgressSnapshot};

pub(crate) const DOWNLOAD_BUFFER_SIZE: usize = 64 * 1024;

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
    pub temp_path: std::path::PathBuf,
    pub temp_layout: TempLayout,
    pub existing_size: u64,
    pub etag: Option<String>,
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
    pub path: std::path::PathBuf,
}

/// Transport boundary used by the queue orchestrator.
pub trait Transfer: Send + Sync {
    fn probe(&self, request: &DownloadRequest) -> Result<ProbeInfo, NetError>;

    fn download(
        &self,
        task: &TransferTask,
        probe: Option<ProbeInfo>,
        on_update: &mut dyn FnMut(TransferUpdate) -> Result<(), NetError>,
        control: &dyn Fn() -> ControlSignal,
    ) -> Result<TransferOutcome, NetError>;

    fn set_connections(&self, _connections: usize) {}
}

/// Blocking reqwest transfer implementation.
#[derive(Debug)]
pub struct ReqwestTransfer {
    client: Client,
    connections: AtomicUsize,
}

impl ReqwestTransfer {
    pub fn new(connections: usize) -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .unwrap_or_else(|_| Client::new()),
            connections: AtomicUsize::new(connections.max(1)),
        }
    }
}

impl Default for ReqwestTransfer {
    fn default() -> Self {
        Self::new(1)
    }
}

impl Transfer for ReqwestTransfer {
    fn probe(&self, request: &DownloadRequest) -> Result<ProbeInfo, NetError> {
        let response = self.client.head(&request.url).send();

        let head = match response {
            Ok(resp) => resp,
            Err(_) => return Ok(ProbeInfo::default()),
        };

        let total_size = head
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());

        let accept_ranges = head
            .headers()
            .get(ACCEPT_RANGES)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.eq_ignore_ascii_case("bytes"))
            .unwrap_or(false);

        let etag = head
            .headers()
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(ToString::to_string);

        let last_modified = head
            .headers()
            .get(LAST_MODIFIED)
            .and_then(|value| value.to_str().ok())
            .map(ToString::to_string);

        let file_name = head
            .headers()
            .get(CONTENT_DISPOSITION)
            .and_then(|value| value.to_str().ok())
            .and_then(parse_content_disposition_file_name);

        Ok(ProbeInfo {
            total_size,
            accept_ranges,
            etag,
            last_modified,
            file_name,
        })
    }

    fn download(
        &self,
        task: &TransferTask,
        probe: Option<ProbeInfo>,
        on_update: &mut dyn FnMut(TransferUpdate) -> Result<(), NetError>,
        control: &dyn Fn() -> ControlSignal,
    ) -> Result<TransferOutcome, NetError> {
        let connections = self.connections.load(Ordering::Relaxed).max(1);
        if let Some(parent) = task.temp_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let probe = match probe {
            Some(probe) => probe,
            None => self.probe(&task.request)?,
        };
        if connections > 1 {
            match &task.temp_layout {
                TempLayout::Multipart(layout) if layout.total_size > 1 => {
                    return match multipart::download(
                        self.client.clone(),
                        connections,
                        task,
                        layout.total_size,
                        on_update,
                        control,
                    ) {
                        Ok(outcome) => Ok(outcome),
                        Err(multipart::MultipartError::RangeNotHonored) => {
                            let restarted = TransferTask {
                                temp_layout: TempLayout::Single,
                                existing_size: 0,
                                ..task.clone()
                            };
                            single::download(
                                &self.client,
                                &restarted,
                                probe.total_size,
                                on_update,
                                control,
                            )
                        }
                        Err(multipart::MultipartError::Other(error)) => Err(error),
                    };
                }
                TempLayout::Single
                    if task.existing_size == 0
                        && probe.accept_ranges
                        && matches!(probe.total_size, Some(total_size) if total_size > 1) =>
                {
                    if let Some(total_size) = probe.total_size {
                        return match multipart::download(
                            self.client.clone(),
                            connections,
                            task,
                            total_size,
                            on_update,
                            control,
                        ) {
                            Ok(outcome) => Ok(outcome),
                            Err(multipart::MultipartError::RangeNotHonored) => {
                                let restarted = TransferTask {
                                    temp_layout: TempLayout::Single,
                                    existing_size: 0,
                                    ..task.clone()
                                };
                                single::download(
                                    &self.client,
                                    &restarted,
                                    probe.total_size,
                                    on_update,
                                    control,
                                )
                            }
                            Err(multipart::MultipartError::Other(error)) => Err(error),
                        };
                    }
                }
                _ => {}
            }
        }

        single::download(&self.client, task, probe.total_size, on_update, control)
    }

    fn set_connections(&self, connections: usize) {
        self.connections
            .store(connections.max(1), Ordering::Relaxed);
    }
}

fn parse_content_disposition_file_name(value: &str) -> Option<String> {
    for part in value.split(';').map(str::trim) {
        if let Some(encoded) = part.strip_prefix("filename*=") {
            let encoded = encoded.trim().trim_matches('"');
            let encoded = encoded
                .split_once("''")
                .map(|(_, file_name)| file_name)
                .unwrap_or(encoded);
            if let Some(decoded) = percent_decode(encoded) {
                let candidate = decoded.trim();
                if !candidate.is_empty() {
                    return Some(candidate.to_string());
                }
            }
        }
    }

    for part in value.split(';').map(str::trim) {
        if let Some(raw) = part.strip_prefix("filename=") {
            let candidate = raw.trim().trim_matches('"');
            if !candidate.is_empty() {
                return Some(candidate.to_string());
            }
        }
    }

    None
}

fn percent_decode(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return None;
            }
            let hi = from_hex(bytes[index + 1])?;
            let lo = from_hex(bytes[index + 2])?;
            decoded.push((hi << 4) | lo);
            index += 3;
            continue;
        }

        decoded.push(bytes[index]);
        index += 1;
    }

    String::from_utf8(decoded).ok()
}

fn from_hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub(crate) fn progress_from_metrics(
    downloaded: u64,
    total: Option<u64>,
    started_at: std::time::Instant,
) -> ProgressSnapshot {
    let elapsed = started_at.elapsed().as_secs_f64();
    let speed_bps = if elapsed > 0.0 {
        Some((downloaded as f64 / elapsed) as u64)
    } else {
        Some(0)
    };

    let eta_seconds = match (total, speed_bps) {
        (Some(total_size), Some(speed)) if speed > 0 && total_size >= downloaded => {
            Some((total_size - downloaded) / speed)
        }
        _ => None,
    };

    ProgressSnapshot {
        downloaded,
        total,
        speed_bps,
        eta_seconds,
    }
}
