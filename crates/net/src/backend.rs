mod multipart;

#[cfg(test)]
mod tests;

use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use reqwest::blocking::Client;
use reqwest::header::{ACCEPT_RANGES, CONTENT_DISPOSITION, ETAG, IF_RANGE, LAST_MODIFIED, RANGE};

use crate::error::NetError;
use crate::types::{DownloadRequest, DownloadSnapshot, ProgressSnapshot, TempLayout};

pub(crate) const DOWNLOAD_BUFFER_SIZE: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlSignal {
    Run,
    Pause,
    Cancel,
}

#[derive(Debug, Clone, Default)]
/// Metadata discovered from a lightweight probe before starting a download.
pub struct ProbeInfo {
    pub total_size: Option<u64>,
    pub accept_ranges: bool,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub file_name: Option<String>,
}

#[derive(Debug, Clone)]
/// Immutable input passed from the queue worker into a backend run.
///
/// `temp_layout` and `existing_size` describe what is already on disk so the
/// backend can decide whether to resume as single-stream or multipart.
pub struct DownloadTask {
    pub request: DownloadRequest,
    pub temp_path: PathBuf,
    pub temp_layout: TempLayout,
    pub existing_size: u64,
    pub allow_resume: bool,
    pub etag: Option<String>,
}

#[derive(Debug, Clone)]
/// Final result returned by the backend when a download stops running.
pub enum DownloadOutcome {
    Completed(DownloadSnapshot),
    Paused(DownloadSnapshot),
    Cancelled(DownloadSnapshot),
}

pub trait DownloadBackend: Send + Sync {
    /// Probes a remote resource for size, resume support, and file name hints.
    fn probe(&self, request: &DownloadRequest) -> Result<ProbeInfo, NetError>;

    /// Downloads a resource until completion, pause, cancel, or error.
    ///
    /// Implementations push live snapshots through `on_progress` and poll
    /// `control` so queue actions can interrupt the transfer.
    fn download(
        &self,
        task: &DownloadTask,
        on_progress: &mut dyn FnMut(DownloadSnapshot) -> Result<(), NetError>,
        control: &dyn Fn() -> ControlSignal,
    ) -> Result<DownloadOutcome, NetError>;
}

#[derive(Debug)]
/// Blocking reqwest backend used by the queue worker threads.
pub struct ReqwestBackend {
    client: Client,
    connections: usize,
}

impl ReqwestBackend {
    /// Creates a backend with the requested per-download connection count.
    ///
    /// `1` preserves the old single-connection behavior. Values above `1`
    /// enable ranged multipart downloads when the server supports them.
    pub fn new(connections: usize) -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .unwrap_or_else(|_| Client::new()),
            connections: connections.max(1),
        }
    }
}

impl Default for ReqwestBackend {
    fn default() -> Self {
        Self::new(1)
    }
}

impl DownloadBackend for ReqwestBackend {
    fn probe(&self, request: &DownloadRequest) -> Result<ProbeInfo, NetError> {
        let response = self.client.head(&request.url).send();

        let head = match response {
            Ok(resp) => resp,
            Err(_) => {
                return Ok(ProbeInfo::default());
            }
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
        task: &DownloadTask,
        on_progress: &mut dyn FnMut(DownloadSnapshot) -> Result<(), NetError>,
        control: &dyn Fn() -> ControlSignal,
    ) -> Result<DownloadOutcome, NetError> {
        if let Some(parent) = task.temp_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let probe = self.probe(&task.request)?;
        if self.connections > 1 {
            match &task.temp_layout {
                TempLayout::Multipart(layout) if layout.total_size > 1 => {
                    return multipart::download(
                        self.client.clone(),
                        self.connections,
                        task,
                        layout.total_size,
                        on_progress,
                        control,
                    );
                }
                TempLayout::Single
                    if task.existing_size == 0
                        && probe.accept_ranges
                        && matches!(probe.total_size, Some(total_size) if total_size > 1) =>
                {
                    if let Some(total_size) = probe.total_size {
                        return multipart::download(
                            self.client.clone(),
                            self.connections,
                            task,
                            total_size,
                            on_progress,
                            control,
                        );
                    }
                }
                _ => {}
            }
        }

        self.download_single_stream(task, probe.total_size, on_progress, control)
    }
}

impl ReqwestBackend {
    /// Runs the legacy single-response path against the main temp file.
    ///
    /// This path is used when multipart ranges are unavailable or when an
    /// existing single temp file should continue in place.
    fn download_single_stream(
        &self,
        task: &DownloadTask,
        probe_total_size: Option<u64>,
        on_progress: &mut dyn FnMut(DownloadSnapshot) -> Result<(), NetError>,
        control: &dyn Fn() -> ControlSignal,
    ) -> Result<DownloadOutcome, NetError> {
        let can_resume = task.existing_size > 0;
        let start_offset = task.existing_size;

        let mut request = self.client.get(&task.request.url);

        if can_resume {
            request = request.header(RANGE, format!("bytes={start_offset}-"));
            if let Some(etag) = &task.etag {
                request = request.header(IF_RANGE, etag);
            }
        }

        let response = request.send()?;

        if can_resume && response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            match fs::remove_file(&task.temp_path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(NetError::Io(error)),
            }
            return self.download(
                &DownloadTask {
                    temp_layout: TempLayout::Single,
                    existing_size: 0,
                    allow_resume: false,
                    ..task.clone()
                },
                on_progress,
                control,
            );
        }

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(can_resume)
            .truncate(!can_resume)
            .open(&task.temp_path)?;

        let response_total = response.content_length();
        let total_size = if can_resume {
            response_total.map(|value| value + start_offset)
        } else {
            probe_total_size.or(response_total)
        };

        let mut reader = response;
        let mut downloaded = start_offset;
        let started_at = Instant::now();
        let mut buffer = [0u8; DOWNLOAD_BUFFER_SIZE];

        on_progress(DownloadSnapshot::from_progress(ProgressSnapshot {
            downloaded,
            total: total_size,
            speed_bps: Some(0),
            eta_seconds: None,
        }))?;

        loop {
            match control() {
                ControlSignal::Pause => {
                    file.flush()?;
                    return Ok(DownloadOutcome::Paused(DownloadSnapshot::from_progress(
                        progress_from_metrics(downloaded, total_size, started_at),
                    )));
                }
                ControlSignal::Cancel => {
                    file.flush()?;
                    return Ok(DownloadOutcome::Cancelled(DownloadSnapshot::from_progress(
                        progress_from_metrics(downloaded, total_size, started_at),
                    )));
                }
                ControlSignal::Run => {}
            }

            let read = reader.read(&mut buffer)?;
            if read == 0 {
                file.flush()?;
                return Ok(DownloadOutcome::Completed(DownloadSnapshot::from_progress(
                    progress_from_metrics(downloaded, total_size, started_at),
                )));
            }

            file.write_all(&buffer[..read])?;
            downloaded += read as u64;
            on_progress(DownloadSnapshot::from_progress(progress_from_metrics(
                downloaded,
                total_size,
                started_at,
            )))?;
        }
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
    started_at: Instant,
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
