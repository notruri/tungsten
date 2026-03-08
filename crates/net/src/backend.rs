use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use reqwest::blocking::Client;
use reqwest::header::{ACCEPT_RANGES, ETAG, IF_RANGE, LAST_MODIFIED, RANGE};

use crate::error::NetError;
use crate::types::{DownloadRequest, ProgressSnapshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlSignal {
    Run,
    Pause,
    Cancel,
}

#[derive(Debug, Clone, Default)]
pub struct ProbeInfo {
    pub total_size: Option<u64>,
    pub accept_ranges: bool,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub request: DownloadRequest,
    pub temp_path: PathBuf,
    pub existing_size: u64,
    pub allow_resume: bool,
    pub etag: Option<String>,
}

#[derive(Debug, Clone)]
pub enum DownloadOutcome {
    Completed(ProgressSnapshot),
    Paused(ProgressSnapshot),
    Cancelled(ProgressSnapshot),
}

pub trait DownloadBackend: Send + Sync {
    fn probe(&self, request: &DownloadRequest) -> Result<ProbeInfo, NetError>;

    fn download(
        &self,
        task: &DownloadTask,
        on_progress: &mut dyn FnMut(ProgressSnapshot) -> Result<(), NetError>,
        control: &dyn Fn() -> ControlSignal,
    ) -> Result<DownloadOutcome, NetError>;
}

#[derive(Debug)]
pub struct ReqwestBackend {
    client: Client,
}

impl Default for ReqwestBackend {
    fn default() -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .unwrap_or_else(|_| Client::new()),
        }
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

        Ok(ProbeInfo {
            total_size,
            accept_ranges,
            etag,
            last_modified,
        })
    }

    fn download(
        &self,
        task: &DownloadTask,
        on_progress: &mut dyn FnMut(ProgressSnapshot) -> Result<(), NetError>,
        control: &dyn Fn() -> ControlSignal,
    ) -> Result<DownloadOutcome, NetError> {
        if let Some(parent) = task.temp_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let probe = self.probe(&task.request)?;
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
            probe.total_size.or(response_total)
        };

        let mut reader = response;
        let mut downloaded = start_offset;
        let started_at = Instant::now();
        let mut buffer = [0u8; 64 * 1024];

        on_progress(ProgressSnapshot {
            downloaded,
            total: total_size,
            speed_bps: Some(0),
            eta_seconds: None,
        })?;

        loop {
            match control() {
                ControlSignal::Pause => {
                    file.flush()?;
                    return Ok(DownloadOutcome::Paused(progress_from_metrics(
                        downloaded, total_size, started_at,
                    )));
                }
                ControlSignal::Cancel => {
                    file.flush()?;
                    return Ok(DownloadOutcome::Cancelled(progress_from_metrics(
                        downloaded, total_size, started_at,
                    )));
                }
                ControlSignal::Run => {}
            }

            let read = reader.read(&mut buffer)?;
            if read == 0 {
                file.flush()?;
                return Ok(DownloadOutcome::Completed(progress_from_metrics(
                    downloaded, total_size, started_at,
                )));
            }

            file.write_all(&buffer[..read])?;
            downloaded += read as u64;
            on_progress(progress_from_metrics(downloaded, total_size, started_at))?;
        }
    }
}

fn progress_from_metrics(
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
