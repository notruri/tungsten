//! Reqwest backend engine used by `tungsten-net`.
//!
//! This module owns the network-facing download pipeline:
//! - probing remote metadata before a transfer starts
//! - choosing between single-part and multipart execution
//! - applying global or per-download speed limits
//! - translating core transfer tasks into runtime state needed by the
//!   transport implementation
//! - producing progress snapshots for the queue layer
//!
//! Submodules keep the responsibilities narrow:
//! - [`single`] handles sequential downloads and single-file resume
//! - [`multipart`] handles range-based multipart downloads
//! - [`temp`] manages multipart temp-file layout and resume metadata
//! - [`limit`] implements bandwidth limiting shared by both paths

mod multipart;
mod single;
mod speed;
pub(crate) mod temp;

#[cfg(test)]
mod tests;

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use reqwest::Client;
use reqwest::header::{ACCEPT_RANGES, CONTENT_DISPOSITION, ETAG, LAST_MODIFIED};
use tracing::debug;
use tungsten_core::{
    ControlSignal, CoreError, DownloadId, DownloadRequest, MultipartPart, MultipartState,
    ProbeInfo, ProgressSnapshot, TempLayout, Transfer, TransferOutcome, TransferTask,
    TransferUpdate,
};
use tungsten_io::SingleSession;

use crate::error::NetError;

pub(crate) const CONTROL_TICK: Duration = Duration::from_millis(50);
pub(crate) use speed::{Limiter, SpeedLimit, set_speed_limit_override, speed_limit_override};

const SPEED_SAMPLE_WINDOW: usize = 512;
const ETA_SMOOTHING_TAU_SECS: f64 = 10.0;

#[derive(Debug, Clone)]
struct RuntimeTask {
    pub request: DownloadRequest,
    pub temp_path: std::path::PathBuf,
    pub temp_layout: TempLayout,
    pub existing_size: u64,
    pub etag: Option<String>,
    pub resume_speed_bps: Option<u64>,
    pub(crate) speed_limit: SpeedLimit,
}

#[derive(Debug, Clone, Copy)]
struct FallbackDownload<'a> {
    task: &'a RuntimeTask,
    probe_total_size: Option<u64>,
    connections: usize,
    total_size: u64,
    fallback_message: &'static str,
}

/// Reqwest transfer implementation backed by Tokio.
#[derive(Debug)]
pub struct Transport {
    client: Client,
    connections: AtomicUsize,
    global_limit_kbps: std::sync::Arc<AtomicU64>,
    speed_limits: Mutex<HashMap<u64, std::sync::Arc<AtomicU64>>>,
}

impl Transport {
    pub fn new(connections: usize) -> Self {
        Self::with_client(
            build_client(Client::builder().connect_timeout(Duration::from_secs(60))),
            connections,
        )
    }

    fn with_client(client: Client, connections: usize) -> Self {
        Self {
            client,
            connections: AtomicUsize::new(connections.max(1)),
            global_limit_kbps: std::sync::Arc::new(AtomicU64::new(0)),
            speed_limits: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for Transport {
    fn default() -> Self {
        Self::new(1)
    }
}

fn build_client(builder: reqwest::ClientBuilder) -> Client {
    builder.build().unwrap_or_else(|_| Client::new())
}

#[async_trait]
impl Transfer for Transport {
    async fn probe(&self, request: &DownloadRequest) -> Result<ProbeInfo, CoreError> {
        self.probe(request).await.map_err(Into::into)
    }

    async fn run(
        &self,
        task: &TransferTask,
        probe: Option<ProbeInfo>,
        on_update: &mut (dyn FnMut(TransferUpdate) -> Result<(), CoreError> + Send),
        control: &(dyn Fn() -> ControlSignal + Send + Sync),
    ) -> Result<TransferOutcome, CoreError> {
        debug!(?task, "running transfer task");

        self.start(task, probe, on_update, control)
            .await
            .map_err(Into::into)
    }

    fn set_connections(&self, connections: usize) {
        self.connections
            .store(connections.max(1), Ordering::Relaxed);
    }

    fn set_download_limit(&self, download_limit_kbps: u64) {
        self.global_limit_kbps
            .store(download_limit_kbps, Ordering::Relaxed);
    }

    fn set_speed_limit(
        &self,
        download_id: DownloadId,
        speed_limit_kbps: Option<u64>,
    ) -> Result<(), CoreError> {
        let slot = self.speed_limit_slot(download_id.0, speed_limit_kbps)?;
        set_speed_limit_override(slot.as_ref(), speed_limit_kbps);
        Ok(())
    }

    fn clear_download(&self, download_id: DownloadId) {
        if let Ok(mut speed_limits) = self.speed_limits.lock() {
            speed_limits.remove(&download_id.0);
        }
    }
}

impl Transport {
    async fn probe(&self, request: &DownloadRequest) -> Result<ProbeInfo, NetError> {
        let response = self.client.head(&request.url).send().await;

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

    async fn start(
        &self,
        task: &TransferTask,
        probe: Option<ProbeInfo>,
        on_update: &mut (dyn FnMut(TransferUpdate) -> Result<(), CoreError> + Send),
        control: &(dyn Fn() -> ControlSignal + Send + Sync),
    ) -> Result<TransferOutcome, NetError> {
        let mut task = runtime_task(task, self).map_err(NetError::from)?;
        let connections = self.connections.load(Ordering::Relaxed).max(1);
        if let Some(parent) = task.temp_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let probe = match probe {
            Some(probe) => probe,
            None => self.probe(&task.request).await?,
        };
        if probe.total_size.is_none() {
            task.existing_size = 0;
            task.temp_layout = TempLayout::Single;
            match tokio::fs::remove_file(&task.temp_path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(NetError::Io(error)),
            }
        }

        let mut single_session = if matches!(task.temp_layout, TempLayout::Single) {
            Some(
                SingleSession::open(task.temp_path.clone())
                    .await
                    .map_err(NetError::from)?,
            )
        } else {
            None
        };
        if let Some(session) = &single_session {
            task.existing_size = session.existing_size();
        }

        if connections > 1 {
            match &task.temp_layout {
                TempLayout::Multipart(layout) if layout.total_size > 1 => {
                    return self
                        .download_with_fallback(
                            FallbackDownload {
                                task: &task,
                                probe_total_size: probe.total_size,
                                connections,
                                total_size: layout.total_size,
                                fallback_message: "multipart range request was not honored; restarting as single download",
                            },
                            on_update,
                            control,
                        )
                        .await;
                }
                TempLayout::Single
                    if task.existing_size == 0
                        && probe.accept_ranges
                        && matches!(probe.total_size, Some(total_size) if total_size > 1) =>
                {
                    if let Some(total_size) = probe.total_size {
                        return self
                            .download_with_fallback(
                                FallbackDownload {
                                    task: &task,
                                    probe_total_size: probe.total_size,
                                    connections,
                                    total_size,
                                    fallback_message: "multipart startup fell back to single download",
                                },
                                on_update,
                                control,
                            )
                            .await;
                    }
                }
                _ => {}
            }
        }

        single::download(
            &self.client,
            &task,
            single_session.take(),
            probe.total_size,
            on_update,
            control,
        )
        .await
    }

    async fn download_with_fallback(
        &self,
        fallback: FallbackDownload<'_>,
        on_update: &mut (dyn FnMut(TransferUpdate) -> Result<(), CoreError> + Send),
        control: &(dyn Fn() -> ControlSignal + Send + Sync),
    ) -> Result<TransferOutcome, NetError> {
        let FallbackDownload {
            task,
            probe_total_size,
            connections,
            total_size,
            fallback_message,
        } = fallback;

        match multipart::download(
            self.client.clone(),
            connections,
            task,
            total_size,
            on_update,
            control,
        )
        .await
        {
            Ok(outcome) => Ok(outcome),
            Err(multipart::MultipartError::RangeNotHonored) => {
                tracing::debug!(
                    url = %task.request.url,
                    path = %task.temp_path.display(),
                    fallback_message
                );
                let restarted = RuntimeTask {
                    temp_layout: TempLayout::Single,
                    existing_size: 0,
                    ..task.clone()
                };
                single::download(
                    &self.client,
                    &restarted,
                    None,
                    probe_total_size,
                    on_update,
                    control,
                )
                .await
            }
            Err(multipart::MultipartError::Other(error)) => Err(error),
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

fn runtime_task(task: &TransferTask, transfer: &Transport) -> Result<RuntimeTask, CoreError> {
    let override_slot =
        transfer.speed_limit_slot(task.download_id.0, task.request.speed_limit_kbps)?;
    let base_limit = SpeedLimit::new(std::sync::Arc::clone(&transfer.global_limit_kbps), None);

    Ok(RuntimeTask {
        request: task.request.clone(),
        temp_path: task.temp_path.clone(),
        temp_layout: task.temp_layout.clone(),
        existing_size: task.existing_size,
        etag: task.etag.clone(),
        resume_speed_bps: task.resume_speed_bps,
        speed_limit: base_limit.for_task(override_slot),
    })
}

impl Transport {
    fn speed_limit_slot(
        &self,
        download_id: u64,
        initial_kbps: Option<u64>,
    ) -> Result<std::sync::Arc<AtomicU64>, CoreError> {
        let mut speed_limits = self
            .speed_limits
            .lock()
            .map_err(|error| CoreError::State(format!("speed limit map poisoned: {error}")))?;

        Ok(speed_limits
            .entry(download_id)
            .or_insert_with(|| speed_limit_override(initial_kbps))
            .clone())
    }
}

pub(crate) fn progress_from_metrics(
    downloaded: u64,
    total: Option<u64>,
    elapsed: Duration,
    speed_tracker: &mut SpeedTracker,
    speed_limit_bps: Option<u64>,
) -> ProgressSnapshot {
    speed_tracker.snapshot(downloaded, total, elapsed, speed_limit_bps)
}

#[cfg(test)]
pub(crate) fn progress_for_speed_limit(
    progress: &ProgressSnapshot,
    speed_limit_bps: Option<u64>,
) -> ProgressSnapshot {
    let speed_bps = effective_speed_bps(progress.speed_bps, speed_limit_bps);
    let eta_seconds = match (progress.total, speed_bps) {
        (Some(total_size), Some(speed)) if speed > 0 && total_size >= progress.downloaded => {
            Some((total_size - progress.downloaded) / speed)
        }
        _ => None,
    };

    ProgressSnapshot {
        downloaded: progress.downloaded,
        total: progress.total,
        speed_bps,
        eta_seconds,
    }
}

fn effective_speed_bps(
    measured_speed_bps: Option<u64>,
    speed_limit_bps: Option<u64>,
) -> Option<u64> {
    match speed_limit_bps {
        Some(limit_bps) if limit_bps > 0 => Some(limit_bps),
        _ => measured_speed_bps,
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SpeedTracker {
    samples: VecDeque<SpeedSample>,
    fallback_speed_bps: Option<u64>,
    eta_rate_bps: Option<f64>,
}

impl SpeedTracker {
    pub(crate) fn new(initial_downloaded: u64, fallback_speed_bps: Option<u64>) -> Self {
        let mut samples = VecDeque::with_capacity(SPEED_SAMPLE_WINDOW);
        samples.push_back(SpeedSample {
            downloaded: initial_downloaded,
            elapsed: Duration::ZERO,
        });
        Self {
            samples,
            fallback_speed_bps,
            eta_rate_bps: fallback_speed_bps.map(|speed| speed as f64),
        }
    }

    fn snapshot(
        &mut self,
        downloaded: u64,
        total: Option<u64>,
        elapsed: Duration,
        speed_limit_bps: Option<u64>,
    ) -> ProgressSnapshot {
        self.record(downloaded, elapsed);

        let measured_speed_bps = self.measured_speed_bps();
        let speed_bps = effective_speed_bps(measured_speed_bps, speed_limit_bps);
        let eta_speed_bps = effective_speed_bps(self.eta_speed_bps(), speed_limit_bps);
        let eta_seconds = match (total, eta_speed_bps) {
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

    fn record(&mut self, downloaded: u64, elapsed: Duration) {
        let Some(last) = self.samples.back() else {
            self.samples.push_back(SpeedSample {
                downloaded,
                elapsed,
            });
            return;
        };

        if downloaded < last.downloaded || elapsed < last.elapsed {
            self.samples.clear();
            self.samples.push_back(SpeedSample {
                downloaded,
                elapsed,
            });
            self.eta_rate_bps = self.fallback_speed_bps.map(|speed| speed as f64);
            return;
        }

        if downloaded == last.downloaded {
            return;
        }

        self.update_eta_rate(*last, downloaded, elapsed);

        if self.samples.len() == SPEED_SAMPLE_WINDOW {
            self.samples.pop_front();
        }
        self.samples.push_back(SpeedSample {
            downloaded,
            elapsed,
        });
    }

    fn measured_speed_bps(&self) -> Option<u64> {
        let Some(first) = self.samples.front() else {
            return self.fallback_speed_bps.or(Some(0));
        };
        let Some(last) = self.samples.back() else {
            return self.fallback_speed_bps.or(Some(0));
        };

        if self.samples.len() < 2 {
            return self.fallback_speed_bps.or(Some(0));
        }

        let elapsed_seconds = last.elapsed.saturating_sub(first.elapsed).as_secs_f64();
        let downloaded = last.downloaded.saturating_sub(first.downloaded);
        if elapsed_seconds > 0.0 {
            Some((downloaded as f64 / elapsed_seconds) as u64)
        } else {
            self.fallback_speed_bps.or(Some(0))
        }
    }

    fn update_eta_rate(&mut self, previous: SpeedSample, downloaded: u64, elapsed: Duration) {
        let delta_downloaded = downloaded.saturating_sub(previous.downloaded);
        let delta_elapsed = elapsed.saturating_sub(previous.elapsed).as_secs_f64();
        if delta_downloaded == 0 || delta_elapsed <= 0.0 {
            return;
        }

        let instant_rate_bps = delta_downloaded as f64 / delta_elapsed;
        let alpha = 1.0 - (-delta_elapsed / ETA_SMOOTHING_TAU_SECS).exp();
        let next_rate_bps = match self.eta_rate_bps {
            Some(current_rate_bps) if current_rate_bps > 0.0 => {
                current_rate_bps + (instant_rate_bps - current_rate_bps) * alpha
            }
            _ => instant_rate_bps,
        };
        self.eta_rate_bps = Some(next_rate_bps);
    }

    fn eta_speed_bps(&self) -> Option<u64> {
        self.eta_rate_bps
            .map(|speed| speed as u64)
            .or(self.fallback_speed_bps)
            .or_else(|| self.measured_speed_bps())
    }
}

#[derive(Debug, Clone, Copy)]
struct SpeedSample {
    downloaded: u64,
    elapsed: Duration,
}
