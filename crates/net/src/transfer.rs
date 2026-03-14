mod limit;
mod multipart;
mod single;
pub(crate) mod temp;

#[cfg(test)]
mod tests;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use reqwest::blocking::Client;
use reqwest::header::{ACCEPT_RANGES, CONTENT_DISPOSITION, ETAG, LAST_MODIFIED};
use serde::{Deserialize, Serialize};

use crate::error::NetError;
use crate::model::{DownloadRequest, ProgressSnapshot};

pub(crate) const DOWNLOAD_BUFFER_SIZE: usize = 64 * 1024;
pub(crate) use limit::{Limiter, SpeedLimit, set_speed_limit_override, speed_limit_override};

const SPEED_SAMPLE_WINDOW: usize = 512;
const ETA_SMOOTHING_TAU_SECS: f64 = 10.0;

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
    pub resume_speed_bps: Option<u64>,
    pub(crate) speed_limit: SpeedLimit,
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

impl tungsten_core::Transfer for ReqwestTransfer {
    fn probe(
        &self,
        request: &tungsten_core::DownloadRequest,
    ) -> Result<tungsten_core::ProbeInfo, tungsten_core::CoreError> {
        let request = map_core_request(request);
        let probe = <ReqwestTransfer as Transfer>::probe(self, &request)?;
        Ok(map_probe_info(probe))
    }

    fn download(
        &self,
        task: &tungsten_core::TransferTask,
        probe: Option<tungsten_core::ProbeInfo>,
        on_update: &mut dyn FnMut(
            tungsten_core::TransferUpdate,
        ) -> Result<(), tungsten_core::CoreError>,
        control: &dyn Fn() -> tungsten_core::ControlSignal,
    ) -> Result<tungsten_core::TransferOutcome, tungsten_core::CoreError> {
        let task = map_core_task(task);
        let probe = probe.map(map_core_probe_info);
        let mut on_update = |update: TransferUpdate| -> Result<(), NetError> {
            on_update(map_transfer_update(update)).map_err(NetError::from)
        };
        let control = || match control() {
            tungsten_core::ControlSignal::Run => ControlSignal::Run,
            tungsten_core::ControlSignal::Pause => ControlSignal::Pause,
            tungsten_core::ControlSignal::Cancel => ControlSignal::Cancel,
        };

        let outcome =
            <ReqwestTransfer as Transfer>::download(self, &task, probe, &mut on_update, &control)?;
        Ok(map_transfer_outcome(outcome))
    }

    fn set_connections(&self, connections: usize) {
        <ReqwestTransfer as Transfer>::set_connections(self, connections);
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

fn map_core_request(request: &tungsten_core::DownloadRequest) -> DownloadRequest {
    DownloadRequest {
        url: request.url.clone(),
        destination: request.destination.clone(),
        conflict: match request.conflict {
            tungsten_core::ConflictPolicy::AutoRename => crate::model::ConflictPolicy::AutoRename,
        },
        integrity: match &request.integrity {
            tungsten_core::IntegrityRule::None => crate::model::IntegrityRule::None,
            tungsten_core::IntegrityRule::Sha256(value) => {
                crate::model::IntegrityRule::Sha256(value.clone())
            }
        },
        speed_limit_kbps: request.speed_limit_kbps,
    }
}

fn map_progress(progress: ProgressSnapshot) -> tungsten_core::ProgressSnapshot {
    tungsten_core::ProgressSnapshot {
        downloaded: progress.downloaded,
        total: progress.total,
        speed_bps: progress.speed_bps,
        eta_seconds: progress.eta_seconds,
    }
}

fn map_core_temp_layout(layout: &tungsten_core::TempLayout) -> TempLayout {
    match layout {
        tungsten_core::TempLayout::Single => TempLayout::Single,
        tungsten_core::TempLayout::Multipart(layout) => TempLayout::Multipart(MultipartState {
            total_size: layout.total_size,
            parts: layout
                .parts
                .iter()
                .map(|part| MultipartPart {
                    index: part.index,
                    start: part.start,
                    end: part.end,
                    path: part.path.clone(),
                })
                .collect(),
        }),
    }
}

fn map_temp_layout(layout: TempLayout) -> tungsten_core::TempLayout {
    match layout {
        TempLayout::Single => tungsten_core::TempLayout::Single,
        TempLayout::Multipart(layout) => {
            tungsten_core::TempLayout::Multipart(tungsten_core::MultipartState {
                total_size: layout.total_size,
                parts: layout
                    .parts
                    .into_iter()
                    .map(|part| tungsten_core::MultipartPart {
                        index: part.index,
                        start: part.start,
                        end: part.end,
                        path: part.path,
                    })
                    .collect(),
            })
        }
    }
}

fn map_core_probe_info(probe: tungsten_core::ProbeInfo) -> ProbeInfo {
    ProbeInfo {
        total_size: probe.total_size,
        accept_ranges: probe.accept_ranges,
        etag: probe.etag,
        last_modified: probe.last_modified,
        file_name: probe.file_name,
    }
}

fn map_probe_info(probe: ProbeInfo) -> tungsten_core::ProbeInfo {
    tungsten_core::ProbeInfo {
        total_size: probe.total_size,
        accept_ranges: probe.accept_ranges,
        etag: probe.etag,
        last_modified: probe.last_modified,
        file_name: probe.file_name,
    }
}

fn map_core_task(task: &tungsten_core::TransferTask) -> TransferTask {
    let base_limit = SpeedLimit::shared_global(0);

    TransferTask {
        request: map_core_request(&task.request),
        temp_path: task.temp_path.clone(),
        temp_layout: map_core_temp_layout(&task.temp_layout),
        existing_size: task.existing_size,
        etag: task.etag.clone(),
        resume_speed_bps: task.resume_speed_bps,
        speed_limit: base_limit.for_task(speed_limit_override(task.request.speed_limit_kbps)),
    }
}

fn map_transfer_update(update: TransferUpdate) -> tungsten_core::TransferUpdate {
    tungsten_core::TransferUpdate {
        progress: map_progress(update.progress),
        temp_layout: map_temp_layout(update.temp_layout),
    }
}

fn map_transfer_outcome(outcome: TransferOutcome) -> tungsten_core::TransferOutcome {
    match outcome {
        TransferOutcome::Completed(update) => {
            tungsten_core::TransferOutcome::Completed(map_transfer_update(update))
        }
        TransferOutcome::Paused(update) => {
            tungsten_core::TransferOutcome::Paused(map_transfer_update(update))
        }
        TransferOutcome::Cancelled(update) => {
            tungsten_core::TransferOutcome::Cancelled(map_transfer_update(update))
        }
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
