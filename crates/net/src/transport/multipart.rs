use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use reqwest::Client;
use reqwest::header::{IF_RANGE, RANGE};
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tracing::debug;

use crate::error::NetError;

use super::temp::{cleanup_parts, load_part_progress, merge_parts, part_len, prepare_layout};
use super::{
    CONTROL_TICK, ControlSignal, Limiter, MultipartPart, SpeedTracker, TempLayout, TransferOutcome,
    TransferTask, TransferUpdate, progress_from_metrics,
};

const PART_RUN: u8 = 0;
const PART_STOP: u8 = 1;
const PART_TICK: Duration = Duration::from_millis(50);

#[derive(Debug)]
pub(crate) enum MultipartError {
    RangeNotHonored,
    Other(NetError),
}

impl From<NetError> for MultipartError {
    fn from(error: NetError) -> Self {
        Self::Other(error)
    }
}

enum PartEvent {
    Progress { index: usize, downloaded: u64 },
    Finished { index: usize },
    Error(PartError),
}

enum PartError {
    RangeNotHonored,
    Other(NetError),
}

impl From<NetError> for PartError {
    fn from(error: NetError) -> Self {
        Self::Other(error)
    }
}

impl From<std::io::Error> for PartError {
    fn from(error: std::io::Error) -> Self {
        Self::Other(NetError::Io(error))
    }
}

impl From<reqwest::Error> for PartError {
    fn from(error: reqwest::Error) -> Self {
        Self::Other(NetError::Http(error))
    }
}

struct PartRunner {
    client: Client,
    url: String,
    etag: Option<String>,
    stop: Arc<AtomicU8>,
    tx: mpsc::UnboundedSender<PartEvent>,
    limiter: Limiter,
}

pub(crate) async fn download(
    client: Client,
    connections: usize,
    task: &TransferTask,
    total_size: u64,
    on_update: &mut (dyn FnMut(TransferUpdate) -> Result<(), NetError> + Send),
    control: &(dyn Fn() -> ControlSignal + Send + Sync),
) -> Result<TransferOutcome, MultipartError> {
    debug!(?connections, ?total_size, "starting multipart download");

    let layout = prepare_layout(&task.temp_path, &task.temp_layout, total_size, connections)?;
    let mut part_downloaded = load_part_progress(&layout)?;
    let mut total_downloaded = part_downloaded.iter().sum::<u64>();
    let started_at = Instant::now();
    let mut speed_tracker = SpeedTracker::new(total_downloaded, task.resume_speed_bps);

    on_update(progress_update(
        total_downloaded,
        total_size,
        started_at.elapsed(),
        &mut speed_tracker,
        task.speed_limit.override_bps(),
        TempLayout::Multipart(layout.clone()),
    ))
    .map_err(MultipartError::Other)?;

    if total_downloaded >= total_size {
        merge_parts(&task.temp_path, &layout)?;
        return Ok(TransferOutcome::Completed(progress_update(
            total_size,
            total_size,
            started_at.elapsed(),
            &mut speed_tracker,
            task.speed_limit.override_bps(),
            TempLayout::Single,
        )));
    }

    let (tx, mut rx) = mpsc::unbounded_channel();
    let stop = Arc::new(AtomicU8::new(PART_RUN));
    let limiter = Limiter::new(task.speed_limit.clone());
    let mut handles = Vec::new();

    for part in &layout.parts {
        let expected = part_len(part);
        let existing = part_downloaded
            .get(part.index)
            .copied()
            .unwrap_or(0)
            .min(expected);
        if existing >= expected {
            continue;
        }

        let tx = tx.clone();
        let stop = Arc::clone(&stop);
        let client = client.clone();
        let request = task.request.clone();
        let etag = task.etag.clone();
        let part = part.clone();
        let limiter = limiter.clone();
        let runner = PartRunner {
            client,
            url: request.url,
            etag,
            stop,
            tx,
            limiter,
        };

        handles.push(tokio::spawn(async move {
            run_part(runner, part, existing).await;
        }));
    }
    drop(tx);

    let active_parts = handles.len();
    let mut finished_parts = 0usize;

    loop {
        match control() {
            ControlSignal::Pause => {
                stop.store(PART_STOP, Ordering::SeqCst);
                join_parts(handles).await.map_err(MultipartError::Other)?;
                return Ok(TransferOutcome::Paused(progress_update(
                    total_downloaded,
                    total_size,
                    started_at.elapsed(),
                    &mut speed_tracker,
                    task.speed_limit.override_bps(),
                    TempLayout::Multipart(layout),
                )));
            }
            ControlSignal::Cancel => {
                stop.store(PART_STOP, Ordering::SeqCst);
                join_parts(handles).await.map_err(MultipartError::Other)?;
                return Ok(TransferOutcome::Cancelled(progress_update(
                    total_downloaded,
                    total_size,
                    started_at.elapsed(),
                    &mut speed_tracker,
                    task.speed_limit.override_bps(),
                    TempLayout::Multipart(layout),
                )));
            }
            ControlSignal::Run => {}
        }

        match tokio::time::timeout(PART_TICK, rx.recv()).await {
            Ok(Some(PartEvent::Progress { index, downloaded })) => {
                if let Some(slot) = part_downloaded.get_mut(index) {
                    let previous = *slot;
                    *slot = downloaded.min(part_len(&layout.parts[index]));
                    total_downloaded = total_downloaded
                        .saturating_sub(previous)
                        .saturating_add(*slot);
                }
                on_update(progress_update(
                    total_downloaded,
                    total_size,
                    started_at.elapsed(),
                    &mut speed_tracker,
                    task.speed_limit.override_bps(),
                    TempLayout::Multipart(layout.clone()),
                ))
                .map_err(MultipartError::Other)?;
            }
            Ok(Some(PartEvent::Finished { index })) => {
                finished_parts = finished_parts.saturating_add(1);
                if let Some(slot) = part_downloaded.get(index) {
                    total_downloaded = total_downloaded.max(part_downloaded.iter().sum::<u64>());
                    if *slot < part_len(&layout.parts[index]) {
                        return Err(MultipartError::Other(NetError::Backend(format!(
                            "multipart part {} completed with incomplete size",
                            index
                        ))));
                    }
                }
                if finished_parts >= active_parts {
                    break;
                }
            }
            Ok(Some(PartEvent::Error(error))) => {
                stop.store(PART_STOP, Ordering::SeqCst);
                join_parts(handles).await.map_err(MultipartError::Other)?;
                match error {
                    PartError::RangeNotHonored => {
                        cleanup_parts(&layout).map_err(MultipartError::Other)?;
                        return Err(MultipartError::RangeNotHonored);
                    }
                    PartError::Other(error) => return Err(MultipartError::Other(error)),
                }
            }
            Ok(None) => {
                if finished_parts >= active_parts {
                    break;
                }
                stop.store(PART_STOP, Ordering::SeqCst);
                join_parts(handles).await.map_err(MultipartError::Other)?;
                return Err(MultipartError::Other(NetError::Backend(
                    "multipart worker channel disconnected unexpectedly".to_string(),
                )));
            }
            Err(_) => {}
        }
    }

    join_parts(handles).await.map_err(MultipartError::Other)?;
    merge_parts(&task.temp_path, &layout)?;
    Ok(TransferOutcome::Completed(progress_update(
        total_size,
        total_size,
        started_at.elapsed(),
        &mut speed_tracker,
        task.speed_limit.override_bps(),
        TempLayout::Single,
    )))
}

async fn run_part(runner: PartRunner, part: MultipartPart, existing: u64) {
    if let Err(error) = run_part_inner(&runner, &part, existing).await
        && let Err(send_error) = runner.tx.send(PartEvent::Error(error))
    {
        debug!(error = %send_error, "part worker failed to send error event");
    }
}

async fn run_part_inner(
    runner: &PartRunner,
    part: &MultipartPart,
    existing: u64,
) -> Result<(), PartError> {
    if runner.stop.load(Ordering::SeqCst) != PART_RUN {
        return Ok(());
    }

    if let Some(parent) = part.path.parent() {
        fs::create_dir_all(parent).await?;
    }

    let expected = part_len(part);
    if existing >= expected {
        runner
            .tx
            .send(PartEvent::Finished { index: part.index })
            .map_err(|error| {
                PartError::Other(NetError::Backend(format!(
                    "part event send failed: {error}"
                )))
            })?;
        return Ok(());
    }

    let start = part.start + existing;
    let mut request = runner
        .client
        .get(&runner.url)
        .header(RANGE, format!("bytes={start}-{}", part.end));
    if let Some(etag) = &runner.etag {
        request = request.header(IF_RANGE, etag);
    }

    let mut response = request.send().await?;
    if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
        return Err(PartError::RangeNotHonored);
    }

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(existing > 0)
        .truncate(existing == 0)
        .open(&part.path)
        .await?;
    let mut downloaded = existing;

    loop {
        if runner.stop.load(Ordering::SeqCst) != PART_RUN {
            file.flush().await?;
            return Ok(());
        }

        let chunk = tokio::select! {
            _ = tokio::time::sleep(CONTROL_TICK) => continue,
            result = response.chunk() => result.map_err(PartError::from)?,
        };

        let Some(chunk) = chunk else {
            break;
        };

        runner
            .limiter
            .wait_for_async(chunk.len() as u64, || {
                runner.stop.load(Ordering::SeqCst) != PART_RUN
            })
            .await;
        if runner.stop.load(Ordering::SeqCst) != PART_RUN {
            file.flush().await?;
            return Ok(());
        }

        file.write_all(&chunk).await?;
        downloaded += chunk.len() as u64;
        runner
            .tx
            .send(PartEvent::Progress {
                index: part.index,
                downloaded,
            })
            .map_err(|error| {
                PartError::Other(NetError::Backend(format!(
                    "part event send failed: {error}"
                )))
            })?;
    }

    file.flush().await?;
    if downloaded != expected {
        return Err(PartError::Other(NetError::Backend(format!(
            "multipart part {} ended early",
            part.index
        ))));
    }

    runner
        .tx
        .send(PartEvent::Finished { index: part.index })
        .map_err(|error| {
            PartError::Other(NetError::Backend(format!(
                "part event send failed: {error}"
            )))
        })?;
    Ok(())
}

async fn join_parts(handles: Vec<tokio::task::JoinHandle<()>>) -> Result<(), NetError> {
    for handle in handles {
        handle
            .await
            .map_err(|error| NetError::Backend(format!("multipart task join failed: {error}")))?;
    }
    Ok(())
}

fn progress_update(
    downloaded: u64,
    total: u64,
    elapsed: Duration,
    speed_tracker: &mut SpeedTracker,
    speed_limit_bps: Option<u64>,
    temp_layout: TempLayout,
) -> TransferUpdate {
    TransferUpdate {
        progress: progress_from_metrics(
            downloaded,
            Some(total),
            elapsed,
            speed_tracker,
            speed_limit_bps,
        ),
        temp_layout,
    }
}
