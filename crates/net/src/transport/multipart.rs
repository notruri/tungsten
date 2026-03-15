//! Multipart transport path.
//!
//! This module coordinates range-based downloads across multiple workers.
//! Each worker fetches one byte range and writes sequentially into its own
//! stream-backed part file. Once every part completes, the writer finalizes
//! the payload by merging those part files back into the temp payload path.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use reqwest::Client;
use reqwest::header::{IF_RANGE, RANGE};
use tokio::sync::mpsc;
use tracing::{debug, trace};
use tungsten_core::CoreError;
use tungsten_io::{
    MultiConfig, MultiPart as WriterPart, MultiWriter, WriteStream as _, Writer as _,
};

use crate::error::NetError;

use super::temp::{load_part_progress, part_len, prepare_layout};
use super::{
    CONTROL_TICK, ControlSignal, Limiter, MultipartPart, RuntimeTask, SpeedTracker, TempLayout,
    TransferOutcome, TransferUpdate, progress_from_metrics,
};

const PART_RUN: u8 = 0;
const PART_STOP: u8 = 1;
const PART_TICK: Duration = Duration::from_millis(50);
const STALL_LOG_THRESHOLD: Duration = Duration::from_millis(500);

enum DownloadExit {
    Paused,
    Cancelled,
    Completed,
    Error(PartError),
    ChannelClosedUnexpectedly,
}

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
    stream: tungsten_io::MultiStream,
}

pub(crate) async fn download(
    client: Client,
    connections: usize,
    task: &RuntimeTask,
    total_size: u64,
    on_update: &mut (dyn FnMut(TransferUpdate) -> Result<(), CoreError> + Send),
    control: &(dyn Fn() -> ControlSignal + Send + Sync),
) -> Result<TransferOutcome, MultipartError> {
    debug!(
        ?task,
        ?connections,
        ?total_size,
        "starting multipart download"
    );

    let mut layout = prepare_layout(&task.temp_path, &task.temp_layout, total_size, connections)?;
    let mut part_downloaded = load_part_progress(&layout)?;
    for (index, downloaded) in part_downloaded.iter_mut().enumerate() {
        let expected = part_len(&layout.parts[index]);
        *downloaded = (*downloaded).min(expected);
        layout.parts[index].cursor = layout.parts[index].start + *downloaded;
    }

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
    .map_err(NetError::from)
    .map_err(MultipartError::Other)?;

    let mut writer = MultiWriter::new(MultiConfig {
        payload_path: task.temp_path.clone(),
        parts: layout
            .parts
            .iter()
            .map(|part| WriterPart {
                index: part.index,
                path: part.path.clone(),
                len: part_len(part),
                downloaded: part_downloaded
                    .get(part.index)
                    .copied()
                    .unwrap_or(0)
                    .min(part_len(part)),
            })
            .collect(),
    });
    writer.create().await.map_err(NetError::from)?;

    if total_downloaded >= total_size {
        let finish_started_at = Instant::now();
        trace!(
            url = %task.request.url,
            total_size,
            payload_path = %task.temp_path.display(),
            "starting multipart finalization merge"
        );
        writer.finish().await.map_err(NetError::from)?;
        trace!(
            url = %task.request.url,
            total_size,
            payload_path = %task.temp_path.display(),
            elapsed_ms = finish_started_at.elapsed().as_millis() as u64,
            "finished multipart finalization merge"
        );
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
        let part_stream = writer.stream(part.index).await.map_err(NetError::from)?;
        let runner = PartRunner {
            client: client.clone(),
            url: task.request.url.clone(),
            etag: task.etag.clone(),
            stop,
            tx,
            limiter: limiter.clone(),
            stream: part_stream,
        };
        let part = part.clone();

        handles.push(tokio::spawn(async move {
            run_part(runner, part, existing).await;
        }));
    }
    drop(tx);

    let active_parts = handles.len();
    let mut finished_parts = 0usize;

    let exit = loop {
        match control() {
            ControlSignal::Pause => {
                stop.store(PART_STOP, Ordering::SeqCst);
                break DownloadExit::Paused;
            }
            ControlSignal::Cancel => {
                stop.store(PART_STOP, Ordering::SeqCst);
                break DownloadExit::Cancelled;
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
                    layout.parts[index].cursor = layout.parts[index].start + *slot;
                }
                on_update(progress_update(
                    total_downloaded,
                    total_size,
                    started_at.elapsed(),
                    &mut speed_tracker,
                    task.speed_limit.override_bps(),
                    TempLayout::Multipart(layout.clone()),
                ))
                .map_err(NetError::from)
                .map_err(MultipartError::Other)?;
            }
            Ok(Some(PartEvent::Finished { index })) => {
                finished_parts = finished_parts.saturating_add(1);
                if let Some(slot) = part_downloaded.get(index) {
                    total_downloaded = total_downloaded.max(part_downloaded.iter().sum::<u64>());
                    layout.parts[index].cursor = layout.parts[index].start + *slot;
                    if *slot < part_len(&layout.parts[index]) {
                        break DownloadExit::Error(PartError::Other(NetError::Backend(format!(
                            "multipart part {} completed with incomplete size",
                            index
                        ))));
                    }
                }
                if finished_parts >= active_parts {
                    break DownloadExit::Completed;
                }
            }
            Ok(Some(PartEvent::Error(error))) => {
                stop.store(PART_STOP, Ordering::SeqCst);
                break DownloadExit::Error(error);
            }
            Ok(None) => {
                if finished_parts >= active_parts {
                    break DownloadExit::Completed;
                }
                stop.store(PART_STOP, Ordering::SeqCst);
                break DownloadExit::ChannelClosedUnexpectedly;
            }
            Err(_) => {}
        }
    };

    join_parts(handles).await.map_err(MultipartError::Other)?;

    match exit {
        DownloadExit::Paused => Ok(TransferOutcome::Paused(progress_update(
            total_downloaded,
            total_size,
            started_at.elapsed(),
            &mut speed_tracker,
            task.speed_limit.override_bps(),
            TempLayout::Multipart(layout),
        ))),
        DownloadExit::Cancelled => Ok(TransferOutcome::Cancelled(progress_update(
            total_downloaded,
            total_size,
            started_at.elapsed(),
            &mut speed_tracker,
            task.speed_limit.override_bps(),
            TempLayout::Multipart(layout),
        ))),
        DownloadExit::Completed => {
            let finish_started_at = Instant::now();
            trace!(
                url = %task.request.url,
                total_size,
                payload_path = %task.temp_path.display(),
                "starting multipart finalization merge"
            );
            writer.finish().await.map_err(NetError::from)?;
            trace!(
                url = %task.request.url,
                total_size,
                payload_path = %task.temp_path.display(),
                elapsed_ms = finish_started_at.elapsed().as_millis() as u64,
                "finished multipart finalization merge"
            );
            Ok(TransferOutcome::Completed(progress_update(
                total_size,
                total_size,
                started_at.elapsed(),
                &mut speed_tracker,
                task.speed_limit.override_bps(),
                TempLayout::Single,
            )))
        }
        DownloadExit::Error(error) => match error {
            PartError::RangeNotHonored => {
                writer.cleanup().await.map_err(NetError::from)?;
                Err(MultipartError::RangeNotHonored)
            }
            PartError::Other(error) => Err(MultipartError::Other(error)),
        },
        DownloadExit::ChannelClosedUnexpectedly => Err(MultipartError::Other(NetError::Backend(
            "multipart worker channel disconnected unexpectedly".to_string(),
        ))),
    }
}

async fn run_part(runner: PartRunner, part: MultipartPart, existing: u64) {
    let tx = runner.tx.clone();
    if let Err(error) = run_part_inner(runner, &part, existing).await
        && let Err(send_error) = tx.send(PartEvent::Error(error))
    {
        trace!(error = %send_error, "part worker failed to send error event");
    }
}

async fn run_part_inner(
    mut runner: PartRunner,
    part: &MultipartPart,
    existing: u64,
) -> Result<(), PartError> {
    if runner.stop.load(Ordering::SeqCst) != PART_RUN {
        return Ok(());
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

    let request_started_at = Instant::now();
    trace!(
        url = %runner.url,
        part_index = part.index,
        range_start = start,
        range_end = part.end,
        "sending multipart range request"
    );
    let mut response = request.send().await?;
    trace!(
        url = %runner.url,
        part_index = part.index,
        status = %response.status(),
        elapsed_ms = request_started_at.elapsed().as_millis() as u64,
        "received multipart range response"
    );
    if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
        trace!(
            url = %runner.url,
            part_index = part.index,
            status = %response.status(),
            "multipart range request was not honored"
        );
        return Err(PartError::RangeNotHonored);
    }

    let mut downloaded = existing;
    let mut last_chunk_at = request_started_at;
    let mut saw_first_chunk = false;
    let (chunk_tx, mut chunk_rx) = mpsc::channel(1);
    let reader_task = tokio::spawn(async move {
        loop {
            let next = response.chunk().await.map_err(PartError::from);
            let is_done = matches!(next, Ok(None));
            if chunk_tx.send(next).await.is_err() {
                return;
            }
            if is_done {
                return;
            }
        }
    });

    loop {
        if runner.stop.load(Ordering::SeqCst) != PART_RUN {
            reader_task.abort();
            runner.stream.flush().await.map_err(NetError::from)?;
            return Ok(());
        }

        let chunk = tokio::select! {
            _ = tokio::time::sleep(CONTROL_TICK) => continue,
            next = chunk_rx.recv() => match next {
                Some(Ok(Some(chunk))) => chunk,
                Some(Ok(None)) => break,
                Some(Err(error)) => return Err(error),
                None => return Err(PartError::Other(NetError::Backend(
                    "multipart chunk reader task ended unexpectedly".to_string(),
                ))),
            },
        };

        let chunk_received_at = Instant::now();
        let chunk_gap = chunk_received_at.duration_since(last_chunk_at);
        if !saw_first_chunk {
            trace!(
                url = %runner.url,
                part_index = part.index,
                chunk_len = chunk.len(),
                elapsed_ms = request_started_at.elapsed().as_millis() as u64,
                "received first multipart chunk"
            );
            saw_first_chunk = true;
        } else if chunk_gap >= STALL_LOG_THRESHOLD {
            trace!(
                url = %runner.url,
                part_index = part.index,
                downloaded,
                chunk_len = chunk.len(),
                gap_ms = chunk_gap.as_millis() as u64,
                "multipart part stalled between chunks"
            );
        }
        last_chunk_at = chunk_received_at;

        runner
            .limiter
            .wait_for_async(chunk.len() as u64, || {
                runner.stop.load(Ordering::SeqCst) != PART_RUN
            })
            .await;
        if runner.stop.load(Ordering::SeqCst) != PART_RUN {
            reader_task.abort();
            runner.stream.flush().await.map_err(NetError::from)?;
            return Ok(());
        }

        let write_started_at = Instant::now();
        runner.stream.send(&chunk).await.map_err(NetError::from)?;
        let write_elapsed = write_started_at.elapsed();
        if write_elapsed >= STALL_LOG_THRESHOLD {
            trace!(
                url = %runner.url,
                part_index = part.index,
                chunk_len = chunk.len(),
                elapsed_ms = write_elapsed.as_millis() as u64,
                "multipart part write took unusually long"
            );
        }

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

    runner.stream.flush().await.map_err(NetError::from)?;

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
