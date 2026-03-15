//! Single-part transport path.
//!
//! This module drives one HTTP request from start to finish, including:
//! - resume requests via `Range` and `If-Range`
//! - sequential writes through [`tungsten_io::SingleWriter`]
//! - pause and cancel handling
//! - progress and stall reporting for the queue layer

use std::time::Instant;

use reqwest::Client;
use reqwest::header::{IF_RANGE, RANGE};
use tokio::fs;
use tokio::sync::mpsc;
use tracing::{debug, trace};
use tungsten_core::CoreError;
use tungsten_io::{SingleWriter, WriteStream as _, Writer as _};

use crate::error::NetError;
use crate::transport::{RuntimeTask, TransferOutcome, TransferUpdate};

use super::{
    CONTROL_TICK, ControlSignal, Limiter, SpeedTracker, TempLayout, progress_from_metrics,
};

const STALL_LOG_THRESHOLD: std::time::Duration = std::time::Duration::from_millis(500);

pub(crate) async fn download(
    client: &Client,
    task: &RuntimeTask,
    total_size: Option<u64>,
    on_update: &mut (dyn FnMut(TransferUpdate) -> Result<(), CoreError> + Send),
    control: &(dyn Fn() -> ControlSignal + Send + Sync),
) -> Result<TransferOutcome, NetError> {
    debug!(?task, ?total_size, "starting single download");

    let start_offset = match total_size {
        Some(size) => task.existing_size.min(size),
        None => task.existing_size,
    };
    let can_resume = start_offset > 0;
    let mut writer = SingleWriter::new(task.temp_path.clone(), total_size, start_offset);
    writer.create().await?;

    let mut request = client.get(&task.request.url);
    if can_resume {
        request = request.header(RANGE, format!("bytes={start_offset}-"));
        if let Some(etag) = &task.etag {
            request = request.header(IF_RANGE, etag);
        }
    }

    let request_started_at = Instant::now();
    trace!(
        url = %task.request.url,
        path = %task.temp_path.display(),
        start_offset,
        can_resume,
        "sending single download request"
    );
    let response = request.send().await?;
    trace!(
        url = %task.request.url,
        path = %task.temp_path.display(),
        status = %response.status(),
        response_content_length = ?response.content_length(),
        elapsed_ms = request_started_at.elapsed().as_millis() as u64,
        "received single download response"
    );
    if can_resume && response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
        trace!(
            url = %task.request.url,
            path = %task.temp_path.display(),
            status = %response.status(),
            "resume range was not honored; restarting single download from byte 0"
        );
        match fs::remove_file(&task.temp_path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(NetError::Io(error)),
        }

        let restarted = RuntimeTask {
            temp_layout: TempLayout::Single,
            existing_size: 0,
            ..task.clone()
        };
        return Box::pin(download(client, &restarted, total_size, on_update, control)).await;
    }

    let response_total = response.content_length();
    let total_size = if can_resume {
        response_total.map(|value| value + start_offset)
    } else {
        total_size.or(response_total)
    };
    let mut stream = writer.stream(0).await?;

    let mut reader = response;
    let (chunk_tx, mut chunk_rx) = mpsc::channel(1);
    let reader_task = tokio::spawn(async move {
        loop {
            let next = reader.chunk().await.map_err(NetError::Http);
            let is_done = matches!(next, Ok(None));
            if chunk_tx.send(next).await.is_err() {
                return;
            }
            if is_done {
                return;
            }
        }
    });
    let mut downloaded = start_offset;
    let started_at = Instant::now();
    let mut last_chunk_at = request_started_at;
    let mut saw_first_chunk = false;
    let mut speed_tracker = SpeedTracker::new(start_offset, task.resume_speed_bps);
    let limiter = Limiter::new(task.speed_limit.clone());

    on_update(TransferUpdate::from_progress(
        crate::transport::progress_from_metrics(
            downloaded,
            total_size,
            started_at.elapsed(),
            &mut speed_tracker,
            task.speed_limit.override_bps(),
        ),
    ))
    .map_err(NetError::from)?;

    loop {
        match control() {
            ControlSignal::Pause => {
                reader_task.abort();
                stream.flush().await?;
                return Ok(TransferOutcome::Paused(TransferUpdate::from_progress(
                    progress_from_metrics(
                        downloaded,
                        total_size,
                        started_at.elapsed(),
                        &mut speed_tracker,
                        task.speed_limit.override_bps(),
                    ),
                )));
            }
            ControlSignal::Cancel => {
                reader_task.abort();
                stream.flush().await?;
                return Ok(TransferOutcome::Cancelled(TransferUpdate::from_progress(
                    progress_from_metrics(
                        downloaded,
                        total_size,
                        started_at.elapsed(),
                        &mut speed_tracker,
                        task.speed_limit.override_bps(),
                    ),
                )));
            }
            ControlSignal::Run => {}
        }

        let chunk = tokio::select! {
            _ = tokio::time::sleep(CONTROL_TICK) => continue,
            next = chunk_rx.recv() => match next {
                Some(Ok(Some(chunk))) => chunk,
                Some(Ok(None)) => {
                    stream.flush().await?;
                    return Ok(TransferOutcome::Completed(TransferUpdate::from_progress(
                        progress_from_metrics(
                            downloaded,
                            total_size,
                            started_at.elapsed(),
                            &mut speed_tracker,
                            task.speed_limit.override_bps(),
                        ),
                    )));
                }
                Some(Err(error)) => return Err(error),
                None => {
                    return Err(NetError::Backend(
                        "chunk reader task ended unexpectedly".to_string(),
                    ));
                }
            },
        };

        let chunk_received_at = Instant::now();
        let chunk_gap = chunk_received_at.duration_since(last_chunk_at);
        if !saw_first_chunk {
            trace!(
                url = %task.request.url,
                path = %task.temp_path.display(),
                chunk_len = chunk.len(),
                elapsed_ms = request_started_at.elapsed().as_millis() as u64,
                "received first single download chunk"
            );
            saw_first_chunk = true;
        } else if chunk_gap >= STALL_LOG_THRESHOLD {
            trace!(
                url = %task.request.url,
                path = %task.temp_path.display(),
                downloaded,
                chunk_len = chunk.len(),
                gap_ms = chunk_gap.as_millis() as u64,
                "single download stalled between chunks"
            );
        }
        last_chunk_at = chunk_received_at;

        limiter
            .wait_for_async(chunk.len() as u64, || {
                !matches!(control(), ControlSignal::Run)
            })
            .await;

        match control() {
            ControlSignal::Pause => {
                reader_task.abort();
                stream.flush().await?;
                return Ok(TransferOutcome::Paused(TransferUpdate::from_progress(
                    progress_from_metrics(
                        downloaded,
                        total_size,
                        started_at.elapsed(),
                        &mut speed_tracker,
                        task.speed_limit.override_bps(),
                    ),
                )));
            }
            ControlSignal::Cancel => {
                reader_task.abort();
                stream.flush().await?;
                return Ok(TransferOutcome::Cancelled(TransferUpdate::from_progress(
                    progress_from_metrics(
                        downloaded,
                        total_size,
                        started_at.elapsed(),
                        &mut speed_tracker,
                        task.speed_limit.override_bps(),
                    ),
                )));
            }
            ControlSignal::Run => {}
        }

        let write_started_at = Instant::now();
        stream.send(&chunk).await?;
        let write_elapsed = write_started_at.elapsed();
        if write_elapsed >= STALL_LOG_THRESHOLD {
            trace!(
                url = %task.request.url,
                path = %task.temp_path.display(),
                downloaded,
                chunk_len = chunk.len(),
                elapsed_ms = write_elapsed.as_millis() as u64,
                "single download write took unusually long"
            );
        }
        downloaded += chunk.len() as u64;
        on_update(TransferUpdate::from_progress(progress_from_metrics(
            downloaded,
            total_size,
            started_at.elapsed(),
            &mut speed_tracker,
            task.speed_limit.override_bps(),
        )))
        .map_err(NetError::from)?;
    }
}
