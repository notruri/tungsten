use std::time::Instant;

use reqwest::Client;
use reqwest::header::{IF_RANGE, RANGE};
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tracing::debug;

use crate::error::NetError;
use crate::transport::{TransferOutcome, TransferTask, TransferUpdate};

use super::{
    CONTROL_TICK, ControlSignal, Limiter, SpeedTracker, TempLayout, progress_from_metrics,
};

pub(crate) async fn download(
    client: &Client,
    task: &TransferTask,
    total_size: Option<u64>,
    on_update: &mut (dyn FnMut(TransferUpdate) -> Result<(), NetError> + Send),
    control: &(dyn Fn() -> ControlSignal + Send + Sync),
) -> Result<TransferOutcome, NetError> {
    debug!(?total_size, "starting single download");

    let can_resume = task.existing_size > 0;
    let start_offset = task.existing_size;

    let mut request = client.get(&task.request.url);
    if can_resume {
        request = request.header(RANGE, format!("bytes={start_offset}-"));
        if let Some(etag) = &task.etag {
            request = request.header(IF_RANGE, etag);
        }
    }

    let response = request.send().await?;
    if can_resume && response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
        match fs::remove_file(&task.temp_path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(NetError::Io(error)),
        }

        let restarted = TransferTask {
            temp_layout: TempLayout::Single,
            existing_size: 0,
            etag: task.etag.clone(),
            ..task.clone()
        };
        return Box::pin(download(client, &restarted, total_size, on_update, control)).await;
    }

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(can_resume)
        .truncate(!can_resume)
        .open(&task.temp_path)
        .await?;

    let response_total = response.content_length();
    let total_size = if can_resume {
        response_total.map(|value| value + start_offset)
    } else {
        total_size.or(response_total)
    };

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
    ))?;

    loop {
        match control() {
            ControlSignal::Pause => {
                reader_task.abort();
                file.flush().await?;
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
                file.flush().await?;
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
                    file.flush().await?;
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

        limiter
            .wait_for_async(chunk.len() as u64, || {
                !matches!(control(), ControlSignal::Run)
            })
            .await;

        match control() {
            ControlSignal::Pause => {
                reader_task.abort();
                file.flush().await?;
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
                file.flush().await?;
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

        file.write_all(&chunk).await?;
        downloaded += chunk.len() as u64;
        on_update(TransferUpdate::from_progress(progress_from_metrics(
            downloaded,
            total_size,
            started_at.elapsed(),
            &mut speed_tracker,
            task.speed_limit.override_bps(),
        )))?;
    }
}
