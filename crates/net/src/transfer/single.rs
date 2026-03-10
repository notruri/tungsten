use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::time::Instant;

use reqwest::blocking::Client;
use reqwest::header::{IF_RANGE, RANGE};

use crate::error::NetError;
use crate::transfer::{TransferOutcome, TransferTask, TransferUpdate};

use super::{ControlSignal, DOWNLOAD_BUFFER_SIZE, TempLayout, progress_from_metrics};

pub(crate) fn download(
    client: &Client,
    task: &TransferTask,
    probe_total_size: Option<u64>,
    on_update: &mut dyn FnMut(TransferUpdate) -> Result<(), NetError>,
    control: &dyn Fn() -> ControlSignal,
) -> Result<TransferOutcome, NetError> {
    let can_resume = task.existing_size > 0;
    let start_offset = task.existing_size;

    let mut request = client.get(&task.request.url);
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

        let restarted = TransferTask {
            temp_layout: TempLayout::Single,
            existing_size: 0,
            etag: task.etag.clone(),
            ..task.clone()
        };
        return download(client, &restarted, probe_total_size, on_update, control);
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

    on_update(TransferUpdate::from_progress(
        crate::transfer::progress_from_metrics(downloaded, total_size, started_at),
    ))?;

    loop {
        match control() {
            ControlSignal::Pause => {
                file.flush()?;
                return Ok(TransferOutcome::Paused(TransferUpdate::from_progress(
                    progress_from_metrics(downloaded, total_size, started_at),
                )));
            }
            ControlSignal::Cancel => {
                file.flush()?;
                return Ok(TransferOutcome::Cancelled(TransferUpdate::from_progress(
                    progress_from_metrics(downloaded, total_size, started_at),
                )));
            }
            ControlSignal::Run => {}
        }

        let read = reader.read(&mut buffer)?;
        if read == 0 {
            file.flush()?;
            return Ok(TransferOutcome::Completed(TransferUpdate::from_progress(
                progress_from_metrics(downloaded, total_size, started_at),
            )));
        }

        file.write_all(&buffer[..read])?;
        downloaded += read as u64;
        on_update(TransferUpdate::from_progress(progress_from_metrics(
            downloaded, total_size, started_at,
        )))?;
    }
}
