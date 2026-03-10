use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use reqwest::blocking::Client;
use reqwest::header::{IF_RANGE, RANGE};
use tracing::debug;

use crate::error::NetError;

use super::temp::{cleanup_parts, load_part_progress, merge_parts, part_len, prepare_layout};
use super::{
    ControlSignal, DOWNLOAD_BUFFER_SIZE, MultipartPart, TempLayout, TransferOutcome, TransferTask,
    TransferUpdate, progress_from_metrics,
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

pub(crate) fn download(
    client: Client,
    connections: usize,
    task: &TransferTask,
    total_size: u64,
    on_update: &mut dyn FnMut(TransferUpdate) -> Result<(), NetError>,
    control: &dyn Fn() -> ControlSignal,
) -> Result<TransferOutcome, MultipartError> {
    let layout = prepare_layout(&task.temp_path, &task.temp_layout, total_size, connections)?;
    let mut part_downloaded = load_part_progress(&layout)?;
    let mut total_downloaded = part_downloaded.iter().sum::<u64>();
    let started_at = Instant::now();

    on_update(progress_update(
        total_downloaded,
        total_size,
        started_at,
        TempLayout::Multipart(layout.clone()),
    ))
    .map_err(MultipartError::Other)?;

    if total_downloaded >= total_size {
        merge_parts(&task.temp_path, &layout)?;
        return Ok(TransferOutcome::Completed(progress_update(
            total_size,
            total_size,
            started_at,
            TempLayout::Single,
        )));
    }

    let (tx, rx) = mpsc::channel();
    let stop = Arc::new(AtomicU8::new(PART_RUN));
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

        handles.push(thread::spawn(move || {
            run_part(client, request.url, etag, part, existing, stop, tx)
        }));
    }
    drop(tx);

    let active_parts = handles.len();
    let mut finished_parts = 0usize;

    loop {
        match control() {
            ControlSignal::Pause => {
                stop.store(PART_STOP, Ordering::SeqCst);
                join_parts(handles).map_err(MultipartError::Other)?;
                return Ok(TransferOutcome::Paused(progress_update(
                    total_downloaded,
                    total_size,
                    started_at,
                    TempLayout::Multipart(layout),
                )));
            }
            ControlSignal::Cancel => {
                stop.store(PART_STOP, Ordering::SeqCst);
                join_parts(handles).map_err(MultipartError::Other)?;
                return Ok(TransferOutcome::Cancelled(progress_update(
                    total_downloaded,
                    total_size,
                    started_at,
                    TempLayout::Multipart(layout),
                )));
            }
            ControlSignal::Run => {}
        }

        match rx.recv_timeout(PART_TICK) {
            Ok(PartEvent::Progress { index, downloaded }) => {
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
                    started_at,
                    TempLayout::Multipart(layout.clone()),
                ))
                .map_err(MultipartError::Other)?;
            }
            Ok(PartEvent::Finished { index }) => {
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
            Ok(PartEvent::Error(error)) => {
                stop.store(PART_STOP, Ordering::SeqCst);
                join_parts(handles).map_err(MultipartError::Other)?;
                match error {
                    PartError::RangeNotHonored => {
                        cleanup_parts(&layout).map_err(MultipartError::Other)?;
                        return Err(MultipartError::RangeNotHonored);
                    }
                    PartError::Other(error) => return Err(MultipartError::Other(error)),
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                if finished_parts >= active_parts {
                    break;
                }
                stop.store(PART_STOP, Ordering::SeqCst);
                join_parts(handles).map_err(MultipartError::Other)?;
                return Err(MultipartError::Other(NetError::Backend(
                    "multipart worker channel disconnected unexpectedly".to_string(),
                )));
            }
        }
    }

    join_parts(handles).map_err(MultipartError::Other)?;
    merge_parts(&task.temp_path, &layout)?;
    Ok(TransferOutcome::Completed(progress_update(
        total_size,
        total_size,
        started_at,
        TempLayout::Single,
    )))
}

fn run_part(
    client: Client,
    url: String,
    etag: Option<String>,
    part: MultipartPart,
    existing: u64,
    stop: Arc<AtomicU8>,
    tx: mpsc::Sender<PartEvent>,
) {
    if let Err(error) = run_part_inner(client, url, etag, &part, existing, &stop, &tx) {
        if let Err(send_error) = tx.send(PartEvent::Error(error)) {
            debug!(error = %send_error, "part worker failed to send error event");
        }
    }
}

fn run_part_inner(
    client: Client,
    url: String,
    etag: Option<String>,
    part: &MultipartPart,
    existing: u64,
    stop: &AtomicU8,
    tx: &mpsc::Sender<PartEvent>,
) -> Result<(), PartError> {
    if stop.load(Ordering::SeqCst) != PART_RUN {
        return Ok(());
    }

    if let Some(parent) = part.path.parent() {
        fs::create_dir_all(parent)?;
    }

    let expected = part_len(part);
    if existing >= expected {
        tx.send(PartEvent::Finished { index: part.index })
            .map_err(|error| {
                PartError::Other(NetError::Backend(format!(
                    "part event send failed: {error}"
                )))
            })?;
        return Ok(());
    }

    let start = part.start + existing;
    let mut request = client
        .get(&url)
        .header(RANGE, format!("bytes={start}-{}", part.end));
    if let Some(etag) = etag {
        request = request.header(IF_RANGE, etag);
    }

    let mut response = request.send()?;
    if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
        return Err(PartError::RangeNotHonored);
    }

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(existing > 0)
        .truncate(existing == 0)
        .open(&part.path)?;
    let mut downloaded = existing;
    let mut buffer = [0u8; DOWNLOAD_BUFFER_SIZE];

    loop {
        if stop.load(Ordering::SeqCst) != PART_RUN {
            file.flush()?;
            return Ok(());
        }

        let read = response.read(&mut buffer)?;
        if read == 0 {
            break;
        }

        file.write_all(&buffer[..read])?;
        downloaded += read as u64;
        tx.send(PartEvent::Progress {
            index: part.index,
            downloaded,
        })
        .map_err(|error| {
            PartError::Other(NetError::Backend(format!(
                "part event send failed: {error}"
            )))
        })?;
    }

    file.flush()?;
    if downloaded != expected {
        return Err(PartError::Other(NetError::Backend(format!(
            "multipart part {} ended early",
            part.index
        ))));
    }

    tx.send(PartEvent::Finished { index: part.index })
        .map_err(|error| {
            PartError::Other(NetError::Backend(format!(
                "part event send failed: {error}"
            )))
        })?;
    Ok(())
}

fn join_parts(handles: Vec<thread::JoinHandle<()>>) -> Result<(), NetError> {
    for handle in handles {
        if handle.join().is_err() {
            return Err(NetError::Backend(
                "multipart worker thread panicked".to_string(),
            ));
        }
    }
    Ok(())
}

fn progress_update(
    downloaded: u64,
    total_size: u64,
    started_at: Instant,
    temp_layout: TempLayout,
) -> TransferUpdate {
    TransferUpdate {
        progress: progress_from_metrics(downloaded, Some(total_size), started_at),
        temp_layout,
    }
}
