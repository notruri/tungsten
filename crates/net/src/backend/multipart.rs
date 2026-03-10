use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use reqwest::blocking::Client;
use reqwest::header::{IF_RANGE, RANGE};

use crate::error::NetError;
use crate::types::{DownloadSnapshot, MultipartPart, MultipartState, TempLayout};

use super::{
    ControlSignal, DOWNLOAD_BUFFER_SIZE, DownloadOutcome, DownloadTask, progress_from_metrics,
};

const PART_RUN: u8 = 0;
const PART_STOP: u8 = 1;
const PART_TICK: Duration = Duration::from_millis(50);

enum PartEvent {
    Progress { index: usize, downloaded: u64 },
    Finished { index: usize },
    Error(NetError),
}

/// Runs one logical download as multiple ranged requests and aggregates them
/// back into a single snapshot stream for the queue worker.
pub(super) fn download(
    client: Client,
    parallel_parts: usize,
    task: &DownloadTask,
    total_size: u64,
    on_progress: &mut dyn FnMut(DownloadSnapshot) -> Result<(), NetError>,
    control: &dyn Fn() -> ControlSignal,
) -> Result<DownloadOutcome, NetError> {
    let layout = prepare_layout(task, total_size, parallel_parts)?;
    let mut part_downloaded = load_part_progress(&layout)?;
    let mut total_downloaded = part_downloaded.iter().sum::<u64>();
    let started_at = Instant::now();

    on_progress(progress_snapshot(
        total_downloaded,
        total_size,
        started_at,
        TempLayout::Multipart(layout.clone()),
    ))?;

    if total_downloaded >= total_size {
        merge_parts(&task.temp_path, &layout)?;
        return Ok(DownloadOutcome::Completed(progress_snapshot(
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
        let existing = part_downloaded.get(part.index).copied().unwrap_or(0).min(expected);
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
                join_parts(handles)?;
                return Ok(DownloadOutcome::Paused(progress_snapshot(
                    total_downloaded,
                    total_size,
                    started_at,
                    TempLayout::Multipart(layout),
                )));
            }
            ControlSignal::Cancel => {
                stop.store(PART_STOP, Ordering::SeqCst);
                join_parts(handles)?;
                return Ok(DownloadOutcome::Cancelled(progress_snapshot(
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
                    *slot = downloaded.min(layout.parts[index].end - layout.parts[index].start + 1);
                    total_downloaded = total_downloaded.saturating_sub(previous).saturating_add(*slot);
                }
                on_progress(progress_snapshot(
                    total_downloaded,
                    total_size,
                    started_at,
                    TempLayout::Multipart(layout.clone()),
                ))?;
            }
            Ok(PartEvent::Finished { index }) => {
                finished_parts = finished_parts.saturating_add(1);
                if let Some(slot) = part_downloaded.get(index) {
                    total_downloaded = total_downloaded.max(part_downloaded.iter().sum::<u64>());
                    if *slot < part_len(&layout.parts[index]) {
                        return Err(NetError::Backend(format!(
                            "multipart part {} completed with incomplete size",
                            index
                        )));
                    }
                }

                if finished_parts >= active_parts {
                    break;
                }
            }
            Ok(PartEvent::Error(error)) => {
                stop.store(PART_STOP, Ordering::SeqCst);
                join_parts(handles)?;
                return Err(error);
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                if finished_parts >= active_parts {
                    break;
                }
                stop.store(PART_STOP, Ordering::SeqCst);
                join_parts(handles)?;
                return Err(NetError::Backend(
                    "multipart worker channel disconnected unexpectedly".to_string(),
                ));
            }
        }
    }

    join_parts(handles)?;
    merge_parts(&task.temp_path, &layout)?;
    Ok(DownloadOutcome::Completed(progress_snapshot(
        total_size,
        total_size,
        started_at,
        TempLayout::Single,
    )))
}

/// Restores or rebuilds the multipart temp-file layout for the current run.
fn prepare_layout(
    task: &DownloadTask,
    total_size: u64,
    parallel_parts: usize,
) -> Result<MultipartState, NetError> {
    match &task.temp_layout {
        TempLayout::Multipart(layout) if layout.total_size == total_size && !layout.parts.is_empty() => {
            Ok(layout.clone())
        }
        TempLayout::Multipart(layout) => {
            cleanup_parts(layout)?;
            Ok(build_layout(&task.temp_path, total_size, parallel_parts))
        }
        TempLayout::Single => Ok(build_layout(&task.temp_path, total_size, parallel_parts)),
    }
}

/// Splits a file into deterministic byte ranges and temp part paths.
fn build_layout(temp_path: &Path, total_size: u64, parallel_parts: usize) -> MultipartState {
    let part_count = parallel_parts.max(1).min(total_size as usize).max(1);
    let base = total_size / part_count as u64;
    let extra = total_size % part_count as u64;
    let mut parts = Vec::with_capacity(part_count);
    let mut start = 0u64;

    for index in 0..part_count {
        let len = base + u64::from(index < extra as usize);
        let end = start + len.saturating_sub(1);
        parts.push(MultipartPart {
            index,
            start,
            end,
            path: part_path_for(temp_path, index),
        });
        start = end.saturating_add(1);
    }

    MultipartState { total_size, parts }
}

/// Reads current on-disk part sizes so restart resume can pick up real progress.
fn load_part_progress(layout: &MultipartState) -> Result<Vec<u64>, NetError> {
    let mut progress = Vec::with_capacity(layout.parts.len());
    for part in &layout.parts {
        let expected = part_len(part);
        let size = match fs::metadata(&part.path) {
            Ok(metadata) if metadata.len() <= expected => metadata.len(),
            Ok(_) => {
                fs::remove_file(&part.path)?;
                0
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(NetError::Io(error)),
        };
        progress.push(size);
    }

    Ok(progress)
}

/// Entry point for an individual ranged part worker thread.
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
        let _ = tx.send(PartEvent::Error(error));
    }
}

/// Downloads one byte range into its dedicated temp file.
fn run_part_inner(
    client: Client,
    url: String,
    etag: Option<String>,
    part: &MultipartPart,
    existing: u64,
    stop: &AtomicU8,
    tx: &mpsc::Sender<PartEvent>,
) -> Result<(), NetError> {
    if stop.load(Ordering::SeqCst) != PART_RUN {
        return Ok(());
    }

    if let Some(parent) = part.path.parent() {
        fs::create_dir_all(parent)?;
    }

    let expected = part_len(part);
    if existing >= expected {
        tx.send(PartEvent::Finished { index: part.index })
            .map_err(|error| NetError::Backend(format!("part event send failed: {error}")))?;
        return Ok(());
    }

    let start = part.start + existing;
    let mut request = client.get(&url).header(RANGE, format!("bytes={start}-{}", part.end));
    if let Some(etag) = etag {
        request = request.header(IF_RANGE, etag);
    }

    let mut response = request.send()?;
    if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
        return Err(NetError::Backend(format!(
            "server did not honor multipart range request for part {}",
            part.index
        )));
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
        .map_err(|error| NetError::Backend(format!("part event send failed: {error}")))?;
    }

    file.flush()?;
    if downloaded != expected {
        return Err(NetError::Backend(format!(
            "multipart part {} ended early",
            part.index
        )));
    }

    tx.send(PartEvent::Finished { index: part.index })
        .map_err(|error| NetError::Backend(format!("part event send failed: {error}")))?;
    Ok(())
}

/// Concatenates finished part files into the main temp file and removes them.
fn merge_parts(temp_path: &Path, layout: &MultipartState) -> Result<(), NetError> {
    if let Some(parent) = temp_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let merge_result = (|| -> Result<(), NetError> {
        let mut output = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(temp_path)?;
        let mut buffer = [0u8; DOWNLOAD_BUFFER_SIZE];

        for part in &layout.parts {
            let mut input = fs::File::open(&part.path)?;
            loop {
                let read = input.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                output.write_all(&buffer[..read])?;
            }
        }

        output.flush()?;
        Ok(())
    })();

    if let Err(error) = &merge_result {
        let _ = fs::remove_file(temp_path);
        return Err(NetError::Backend(format!("failed to merge multipart download: {error}")));
    }

    cleanup_parts(layout)
}

/// Removes any temp part files associated with a multipart layout.
fn cleanup_parts(layout: &MultipartState) -> Result<(), NetError> {
    for part in &layout.parts {
        match fs::remove_file(&part.path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(NetError::Io(error)),
        }
    }
    Ok(())
}

/// Waits for all part workers and converts thread panics into backend errors.
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

/// Builds the aggregated progress snapshot reported back to the queue worker.
fn progress_snapshot(
    downloaded: u64,
    total_size: u64,
    started_at: Instant,
    temp_layout: TempLayout,
) -> DownloadSnapshot {
    DownloadSnapshot {
        progress: progress_from_metrics(downloaded, Some(total_size), started_at),
        temp_layout,
    }
}

/// Returns the inclusive length of a multipart byte range.
fn part_len(part: &MultipartPart) -> u64 {
    part.end.saturating_sub(part.start).saturating_add(1)
}

/// Builds the deterministic temp path for a multipart segment.
fn part_path_for(temp_path: &Path, index: usize) -> PathBuf {
    let mut name = OsString::from(temp_path.as_os_str());
    name.push(format!(".p{index}"));
    PathBuf::from(name)
}
