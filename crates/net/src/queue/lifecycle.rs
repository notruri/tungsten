use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use tracing::{debug, warn};

use crate::error::NetError;
use crate::model::{DownloadId, DownloadStatus, IntegrityRule, QueueEvent};
use crate::store::PersistedDownload;
use crate::transfer::{
    ControlSignal, ProbeInfo, TempLayout, TransferOutcome, TransferTask, TransferUpdate,
};

use super::files::{
    destination_from_request, remove_file_if_exists, resolve_destination, sha256_file,
    temp_path_for,
};
use super::persist::{lock_state, publish_event, save_full_state};
use super::runtime::{capture_runtime_update, current_update};
use super::{CONTROL_CANCEL, CONTROL_PAUSE, CONTROL_RUN, Shared};

pub(crate) fn spawn_enqueue_resolution(shared: Arc<Shared>, download_id: DownloadId) {
    thread::spawn(move || {
        if let Err(error) = resolve_download_preflight(&shared, download_id) {
            warn!(
                download_id = %download_id,
                error = %error,
                "failed to resolve destination during enqueue preflight"
            );
        }
    });
}

fn resolve_download_preflight(shared: &Shared, download_id: DownloadId) -> Result<(), NetError> {
    let initial_record = {
        let state = lock_state(shared)?;
        state
            .downloads
            .get(&download_id)
            .cloned()
            .ok_or(NetError::DownloadNotFound(download_id))?
    };

    let probe = match shared.transfer.probe(&initial_record.request) {
        Ok(probe) => Some(probe),
        Err(error) => {
            warn!(
                download_id = %download_id,
                error = %error,
                "preflight probe failed; falling back to URL/default destination"
            );
            None
        }
    };

    let resolved_record = {
        let mut state = lock_state(shared)?;
        let Some(current) = state.downloads.get(&download_id).cloned() else {
            return Ok(());
        };

        if current.destination.is_some() {
            return Ok(());
        }

        let mut next = current;
        let fallback_filename = state.fallback_filename.clone();
        let candidate = destination_from_request(
            &next.request.destination,
            &next.request.url,
            probe.as_ref().and_then(|value| value.file_name.as_deref()),
            &fallback_filename,
        );
        let resolved_destination =
            resolve_destination(&candidate, &state.downloads, &next.request.conflict);
        next.temp_path = temp_path_for(&resolved_destination, download_id);
        next.destination = Some(resolved_destination);

        if let Some(probe) = &probe {
            next.supports_resume = probe.accept_ranges;
            if let Some(total_size) = probe.total_size {
                next.progress.total = Some(total_size);
            }
            if let Some(etag) = &probe.etag {
                next.etag = Some(etag.clone());
            }
            if let Some(last_modified) = &probe.last_modified {
                next.last_modified = Some(last_modified.clone());
            }
        }

        next.touch();
        if let Some(record) = state.downloads.get_mut(&download_id) {
            *record = next.clone();
        }
        publish_event(&mut state, QueueEvent::Updated(next.to_record()));
        next
    };

    save_full_state(shared)?;
    debug!(
        download_id = %download_id,
        destination = ?resolved_record.destination,
        "resolved destination during enqueue preflight"
    );

    Ok(())
}

pub(crate) fn run_download_worker(
    shared: Arc<Shared>,
    download_id: DownloadId,
) -> Result<(), NetError> {
    let (mut record, control, speed_limit) = {
        let state = lock_state(&shared)?;
        let record = state
            .downloads
            .get(&download_id)
            .cloned()
            .ok_or(NetError::DownloadNotFound(download_id))?;
        let control = state
            .controls
            .get(&download_id)
            .cloned()
            .ok_or(NetError::DownloadNotFound(download_id))?;
        let speed_limit = state
            .speed_limits
            .get(&download_id)
            .cloned()
            .ok_or(NetError::DownloadNotFound(download_id))?;
        debug!(
            download_id = %download_id,
            destination = ?record.destination,
            status = ?record.status,
            "starting download worker"
        );
        (record, control, speed_limit)
    };

    let probe = match shared.transfer.probe(&record.request) {
        Ok(probe) => Some(probe),
        Err(error) => {
            warn!(
                download_id = %download_id,
                error = %error,
                "download probe failed; continuing with cached/default metadata"
            );
            None
        }
    };
    record = apply_probe_info(&shared, download_id, probe.as_ref())?;

    let existing_size = match fs::metadata(&record.temp_path) {
        Ok(metadata) if matches!(record.temp_layout, TempLayout::Single) => metadata.len(),
        Ok(_) => 0,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => {
            set_failed(
                &shared,
                download_id,
                TransferUpdate {
                    progress: record.progress.clone(),
                    temp_layout: record.temp_layout.clone(),
                },
                format!("failed to read temp file metadata: {error}"),
            )?;
            return Ok(());
        }
    };

    let task = TransferTask {
        request: record.request.clone(),
        temp_path: record.temp_path.clone(),
        temp_layout: record.temp_layout.clone(),
        existing_size,
        etag: record.etag.clone(),
        resume_speed_bps: record.progress.speed_bps,
        speed_limit: shared.global_speed_limit.for_task(speed_limit),
    };
    debug!(
        download_id = %download_id,
        existing_size,
        temp_layout = ?task.temp_layout,
        "prepared transfer task"
    );

    let update_shared = Arc::clone(&shared);
    let mut on_update = move |update: TransferUpdate| -> Result<(), NetError> {
        capture_runtime_update(&update_shared, download_id, update)
    };

    let control_for_backend = Arc::clone(&control);
    let outcome =
        shared
            .transfer
            .download(&task, probe, &mut on_update, &|| match control_for_backend
                .load(Ordering::SeqCst)
            {
                CONTROL_PAUSE => ControlSignal::Pause,
                CONTROL_CANCEL => ControlSignal::Cancel,
                _ => ControlSignal::Run,
            });

    match outcome {
        Ok(TransferOutcome::Completed(update)) => {
            debug!(
                download_id = %download_id,
                downloaded = update.progress.downloaded,
                total = ?update.progress.total,
                "transfer completed"
            );
            finish_completed(&shared, download_id, update, &record)
        }
        Ok(TransferOutcome::Paused(update)) => {
            debug!(download_id = %download_id, "transfer paused");
            set_paused(&shared, download_id, update)
        }
        Ok(TransferOutcome::Cancelled(update)) => {
            debug!(download_id = %download_id, "transfer cancelled");
            set_cancelled(&shared, download_id, update, &record.temp_path)
        }
        Err(error) => {
            debug!(
                download_id = %download_id,
                error = %error,
                "transfer failed"
            );
            let update = current_update(&shared, download_id).unwrap_or(TransferUpdate {
                progress: record.progress,
                temp_layout: record.temp_layout,
            });
            set_failed(&shared, download_id, update, error.to_string())
        }
    }
}

fn apply_probe_info(
    shared: &Shared,
    download_id: DownloadId,
    probe: Option<&ProbeInfo>,
) -> Result<crate::store::PersistedDownload, NetError> {
    let mut should_persist = false;
    let current_record = {
        let mut state = lock_state(shared)?;
        let current = state
            .downloads
            .get(&download_id)
            .cloned()
            .ok_or(NetError::DownloadNotFound(download_id))?;
        let mut next = current.clone();
        let mut changed = false;

        if let Some(probe) = probe {
            if next.supports_resume != probe.accept_ranges {
                next.supports_resume = probe.accept_ranges;
                changed = true;
            }
            if let Some(total_size) = probe.total_size
                && next.progress.total != Some(total_size) {
                    next.progress.total = Some(total_size);
                    changed = true;
                }
            if let Some(etag) = &probe.etag
                && next.etag.as_ref() != Some(etag) {
                    next.etag = Some(etag.clone());
                    changed = true;
                }
            if let Some(last_modified) = &probe.last_modified
                && next.last_modified.as_ref() != Some(last_modified) {
                    next.last_modified = Some(last_modified.clone());
                    changed = true;
                }
        }

        if changed {
            next.touch();
            if let Some(runtime) = state.runtime.get_mut(&download_id) {
                runtime.update.progress = next.progress.clone();
                runtime.persist_dirty = true;
            }
            if let Some(record) = state.downloads.get_mut(&download_id) {
                *record = next.clone();
            }
            publish_event(&mut state, QueueEvent::Updated(next.to_record()));
            should_persist = true;
            next
        } else {
            current
        }
    };

    if should_persist {
        save_full_state(shared)?;
    }

    Ok(current_record)
}

fn finish_completed(
    shared: &Shared,
    download_id: DownloadId,
    update: TransferUpdate,
    record: &PersistedDownload,
) -> Result<(), NetError> {
    let destination = record
        .destination
        .as_ref()
        .ok_or_else(|| NetError::Backend("download destination is unresolved".to_string()))?;

    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::rename(&record.temp_path, destination)?;
    debug!(
        download_id = %download_id,
        destination = %destination.display(),
        "download file moved to destination, starting verification"
    );

    set_status(
        shared,
        download_id,
        DownloadStatus::Verifying,
        update.clone(),
        None,
    )?;

    match &record.request.integrity {
        IntegrityRule::None => {
            debug!(download_id = %download_id, "integrity verification skipped");
            set_status(shared, download_id, DownloadStatus::Completed, update, None)
        }
        IntegrityRule::Sha256(expected) => {
            let actual = sha256_file(destination)?;
            if actual.eq_ignore_ascii_case(expected) {
                debug!(download_id = %download_id, "sha256 verification passed");
                set_status(shared, download_id, DownloadStatus::Completed, update, None)
            } else {
                debug!(
                    download_id = %download_id,
                    expected = %expected,
                    actual = %actual,
                    "sha256 verification failed"
                );
                set_status(
                    shared,
                    download_id,
                    DownloadStatus::Failed,
                    update,
                    Some(format!(
                        "sha256 mismatch: expected {expected}, got {actual}"
                    )),
                )
            }
        }
    }
}

fn set_paused(
    shared: &Shared,
    download_id: DownloadId,
    update: TransferUpdate,
) -> Result<(), NetError> {
    debug!(download_id = %download_id, "setting download status to paused");
    {
        let state = lock_state(shared)?;
        if let Some(control) = state.controls.get(&download_id) {
            control.store(CONTROL_RUN, Ordering::SeqCst);
        }
    }

    set_status(shared, download_id, DownloadStatus::Paused, update, None)
}

fn set_cancelled(
    shared: &Shared,
    download_id: DownloadId,
    update: TransferUpdate,
    temp_path: &Path,
) -> Result<(), NetError> {
    debug!(download_id = %download_id, "cleaning up cancelled download temp files");
    remove_file_if_exists(temp_path)?;
    if let TempLayout::Multipart(layout) = &update.temp_layout {
        for part in &layout.parts {
            remove_file_if_exists(&part.path)?;
        }
    }

    set_status(
        shared,
        download_id,
        DownloadStatus::Cancelled,
        TransferUpdate {
            progress: update.progress,
            temp_layout: TempLayout::Single,
        },
        None,
    )
}

fn set_failed(
    shared: &Shared,
    download_id: DownloadId,
    update: TransferUpdate,
    error_message: String,
) -> Result<(), NetError> {
    debug!(
        download_id = %download_id,
        error_message = %error_message,
        "setting download status to failed"
    );
    set_status(
        shared,
        download_id,
        DownloadStatus::Failed,
        update,
        Some(error_message),
    )
}

fn set_status(
    shared: &Shared,
    download_id: DownloadId,
    status: DownloadStatus,
    update: TransferUpdate,
    error: Option<String>,
) -> Result<(), NetError> {
    let has_error = error.is_some();
    debug!(
        download_id = %download_id,
        status = ?status,
        downloaded = update.progress.downloaded,
        total = ?update.progress.total,
        has_error,
        "applying status update"
    );

    {
        let mut state = lock_state(shared)?;
        let control = state.controls.get(&download_id).cloned();
        let updated_record = {
            let record = state
                .downloads
                .get_mut(&download_id)
                .ok_or(NetError::DownloadNotFound(download_id))?;

            record.status = status.clone();
            record.progress = update.progress;
            record.temp_layout = match status {
                DownloadStatus::Completed
                | DownloadStatus::Cancelled
                | DownloadStatus::Verifying => TempLayout::Single,
                _ => update.temp_layout,
            };
            record.error = error;
            record.touch();
            record.to_record()
        };

        state.runtime.remove(&download_id);
        if let Some(control) = control {
            control.store(CONTROL_RUN, Ordering::SeqCst);
        }
        publish_event(&mut state, QueueEvent::Updated(updated_record));
    }

    save_full_state(shared)
}
