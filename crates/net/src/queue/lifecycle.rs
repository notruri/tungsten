use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::error::NetError;
use crate::model::{DownloadId, DownloadStatus, IntegrityRule, QueueEvent};
use crate::transfer::{
    ControlSignal, TempLayout, TransferOutcome, TransferTask, TransferUpdate,
};

use super::files::{remove_file_if_exists, sha256_file};
use super::persist::{lock_state, publish_event, save_full_state};
use super::runtime::{capture_runtime_update, current_update};
use super::{CONTROL_CANCEL, CONTROL_PAUSE, CONTROL_RUN, Shared};

pub(crate) fn run_download_worker(
    shared: Arc<Shared>,
    download_id: DownloadId,
) -> Result<(), NetError> {
    let (record, control) = {
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
        (record, control)
    };

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
    };

    let update_shared = Arc::clone(&shared);
    let mut on_update = move |update: TransferUpdate| -> Result<(), NetError> {
        capture_runtime_update(&update_shared, download_id, update)
    };

    let control_for_backend = Arc::clone(&control);
    let outcome = shared.transfer.download(
        &task,
        &mut on_update,
        &|| match control_for_backend.load(Ordering::SeqCst) {
            CONTROL_PAUSE => ControlSignal::Pause,
            CONTROL_CANCEL => ControlSignal::Cancel,
            _ => ControlSignal::Run,
        },
    );

    match outcome {
        Ok(TransferOutcome::Completed(update)) => finish_completed(&shared, download_id, update, &record),
        Ok(TransferOutcome::Paused(update)) => set_paused(&shared, download_id, update),
        Ok(TransferOutcome::Cancelled(update)) => {
            set_cancelled(&shared, download_id, update, &record.temp_path)
        }
        Err(error) => {
            let update = current_update(&shared, download_id).unwrap_or(TransferUpdate {
                progress: record.progress,
                temp_layout: record.temp_layout,
            });
            set_failed(&shared, download_id, update, error.to_string())
        }
    }
}

fn finish_completed(
    shared: &Shared,
    download_id: DownloadId,
    update: TransferUpdate,
    record: &crate::store::PersistedDownload,
) -> Result<(), NetError> {
    if let Some(parent) = record.request.destination.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::rename(&record.temp_path, &record.request.destination)?;

    set_status(
        shared,
        download_id,
        DownloadStatus::Verifying,
        update.clone(),
        None,
    )?;

    match &record.request.integrity {
        IntegrityRule::None => {
            set_status(shared, download_id, DownloadStatus::Completed, update, None)
        }
        IntegrityRule::Sha256(expected) => {
            let actual = sha256_file(&record.request.destination)?;
            if actual.eq_ignore_ascii_case(expected) {
                set_status(shared, download_id, DownloadStatus::Completed, update, None)
            } else {
                set_status(
                    shared,
                    download_id,
                    DownloadStatus::Failed,
                    update,
                    Some(format!("sha256 mismatch: expected {expected}, got {actual}")),
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
                DownloadStatus::Completed | DownloadStatus::Cancelled | DownloadStatus::Verifying => {
                    TempLayout::Single
                }
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
