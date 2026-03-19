use std::fs;
use std::io::{Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tracing::{debug, warn};

use crate::error::CoreError;
use crate::model::{DownloadId, DownloadStatus, IntegrityRule, QueueEvent};
use crate::store::PersistedDownload;
use crate::transfer::{
    ControlSignal, ProbeInfo, TempLayout, TransferOutcome, TransferTask, TransferUpdate,
};

use super::coordinator::{capture_progress_update, current_progress_update};
use super::files::{
    destination_from_request, remove_file_if_exists, remove_temp_layout_files, resolve_destination,
    sha256_reader, temp_path_for,
};
use super::{
    CONTROL_CANCEL, CONTROL_PAUSE, CONTROL_RUN, Shared, lock_coordinator, lock_state,
    log_status_change, publish_event, save_full_state,
};

pub(crate) fn spawn_enqueue_resolution(shared: Arc<Shared>, download_id: DownloadId) {
    let tokio = shared.tokio.clone();
    tokio.spawn(async move {
        if let Err(error) = resolve_download_preflight(&shared, download_id).await {
            warn!(
                download_id = %download_id,
                error = %error,
                "failed to resolve destination during enqueue preflight"
            );
        }
    });
}

async fn resolve_download_preflight(
    shared: &Shared,
    download_id: DownloadId,
) -> Result<(), CoreError> {
    let initial_record = {
        let state = lock_state(shared)?;
        state
            .downloads
            .get(&download_id)
            .cloned()
            .ok_or(CoreError::DownloadNotFound(download_id))?
    };

    let probe = match shared.transfer.probe(&initial_record.request).await {
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

        let next = apply_probe_info(current, &state, download_id, probe.as_ref());
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

pub(crate) async fn run_download_worker(
    shared: Arc<Shared>,
    download_id: DownloadId,
) -> Result<(), CoreError> {
    let (mut record, control) = {
        let state = lock_state(&shared)?;
        let record = state
            .downloads
            .get(&download_id)
            .cloned()
            .ok_or(CoreError::DownloadNotFound(download_id))?;
        let control = state
            .controls
            .get(&download_id)
            .cloned()
            .ok_or(CoreError::DownloadNotFound(download_id))?;
        debug!(
            download_id = %download_id,
            destination = ?record.destination,
            status = ?record.status,
            "starting download worker"
        );
        (record, control)
    };

    let probe = match shared.transfer.probe(&record.request).await {
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
    record = apply_probe_updates(&shared, download_id, probe.as_ref())?;

    let task = TransferTask {
        download_id,
        request: record.request.clone(),
        temp_path: record.temp_path.clone(),
        temp_layout: record.temp_layout.clone(),
        existing_size: 0,
        etag: record.etag.clone(),
        resume_speed_bps: record.progress.speed_bps,
    };
    debug!(
        download_id = %download_id,
        temp_layout = ?task.temp_layout,
        "prepared transfer task"
    );

    let update_shared = Arc::clone(&shared);
    let mut on_update = move |update: TransferUpdate| -> Result<(), CoreError> {
        capture_progress_update(&update_shared, download_id, update)
    };
    let control_for_backend = Arc::clone(&control);
    let outcome = shared
        .transfer
        .run(
            &task,
            probe,
            &mut on_update,
            &|| match control_for_backend.load(Ordering::SeqCst) {
                CONTROL_PAUSE => ControlSignal::Pause,
                CONTROL_CANCEL => ControlSignal::Cancel,
                _ => ControlSignal::Run,
            },
        )
        .await;

    match outcome {
        Ok(TransferOutcome::Completed(update)) => {
            debug!(
                download_id = %download_id,
                downloaded = update.progress.downloaded,
                total = ?update.progress.total,
                "transfer completed"
            );
            finalize_download(&shared, download_id, update, &record)
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
            let update = current_progress_update(&shared, download_id).unwrap_or(TransferUpdate {
                progress: record.progress,
                temp_layout: record.temp_layout,
            });
            set_failed(&shared, download_id, update, error.to_string())
        }
    }
}

fn apply_probe_updates(
    shared: &Shared,
    download_id: DownloadId,
    probe: Option<&ProbeInfo>,
) -> Result<PersistedDownload, CoreError> {
    let mut should_persist = false;
    let mut coordinator_progress = None;
    let current_record = {
        let mut state = lock_state(shared)?;
        let current = state
            .downloads
            .get(&download_id)
            .cloned()
            .ok_or(CoreError::DownloadNotFound(download_id))?;
        let mut next = current.clone();
        let mut changed = false;

        if next.destination.is_none() {
            next = apply_probe_info(next, &state, download_id, probe);
            changed = true;
        }

        if let Some(probe) = probe {
            if next.supports_resume != probe.accept_ranges {
                next.supports_resume = probe.accept_ranges;
                changed = true;
            }
            if let Some(total_size) = probe.total_size
                && next.progress.total != Some(total_size)
            {
                next.progress.total = Some(total_size);
                changed = true;
            }
            if let Some(etag) = &probe.etag
                && next.etag.as_ref() != Some(etag)
            {
                next.etag = Some(etag.clone());
                changed = true;
            }
            if let Some(last_modified) = &probe.last_modified
                && next.last_modified.as_ref() != Some(last_modified)
            {
                next.last_modified = Some(last_modified.clone());
                changed = true;
            }
        }

        if changed {
            next.touch();
            coordinator_progress = Some(next.progress.clone());
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

    if let Some(progress) = coordinator_progress {
        let mut coordinator = lock_coordinator(shared)?;
        if let Some(current) = coordinator.updates.get_mut(&download_id) {
            current.update.progress = progress;
        }
    }

    if should_persist {
        save_full_state(shared)?;
    }

    Ok(current_record)
}

fn apply_probe_info(
    mut record: crate::store::PersistedDownload,
    state: &super::QueueState,
    download_id: DownloadId,
    probe: Option<&ProbeInfo>,
) -> crate::store::PersistedDownload {
    let candidate = destination_from_request(
        &record.request.destination,
        &record.request.url,
        probe.as_ref().and_then(|value| value.file_name.as_deref()),
        &state.fallback_filename,
    );
    let resolved_destination = resolve_destination(
        &candidate,
        &state.downloads,
        &record.request.conflict,
        Some(download_id),
    );
    record.temp_path = temp_path_for(&resolved_destination, &state.temp_root, download_id);
    record.destination = Some(resolved_destination);

    if let Some(probe) = probe {
        record.supports_resume = probe.accept_ranges;
        if let Some(total_size) = probe.total_size {
            record.progress.total = Some(total_size);
        }
        if let Some(etag) = &probe.etag {
            record.etag = Some(etag.clone());
        }
        if let Some(last_modified) = &probe.last_modified {
            record.last_modified = Some(last_modified.clone());
        }
    }

    record.touch();
    record
}

fn finalize_download(
    shared: &Shared,
    download_id: DownloadId,
    update: TransferUpdate,
    record: &PersistedDownload,
) -> Result<(), CoreError> {
    debug!(?download_id, "finalizing download");

    let temp_path = &record.temp_path;
    let destination = record
        .destination
        .as_ref()
        .ok_or_else(|| CoreError::Backend("download destination is unresolved".to_string()))?;

    set_status(
        shared,
        download_id,
        DownloadStatus::Verifying,
        update.clone(),
        None,
    )?;

    let (destination, output) = match reserve_dest(shared, download_id, destination) {
        Ok(reserved) => reserved,
        Err(error) => {
            return set_failed(
                shared,
                download_id,
                update,
                format!("failed to reserve destination: {error}"),
            );
        }
    };

    match promote_file(output, temp_path, &destination, &record.request.integrity) {
        Ok(()) => {
            if matches!(record.request.integrity, IntegrityRule::None) {
                debug!(download_id = %download_id, "integrity verification skipped");
            } else {
                debug!(download_id = %download_id, "sha256 verification passed");
            }
            debug!(
                download_id = %download_id,
                destination = %destination.display(),
                downloaded = update.progress.downloaded,
                total = ?update.progress.total,
                "download file copied to destination after verification"
            );
            set_status(shared, download_id, DownloadStatus::Completed, update, None)
        }
        Err(FinalizeError::IntegrityMismatch { expected, actual }) => {
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
        Err(FinalizeError::Core(error)) => set_failed(
            shared,
            download_id,
            update,
            format!("failed to promote verified file: {error}"),
        ),
    }
}

#[derive(Debug)]
enum FinalizeError {
    IntegrityMismatch { expected: String, actual: String },
    Core(CoreError),
}

impl From<CoreError> for FinalizeError {
    fn from(error: CoreError) -> Self {
        Self::Core(error)
    }
}

fn reserve_dest(
    shared: &Shared,
    download_id: DownloadId,
    requested: &Path,
) -> Result<(std::path::PathBuf, fs::File), CoreError> {
    let mut candidate = requested.to_path_buf();

    loop {
        if let Some(parent) = candidate.parent() {
            fs::create_dir_all(parent)?;
        }

        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => {
                let mut state = lock_state(shared)?;
                let record = state
                    .downloads
                    .get_mut(&download_id)
                    .ok_or(CoreError::DownloadNotFound(download_id))?;
                record.destination = Some(candidate.clone());
                return Ok((candidate, file));
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let state = lock_state(shared)?;
                let record = state
                    .downloads
                    .get(&download_id)
                    .ok_or(CoreError::DownloadNotFound(download_id))?;
                let next = resolve_destination(
                    &candidate,
                    &state.downloads,
                    &record.request.conflict,
                    Some(download_id),
                );
                if next == candidate {
                    return Err(CoreError::Io(error));
                }
                candidate = next;
            }
            Err(error) => return Err(CoreError::Io(error)),
        }
    }
}

fn promote_file(
    mut output: fs::File,
    temp_path: &Path,
    destination: &Path,
    integrity: &IntegrityRule,
) -> Result<(), FinalizeError> {
    let mut input = fs::File::open(temp_path).map_err(CoreError::from)?;

    if let IntegrityRule::Sha256(expected) = integrity {
        let actual = sha256_reader(&mut input)?;
        if !actual.eq_ignore_ascii_case(expected) {
            return Err(FinalizeError::IntegrityMismatch {
                expected: expected.clone(),
                actual,
            });
        }
        input.seek(SeekFrom::Start(0)).map_err(CoreError::from)?;
    }

    let expected_len = input.metadata().map_err(CoreError::from)?.len();
    let copy_result = (|| -> Result<(), CoreError> {
        let copied = std::io::copy(&mut input, &mut output)?;
        if copied != expected_len {
            return Err(CoreError::Backend(format!(
                "copied {copied} bytes into {}, expected {expected_len}",
                destination.display()
            )));
        }
        output.sync_all()?;
        Ok(())
    })();

    drop(input);

    if let Err(error) = copy_result {
        drop(output);
        remove_file_if_exists(destination)?;
        return Err(FinalizeError::Core(error));
    }

    drop(output);

    match fs::remove_file(temp_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            warn!(
                temp_path = %temp_path.display(),
                error = %error,
                "temp file could not be removed"
            );
        }
    }

    Ok(())
}

fn set_paused(
    shared: &Shared,
    download_id: DownloadId,
    update: TransferUpdate,
) -> Result<(), CoreError> {
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
) -> Result<(), CoreError> {
    debug!(download_id = %download_id, "cleaning up cancelled download temp files");
    remove_file_if_exists(temp_path)?;
    remove_temp_layout_files(&update.temp_layout)?;

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
) -> Result<(), CoreError> {
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
) -> Result<(), CoreError> {
    let has_error = error.is_some();
    debug!(
        download_id = %download_id,
        status = ?status,
        downloaded = update.progress.downloaded,
        total = ?update.progress.total,
        has_error,
        "applying status update"
    );

    let (updated_record, control) = {
        let mut state = lock_state(shared)?;
        let control = state.controls.get(&download_id).cloned();
        let updated_record = {
            let record = state
                .downloads
                .get_mut(&download_id)
                .ok_or(CoreError::DownloadNotFound(download_id))?;

            let previous = record.status.clone();
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
            log_status_change(download_id, &previous, &record.status, "lifecycle update");
            record.to_record()
        };
        (updated_record, control)
    };

    lock_coordinator(shared)?.updates.remove(&download_id);
    if matches!(status, DownloadStatus::Completed) {
        shared.transfer.clear_download(download_id);
    }
    if let Some(control) = control {
        control.store(CONTROL_RUN, Ordering::SeqCst);
    }
    {
        let mut state = lock_state(shared)?;
        publish_event(&mut state, QueueEvent::Updated(updated_record));
    }

    save_full_state(shared)
}
