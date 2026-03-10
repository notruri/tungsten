use std::fs;
use std::sync::atomic::Ordering;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use crate::backend::{ControlSignal, DownloadOutcome, DownloadTask};
use crate::error::NetError;
use crate::types::{
    DownloadId, DownloadRecord, DownloadSnapshot, DownloadStatus, IntegrityRule, QueueEvent,
    TempLayout,
};

use super::files::{remove_file_if_exists, sha256_file};
use super::shared::{lock_state, publish_event, save_full_state};
use super::{
    CONTROL_CANCEL, CONTROL_PAUSE, CONTROL_RUN, COORDINATOR_TICK, PERSIST_INTERVAL,
    RuntimeDownloadState, Shared, UI_EVENT_INTERVAL,
};

/// Spawns the scheduler loop that launches download workers up to `max_parallel`.
pub(super) fn spawn_scheduler(shared: Arc<Shared>) {
    thread::spawn(move || {
        loop {
            let launch_ids = match pick_next_downloads(&shared) {
                Ok(ids) => ids,
                Err(error) => {
                    eprintln!("scheduler lock failed: {error}");
                    thread::sleep(Duration::from_millis(300));
                    continue;
                }
            };

            if !launch_ids.is_empty() {
                if let Err(error) = save_full_state(&shared) {
                    eprintln!("failed to save state before launch: {error}");
                }
            }

            for download_id in launch_ids {
                let shared_for_worker = Arc::clone(&shared);
                thread::spawn(move || {
                    if let Err(error) = run_download_worker(shared_for_worker, download_id) {
                        eprintln!("worker failed for {download_id}: {error}");
                    }
                });
            }

            thread::sleep(Duration::from_millis(250));
        }
    });
}

/// Spawns the coordinator that drains runtime snapshots into queue state.
pub(super) fn spawn_coordinator(shared: Arc<Shared>, coordinator_rx: mpsc::Receiver<()>) {
    thread::spawn(move || {
        loop {
            match coordinator_rx.recv_timeout(COORDINATOR_TICK) {
                Ok(_) | Err(mpsc::RecvTimeoutError::Timeout) => {
                    if let Err(error) = process_runtime_updates(&shared, false) {
                        eprintln!("runtime coordinator failed: {error}");
                        thread::sleep(Duration::from_millis(50));
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => return,
            }
        }
    });
}

/// Forces the latest runtime snapshots into persisted queue state.
pub(super) fn flush_runtime_and_persist(shared: &Shared) -> Result<(), NetError> {
    process_runtime_updates(shared, true)?;
    save_full_state(shared)
}

/// Selects the next queued downloads that should start running.
fn pick_next_downloads(shared: &Shared) -> Result<Vec<DownloadId>, NetError> {
    let mut state = lock_state(shared)?;
    let running_count = state
        .downloads
        .values()
        .filter(|record| {
            matches!(
                record.status,
                DownloadStatus::Running | DownloadStatus::Verifying
            )
        })
        .count();

    let available_slots = state.max_parallel.saturating_sub(running_count);
    if available_slots == 0 {
        return Ok(Vec::new());
    }

    let mut queued_ids = state
        .downloads
        .values()
        .filter(|record| record.status == DownloadStatus::Queued)
        .map(|record| record.id)
        .collect::<Vec<_>>();
    queued_ids.sort_by_key(|id| id.0);

    let mut picked_ids = Vec::new();
    for download_id in queued_ids.into_iter().take(available_slots) {
        let mut updated = None;
        let mut initial_snapshot = None;
        if let Some(record) = state.downloads.get_mut(&download_id) {
            record.status = DownloadStatus::Running;
            record.error = None;
            record.touch();
            initial_snapshot = Some(DownloadSnapshot {
                progress: record.progress.clone(),
                temp_layout: record.temp_layout.clone(),
            });
            updated = Some(record.clone());
            picked_ids.push(download_id);
        }

        if let Some(snapshot) = initial_snapshot {
            state
                .runtime
                .entry(download_id)
                .or_insert_with(|| RuntimeDownloadState::new(snapshot));
        }

        if let Some(updated_record) = updated {
            publish_event(&mut state, QueueEvent::Updated(updated_record));
        }

        if let Some(control) = state.controls.get(&download_id) {
            control.store(CONTROL_RUN, Ordering::SeqCst);
        }
    }

    Ok(picked_ids)
}

/// Executes one queue worker from task construction through terminal outcome.
fn run_download_worker(shared: Arc<Shared>, download_id: DownloadId) -> Result<(), NetError> {
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
                DownloadSnapshot {
                    progress: record.progress.clone(),
                    temp_layout: record.temp_layout.clone(),
                },
                format!("failed to read temp file metadata: {error}"),
            )?;
            return Ok(());
        }
    };

    let task = DownloadTask {
        request: record.request.clone(),
        temp_path: record.temp_path.clone(),
        temp_layout: record.temp_layout.clone(),
        existing_size,
        allow_resume: record.supports_resume,
        etag: record.etag.clone(),
    };

    let progress_shared = Arc::clone(&shared);
    let mut on_progress = move |snapshot: DownloadSnapshot| -> Result<(), NetError> {
        capture_runtime_progress(&progress_shared, download_id, snapshot)
    };

    let control_for_backend = Arc::clone(&control);
    let outcome = shared
        .backend
        .download(
            &task,
            &mut on_progress,
            &|| match control_for_backend.load(Ordering::SeqCst) {
                CONTROL_PAUSE => ControlSignal::Pause,
                CONTROL_CANCEL => ControlSignal::Cancel,
                _ => ControlSignal::Run,
            },
        );

    match outcome {
        Ok(DownloadOutcome::Completed(progress)) => {
            finish_completed(&shared, download_id, progress, &record)
        }
        Ok(DownloadOutcome::Paused(progress)) => set_paused(&shared, download_id, progress),
        Ok(DownloadOutcome::Cancelled(progress)) => {
            set_cancelled(&shared, download_id, progress, &record.temp_path)
        }
        Err(error) => {
            let snapshot = current_progress(&shared, download_id).unwrap_or(DownloadSnapshot {
                progress: record.progress,
                temp_layout: record.temp_layout,
            });
            set_failed(&shared, download_id, snapshot, error.to_string())
        }
    }
}

/// Finalizes a completed backend run by moving the merged temp file into place
/// and running optional integrity verification.
fn finish_completed(
    shared: &Shared,
    download_id: DownloadId,
    snapshot: DownloadSnapshot,
    record: &DownloadRecord,
) -> Result<(), NetError> {
    if let Some(parent) = record.request.destination.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::rename(&record.temp_path, &record.request.destination)?;

    set_status(
        shared,
        download_id,
        DownloadStatus::Verifying,
        snapshot.clone(),
        None,
    )?;

    match &record.request.integrity {
        IntegrityRule::None => set_status(
            shared,
            download_id,
            DownloadStatus::Completed,
            snapshot,
            None,
        ),
        IntegrityRule::Sha256(expected) => {
            let actual = sha256_file(&record.request.destination)?;
            if actual.eq_ignore_ascii_case(expected) {
                set_status(
                    shared,
                    download_id,
                    DownloadStatus::Completed,
                    snapshot,
                    None,
                )
            } else {
                set_status(
                    shared,
                    download_id,
                    DownloadStatus::Failed,
                    snapshot,
                    Some(format!(
                        "sha256 mismatch: expected {expected}, got {actual}"
                    )),
                )
            }
        }
    }
}

/// Updates the in-memory runtime snapshot for a running download and wakes the
/// coordinator if needed.
fn capture_runtime_progress(
    shared: &Shared,
    download_id: DownloadId,
    snapshot: DownloadSnapshot,
) -> Result<(), NetError> {
    let should_wake = {
        let mut state = lock_state(shared)?;
        let runtime = state
            .runtime
            .entry(download_id)
            .or_insert_with(|| RuntimeDownloadState::new(snapshot.clone()));
        runtime.snapshot = snapshot;
        runtime.ui_dirty = true;
        runtime.persist_dirty = true;

        if runtime.wake_pending {
            false
        } else {
            runtime.wake_pending = true;
            true
        }
    };

    if should_wake && shared.coordinator_tx.send(()).is_err() {
        eprintln!("failed to wake runtime coordinator for {download_id}");
    }

    Ok(())
}

/// Persists the latest paused snapshot and resets the queue control flag.
fn set_paused(
    shared: &Shared,
    download_id: DownloadId,
    snapshot: DownloadSnapshot,
) -> Result<(), NetError> {
    {
        let state = lock_state(shared)?;
        if let Some(control) = state.controls.get(&download_id) {
            control.store(CONTROL_RUN, Ordering::SeqCst);
        }
    }

    set_status(shared, download_id, DownloadStatus::Paused, snapshot, None)
}

/// Removes any temp artifacts and persists a cancelled queue record.
fn set_cancelled(
    shared: &Shared,
    download_id: DownloadId,
    snapshot: DownloadSnapshot,
    temp_path: &std::path::Path,
) -> Result<(), NetError> {
    remove_file_if_exists(temp_path)?;
    if let TempLayout::Multipart(layout) = &snapshot.temp_layout {
        for part in &layout.parts {
            remove_file_if_exists(&part.path)?;
        }
    }
    set_status(
        shared,
        download_id,
        DownloadStatus::Cancelled,
        DownloadSnapshot {
            progress: snapshot.progress,
            temp_layout: TempLayout::Single,
        },
        None,
    )
}

/// Persists a failed terminal state while keeping resumable temp metadata.
fn set_failed(
    shared: &Shared,
    download_id: DownloadId,
    snapshot: DownloadSnapshot,
    error_message: String,
) -> Result<(), NetError> {
    set_status(
        shared,
        download_id,
        DownloadStatus::Failed,
        snapshot,
        Some(error_message),
    )
}

/// Applies a terminal or transitional status update to the persisted record.
fn set_status(
    shared: &Shared,
    download_id: DownloadId,
    status: DownloadStatus,
    snapshot: DownloadSnapshot,
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
            record.progress = snapshot.progress;
            record.temp_layout = match status {
                DownloadStatus::Completed | DownloadStatus::Cancelled | DownloadStatus::Verifying => {
                    TempLayout::Single
                }
                _ => snapshot.temp_layout,
            };
            record.error = error;
            record.touch();
            record.clone()
        };
        state.runtime.remove(&download_id);

        if let Some(control) = control {
            control.store(CONTROL_RUN, Ordering::SeqCst);
        }

        publish_event(&mut state, QueueEvent::Updated(updated_record));
    }

    save_full_state(shared)
}

/// Copies runtime snapshots into queue records, emits throttled UI events, and
/// periodically coalesces persistence.
fn process_runtime_updates(shared: &Shared, force_persist: bool) -> Result<(), NetError> {
    let mut should_persist = false;
    {
        let mut state = lock_state(shared)?;
        let now = Instant::now();

        let mut running_updates = Vec::new();
        let runtime_ids = state.runtime.keys().copied().collect::<Vec<_>>();

        for download_id in runtime_ids {
            let Some((snapshot, emit_ui)) = ({
                if let Some(runtime) = state.runtime.get_mut(&download_id) {
                    runtime.wake_pending = false;
                    let emit_ui = runtime.ui_dirty
                        && now.duration_since(runtime.last_event_at) >= UI_EVENT_INTERVAL;
                    if emit_ui {
                        runtime.ui_dirty = false;
                        runtime.last_event_at = now;
                    }
                    Some((runtime.snapshot.clone(), emit_ui))
                } else {
                    None
                }
            }) else {
                continue;
            };

            let Some(record) = state.downloads.get_mut(&download_id) else {
                continue;
            };

            record.progress = snapshot.progress;
            record.temp_layout = snapshot.temp_layout;
            record.status = DownloadStatus::Running;
            record.error = None;
            record.touch();

            if emit_ui {
                running_updates.push(record.clone());
            }
        }

        let persist_due =
            force_persist || now.duration_since(state.last_persist_at) >= PERSIST_INTERVAL;
        if persist_due {
            let mut has_dirty = false;
            for runtime in state.runtime.values_mut() {
                if runtime.persist_dirty {
                    runtime.persist_dirty = false;
                    has_dirty = true;
                }
            }

            if has_dirty {
                state.last_persist_at = now;
                should_persist = true;
            }
        }

        for updated_record in running_updates {
            publish_event(&mut state, QueueEvent::Updated(updated_record));
        }
    }

    if should_persist {
        save_full_state(shared)?;
    }

    Ok(())
}

/// Returns the latest in-memory snapshot for a running download, if any.
fn current_progress(shared: &Shared, download_id: DownloadId) -> Option<DownloadSnapshot> {
    let state = lock_state(shared).ok()?;
    let runtime = state.runtime.get(&download_id)?;
    Some(runtime.snapshot.clone())
}
