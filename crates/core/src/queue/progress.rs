use std::sync::{Weak, mpsc};
use std::thread;
use std::time::Instant;

use tracing::{debug, warn};

use crate::error::CoreError;
use crate::model::{DownloadId, DownloadStatus, QueueEvent};
use crate::transfer::TransferUpdate;

use super::{
    COORDINATOR_TICK, PERSIST_INTERVAL, ProgressState, QueueState, Shared, UI_EVENT_INTERVAL,
    lock_state, publish_event, save_full_state,
};

pub(crate) fn spawn_coordinator(shared: Weak<Shared>, coordinator_rx: mpsc::Receiver<()>) {
    thread::spawn(move || {
        loop {
            match coordinator_rx.recv_timeout(COORDINATOR_TICK) {
                Ok(_) | Err(mpsc::RecvTimeoutError::Timeout) => {
                    let Some(shared) = shared.upgrade() else {
                        return;
                    };

                    if let Err(error) = process_progress_updates(&shared, false) {
                        warn!(error = %error, "progress coordinator failed");
                        thread::sleep(std::time::Duration::from_millis(50));
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => return,
            }
        }
    });
}

pub(crate) fn flush_progress_and_persist(shared: &Shared) -> Result<(), CoreError> {
    debug!("flushing progress updates before shutdown");
    process_progress_updates(shared, true)?;
    save_full_state(shared)
}

pub(crate) fn capture_progress_update(
    shared: &Shared,
    download_id: DownloadId,
    update: TransferUpdate,
) -> Result<(), CoreError> {
    let should_wake = {
        let mut state = lock_state(shared)?;
        let progress = state
            .updates
            .entry(download_id)
            .or_insert_with(|| ProgressState::new(update.clone()));
        progress.update = update;
        progress.ui_dirty = true;
        progress.persist_dirty = true;

        if progress.wake_pending {
            false
        } else {
            progress.wake_pending = true;
            true
        }
    };

    if should_wake && shared.coordinator_tx.send(()).is_err() {
        warn!(
            download_id = %download_id,
            "failed to wake progress coordinator"
        );
    }

    Ok(())
}

pub(crate) fn current_progress_update(
    shared: &Shared,
    download_id: DownloadId,
) -> Option<TransferUpdate> {
    let state = lock_state(shared).ok()?;
    let progress = state.updates.get(&download_id)?;
    Some(progress.update.clone())
}

fn process_progress_updates(shared: &Shared, force_persist: bool) -> Result<(), CoreError> {
    let mut should_persist = false;
    {
        let mut state = lock_state(shared)?;
        let now = Instant::now();
        let mut running_updates = Vec::new();
        let update_ids = state.updates.keys().copied().collect::<Vec<_>>();

        for download_id in update_ids {
            let Some((update, emit_ui)) = pull_progress_update(&mut state, download_id, now) else {
                continue;
            };

            let Some(record) = state.downloads.get_mut(&download_id) else {
                continue;
            };

            record.progress = update.progress;
            record.temp_layout = update.temp_layout;
            record.status = DownloadStatus::Running;
            record.error = None;
            record.touch();

            if emit_ui {
                running_updates.push(record.to_record());
            }
        }

        let persist_due =
            force_persist || now.duration_since(state.last_persist_at) >= PERSIST_INTERVAL;
        if persist_due {
            let mut has_dirty = false;
            for progress in state.updates.values_mut() {
                if progress.persist_dirty {
                    progress.persist_dirty = false;
                    has_dirty = true;
                }
            }

            if has_dirty {
                state.last_persist_at = now;
                should_persist = true;
            }
        }

        for record in running_updates {
            publish_event(&mut state, QueueEvent::Updated(record));
        }
    }

    if should_persist {
        save_full_state(shared)?;
    }

    Ok(())
}

fn pull_progress_update(
    state: &mut QueueState,
    download_id: DownloadId,
    now: Instant,
) -> Option<(TransferUpdate, bool)> {
    let progress = state.updates.get_mut(&download_id)?;
    progress.wake_pending = false;
    let emit_ui =
        progress.ui_dirty && now.duration_since(progress.last_event_at) >= UI_EVENT_INTERVAL;
    if emit_ui {
        progress.ui_dirty = false;
        progress.last_event_at = now;
    }
    Some((progress.update.clone(), emit_ui))
}
