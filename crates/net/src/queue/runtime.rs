use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Instant;

use crate::error::NetError;
use crate::model::{DownloadId, DownloadStatus, QueueEvent};
use crate::transfer::TransferUpdate;

use super::persist::{lock_state, publish_event, save_full_state};
use super::{
    COORDINATOR_TICK, PERSIST_INTERVAL, QueueState, RuntimeDownloadState, Shared, UI_EVENT_INTERVAL,
};

pub(crate) fn spawn_coordinator(shared: Arc<Shared>, coordinator_rx: mpsc::Receiver<()>) {
    thread::spawn(move || {
        loop {
            match coordinator_rx.recv_timeout(COORDINATOR_TICK) {
                Ok(_) | Err(mpsc::RecvTimeoutError::Timeout) => {
                    if let Err(error) = process_runtime_updates(&shared, false) {
                        eprintln!("runtime coordinator failed: {error}");
                        thread::sleep(std::time::Duration::from_millis(50));
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => return,
            }
        }
    });
}

pub(crate) fn flush_runtime_and_persist(shared: &Shared) -> Result<(), NetError> {
    process_runtime_updates(shared, true)?;
    save_full_state(shared)
}

pub(crate) fn capture_runtime_update(
    shared: &Shared,
    download_id: DownloadId,
    update: TransferUpdate,
) -> Result<(), NetError> {
    let should_wake = {
        let mut state = lock_state(shared)?;
        let runtime = state
            .runtime
            .entry(download_id)
            .or_insert_with(|| RuntimeDownloadState::new(update.clone()));
        runtime.update = update;
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

pub(crate) fn current_update(shared: &Shared, download_id: DownloadId) -> Option<TransferUpdate> {
    let state = lock_state(shared).ok()?;
    let runtime = state.runtime.get(&download_id)?;
    Some(runtime.update.clone())
}

fn process_runtime_updates(shared: &Shared, force_persist: bool) -> Result<(), NetError> {
    let mut should_persist = false;
    {
        let mut state = lock_state(shared)?;
        let now = Instant::now();
        let mut running_updates = Vec::new();
        let runtime_ids = state.runtime.keys().copied().collect::<Vec<_>>();

        for download_id in runtime_ids {
            let Some((update, emit_ui)) = pull_runtime_update(&mut state, download_id, now) else {
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

        for record in running_updates {
            publish_event(&mut state, QueueEvent::Updated(record));
        }
    }

    if should_persist {
        save_full_state(shared)?;
    }

    Ok(())
}

fn pull_runtime_update(
    state: &mut QueueState,
    download_id: DownloadId,
    now: Instant,
) -> Option<(TransferUpdate, bool)> {
    let runtime = state.runtime.get_mut(&download_id)?;
    runtime.wake_pending = false;
    let emit_ui =
        runtime.ui_dirty && now.duration_since(runtime.last_event_at) >= UI_EVENT_INTERVAL;
    if emit_ui {
        runtime.ui_dirty = false;
        runtime.last_event_at = now;
    }
    Some((runtime.update.clone(), emit_ui))
}
