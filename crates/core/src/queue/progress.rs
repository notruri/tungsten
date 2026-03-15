use std::sync::Weak;
use std::time::Instant;

use tokio::runtime::Handle;
use tracing::{debug, warn};

use crate::error::CoreError;
use crate::model::{DownloadId, DownloadStatus, QueueEvent};
use crate::transfer::TransferUpdate;

use super::{
    COORDINATOR_TICK, PERSIST_INTERVAL, ProgressState, QueueState, Shared, UI_EVENT_INTERVAL,
    lock_state, log_status_change, publish_event, save_full_state,
};

pub(crate) fn spawn_coordinator(
    shared: Weak<Shared>,
    mut coordinator_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
    tokio: Handle,
) {
    tokio.spawn(async move {
        loop {
            match tokio::time::timeout(COORDINATOR_TICK, coordinator_rx.recv()).await {
                Ok(Some(_)) | Err(_) => {
                    let Some(shared) = shared.upgrade() else {
                        return;
                    };

                    if let Err(error) = process_progress_updates(&shared, false) {
                        warn!(error = %error, "progress coordinator failed");
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                }
                Ok(None) => return,
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
        let mut updated_records = Vec::new();
        let update_ids = state.updates.keys().copied().collect::<Vec<_>>();

        for download_id in update_ids {
            let Some((update, emit_ui)) = pull_progress_update(&mut state, download_id, now) else {
                continue;
            };

            let Some(record) = state.downloads.get_mut(&download_id) else {
                continue;
            };

            let status = progress_status(
                &record.status,
                record.progress.downloaded,
                update.progress.downloaded,
            );
            let previous = record.status.clone();
            record.progress = update.progress;
            record.temp_layout = update.temp_layout;
            record.status = status;
            record.error = None;
            record.touch();
            log_status_change(download_id, &previous, &record.status, "progress update");

            if emit_ui {
                updated_records.push(record.to_record());
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

        for record in updated_records {
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

fn progress_status(
    current: &DownloadStatus,
    previous_downloaded: u64,
    next_downloaded: u64,
) -> DownloadStatus {
    if matches!(current, DownloadStatus::Preparing) && next_downloaded <= previous_downloaded {
        DownloadStatus::Preparing
    } else {
        DownloadStatus::Running
    }
}

#[cfg(test)]
mod tests {
    use crate::model::DownloadStatus;

    use super::progress_status;

    #[test]
    fn preparing_stays_preparing_without_new_bytes() {
        assert_eq!(
            progress_status(&DownloadStatus::Preparing, 1024, 1024),
            DownloadStatus::Preparing
        );
    }

    #[test]
    fn preparing_becomes_running_when_bytes_advance() {
        assert_eq!(
            progress_status(&DownloadStatus::Preparing, 1024, 2048),
            DownloadStatus::Running
        );
    }
}
