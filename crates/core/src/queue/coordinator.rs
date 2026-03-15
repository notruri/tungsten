use std::sync::Weak;
use std::time::Instant;

use tokio::runtime::Handle;
use tracing::{debug, warn};

use crate::error::CoreError;
use crate::model::{DownloadId, DownloadStatus, QueueEvent};
use crate::transfer::{TempLayout, TransferUpdate};

use super::{
    COORDINATOR_TICK, PERSIST_INTERVAL, ProgressState, Shared, lock_coordinator, lock_state,
    log_status_change, publish_event, save_full_state,
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
        let mut coordinator = lock_coordinator(shared)?;
        let progress = coordinator
            .updates
            .entry(download_id)
            .or_insert_with(|| ProgressState::new(update.clone()));
        progress.update = update;
        progress.ui_dirty = true;

        if progress.queued {
            false
        } else {
            progress.queued = true;
            coordinator.dirty_ids.push_back(download_id);
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
    let coordinator = lock_coordinator(shared).ok()?;
    let progress = coordinator.updates.get(&download_id)?;
    Some(progress.update.clone())
}

fn process_progress_updates(shared: &Shared, force_persist: bool) -> Result<(), CoreError> {
    let now = Instant::now();
    let pending = pull_progress_updates(shared, now)?;
    let mut applied_updates = false;

    if !pending.is_empty() {
        let mut state = lock_state(shared)?;
        let mut updated_records = Vec::new();

        for (download_id, update, emit_ui) in pending {
            let Some(record) = state.downloads.get_mut(&download_id) else {
                continue;
            };

            let status = progress_status(&record.status, record.progress.downloaded, &update);
            let previous = record.status.clone();
            record.progress = update.progress;
            record.temp_layout = update.temp_layout;
            record.status = status;
            record.error = None;
            record.touch();
            log_status_change(download_id, &previous, &record.status, "progress update");
            applied_updates = true;

            if emit_ui {
                updated_records.push(record.to_record());
            }
        }

        for record in updated_records {
            publish_event(&mut state, QueueEvent::Updated(record));
        }
    }

    if applied_updates {
        let mut coordinator = lock_coordinator(shared)?;
        coordinator.has_dirty_persist = true;
    }

    if begin_persist_if_due(shared, force_persist, now)? {
        save_full_state(shared)?;
    }

    Ok(())
}

fn pull_progress_updates(
    shared: &Shared,
    now: Instant,
) -> Result<Vec<(DownloadId, TransferUpdate, bool)>, CoreError> {
    let mut coordinator = lock_coordinator(shared)?;
    let dirty_ids = coordinator.dirty_ids.drain(..).collect::<Vec<_>>();
    let mut pending = Vec::with_capacity(dirty_ids.len());

    for download_id in dirty_ids {
        let Some(progress) = coordinator.updates.get_mut(&download_id) else {
            continue;
        };

        progress.queued = false;
        let emit_ui =
            progress.ui_dirty && now.duration_since(progress.last_event_at) >= COORDINATOR_TICK;
        if emit_ui {
            progress.ui_dirty = false;
            progress.last_event_at = now;
        }

        pending.push((download_id, progress.update.clone(), emit_ui));
    }

    Ok(pending)
}

fn begin_persist_if_due(
    shared: &Shared,
    force_persist: bool,
    now: Instant,
) -> Result<bool, CoreError> {
    let mut coordinator = lock_coordinator(shared)?;
    let persist_due =
        force_persist || now.duration_since(coordinator.last_persist_at) >= PERSIST_INTERVAL;
    if persist_due && coordinator.has_dirty_persist {
        coordinator.has_dirty_persist = false;
        coordinator.last_persist_at = now;
        return Ok(true);
    }

    Ok(false)
}

fn progress_status(
    current: &DownloadStatus,
    previous_downloaded: u64,
    update: &TransferUpdate,
) -> DownloadStatus {
    let next_downloaded = update.progress.downloaded;
    let is_finalizing = matches!(update.temp_layout, TempLayout::Multipart(_))
        && update.progress.total == Some(next_downloaded);

    if is_finalizing {
        DownloadStatus::Finalizing
    } else if matches!(current, DownloadStatus::Preparing) && next_downloaded <= previous_downloaded
    {
        DownloadStatus::Preparing
    } else {
        DownloadStatus::Running
    }
}

#[cfg(test)]
mod tests {
    use crate::model::DownloadStatus;
    use crate::transfer::{TempLayout, TransferUpdate};

    use super::progress_status;

    #[test]
    fn preparing_stays_preparing_without_new_bytes() {
        assert_eq!(
            progress_status(
                &DownloadStatus::Preparing,
                1024,
                &TransferUpdate {
                    progress: crate::model::ProgressSnapshot {
                        downloaded: 1024,
                        total: Some(2048),
                        speed_bps: None,
                        eta_seconds: None,
                    },
                    temp_layout: TempLayout::Single,
                }
            ),
            DownloadStatus::Preparing
        );
    }

    #[test]
    fn preparing_becomes_running_when_bytes_advance() {
        assert_eq!(
            progress_status(
                &DownloadStatus::Preparing,
                1024,
                &TransferUpdate {
                    progress: crate::model::ProgressSnapshot {
                        downloaded: 2048,
                        total: Some(4096),
                        speed_bps: None,
                        eta_seconds: None,
                    },
                    temp_layout: TempLayout::Single,
                }
            ),
            DownloadStatus::Running
        );
    }

    #[test]
    fn multipart_full_progress_becomes_finalizing() {
        assert_eq!(
            progress_status(
                &DownloadStatus::Running,
                1024,
                &TransferUpdate {
                    progress: crate::model::ProgressSnapshot {
                        downloaded: 4096,
                        total: Some(4096),
                        speed_bps: None,
                        eta_seconds: None,
                    },
                    temp_layout: TempLayout::Multipart(crate::transfer::MultipartState {
                        total_size: 4096,
                        parts: Vec::new(),
                    }),
                }
            ),
            DownloadStatus::Finalizing
        );
    }
}
