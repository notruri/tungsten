use std::sync::Weak;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::runtime::Handle;
use tracing::{debug, error, warn};

use crate::error::CoreError;
use crate::model::{DownloadId, DownloadStatus, QueueEvent};
use crate::transfer::TransferUpdate;

use super::lifecycle::run_download_worker;
use super::{
    CONTROL_RUN, ProgressState, Shared, lock_state, log_status_change, publish_event,
    save_full_state,
};

pub(crate) fn spawn_scheduler(shared: Weak<Shared>, tokio: Handle) {
    let scheduler_handle = tokio.clone();
    scheduler_handle.spawn(async move {
        loop {
            let Some(shared) = shared.upgrade() else {
                return;
            };

            let launch_ids = match pick_next_downloads(&shared) {
                Ok(ids) => ids,
                Err(error) => {
                    warn!(error = %error, "scheduler lock failed");
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    continue;
                }
            };

            if !launch_ids.is_empty() {
                debug!(count = launch_ids.len(), "launching queued downloads");
                if let Err(error) = save_full_state(&shared) {
                    warn!(error = %error, "failed to save state before launch");
                }
            }

            for download_id in launch_ids {
                let shared_for_worker = shared.clone();
                let tokio = tokio.clone();
                tokio.spawn(async move {
                    if let Err(error) = run_download_worker(shared_for_worker, download_id).await {
                        error!(download_id = %download_id, error = %error, "worker failed");
                    }
                });
            }

            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    });
}

fn pick_next_downloads(shared: &Shared) -> Result<Vec<DownloadId>, CoreError> {
    let mut state = lock_state(shared)?;
    let running_count = state
        .downloads
        .values()
        .filter(|record| {
            matches!(
                record.status,
                DownloadStatus::Preparing
                    | DownloadStatus::Running
                    | DownloadStatus::Finalizing
                    | DownloadStatus::Verifying
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
        let mut initial_update = None;
        if let Some(record) = state.downloads.get_mut(&download_id) {
            let previous = record.status.clone();
            record.status = DownloadStatus::Preparing;
            record.error = None;
            record.touch();
            log_status_change(
                download_id,
                &previous,
                &record.status,
                "scheduler picked download",
            );
            initial_update = Some(TransferUpdate {
                progress: record.progress.clone(),
                temp_layout: record.temp_layout.clone(),
            });
            updated = Some(record.to_record());
            picked_ids.push(download_id);
        }

        if let Some(update) = initial_update {
            state
                .updates
                .entry(download_id)
                .or_insert_with(|| ProgressState::new(update));
        }

        if let Some(record) = updated {
            publish_event(&mut state, QueueEvent::Updated(record));
        }

        if let Some(control) = state.controls.get(&download_id) {
            control.store(CONTROL_RUN, Ordering::SeqCst);
        }
    }

    Ok(picked_ids)
}
