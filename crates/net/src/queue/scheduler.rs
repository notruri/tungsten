use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

use crate::error::NetError;
use crate::model::{DownloadId, DownloadStatus, QueueEvent};
use crate::transfer::TransferUpdate;

use super::lifecycle::run_download_worker;
use super::persist::{lock_state, publish_event, save_full_state};
use super::{CONTROL_RUN, RuntimeDownloadState, Shared};

pub(crate) fn spawn_scheduler(shared: Arc<Shared>) {
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
        let mut initial_update = None;
        if let Some(record) = state.downloads.get_mut(&download_id) {
            record.status = DownloadStatus::Running;
            record.error = None;
            record.touch();
            initial_update = Some(TransferUpdate {
                progress: record.progress.clone(),
                temp_layout: record.temp_layout.clone(),
            });
            updated = Some(record.to_record());
            picked_ids.push(download_id);
        }

        if let Some(update) = initial_update {
            state
                .runtime
                .entry(download_id)
                .or_insert_with(|| RuntimeDownloadState::new(update));
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
