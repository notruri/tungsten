use std::sync::atomic::Ordering;
use std::sync::mpsc;

use crate::error::NetError;
use crate::model::{
    DownloadId, DownloadRecord, DownloadRequest, DownloadStatus, ProgressSnapshot, QueueEvent,
};
use crate::store::PersistedDownload;
use crate::transfer::TempLayout;

use super::files::{
    apply_inferred_destination_file_name, remove_file_if_exists, remove_temp_layout_files,
    resolve_destination, temp_path_for,
};
use super::persist::{lock_state, publish_event, save_full_state};
use super::{CONTROL_CANCEL, CONTROL_PAUSE, CONTROL_RUN, QueueService};

impl QueueService {
    pub fn enqueue(&self, mut request: DownloadRequest) -> Result<DownloadId, NetError> {
        request.validate()?;

        let probe = self.shared.transfer.probe(&request).unwrap_or_default();
        apply_inferred_destination_file_name(&mut request, probe.file_name.as_deref());

        let mut state = lock_state(&self.shared)?;
        let destination =
            resolve_destination(&request.destination, &state.downloads, &request.conflict);
        request.destination = destination.clone();

        let download_id = DownloadId(state.next_id.max(1));
        state.next_id = download_id.0 + 1;

        let now = DownloadRecord::now_epoch();
        let record = PersistedDownload {
            id: download_id,
            request,
            temp_path: temp_path_for(&destination, download_id),
            temp_layout: TempLayout::Single,
            supports_resume: probe.accept_ranges,
            status: DownloadStatus::Queued,
            progress: ProgressSnapshot {
                downloaded: 0,
                total: probe.total_size,
                speed_bps: None,
                eta_seconds: None,
            },
            error: None,
            etag: probe.etag,
            last_modified: probe.last_modified,
            created_at: now,
            updated_at: now,
        };

        state
            .controls
            .insert(download_id, std::sync::Arc::new(std::sync::atomic::AtomicU8::new(CONTROL_RUN)));
        state.downloads.insert(download_id, record.clone());
        publish_event(&mut state, QueueEvent::Added(record.to_record()));
        drop(state);

        save_full_state(&self.shared)?;
        Ok(download_id)
    }

    pub fn pause(&self, download_id: DownloadId) -> Result<(), NetError> {
        let mut should_persist = false;
        {
            let mut state = lock_state(&self.shared)?;
            let status = state
                .downloads
                .get(&download_id)
                .map(|record| record.status.clone())
                .ok_or(NetError::DownloadNotFound(download_id))?;

            match status {
                DownloadStatus::Queued => {
                    let mut updated = None;
                    if let Some(record) = state.downloads.get_mut(&download_id) {
                        record.status = DownloadStatus::Paused;
                        record.touch();
                        updated = Some(record.to_record());
                        should_persist = true;
                    }
                    if let Some(record) = updated {
                        publish_event(&mut state, QueueEvent::Updated(record));
                    }
                }
                DownloadStatus::Running => {
                    if let Some(control) = state.controls.get(&download_id) {
                        control.store(CONTROL_PAUSE, Ordering::SeqCst);
                    }
                }
                _ => {}
            }
        }

        if should_persist {
            save_full_state(&self.shared)?;
        }

        Ok(())
    }

    pub fn resume(&self, download_id: DownloadId) -> Result<(), NetError> {
        {
            let mut state = lock_state(&self.shared)?;
            let mut updated = None;
            let record = state
                .downloads
                .get_mut(&download_id)
                .ok_or(NetError::DownloadNotFound(download_id))?;

            if matches!(
                record.status,
                DownloadStatus::Paused | DownloadStatus::Failed | DownloadStatus::Cancelled
            ) {
                record.status = DownloadStatus::Queued;
                record.error = None;
                record.touch();
                updated = Some(record.to_record());
            }

            if let Some(control) = state.controls.get(&download_id) {
                control.store(CONTROL_RUN, Ordering::SeqCst);
            }

            if let Some(record) = updated {
                publish_event(&mut state, QueueEvent::Updated(record));
            }
        }

        save_full_state(&self.shared)?;
        Ok(())
    }

    pub fn cancel(&self, download_id: DownloadId) -> Result<(), NetError> {
        let mut temp_to_remove = None;
        let mut layout_to_remove = None;
        let mut should_persist = false;
        {
            let mut state = lock_state(&self.shared)?;
            let mut updated = None;
            let record = state
                .downloads
                .get_mut(&download_id)
                .ok_or(NetError::DownloadNotFound(download_id))?;

            match record.status {
                DownloadStatus::Running => {
                    if let Some(control) = state.controls.get(&download_id) {
                        control.store(CONTROL_CANCEL, Ordering::SeqCst);
                    }
                }
                DownloadStatus::Queued | DownloadStatus::Paused | DownloadStatus::Failed => {
                    record.status = DownloadStatus::Cancelled;
                    record.touch();
                    record.error = None;
                    temp_to_remove = Some(record.temp_path.clone());
                    layout_to_remove = Some(record.temp_layout.clone());
                    record.temp_layout = TempLayout::Single;
                    updated = Some(record.to_record());
                    should_persist = true;
                }
                _ => {}
            }

            if let Some(record) = updated {
                publish_event(&mut state, QueueEvent::Updated(record));
            }
        }

        if let Some(path) = temp_to_remove {
            remove_file_if_exists(&path)?;
        }
        if let Some(layout) = layout_to_remove {
            remove_temp_layout_files(&layout)?;
        }
        if should_persist {
            save_full_state(&self.shared)?;
        }

        Ok(())
    }

    pub fn delete(&self, download_id: DownloadId) -> Result<(), NetError> {
        let (temp_to_remove, layout_to_remove) = {
            let mut state = lock_state(&self.shared)?;
            let record = state
                .downloads
                .get(&download_id)
                .ok_or(NetError::DownloadNotFound(download_id))?;

            if matches!(
                record.status,
                DownloadStatus::Running | DownloadStatus::Verifying
            ) {
                return Err(NetError::InvalidRequest(
                    "cannot delete running download; cancel first".to_string(),
                ));
            }

            let temp_path = record.temp_path.clone();
            let temp_layout = record.temp_layout.clone();
            state.downloads.remove(&download_id);
            state.controls.remove(&download_id);
            state.runtime.remove(&download_id);
            publish_event(&mut state, QueueEvent::Removed(download_id));
            (temp_path, temp_layout)
        };

        remove_file_if_exists(&temp_to_remove)?;
        remove_temp_layout_files(&layout_to_remove)?;
        save_full_state(&self.shared)?;
        Ok(())
    }

    pub fn retry(&self, download_id: DownloadId) -> Result<(), NetError> {
        {
            let mut state = lock_state(&self.shared)?;
            let mut updated = None;
            let record = state
                .downloads
                .get_mut(&download_id)
                .ok_or(NetError::DownloadNotFound(download_id))?;

            if matches!(
                record.status,
                DownloadStatus::Failed | DownloadStatus::Cancelled
            ) {
                record.status = DownloadStatus::Queued;
                record.error = None;
                record.touch();
                updated = Some(record.to_record());
            }

            if let Some(control) = state.controls.get(&download_id) {
                control.store(CONTROL_RUN, Ordering::SeqCst);
            }

            if let Some(record) = updated {
                publish_event(&mut state, QueueEvent::Updated(record));
            }
        }

        save_full_state(&self.shared)?;
        Ok(())
    }

    pub fn set_max_parallel(&self, max_parallel: usize) -> Result<(), NetError> {
        {
            let mut state = lock_state(&self.shared)?;
            state.max_parallel = max_parallel.max(1);
        }

        save_full_state(&self.shared)?;
        Ok(())
    }

    pub fn snapshot(&self) -> Result<Vec<DownloadRecord>, NetError> {
        let state = lock_state(&self.shared)?;
        let mut records = state
            .downloads
            .values()
            .map(PersistedDownload::to_record)
            .collect::<Vec<_>>();
        records.sort_by_key(|record| record.id.0);
        Ok(records)
    }

    pub fn first_download_id(&self) -> Result<Option<DownloadId>, NetError> {
        let records = self.snapshot()?;
        Ok(records.first().map(|record| record.id))
    }

    pub fn subscribe(&self) -> Result<mpsc::Receiver<QueueEvent>, NetError> {
        let (tx, rx) = mpsc::channel();
        let mut state = lock_state(&self.shared)?;
        state.subscribers.push(tx);
        Ok(rx)
    }
}
