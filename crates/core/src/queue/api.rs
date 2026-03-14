use std::sync::atomic::Ordering;
use std::sync::mpsc;

use chrono::Utc;

use crate::error::CoreError;
use crate::model::{
    DownloadId, DownloadRecord, DownloadRequest, DownloadStatus, ProgressSnapshot, QueueEvent,
};
use crate::store::PersistedDownload;
use crate::transfer::TempLayout;

use super::files::{fallback_destination, temp_path_for};
use super::lifecycle::spawn_enqueue_resolution;
use super::{
    CONTROL_CANCEL, CONTROL_PAUSE, CONTROL_RUN, QueueService, lock_state, publish_event,
    save_full_state,
};

impl QueueService {
    pub fn enqueue(&self, request: DownloadRequest) -> Result<DownloadId, CoreError> {
        request.validate()?;

        let mut state = lock_state(&self.shared)?;
        let download_id = DownloadId(state.next_id.max(1));
        state.next_id = download_id.0 + 1;

        let unresolved_path = fallback_destination(&request.destination, &state.fallback_filename);
        let now = Utc::now();
        let record = PersistedDownload {
            id: download_id,
            request,
            destination: None,
            loaded_from_store: false,
            temp_path: temp_path_for(&unresolved_path, download_id),
            temp_layout: TempLayout::Single,
            supports_resume: false,
            status: DownloadStatus::Queued,
            progress: ProgressSnapshot {
                downloaded: 0,
                total: None,
                speed_bps: None,
                eta_seconds: None,
            },
            error: None,
            etag: None,
            last_modified: None,
            created_at: now,
            updated_at: now,
        };

        state.controls.insert(
            download_id,
            std::sync::Arc::new(std::sync::atomic::AtomicU8::new(CONTROL_RUN)),
        );
        state.downloads.insert(download_id, record.clone());
        publish_event(&mut state, QueueEvent::Added(record.to_record()));
        drop(state);

        save_full_state(&self.shared)?;
        spawn_enqueue_resolution(self.shared.clone(), download_id);
        Ok(download_id)
    }

    pub fn pause(&self, download_id: DownloadId) -> Result<(), CoreError> {
        let mut should_persist = false;
        {
            let mut state = lock_state(&self.shared)?;
            let status = state
                .downloads
                .get(&download_id)
                .map(|record| record.status.clone())
                .ok_or(CoreError::DownloadNotFound(download_id))?;

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

    pub fn resume(&self, download_id: DownloadId) -> Result<(), CoreError> {
        {
            let mut state = lock_state(&self.shared)?;
            let mut updated = None;
            let record = state
                .downloads
                .get_mut(&download_id)
                .ok_or(CoreError::DownloadNotFound(download_id))?;

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

    pub fn retry(&self, download_id: DownloadId) -> Result<(), CoreError> {
        {
            let mut state = lock_state(&self.shared)?;
            let mut updated = None;
            let record = state
                .downloads
                .get_mut(&download_id)
                .ok_or(CoreError::DownloadNotFound(download_id))?;

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

    pub fn cancel(&self, download_id: DownloadId) -> Result<(), CoreError> {
        let mut should_persist = false;
        {
            let mut state = lock_state(&self.shared)?;
            let mut updated = None;
            let record = state
                .downloads
                .get_mut(&download_id)
                .ok_or(CoreError::DownloadNotFound(download_id))?;

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

        if should_persist {
            save_full_state(&self.shared)?;
        }

        Ok(())
    }

    pub fn delete(&self, download_id: DownloadId) -> Result<(), CoreError> {
        {
            let mut state = lock_state(&self.shared)?;
            let record = state
                .downloads
                .get(&download_id)
                .ok_or(CoreError::DownloadNotFound(download_id))?;

            if matches!(
                record.status,
                DownloadStatus::Running | DownloadStatus::Verifying
            ) {
                return Err(CoreError::InvalidRequest(
                    "cannot delete running download; cancel first".to_string(),
                ));
            }

            state.downloads.remove(&download_id);
            state.controls.remove(&download_id);
            publish_event(&mut state, QueueEvent::Removed(download_id));
        }

        save_full_state(&self.shared)?;
        Ok(())
    }

    pub fn set_connections(&self, connections: usize) -> Result<(), CoreError> {
        self.shared.transfer.set_connections(connections.max(1));
        Ok(())
    }

    pub fn set_fallback_filename(
        &self,
        fallback_filename: impl Into<String>,
    ) -> Result<(), CoreError> {
        let fallback_filename = fallback_filename.into();
        let fallback_filename = fallback_filename.trim().to_string();
        if fallback_filename.is_empty() {
            return Err(CoreError::InvalidRequest(
                "fallback filename must not be empty".to_string(),
            ));
        }
        if fallback_filename.contains('/') || fallback_filename.contains('\\') {
            return Err(CoreError::InvalidRequest(
                "fallback filename must not contain path separators".to_string(),
            ));
        }

        let mut state = lock_state(&self.shared)?;
        state.fallback_filename = fallback_filename;
        Ok(())
    }

    pub fn set_max_parallel(&self, max_parallel: usize) -> Result<(), CoreError> {
        let mut state = lock_state(&self.shared)?;
        state.max_parallel = max_parallel.max(1);
        Ok(())
    }

    pub fn snapshot(&self) -> Result<Vec<DownloadRecord>, CoreError> {
        let state = lock_state(&self.shared)?;
        let mut records = state
            .downloads
            .values()
            .map(PersistedDownload::to_record)
            .collect::<Vec<_>>();
        records.sort_by_key(|record| record.id.0);
        Ok(records)
    }

    pub fn first_download_id(&self) -> Result<Option<DownloadId>, CoreError> {
        let records = self.snapshot()?;
        Ok(records.first().map(|record| record.id))
    }

    pub fn subscribe(&self) -> Result<mpsc::Receiver<QueueEvent>, CoreError> {
        let (tx, rx) = mpsc::channel();
        let mut state = lock_state(&self.shared)?;
        state.subscribers.push(tx);
        Ok(rx)
    }

    pub fn save(&self) -> Result<(), CoreError> {
        save_full_state(&self.shared)
    }
}
