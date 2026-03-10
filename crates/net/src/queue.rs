mod files;
mod runtime;
mod shared;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use crate::backend::DownloadBackend;
use crate::error::NetError;
use crate::state::StateStore;
use crate::types::{
    DownloadId, DownloadRecord, DownloadRequest, DownloadSnapshot, DownloadStatus,
    ProgressSnapshot, QueueEvent, TempLayout,
};

use self::files::{
    apply_inferred_destination_file_name, remove_file_if_exists, remove_temp_layout_files,
    resolve_destination, temp_path_for,
};
use self::runtime::{flush_runtime_and_persist, spawn_coordinator, spawn_scheduler};
use self::shared::{build_state_from_persisted, lock_state, publish_event, save_full_state};

const CONTROL_RUN: u8 = 0;
const CONTROL_PAUSE: u8 = 1;
const CONTROL_CANCEL: u8 = 2;
const DEFAULT_DOWNLOAD_FILE_NAME: &str = "download.bin";
const UI_EVENT_INTERVAL: Duration = Duration::from_millis(33);
const PERSIST_INTERVAL: Duration = Duration::from_secs(3);
const COORDINATOR_TICK: Duration = Duration::from_millis(16);

#[derive(Clone)]
/// High-level download queue API used by the GUI and tests.
pub struct QueueService {
    pub(super) shared: Arc<Shared>,
}

/// Shared internals owned by queue service clones and background threads.
pub(super) struct Shared {
    pub(super) state: Mutex<QueueState>,
    pub(super) backend: Arc<dyn DownloadBackend>,
    pub(super) store: Arc<dyn StateStore>,
    pub(super) coordinator_tx: mpsc::Sender<()>,
}

/// Mutable queue state guarded by the queue mutex.
pub(super) struct QueueState {
    pub(super) next_id: u64,
    pub(super) max_parallel: usize,
    pub(super) downloads: HashMap<DownloadId, DownloadRecord>,
    pub(super) controls: HashMap<DownloadId, Arc<AtomicU8>>,
    pub(super) runtime: HashMap<DownloadId, RuntimeDownloadState>,
    pub(super) last_persist_at: Instant,
    pub(super) subscribers: Vec<mpsc::Sender<QueueEvent>>,
}

/// Runtime-only snapshot state for an actively running download.
pub(super) struct RuntimeDownloadState {
    pub(super) snapshot: DownloadSnapshot,
    pub(super) ui_dirty: bool,
    pub(super) persist_dirty: bool,
    pub(super) wake_pending: bool,
    pub(super) last_event_at: Instant,
}

impl RuntimeDownloadState {
    /// Creates the runtime state wrapper for a newly running download.
    pub(super) fn new(snapshot: DownloadSnapshot) -> Self {
        Self {
            snapshot,
            ui_dirty: false,
            persist_dirty: false,
            wake_pending: false,
            last_event_at: Instant::now(),
        }
    }
}

impl QueueService {
    /// Builds a queue service, restores persisted records, and starts the
    /// scheduler and coordinator background threads.
    pub fn new(
        backend: Arc<dyn DownloadBackend>,
        store: Arc<dyn StateStore>,
        max_parallel: usize,
    ) -> Result<Self, NetError> {
        let persisted = store.load_state()?;
        let normalized_max = max_parallel.max(1);
        let (downloads, controls, next_id) = build_state_from_persisted(persisted);
        let (coordinator_tx, coordinator_rx) = mpsc::channel();

        let shared = Arc::new(Shared {
            state: Mutex::new(QueueState {
                next_id,
                max_parallel: normalized_max,
                downloads,
                controls,
                runtime: HashMap::new(),
                last_persist_at: Instant::now(),
                subscribers: Vec::new(),
            }),
            backend,
            store,
            coordinator_tx,
        });

        let service = Self {
            shared: Arc::clone(&shared),
        };

        save_full_state(&service.shared)?;
        spawn_coordinator(Arc::clone(&shared), coordinator_rx);
        spawn_scheduler(shared);

        Ok(service)
    }

    /// Adds a new request to the queue and persists its initial record.
    pub fn enqueue(&self, mut request: DownloadRequest) -> Result<DownloadId, NetError> {
        request.validate()?;

        let probe = self.shared.backend.probe(&request).unwrap_or_default();
        apply_inferred_destination_file_name(&mut request, probe.file_name.as_deref());

        let mut state = lock_state(&self.shared)?;
        let destination =
            resolve_destination(&request.destination, &state.downloads, &request.conflict);
        request.destination = destination.clone();

        let download_id = DownloadId(state.next_id.max(1));
        state.next_id = download_id.0 + 1;

        let now = DownloadRecord::now_epoch();
        let record = DownloadRecord {
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
            .insert(download_id, Arc::new(AtomicU8::new(CONTROL_RUN)));
        state.downloads.insert(download_id, record.clone());
        publish_event(&mut state, QueueEvent::Added(record));
        drop(state);

        save_full_state(&self.shared)?;
        Ok(download_id)
    }

    /// Requests that a queued or running download transition into `Paused`.
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
                        updated = Some(record.clone());
                        should_persist = true;
                    }
                    if let Some(updated_record) = updated {
                        publish_event(&mut state, QueueEvent::Updated(updated_record));
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

    /// Moves a paused, failed, or cancelled download back into the queue.
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
                updated = Some(record.clone());
            }

            if let Some(control) = state.controls.get(&download_id) {
                control.store(CONTROL_RUN, Ordering::SeqCst);
            }

            if let Some(updated_record) = updated {
                publish_event(&mut state, QueueEvent::Updated(updated_record));
            }
        }

        save_full_state(&self.shared)?;
        Ok(())
    }

    /// Cancels a download and removes any on-disk temp artifacts.
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
                    updated = Some(record.clone());
                    should_persist = true;
                }
                _ => {}
            }

            if let Some(updated_record) = updated {
                publish_event(&mut state, QueueEvent::Updated(updated_record));
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

    /// Removes a non-running download from the queue and deletes temp files.
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

    /// Re-queues a failed or cancelled download without creating a new record.
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
                updated = Some(record.clone());
            }

            if let Some(control) = state.controls.get(&download_id) {
                control.store(CONTROL_RUN, Ordering::SeqCst);
            }

            if let Some(updated_record) = updated {
                publish_event(&mut state, QueueEvent::Updated(updated_record));
            }
        }

        save_full_state(&self.shared)?;
        Ok(())
    }

    /// Updates the queue-level parallel download limit.
    pub fn set_max_parallel(&self, max_parallel: usize) -> Result<(), NetError> {
        {
            let mut state = lock_state(&self.shared)?;
            state.max_parallel = max_parallel.max(1);
        }

        save_full_state(&self.shared)?;
        Ok(())
    }

    /// Returns a sorted snapshot of the persisted download records.
    pub fn snapshot(&self) -> Result<Vec<DownloadRecord>, NetError> {
        let state = lock_state(&self.shared)?;
        let mut records = state.downloads.values().cloned().collect::<Vec<_>>();
        records.sort_by_key(|record| record.id.0);
        Ok(records)
    }

    /// Convenience helper used by tests to locate the first queue record.
    pub fn first_download_id(&self) -> Result<Option<DownloadId>, NetError> {
        let records = self.snapshot()?;
        Ok(records.first().map(|record| record.id))
    }

    /// Registers a subscriber for queue events emitted by the coordinator.
    pub fn subscribe(&self) -> Result<mpsc::Receiver<QueueEvent>, NetError> {
        let (tx, rx) = mpsc::channel();
        let mut state = lock_state(&self.shared)?;
        state.subscribers.push(tx);
        Ok(rx)
    }
}

impl Drop for QueueService {
    /// Flushes the latest runtime snapshots when the last queue handle drops.
    fn drop(&mut self) {
        if Arc::strong_count(&self.shared) != 1 {
            return;
        }

        if let Err(error) = flush_runtime_and_persist(&self.shared) {
            eprintln!("failed to flush queue state on shutdown: {error}");
        }
    }
}
