mod api;
mod files;
mod lifecycle;
mod progress;
mod scheduler;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicU8;
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tracing::{debug, error};

use crate::error::CoreError;
use crate::model::{DownloadId, DownloadStatus, QueueEvent};
use crate::store::{PersistedDownload, PersistedQueue, QueueStore};
use crate::transfer::{Transfer, TransferUpdate};

pub(crate) const CONTROL_RUN: u8 = 0;
pub(crate) const CONTROL_PAUSE: u8 = 1;
pub(crate) const CONTROL_CANCEL: u8 = 2;
pub const DEFAULT_DOWNLOAD_FILE_NAME: &str = "download.bin";
pub(crate) const UI_EVENT_INTERVAL: Duration = Duration::from_millis(33);
pub(crate) const PERSIST_INTERVAL: Duration = Duration::from_secs(3);
pub(crate) const COORDINATOR_TICK: Duration = Duration::from_millis(16);

/// Queue-level runtime configuration.
#[derive(Debug, Clone)]
pub struct QueueConfig {
    pub max_parallel: usize,
    pub connections: usize,
    pub download_limit_kbps: u64,
    pub fallback_filename: String,
    pub temp_root: PathBuf,
}

impl QueueConfig {
    pub fn new(max_parallel: usize, connections: usize) -> Self {
        Self {
            max_parallel: max_parallel.max(1),
            connections: connections.max(1),
            download_limit_kbps: 0,
            fallback_filename: DEFAULT_DOWNLOAD_FILE_NAME.to_string(),
            temp_root: std::env::temp_dir().join("Tungsten"),
        }
    }

    pub fn download_limit_kbps(mut self, download_limit_kbps: u64) -> Self {
        self.download_limit_kbps = download_limit_kbps;
        self
    }

    pub fn fallback_filename(mut self, fallback_filename: impl Into<String>) -> Self {
        let fallback_filename = fallback_filename.into().trim().to_string();
        if fallback_filename.is_empty() {
            return self;
        }

        self.fallback_filename = fallback_filename;
        self
    }

    pub fn temp_root(mut self, temp_root: PathBuf) -> Self {
        if temp_root.as_os_str().is_empty() {
            return self;
        }

        self.temp_root = temp_root;
        self
    }
}

/// High-level queue orchestrator used by the runtime.
pub struct QueueService {
    shared: Arc<Shared>,
    tokio: Arc<tokio::runtime::Runtime>,
}

impl Clone for QueueService {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            tokio: Arc::clone(&self.tokio),
        }
    }
}

pub(crate) struct Shared {
    pub(crate) state: Mutex<QueueState>,
    pub(crate) transfer: Arc<dyn Transfer>,
    pub(crate) store: Arc<dyn QueueStore>,
    pub(crate) coordinator_tx: tokio::sync::mpsc::UnboundedSender<()>,
    pub(crate) tokio: Handle,
}

pub(crate) struct QueueState {
    pub(crate) next_id: u64,
    pub(crate) max_parallel: usize,
    pub(crate) download_limit_kbps: u64,
    pub(crate) fallback_filename: String,
    pub(crate) temp_root: PathBuf,
    pub(crate) downloads: HashMap<DownloadId, PersistedDownload>,
    pub(crate) controls: HashMap<DownloadId, Arc<AtomicU8>>,
    pub(crate) updates: HashMap<DownloadId, ProgressState>,
    pub(crate) last_persist_at: Instant,
    pub(crate) subscribers: Vec<mpsc::Sender<QueueEvent>>,
}

pub(crate) struct ProgressState {
    pub(crate) update: TransferUpdate,
    pub(crate) ui_dirty: bool,
    pub(crate) persist_dirty: bool,
    pub(crate) wake_pending: bool,
    pub(crate) last_event_at: Instant,
}

impl ProgressState {
    pub(crate) fn new(update: TransferUpdate) -> Self {
        Self {
            update,
            ui_dirty: false,
            persist_dirty: false,
            wake_pending: false,
            last_event_at: Instant::now(),
        }
    }
}

impl QueueService {
    pub fn new(
        config: QueueConfig,
        transfer: Arc<dyn Transfer>,
        store: Arc<dyn QueueStore>,
        tokio: tokio::runtime::Runtime,
    ) -> Result<Self, CoreError> {
        let tokio = Arc::new(tokio);
        let persisted = store.load_queue()?;
        let (downloads, controls, next_id) = build_state_from_persisted(persisted);
        let (coordinator_tx, coordinator_rx) = tokio::sync::mpsc::unbounded_channel();

        transfer.set_connections(config.connections);
        transfer.set_download_limit(config.download_limit_kbps);

        for record in downloads.values() {
            transfer.set_speed_limit(record.id, record.request.speed_limit_kbps)?;
        }

        let shared = Arc::new(Shared {
            state: Mutex::new(QueueState {
                next_id,
                max_parallel: config.max_parallel.max(1),
                download_limit_kbps: config.download_limit_kbps,
                fallback_filename: config.fallback_filename,
                temp_root: config.temp_root,
                downloads,
                controls,
                updates: HashMap::new(),
                last_persist_at: Instant::now(),
                subscribers: Vec::new(),
            }),
            transfer,
            store,
            coordinator_tx,
            tokio: tokio.handle().clone(),
        });

        let service = Self {
            shared: Arc::clone(&shared),
            tokio: Arc::clone(&tokio),
        };

        save_full_state(&service.shared)?;
        progress::spawn_coordinator(
            Arc::downgrade(&shared),
            coordinator_rx,
            shared.tokio.clone(),
        );
        scheduler::spawn_scheduler(Arc::downgrade(&shared), shared.tokio.clone());
        Ok(service)
    }
}

impl Drop for QueueService {
    fn drop(&mut self) {
        if Arc::strong_count(&self.shared) != 1 {
            return;
        }

        if let Err(error) = progress::flush_progress_and_persist(&self.shared) {
            error!(error = %error, "failed to flush queue state on shutdown");
        }
    }
}

pub(crate) fn build_state_from_persisted(
    persisted: PersistedQueue,
) -> (
    HashMap<DownloadId, PersistedDownload>,
    HashMap<DownloadId, Arc<AtomicU8>>,
    u64,
) {
    let persisted_next_id = persisted.next_id;
    let mut downloads = HashMap::new();
    let mut controls = HashMap::new();

    for mut record in persisted.downloads {
        if matches!(
            record.status,
            DownloadStatus::Preparing
                | DownloadStatus::Running
                | DownloadStatus::Finalizing
                | DownloadStatus::Verifying
        ) {
            let previous = record.status.clone();
            record.status = DownloadStatus::Queued;
            record.error = None;
            record.touch();
            log_status_change(record.id, &previous, &record.status, "restore reset");
        }

        record.loaded_from_store = true;
        controls.insert(record.id, Arc::new(AtomicU8::new(CONTROL_RUN)));
        downloads.insert(record.id, record);
    }

    let next_id = persisted_next_id
        .max(next_id_from_downloads(&downloads))
        .max(1);
    (downloads, controls, next_id)
}

pub(crate) fn save_full_state(shared: &Shared) -> Result<(), CoreError> {
    let snapshot = {
        let state = lock_state(shared)?;
        build_persisted_queue(&state)
    };

    shared.store.save_queue(&snapshot)
}

pub(crate) fn publish_event(state: &mut QueueState, event: QueueEvent) {
    state
        .subscribers
        .retain(|subscriber| subscriber.send(event.clone()).is_ok());
}

pub(crate) fn log_status_change(
    download_id: DownloadId,
    from: &DownloadStatus,
    to: &DownloadStatus,
    reason: &'static str,
) {
    if from == to {
        return;
    }

    debug!(
        download_id = %download_id,
        from = ?from,
        to = ?to,
        reason,
        "download status changed"
    );
}

pub(crate) fn lock_state(
    shared: &Shared,
) -> Result<std::sync::MutexGuard<'_, QueueState>, CoreError> {
    shared
        .state
        .lock()
        .map_err(|error| CoreError::State(format!("queue state poisoned: {error}")))
}

fn build_persisted_queue(state: &QueueState) -> PersistedQueue {
    let mut downloads = state.downloads.values().cloned().collect::<Vec<_>>();
    downloads.sort_by_key(|record| record.id.0);

    PersistedQueue {
        next_id: state.next_id,
        downloads,
    }
}

fn next_id_from_downloads(downloads: &HashMap<DownloadId, PersistedDownload>) -> u64 {
    downloads
        .keys()
        .map(|id| id.0)
        .max()
        .unwrap_or(0)
        .saturating_add(1)
}

pub(crate) fn refresh_progress_for_speed_limit(
    state: &mut QueueState,
    download_id: DownloadId,
    speed_limit_bps: Option<u64>,
    now: Instant,
) -> Option<crate::model::DownloadRecord> {
    let is_running = state.updates.contains_key(&download_id)
        || matches!(
            state
                .downloads
                .get(&download_id)
                .map(|record| &record.status),
            Some(DownloadStatus::Preparing | DownloadStatus::Running | DownloadStatus::Finalizing)
        );
    if !is_running {
        return None;
    }

    let current = state
        .updates
        .get(&download_id)
        .map(|progress| progress.update.progress.clone())
        .or_else(|| {
            state
                .downloads
                .get(&download_id)
                .map(|record| record.progress.clone())
        })?;
    let next = progress_for_speed_limit(&current, speed_limit_bps);

    if let Some(progress) = state.updates.get_mut(&download_id) {
        progress.update.progress = next.clone();
        progress.ui_dirty = false;
        progress.persist_dirty = true;
        progress.last_event_at = now;
    }

    let record = state.downloads.get_mut(&download_id)?;
    record.progress = next;
    record.touch();
    Some(record.to_record())
}

fn progress_for_speed_limit(
    progress: &crate::model::ProgressSnapshot,
    speed_limit_bps: Option<u64>,
) -> crate::model::ProgressSnapshot {
    let speed_bps = match speed_limit_bps {
        Some(limit_bps) if limit_bps > 0 => Some(limit_bps),
        _ => progress.speed_bps,
    };
    let eta_seconds = match (progress.total, speed_bps) {
        (Some(total_size), Some(speed)) if speed > 0 && total_size >= progress.downloaded => {
            Some((total_size - progress.downloaded) / speed)
        }
        _ => None,
    };

    crate::model::ProgressSnapshot {
        downloaded: progress.downloaded,
        total: progress.total,
        speed_bps,
        eta_seconds,
    }
}

pub(crate) fn kbps_to_bps(speed_limit_kbps: Option<u64>) -> Option<u64> {
    match speed_limit_kbps {
        Some(0) | None => None,
        Some(kbps) => Some(kbps.saturating_mul(1024)),
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use crate::model::{
        ConflictPolicy, DownloadId, DownloadRequest, DownloadStatus, IntegrityRule,
        ProgressSnapshot,
    };
    use crate::store::{PersistedDownload, PersistedQueue};
    use crate::transfer::TempLayout;

    use super::build_state_from_persisted;

    #[test]
    fn build_state_from_persisted_resets_active_statuses_to_queued() {
        let now = Utc::now();
        let statuses = [
            DownloadStatus::Preparing,
            DownloadStatus::Running,
            DownloadStatus::Finalizing,
            DownloadStatus::Verifying,
        ];

        for (offset, status) in statuses.into_iter().enumerate() {
            let download_id = DownloadId((offset + 1) as u64);
            let (downloads, _, _) = build_state_from_persisted(PersistedQueue {
                next_id: 1,
                downloads: vec![PersistedDownload {
                    id: download_id,
                    request: DownloadRequest::new(
                        "https://example.com/file.bin".to_string(),
                        "file.bin",
                        ConflictPolicy::AutoRename,
                        IntegrityRule::None,
                    ),
                    destination: Some("file.bin".into()),
                    loaded_from_store: false,
                    temp_path: "file.bin.part".into(),
                    temp_layout: TempLayout::Single,
                    supports_resume: true,
                    status,
                    progress: ProgressSnapshot::default(),
                    error: Some("stale".to_string()),
                    etag: None,
                    last_modified: None,
                    created_at: now,
                    updated_at: now,
                }],
            });

            let record = downloads
                .get(&download_id)
                .unwrap_or_else(|| panic!("download should be restored"));
            assert_eq!(record.status, DownloadStatus::Queued);
            assert_eq!(record.error, None);
        }
    }
}
