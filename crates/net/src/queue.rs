mod api;
mod files;
mod lifecycle;
mod persist;
mod runtime;
mod scheduler;

#[cfg(test)]
mod tests;

use self::persist::publish_event;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, AtomicU64};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};
use tracing::{debug, error};

use crate::error::NetError;
use crate::model::{DownloadId, DownloadStatus, QueueEvent};
use crate::store::{PersistedDownload, QueueStore};
use crate::transfer::{
    ReqwestTransfer, SpeedLimit, Transfer, TransferUpdate, progress_for_speed_limit,
};

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
}

impl QueueConfig {
    pub fn new(max_parallel: usize, connections: usize) -> Self {
        Self {
            max_parallel: max_parallel.max(1),
            connections: connections.max(1),
            download_limit_kbps: 0,
            fallback_filename: DEFAULT_DOWNLOAD_FILE_NAME.to_string(),
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
}

/// High-level queue orchestrator used by the GUI.
#[derive(Clone)]
pub struct QueueService {
    pub(crate) shared: Arc<Shared>,
}

pub(crate) struct Shared {
    pub(crate) state: Mutex<QueueState>,
    pub(crate) transfer: Arc<dyn Transfer>,
    pub(crate) store: Arc<dyn QueueStore>,
    pub(crate) coordinator_tx: mpsc::Sender<()>,
    pub(crate) global_speed_limit: SpeedLimit,
}

pub(crate) struct QueueState {
    pub(crate) next_id: u64,
    pub(crate) max_parallel: usize,
    pub(crate) fallback_filename: String,
    pub(crate) downloads: HashMap<DownloadId, PersistedDownload>,
    pub(crate) controls: HashMap<DownloadId, Arc<AtomicU8>>,
    pub(crate) speed_limits: HashMap<DownloadId, Arc<AtomicU64>>,
    pub(crate) runtime: HashMap<DownloadId, RuntimeDownloadState>,
    pub(crate) last_persist_at: Instant,
    pub(crate) subscribers: Vec<mpsc::Sender<QueueEvent>>,
}

pub(crate) struct RuntimeDownloadState {
    pub(crate) update: TransferUpdate,
    pub(crate) ui_dirty: bool,
    pub(crate) persist_dirty: bool,
    pub(crate) wake_pending: bool,
    pub(crate) last_event_at: Instant,
}

impl RuntimeDownloadState {
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
    pub fn new(config: QueueConfig, store: Arc<dyn QueueStore>) -> Result<Self, NetError> {
        let transfer = Arc::new(ReqwestTransfer::new(config.connections));
        Self::with_transfer(config, transfer, store)
    }

    pub fn with_transfer(
        config: QueueConfig,
        transfer: Arc<dyn Transfer>,
        store: Arc<dyn QueueStore>,
    ) -> Result<Self, NetError> {
        let persisted = store.load_queue()?;
        debug!(
            persisted_downloads = persisted.downloads.len(),
            "loaded persisted queue state"
        );
        let (downloads, controls, speed_limits, next_id) =
            persist::build_state_from_persisted(persisted, &config.fallback_filename);
        let (coordinator_tx, coordinator_rx) = mpsc::channel();

        let shared = Arc::new(Shared {
            state: Mutex::new(QueueState {
                next_id,
                max_parallel: config.max_parallel.max(1),
                fallback_filename: config.fallback_filename.clone(),
                downloads,
                controls,
                speed_limits,
                runtime: HashMap::new(),
                last_persist_at: Instant::now(),
                subscribers: Vec::new(),
            }),
            transfer,
            store,
            coordinator_tx,
            global_speed_limit: SpeedLimit::shared_global(config.download_limit_kbps),
        });

        let service = Self {
            shared: Arc::clone(&shared),
        };

        persist::save_full_state(&service.shared)?;
        runtime::spawn_coordinator(Arc::clone(&shared), coordinator_rx);
        scheduler::spawn_scheduler(shared);
        debug!(
            max_parallel = config.max_parallel,
            "queue service initialized"
        );
        Ok(service)
    }
}

impl Drop for QueueService {
    fn drop(&mut self) {
        if Arc::strong_count(&self.shared) != 1 {
            return;
        }

        if let Err(error) = runtime::flush_runtime_and_persist(&self.shared) {
            error!(error = %error, "failed to flush queue state on shutdown");
        }
    }
}

impl QueueService {
    pub fn set_connections(&self, connections: usize) -> Result<(), NetError> {
        self.shared.transfer.set_connections(connections.max(1));
        Ok(())
    }

    pub fn set_fallback_filename(
        &self,
        fallback_filename: impl Into<String>,
    ) -> Result<(), NetError> {
        let fallback_filename = fallback_filename.into();
        let fallback_filename = fallback_filename.trim().to_string();
        if fallback_filename.is_empty() {
            return Err(NetError::InvalidRequest(
                "fallback filename must not be empty".to_string(),
            ));
        }
        if fallback_filename.contains('/') || fallback_filename.contains('\\') {
            return Err(NetError::InvalidRequest(
                "fallback filename must not contain path separators".to_string(),
            ));
        }

        {
            let mut state = self
                .shared
                .state
                .lock()
                .map_err(|error| NetError::State(format!("queue state poisoned: {error}")))?;
            state.fallback_filename = fallback_filename;
        }

        Ok(())
    }

    pub fn set_download_limit(&self, download_limit_kbps: u64) -> Result<(), NetError> {
        self.shared
            .global_speed_limit
            .set_global_kbps(download_limit_kbps);

        let mut updates = Vec::new();
        {
            let mut state = self
                .shared
                .state
                .lock()
                .map_err(|error| NetError::State(format!("queue state poisoned: {error}")))?;
            let now = Instant::now();
            let download_ids = state.downloads.keys().copied().collect::<Vec<_>>();
            for download_id in download_ids {
                let Some(record) = state.downloads.get(&download_id) else {
                    continue;
                };
                if record.request.speed_limit_kbps.is_some() {
                    continue;
                }

                if let Some(updated) = refresh_progress_for_speed_limit(
                    &mut state,
                    download_id,
                    kbps_to_bps(download_limit_kbps),
                    now,
                ) {
                    updates.push(updated);
                }
            }

            for record in updates.drain(..) {
                publish_event(&mut state, QueueEvent::Updated(record));
            }
        }

        Ok(())
    }
}

fn refresh_progress_for_speed_limit(
    state: &mut QueueState,
    download_id: DownloadId,
    speed_limit_bps: Option<u64>,
    now: Instant,
) -> Option<crate::model::DownloadRecord> {
    let is_running = state.runtime.contains_key(&download_id)
        || matches!(
            state
                .downloads
                .get(&download_id)
                .map(|record| &record.status),
            Some(DownloadStatus::Running)
        );
    if !is_running {
        return None;
    }

    let current = state
        .runtime
        .get(&download_id)
        .map(|runtime| runtime.update.progress.clone())
        .or_else(|| {
            state
                .downloads
                .get(&download_id)
                .map(|record| record.progress.clone())
        })?;
    let next = progress_for_speed_limit(&current, speed_limit_bps);

    if let Some(runtime) = state.runtime.get_mut(&download_id) {
        runtime.update.progress = next.clone();
        runtime.ui_dirty = false;
        runtime.persist_dirty = true;
        runtime.last_event_at = now;
    }

    let record = state.downloads.get_mut(&download_id)?;
    record.progress = next;
    record.touch();
    Some(record.to_record())
}

fn kbps_to_bps(speed_limit_kbps: u64) -> Option<u64> {
    match speed_limit_kbps {
        0 => None,
        kbps => Some(kbps.saturating_mul(1024)),
    }
}
