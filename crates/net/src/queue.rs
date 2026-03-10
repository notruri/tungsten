mod api;
mod files;
mod lifecycle;
mod persist;
mod runtime;
mod scheduler;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::atomic::AtomicU8;
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use crate::error::NetError;
use crate::model::{DownloadId, QueueEvent};
use crate::store::{PersistedDownload, QueueStore};
use crate::transfer::{ReqwestTransfer, Transfer, TransferUpdate};

pub(crate) const CONTROL_RUN: u8 = 0;
pub(crate) const CONTROL_PAUSE: u8 = 1;
pub(crate) const CONTROL_CANCEL: u8 = 2;
pub(crate) const DEFAULT_DOWNLOAD_FILE_NAME: &str = "download.bin";
pub(crate) const UI_EVENT_INTERVAL: Duration = Duration::from_millis(33);
pub(crate) const PERSIST_INTERVAL: Duration = Duration::from_secs(3);
pub(crate) const COORDINATOR_TICK: Duration = Duration::from_millis(16);

/// Queue-level runtime configuration.
#[derive(Debug, Clone, Copy)]
pub struct QueueConfig {
    pub max_parallel: usize,
    pub connections: usize,
}

impl QueueConfig {
    pub fn new(max_parallel: usize, connections: usize) -> Self {
        Self {
            max_parallel: max_parallel.max(1),
            connections: connections.max(1),
        }
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
}

pub(crate) struct QueueState {
    pub(crate) next_id: u64,
    pub(crate) max_parallel: usize,
    pub(crate) downloads: HashMap<DownloadId, PersistedDownload>,
    pub(crate) controls: HashMap<DownloadId, Arc<AtomicU8>>,
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
        let (downloads, controls, next_id) = persist::build_state_from_persisted(persisted);
        let (coordinator_tx, coordinator_rx) = mpsc::channel();

        let shared = Arc::new(Shared {
            state: Mutex::new(QueueState {
                next_id,
                max_parallel: config.max_parallel.max(1),
                downloads,
                controls,
                runtime: HashMap::new(),
                last_persist_at: Instant::now(),
                subscribers: Vec::new(),
            }),
            transfer,
            store,
            coordinator_tx,
        });

        let service = Self {
            shared: Arc::clone(&shared),
        };

        persist::save_full_state(&service.shared)?;
        runtime::spawn_coordinator(Arc::clone(&shared), coordinator_rx);
        scheduler::spawn_scheduler(shared);
        Ok(service)
    }
}

impl Drop for QueueService {
    fn drop(&mut self) {
        if Arc::strong_count(&self.shared) != 1 {
            return;
        }

        if let Err(error) = runtime::flush_runtime_and_persist(&self.shared) {
            eprintln!("failed to flush queue state on shutdown: {error}");
        }
    }
}
