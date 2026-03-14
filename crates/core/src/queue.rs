mod api;
mod files;
mod lifecycle;

use std::collections::HashMap;
use std::sync::atomic::AtomicU8;
use std::sync::{Arc, Mutex, mpsc};

use crate::error::CoreError;
use crate::model::{DownloadId, DownloadStatus, QueueEvent};
use crate::store::{PersistedDownload, PersistedQueue, QueueStore};
use crate::transfer::Transfer;

pub(crate) const CONTROL_RUN: u8 = 0;
pub(crate) const CONTROL_PAUSE: u8 = 1;
pub(crate) const CONTROL_CANCEL: u8 = 2;
pub const DEFAULT_DOWNLOAD_FILE_NAME: &str = "download.bin";

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

/// High-level queue orchestrator used by the runtime.
#[derive(Clone)]
pub struct QueueService {
    shared: Arc<Shared>,
}

pub(crate) struct Shared {
    pub(crate) state: Mutex<QueueState>,
    pub(crate) transfer: Arc<dyn Transfer>,
    pub(crate) store: Arc<dyn QueueStore>,
}

pub(crate) struct QueueState {
    pub(crate) next_id: u64,
    pub(crate) max_parallel: usize,
    pub(crate) fallback_filename: String,
    pub(crate) downloads: HashMap<DownloadId, PersistedDownload>,
    pub(crate) controls: HashMap<DownloadId, Arc<AtomicU8>>,
    pub(crate) subscribers: Vec<mpsc::Sender<QueueEvent>>,
}

impl QueueService {
    pub fn new(
        config: QueueConfig,
        transfer: Arc<dyn Transfer>,
        store: Arc<dyn QueueStore>,
    ) -> Result<Self, CoreError> {
        let persisted = store.load_queue()?;
        let (downloads, controls, next_id) = build_state_from_persisted(persisted);

        let shared = Arc::new(Shared {
            state: Mutex::new(QueueState {
                next_id,
                max_parallel: config.max_parallel.max(1),
                fallback_filename: config.fallback_filename,
                downloads,
                controls,
                subscribers: Vec::new(),
            }),
            transfer,
            store,
        });

        Ok(Self { shared })
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
            DownloadStatus::Running | DownloadStatus::Verifying
        ) {
            record.status = DownloadStatus::Queued;
            record.error = None;
            record.touch();
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
