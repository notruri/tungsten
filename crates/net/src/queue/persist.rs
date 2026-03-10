use std::collections::HashMap;
use std::sync::atomic::AtomicU8;
use std::sync::{Arc, MutexGuard};

use crate::error::NetError;
use crate::model::{DownloadId, DownloadStatus, QueueEvent};
use crate::store::{PersistedDownload, PersistedQueue};

use super::DEFAULT_DOWNLOAD_FILE_NAME;
use super::files::resolve_destination;
use super::{CONTROL_RUN, QueueState, Shared};

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

        if record.destination.is_none() {
            let fallback = if looks_like_directory_path(&record.request.destination) {
                record.request.destination.join(DEFAULT_DOWNLOAD_FILE_NAME)
            } else {
                record.request.destination.clone()
            };
            let resolved = resolve_destination(&fallback, &downloads, &record.request.conflict);
            record.destination = Some(resolved);
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

fn looks_like_directory_path(path: &std::path::Path) -> bool {
    if path.is_dir() {
        return true;
    }

    let raw = path.to_string_lossy();
    if raw.ends_with('/') || raw.ends_with('\\') {
        return true;
    }

    path.extension().is_none()
}

pub(crate) fn save_full_state(shared: &Shared) -> Result<(), NetError> {
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

pub(crate) fn lock_state(shared: &Shared) -> Result<MutexGuard<'_, QueueState>, NetError> {
    shared
        .state
        .lock()
        .map_err(|error| NetError::State(format!("queue state poisoned: {error}")))
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
