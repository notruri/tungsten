use std::collections::HashMap;
use std::sync::atomic::AtomicU8;
use std::sync::{Arc, MutexGuard};

use crate::error::NetError;
use crate::state::PersistedState;
use crate::types::{DownloadId, DownloadRecord, DownloadStatus, QueueEvent};

use super::{CONTROL_RUN, QueueState, Shared};

pub(super) fn build_state_from_persisted(
    persisted: PersistedState,
) -> (
    HashMap<DownloadId, DownloadRecord>,
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

        controls.insert(record.id, Arc::new(AtomicU8::new(CONTROL_RUN)));
        downloads.insert(record.id, record);
    }

    let next_id = persisted_next_id
        .max(next_id_from_downloads(&downloads))
        .max(1);
    (downloads, controls, next_id)
}

pub(super) fn save_full_state(shared: &Shared) -> Result<(), NetError> {
    let snapshot = {
        let state = lock_state(shared)?;
        build_persisted_state(&state)
    };

    shared.store.save_state(&snapshot)
}

pub(super) fn publish_event(state: &mut QueueState, event: QueueEvent) {
    state
        .subscribers
        .retain(|subscriber| subscriber.send(event.clone()).is_ok());
}

pub(super) fn lock_state(shared: &Shared) -> Result<MutexGuard<'_, QueueState>, NetError> {
    shared
        .state
        .lock()
        .map_err(|error| NetError::State(format!("queue state poisoned: {error}")))
}

fn build_persisted_state(state: &QueueState) -> PersistedState {
    let mut downloads = state.downloads.values().cloned().collect::<Vec<_>>();
    downloads.sort_by_key(|record| record.id.0);

    PersistedState {
        next_id: state.next_id,
        downloads,
    }
}

fn next_id_from_downloads(downloads: &HashMap<DownloadId, DownloadRecord>) -> u64 {
    downloads
        .keys()
        .map(|id| id.0)
        .max()
        .unwrap_or(0)
        .saturating_add(1)
}
