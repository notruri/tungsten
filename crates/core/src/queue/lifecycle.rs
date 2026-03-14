use std::sync::Arc;
use std::thread;

use tracing::{debug, warn};

use crate::error::CoreError;
use crate::model::{DownloadId, QueueEvent};
use crate::transfer::ProbeInfo;

use super::files::{destination_from_request, resolve_destination, temp_path_for};
use super::{Shared, lock_state, publish_event, save_full_state};

pub(crate) fn spawn_enqueue_resolution(shared: Arc<Shared>, download_id: DownloadId) {
    thread::spawn(move || {
        if let Err(error) = resolve_download_preflight(&shared, download_id) {
            warn!(
                download_id = %download_id,
                error = %error,
                "failed to resolve destination during enqueue preflight"
            );
        }
    });
}

fn resolve_download_preflight(shared: &Shared, download_id: DownloadId) -> Result<(), CoreError> {
    let initial_record = {
        let state = lock_state(shared)?;
        state
            .downloads
            .get(&download_id)
            .cloned()
            .ok_or(CoreError::DownloadNotFound(download_id))?
    };

    let probe = match shared.transfer.probe(&initial_record.request) {
        Ok(probe) => Some(probe),
        Err(error) => {
            warn!(
                download_id = %download_id,
                error = %error,
                "preflight probe failed; falling back to URL/default destination"
            );
            None
        }
    };

    let resolved_record = {
        let mut state = lock_state(shared)?;
        let Some(current) = state.downloads.get(&download_id).cloned() else {
            return Ok(());
        };

        if current.destination.is_some() {
            return Ok(());
        }

        let next = apply_probe_info(current, &state, download_id, probe.as_ref());
        if let Some(record) = state.downloads.get_mut(&download_id) {
            *record = next.clone();
        }
        publish_event(&mut state, QueueEvent::Updated(next.to_record()));
        next
    };

    save_full_state(shared)?;
    debug!(
        download_id = %download_id,
        destination = ?resolved_record.destination,
        "resolved destination during enqueue preflight"
    );

    Ok(())
}

fn apply_probe_info(
    mut record: crate::store::PersistedDownload,
    state: &super::QueueState,
    download_id: DownloadId,
    probe: Option<&ProbeInfo>,
) -> crate::store::PersistedDownload {
    let candidate = destination_from_request(
        &record.request.destination,
        &record.request.url,
        probe.as_ref().and_then(|value| value.file_name.as_deref()),
        &state.fallback_filename,
    );
    let resolved_destination =
        resolve_destination(&candidate, &state.downloads, &record.request.conflict);
    record.temp_path = temp_path_for(&resolved_destination, download_id);
    record.destination = Some(resolved_destination);

    if let Some(probe) = probe {
        record.supports_resume = probe.accept_ranges;
        if let Some(total_size) = probe.total_size {
            record.progress.total = Some(total_size);
        }
        if let Some(etag) = &probe.etag {
            record.etag = Some(etag.clone());
        }
        if let Some(last_modified) = &probe.last_modified {
            record.last_modified = Some(last_modified.clone());
        }
    }

    record.touch();
    record
}
