use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use tracing::debug;
use tungsten_core::CoreError;
use tungsten_core::store::{
    PersistedDownload as CorePersistedDownload, PersistedQueue as CorePersistedQueue,
    QueueStore as CoreQueueStore,
};
use tungsten_net::NetError;
use tungsten_net::model::{
    ConflictPolicy as NetConflictPolicy, DownloadId as NetDownloadId,
    DownloadRequest as NetDownloadRequest, DownloadStatus as NetDownloadStatus,
    IntegrityRule as NetIntegrityRule, ProgressSnapshot as NetProgressSnapshot,
};
use tungsten_net::store::{PersistedQueue, QueueStore};
use tungsten_net::transfer::{
    MultipartPart as NetMultipartPart, MultipartState as NetMultipartState,
    TempLayout as NetTempLayout,
};

mod codec;
mod db;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone)]
pub struct DiskStateStore {
    path: PathBuf,
    io_lock: Arc<Mutex<()>>,
}

impl DiskStateStore {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            io_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn lock_io(&self) -> Result<MutexGuard<'_, ()>, NetError> {
        self.io_lock
            .lock()
            .map_err(|error| NetError::State(format!("state db lock poisoned: {error}")))
    }

    fn load_queue_unlocked(&self) -> Result<PersistedQueue, NetError> {
        if !self.path.exists() {
            return Ok(PersistedQueue::default());
        }

        db::read_queue(&self.path)
    }

    fn write_queue_db(&self, state: &PersistedQueue) -> Result<(), NetError> {
        db::write_queue(&self.path, state)
    }
}

impl QueueStore for DiskStateStore {
    fn load_queue(&self) -> Result<PersistedQueue, NetError> {
        let _guard = self.lock_io()?;
        let state = self.load_queue_unlocked()?;
        debug!(
            path = %self.path.display(),
            downloads = state.downloads.len(),
            "loaded queue state"
        );
        Ok(state)
    }

    fn save_queue(&self, state: &PersistedQueue) -> Result<(), NetError> {
        let _guard = self.lock_io()?;
        debug!(
            path = %self.path.display(),
            downloads = state.downloads.len(),
            "saving queue state"
        );
        self.write_queue_db(state)
    }
}

impl CoreQueueStore for DiskStateStore {
    fn load_queue(&self) -> Result<CorePersistedQueue, CoreError> {
        let state = <Self as QueueStore>::load_queue(self)?;
        Ok(map_persisted_queue_to_core(state))
    }

    fn save_queue(&self, state: &CorePersistedQueue) -> Result<(), CoreError> {
        let state = map_persisted_queue_to_net(state);
        <Self as QueueStore>::save_queue(self, &state)?;
        Ok(())
    }
}

fn map_persisted_queue_to_core(queue: PersistedQueue) -> CorePersistedQueue {
    CorePersistedQueue {
        next_id: queue.next_id,
        downloads: queue
            .downloads
            .into_iter()
            .map(map_persisted_download_to_core)
            .collect(),
    }
}

fn map_persisted_download_to_core(
    download: tungsten_net::store::PersistedDownload,
) -> CorePersistedDownload {
    CorePersistedDownload {
        id: tungsten_core::DownloadId(download.id.0),
        request: map_request_to_core(download.request),
        destination: download.destination,
        loaded_from_store: download.loaded_from_store,
        temp_path: download.temp_path,
        temp_layout: map_temp_layout_to_core(download.temp_layout),
        supports_resume: download.supports_resume,
        status: map_status_to_core(download.status),
        progress: map_progress_to_core(download.progress),
        error: download.error,
        etag: download.etag,
        last_modified: download.last_modified,
        created_at: download.created_at,
        updated_at: download.updated_at,
    }
}

fn map_request_to_core(request: NetDownloadRequest) -> tungsten_core::DownloadRequest {
    tungsten_core::DownloadRequest {
        url: request.url,
        destination: request.destination,
        conflict: match request.conflict {
            NetConflictPolicy::AutoRename => tungsten_core::ConflictPolicy::AutoRename,
        },
        integrity: match request.integrity {
            NetIntegrityRule::None => tungsten_core::IntegrityRule::None,
            NetIntegrityRule::Sha256(value) => tungsten_core::IntegrityRule::Sha256(value),
        },
        speed_limit_kbps: request.speed_limit_kbps,
    }
}

fn map_temp_layout_to_core(layout: NetTempLayout) -> tungsten_core::TempLayout {
    match layout {
        NetTempLayout::Single => tungsten_core::TempLayout::Single,
        NetTempLayout::Multipart(layout) => {
            tungsten_core::TempLayout::Multipart(tungsten_core::MultipartState {
                total_size: layout.total_size,
                parts: layout
                    .parts
                    .into_iter()
                    .map(|part| tungsten_core::MultipartPart {
                        index: part.index,
                        start: part.start,
                        end: part.end,
                        path: part.path,
                    })
                    .collect(),
            })
        }
    }
}

fn map_status_to_core(status: NetDownloadStatus) -> tungsten_core::DownloadStatus {
    match status {
        NetDownloadStatus::Queued => tungsten_core::DownloadStatus::Queued,
        NetDownloadStatus::Running => tungsten_core::DownloadStatus::Running,
        NetDownloadStatus::Paused => tungsten_core::DownloadStatus::Paused,
        NetDownloadStatus::Verifying => tungsten_core::DownloadStatus::Verifying,
        NetDownloadStatus::Completed => tungsten_core::DownloadStatus::Completed,
        NetDownloadStatus::Failed => tungsten_core::DownloadStatus::Failed,
        NetDownloadStatus::Cancelled => tungsten_core::DownloadStatus::Cancelled,
    }
}

fn map_progress_to_core(progress: NetProgressSnapshot) -> tungsten_core::ProgressSnapshot {
    tungsten_core::ProgressSnapshot {
        downloaded: progress.downloaded,
        total: progress.total,
        speed_bps: progress.speed_bps,
        eta_seconds: progress.eta_seconds,
    }
}

fn map_persisted_queue_to_net(queue: &CorePersistedQueue) -> PersistedQueue {
    PersistedQueue {
        next_id: queue.next_id,
        downloads: queue
            .downloads
            .iter()
            .cloned()
            .map(map_persisted_download_to_net)
            .collect(),
    }
}

fn map_persisted_download_to_net(
    download: CorePersistedDownload,
) -> tungsten_net::store::PersistedDownload {
    tungsten_net::store::PersistedDownload {
        id: NetDownloadId(download.id.0),
        request: map_request_to_net(download.request),
        destination: download.destination,
        loaded_from_store: download.loaded_from_store,
        temp_path: download.temp_path,
        temp_layout: map_temp_layout_to_net(download.temp_layout),
        supports_resume: download.supports_resume,
        status: map_status_to_net(download.status),
        progress: map_progress_to_net(download.progress),
        error: download.error,
        etag: download.etag,
        last_modified: download.last_modified,
        created_at: download.created_at,
        updated_at: download.updated_at,
    }
}

fn map_request_to_net(request: tungsten_core::DownloadRequest) -> NetDownloadRequest {
    NetDownloadRequest {
        url: request.url,
        destination: request.destination,
        conflict: match request.conflict {
            tungsten_core::ConflictPolicy::AutoRename => NetConflictPolicy::AutoRename,
        },
        integrity: match request.integrity {
            tungsten_core::IntegrityRule::None => NetIntegrityRule::None,
            tungsten_core::IntegrityRule::Sha256(value) => NetIntegrityRule::Sha256(value),
        },
        speed_limit_kbps: request.speed_limit_kbps,
    }
}

fn map_temp_layout_to_net(layout: tungsten_core::TempLayout) -> NetTempLayout {
    match layout {
        tungsten_core::TempLayout::Single => NetTempLayout::Single,
        tungsten_core::TempLayout::Multipart(layout) => {
            NetTempLayout::Multipart(NetMultipartState {
                total_size: layout.total_size,
                parts: layout
                    .parts
                    .into_iter()
                    .map(|part| NetMultipartPart {
                        index: part.index,
                        start: part.start,
                        end: part.end,
                        path: part.path,
                    })
                    .collect(),
            })
        }
    }
}

fn map_status_to_net(status: tungsten_core::DownloadStatus) -> NetDownloadStatus {
    match status {
        tungsten_core::DownloadStatus::Queued => NetDownloadStatus::Queued,
        tungsten_core::DownloadStatus::Running => NetDownloadStatus::Running,
        tungsten_core::DownloadStatus::Paused => NetDownloadStatus::Paused,
        tungsten_core::DownloadStatus::Verifying => NetDownloadStatus::Verifying,
        tungsten_core::DownloadStatus::Completed => NetDownloadStatus::Completed,
        tungsten_core::DownloadStatus::Failed => NetDownloadStatus::Failed,
        tungsten_core::DownloadStatus::Cancelled => NetDownloadStatus::Cancelled,
    }
}

fn map_progress_to_net(progress: tungsten_core::ProgressSnapshot) -> NetProgressSnapshot {
    NetProgressSnapshot {
        downloaded: progress.downloaded,
        total: progress.total,
        speed_bps: progress.speed_bps,
        eta_seconds: progress.eta_seconds,
    }
}
