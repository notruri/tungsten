use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use tracing::debug;
use tungsten_net::NetError;
use tungsten_net::store::{PersistedQueue, QueueStore};

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
            .map_err(|error| NetError::State(format!("disk state lock poisoned: {error}")))
    }

    fn load_queue_unlocked(&self) -> Result<PersistedQueue, NetError> {
        if !self.path.exists() {
            return Ok(PersistedQueue::default());
        }

        let content = fs::read_to_string(&self.path)?;
        serde_json::from_str::<PersistedQueue>(&content)
            .map_err(|error| NetError::State(format!("failed to parse state json: {error}")))
    }

    fn write_queue_file(&self, state: &PersistedQueue) -> Result<(), NetError> {
        let serialized = serde_json::to_string_pretty(state)
            .map_err(|error| NetError::State(format!("failed to serialize state: {error}")))?;

        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }

        let temp_path = self.path.with_extension("tmp");
        fs::write(&temp_path, serialized)?;
        fs::rename(temp_path, &self.path)?;
        Ok(())
    }
}

impl QueueStore for DiskStateStore {
    fn load_queue(&self) -> Result<PersistedQueue, NetError> {
        let _guard = self.lock_io()?;
        let state = self.load_queue_unlocked()?;
        debug!(
            path = %self.path.display(),
            downloads = state.downloads.len(),
            "loaded queue state from disk"
        );
        Ok(state)
    }

    fn save_queue(&self, state: &PersistedQueue) -> Result<(), NetError> {
        let _guard = self.lock_io()?;
        debug!(
            path = %self.path.display(),
            downloads = state.downloads.len(),
            "saving queue state to disk"
        );
        self.write_queue_file(state)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use tungsten_net::model::{
        ConflictPolicy, DownloadId, DownloadRequest, DownloadStatus, IntegrityRule,
        ProgressSnapshot,
    };
    use tungsten_net::store::PersistedDownload;
    use tungsten_net::transfer::TempLayout;

    use super::*;

    fn build_record(id: u64, path: &Path) -> PersistedDownload {
        PersistedDownload {
            id: DownloadId(id),
            request: DownloadRequest::new(
                format!("https://example.com/{id}.bin"),
                path.join(format!("file-{id}.bin")),
                ConflictPolicy::AutoRename,
                IntegrityRule::None,
            ),
            temp_path: path.join(format!("file-{id}.part")),
            temp_layout: TempLayout::Single,
            supports_resume: true,
            status: DownloadStatus::Queued,
            progress: ProgressSnapshot::default(),
            error: None,
            etag: None,
            last_modified: None,
            created_at: 0,
            updated_at: 0,
        }
    }

    #[test]
    fn save_and_load_queue_round_trip() {
        let temp = tempdir().unwrap_or_else(|error| panic!("failed to create temp dir: {error}"));
        let state_path = temp.path().join("state.json");
        let store = DiskStateStore::new(state_path);

        let initial_state = PersistedQueue {
            next_id: 3,
            downloads: vec![build_record(1, temp.path()), build_record(2, temp.path())],
        };
        store
            .save_queue(&initial_state)
            .unwrap_or_else(|error| panic!("failed to save state: {error}"));

        let loaded = store
            .load_queue()
            .unwrap_or_else(|error| panic!("failed to load state: {error}"));

        assert_eq!(loaded.next_id, 3);
        assert_eq!(loaded.downloads.len(), 2);
        assert_eq!(loaded.downloads[0].id, DownloadId(1));
        assert_eq!(loaded.downloads[1].id, DownloadId(2));
    }
}
