use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use tungsten_net::{DownloadId, NetError, PersistedState, ProgressSnapshot, StateStore};

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

    fn load_state_unlocked(&self) -> Result<PersistedState, NetError> {
        if !self.path.exists() {
            return Ok(PersistedState::default());
        }

        let content = fs::read_to_string(&self.path)?;
        let parsed = serde_json::from_str::<PersistedState>(&content)
            .map_err(|error| NetError::State(format!("failed to parse state json: {error}")))?;

        Ok(parsed)
    }

    fn write_state_file(&self, state: &PersistedState) -> Result<(), NetError> {
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

impl StateStore for DiskStateStore {
    fn load_state(&self) -> Result<PersistedState, NetError> {
        let _guard = self.lock_io()?;
        self.load_state_unlocked()
    }

    fn save_state(&self, state: &PersistedState) -> Result<(), NetError> {
        let _guard = self.lock_io()?;
        self.write_state_file(state)
    }

    fn checkpoint_progress(
        &self,
        download_id: DownloadId,
        progress: &ProgressSnapshot,
    ) -> Result<(), NetError> {
        let _guard = self.lock_io()?;
        let mut state = self.load_state_unlocked()?;
        for record in &mut state.downloads {
            if record.id == download_id {
                record.progress = progress.clone();
                record.touch();
                break;
            }
        }

        self.write_state_file(&state)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::{Arc, Barrier};
    use std::thread;

    use tempfile::tempdir;
    use tungsten_net::{
        ConflictPolicy, DownloadId, DownloadRecord, DownloadRequest, DownloadStatus, IntegrityRule,
        PersistedState, ProgressSnapshot, StateStore,
    };

    use super::DiskStateStore;

    fn build_record(id: u64, path: &Path) -> DownloadRecord {
        DownloadRecord {
            id: DownloadId(id),
            request: DownloadRequest::new(
                format!("https://example.com/{id}.bin"),
                path.join(format!("file-{id}.bin")),
                ConflictPolicy::AutoRename,
                IntegrityRule::None,
            ),
            temp_path: path.join(format!("file-{id}.part")),
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
    fn checkpoint_progress_handles_concurrent_writes() {
        let temp = match tempdir() {
            Ok(value) => value,
            Err(error) => panic!("failed to create temp dir: {error}"),
        };
        let state_path = temp.path().join("state.json");
        let store = Arc::new(DiskStateStore::new(state_path));

        let initial_state = PersistedState {
            next_id: 3,
            downloads: vec![build_record(1, temp.path()), build_record(2, temp.path())],
        };
        if let Err(error) = store.save_state(&initial_state) {
            panic!("failed to save initial state: {error}");
        }

        let barrier = Arc::new(Barrier::new(3));
        let workers = [DownloadId(1), DownloadId(2)].map(|download_id| {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for i in 0..200 {
                    let progress = ProgressSnapshot {
                        downloaded: i,
                        total: Some(200),
                        speed_bps: None,
                        eta_seconds: None,
                    };
                    if let Err(error) = store.checkpoint_progress(download_id, &progress) {
                        panic!("checkpoint failed for {download_id}: {error}");
                    }
                }
            })
        });

        barrier.wait();
        for worker in workers {
            if let Err(error) = worker.join() {
                panic!("worker thread panicked: {error:?}");
            }
        }

        let state = match store.load_state() {
            Ok(value) => value,
            Err(error) => panic!("failed to load state after checkpoints: {error}"),
        };

        let mut progress_for_1 = None;
        let mut progress_for_2 = None;
        for record in state.downloads {
            if record.id == DownloadId(1) {
                progress_for_1 = Some(record.progress.downloaded);
            }
            if record.id == DownloadId(2) {
                progress_for_2 = Some(record.progress.downloaded);
            }
        }

        assert_eq!(progress_for_1, Some(199));
        assert_eq!(progress_for_2, Some(199));
    }
}
