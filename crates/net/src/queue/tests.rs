use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::backend::{ControlSignal, DownloadBackend, DownloadOutcome, DownloadTask, ProbeInfo};
use crate::error::NetError;
use crate::state::{PersistedState, StateStore};
use crate::types::{
    ConflictPolicy, DownloadId, DownloadRequest, DownloadStatus, IntegrityRule, ProgressSnapshot,
};

use super::files::resolve_destination;
use super::*;

#[derive(Default)]
struct MemoryStore {
    inner: Mutex<PersistedState>,
    save_calls: AtomicUsize,
    checkpoint_calls: AtomicUsize,
}

impl StateStore for MemoryStore {
    fn load_state(&self) -> Result<PersistedState, NetError> {
        let state = self
            .inner
            .lock()
            .map_err(|error| NetError::State(format!("memory store poisoned: {error}")))?;
        Ok(state.clone())
    }

    fn save_state(&self, state: &PersistedState) -> Result<(), NetError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|error| NetError::State(format!("memory store poisoned: {error}")))?;
        *guard = state.clone();
        self.save_calls.fetch_add(1, AtomicOrdering::SeqCst);
        Ok(())
    }

    fn checkpoint_progress(
        &self,
        _download_id: DownloadId,
        _progress: &ProgressSnapshot,
    ) -> Result<(), NetError> {
        self.checkpoint_calls.fetch_add(1, AtomicOrdering::SeqCst);
        Ok(())
    }
}

impl MemoryStore {
    fn save_calls(&self) -> usize {
        self.save_calls.load(AtomicOrdering::SeqCst)
    }

    fn checkpoint_calls(&self) -> usize {
        self.checkpoint_calls.load(AtomicOrdering::SeqCst)
    }
}

struct ImmediateBackend;

impl DownloadBackend for ImmediateBackend {
    fn probe(&self, _request: &DownloadRequest) -> Result<ProbeInfo, NetError> {
        Ok(ProbeInfo {
            total_size: Some(10),
            accept_ranges: true,
            etag: None,
            last_modified: None,
            file_name: None,
        })
    }

    fn download(
        &self,
        _task: &DownloadTask,
        on_progress: &mut dyn FnMut(ProgressSnapshot) -> Result<(), NetError>,
        control: &dyn Fn() -> ControlSignal,
    ) -> Result<DownloadOutcome, NetError> {
        match control() {
            ControlSignal::Pause => Ok(DownloadOutcome::Paused(ProgressSnapshot::default())),
            ControlSignal::Cancel => Ok(DownloadOutcome::Cancelled(ProgressSnapshot::default())),
            ControlSignal::Run => {
                on_progress(ProgressSnapshot {
                    downloaded: 10,
                    total: Some(10),
                    speed_bps: Some(10),
                    eta_seconds: Some(0),
                })?;
                Ok(DownloadOutcome::Completed(ProgressSnapshot {
                    downloaded: 10,
                    total: Some(10),
                    speed_bps: Some(10),
                    eta_seconds: Some(0),
                }))
            }
        }
    }
}

#[test]
fn auto_rename_when_destination_exists() {
    let temp = match tempfile::tempdir() {
        Ok(value) => value,
        Err(error) => panic!("tempdir should be created: {error}"),
    };
    let requested = temp.path().join("file.bin");
    if let Err(error) = fs::write(&requested, b"x") {
        panic!("test file should be created: {error}");
    }

    let resolved = resolve_destination(&requested, &HashMap::new(), &ConflictPolicy::AutoRename);
    assert_ne!(resolved, requested);
    assert_eq!(
        resolved.file_name().and_then(|name| name.to_str()),
        Some("file (1).bin")
    );
}

#[test]
fn enqueue_persists_state() {
    let store = Arc::new(MemoryStore::default());
    let backend = Arc::new(ImmediateBackend);
    let queue = match QueueService::new(backend, store.clone(), 3) {
        Ok(value) => value,
        Err(error) => panic!("queue should initialize: {error}"),
    };

    let request = DownloadRequest {
        url: "https://example.com/file.bin".to_string(),
        destination: PathBuf::from("file.bin"),
        conflict: ConflictPolicy::AutoRename,
        integrity: IntegrityRule::None,
    };

    let id = match queue.enqueue(request) {
        Ok(value) => value,
        Err(error) => panic!("enqueue should succeed: {error}"),
    };
    let snapshot = match store.load_state() {
        Ok(value) => value,
        Err(error) => panic!("state should load: {error}"),
    };

    assert_eq!(id.0, 1);
    assert_eq!(snapshot.downloads.len(), 1);
    assert_eq!(snapshot.downloads[0].status, DownloadStatus::Queued);
}

#[test]
fn progress_updates_do_not_use_checkpoint_writes() {
    struct MultiProgressBackend;

    impl DownloadBackend for MultiProgressBackend {
        fn probe(&self, _request: &DownloadRequest) -> Result<ProbeInfo, NetError> {
            Ok(ProbeInfo {
                total_size: Some(100),
                accept_ranges: true,
                etag: None,
                last_modified: None,
                file_name: None,
            })
        }

        fn download(
            &self,
            task: &DownloadTask,
            on_progress: &mut dyn FnMut(ProgressSnapshot) -> Result<(), NetError>,
            control: &dyn Fn() -> ControlSignal,
        ) -> Result<DownloadOutcome, NetError> {
            if !matches!(control(), ControlSignal::Run) {
                return Ok(DownloadOutcome::Cancelled(ProgressSnapshot::default()));
            }

            if let Some(parent) = task.temp_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&task.temp_path, vec![0u8; 100])?;

            for downloaded in [20, 40, 60, 80, 100] {
                on_progress(ProgressSnapshot {
                    downloaded,
                    total: Some(100),
                    speed_bps: Some(100),
                    eta_seconds: Some(0),
                })?;
            }

            Ok(DownloadOutcome::Completed(ProgressSnapshot {
                downloaded: 100,
                total: Some(100),
                speed_bps: Some(100),
                eta_seconds: Some(0),
            }))
        }
    }

    let temp = match tempfile::tempdir() {
        Ok(value) => value,
        Err(error) => panic!("tempdir should be created: {error}"),
    };
    let store = Arc::new(MemoryStore::default());
    let backend = Arc::new(MultiProgressBackend);
    let queue = match QueueService::new(backend, store.clone(), 1) {
        Ok(value) => value,
        Err(error) => panic!("queue should initialize: {error}"),
    };

    if let Err(error) = queue.enqueue(DownloadRequest {
        url: "https://example.com/file.bin".to_string(),
        destination: temp.path().join("checkpoint.bin"),
        conflict: ConflictPolicy::AutoRename,
        integrity: IntegrityRule::None,
    }) {
        panic!("enqueue should succeed: {error}");
    }

    let started = Instant::now();
    loop {
        let records = match queue.snapshot() {
            Ok(value) => value,
            Err(error) => panic!("snapshot should succeed: {error}"),
        };

        if records
            .iter()
            .any(|record| matches!(record.status, DownloadStatus::Completed))
        {
            break;
        }

        if started.elapsed() > Duration::from_secs(2) {
            panic!("download should complete");
        }

        thread::sleep(Duration::from_millis(20));
    }

    assert_eq!(store.checkpoint_calls(), 0);
    assert!(
        store.save_calls() < 10,
        "save_state should not be called for every progress update"
    );
}

#[test]
fn retry_moves_failed_to_queued() {
    let store = Arc::new(MemoryStore::default());
    let backend = Arc::new(ImmediateBackend);
    let queue = match QueueService::new(backend, store.clone(), 3) {
        Ok(value) => value,
        Err(error) => panic!("queue should initialize: {error}"),
    };

    let id = match queue.enqueue(DownloadRequest {
        url: "https://example.com/file.bin".to_string(),
        destination: PathBuf::from("retry.bin"),
        conflict: ConflictPolicy::AutoRename,
        integrity: IntegrityRule::None,
    }) {
        Ok(value) => value,
        Err(error) => panic!("enqueue should succeed: {error}"),
    };

    {
        let mut state = match queue.shared.state.lock() {
            Ok(value) => value,
            Err(error) => panic!("queue lock should be available: {error}"),
        };
        let record = match state.downloads.get_mut(&id) {
            Some(value) => value,
            None => panic!("record should exist"),
        };
        record.status = DownloadStatus::Failed;
        record.error = Some("network error".to_string());
    }

    if let Err(error) = queue.retry(id) {
        panic!("retry should succeed: {error}");
    }

    let records = match queue.snapshot() {
        Ok(value) => value,
        Err(error) => panic!("snapshot should succeed: {error}"),
    };
    let status = records
        .into_iter()
        .find(|record| record.id == id)
        .map(|record| record.status)
        .unwrap_or_else(|| panic!("record should exist after retry"));

    assert!(
        matches!(status, DownloadStatus::Queued | DownloadStatus::Running),
        "status after retry should be queued or running, got {status:?}"
    );
}

#[test]
fn delete_removes_queued_download() {
    let store = Arc::new(MemoryStore::default());
    let backend = Arc::new(ImmediateBackend);
    let queue = match QueueService::new(backend, store.clone(), 3) {
        Ok(value) => value,
        Err(error) => panic!("queue should initialize: {error}"),
    };

    let id = match queue.enqueue(DownloadRequest {
        url: "https://example.com/file.bin".to_string(),
        destination: PathBuf::from("delete.bin"),
        conflict: ConflictPolicy::AutoRename,
        integrity: IntegrityRule::None,
    }) {
        Ok(value) => value,
        Err(error) => panic!("enqueue should succeed: {error}"),
    };

    if let Err(error) = queue.delete(id) {
        panic!("delete should succeed: {error}");
    }

    let records = match queue.snapshot() {
        Ok(value) => value,
        Err(error) => panic!("snapshot should succeed: {error}"),
    };
    assert!(records.into_iter().all(|record| record.id != id));

    let persisted = match store.load_state() {
        Ok(value) => value,
        Err(error) => panic!("state should load: {error}"),
    };
    assert!(
        persisted
            .downloads
            .into_iter()
            .all(|record| record.id != id)
    );
}

#[test]
fn enqueue_infers_remote_file_name() {
    struct BackendWithName;

    impl DownloadBackend for BackendWithName {
        fn probe(&self, _request: &DownloadRequest) -> Result<ProbeInfo, NetError> {
            Ok(ProbeInfo {
                total_size: Some(10),
                accept_ranges: true,
                etag: None,
                last_modified: None,
                file_name: Some("remote.bin".to_string()),
            })
        }

        fn download(
            &self,
            _task: &DownloadTask,
            _on_progress: &mut dyn FnMut(ProgressSnapshot) -> Result<(), NetError>,
            _control: &dyn Fn() -> ControlSignal,
        ) -> Result<DownloadOutcome, NetError> {
            Ok(DownloadOutcome::Paused(ProgressSnapshot::default()))
        }
    }

    let store = Arc::new(MemoryStore::default());
    let backend = Arc::new(BackendWithName);
    let queue = match QueueService::new(backend, store, 1) {
        Ok(value) => value,
        Err(error) => panic!("queue should initialize: {error}"),
    };

    let id = match queue.enqueue(DownloadRequest {
        url: "https://example.com/path/from-url.bin".to_string(),
        destination: PathBuf::from("storage/downloads"),
        conflict: ConflictPolicy::AutoRename,
        integrity: IntegrityRule::None,
    }) {
        Ok(value) => value,
        Err(error) => panic!("enqueue should succeed: {error}"),
    };

    let record = match queue
        .snapshot()
        .ok()
        .and_then(|records| records.into_iter().find(|record| record.id == id))
    {
        Some(value) => value,
        None => panic!("record should exist"),
    };

    assert_eq!(
        record
            .request
            .destination
            .file_name()
            .and_then(|value| value.to_str()),
        Some("remote.bin")
    );
}

#[test]
fn enqueue_falls_back_to_url_file_name() {
    let store = Arc::new(MemoryStore::default());
    let backend = Arc::new(ImmediateBackend);
    let queue = match QueueService::new(backend, store, 1) {
        Ok(value) => value,
        Err(error) => panic!("queue should initialize: {error}"),
    };

    let id = match queue.enqueue(DownloadRequest {
        url: "https://example.com/path/from-url.bin".to_string(),
        destination: PathBuf::from("storage/downloads"),
        conflict: ConflictPolicy::AutoRename,
        integrity: IntegrityRule::None,
    }) {
        Ok(value) => value,
        Err(error) => panic!("enqueue should succeed: {error}"),
    };

    let record = match queue
        .snapshot()
        .ok()
        .and_then(|records| records.into_iter().find(|record| record.id == id))
    {
        Some(value) => value,
        None => panic!("record should exist"),
    };

    assert_eq!(
        record
            .request
            .destination
            .file_name()
            .and_then(|value| value.to_str()),
        Some("from-url.bin")
    );
}

#[test]
fn enqueue_uses_default_name_when_inference_missing() {
    let store = Arc::new(MemoryStore::default());
    let backend = Arc::new(ImmediateBackend);
    let queue = match QueueService::new(backend, store, 1) {
        Ok(value) => value,
        Err(error) => panic!("queue should initialize: {error}"),
    };

    let id = match queue.enqueue(DownloadRequest {
        url: "https://example.com/".to_string(),
        destination: PathBuf::from("storage/downloads"),
        conflict: ConflictPolicy::AutoRename,
        integrity: IntegrityRule::None,
    }) {
        Ok(value) => value,
        Err(error) => panic!("enqueue should succeed: {error}"),
    };

    let record = match queue
        .snapshot()
        .ok()
        .and_then(|records| records.into_iter().find(|record| record.id == id))
    {
        Some(value) => value,
        None => panic!("record should exist"),
    };

    assert_eq!(
        record
            .request
            .destination
            .file_name()
            .and_then(|value| value.to_str()),
        Some(DEFAULT_DOWNLOAD_FILE_NAME)
    );
}
