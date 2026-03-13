use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::error::NetError;
use crate::model::{
    ConflictPolicy, DownloadId, DownloadRequest, DownloadStatus, IntegrityRule, ProgressSnapshot,
};
use crate::queue::files::resolve_destination;
use crate::queue::{QueueConfig, QueueService};
use crate::store::{PersistedDownload, PersistedQueue, QueueStore};
use crate::transfer::{
    ControlSignal, ProbeInfo, TempLayout, Transfer, TransferOutcome, TransferTask, TransferUpdate,
};
use chrono::{TimeZone, Utc};

fn test_time(seconds: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0)
        .single()
        .unwrap_or_else(|| panic!("valid timestamp should be created for {seconds}"))
}

fn storage_dir(test_name: &str) -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let project_root = manifest_dir
        .parent()
        .and_then(|path| path.parent())
        .unwrap_or_else(|| panic!("crate manifest dir should be under project root"));

    project_root
        .join("storage")
        .join("tungsten-tests")
        .join(test_name)
}

#[derive(Default)]
struct MemoryStore {
    inner: Mutex<PersistedQueue>,
    save_calls: AtomicUsize,
}

impl QueueStore for MemoryStore {
    fn load_queue(&self) -> Result<PersistedQueue, NetError> {
        let state = self
            .inner
            .lock()
            .map_err(|error| NetError::State(format!("memory store poisoned: {error}")))?;
        Ok(state.clone())
    }

    fn save_queue(&self, state: &PersistedQueue) -> Result<(), NetError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|error| NetError::State(format!("memory store poisoned: {error}")))?;
        *guard = state.clone();
        self.save_calls.fetch_add(1, AtomicOrdering::SeqCst);
        Ok(())
    }
}

impl MemoryStore {
    fn save_calls(&self) -> usize {
        self.save_calls.load(AtomicOrdering::SeqCst)
    }
}

struct HoldingTransfer {
    started: Arc<AtomicBool>,
    release: Arc<AtomicBool>,
    progress: ProgressSnapshot,
    payload_len: usize,
}

impl Transfer for HoldingTransfer {
    fn probe(&self, _request: &DownloadRequest) -> Result<ProbeInfo, NetError> {
        Ok(ProbeInfo {
            total_size: self.progress.total,
            accept_ranges: true,
            etag: None,
            last_modified: None,
            file_name: None,
        })
    }

    fn download(
        &self,
        task: &TransferTask,
        _probe: Option<ProbeInfo>,
        on_update: &mut dyn FnMut(TransferUpdate) -> Result<(), NetError>,
        control: &dyn Fn() -> ControlSignal,
    ) -> Result<TransferOutcome, NetError> {
        on_update(TransferUpdate::from_progress(self.progress.clone()))?;
        self.started.store(true, AtomicOrdering::SeqCst);

        while !self.release.load(AtomicOrdering::SeqCst) {
            if !matches!(control(), ControlSignal::Run) {
                return Ok(TransferOutcome::Cancelled(TransferUpdate::default()));
            }
            thread::sleep(Duration::from_millis(20));
        }

        if let Some(parent) = task.temp_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&task.temp_path, vec![0u8; self.payload_len])?;

        Ok(TransferOutcome::Completed(TransferUpdate::from_progress(
            ProgressSnapshot {
                downloaded: self.payload_len as u64,
                total: Some(self.payload_len as u64),
                speed_bps: self.progress.speed_bps,
                eta_seconds: Some(0),
            },
        )))
    }
}

struct ImmediateTransfer;

impl Transfer for ImmediateTransfer {
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
        _task: &TransferTask,
        _probe: Option<ProbeInfo>,
        on_update: &mut dyn FnMut(TransferUpdate) -> Result<(), NetError>,
        control: &dyn Fn() -> ControlSignal,
    ) -> Result<TransferOutcome, NetError> {
        match control() {
            ControlSignal::Pause => Ok(TransferOutcome::Paused(TransferUpdate::default())),
            ControlSignal::Cancel => Ok(TransferOutcome::Cancelled(TransferUpdate::default())),
            ControlSignal::Run => {
                let update = TransferUpdate::from_progress(ProgressSnapshot {
                    downloaded: 10,
                    total: Some(10),
                    speed_bps: Some(10),
                    eta_seconds: Some(0),
                });
                on_update(update.clone())?;
                Ok(TransferOutcome::Completed(update))
            }
        }
    }
}

#[test]
fn auto_rename_when_destination_exists() {
    let temp =
        tempfile::tempdir().unwrap_or_else(|error| panic!("tempdir should be created: {error}"));
    let requested = temp.path().join("file.bin");
    fs::write(&requested, b"x")
        .unwrap_or_else(|error| panic!("test file should be created: {error}"));

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
    let transfer = Arc::new(ImmediateTransfer);
    let queue = QueueService::with_transfer(QueueConfig::new(3, 1), transfer, store.clone())
        .unwrap_or_else(|error| panic!("queue should initialize: {error}"));

    let request = DownloadRequest::new(
        "https://example.com/file.bin".to_string(),
        storage_dir("enqueue_persists_state").join("file.bin"),
        ConflictPolicy::AutoRename,
        IntegrityRule::None,
    );

    let id = queue
        .enqueue(request)
        .unwrap_or_else(|error| panic!("enqueue should succeed: {error}"));
    let snapshot = store
        .load_queue()
        .unwrap_or_else(|error| panic!("state should load: {error}"));

    assert_eq!(id.0, 1);
    assert_eq!(snapshot.downloads.len(), 1);
    assert!(
        matches!(
            snapshot.downloads[0].status,
            DownloadStatus::Queued | DownloadStatus::Running
        ),
        "status should be queued or running, got {:?}",
        snapshot.downloads[0].status
    );
}

#[test]
fn progress_updates_are_coalesced() {
    struct MultiProgressTransfer;

    impl Transfer for MultiProgressTransfer {
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
            task: &TransferTask,
            _probe: Option<ProbeInfo>,
            on_update: &mut dyn FnMut(TransferUpdate) -> Result<(), NetError>,
            control: &dyn Fn() -> ControlSignal,
        ) -> Result<TransferOutcome, NetError> {
            if !matches!(control(), ControlSignal::Run) {
                return Ok(TransferOutcome::Cancelled(TransferUpdate::default()));
            }

            if let Some(parent) = task.temp_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&task.temp_path, vec![0u8; 100])?;

            for downloaded in [20, 40, 60, 80, 100] {
                on_update(TransferUpdate::from_progress(ProgressSnapshot {
                    downloaded,
                    total: Some(100),
                    speed_bps: Some(100),
                    eta_seconds: Some(0),
                }))?;
            }

            Ok(TransferOutcome::Completed(TransferUpdate::from_progress(
                ProgressSnapshot {
                    downloaded: 100,
                    total: Some(100),
                    speed_bps: Some(100),
                    eta_seconds: Some(0),
                },
            )))
        }
    }

    let temp =
        tempfile::tempdir().unwrap_or_else(|error| panic!("tempdir should be created: {error}"));
    let store = Arc::new(MemoryStore::default());
    let transfer = Arc::new(MultiProgressTransfer);
    let queue = QueueService::with_transfer(QueueConfig::new(1, 1), transfer, store.clone())
        .unwrap_or_else(|error| panic!("queue should initialize: {error}"));

    queue
        .enqueue(DownloadRequest::new(
            "https://example.com/file.bin".to_string(),
            temp.path().join("checkpoint.bin"),
            ConflictPolicy::AutoRename,
            IntegrityRule::None,
        ))
        .unwrap_or_else(|error| panic!("enqueue should succeed: {error}"));

    let started = Instant::now();
    loop {
        let records = queue
            .snapshot()
            .unwrap_or_else(|error| panic!("snapshot should succeed: {error}"));

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

    assert!(store.save_calls() < 10);
}

#[test]
fn resume_reuses_paused_speed_baseline() {
    #[derive(Default)]
    struct ResumeAwareTransfer {
        calls: AtomicUsize,
        seen_resume_speeds: Mutex<Vec<Option<u64>>>,
    }

    impl ResumeAwareTransfer {
        fn seen_resume_speeds(&self) -> Vec<Option<u64>> {
            self.seen_resume_speeds
                .lock()
                .unwrap_or_else(|error| panic!("resume speeds should lock: {error}"))
                .clone()
        }
    }

    impl Transfer for ResumeAwareTransfer {
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
            task: &TransferTask,
            _probe: Option<ProbeInfo>,
            _on_update: &mut dyn FnMut(TransferUpdate) -> Result<(), NetError>,
            control: &dyn Fn() -> ControlSignal,
        ) -> Result<TransferOutcome, NetError> {
            if !matches!(control(), ControlSignal::Run) {
                return Ok(TransferOutcome::Cancelled(TransferUpdate::default()));
            }

            self.seen_resume_speeds
                .lock()
                .unwrap_or_else(|error| panic!("resume speeds should lock: {error}"))
                .push(task.resume_speed_bps);

            let call = self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            if call == 0 {
                return Ok(TransferOutcome::Paused(TransferUpdate::from_progress(
                    ProgressSnapshot {
                        downloaded: 50,
                        total: Some(100),
                        speed_bps: Some(25),
                        eta_seconds: Some(2),
                    },
                )));
            }

            if let Some(parent) = task.temp_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&task.temp_path, vec![0u8; 100])?;

            Ok(TransferOutcome::Completed(TransferUpdate::from_progress(
                ProgressSnapshot {
                    downloaded: 100,
                    total: Some(100),
                    speed_bps: task.resume_speed_bps,
                    eta_seconds: Some(0),
                },
            )))
        }
    }

    let temp =
        tempfile::tempdir().unwrap_or_else(|error| panic!("tempdir should be created: {error}"));
    let store = Arc::new(MemoryStore::default());
    let transfer = Arc::new(ResumeAwareTransfer::default());
    let queue = QueueService::with_transfer(QueueConfig::new(1, 1), transfer.clone(), store)
        .unwrap_or_else(|error| panic!("queue should initialize: {error}"));

    let id = queue
        .enqueue(DownloadRequest::new(
            "https://example.com/file.bin".to_string(),
            temp.path().join("resume-baseline.bin"),
            ConflictPolicy::AutoRename,
            IntegrityRule::None,
        ))
        .unwrap_or_else(|error| panic!("enqueue should succeed: {error}"));

    let started = Instant::now();
    loop {
        let record = queue
            .snapshot()
            .unwrap_or_else(|error| panic!("snapshot should succeed: {error}"))
            .into_iter()
            .find(|record| record.id == id)
            .unwrap_or_else(|| panic!("record should exist"));

        if matches!(record.status, DownloadStatus::Paused) {
            break;
        }

        if started.elapsed() > Duration::from_secs(2) {
            panic!("download should reach paused state");
        }

        thread::sleep(Duration::from_millis(20));
    }

    queue
        .resume(id)
        .unwrap_or_else(|error| panic!("resume should succeed: {error}"));

    let resumed = Instant::now();
    loop {
        let record = queue
            .snapshot()
            .unwrap_or_else(|error| panic!("snapshot should succeed: {error}"))
            .into_iter()
            .find(|record| record.id == id)
            .unwrap_or_else(|| panic!("record should exist"));

        if matches!(record.status, DownloadStatus::Completed) {
            break;
        }

        if resumed.elapsed() > Duration::from_secs(2) {
            panic!("download should complete after resume");
        }

        thread::sleep(Duration::from_millis(20));
    }

    assert_eq!(transfer.seen_resume_speeds(), vec![None, Some(25)]);
}

#[test]
fn running_download_observes_live_global_limit_updates() {
    struct ObservingLimitTransfer {
        seen: Arc<Mutex<Vec<u64>>>,
    }

    impl Transfer for ObservingLimitTransfer {
        fn probe(&self, _request: &DownloadRequest) -> Result<ProbeInfo, NetError> {
            Ok(ProbeInfo {
                total_size: Some(32),
                accept_ranges: true,
                etag: None,
                last_modified: None,
                file_name: Some("observed.bin".to_string()),
            })
        }

        fn download(
            &self,
            task: &TransferTask,
            _probe: Option<ProbeInfo>,
            on_update: &mut dyn FnMut(TransferUpdate) -> Result<(), NetError>,
            control: &dyn Fn() -> ControlSignal,
        ) -> Result<TransferOutcome, NetError> {
            if let Some(parent) = task.temp_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&task.temp_path, vec![0u8; 32])?;

            for step in 0..8 {
                if !matches!(control(), ControlSignal::Run) {
                    return Ok(TransferOutcome::Cancelled(TransferUpdate::default()));
                }

                self.seen
                    .lock()
                    .unwrap_or_else(|error| panic!("seen lock should be available: {error}"))
                    .push(task.speed_limit.current_kbps());
                on_update(TransferUpdate::from_progress(ProgressSnapshot {
                    downloaded: ((step + 1) * 4) as u64,
                    total: Some(32),
                    speed_bps: Some(4096),
                    eta_seconds: Some((7 - step) as u64),
                }))?;
                thread::sleep(Duration::from_millis(50));
            }

            Ok(TransferOutcome::Completed(TransferUpdate::from_progress(
                ProgressSnapshot {
                    downloaded: 32,
                    total: Some(32),
                    speed_bps: Some(4096),
                    eta_seconds: Some(0),
                },
            )))
        }
    }

    let temp =
        tempfile::tempdir().unwrap_or_else(|error| panic!("tempdir should be created: {error}"));
    let store = Arc::new(MemoryStore::default());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let transfer = Arc::new(ObservingLimitTransfer {
        seen: Arc::clone(&seen),
    });
    let queue = QueueService::with_transfer(
        QueueConfig::new(1, 1).download_limit_kbps(64),
        transfer,
        store,
    )
    .unwrap_or_else(|error| panic!("queue should initialize: {error}"));

    queue
        .enqueue(DownloadRequest::new(
            "https://example.com/file.bin".to_string(),
            temp.path(),
            ConflictPolicy::AutoRename,
            IntegrityRule::None,
        ))
        .unwrap_or_else(|error| panic!("enqueue should succeed: {error}"));

    let started = Instant::now();
    loop {
        if seen
            .lock()
            .unwrap_or_else(|error| panic!("seen lock should be available: {error}"))
            .len()
            >= 2
        {
            break;
        }

        if started.elapsed() > Duration::from_secs(2) {
            panic!("download should start reporting observed limits");
        }

        thread::sleep(Duration::from_millis(20));
    }

    queue
        .set_download_limit(8)
        .unwrap_or_else(|error| panic!("download limit should update: {error}"));

    let started = Instant::now();
    loop {
        let completed = queue
            .snapshot()
            .unwrap_or_else(|error| panic!("snapshot should succeed: {error}"))
            .into_iter()
            .any(|record| matches!(record.status, DownloadStatus::Completed));

        if completed {
            break;
        }

        if started.elapsed() > Duration::from_secs(2) {
            panic!("download should complete");
        }

        thread::sleep(Duration::from_millis(20));
    }

    let seen = seen
        .lock()
        .unwrap_or_else(|error| panic!("seen lock should be available: {error}"))
        .clone();
    assert!(
        seen.contains(&64),
        "expected to observe initial limit, got {seen:?}"
    );
    assert!(
        seen.contains(&8),
        "expected to observe updated limit, got {seen:?}"
    );
}

#[test]
fn running_download_observes_live_per_download_limit_updates() {
    struct ObservingLimitTransfer {
        seen: Arc<Mutex<Vec<u64>>>,
    }

    impl Transfer for ObservingLimitTransfer {
        fn probe(&self, _request: &DownloadRequest) -> Result<ProbeInfo, NetError> {
            Ok(ProbeInfo {
                total_size: Some(32),
                accept_ranges: true,
                etag: None,
                last_modified: None,
                file_name: Some("override-live.bin".to_string()),
            })
        }

        fn download(
            &self,
            task: &TransferTask,
            _probe: Option<ProbeInfo>,
            on_update: &mut dyn FnMut(TransferUpdate) -> Result<(), NetError>,
            control: &dyn Fn() -> ControlSignal,
        ) -> Result<TransferOutcome, NetError> {
            if let Some(parent) = task.temp_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&task.temp_path, vec![0u8; 32])?;

            for step in 0..8 {
                if !matches!(control(), ControlSignal::Run) {
                    return Ok(TransferOutcome::Cancelled(TransferUpdate::default()));
                }

                self.seen
                    .lock()
                    .unwrap_or_else(|error| panic!("seen lock should be available: {error}"))
                    .push(task.speed_limit.current_kbps());
                on_update(TransferUpdate::from_progress(ProgressSnapshot {
                    downloaded: ((step + 1) * 4) as u64,
                    total: Some(32),
                    speed_bps: Some(4096),
                    eta_seconds: Some((7 - step) as u64),
                }))?;
                thread::sleep(Duration::from_millis(50));
            }

            Ok(TransferOutcome::Completed(TransferUpdate::from_progress(
                ProgressSnapshot {
                    downloaded: 32,
                    total: Some(32),
                    speed_bps: Some(4096),
                    eta_seconds: Some(0),
                },
            )))
        }
    }

    let temp =
        tempfile::tempdir().unwrap_or_else(|error| panic!("tempdir should be created: {error}"));
    let store = Arc::new(MemoryStore::default());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let transfer = Arc::new(ObservingLimitTransfer {
        seen: Arc::clone(&seen),
    });
    let queue = QueueService::with_transfer(
        QueueConfig::new(1, 1).download_limit_kbps(64),
        transfer,
        store.clone(),
    )
    .unwrap_or_else(|error| panic!("queue should initialize: {error}"));

    let download_id = queue
        .enqueue(DownloadRequest::new(
            "https://example.com/file.bin".to_string(),
            temp.path(),
            ConflictPolicy::AutoRename,
            IntegrityRule::None,
        ))
        .unwrap_or_else(|error| panic!("enqueue should succeed: {error}"));

    let started = Instant::now();
    loop {
        if seen
            .lock()
            .unwrap_or_else(|error| panic!("seen lock should be available: {error}"))
            .len()
            >= 2
        {
            break;
        }

        if started.elapsed() > Duration::from_secs(2) {
            panic!("download should start reporting observed limits");
        }

        thread::sleep(Duration::from_millis(20));
    }

    queue
        .set_speed_limit(download_id, Some(16))
        .unwrap_or_else(|error| panic!("per-download limit should update: {error}"));

    let started = Instant::now();
    loop {
        let completed = queue
            .snapshot()
            .unwrap_or_else(|error| panic!("snapshot should succeed: {error}"))
            .into_iter()
            .find(|record| record.id == download_id);

        if let Some(record) = completed
            && matches!(record.status, DownloadStatus::Completed)
        {
            assert_eq!(record.request.speed_limit_kbps, Some(16));
            break;
        }

        if started.elapsed() > Duration::from_secs(2) {
            panic!("download should complete");
        }

        thread::sleep(Duration::from_millis(20));
    }

    let persisted = store
        .load_queue()
        .unwrap_or_else(|error| panic!("state should load: {error}"));
    let persisted_limit = persisted
        .downloads
        .into_iter()
        .find(|record| record.id == download_id)
        .and_then(|record| record.request.speed_limit_kbps);
    assert_eq!(persisted_limit, Some(16));

    let seen = seen
        .lock()
        .unwrap_or_else(|error| panic!("seen lock should be available: {error}"))
        .clone();
    assert!(
        seen.contains(&64),
        "expected to observe global limit first, got {seen:?}"
    );
    assert!(
        seen.contains(&16),
        "expected to observe live override, got {seen:?}"
    );
}

#[test]
fn set_download_limit_keeps_running_progress_snapshot() {
    let temp =
        tempfile::tempdir().unwrap_or_else(|error| panic!("tempdir should be created: {error}"));
    let store = Arc::new(MemoryStore::default());
    let started = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let transfer = Arc::new(HoldingTransfer {
        started: Arc::clone(&started),
        release: Arc::clone(&release),
        progress: ProgressSnapshot {
            downloaded: 64 * 1024,
            total: Some(1024 * 1024),
            speed_bps: Some(64 * 1024),
            eta_seconds: Some(15),
        },
        payload_len: 1024 * 1024,
    });
    let queue = QueueService::with_transfer(
        QueueConfig::new(1, 1).download_limit_kbps(64),
        transfer,
        store,
    )
    .unwrap_or_else(|error| panic!("queue should initialize: {error}"));

    let download_id = queue
        .enqueue(DownloadRequest::new(
            "https://example.com/file.bin".to_string(),
            temp.path().join("global-limit.bin"),
            ConflictPolicy::AutoRename,
            IntegrityRule::None,
        ))
        .unwrap_or_else(|error| panic!("enqueue should succeed: {error}"));

    let started_at = Instant::now();
    loop {
        let record = queue
            .snapshot()
            .unwrap_or_else(|error| panic!("snapshot should succeed: {error}"))
            .into_iter()
            .find(|record| record.id == download_id);

        if let Some(record) = record
            && started.load(AtomicOrdering::SeqCst)
            && matches!(record.status, DownloadStatus::Running)
            && record.progress.speed_bps == Some(64 * 1024)
        {
            break;
        }

        if started_at.elapsed() > Duration::from_secs(2) {
            panic!("running download should expose initial progress");
        }

        thread::sleep(Duration::from_millis(20));
    }

    queue
        .set_download_limit(16)
        .unwrap_or_else(|error| panic!("download limit should update: {error}"));

    let record = queue
        .snapshot()
        .unwrap_or_else(|error| panic!("snapshot should succeed: {error}"))
        .into_iter()
        .find(|record| record.id == download_id)
        .unwrap_or_else(|| panic!("download should still exist"));
    assert_eq!(record.progress.speed_bps, Some(64 * 1024));
    assert_eq!(record.progress.eta_seconds, Some(15));

    release.store(true, AtomicOrdering::SeqCst);

    let started_at = Instant::now();
    loop {
        let completed = queue
            .snapshot()
            .unwrap_or_else(|error| panic!("snapshot should succeed: {error}"))
            .into_iter()
            .find(|record| record.id == download_id);

        if let Some(record) = completed
            && matches!(record.status, DownloadStatus::Completed)
        {
            break;
        }

        if started_at.elapsed() > Duration::from_secs(2) {
            panic!("download should complete");
        }

        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn set_speed_limit_recalculates_running_progress_immediately() {
    let temp =
        tempfile::tempdir().unwrap_or_else(|error| panic!("tempdir should be created: {error}"));
    let store = Arc::new(MemoryStore::default());
    let started = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let transfer = Arc::new(HoldingTransfer {
        started: Arc::clone(&started),
        release: Arc::clone(&release),
        progress: ProgressSnapshot {
            downloaded: 64 * 1024,
            total: Some(1024 * 1024),
            speed_bps: Some(64 * 1024),
            eta_seconds: Some(15),
        },
        payload_len: 1024 * 1024,
    });
    let queue = QueueService::with_transfer(QueueConfig::new(1, 1), transfer, store)
        .unwrap_or_else(|error| panic!("queue should initialize: {error}"));

    let download_id = queue
        .enqueue(DownloadRequest::new(
            "https://example.com/file.bin".to_string(),
            temp.path().join("per-download-limit.bin"),
            ConflictPolicy::AutoRename,
            IntegrityRule::None,
        ))
        .unwrap_or_else(|error| panic!("enqueue should succeed: {error}"));

    let started_at = Instant::now();
    loop {
        let record = queue
            .snapshot()
            .unwrap_or_else(|error| panic!("snapshot should succeed: {error}"))
            .into_iter()
            .find(|record| record.id == download_id);

        if let Some(record) = record
            && started.load(AtomicOrdering::SeqCst)
            && matches!(record.status, DownloadStatus::Running)
            && record.progress.speed_bps == Some(64 * 1024)
        {
            break;
        }

        if started_at.elapsed() > Duration::from_secs(2) {
            panic!("running download should expose initial progress");
        }

        thread::sleep(Duration::from_millis(20));
    }

    queue
        .set_speed_limit(download_id, Some(16))
        .unwrap_or_else(|error| panic!("per-download limit should update: {error}"));

    let record = queue
        .snapshot()
        .unwrap_or_else(|error| panic!("snapshot should succeed: {error}"))
        .into_iter()
        .find(|record| record.id == download_id)
        .unwrap_or_else(|| panic!("download should still exist"));
    assert_eq!(record.request.speed_limit_kbps, Some(16));
    assert_eq!(record.progress.speed_bps, Some(16 * 1024));
    assert_eq!(record.progress.eta_seconds, Some(60));

    release.store(true, AtomicOrdering::SeqCst);

    let started_at = Instant::now();
    loop {
        let completed = queue
            .snapshot()
            .unwrap_or_else(|error| panic!("snapshot should succeed: {error}"))
            .into_iter()
            .find(|record| record.id == download_id);

        if let Some(record) = completed
            && matches!(record.status, DownloadStatus::Completed)
        {
            break;
        }

        if started_at.elapsed() > Duration::from_secs(2) {
            panic!("download should complete");
        }

        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn per_download_override_replaces_global_limit() {
    struct ObservingLimitTransfer {
        seen: Arc<Mutex<Vec<u64>>>,
    }

    impl Transfer for ObservingLimitTransfer {
        fn probe(&self, _request: &DownloadRequest) -> Result<ProbeInfo, NetError> {
            Ok(ProbeInfo {
                total_size: Some(32),
                accept_ranges: true,
                etag: None,
                last_modified: None,
                file_name: Some("override.bin".to_string()),
            })
        }

        fn download(
            &self,
            task: &TransferTask,
            _probe: Option<ProbeInfo>,
            on_update: &mut dyn FnMut(TransferUpdate) -> Result<(), NetError>,
            control: &dyn Fn() -> ControlSignal,
        ) -> Result<TransferOutcome, NetError> {
            if let Some(parent) = task.temp_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&task.temp_path, vec![0u8; 32])?;

            for step in 0..8 {
                if !matches!(control(), ControlSignal::Run) {
                    return Ok(TransferOutcome::Cancelled(TransferUpdate::default()));
                }

                self.seen
                    .lock()
                    .unwrap_or_else(|error| panic!("seen lock should be available: {error}"))
                    .push(task.speed_limit.current_kbps());
                on_update(TransferUpdate::from_progress(ProgressSnapshot {
                    downloaded: ((step + 1) * 4) as u64,
                    total: Some(32),
                    speed_bps: Some(4096),
                    eta_seconds: Some((7 - step) as u64),
                }))?;
                thread::sleep(Duration::from_millis(50));
            }

            Ok(TransferOutcome::Completed(TransferUpdate::from_progress(
                ProgressSnapshot {
                    downloaded: 32,
                    total: Some(32),
                    speed_bps: Some(4096),
                    eta_seconds: Some(0),
                },
            )))
        }
    }

    let temp =
        tempfile::tempdir().unwrap_or_else(|error| panic!("tempdir should be created: {error}"));
    let store = Arc::new(MemoryStore::default());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let transfer = Arc::new(ObservingLimitTransfer {
        seen: Arc::clone(&seen),
    });
    let queue = QueueService::with_transfer(
        QueueConfig::new(1, 1).download_limit_kbps(64),
        transfer,
        store,
    )
    .unwrap_or_else(|error| panic!("queue should initialize: {error}"));

    queue
        .enqueue(
            DownloadRequest::new(
                "https://example.com/file.bin".to_string(),
                temp.path(),
                ConflictPolicy::AutoRename,
                IntegrityRule::None,
            )
            .speed_limit_kbps(Some(16)),
        )
        .unwrap_or_else(|error| panic!("enqueue should succeed: {error}"));

    let started = Instant::now();
    loop {
        if seen
            .lock()
            .unwrap_or_else(|error| panic!("seen lock should be available: {error}"))
            .len()
            >= 2
        {
            break;
        }

        if started.elapsed() > Duration::from_secs(2) {
            panic!("download should start reporting observed limits");
        }

        thread::sleep(Duration::from_millis(20));
    }

    queue
        .set_download_limit(8)
        .unwrap_or_else(|error| panic!("download limit should update: {error}"));

    let started = Instant::now();
    loop {
        let completed = queue
            .snapshot()
            .unwrap_or_else(|error| panic!("snapshot should succeed: {error}"))
            .into_iter()
            .any(|record| matches!(record.status, DownloadStatus::Completed));

        if completed {
            break;
        }

        if started.elapsed() > Duration::from_secs(2) {
            panic!("download should complete");
        }

        thread::sleep(Duration::from_millis(20));
    }

    let seen = seen
        .lock()
        .unwrap_or_else(|error| panic!("seen lock should be available: {error}"))
        .clone();
    assert!(
        seen.iter().all(|value| *value == 16),
        "override should replace global limit, got {seen:?}"
    );
}

#[test]
fn retry_moves_failed_to_queued() {
    let store = Arc::new(MemoryStore::default());
    let transfer = Arc::new(ImmediateTransfer);
    let queue = QueueService::with_transfer(QueueConfig::new(3, 1), transfer, store.clone())
        .unwrap_or_else(|error| panic!("queue should initialize: {error}"));

    let id = queue
        .enqueue(DownloadRequest::new(
            "https://example.com/file.bin".to_string(),
            storage_dir("retry_moves_failed_to_queued").join("retry.bin"),
            ConflictPolicy::AutoRename,
            IntegrityRule::None,
        ))
        .unwrap_or_else(|error| panic!("enqueue should succeed: {error}"));

    {
        let mut state = queue
            .shared
            .state
            .lock()
            .unwrap_or_else(|error| panic!("queue lock should be available: {error}"));
        let record = state
            .downloads
            .get_mut(&id)
            .unwrap_or_else(|| panic!("record should exist"));
        record.status = DownloadStatus::Failed;
        record.error = Some("network error".to_string());
    }

    queue
        .retry(id)
        .unwrap_or_else(|error| panic!("retry should succeed: {error}"));

    let records = queue
        .snapshot()
        .unwrap_or_else(|error| panic!("snapshot should succeed: {error}"));
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
    let transfer = Arc::new(ImmediateTransfer);
    let queue = QueueService::with_transfer(QueueConfig::new(3, 1), transfer, store.clone())
        .unwrap_or_else(|error| panic!("queue should initialize: {error}"));

    let id = queue
        .enqueue(DownloadRequest::new(
            "https://example.com/file.bin".to_string(),
            storage_dir("delete_removes_queued_download").join("delete.bin"),
            ConflictPolicy::AutoRename,
            IntegrityRule::None,
        ))
        .unwrap_or_else(|error| panic!("enqueue should succeed: {error}"));

    queue
        .delete(id)
        .unwrap_or_else(|error| panic!("delete should succeed: {error}"));

    let records = queue
        .snapshot()
        .unwrap_or_else(|error| panic!("snapshot should succeed: {error}"));
    assert!(records.into_iter().all(|record| record.id != id));

    let persisted = store
        .load_queue()
        .unwrap_or_else(|error| panic!("state should load: {error}"));
    assert!(
        persisted
            .downloads
            .into_iter()
            .all(|record| record.id != id)
    );
}

#[test]
fn probe_uses_server_file_name_for_inferred_destination() {
    struct TransferWithName;

    impl Transfer for TransferWithName {
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
            _task: &TransferTask,
            _probe: Option<ProbeInfo>,
            _on_update: &mut dyn FnMut(TransferUpdate) -> Result<(), NetError>,
            _control: &dyn Fn() -> ControlSignal,
        ) -> Result<TransferOutcome, NetError> {
            Ok(TransferOutcome::Paused(TransferUpdate::default()))
        }
    }

    let store = Arc::new(MemoryStore::default());
    let transfer = Arc::new(TransferWithName);
    let queue = QueueService::with_transfer(QueueConfig::new(1, 1), transfer, store)
        .unwrap_or_else(|error| panic!("queue should initialize: {error}"));

    let id = queue
        .enqueue(DownloadRequest::new(
            "https://example.com/path/from-url.bin".to_string(),
            storage_dir("probe_uses_server_file_name_for_inferred_destination"),
            ConflictPolicy::AutoRename,
            IntegrityRule::None,
        ))
        .unwrap_or_else(|error| panic!("enqueue should succeed: {error}"));

    let started = Instant::now();
    loop {
        let record = queue
            .snapshot()
            .ok()
            .and_then(|records| records.into_iter().find(|record| record.id == id))
            .unwrap_or_else(|| panic!("record should exist"));

        if matches!(record.status, DownloadStatus::Paused) {
            assert_eq!(
                record
                    .destination
                    .as_ref()
                    .and_then(|path| path.file_name())
                    .and_then(|value| value.to_str()),
                Some("remote.bin")
            );
            break;
        }

        if started.elapsed() > Duration::from_secs(2) {
            panic!("download should reach paused state");
        }

        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn enqueue_returns_without_waiting_for_probe() {
    struct SlowProbeTransfer;

    impl Transfer for SlowProbeTransfer {
        fn probe(&self, _request: &DownloadRequest) -> Result<ProbeInfo, NetError> {
            thread::sleep(Duration::from_millis(800));
            Ok(ProbeInfo::default())
        }

        fn download(
            &self,
            _task: &TransferTask,
            _probe: Option<ProbeInfo>,
            _on_update: &mut dyn FnMut(TransferUpdate) -> Result<(), NetError>,
            _control: &dyn Fn() -> ControlSignal,
        ) -> Result<TransferOutcome, NetError> {
            Ok(TransferOutcome::Paused(TransferUpdate::default()))
        }
    }

    let store = Arc::new(MemoryStore::default());
    let transfer = Arc::new(SlowProbeTransfer);
    let queue = QueueService::with_transfer(QueueConfig::new(1, 1), transfer, store)
        .unwrap_or_else(|error| panic!("queue should initialize: {error}"));

    let started = Instant::now();
    let id = queue
        .enqueue(DownloadRequest::new(
            "https://example.com/path/file.bin".to_string(),
            storage_dir("enqueue_returns_without_waiting_for_probe"),
            ConflictPolicy::AutoRename,
            IntegrityRule::None,
        ))
        .unwrap_or_else(|error| panic!("enqueue should succeed quickly: {error}"));

    assert!(
        started.elapsed() < Duration::from_millis(300),
        "enqueue should not block on probe; elapsed={:?}",
        started.elapsed()
    );

    let record = queue
        .snapshot()
        .unwrap_or_else(|error| panic!("snapshot should succeed: {error}"))
        .into_iter()
        .find(|record| record.id == id)
        .unwrap_or_else(|| panic!("record should exist"));
    assert!(matches!(record.status, DownloadStatus::Queued));
    assert!(record.destination.is_none());
}

#[test]
fn probe_failure_falls_back_and_downloads() {
    struct FailingProbeTransfer;

    impl Transfer for FailingProbeTransfer {
        fn probe(&self, _request: &DownloadRequest) -> Result<ProbeInfo, NetError> {
            Err(NetError::Backend("probe down".to_string()))
        }

        fn download(
            &self,
            _task: &TransferTask,
            _probe: Option<ProbeInfo>,
            _on_update: &mut dyn FnMut(TransferUpdate) -> Result<(), NetError>,
            control: &dyn Fn() -> ControlSignal,
        ) -> Result<TransferOutcome, NetError> {
            if !matches!(control(), ControlSignal::Run) {
                return Ok(TransferOutcome::Cancelled(TransferUpdate::default()));
            }

            Ok(TransferOutcome::Paused(TransferUpdate::default()))
        }
    }

    let store = Arc::new(MemoryStore::default());
    let transfer = Arc::new(FailingProbeTransfer);
    let queue = QueueService::with_transfer(QueueConfig::new(1, 1), transfer, store)
        .unwrap_or_else(|error| panic!("queue should initialize: {error}"));

    let id = queue
        .enqueue(DownloadRequest::new(
            "https://example.com/path/file.bin".to_string(),
            storage_dir("probe_failure_falls_back_and_downloads"),
            ConflictPolicy::AutoRename,
            IntegrityRule::None,
        ))
        .unwrap_or_else(|error| panic!("enqueue should succeed: {error}"));

    let started = Instant::now();
    loop {
        let status = queue
            .snapshot()
            .unwrap_or_else(|error| panic!("snapshot should succeed: {error}"))
            .into_iter()
            .find(|record| record.id == id)
            .map(|record| (record.status, record.error, record.destination));

        if let Some((DownloadStatus::Paused, None, Some(destination))) = status {
            assert!(
                destination
                    .file_name()
                    .and_then(|value| value.to_str())
                    .is_some_and(|value| value == "file.bin"),
                "fallback destination should use URL path name"
            );
            break;
        }

        if started.elapsed() > Duration::from_secs(2) {
            panic!("download should continue when probe fails");
        }

        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn probe_does_not_rename_when_partial_data_exists() {
    struct TransferWithName;

    impl Transfer for TransferWithName {
        fn probe(&self, _request: &DownloadRequest) -> Result<ProbeInfo, NetError> {
            Ok(ProbeInfo {
                total_size: Some(64),
                accept_ranges: true,
                etag: None,
                last_modified: None,
                file_name: Some("remote.bin".to_string()),
            })
        }

        fn download(
            &self,
            _task: &TransferTask,
            _probe: Option<ProbeInfo>,
            _on_update: &mut dyn FnMut(TransferUpdate) -> Result<(), NetError>,
            _control: &dyn Fn() -> ControlSignal,
        ) -> Result<TransferOutcome, NetError> {
            Ok(TransferOutcome::Paused(TransferUpdate::default()))
        }
    }

    let temp =
        tempfile::tempdir().unwrap_or_else(|error| panic!("tempdir should be created: {error}"));
    let destination = temp.path().join(super::DEFAULT_DOWNLOAD_FILE_NAME);
    let temp_path = temp.path().join("download.bin.1.part");
    fs::write(&temp_path, vec![0u8; 32])
        .unwrap_or_else(|error| panic!("temp part should be created: {error}"));

    let store = Arc::new(MemoryStore {
        inner: Mutex::new(PersistedQueue {
            next_id: 2,
            downloads: vec![PersistedDownload {
                id: DownloadId(1),
                request: DownloadRequest::new(
                    "https://example.com/path/from-url.bin".to_string(),
                    temp.path(),
                    ConflictPolicy::AutoRename,
                    IntegrityRule::None,
                ),
                destination: Some(destination.clone()),
                loaded_from_store: true,
                temp_path,
                temp_layout: TempLayout::Single,
                supports_resume: true,
                status: DownloadStatus::Queued,
                progress: ProgressSnapshot {
                    downloaded: 32,
                    total: Some(64),
                    speed_bps: None,
                    eta_seconds: None,
                },
                error: None,
                etag: None,
                last_modified: None,
                created_at: test_time(0),
                updated_at: test_time(1),
            }],
        }),
        save_calls: AtomicUsize::new(0),
    });
    let queue =
        QueueService::with_transfer(QueueConfig::new(1, 1), Arc::new(TransferWithName), store)
            .unwrap_or_else(|error| panic!("queue should initialize: {error}"));

    let started = Instant::now();
    loop {
        let record = queue
            .snapshot()
            .unwrap_or_else(|error| panic!("snapshot should succeed: {error}"))
            .into_iter()
            .find(|record| record.id == DownloadId(1))
            .unwrap_or_else(|| panic!("record should exist"));

        if matches!(record.status, DownloadStatus::Paused) {
            assert_eq!(
                record
                    .destination
                    .as_ref()
                    .and_then(|path| path.file_name())
                    .and_then(|value| value.to_str()),
                Some(super::DEFAULT_DOWNLOAD_FILE_NAME)
            );
            break;
        }

        if started.elapsed() > Duration::from_secs(2) {
            panic!("resumed download should reach paused state");
        }

        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn enqueue_uses_url_path_name_when_server_name_missing() {
    let store = Arc::new(MemoryStore::default());
    let transfer = Arc::new(ImmediateTransfer);
    let queue = QueueService::with_transfer(QueueConfig::new(1, 1), transfer, store)
        .unwrap_or_else(|error| panic!("queue should initialize: {error}"));

    let id = queue
        .enqueue(DownloadRequest::new(
            "https://example.com/path/from-url.bin".to_string(),
            storage_dir("enqueue_uses_url_path_name_when_server_name_missing"),
            ConflictPolicy::AutoRename,
            IntegrityRule::None,
        ))
        .unwrap_or_else(|error| panic!("enqueue should succeed: {error}"));

    let started = Instant::now();
    loop {
        let record = queue
            .snapshot()
            .ok()
            .and_then(|records| records.into_iter().find(|record| record.id == id))
            .unwrap_or_else(|| panic!("record should exist"));

        if record.destination.is_some() {
            assert_eq!(
                record
                    .destination
                    .as_ref()
                    .and_then(|path| path.file_name())
                    .and_then(|value| value.to_str()),
                Some("from-url.bin")
            );
            break;
        }

        if started.elapsed() > Duration::from_secs(2) {
            panic!("destination should be resolved");
        }

        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn enqueue_uses_default_name_when_inference_missing() {
    let store = Arc::new(MemoryStore::default());
    let transfer = Arc::new(ImmediateTransfer);
    let queue = QueueService::with_transfer(QueueConfig::new(1, 1), transfer, store)
        .unwrap_or_else(|error| panic!("queue should initialize: {error}"));

    let id = queue
        .enqueue(DownloadRequest::new(
            "https://example.com/".to_string(),
            storage_dir("enqueue_uses_default_name_when_inference_missing"),
            ConflictPolicy::AutoRename,
            IntegrityRule::None,
        ))
        .unwrap_or_else(|error| panic!("enqueue should succeed: {error}"));

    let started = Instant::now();
    loop {
        let record = queue
            .snapshot()
            .ok()
            .and_then(|records| records.into_iter().find(|record| record.id == id))
            .unwrap_or_else(|| panic!("record should exist"));

        if record.destination.is_some() {
            assert_eq!(
                record
                    .destination
                    .as_ref()
                    .and_then(|path| path.file_name())
                    .and_then(|value| value.to_str()),
                Some(super::DEFAULT_DOWNLOAD_FILE_NAME)
            );
            break;
        }

        if started.elapsed() > Duration::from_secs(2) {
            panic!("destination should be resolved");
        }

        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn public_snapshot_hides_transfer_internals() {
    let store = Arc::new(MemoryStore {
        inner: Mutex::new(PersistedQueue {
            next_id: 2,
            downloads: vec![PersistedDownload {
                id: DownloadId(1),
                request: DownloadRequest::new(
                    "https://example.com/file.bin".to_string(),
                    storage_dir("public_snapshot_hides_transfer_internals"),
                    ConflictPolicy::AutoRename,
                    IntegrityRule::None,
                ),
                destination: Some(PathBuf::from("file.bin")),
                loaded_from_store: true,
                temp_path: PathBuf::from("file.bin.1.part"),
                temp_layout: TempLayout::Single,
                supports_resume: true,
                status: DownloadStatus::Queued,
                progress: ProgressSnapshot::default(),
                error: None,
                etag: Some("etag".to_string()),
                last_modified: Some("lm".to_string()),
                created_at: test_time(0),
                updated_at: test_time(1),
            }],
        }),
        save_calls: AtomicUsize::new(0),
    });
    let queue =
        QueueService::with_transfer(QueueConfig::new(1, 1), Arc::new(ImmediateTransfer), store)
            .unwrap_or_else(|error| panic!("queue should initialize: {error}"));

    let record = queue
        .snapshot()
        .unwrap_or_else(|error| panic!("snapshot should succeed: {error}"))
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("record should exist"));

    assert_eq!(record.id, DownloadId(1));
    assert_eq!(record.destination, Some(PathBuf::from("file.bin")));
    assert!(record.error.is_none());
}
