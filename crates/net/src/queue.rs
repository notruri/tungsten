use std::collections::HashMap;
use std::ffi::OsString;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, mpsc};
use std::thread;
use std::time::Duration;

use reqwest::Url;
use sha2::{Digest, Sha256};

use crate::backend::{ControlSignal, DownloadBackend, DownloadOutcome, DownloadTask};
use crate::error::NetError;
use crate::state::{PersistedState, StateStore};
use crate::types::{
    ConflictPolicy, DownloadId, DownloadRecord, DownloadRequest, DownloadStatus, IntegrityRule,
    ProgressSnapshot, QueueEvent,
};

const CONTROL_RUN: u8 = 0;
const CONTROL_PAUSE: u8 = 1;
const CONTROL_CANCEL: u8 = 2;

#[derive(Clone)]
pub struct QueueService {
    shared: Arc<Shared>,
}

struct Shared {
    state: Mutex<QueueState>,
    backend: Arc<dyn DownloadBackend>,
    store: Arc<dyn StateStore>,
}

struct QueueState {
    next_id: u64,
    max_parallel: usize,
    downloads: HashMap<DownloadId, DownloadRecord>,
    controls: HashMap<DownloadId, Arc<AtomicU8>>,
    subscribers: Vec<mpsc::Sender<QueueEvent>>,
}

impl QueueService {
    pub fn new(
        backend: Arc<dyn DownloadBackend>,
        store: Arc<dyn StateStore>,
        max_parallel: usize,
    ) -> Result<Self, NetError> {
        let persisted = store.load_state()?;
        let normalized_max = max_parallel.max(1);
        let (downloads, controls, next_id) = build_state_from_persisted(persisted);

        let shared = Arc::new(Shared {
            state: Mutex::new(QueueState {
                next_id,
                max_parallel: normalized_max,
                downloads,
                controls,
                subscribers: Vec::new(),
            }),
            backend,
            store,
        });

        let service = Self {
            shared: Arc::clone(&shared),
        };

        save_full_state(&service.shared)?;
        spawn_scheduler(shared);

        Ok(service)
    }

    pub fn enqueue(&self, mut request: DownloadRequest) -> Result<DownloadId, NetError> {
        request.validate()?;

        let probe = self.shared.backend.probe(&request).unwrap_or_default();
        apply_inferred_destination_file_name(&mut request, probe.file_name.as_deref());

        let mut state = lock_state(&self.shared)?;
        let destination =
            resolve_destination(&request.destination, &state.downloads, &request.conflict);
        request.destination = destination.clone();

        let download_id = DownloadId(state.next_id.max(1));
        state.next_id = download_id.0 + 1;

        let now = DownloadRecord::now_epoch();
        let record = DownloadRecord {
            id: download_id,
            request,
            temp_path: temp_path_for(&destination, download_id),
            supports_resume: probe.accept_ranges,
            status: DownloadStatus::Queued,
            progress: ProgressSnapshot {
                downloaded: 0,
                total: probe.total_size,
                speed_bps: None,
                eta_seconds: None,
            },
            error: None,
            etag: probe.etag,
            last_modified: probe.last_modified,
            created_at: now,
            updated_at: now,
        };

        state
            .controls
            .insert(download_id, Arc::new(AtomicU8::new(CONTROL_RUN)));
        state.downloads.insert(download_id, record.clone());
        publish_event(&mut state, QueueEvent::Added(record));
        drop(state);

        save_full_state(&self.shared)?;
        Ok(download_id)
    }

    pub fn pause(&self, download_id: DownloadId) -> Result<(), NetError> {
        let mut should_persist = false;
        {
            let mut state = lock_state(&self.shared)?;
            let status = state
                .downloads
                .get(&download_id)
                .map(|record| record.status.clone())
                .ok_or(NetError::DownloadNotFound(download_id))?;

            match status {
                DownloadStatus::Queued => {
                    let mut updated = None;
                    if let Some(record) = state.downloads.get_mut(&download_id) {
                        record.status = DownloadStatus::Paused;
                        record.touch();
                        updated = Some(record.clone());
                        should_persist = true;
                    }
                    if let Some(updated_record) = updated {
                        publish_event(&mut state, QueueEvent::Updated(updated_record));
                    }
                }
                DownloadStatus::Running => {
                    if let Some(control) = state.controls.get(&download_id) {
                        control.store(CONTROL_PAUSE, Ordering::SeqCst);
                    }
                }
                _ => {}
            }
        }

        if should_persist {
            save_full_state(&self.shared)?;
        }

        Ok(())
    }

    pub fn resume(&self, download_id: DownloadId) -> Result<(), NetError> {
        {
            let mut state = lock_state(&self.shared)?;
            let mut updated = None;
            let record = state
                .downloads
                .get_mut(&download_id)
                .ok_or(NetError::DownloadNotFound(download_id))?;

            if matches!(
                record.status,
                DownloadStatus::Paused | DownloadStatus::Failed | DownloadStatus::Cancelled
            ) {
                record.status = DownloadStatus::Queued;
                record.error = None;
                record.touch();
                updated = Some(record.clone());
            }

            if let Some(control) = state.controls.get(&download_id) {
                control.store(CONTROL_RUN, Ordering::SeqCst);
            }

            if let Some(updated_record) = updated {
                publish_event(&mut state, QueueEvent::Updated(updated_record));
            }
        }

        save_full_state(&self.shared)?;
        Ok(())
    }

    pub fn cancel(&self, download_id: DownloadId) -> Result<(), NetError> {
        let mut temp_to_remove = None;
        let mut should_persist = false;
        {
            let mut state = lock_state(&self.shared)?;
            let mut updated = None;
            let record = state
                .downloads
                .get_mut(&download_id)
                .ok_or(NetError::DownloadNotFound(download_id))?;

            match record.status {
                DownloadStatus::Running => {
                    if let Some(control) = state.controls.get(&download_id) {
                        control.store(CONTROL_CANCEL, Ordering::SeqCst);
                    }
                }
                DownloadStatus::Queued | DownloadStatus::Paused | DownloadStatus::Failed => {
                    record.status = DownloadStatus::Cancelled;
                    record.touch();
                    record.error = None;
                    temp_to_remove = Some(record.temp_path.clone());
                    updated = Some(record.clone());
                    should_persist = true;
                }
                _ => {}
            }

            if let Some(updated_record) = updated {
                publish_event(&mut state, QueueEvent::Updated(updated_record));
            }
        }

        if let Some(path) = temp_to_remove {
            remove_file_if_exists(&path)?;
        }
        if should_persist {
            save_full_state(&self.shared)?;
        }

        Ok(())
    }

    pub fn delete(&self, download_id: DownloadId) -> Result<(), NetError> {
        let temp_to_remove = {
            let mut state = lock_state(&self.shared)?;
            let record = state
                .downloads
                .get(&download_id)
                .ok_or(NetError::DownloadNotFound(download_id))?;

            if matches!(
                record.status,
                DownloadStatus::Running | DownloadStatus::Verifying
            ) {
                return Err(NetError::InvalidRequest(
                    "cannot delete running download; cancel first".to_string(),
                ));
            }

            let temp_path = record.temp_path.clone();
            state.downloads.remove(&download_id);
            state.controls.remove(&download_id);
            publish_event(&mut state, QueueEvent::Removed(download_id));
            temp_path
        };

        remove_file_if_exists(&temp_to_remove)?;
        save_full_state(&self.shared)?;
        Ok(())
    }

    pub fn retry(&self, download_id: DownloadId) -> Result<(), NetError> {
        {
            let mut state = lock_state(&self.shared)?;
            let mut updated = None;
            let record = state
                .downloads
                .get_mut(&download_id)
                .ok_or(NetError::DownloadNotFound(download_id))?;

            if matches!(
                record.status,
                DownloadStatus::Failed | DownloadStatus::Cancelled
            ) {
                record.status = DownloadStatus::Queued;
                record.error = None;
                record.touch();
                updated = Some(record.clone());
            }

            if let Some(control) = state.controls.get(&download_id) {
                control.store(CONTROL_RUN, Ordering::SeqCst);
            }

            if let Some(updated_record) = updated {
                publish_event(&mut state, QueueEvent::Updated(updated_record));
            }
        }

        save_full_state(&self.shared)?;
        Ok(())
    }

    pub fn set_max_parallel(&self, max_parallel: usize) -> Result<(), NetError> {
        {
            let mut state = lock_state(&self.shared)?;
            state.max_parallel = max_parallel.max(1);
        }

        save_full_state(&self.shared)?;
        Ok(())
    }

    pub fn snapshot(&self) -> Result<Vec<DownloadRecord>, NetError> {
        let state = lock_state(&self.shared)?;
        let mut records = state.downloads.values().cloned().collect::<Vec<_>>();
        records.sort_by_key(|record| record.id.0);
        Ok(records)
    }

    pub fn first_download_id(&self) -> Result<Option<DownloadId>, NetError> {
        let records = self.snapshot()?;
        Ok(records.first().map(|record| record.id))
    }

    pub fn subscribe(&self) -> Result<mpsc::Receiver<QueueEvent>, NetError> {
        let (tx, rx) = mpsc::channel();
        let mut state = lock_state(&self.shared)?;
        state.subscribers.push(tx);
        Ok(rx)
    }
}

fn build_state_from_persisted(
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

fn next_id_from_downloads(downloads: &HashMap<DownloadId, DownloadRecord>) -> u64 {
    downloads
        .keys()
        .map(|id| id.0)
        .max()
        .unwrap_or(0)
        .saturating_add(1)
}

fn spawn_scheduler(shared: Arc<Shared>) {
    thread::spawn(move || {
        loop {
            let launch_ids = match pick_next_downloads(&shared) {
                Ok(ids) => ids,
                Err(error) => {
                    eprintln!("scheduler lock failed: {error}");
                    thread::sleep(Duration::from_millis(300));
                    continue;
                }
            };

            if !launch_ids.is_empty() {
                if let Err(error) = save_full_state(&shared) {
                    eprintln!("failed to save state before launch: {error}");
                }
            }

            for download_id in launch_ids {
                let shared_for_worker = Arc::clone(&shared);
                thread::spawn(move || {
                    if let Err(error) = run_download_worker(shared_for_worker, download_id) {
                        eprintln!("worker failed for {download_id}: {error}");
                    }
                });
            }

            thread::sleep(Duration::from_millis(250));
        }
    });
}

fn pick_next_downloads(shared: &Shared) -> Result<Vec<DownloadId>, NetError> {
    let mut state = lock_state(shared)?;
    let running_count = state
        .downloads
        .values()
        .filter(|record| {
            matches!(
                record.status,
                DownloadStatus::Running | DownloadStatus::Verifying
            )
        })
        .count();

    let available_slots = state.max_parallel.saturating_sub(running_count);
    if available_slots == 0 {
        return Ok(Vec::new());
    }

    let mut queued_ids = state
        .downloads
        .values()
        .filter(|record| record.status == DownloadStatus::Queued)
        .map(|record| record.id)
        .collect::<Vec<_>>();
    queued_ids.sort_by_key(|id| id.0);

    let mut picked_ids = Vec::new();
    for download_id in queued_ids.into_iter().take(available_slots) {
        let mut updated = None;
        if let Some(record) = state.downloads.get_mut(&download_id) {
            record.status = DownloadStatus::Running;
            record.error = None;
            record.touch();
            updated = Some(record.clone());
            picked_ids.push(download_id);
        }

        if let Some(updated_record) = updated {
            publish_event(&mut state, QueueEvent::Updated(updated_record));
        }

        if let Some(control) = state.controls.get(&download_id) {
            control.store(CONTROL_RUN, Ordering::SeqCst);
        }
    }

    Ok(picked_ids)
}

fn run_download_worker(shared: Arc<Shared>, download_id: DownloadId) -> Result<(), NetError> {
    let (record, control) = {
        let state = lock_state(&shared)?;
        let record = state
            .downloads
            .get(&download_id)
            .cloned()
            .ok_or(NetError::DownloadNotFound(download_id))?;
        let control = state
            .controls
            .get(&download_id)
            .cloned()
            .ok_or(NetError::DownloadNotFound(download_id))?;

        (record, control)
    };

    let existing_size = match fs::metadata(&record.temp_path) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => {
            set_failed(
                &shared,
                download_id,
                record.progress.clone(),
                format!("failed to read temp file metadata: {error}"),
            )?;
            return Ok(());
        }
    };

    let task = DownloadTask {
        request: record.request.clone(),
        temp_path: record.temp_path.clone(),
        existing_size,
        allow_resume: record.supports_resume,
        etag: record.etag.clone(),
    };

    let progress_shared = Arc::clone(&shared);
    let mut on_progress = move |progress: ProgressSnapshot| -> Result<(), NetError> {
        set_running_progress(&progress_shared, download_id, progress.clone())?;
        if let Err(error) = progress_shared
            .store
            .checkpoint_progress(download_id, &progress)
        {
            eprintln!("failed to checkpoint download {download_id}: {error}");
        }

        Ok(())
    };

    let control_for_backend = Arc::clone(&control);
    let outcome = shared
        .backend
        .download(
            &task,
            &mut on_progress,
            &|| match control_for_backend.load(Ordering::SeqCst) {
                CONTROL_PAUSE => ControlSignal::Pause,
                CONTROL_CANCEL => ControlSignal::Cancel,
                _ => ControlSignal::Run,
            },
        );

    match outcome {
        Ok(DownloadOutcome::Completed(progress)) => {
            finish_completed(&shared, download_id, progress, &record)
        }
        Ok(DownloadOutcome::Paused(progress)) => set_paused(&shared, download_id, progress),
        Ok(DownloadOutcome::Cancelled(progress)) => {
            set_cancelled(&shared, download_id, progress, &record.temp_path)
        }
        Err(error) => set_failed(&shared, download_id, record.progress, error.to_string()),
    }
}

fn finish_completed(
    shared: &Shared,
    download_id: DownloadId,
    progress: ProgressSnapshot,
    record: &DownloadRecord,
) -> Result<(), NetError> {
    if let Some(parent) = record.request.destination.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::rename(&record.temp_path, &record.request.destination)?;

    set_status(
        shared,
        download_id,
        DownloadStatus::Verifying,
        progress.clone(),
        None,
    )?;

    match &record.request.integrity {
        IntegrityRule::None => set_status(
            shared,
            download_id,
            DownloadStatus::Completed,
            progress,
            None,
        ),
        IntegrityRule::Sha256(expected) => {
            let actual = sha256_file(&record.request.destination)?;
            if actual.eq_ignore_ascii_case(expected) {
                set_status(
                    shared,
                    download_id,
                    DownloadStatus::Completed,
                    progress,
                    None,
                )
            } else {
                set_status(
                    shared,
                    download_id,
                    DownloadStatus::Failed,
                    progress,
                    Some(format!(
                        "sha256 mismatch: expected {expected}, got {actual}"
                    )),
                )
            }
        }
    }
}

fn set_running_progress(
    shared: &Shared,
    download_id: DownloadId,
    progress: ProgressSnapshot,
) -> Result<(), NetError> {
    let mut state = lock_state(shared)?;
    let record = state
        .downloads
        .get_mut(&download_id)
        .ok_or(NetError::DownloadNotFound(download_id))?;

    record.progress = progress;
    record.status = DownloadStatus::Running;
    record.touch();
    let updated_record = record.clone();

    publish_event(&mut state, QueueEvent::Updated(updated_record));
    Ok(())
}

fn set_paused(
    shared: &Shared,
    download_id: DownloadId,
    progress: ProgressSnapshot,
) -> Result<(), NetError> {
    {
        let state = lock_state(shared)?;
        if let Some(control) = state.controls.get(&download_id) {
            control.store(CONTROL_RUN, Ordering::SeqCst);
        }
    }

    set_status(shared, download_id, DownloadStatus::Paused, progress, None)
}

fn set_cancelled(
    shared: &Shared,
    download_id: DownloadId,
    progress: ProgressSnapshot,
    temp_path: &Path,
) -> Result<(), NetError> {
    remove_file_if_exists(temp_path)?;
    set_status(
        shared,
        download_id,
        DownloadStatus::Cancelled,
        progress,
        None,
    )
}

fn set_failed(
    shared: &Shared,
    download_id: DownloadId,
    progress: ProgressSnapshot,
    error_message: String,
) -> Result<(), NetError> {
    set_status(
        shared,
        download_id,
        DownloadStatus::Failed,
        progress,
        Some(error_message),
    )
}

fn set_status(
    shared: &Shared,
    download_id: DownloadId,
    status: DownloadStatus,
    progress: ProgressSnapshot,
    error: Option<String>,
) -> Result<(), NetError> {
    {
        let mut state = lock_state(shared)?;
        let control = state.controls.get(&download_id).cloned();
        let record = state
            .downloads
            .get_mut(&download_id)
            .ok_or(NetError::DownloadNotFound(download_id))?;

        record.status = status;
        record.progress = progress;
        record.error = error;
        record.touch();
        let updated_record = record.clone();

        if let Some(control) = control {
            control.store(CONTROL_RUN, Ordering::SeqCst);
        }

        publish_event(&mut state, QueueEvent::Updated(updated_record));
    }

    save_full_state(shared)
}

fn save_full_state(shared: &Shared) -> Result<(), NetError> {
    let snapshot = {
        let state = lock_state(shared)?;
        build_persisted_state(&state)
    };

    shared.store.save_state(&snapshot)
}

fn build_persisted_state(state: &QueueState) -> PersistedState {
    let mut downloads = state.downloads.values().cloned().collect::<Vec<_>>();
    downloads.sort_by_key(|record| record.id.0);

    PersistedState {
        next_id: state.next_id,
        downloads,
    }
}

fn publish_event(state: &mut QueueState, event: QueueEvent) {
    state
        .subscribers
        .retain(|subscriber| subscriber.send(event.clone()).is_ok());
}

fn lock_state(shared: &Shared) -> Result<MutexGuard<'_, QueueState>, NetError> {
    shared
        .state
        .lock()
        .map_err(|error| NetError::State(format!("queue state poisoned: {error}")))
}

fn resolve_destination(
    requested: &Path,
    downloads: &HashMap<DownloadId, DownloadRecord>,
    conflict: &ConflictPolicy,
) -> PathBuf {
    if !matches!(conflict, ConflictPolicy::AutoRename) {
        return requested.to_path_buf();
    }

    if !path_conflicts(requested, downloads) {
        return requested.to_path_buf();
    }

    let parent = requested
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let stem = requested
        .file_stem()
        .map(OsString::from)
        .unwrap_or_else(|| OsString::from("download"));
    let extension = requested.extension().map(OsString::from);

    let mut index = 1u64;
    loop {
        let mut candidate_name = OsString::from(&stem);
        candidate_name.push(format!(" ({index})"));

        let candidate = if let Some(ext) = &extension {
            let mut with_ext = candidate_name.clone();
            with_ext.push(".");
            with_ext.push(ext);
            parent.join(with_ext)
        } else {
            parent.join(candidate_name)
        };

        if !path_conflicts(&candidate, downloads) {
            return candidate;
        }

        index = index.saturating_add(1);
    }
}

fn apply_inferred_destination_file_name(request: &mut DownloadRequest, remote_file_name: Option<&str>) {
    if !should_infer_destination_file_name(&request.destination, &request.url) {
        return;
    }

    let file_name = remote_file_name
        .and_then(sanitize_file_name)
        .or_else(|| infer_file_name_from_url(&request.url));

    if let Some(file_name) = file_name {
        request.destination = request.destination.with_file_name(file_name);
    }
}

fn should_infer_destination_file_name(destination: &Path, url: &str) -> bool {
    let file_name = destination
        .file_name()
        .and_then(|value| value.to_str())
        .map(str::trim)
        .unwrap_or_default();

    if file_name.eq_ignore_ascii_case("download.bin") {
        return true;
    }

    infer_file_name_from_url(url)
        .map(|inferred| inferred == file_name)
        .unwrap_or(false)
}

fn infer_file_name_from_url(url: &str) -> Option<String> {
    let parsed = Url::parse(url).ok()?;
    let segment = parsed.path_segments()?.filter(|value| !value.is_empty()).last()?;
    sanitize_file_name(segment)
}

fn sanitize_file_name(value: &str) -> Option<String> {
    let trimmed = value.trim().trim_matches('.');
    if trimmed.is_empty() {
        return None;
    }

    let mut output = String::with_capacity(trimmed.len());
    for ch in trimmed.chars() {
        match ch {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => output.push('_'),
            '\u{0000}'..='\u{001F}' => output.push('_'),
            _ => output.push(ch),
        }
    }

    if output.trim().is_empty() {
        None
    } else {
        Some(output)
    }
}

fn path_conflicts(path: &Path, downloads: &HashMap<DownloadId, DownloadRecord>) -> bool {
    if path.exists() {
        return true;
    }

    downloads
        .values()
        .any(|record| record.request.destination == path)
}

fn temp_path_for(destination: &Path, download_id: DownloadId) -> PathBuf {
    let file_name = destination
        .file_name()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_else(|| "download".to_string());

    let temp_name = format!("{file_name}.{download_id}.part");

    match destination.parent() {
        Some(parent) => parent.join(temp_name),
        None => PathBuf::from(temp_name),
    }
}

fn remove_file_if_exists(path: &Path) -> Result<(), NetError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(NetError::Io(error)),
    }
}

fn sha256_file(path: &Path) -> Result<String, NetError> {
    let file = fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];

    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    let digest = hasher.finalize();
    Ok(hex::encode(digest))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::backend::{
        ControlSignal, DownloadBackend, DownloadOutcome, DownloadTask, ProbeInfo,
    };
    use crate::state::StateStore;

    use super::*;

    #[derive(Default)]
    struct MemoryStore {
        inner: Mutex<PersistedState>,
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
            Ok(())
        }

        fn checkpoint_progress(
            &self,
            _download_id: DownloadId,
            _progress: &ProgressSnapshot,
        ) -> Result<(), NetError> {
            Ok(())
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
                ControlSignal::Cancel => {
                    Ok(DownloadOutcome::Cancelled(ProgressSnapshot::default()))
                }
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

        let resolved =
            resolve_destination(&requested, &HashMap::new(), &ConflictPolicy::AutoRename);
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
        assert!(persisted.downloads.into_iter().all(|record| record.id != id));
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
            destination: PathBuf::from("storage/downloads/download.bin"),
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
            destination: PathBuf::from("storage/downloads/download.bin"),
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
}
