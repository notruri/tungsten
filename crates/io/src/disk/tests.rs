use std::path::Path;

use chrono::{TimeZone, Utc};
use tempfile::tempdir;
use tungsten_core::TempLayout;
use tungsten_core::store::{PersistedDownload, PersistedQueue, QueueStore};
use tungsten_core::{
    ConflictPolicy, DownloadId, DownloadRequest, DownloadStatus, IntegrityRule, ProgressSnapshot,
};

use super::DiskStateStore;

fn test_time(seconds: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0)
        .single()
        .unwrap_or_else(|| panic!("valid timestamp should be created for {seconds}"))
}

fn build_record(id: u64, path: &Path) -> PersistedDownload {
    PersistedDownload {
        id: DownloadId(id),
        request: DownloadRequest::new(
            format!("https://example.com/{id}.bin"),
            path.join(format!("file-{id}.bin")),
            ConflictPolicy::AutoRename,
            IntegrityRule::None,
        )
        .speed_limit_kbps(Some(id * 10)),
        destination: Some(path.join(format!("file-{id}.bin"))),
        loaded_from_store: false,
        temp_path: path.join(format!("file-{id}.part")),
        temp_layout: TempLayout::Single,
        supports_resume: true,
        status: DownloadStatus::Queued,
        progress: ProgressSnapshot::default(),
        error: None,
        etag: None,
        last_modified: None,
        created_at: test_time(id as i64),
        updated_at: test_time(id as i64 + 1),
    }
}

#[test]
fn save_and_load_queue_round_trip() {
    let temp = tempdir().unwrap_or_else(|error| panic!("failed to create temp dir: {error}"));
    let state_path = temp.path().join("appstate.db");
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
    assert_eq!(loaded.downloads[0].request.url, "https://example.com/1.bin");
    assert_eq!(loaded.downloads[0].request.speed_limit_kbps, Some(10));
    assert_eq!(loaded.downloads[0].created_at, test_time(1));
    assert_eq!(loaded.downloads[1].updated_at, test_time(3));
}
