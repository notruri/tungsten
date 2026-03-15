use std::path::Path;

use chrono::{TimeZone, Utc};
use tempfile::tempdir;
use tungsten_core::TempLayout;
use tungsten_core::store::{PersistedDownload, PersistedQueue, QueueStore};
use tungsten_core::{
    ConflictPolicy, DownloadId, DownloadRequest, DownloadStatus, IntegrityRule, MultipartPart,
    MultipartState, ProgressSnapshot,
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

#[test]
fn save_and_load_multipart_layout_with_cursor() {
    let temp = tempdir().unwrap_or_else(|error| panic!("failed to create temp dir: {error}"));
    let state_path = temp.path().join("appstate.db");
    let store = DiskStateStore::new(state_path);

    let mut record = build_record(7, temp.path());
    record.temp_layout = TempLayout::Multipart(MultipartState {
        total_size: 100,
        parts: vec![
            MultipartPart {
                index: 0,
                start: 0,
                end: 49,
                cursor: 20,
                path: temp.path().join("file-7.part.p0"),
            },
            MultipartPart {
                index: 1,
                start: 50,
                end: 99,
                cursor: 75,
                path: temp.path().join("file-7.part.p1"),
            },
        ],
    });

    let initial_state = PersistedQueue {
        next_id: 8,
        downloads: vec![record],
    };
    store
        .save_queue(&initial_state)
        .unwrap_or_else(|error| panic!("failed to save state: {error}"));

    let loaded = store
        .load_queue()
        .unwrap_or_else(|error| panic!("failed to load state: {error}"));
    assert_eq!(loaded.downloads.len(), 1);

    let layout = match &loaded.downloads[0].temp_layout {
        TempLayout::Multipart(layout) => layout,
        other => panic!("expected multipart layout, got {other:?}"),
    };
    assert_eq!(layout.total_size, 100);
    assert_eq!(layout.parts.len(), 2);
    assert_eq!(layout.parts[0].cursor, 20);
    assert_eq!(layout.parts[1].cursor, 75);
}
