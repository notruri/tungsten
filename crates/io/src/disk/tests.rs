use std::path::Path;

use tempfile::tempdir;
use tungsten_net::model::{
    ConflictPolicy, DownloadId, DownloadRequest, DownloadStatus, IntegrityRule, ProgressSnapshot,
};
use tungsten_net::store::{PersistedDownload, PersistedQueue, QueueStore};
use tungsten_net::transfer::TempLayout;

use super::DiskStateStore;

fn build_record(id: u64, path: &Path) -> PersistedDownload {
    PersistedDownload {
        id: DownloadId(id),
        request: DownloadRequest::new(
            format!("https://example.com/{id}.bin"),
            path.join(format!("file-{id}.bin")),
            ConflictPolicy::AutoRename,
            IntegrityRule::None,
        ),
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
        created_at: 0,
        updated_at: 0,
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
}
