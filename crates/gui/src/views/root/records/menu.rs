use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use gpui_component::menu::{PopupMenu, PopupMenuItem};
use tracing::{debug, error};
use tungsten_net::model::{DownloadId, DownloadStatus};
use tungsten_net::queue::QueueService;

use super::explorer::open_in_file_explorer;
use super::format::truncate_text;

pub(super) fn build_task_menu(
    menu: PopupMenu,
    queue: Arc<QueueService>,
    download_id: DownloadId,
    status: DownloadStatus,
    file_name: String,
    destination: Option<PathBuf>,
) -> PopupMenu {
    let should_resume = matches!(
        status,
        DownloadStatus::Paused | DownloadStatus::Failed | DownloadStatus::Cancelled
    );
    let pause_label = if should_resume { "resume" } else { "pause" };
    let can_pause_resume = matches!(
        status,
        DownloadStatus::Queued
            | DownloadStatus::Running
            | DownloadStatus::Paused
            | DownloadStatus::Failed
            | DownloadStatus::Cancelled
    );
    let can_cancel = matches!(
        status,
        DownloadStatus::Queued
            | DownloadStatus::Running
            | DownloadStatus::Paused
            | DownloadStatus::Failed
    );
    let can_delete = !matches!(status, DownloadStatus::Running | DownloadStatus::Verifying);
    let can_open_explorer = destination.is_some();

    let queue_for_pause_resume = Arc::clone(&queue);
    let queue_for_cancel = Arc::clone(&queue);
    let queue_for_delete = Arc::clone(&queue);
    let destination_for_open = destination.clone();

    menu.label(truncate_text(&file_name, 28))
        .separator()
        .item(
            PopupMenuItem::new(pause_label)
                .disabled(!can_pause_resume)
                .on_click(move |_, _, _| {
                    let result = if should_resume {
                        queue_for_pause_resume.resume(download_id)
                    } else {
                        queue_for_pause_resume.pause(download_id)
                    };

                    if let Err(error) = result {
                        error!(
                            download_id = %download_id,
                            error = %error,
                            "failed to run pause/resume action"
                        );
                    }
                }),
        )
        .item(
            PopupMenuItem::new("cancel")
                .disabled(!can_cancel)
                .on_click(move |_, _, _| {
                    if let Err(error) = queue_for_cancel.cancel(download_id) {
                        error!(download_id = %download_id, error = %error, "failed to cancel");
                    }
                }),
        )
        .item(
            PopupMenuItem::new("delete")
                .disabled(!can_delete)
                .on_click(move |_, _, _| {
                    if let Err(error) = queue_for_delete.delete(download_id) {
                        error!(download_id = %download_id, error = %error, "failed to delete");
                    }
                }),
        )
        .item(
            PopupMenuItem::new("open in file explorer")
                .disabled(!can_open_explorer)
                .on_click(move |_, _, _| {
                    let Some(destination_path) = destination_for_open.as_ref() else {
                        return;
                    };
                    let destination_path = destination_path.clone();

                    debug!(
                        download_id = %download_id,
                        destination = %destination_path.display(),
                        exists = destination_path.exists(),
                        "opening destination in file explorer"
                    );

                    open_in_file_explorer_async(download_id, destination_path);
                }),
        )
}

fn open_in_file_explorer_async(download_id: DownloadId, destination_path: PathBuf) {
    thread::spawn(move || {
        if let Err(error) = open_in_file_explorer(&destination_path) {
            error!(
                download_id = %download_id,
                destination = %destination_path.display(),
                error = %error,
                "failed to open destination in file explorer"
            );
        }
    });
}
