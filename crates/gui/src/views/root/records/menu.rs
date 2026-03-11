use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use gpui::PromptLevel;
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
    let queue_for_remove = Arc::clone(&queue);
    let queue_for_delete = Arc::clone(&queue);
    let destination_for_delete = destination.clone();
    let file_name_for_delete = file_name.clone();
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
            PopupMenuItem::new("remove")
                .disabled(!can_delete)
                .on_click(move |_, _, _| {
                    if let Err(error) = queue_for_remove.delete(download_id) {
                        error!(download_id = %download_id, error = %error, "failed to remove");
                    }
                }),
        )
        .item(
            PopupMenuItem::new("delete")
                .disabled(!can_delete)
                .on_click(move |_, window, cx| {
                    let detail = format!(
                        "This will remove '{file_name_for_delete}' from the queue and permanently delete it from disk."
                    );
                    let answer = window.prompt(
                        PromptLevel::Warning,
                        "Delete file from disk?",
                        Some(detail.as_str()),
                        &["Delete", "Cancel"],
                        cx,
                    );
                    let queue_for_delete = Arc::clone(&queue_for_delete);
                    let destination_for_delete = destination_for_delete.clone();

                    cx.spawn(async move |_| {
                        match answer.await {
                            Ok(0) => {
                                delete_from_queue_and_disk(
                                    queue_for_delete,
                                    download_id,
                                    destination_for_delete,
                                );
                            }
                            Ok(_) => {}
                            Err(error) => {
                                error!(
                                    download_id = %download_id,
                                    error = %error,
                                    "failed to resolve delete prompt response"
                                );
                            }
                        }
                    })
                    .detach();
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

fn delete_from_queue_and_disk(
    queue: Arc<QueueService>,
    download_id: DownloadId,
    destination: Option<PathBuf>,
) {
    if let Err(error) = queue.delete(download_id) {
        error!(
            download_id = %download_id,
            error = %error,
            "failed to remove queue record before deleting from disk"
        );
        return;
    }

    let Some(destination) = destination else {
        error!(
            download_id = %download_id,
            "delete requested but destination path is unresolved"
        );
        return;
    };

    match fs::remove_file(&destination) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            error!(
                download_id = %download_id,
                destination = %destination.display(),
                error = %error,
                "file not found during delete from disk"
            );
        }
        Err(error) => {
            error!(
                download_id = %download_id,
                destination = %destination.display(),
                error = %error,
                "failed to delete file from disk"
            );
        }
    }
}
