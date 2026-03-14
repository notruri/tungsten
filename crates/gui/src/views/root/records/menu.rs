use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use gpui::PromptLevel;
use gpui_component::menu::{PopupMenu, PopupMenuItem};
use tracing::{debug, error};
use tungsten_runtime::{DownloadId, DownloadStatus, QueueService};

use crate::components::dialog::speed;

use super::explorer::open_in_file_explorer;
use super::format::truncate_text;

#[derive(Clone)]
pub(super) struct GroupMenuTarget {
    pub id: DownloadId,
    pub status: DownloadStatus,
}

pub(super) fn build_task_menu(
    menu: PopupMenu,
    queue: Arc<QueueService>,
    download_id: DownloadId,
    status: DownloadStatus,
    file_name: String,
    destination: Option<PathBuf>,
    speed_limit_kbps: Option<u64>,
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
    let can_remove = !matches!(status, DownloadStatus::Running | DownloadStatus::Verifying);
    let can_delete_file = matches!(status, DownloadStatus::Completed) && destination.is_some();
    let can_open_explorer = matches!(status, DownloadStatus::Completed) && destination.is_some();

    let queue_for_pause_resume = Arc::clone(&queue);
    let queue_for_cancel = Arc::clone(&queue);
    let queue_for_remove = Arc::clone(&queue);
    let queue_for_delete = Arc::clone(&queue);
    let queue_for_speed_limit = Arc::clone(&queue);
    let destination_for_delete = destination.clone();
    let file_name_for_delete = file_name.clone();
    let destination_for_open = destination.clone();
    let file_name_for_speed_limit = file_name.clone();

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
                .disabled(!can_remove)
                .on_click(move |_, _, _| {
                    if let Err(error) = queue_for_remove.delete(download_id) {
                        error!(download_id = %download_id, error = %error, "failed to remove");
                    }
                }),
        )
        .item(
            PopupMenuItem::new("delete")
                .disabled(!can_delete_file)
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
        .item(
            PopupMenuItem::new("speed")
                .on_click(move |_, window, cx| {
                    speed::open_dialog(
                        Arc::clone(&queue_for_speed_limit),
                        download_id,
                        file_name_for_speed_limit.clone(),
                        speed_limit_kbps,
                        window,
                        cx,
                    );
                }),
        )
}

pub(super) fn build_group_task_menu(
    menu: PopupMenu,
    queue: Arc<QueueService>,
    targets: Vec<GroupMenuTarget>,
) -> PopupMenu {
    let selected_count = targets.len();
    let can_pause_resume_any = targets
        .iter()
        .any(|target| can_pause_or_resume(&target.status) || can_resume(&target.status));
    let can_cancel_any = targets.iter().any(|target| can_cancel(&target.status));
    let can_remove_any = targets.iter().any(|target| can_remove(&target.status));

    let queue_for_pause_resume = Arc::clone(&queue);
    let queue_for_cancel = Arc::clone(&queue);
    let queue_for_remove = Arc::clone(&queue);
    let pause_resume_targets = targets.clone();
    let cancel_targets = targets.clone();
    let remove_targets = targets;

    menu.label(format!("{selected_count} selected"))
        .separator()
        .item(
            PopupMenuItem::new("pause/resume")
                .disabled(!can_pause_resume_any)
                .on_click(move |_, _, _| {
                    for target in pause_resume_targets.iter() {
                        let result = if can_resume(&target.status) {
                            queue_for_pause_resume.resume(target.id)
                        } else if can_pause_or_resume(&target.status) {
                            queue_for_pause_resume.pause(target.id)
                        } else {
                            continue;
                        };

                        if let Err(error) = result {
                            error!(
                                download_id = %target.id,
                                status = ?target.status,
                                error = %error,
                                "failed to run batch pause/resume action"
                            );
                        }
                    }
                }),
        )
        .item(
            PopupMenuItem::new("cancel")
                .disabled(!can_cancel_any)
                .on_click(move |_, _, _| {
                    for target in cancel_targets.iter() {
                        if !can_cancel(&target.status) {
                            continue;
                        }

                        if let Err(error) = queue_for_cancel.cancel(target.id) {
                            error!(
                                download_id = %target.id,
                                status = ?target.status,
                                error = %error,
                                "failed to run batch cancel action"
                            );
                        }
                    }
                }),
        )
        .item(
            PopupMenuItem::new("remove")
                .disabled(!can_remove_any)
                .on_click(move |_, _, _| {
                    for target in remove_targets.iter() {
                        if !can_remove(&target.status) {
                            continue;
                        }

                        if let Err(error) = queue_for_remove.delete(target.id) {
                            error!(
                                download_id = %target.id,
                                status = ?target.status,
                                error = %error,
                                "failed to run batch remove action"
                            );
                        }
                    }
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

fn can_resume(status: &DownloadStatus) -> bool {
    matches!(
        status,
        DownloadStatus::Paused | DownloadStatus::Failed | DownloadStatus::Cancelled
    )
}

fn can_pause_or_resume(status: &DownloadStatus) -> bool {
    matches!(
        status,
        DownloadStatus::Queued
            | DownloadStatus::Running
            | DownloadStatus::Paused
            | DownloadStatus::Failed
            | DownloadStatus::Cancelled
    )
}

fn can_cancel(status: &DownloadStatus) -> bool {
    matches!(
        status,
        DownloadStatus::Queued
            | DownloadStatus::Running
            | DownloadStatus::Paused
            | DownloadStatus::Failed
    )
}

fn can_remove(status: &DownloadStatus) -> bool {
    !matches!(status, DownloadStatus::Running | DownloadStatus::Verifying)
}

fn delete_from_queue_and_disk(
    queue: Arc<QueueService>,
    download_id: DownloadId,
    destination: Option<PathBuf>,
) {
    let status = match queue.snapshot() {
        Ok(records) => records
            .into_iter()
            .find(|record| record.id == download_id)
            .map(|record| record.status),
        Err(error) => {
            error!(
                download_id = %download_id,
                error = %error,
                "failed to read queue status before delete"
            );
            return;
        }
    };
    let should_delete_destination = matches!(status, Some(DownloadStatus::Completed));

    if let Err(error) = queue.delete(download_id) {
        error!(
            download_id = %download_id,
            error = %error,
            "failed to remove queue record before deleting from disk"
        );
        return;
    }

    if !should_delete_destination {
        debug!(
            download_id = %download_id,
            status = ?status,
            "skipping delete from disk because download is not completed"
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
