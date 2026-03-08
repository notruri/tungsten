use std::sync::Arc;

use gpui::*;
use gpui_component::{button::*, *};
use tungsten_net::{DownloadRecord, DownloadStatus, QueueService};

pub fn section(queue: Arc<QueueService>) -> Div {
    let records = queue.snapshot().unwrap_or_else(|_| Vec::new());

    div()
        .v_flex()
        .gap_2()
        .children(records.into_iter().map(|record| row(Arc::clone(&queue), record)))
}

fn row(queue: Arc<QueueService>, record: DownloadRecord) -> Div {
    let download_id = record.id;
    let status = record.status.clone();
    let should_resume = matches!(
        status,
        DownloadStatus::Paused | DownloadStatus::Failed | DownloadStatus::Cancelled
    );
    let pause_label = if should_resume { "resume" } else { "pause" };
    let pause_button_id = ("pause-resume", download_id.0);
    let cancel_button_id = ("cancel", download_id.0);
    let delete_button_id = ("delete", download_id.0);

    let queue_for_pause_resume = Arc::clone(&queue);
    let queue_for_cancel = Arc::clone(&queue);
    let queue_for_delete = Arc::clone(&queue);

    div().h_flex().gap_2().children([
        div().child(download_id.to_string()),
        div().child(
            record
                .request
                .destination
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_else(String::new),
        ),
        div().child(format!("{status:?}")),
        div().child(format!(
            "{} / {}",
            record.progress.downloaded,
            record.progress.total.unwrap_or_default()
        )),
        div().child(
            Button::new(pause_button_id)
                .primary()
                .label(pause_label)
                .on_click(move |_, _, _| {
                    let result = if should_resume {
                        queue_for_pause_resume.resume(download_id)
                    } else {
                        queue_for_pause_resume.pause(download_id)
                    };

                    if let Err(error) = result {
                        eprintln!("failed to run pause/resume action for {}: {error}", download_id);
                    }
                }),
        ),
        div().child(Button::new(cancel_button_id).primary().label("cancel").on_click(
            move |_, _, _| {
                if let Err(error) = queue_for_cancel.cancel(download_id) {
                    eprintln!("failed to cancel {}: {error}", download_id);
                }
            },
        )),
        div().child(Button::new(delete_button_id).label("delete").on_click(
            move |_, _, _| {
                if let Err(error) = queue_for_delete.delete(download_id) {
                    eprintln!("failed to delete {}: {error}", download_id);
                }
            },
        )),
    ])
}
