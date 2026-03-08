use std::path::PathBuf;
use std::sync::Arc;

use gpui::*;
use gpui_component::{button::*, input::*, *};
use tungsten_net::{ConflictPolicy, DownloadRequest, DownloadStatus, IntegrityRule, QueueService};

const DEFAULT_DOWNLOAD_DIR: &str = "storage/downloads";

pub struct View {
    queue: Arc<QueueService>,
    input_state: Entity<InputState>,
}

impl View {
    pub fn new(window: &mut Window, cx: &mut Context<Self>, queue: Arc<QueueService>) -> Self {
        let input_state = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("url here")
                .default_value("")
        });
        Self { queue, input_state }
    }

    fn create_interface(&self, _: &mut Window, _: &mut Context<Self>) -> Div {
        let queue = Arc::clone(&self.queue);
        let input_state_for_add = self.input_state.clone();

        div()
            .v_flex()
            .gap_2()
            .size_full()
            .p_4()
            .child("Tungsten")
            .child(
                div()
                    .h_flex()
                    .gap_2()
                    .child(Input::new(&self.input_state))
                    .child(
                        Button::new("add-queue")
                            .primary()
                            .label("add to queue")
                            .on_click(move |_, window, cx| {
                                let url = input_state_for_add.read(cx).value().to_string();
                                if url.trim().is_empty() {
                                    eprintln!("url is required");
                                    return;
                                }

                                let dest = std::env::current_dir()
                                    .unwrap_or_else(|_| PathBuf::from("."))
                                    .join(DEFAULT_DOWNLOAD_DIR);

                                println!("adding to queue: {url} ({dest:?})");

                                let request = DownloadRequest::new(
                                    url,
                                    dest,
                                    ConflictPolicy::AutoRename,
                                    IntegrityRule::None,
                                );
                                match queue.enqueue(request) {
                                    Ok(_) => {
                                        input_state_for_add.update(cx, |input, input_cx| {
                                            input.set_value("", window, input_cx);
                                        });
                                    }
                                    Err(error) => {
                                        eprintln!("failed to enqueue request: {error}");
                                    }
                                }
                            }),
                    ),
            )
            .child(self.records())
    }

    fn records(&self) -> Div {
        let records = self.queue.snapshot().unwrap_or_else(|_| Vec::new());

        div()
            .v_flex()
            .gap_2()
            .children(records.into_iter().map(|record| {
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

                let queue_for_pause_resume = Arc::clone(&self.queue);
                let queue_for_cancel = Arc::clone(&self.queue);
                let queue_for_delete = Arc::clone(&self.queue);

                div().h_flex().gap_2().children([
                    div().child(download_id.to_string()),
                    div().child(
                        record
                            .request
                            .destination
                            .file_name()
                            .map(|n| n.to_string_lossy().into_owned())
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
                                    eprintln!(
                                        "failed to run pause/resume action for {}: {error}",
                                        download_id
                                    );
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
            }))
    }
}

impl Render for View {
    fn render(&mut self, w: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.create_interface(w, cx)
    }
}
