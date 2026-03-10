use std::path::PathBuf;
use std::sync::Arc;

use gpui::*;
use gpui_component::{button::*, input::*, *};
use tungsten_net::model::{ConflictPolicy, DownloadRequest, IntegrityRule};
use tungsten_net::queue::QueueService;

const DEFAULT_DOWNLOAD_DIR: &str = "storage/downloads";

pub fn queue_section(queue: Arc<QueueService>, input_state: Entity<InputState>) -> Div {
    let queue_for_add = Arc::clone(&queue);
    let input_state_for_add = input_state.clone();

    div()
        .h_flex()
        .gap_2()
        .child(Input::new(&input_state))
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

                    let destination = std::env::current_dir()
                        .unwrap_or_else(|_| PathBuf::from("."))
                        .join(DEFAULT_DOWNLOAD_DIR);

                    let request = DownloadRequest::new(
                        url,
                        destination,
                        ConflictPolicy::AutoRename,
                        IntegrityRule::None,
                    );

                    match queue_for_add.enqueue(request) {
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
        )
}
