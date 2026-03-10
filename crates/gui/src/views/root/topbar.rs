use std::sync::Arc;

use gpui::*;
use gpui_component::{button::*, input::*, *};
use tracing::{error, warn};
use tungsten_net::model::{ConflictPolicy, DownloadRequest, IntegrityRule};
use tungsten_net::queue::QueueService;

use crate::paths::resolve_download_dir;

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
                        warn!("url is required");
                        return;
                    }

                    let destination = match resolve_download_dir() {
                        Ok(path) => path,
                        Err(error) => {
                            error!(
                                error = %error,
                                "failed to resolve default download directory"
                            );
                            return;
                        }
                    };

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
                            error!(error = %error, "failed to enqueue request");
                        }
                    }
                }),
        )
}
