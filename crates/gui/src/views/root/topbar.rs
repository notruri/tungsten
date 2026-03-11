use std::sync::Arc;

use gpui::*;
use gpui_component::dialog::DialogButtonProps;
use gpui_component::{button::*, input::*, *};
use tracing::{error, warn};
use tungsten_net::model::{ConflictPolicy, DownloadRequest, IntegrityRule};
use tungsten_net::queue::QueueService;

use crate::paths::resolve_download_dir;

pub fn queue_section(queue: Arc<QueueService>) -> impl IntoElement {
    let queue_for_modal = Arc::clone(&queue);
    TitleBar::new().child(
        div()
            .h_flex()
            .w_full()
            .items_center()
            .justify_between()
            .pr_2()
            .child(div().text_sm().child("Tungsten"))
            .child(
                Button::new("open-add-queue-dialog")
                    .icon(Icon::default().path("icons/plus.svg"))
                    .tooltip("add to queue")
                    .on_click(move |_, window, cx| {
                        open_add_queue_dialog(Arc::clone(&queue_for_modal), window, cx);
                    }),
            ),
    )
}

fn open_add_queue_dialog(queue: Arc<QueueService>, window: &mut Window, cx: &mut App) {
    let input_state = cx.new(|cx| {
        InputState::new(window, cx)
            .multi_line(true)
            .rows(8)
            .default_value("")
    });

    let queue_for_add = Arc::clone(&queue);
    let input_state_for_add = input_state.clone();
    let input_state_for_dialog = input_state.clone();
    window.open_dialog(cx, move |dialog, _, _| {
        dialog
            .confirm()
            .title("add to queue")
            .width(px(580.0))
            .button_props(
                DialogButtonProps::default()
                    .ok_text("add to queue")
                    .cancel_text("cancel"),
            )
            .on_ok({
                let queue_for_add = Arc::clone(&queue_for_add);
                let input_state_for_add = input_state_for_add.clone();
                move |_, _, cx| {
                    let value = input_state_for_add.read(cx).value().to_string();
                    let urls: Vec<&str> = value
                        .lines()
                        .map(str::trim)
                        .filter(|line| !line.is_empty())
                        .collect();

                    if urls.is_empty() {
                        warn!("at least one URL is required");
                        return false;
                    }

                    let destination = match resolve_download_dir() {
                        Ok(path) => path,
                        Err(error) => {
                            error!(
                                error = %error,
                                "failed to resolve default download directory"
                            );
                            return false;
                        }
                    };

                    let mut enqueued = 0usize;
                    for url in urls {
                        let request = DownloadRequest::new(
                            url.to_string(),
                            destination.clone(),
                            ConflictPolicy::AutoRename,
                            IntegrityRule::None,
                        );

                        match queue_for_add.enqueue(request) {
                            Ok(_) => {
                                enqueued += 1;
                            }
                            Err(error) => {
                                error!(url = %url, error = %error, "failed to enqueue request");
                            }
                        }
                    }

                    if enqueued == 0 {
                        warn!("no entries were added to queue");
                        return false;
                    }

                    true
                }
            })
            .child(
                div()
                    .v_flex()
                    .gap_2()
                    .child("paste URLs below, one per line")
                    .child(Input::new(&input_state_for_dialog).h(px(220.0))),
            )
    });

    input_state.update(cx, |input, input_cx| input.focus(window, input_cx));
}
