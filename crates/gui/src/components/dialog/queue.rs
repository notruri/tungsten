use std::sync::Arc;

use gpui::*;
use gpui_component::dialog::DialogButtonProps;
use gpui_component::{input::*, *};
use tracing::{error, warn};
use tungsten_net::model::{ConflictPolicy, DownloadRequest, IntegrityRule};
use tungsten_net::queue::QueueService;

use crate::components::dialog;
use crate::settings::SettingsStore;

pub(crate) fn open_dialog(
    queue: Arc<QueueService>,
    settings: Arc<SettingsStore>,
    window: &mut Window,
    cx: &mut App,
) {
    let input_state = cx.new(|cx| {
        InputState::new(window, cx)
            .multi_line(true)
            .rows(8)
            .default_value("")
    });

    let queue_for_add = Arc::clone(&queue);
    let settings_for_add = Arc::clone(&settings);
    let input_state_for_add = input_state.clone();
    let input_state_for_dialog = input_state.clone();
    window.open_dialog(cx, move |dialog, _, _| {
        dialog
            .title("Add to queue")
            .width(px(580.0))
            .button_props(
                DialogButtonProps::default()
                    .show_cancel(true)
                    .ok_text("add to queue")
                    .cancel_text("cancel"),
            )
            .footer(dialog::dialog_footer("add-queue-dialog", "add to queue"))
            .on_ok({
                let queue_for_add = Arc::clone(&queue_for_add);
                let settings_for_add = Arc::clone(&settings_for_add);
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

                    let destination = match settings_for_add.current() {
                        Ok(current) => current.download_root,
                        Err(error) => {
                            error!(
                                error = %error,
                                "failed to resolve current download root from settings"
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
