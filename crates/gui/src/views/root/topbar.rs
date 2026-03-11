use std::sync::Arc;

use gpui::*;
use gpui_component::dialog::DialogButtonProps;
use gpui_component::menu::{DropdownMenu, PopupMenuItem};
use gpui_component::{button::*, input::*, *};
use tracing::{error, warn};
use tungsten_net::model::{ConflictPolicy, DownloadRequest, IntegrityRule};
use tungsten_net::queue::QueueService;

use crate::settings::{AppSettings, SettingsStore};

pub fn queue_section(queue: Arc<QueueService>, settings: Arc<SettingsStore>) -> impl IntoElement {
    let queue_for_add_modal = Arc::clone(&queue);
    let settings_for_add_modal = Arc::clone(&settings);
    let queue_for_settings_modal = Arc::clone(&queue);
    let settings_for_settings_modal = Arc::clone(&settings);

    TitleBar::new().child(
        div()
            .h_flex()
            .w_full()
            .items_center()
            .justify_between()
            .pr_2()
            .child(
                div()
                    .h_flex()
                    .items_center()
                    .gap_2()
                    .child(
                        Button::new("open-topbar-menu")
                            .icon(Icon::default().path("icons/menu.svg"))
                            .tooltip("open menu")
                            .dropdown_menu_with_anchor(Corner::TopRight, move |menu, _, _| {
                                let queue_for_settings_modal =
                                    Arc::clone(&queue_for_settings_modal);
                                let settings_for_settings_modal =
                                    Arc::clone(&settings_for_settings_modal);

                                menu.item(PopupMenuItem::new("open settings").on_click(
                                    move |_, window, cx| {
                                        open_settings_dialog(
                                            Arc::clone(&queue_for_settings_modal),
                                            Arc::clone(&settings_for_settings_modal),
                                            window,
                                            cx,
                                        );
                                    },
                                ))
                            }),
                    )
                    .child(div().text_sm().child("Tungsten")),
            )
            .child(
                div()
                    .h_flex()
                    .items_center()
                    .gap_2()
                    .child(
                        Button::new("open-add-queue-dialog")
                            .icon(Icon::default().path("icons/plus.svg"))
                            .tooltip("add to queue")
                            .on_click(move |_, window, cx| {
                                open_add_queue_dialog(
                                    Arc::clone(&queue_for_add_modal),
                                    Arc::clone(&settings_for_add_modal),
                                    window,
                                    cx,
                                );
                            }),
                    ),
            ),
    )
}

fn open_add_queue_dialog(
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

fn open_settings_dialog(
    queue: Arc<QueueService>,
    settings: Arc<SettingsStore>,
    window: &mut Window,
    cx: &mut App,
) {
    let current = match settings.current() {
        Ok(current) => current,
        Err(error) => {
            error!(error = %error, "failed to read current settings");
            return;
        }
    };

    let download_root_state = cx.new(|input_cx| {
        InputState::new(window, input_cx)
            .default_value(current.download_root.to_string_lossy().to_string())
    });
    let fallback_name_state = cx.new(|input_cx| {
        InputState::new(window, input_cx).default_value(current.fallback_filename.clone())
    });
    let max_parallel_state = cx.new(|input_cx| {
        InputState::new(window, input_cx).default_value(current.max_parallel.to_string())
    });
    let connections_state = cx.new(|input_cx| {
        InputState::new(window, input_cx).default_value(current.connections.to_string())
    });

    let queue_for_save = Arc::clone(&queue);
    let settings_for_save = Arc::clone(&settings);
    let download_root_for_save = download_root_state.clone();
    let fallback_name_for_save = fallback_name_state.clone();
    let max_parallel_for_save = max_parallel_state.clone();
    let connections_for_save = connections_state.clone();

    let download_root_for_picker = download_root_state.clone();
    let fallback_name_for_dialog = fallback_name_state.clone();
    let max_parallel_for_dialog = max_parallel_state.clone();
    let connections_for_dialog = connections_state.clone();

    window.open_dialog(cx, move |dialog, _, _| {
        dialog
            .confirm()
            .title("settings")
            .width(px(680.0))
            .button_props(
                DialogButtonProps::default()
                    .ok_text("save")
                    .cancel_text("cancel"),
            )
            .on_ok({
                let queue_for_save = Arc::clone(&queue_for_save);
                let settings_for_save = Arc::clone(&settings_for_save);
                let download_root_for_save = download_root_for_save.clone();
                let fallback_name_for_save = fallback_name_for_save.clone();
                let max_parallel_for_save = max_parallel_for_save.clone();
                let connections_for_save = connections_for_save.clone();

                move |_, _, cx| {
                    let download_root = download_root_for_save.read(cx).value().to_string();
                    let fallback_filename = fallback_name_for_save.read(cx).value().to_string();
                    let max_parallel_raw = max_parallel_for_save.read(cx).value().to_string();
                    let connections_raw = connections_for_save.read(cx).value().to_string();

                    let max_parallel = match parse_positive_usize("max_parallel", &max_parallel_raw)
                    {
                        Ok(value) => value,
                        Err(message) => {
                            warn!(error = %message, "invalid settings value");
                            return false;
                        }
                    };
                    let connections = match parse_positive_usize("connections", &connections_raw) {
                        Ok(value) => value,
                        Err(message) => {
                            warn!(error = %message, "invalid settings value");
                            return false;
                        }
                    };

                    let next = AppSettings {
                        download_root: download_root.into(),
                        fallback_filename,
                        max_parallel,
                        connections,
                    }
                    .normalize();
                    if let Err(error) = next.validate() {
                        warn!(error = %error, "invalid settings data");
                        return false;
                    }

                    if let Err(error) = settings_for_save.save(next.clone()) {
                        error!(error = %error, "failed to save config.toml");
                        return false;
                    }

                    if let Err(error) = queue_for_save.set_max_parallel(next.max_parallel) {
                        error!(error = %error, "failed to apply max_parallel");
                        return false;
                    }
                    if let Err(error) = queue_for_save.set_connections(next.connections) {
                        error!(error = %error, "failed to apply connections");
                        return false;
                    }
                    if let Err(error) = queue_for_save.set_fallback_filename(next.fallback_filename)
                    {
                        error!(error = %error, "failed to apply fallback filename");
                        return false;
                    }

                    true
                }
            })
            .child(
                div()
                    .v_flex()
                    .gap_3()
                    .child("download root")
                    .child(
                        div()
                            .h_flex()
                            .items_center()
                            .gap_2()
                            .child(Input::new(&download_root_for_picker).flex_1())
                            .child(Button::new("pick-download-root").label("browse").on_click({
                                let download_root_for_picker = download_root_for_picker.clone();
                                move |_, window, cx| {
                                    let download_root_for_picker = download_root_for_picker.clone();
                                    let window_handle = window.window_handle();
                                    cx.spawn(async move |cx| {
                                        let prompt = match cx.update(|app| {
                                            app.prompt_for_paths(PathPromptOptions {
                                                files: false,
                                                directories: true,
                                                multiple: false,
                                                prompt: Some("Select folder".into()),
                                            })
                                        }) {
                                            Ok(prompt) => prompt,
                                            Err(error) => {
                                                error!(
                                                    error = %error,
                                                    "failed to open download root picker"
                                                );
                                                return;
                                            }
                                        };

                                        let picked = match prompt.await {
                                            Ok(Ok(picked)) => picked,
                                            Ok(Err(error)) => {
                                                error!(
                                                    error = %error,
                                                    "download root picker failed"
                                                );
                                                return;
                                            }
                                            Err(error) => {
                                                error!(
                                                    error = %error,
                                                    "download root picker was dropped"
                                                );
                                                return;
                                            }
                                        };
                                        let Some(paths) = picked else {
                                            return;
                                        };
                                        let Some(path) = paths.into_iter().next() else {
                                            return;
                                        };

                                        let path_value = path.to_string_lossy().to_string();
                                        if let Err(error) = cx.update_window(
                                            window_handle,
                                            move |_, window, app| {
                                                download_root_for_picker.update(
                                                    app,
                                                    |input, input_cx| {
                                                        input.set_value(
                                                            path_value, window, input_cx,
                                                        );
                                                    },
                                                );
                                            },
                                        ) {
                                            error!(
                                                error = %error,
                                                "failed to apply picked download root"
                                            );
                                        }
                                    })
                                    .detach();
                                }
                            })),
                    )
                    .child("fallback filename")
                    .child(Input::new(&fallback_name_for_dialog))
                    .child("max parallel")
                    .child(Input::new(&max_parallel_for_dialog))
                    .child("connections")
                    .child(Input::new(&connections_for_dialog)),
            )
    });

    download_root_state.update(cx, |input, input_cx| input.focus(window, input_cx));
}

fn parse_positive_usize(field: &str, value: &str) -> Result<usize, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("{field} is required"));
    }

    let parsed = trimmed
        .parse::<usize>()
        .map_err(|_| format!("{field} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{field} must be at least 1"));
    }

    Ok(parsed)
}
