use std::sync::Arc;

use gpui::*;
use gpui_component::dialog::DialogButtonProps;
use gpui_component::select::{Select, SelectState};
use gpui_component::{button::*, input::*, *};
use regex::Regex;
use tracing::{error, warn};
use tungsten_net::queue::QueueService;

use crate::settings::{AppSettings, SettingsStore, ThemePreference};

pub(crate) fn open_dialog(
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
    let max_parallel_state = create_number_input(window, cx, current.max_parallel);
    let connections_state = create_number_input(window, cx, current.connections);
    let theme_state = cx.new(|select_cx| {
        SelectState::new(
            ThemePreference::all().into_iter().collect::<Vec<_>>(),
            theme_index(current.theme).map(|ix| IndexPath::default().row(ix)),
            window,
            select_cx,
        )
    });

    let queue_for_save = Arc::clone(&queue);
    let settings_for_save = Arc::clone(&settings);
    let download_root_for_save = download_root_state.clone();
    let fallback_name_for_save = fallback_name_state.clone();
    let max_parallel_for_save = max_parallel_state.clone();
    let connections_for_save = connections_state.clone();
    let theme_for_save = theme_state.clone();

    let download_root_for_picker = download_root_state.clone();
    let fallback_name_for_dialog = fallback_name_state.clone();
    let max_parallel_for_dialog = max_parallel_state.clone();
    let connections_for_dialog = connections_state.clone();
    let theme_for_dialog = theme_state.clone();

    window.open_dialog(cx, move |dialog, _, _| {
        dialog
            .title("Settings")
            .width(px(680.0))
            .button_props(
                DialogButtonProps::default()
                    .show_cancel(true)
                    .ok_text("save")
                    .cancel_text("cancel"),
            )
            .footer(super::dialog_footer("settings-dialog", "save"))
            .on_ok({
                let queue_for_save = Arc::clone(&queue_for_save);
                let settings_for_save = Arc::clone(&settings_for_save);
                let download_root_for_save = download_root_for_save.clone();
                let fallback_name_for_save = fallback_name_for_save.clone();
                let max_parallel_for_save = max_parallel_for_save.clone();
                let connections_for_save = connections_for_save.clone();
                let theme_for_save = theme_for_save.clone();

                move |_, window, cx| {
                    let download_root = download_root_for_save.read(cx).value().to_string();
                    let fallback_filename = fallback_name_for_save.read(cx).value().to_string();
                    let max_parallel_raw = max_parallel_for_save.read(cx).value().to_string();
                    let connections_raw = connections_for_save.read(cx).value().to_string();
                    let theme = theme_for_save
                        .read(cx)
                        .selected_value()
                        .copied()
                        .unwrap_or(current.theme);

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
                        theme,
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

                    next.theme.apply(Some(window), cx);
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
                                    window
                                        .spawn(cx, async move |cx| {
                                            let prompt = match cx.update(|_, app| {
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
                                            if let Err(error) = cx.update(move |window, app| {
                                                download_root_for_picker.update(
                                                    app,
                                                    |input, input_cx| {
                                                        input.set_value(
                                                            path_value, window, input_cx,
                                                        );
                                                    },
                                                );
                                            }) {
                                                error!(
                                                    error = %error,
                                                    "failed to update picked download root"
                                                );
                                            }
                                        })
                                        .detach();
                                }
                            })),
                    )
                    .child("default filename")
                    .child(Input::new(&fallback_name_for_dialog))
                    .child("theme")
                    .child(Select::new(&theme_for_dialog).placeholder("theme"))
                    .child("max parallel")
                    .child(number_input(&max_parallel_for_dialog, "max-parallel"))
                    .child("connections")
                    .child(number_input(&connections_for_dialog, "connections")),
            )
    });

    download_root_state.update(cx, |input, input_cx| input.focus(window, input_cx));
}

fn create_number_input(window: &mut Window, cx: &mut App, value: usize) -> Entity<InputState> {
    cx.new(|input_cx| {
        InputState::new(window, input_cx)
            .pattern(Regex::new(r"^[1-9][0-9]*$").expect("positive integer pattern must compile"))
            .default_value(value.to_string())
    })
}

fn theme_index(theme: ThemePreference) -> Option<usize> {
    ThemePreference::all()
        .iter()
        .position(|candidate| *candidate == theme)
}

fn number_input(state: &Entity<InputState>, id_prefix: &'static str) -> impl IntoElement {
    div()
        .h_flex()
        .items_center()
        .gap_2()
        .child(Button::new((id_prefix, 0usize)).label("-").on_click({
            let state = state.clone();
            move |_, window, cx| {
                step_number_input(&state, StepAction::Decrement, window, cx);
            }
        }))
        .child(Input::new(state).flex_1())
        .child(Button::new((id_prefix, 1usize)).label("+").on_click({
            let state = state.clone();
            move |_, window, cx| {
                step_number_input(&state, StepAction::Increment, window, cx);
            }
        }))
}

fn step_number_input(
    state: &Entity<InputState>,
    action: StepAction,
    window: &mut Window,
    cx: &mut App,
) {
    state.update(cx, |input, input_cx| {
        let current = match input.value().trim().parse::<usize>() {
            Ok(value) => value.max(1),
            Err(_) => 1,
        };
        let next = match action {
            StepAction::Increment => current.saturating_add(1),
            StepAction::Decrement => current.saturating_sub(1).max(1),
        };
        input.set_value(next.to_string(), window, input_cx);
    });
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
