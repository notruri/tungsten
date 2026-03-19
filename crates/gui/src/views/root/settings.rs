use std::path::PathBuf;

use gpui::*;
use gpui_component::button::*;
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::setting::{
    NumberFieldOptions, SettingField, SettingGroup, SettingItem, SettingPage, Settings,
};
use gpui_component::{Size as UiSize, *};
use tracing::error;

use crate::settings::{AppSettings, ThemePreference, ThemePreferenceExt};

#[derive(Debug)]
pub(super) struct Draft {
    persisted: AppSettings,
    current: AppSettings,
}

impl Draft {
    pub fn new(settings: AppSettings) -> Self {
        Self {
            persisted: settings.clone(),
            current: settings,
        }
    }

    pub fn persisted(&self) -> &AppSettings {
        &self.persisted
    }

    pub fn current(&self) -> &AppSettings {
        &self.current
    }

    fn set_download_root(&mut self, value: String) {
        self.current.download_root = PathBuf::from(value);
    }

    fn set_temp_dir(&mut self, value: String) {
        self.current.temp_dir = PathBuf::from(value);
    }

    fn set_fallback_filename(&mut self, value: String) {
        self.current.fallback_filename = value;
    }

    fn set_max_parallel(&mut self, value: f64) {
        self.current.max_parallel = value.max(1.0).round() as usize;
    }

    fn set_connections(&mut self, value: f64) {
        self.current.connections = value.max(1.0).round() as usize;
    }

    fn set_download_limit_kbps(&mut self, value: f64) {
        self.current.download_limit_kbps = value.max(0.0).round() as u64;
    }

    fn set_minimize_to_tray(&mut self, value: bool) {
        self.current.minimize_to_tray = value;
    }

    fn set_theme(&mut self, value: ThemePreference) {
        self.current.theme = value;
    }
}

pub(super) fn create(draft: &Entity<Draft>, _: &mut Window, cx: &mut App) -> impl IntoElement {
    let persisted = draft.read(cx).persisted().clone();

    Settings::new("app-settings")
        .sidebar_width(px(180.0))
        .page(
            SettingPage::new("Downloads")
                .description("Storage defaults for queued downloads.")
                .resettable(false)
                .group(
                    SettingGroup::new()
                        .item(download_root_item(draft))
                        .item(temp_dir_item(draft))
                        .item(
                            SettingItem::new(
                                "Default filename",
                                SettingField::<SharedString>::input(
                                    {
                                        let draft = draft.clone();
                                        move |cx| {
                                            SharedString::from(
                                                draft.read(cx).current().fallback_filename.clone(),
                                            )
                                        }
                                    },
                                    {
                                        let draft = draft.clone();
                                        move |value, cx| {
                                            draft.update(cx, |draft, _| {
                                                draft.set_fallback_filename(value.to_string());
                                            });
                                        }
                                    },
                                )
                                .default_value(SharedString::from(
                                    persisted.fallback_filename.clone(),
                                )),
                            )
                            .description("Used when a download target does not provide a filename."),
                        ),
                ),
        )
        .page(
            SettingPage::new("Performance")
                .description("Queue concurrency and per-download connections.")
                .resettable(false)
                .group(
                    SettingGroup::new()
                        .item(
                            SettingItem::new(
                                "Max parallel",
                                SettingField::<f64>::number_input(
                                    NumberFieldOptions {
                                        min: 1.0,
                                        max: f64::MAX,
                                        step: 1.0,
                                    },
                                    {
                                        let draft = draft.clone();
                                        move |cx| draft.read(cx).current().max_parallel as f64
                                    },
                                    {
                                        let draft = draft.clone();
                                        move |value, cx| {
                                            draft.update(cx, |draft, _| {
                                                draft.set_max_parallel(value);
                                            });
                                        }
                                    },
                                )
                                .default_value(persisted.max_parallel as f64),
                            )
                            .description("Maximum number of downloads running at the same time."),
                        )
                        .item(
                            SettingItem::new(
                                "Connections",
                                SettingField::<f64>::number_input(
                                    NumberFieldOptions {
                                        min: 1.0,
                                        max: f64::MAX,
                                        step: 1.0,
                                    },
                                    {
                                        let draft = draft.clone();
                                        move |cx| draft.read(cx).current().connections as f64
                                    },
                                    {
                                        let draft = draft.clone();
                                        move |value, cx| {
                                            draft.update(cx, |draft, _| {
                                                draft.set_connections(value);
                                            });
                                        }
                                    },
                                )
                                .default_value(persisted.connections as f64),
                            )
                            .description("Number of HTTP connections to use per download."),
                        )
                        .item(
                            SettingItem::new(
                                "Download limit",
                                SettingField::<f64>::number_input(
                                    NumberFieldOptions {
                                        min: 0.0,
                                        max: f64::MAX,
                                        step: 1.0,
                                    },
                                    {
                                        let draft = draft.clone();
                                        move |cx| {
                                            draft.read(cx).current().download_limit_kbps as f64
                                        }
                                    },
                                    {
                                        let draft = draft.clone();
                                        move |value, cx| {
                                            draft.update(cx, |draft, _| {
                                                draft.set_download_limit_kbps(value);
                                            });
                                        }
                                    },
                                )
                                .default_value(persisted.download_limit_kbps as f64),
                            )
                            .description("Aggregated cap in KB/s. Set to 0 for unlimited."),
                        ),
                ),
        )
        .page(
            SettingPage::new("Behavior")
                .description("Background app behavior when the window is closed.")
                .resettable(false)
                .group(
                    SettingGroup::new().item(
                        SettingItem::new(
                            "Close to tray",
                            SettingField::<bool>::switch(
                                {
                                    let draft = draft.clone();
                                    move |cx| draft.read(cx).current().minimize_to_tray
                                },
                                {
                                    let draft = draft.clone();
                                    move |value, cx| {
                                        draft.update(cx, |draft, _| {
                                            draft.set_minimize_to_tray(value);
                                        });
                                    }
                                },
                            )
                            .default_value(persisted.minimize_to_tray),
                        )
                        .description(
                            "Keep Tungsten running in the tray when the window is closed. (Windows only)",
                        ),
                    ),
                ),
        )
        .page(
            SettingPage::new("Appearance")
                .description("Theme changes preview immediately.")
                .resettable(false)
                .group(
                    SettingGroup::new().item(
                        SettingItem::new(
                            "Theme",
                            SettingField::<SharedString>::dropdown(
                                theme_options(),
                                {
                                    let draft = draft.clone();
                                    move |cx| {
                                        SharedString::from(draft.read(cx).current().theme.key())
                                    }
                                },
                                {
                                    let draft = draft.clone();
                                    move |value, cx| {
                                        let Some(theme) = ThemePreference::from_key(value.as_ref())
                                        else {
                                            return;
                                        };

                                        draft.update(cx, |draft, _| {
                                            draft.set_theme(theme);
                                        });
                                        theme.apply(None, cx);
                                    }
                                },
                            )
                            .default_value(SharedString::from(persisted.theme.key())),
                        )
                        .description("Choose whether Tungsten follows the system theme."),
                    ),
                ),
        )
        .with_size(UiSize::Small)
}

fn download_root_item(draft: &Entity<Draft>) -> SettingItem {
    SettingItem::new(
        "Download root",
        SettingField::<SharedString>::render({
            let draft = draft.clone();
            move |_, window, cx| download_root_field(&draft, window, cx)
        }),
    )
    .layout(Axis::Vertical)
    .description("Destination directory for new downloads.")
}

fn download_root_field(draft: &Entity<Draft>, window: &mut Window, cx: &mut App) -> AnyElement {
    let initial_value = draft
        .read(cx)
        .current()
        .download_root
        .to_string_lossy()
        .to_string();

    let state = window.use_keyed_state("settings-download-root", cx, |window, cx| {
        let input = cx.new(|cx| InputState::new(window, cx).default_value(initial_value.clone()));
        let subscription = cx.subscribe(&input, {
            let draft = draft.clone();
            move |_, input, event: &gpui_component::input::InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    let value = input.read(cx).value().to_string();
                    draft.update(cx, |draft, _| draft.set_download_root(value));
                }
            }
        });

        DownloadRootState {
            input,
            _subscription: subscription,
        }
    });
    let input = state.read(cx).input.clone();

    h_flex()
        .w_full()
        .items_center()
        .gap_2()
        .child(Input::new(&input).with_size(UiSize::Small).flex_1())
        .child(Button::new("browse-download-root").small().label("browse").on_click({
            let draft = draft.clone();
            let input = input.clone();
            move |_, window, cx| {
                let draft = draft.clone();
                let input = input.clone();
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
                                error!(error = %error, "failed to open download root picker");
                                return;
                            }
                        };

                        let picked = match prompt.await {
                            Ok(Ok(picked)) => picked,
                            Ok(Err(error)) => {
                                error!(error = %error, "download root picker failed");
                                return;
                            }
                            Err(error) => {
                                error!(error = %error, "download root picker was dropped");
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
                            draft.update(app, |draft, _| draft.set_download_root(path_value.clone()));
                            input.update(app, |input, input_cx| {
                                input.set_value(path_value, window, input_cx);
                            });
                        }) {
                            error!(error = %error, "failed to update picked download root");
                        }
                    })
                    .detach();
            }
        }))
        .into_any_element()
}

fn temp_dir_item(draft: &Entity<Draft>) -> SettingItem {
    SettingItem::new(
        "Temp dir",
        SettingField::<SharedString>::render({
            let draft = draft.clone();
            move |_, window, cx| temp_dir_field(&draft, window, cx)
        }),
    )
    .layout(Axis::Vertical)
    .description("Directory used to store in-progress download parts.")
}

fn temp_dir_field(draft: &Entity<Draft>, window: &mut Window, cx: &mut App) -> AnyElement {
    let initial_value = draft
        .read(cx)
        .current()
        .temp_dir
        .to_string_lossy()
        .to_string();

    let state = window.use_keyed_state("settings-temp-dir", cx, |window, cx| {
        let input = cx.new(|cx| InputState::new(window, cx).default_value(initial_value.clone()));
        let subscription = cx.subscribe(&input, {
            let draft = draft.clone();
            move |_, input, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    let value = input.read(cx).value().to_string();
                    draft.update(cx, |draft, _| draft.set_temp_dir(value));
                }
            }
        });

        TempDirState {
            input,
            _subscription: subscription,
        }
    });
    let input = state.read(cx).input.clone();

    h_flex()
        .w_full()
        .items_center()
        .gap_2()
        .child(Input::new(&input).with_size(UiSize::Small).flex_1())
        .child(
            Button::new("browse-temp-dir")
                .small()
                .label("browse")
                .on_click({
                    let draft = draft.clone();
                    let input = input.clone();
                    move |_, window, cx| {
                        let draft = draft.clone();
                        let input = input.clone();
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
                                        error!(error = %error, "failed to open temp dir picker");
                                        return;
                                    }
                                };

                                let picked = match prompt.await {
                                    Ok(Ok(picked)) => picked,
                                    Ok(Err(error)) => {
                                        error!(error = %error, "temp dir picker failed");
                                        return;
                                    }
                                    Err(error) => {
                                        error!(error = %error, "temp dir picker was dropped");
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
                                    draft.update(app, |draft, _| {
                                        draft.set_temp_dir(path_value.clone())
                                    });
                                    input.update(app, |input, input_cx| {
                                        input.set_value(path_value, window, input_cx);
                                    });
                                }) {
                                    error!(error = %error, "failed to update picked temp dir");
                                }
                            })
                            .detach();
                    }
                }),
        )
        .into_any_element()
}

fn theme_options() -> Vec<(SharedString, SharedString)> {
    ThemePreference::all()
        .into_iter()
        .map(|theme| {
            (
                SharedString::from(theme.key()),
                SharedString::from(theme.label()),
            )
        })
        .collect()
}

struct DownloadRootState {
    input: Entity<InputState>,
    _subscription: Subscription,
}

struct TempDirState {
    input: Entity<InputState>,
    _subscription: Subscription,
}
