mod components;
mod paths;
mod settings;
mod views;

use std::borrow::Cow;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use gpui::Size;
use gpui::*;
use gpui_component::*;
use gpui_platform::application;
use settings::{AppSettings, SettingsStore};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use tungsten_io::DiskStateStore;
use tungsten_net::NetError;
use tungsten_net::queue::{QueueConfig, QueueService};
use tungsten_tray::{Tray, TrayEvent, hide_window, show_window};

use crate::paths::{resolve_config_path, resolve_state_path};
use crate::views::*;

fn main() {
    init_tracing();
    info!("starting gui");

    let settings = match build_settings() {
        Ok(settings) => Arc::new(settings),
        Err(error) => {
            error!(error = %error, "failed to initialize settings");
            return;
        }
    };

    let current_settings = match settings.current() {
        Ok(current) => current,
        Err(error) => {
            error!(error = %error, "failed to read current settings");
            return;
        }
    };

    let queue = match build_queue(&current_settings) {
        Ok(queue) => Arc::new(queue),
        Err(error) => {
            error!(error = %error, "failed to initialize queue service");
            return;
        }
    };
    let initial_theme = current_settings.theme;

    let assets_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets");
    let app = application().with_assets(GuiAssets {
        base: assets_dir.clone(),
    });

    app.run(move |cx| {
        gpui_component::init(cx);
        initial_theme.apply(None, cx);
        let bundled_font_family = match load_bundled_fonts(cx) {
            Ok(family) => family,
            Err(error) => {
                warn!(error = %error, "failed to load bundled inter fonts");
                None
            }
        };

        let queue = Arc::clone(&queue);
        let settings = Arc::clone(&settings);
        let options = WindowOptions {
            window_bounds: Some(WindowBounds::Windowed(Bounds::new(
                point(px(120.0), px(80.0)),
                size(px(800.0), px(500.0)),
            ))),
            titlebar: Some(TitlebarOptions {
                title: Some(SharedString::from("Tungsten")),
                ..TitleBar::title_bar_options()
            }),
            focus: true,
            show: true,
            is_resizable: true,
            is_movable: true,
            is_minimizable: true,
            window_min_size: Some(Size::new(px(640.0), px(480.0))),
            window_background: WindowBackgroundAppearance::Blurred,
            ..WindowOptions::default()
        };
        let theme = Theme::global_mut(cx);
        if let Some(font_family) = bundled_font_family {
            theme.font_family = font_family;
        }
        theme.font_size = px(14.0);

        let icon_path = assets_dir.join("icons/tungsten.ico");
        let mut tray = match Tray::new(&icon_path) {
            Ok(tray) => tray,
            Err(error) => {
                warn!(error = %error, "failed to initialize tray backend");
                Tray::disabled()
            }
        };
        let tray_enabled = tray.is_enabled();
        let quit_requested = Arc::new(AtomicBool::new(false));
        let window_handle = Arc::new(Mutex::new(None::<AnyWindowHandle>));

        if let Some(receiver) = tray.take_receiver().filter(|_| tray_enabled) {
            let receiver = Arc::new(Mutex::new(receiver));
            let quit_requested = Arc::clone(&quit_requested);
            let window_handle = Arc::clone(&window_handle);

            cx.spawn(async move |cx| {
                loop {
                    let receiver_for_wait = Arc::clone(&receiver);
                    let event = cx
                        .background_spawn(async move {
                            let guard = receiver_for_wait.lock().ok()?;
                            guard.recv().ok()
                        })
                        .await;

                    let Some(event) = event else {
                        return;
                    };

                    cx.update(|app| match event {
                        TrayEvent::Show => {
                            let handle = window_handle.lock().ok().and_then(|guard| *guard);
                            if let Some(handle) = handle {
                                let _ = handle.update(app, |_, window, _| {
                                    if let Err(error) = show_window(window) {
                                        warn!(error = %error, "failed to show hidden window");
                                    }
                                    window.activate_window();
                                });
                            }
                        }
                        TrayEvent::Quit => {
                            quit_requested.store(true, Ordering::SeqCst);
                            app.quit();
                        }
                    });
                }
            })
            .detach();
        }

        let _tray = Box::leak(Box::new(tray));

        cx.spawn(async move |cx| {
            let settings = Arc::clone(&settings);
            let quit_requested = Arc::clone(&quit_requested);
            let window_handle = Arc::clone(&window_handle);

            cx.open_window(options, |window, cx| {
                let queue = Arc::clone(&queue);
                let settings = Arc::clone(&settings);
                let close_settings = Arc::clone(&settings);
                let quit_requested = Arc::clone(&quit_requested);
                let window_handle = Arc::clone(&window_handle);
                initial_theme.apply(Some(window), cx);
                if let Ok(mut guard) = window_handle.lock() {
                    *guard = Some(window.window_handle());
                }
                window.on_window_should_close(cx, move |window, _| {
                    if quit_requested.load(Ordering::SeqCst) {
                        return true;
                    }

                    let minimize_to_tray = close_settings
                        .current()
                        .map(|current| current.minimize_to_tray)
                        .unwrap_or(false);
                    if minimize_to_tray {
                        if let Err(error) = hide_window(window) {
                            warn!(error = %error, "failed to hide window to tray");
                            window.minimize_window();
                        }
                        false
                    } else {
                        true
                    }
                });
                let view =
                    cx.new(|cx| View::new(window, cx, Arc::clone(&queue), Arc::clone(&settings)));
                cx.new(|cx| Root::new(view, window, cx))
            })?;

            Ok::<_, anyhow::Error>(())
        })
        .detach();
    });
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("info,tungsten=debug,tungsten_net=debug,tungsten_io=debug")
    });

    if let Err(error) = tracing_subscriber::fmt().with_env_filter(filter).try_init() {
        warn!(error = %error, "failed to initialize tracing subscriber");
    }
}

fn load_bundled_fonts(cx: &mut App) -> anyhow::Result<Option<SharedString>> {
    let fonts_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets/fonts/inter");
    let mut font_paths = std::fs::read_dir(&fonts_dir)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| {
                    ext.eq_ignore_ascii_case("ttf") || ext.eq_ignore_ascii_case("otf")
                })
        })
        .collect::<Vec<_>>();
    font_paths.sort();

    if font_paths.is_empty() {
        warn!(path = %fonts_dir.display(), "no bundled fonts found");
        return Ok(None);
    }

    let mut fonts = Vec::with_capacity(font_paths.len());
    for path in &font_paths {
        match std::fs::read(path) {
            Ok(bytes) => {
                let leaked: &'static mut [u8] = Box::leak(bytes.into_boxed_slice());
                let leaked: &'static [u8] = leaked;
                fonts.push(Cow::Borrowed(leaked));
            }
            Err(error) => {
                warn!(path = %path.display(), error = %error, "failed to read bundled font file");
            }
        }
    }

    if fonts.is_empty() {
        warn!(path = %fonts_dir.display(), "all bundled font reads failed");
        return Ok(None);
    }

    cx.text_system().add_fonts(fonts)?;
    let mut inter_families = cx
        .text_system()
        .all_font_names()
        .into_iter()
        .filter(|name| name.to_ascii_lowercase().contains("inter"))
        .collect::<Vec<_>>();
    inter_families.sort();
    inter_families.dedup();

    let selected_family = select_inter_family(&inter_families).map(SharedString::from);
    info!(
        count = font_paths.len(),
        selected_family = ?selected_family,
        families = ?inter_families,
        path = %fonts_dir.display(),
        "loaded bundled fonts"
    );
    Ok(selected_family)
}

fn select_inter_family(families: &[String]) -> Option<String> {
    const PREFERRED: [&str; 5] = [
        "Inter",
        "Inter Variable",
        "Inter 18pt",
        "Inter 24pt",
        "Inter 28pt",
    ];
    for preferred in PREFERRED {
        if let Some(found) = families
            .iter()
            .find(|family| family.eq_ignore_ascii_case(preferred))
        {
            return Some(found.clone());
        }
    }

    families
        .iter()
        .find(|family| family.to_ascii_lowercase().starts_with("inter "))
        .cloned()
        .or_else(|| families.first().cloned())
}

struct GuiAssets {
    base: PathBuf,
}

impl AssetSource for GuiAssets {
    fn load(&self, path: &str) -> anyhow::Result<Option<Cow<'static, [u8]>>> {
        let full_path = self.base.join(path);
        match std::fs::read(full_path) {
            Ok(bytes) => Ok(Some(Cow::Owned(bytes))),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn list(&self, path: &str) -> anyhow::Result<Vec<SharedString>> {
        let full_path = self.base.join(path);
        let entries = match std::fs::read_dir(full_path) {
            Ok(entries) => entries,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };

        Ok(entries
            .filter_map(|entry| {
                entry
                    .ok()
                    .and_then(|entry| entry.file_name().into_string().ok())
                    .map(SharedString::from)
            })
            .collect())
    }
}

fn build_settings() -> anyhow::Result<SettingsStore> {
    let config_path = resolve_config_path()?;
    match SettingsStore::load(config_path.clone()) {
        Ok(store) => Ok(store),
        Err(error) => {
            warn!(
                path = %config_path.display(),
                error = %error,
                "failed to load config.toml; using defaults"
            );
            SettingsStore::with_defaults(config_path)
        }
    }
}

fn build_queue(settings: &AppSettings) -> Result<QueueService, NetError> {
    let state_path = resolve_state_path().map_err(|error| NetError::State(error.to_string()))?;
    let store = Arc::new(DiskStateStore::new(state_path));
    let config = QueueConfig::new(settings.max_parallel, settings.connections)
        .download_limit_kbps(settings.download_limit_kbps)
        .fallback_filename(settings.fallback_filename.clone());
    QueueService::new(config, store)
}
