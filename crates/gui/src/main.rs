#![cfg_attr(
    all(not(debug_assertions), target_os = "windows"),
    windows_subsystem = "windows"
)]

mod assets;
mod components;
mod paths;
mod settings;
mod views;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use gpui::Size;
use gpui::*;
use gpui_component::*;
use gpui_platform::application;
use settings::{AppSettings, SettingsStore};
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

use tungsten_runtime::*;
use tungsten_tray::*;

use crate::assets::*;
use crate::paths::*;
use crate::views::*;

const WINDOW_NAME: &str = "Tungsten";
const FONT_FAMILY: &str = "Inter";

fn main() {
    init_tracing();
    info!("tungsten is booting");

    let settings = match build_settings() {
        Ok(settings) => Arc::new(settings),
        Err(error) => {
            error!(error = %error, "failed to initialize settings");
            return;
        }
    };

    info!("loading user preferences");
    let preferences = match settings.current() {
        Ok(current) => current,
        Err(error) => {
            error!(error = %error, "failed to load user preferences");
            return;
        }
    };

    info!("initializing runtime");
    let runtime = match build_runtime(&preferences) {
        Ok(runtime) => Arc::new(runtime),
        Err(error) => {
            error!(error = %error, "failed to initialize runtime");
            return;
        }
    };

    info!("initializing");
    let app = application().with_assets(Assets);
    let _ = launch(app, runtime, settings, preferences);
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(
            "info,tungsten=debug,tungsten_runtime=debug,tungsten_net=debug,tungsten_io=debug",
        )
    });

    if let Err(error) = tracing_subscriber::fmt().with_env_filter(filter).try_init() {
        warn!(error = %error, "failed to initialize tracing subscriber");
    }
}

fn build_settings() -> anyhow::Result<SettingsStore> {
    debug!("building settings");
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

fn build_runtime(settings: &AppSettings) -> Result<Runtime, RuntimeError> {
    debug!("building runtime");
    let state_path =
        resolve_state_path().map_err(|error| RuntimeError::State(error.to_string()))?;
    let config = RuntimeConfig::new(state_path, settings.max_parallel, settings.connections)
        .download_limit_kbps(settings.download_limit_kbps)
        .fallback_filename(settings.fallback_filename.clone())
        .temp_root(settings.temp_dir.clone());
    Runtime::new(config)
}

fn launch(
    app: Application,
    runtime: Arc<Runtime>,
    settings: Arc<SettingsStore>,
    preferences: AppSettings,
) {
    debug!("launching app");
    let queue = runtime.queue();
    let initial_theme = preferences.theme;

    app.run(move |cx| {
        let _runtime = Arc::clone(&runtime);
        gpui_component::init(cx);
        initial_theme.apply(None, cx);

        let queue = Arc::clone(&queue);
        let settings = Arc::clone(&settings);
        let options = WindowOptions {
            window_bounds: Some(WindowBounds::Windowed(Bounds::new(
                point(px(120.0), px(80.0)),
                size(px(800.0), px(500.0)),
            ))),
            titlebar: Some(TitlebarOptions {
                title: Some(SharedString::from(WINDOW_NAME)),
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

        theme.font_family = SharedString::from(FONT_FAMILY);
        theme.font_size = px(14.0);

        let tray_icon =
            Assets::tray_icon().expect("missing bundled tray icon at assets/icons/tungsten.ico");
        let mut tray = Tray::new(tray_icon.as_ref()).expect("failed to initialize tray backend");

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
