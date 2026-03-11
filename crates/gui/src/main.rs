mod paths;
mod views;

use std::borrow::Cow;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::Arc;

use gpui::Size;
use gpui::*;
use gpui_component::*;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use tungsten_io::DiskStateStore;
use tungsten_net::NetError;
use tungsten_net::queue::{QueueConfig, QueueService};

use crate::paths::resolve_state_path;
use crate::views::*;

fn main() {
    init_tracing();
    info!("starting gui");

    let queue = match build_queue() {
        Ok(queue) => Arc::new(queue),
        Err(error) => {
            error!(error = %error, "failed to initialize queue service");
            return;
        }
    };

    let app = Application::new().with_assets(GuiAssets {
        base: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets"),
    });

    app.run(move |cx| {
        gpui_component::init(cx);
        let bundled_font_family = match load_bundled_fonts(cx) {
            Ok(family) => family,
            Err(error) => {
                warn!(error = %error, "failed to load bundled inter fonts");
                None
            }
        };

        let queue = Arc::clone(&queue);
        let options = WindowOptions {
            window_bounds: Some(WindowBounds::Windowed(Bounds::new(
                point(px(120.0), px(80.0)),
                size(px(1000.0), px(700.0)),
            ))),
            titlebar: Some(TitlebarOptions {
                title: Some(SharedString::from("Tungsten")),
                appears_transparent: false,
                traffic_light_position: Some(Point::new(px(12.0), px(12.0))),
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

        cx.spawn(async move |cx| {
            cx.open_window(options, |window, cx| {
                let queue = Arc::clone(&queue);
                let view = cx.new(|cx| View::new(window, cx, Arc::clone(&queue)));
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
                .is_some_and(|ext| ext.eq_ignore_ascii_case("ttf") || ext.eq_ignore_ascii_case("otf"))
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
    const PREFERRED: [&str; 5] = ["Inter", "Inter Variable", "Inter 18pt", "Inter 24pt", "Inter 28pt"];
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

fn build_queue() -> Result<QueueService, NetError> {
    let state_path = resolve_state_path().map_err(|error| NetError::State(error.to_string()))?;
    let store = Arc::new(DiskStateStore::new(state_path));
    QueueService::new(QueueConfig::new(3, 4), store)
}
