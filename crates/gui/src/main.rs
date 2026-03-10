mod paths;
mod views;

use std::borrow::Cow;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::Arc;

use gpui::Size;
use gpui::*;
use gpui_component::*;
use tungsten_io::DiskStateStore;
use tungsten_net::NetError;
use tungsten_net::queue::{QueueConfig, QueueService};

use crate::paths::resolve_state_path;
use crate::views::*;

fn main() {
    let queue = match build_queue() {
        Ok(queue) => Arc::new(queue),
        Err(error) => {
            eprintln!("failed to initialize queue service: {error}");
            return;
        }
    };

    let app = Application::new().with_assets(GuiAssets {
        base: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets"),
    });

    app.run(move |cx| {
        gpui_component::init(cx);

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
