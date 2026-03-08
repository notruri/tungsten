mod components;
mod views;

use std::path::PathBuf;
use std::sync::Arc;

use gpui::*;
use gpui_component::*;
use tungsten_io::DiskStateStore;
use tungsten_net::{QueueService, ReqwestBackend};

use crate::views::*;

fn main() {
    let queue = match build_queue() {
        Ok(queue) => Arc::new(queue),
        Err(error) => {
            eprintln!("failed to initialize queue service: {error}");
            return;
        }
    };

    let app = Application::new();

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

fn build_queue() -> Result<QueueService, tungsten_net::NetError> {
    let current = std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join("storage/tungsten-state.json");

    let store = Arc::new(DiskStateStore::new(current));
    let backend = Arc::new(ReqwestBackend::default());
    QueueService::new(backend, store, 3)
}
