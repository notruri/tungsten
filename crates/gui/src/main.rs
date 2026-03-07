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
        cx.spawn(async move |cx| {
            cx.open_window(WindowOptions::default(), |window, cx| {
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
