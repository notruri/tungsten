use std::sync::{Arc, Mutex};

use gpui::*;
use gpui_component::table::TableState;
use gpui_component::*;
use tracing::error;
use tungsten_net::QueueService;

mod records;
mod topbar;

pub struct View {
    queue: Arc<QueueService>,
    records_state: Entity<TableState<records::QueueTableDelegate>>,
    _queue_sync_task: Task<()>,
}

impl View {
    pub fn new(window: &mut Window, cx: &mut Context<Self>, queue: Arc<QueueService>) -> Self {
        let records_state = records::new_state(Arc::clone(&queue), window, cx);
        let queue_sync_task = match queue.subscribe() {
            Ok(receiver) => {
                let receiver = Arc::new(Mutex::new(receiver));
                cx.spawn(async move |view, cx| {
                    loop {
                        let receiver_for_wait = Arc::clone(&receiver);
                        let received = cx
                            .background_spawn(async move {
                                let guard = receiver_for_wait.lock().ok()?;
                                guard.recv().ok()
                            })
                            .await;

                        if received.is_none() {
                            return;
                        }

                        let Some(view) = view.upgrade() else {
                            return;
                        };

                        if view.update(cx, |_, cx| cx.notify()).is_err() {
                            return;
                        }
                    }
                })
            }
            Err(error) => {
                error!(error = %error, "failed to subscribe to queue updates");
                Task::ready(())
            }
        };
        Self {
            queue,
            records_state,
            _queue_sync_task: queue_sync_task,
        }
    }

    fn create_interface(&self, window: &mut Window, cx: &mut Context<Self>) -> Div {
        records::sync(&self.records_state, &self.queue, cx);
        div()
            .v_flex()
            .gap_2()
            .size_full()
            .p_4()
            .child(topbar::queue_section(Arc::clone(&self.queue)))
            .child(records::section(&self.records_state))
            .children(Root::render_dialog_layer(window, cx))
            .children(Root::render_sheet_layer(window, cx))
            .children(Root::render_notification_layer(window, cx))
    }
}

impl Render for View {
    fn render(&mut self, w: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.create_interface(w, cx)
    }
}
