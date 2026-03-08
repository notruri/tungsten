use std::sync::Arc;

use gpui::*;
use gpui_component::{input::*, *};
use tungsten_net::QueueService;

mod topbar;
mod records;

pub struct View {
    queue: Arc<QueueService>,
    input_state: Entity<InputState>,
}

impl View {
    pub fn new(window: &mut Window, cx: &mut Context<Self>, queue: Arc<QueueService>) -> Self {
        let input_state = cx.new(|cx| {
            InputState::new(window, cx)
                .default_value("")
        });
        Self { queue, input_state }
    }

    fn create_interface(&self, _: &mut Window, _: &mut Context<Self>) -> Div {
        div()
            .v_flex()
            .gap_2()
            .size_full()
            .p_4()
            .child(":3")
            .child(topbar::queue_section(Arc::clone(&self.queue), self.input_state.clone()))
            .child(records::section(Arc::clone(&self.queue)))
    }
}

impl Render for View {
    fn render(&mut self, w: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.create_interface(w, cx)
    }
}
