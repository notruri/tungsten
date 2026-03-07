use gpui::*;
use gpui_component::{button::*, *};

pub struct App;
impl Render for App {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        div()
            .v_flex()
            .gap_2()
            .size_full()
            .items_center()
            .justify_center()
            .child("tungsten")
            .child(
                Button::new("ok")
                    .primary()
                    .label("click me!")
                    .on_click(|_, _, _| println!("clicked!")),
            )
    }
}
