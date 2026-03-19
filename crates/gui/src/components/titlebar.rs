use std::sync::Arc;

use gpui::*;
use gpui_component::menu::{DropdownMenu, PopupMenuItem};
use gpui_component::{button::*, *};
use tungsten_client::Client;

use crate::components::dialog::{about, queue};

pub fn create() -> impl IntoElement {
    TitleBar::new().child(
        div()
            .h_flex()
            .w_full()
            .items_center()
            .justify_between()
            .pr_2()
            .child(
                div()
                    .h_flex()
                    .items_center()
                    .gap_2()
                    .child(menu_button())
                    .child(div().text_sm().child("Tungsten")),
            ),
    )
}

pub fn menu_button() -> impl IntoElement {
    Button::new("open-topbar-menu")
        .ghost()
        .icon(Icon::default().path("icons/menu.svg"))
        .tooltip("open menu")
        .dropdown_menu_with_anchor(Corner::TopRight, move |menu, _, _| {
            menu.item(
                PopupMenuItem::new("About Tungsten").on_click(move |_, window, cx| {
                    about::open_dialog(window, cx);
                }),
            )
        })
}

pub fn add_button(
    client: Arc<Client>,
    settings: Arc<crate::preferences::SettingsStore>,
) -> impl IntoElement {
    Button::new("open-add-queue-dialog")
        .icon(Icon::default().path("icons/plus.svg"))
        .tooltip("add to queue")
        .on_mouse_down(MouseButton::Left, |_, window, cx| {
            window.prevent_default();
            cx.stop_propagation();
        })
        .on_click(move |_, window, cx| {
            queue::open_dialog(Arc::clone(&client), Arc::clone(&settings), window, cx);
        })
}
