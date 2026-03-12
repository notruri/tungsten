use std::sync::Arc;

use gpui::*;
use gpui_component::menu::{DropdownMenu, PopupMenuItem};
use gpui_component::{button::*, *};
use tungsten_net::queue::QueueService;

use crate::components::dialog::{about, queue};
use crate::settings::SettingsStore;

pub fn create(
    queue: Arc<QueueService>,
    settings: Arc<SettingsStore>,
    show_add_button: bool,
) -> impl IntoElement {
    let actions = if show_add_button {
        div()
            .h_flex()
            .items_center()
            .gap_2()
            .child(add_button(Arc::clone(&queue), Arc::clone(&settings)))
    } else {
        div().h_flex().items_center().gap_2()
    };

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
            )
            .child(actions),
    )
}

pub fn menu_button() -> impl IntoElement {
    Button::new("open-topbar-menu")
        .ghost()
        .icon(Icon::default().path("icons/menu.svg"))
        .tooltip("open menu")
        .dropdown_menu_with_anchor(Corner::TopRight, move |menu, _, _| {
            menu.item(PopupMenuItem::new("About Tungsten").on_click(move |_, window, cx| {
                about::open_dialog(window, cx);
            }))
        })
}

pub fn add_button(queue: Arc<QueueService>, settings: Arc<SettingsStore>) -> impl IntoElement {
    Button::new("open-add-queue-dialog")
        .icon(Icon::default().path("icons/plus.svg"))
        .tooltip("add to queue")
        .on_mouse_down(MouseButton::Left, |_, window, cx| {
            window.prevent_default();
            cx.stop_propagation();
        })
        .on_click(move |_, window, cx| {
            queue::open_dialog(Arc::clone(&queue), Arc::clone(&settings), window, cx);
        })
}
