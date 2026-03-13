use gpui::*;
use gpui_component::button::*;
use gpui_component::dialog::{CancelDialog, ConfirmDialog, DialogFooter};

pub mod about;
pub mod queue;
pub mod speed;

pub(super) fn dialog_footer(id_prefix: &'static str, ok_text: &'static str) -> impl IntoElement {
    DialogFooter::new()
        .child(
            Button::new((id_prefix, 0usize))
                .label("cancel")
                .outline()
                .on_click(|_, window, cx| {
                    window.dispatch_action(Box::new(CancelDialog), cx);
                }),
        )
        .child(
            Button::new((id_prefix, 1usize))
                .label(ok_text)
                .primary()
                .on_click(|_, window, cx| {
                    window.dispatch_action(Box::new(ConfirmDialog), cx);
                }),
        )
}
