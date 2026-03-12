use gpui::*;
use gpui_component::dialog::DialogButtonProps;
use gpui_component::scroll::ScrollableElement;
use gpui_component::text::markdown;
use gpui_component::*;

const ABOUT_MARKDOWN: &str = concat!(
    "# Tungsten\n\n",
    "`v",
    env!("CARGO_PKG_VERSION"),
    "`\n\n",
    "yet another downloader.\n\n",
    "made by [Ruri](https://github.com/NotRuri) with 🤍\n",
    "built with [GPUI](https://github.com/zed-industries/zed) ",
    "and [gpui-component](https://github.com/longbridge/gpui-component).\n",
);

pub(crate) fn open_dialog(window: &mut Window, cx: &mut App) {
    window.open_dialog(cx, move |dialog, _, _| {
        dialog
            .title("About Tungsten")
            .width(px(520.0))
            .button_props(DialogButtonProps::default().ok_text("close"))
            .child(
                div()
                    .max_h(px(320.0))
                    .overflow_y_scrollbar()
                    .child(markdown(ABOUT_MARKDOWN)),
            )
    });
}
