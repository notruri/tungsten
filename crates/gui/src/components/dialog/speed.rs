use std::sync::Arc;

use gpui::*;
use gpui_component::dialog::DialogButtonProps;
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::slider::{Slider, SliderEvent, SliderState, SliderValue};
use gpui_component::*;
use tracing::{error, warn};
use tungsten_net::model::DownloadId;
use tungsten_net::queue::QueueService;

use crate::components::dialog;

const SLIDER_MAX_KBPS: u64 = 10_240;
const SLIDER_STEP_KBPS: f32 = 1.0;

pub(crate) fn open_dialog(
    queue: Arc<QueueService>,
    download_id: DownloadId,
    file_name: String,
    speed_limit_kbps: Option<u64>,
    window: &mut Window,
    cx: &mut App,
) {
    let initial_value = speed_limit_kbps
        .map(|value| value.to_string())
        .unwrap_or_default();
    let state = cx.new(|cx| SpeedDialogState::new(window, initial_value, cx));
    let input_for_dialog = state.read(cx).input.clone();
    let slider_for_dialog = state.read(cx).slider.clone();

    let queue_for_save = Arc::clone(&queue);
    let state_for_save = state.clone();
    window.open_dialog(cx, move |dialog, _, _| {
        dialog
            .title("Limit download speed")
            .width(px(420.0))
            .button_props(
                DialogButtonProps::default()
                    .show_cancel(true)
                    .ok_text("save")
                    .cancel_text("cancel"),
            )
            .footer(dialog::dialog_footer("speed-limit-dialog", "save"))
            .on_ok({
                let queue_for_save = Arc::clone(&queue_for_save);
                let state_for_save = state_for_save.clone();
                move |_, _, cx| {
                    let raw_value = state_for_save.read(cx).value().to_string();
                    let speed_limit_kbps = match parse_limit_text(&raw_value) {
                        Ok(value) => value,
                        Err(error) => {
                            error!(
                                download_id = %download_id,
                                error = %error,
                                "failed to parse speed limit"
                            );
                            warn!("speed limit must be a whole number");
                            return false;
                        }
                    };

                    if let Err(error) =
                        queue_for_save.set_speed_limit(download_id, speed_limit_kbps)
                    {
                        error!(
                            download_id = %download_id,
                            error = %error,
                            "failed to update download speed limit"
                        );
                        return false;
                    }

                    true
                }
            })
            .child(
                div()
                    .v_flex()
                    .gap_3()
                    .child(format!("set a speed cap for '{file_name}'."))
                    .child(
                        h_flex()
                            .items_center()
                            .gap_3()
                            .child(div().min_w(px(64.0)).child("global"))
                            .child(Slider::new(&slider_for_dialog).w_full())
                            .child(div().min_w(px(84.0)).text_right().child("unlimited")),
                    )
                    .child(Input::new(&input_for_dialog))
                    .child("limit in KiB/s. enter 0 for unlimited. empty to use global cap."),
            )
    });

    state.update(cx, |state, cx| state.focus(window, cx));
}

fn parse_limit_text(value: &str) -> Result<Option<u64>, std::num::ParseIntError> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }

    value.parse::<u64>().map(Some)
}

struct SpeedDialogState {
    input: Entity<InputState>,
    slider: Entity<SliderState>,
    value: SharedString,
    _input_subscription: Subscription,
    _slider_subscription: Subscription,
}

impl SpeedDialogState {
    fn new(window: &mut Window, value: String, cx: &mut Context<Self>) -> Self {
        let input = cx.new(|cx| InputState::new(window, cx).default_value(value.clone()));
        let slider_value = slider_value_for_text(&value).unwrap_or_default();
        let slider = cx.new(|_| {
            SliderState::new()
                .min(0.0)
                .max(slider_unlimited_value())
                .step(SLIDER_STEP_KBPS)
                .default_value(slider_value)
        });
        let input_subscription = cx.subscribe_in(
            &input,
            window,
            |this, input, event: &InputEvent, window, cx| {
                if matches!(event, InputEvent::Change) {
                    let next_value = input.read(cx).value().to_string();
                    this.apply_value(next_value, InputSource::Input, window, cx);
                }
            },
        );
        let slider_subscription = cx.subscribe_in(
            &slider,
            window,
            |this, _, event: &SliderEvent, window, cx| {
                let SliderEvent::Change(SliderValue::Single(value)) = event else {
                    return;
                };

                let next_value = text_for_slider_value(*value);
                this.apply_value(next_value, InputSource::Slider, window, cx);
            },
        );

        Self {
            input,
            slider,
            value: value.into(),
            _input_subscription: input_subscription,
            _slider_subscription: slider_subscription,
        }
    }

    fn value(&self) -> &str {
        self.value.as_ref()
    }

    fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.input
            .update(cx, |input, input_cx| input.focus(window, input_cx));
    }

    fn apply_value(
        &mut self,
        value: impl Into<SharedString>,
        source: InputSource,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let value = value.into();
        let changed = self.value != value;
        if changed {
            self.value = value.clone();
        }

        if source != InputSource::Input && self.input.read(cx).value() != value.as_ref() {
            let value = value.clone();
            self.input.update(cx, |input, input_cx| {
                input.set_value(value, window, input_cx);
            });
        }

        if source != InputSource::Slider {
            if let Ok(next_slider_value) = slider_value_for_text(value.as_ref()) {
                let current_slider_value = match self.slider.read(cx).value() {
                    SliderValue::Single(value) => value,
                    SliderValue::Range(_, end) => end,
                };

                if (current_slider_value - next_slider_value).abs() >= f32::EPSILON {
                    self.slider.update(cx, |slider, slider_cx| {
                        slider.set_value(next_slider_value, window, slider_cx);
                    });
                }
            }
        }

        if changed {
            cx.notify();
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum InputSource {
    Input,
    Slider,
}

fn slider_value_for_text(value: &str) -> Result<f32, std::num::ParseIntError> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(0.0);
    }

    let parsed = value.parse::<u64>()?;
    if parsed == 0 {
        Ok(slider_unlimited_value())
    } else {
        Ok(parsed.min(SLIDER_MAX_KBPS) as f32)
    }
}

fn text_for_slider_value(value: f32) -> SharedString {
    let rounded = value.round();
    if rounded <= 0.0 {
        SharedString::from("")
    } else if rounded >= slider_unlimited_value() {
        SharedString::from("0")
    } else {
        SharedString::from((rounded as u64).to_string())
    }
}

fn slider_unlimited_value() -> f32 {
    (SLIDER_MAX_KBPS + 1) as f32
}
