use std::sync::Arc;

use gpui::*;
use gpui_component::dialog::DialogButtonProps;
use gpui_component::input::{InputEvent, InputState, NumberInput, NumberInputEvent, StepAction};
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
    let initial_value = text_for_limit(speed_limit_kbps).to_string();
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
                    .text_sm()
                    .child(format!("set a speed cap for '{file_name}'."))
                    .child(
                        h_flex()
                            .gap_3()
                            .child(Slider::new(&slider_for_dialog).w_full())
                            .child(
                                NumberInput::new(&input_for_dialog)
                                    .small()
                                    .placeholder("global")
                                    .suffix("KiB/s"),
                            ),
                    )
                    .child("enter 0 for unlimited. empty to use global cap."),
            )
    });

    state.update(cx, |state, cx| state.focus(window, cx));
}

fn parse_limit_text(value: &str) -> Result<Option<u64>, std::num::ParseIntError> {
    let value = value.trim();
    if value.is_empty() || value.eq_ignore_ascii_case("global") || value == "0" {
        return Ok(None);
    }
    if value.eq_ignore_ascii_case("unlimited") {
        return Ok(Some(0));
    }

    value.parse::<u64>().map(Some)
}

struct SpeedDialogState {
    input: Entity<InputState>,
    slider: Entity<SliderState>,
    value: SharedString,
    _input_subscription: Subscription,
    _step_subscription: Subscription,
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
        let step_subscription = cx.subscribe_in(
            &input,
            window,
            |this, _, event: &NumberInputEvent, window, cx| {
                let NumberInputEvent::Step(action) = event;
                let next_value = step_value(this.value(), *action);
                this.apply_value(next_value, InputSource::Input, window, cx);
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
            _step_subscription: step_subscription,
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
        let value = normalize_value(value);
        let changed = self.value != value;
        if changed {
            self.value = value.clone();
        }

        if self.input.read(cx).value() != value.as_ref() {
            let value = value.clone();
            self.input.update(cx, |input, input_cx| {
                input.set_value(value, window, input_cx);
            });
        }

        if source != InputSource::Slider
            && let Ok(next_slider_value) = slider_value_for_text(value.as_ref())
        {
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
    if value.is_empty() || value.eq_ignore_ascii_case("global") || value == "0" {
        return Ok(0.0);
    }
    if value.eq_ignore_ascii_case("unlimited") {
        return Ok(slider_unlimited_value());
    }

    let parsed = value.parse::<u64>()?;
    if parsed == 0 {
        Ok(0.0)
    } else {
        Ok(parsed.min(SLIDER_MAX_KBPS) as f32)
    }
}

fn text_for_slider_value(value: f32) -> SharedString {
    let rounded = value.round();
    if rounded <= 0.0 {
        SharedString::from("global")
    } else if rounded >= slider_unlimited_value() {
        SharedString::from("unlimited")
    } else {
        SharedString::from((rounded as u64).to_string())
    }
}

fn slider_unlimited_value() -> f32 {
    (SLIDER_MAX_KBPS + 1) as f32
}

fn text_for_limit(value: Option<u64>) -> SharedString {
    match value {
        None => SharedString::from("global"),
        Some(0) => SharedString::from("unlimited"),
        Some(value) => SharedString::from(value.to_string()),
    }
}

fn step_value(current: &str, action: StepAction) -> SharedString {
    let current = current.trim();
    if current.is_empty() || current.eq_ignore_ascii_case("global") || current == "0" {
        return match action {
            StepAction::Increment => SharedString::from("1"),
            StepAction::Decrement => SharedString::from("global"),
        };
    }
    if current.eq_ignore_ascii_case("unlimited") {
        return match action {
            StepAction::Increment => SharedString::from("unlimited"),
            StepAction::Decrement => SharedString::from(SLIDER_MAX_KBPS.to_string()),
        };
    }

    let current = match current.parse::<u64>() {
        Ok(value) => value,
        Err(_) => return SharedString::from("global"),
    };

    let next = match action {
        StepAction::Increment => current.saturating_add(1),
        StepAction::Decrement => current.saturating_sub(1),
    };

    if next == 0 {
        SharedString::from("global")
    } else {
        SharedString::from(next.to_string())
    }
}

fn normalize_value(value: impl Into<SharedString>) -> SharedString {
    let value = value.into();
    let value = value.trim();

    if value.is_empty() || value.eq_ignore_ascii_case("global") || value == "0" {
        return SharedString::from("global");
    }
    if value.eq_ignore_ascii_case("unlimited") {
        return SharedString::from("unlimited");
    }

    match value.parse::<u64>() {
        Ok(0) => SharedString::from("global"),
        Ok(value) => SharedString::from(value.to_string()),
        Err(_) => SharedString::from(value.to_string()),
    }
}
