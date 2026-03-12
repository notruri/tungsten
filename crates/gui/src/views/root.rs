use std::sync::{Arc, Mutex};

use crate::components::titlebar;
use crate::settings::SettingsStore;
use gpui::*;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::tab::{Tab, TabBar};
use gpui_component::table::TableState;
use gpui_component::*;
use tracing::error;
use tungsten_net::QueueService;

mod records;
mod settings;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum AppScreen {
    #[default]
    Queue,
    Settings,
}

pub struct View {
    queue: Arc<QueueService>,
    settings: Arc<SettingsStore>,
    records_state: Entity<TableState<records::QueueTableDelegate>>,
    active_screen: AppScreen,
    settings_draft: Option<Entity<settings::Draft>>,
}

impl View {
    pub fn new(
        window: &mut Window,
        cx: &mut Context<Self>,
        queue: Arc<QueueService>,
        settings: Arc<SettingsStore>,
    ) -> Self {
        let records_state = records::new_state(Arc::clone(&queue), window, cx);
        let _ = match queue.subscribe() {
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

                        view.update(cx, |_, cx| cx.notify());
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
            settings,
            records_state,
            active_screen: AppScreen::Queue,
            settings_draft: None,
        }
    }

    fn show_queue(&mut self, _: &ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        self.active_screen = AppScreen::Queue;
        cx.notify();
    }

    fn show_settings(&mut self, _: &ClickEvent, window: &mut Window, cx: &mut Context<Self>) {
        let Some(_) = self.ensure_settings_draft(cx) else {
            return;
        };

        self.active_screen = AppScreen::Settings;
        window.refresh();
        cx.notify();
    }

    fn cancel_settings(&mut self, _: &ClickEvent, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(draft) = self.settings_draft.take() {
            let persisted_theme = draft.read(cx).persisted().theme;
            persisted_theme.apply(Some(window), cx);
        }

        self.active_screen = AppScreen::Queue;
        cx.notify();
    }

    fn save_settings(&mut self, _: &ClickEvent, window: &mut Window, cx: &mut Context<Self>) {
        let Some(draft) = self.settings_draft.clone() else {
            return;
        };

        let next = draft.read(cx).current().clone().normalize();
        if let Err(error) = next.validate() {
            error!(error = %error, "invalid settings data");
            return;
        }

        if let Err(error) = self.settings.save(next.clone()) {
            error!(error = %error, "failed to save config.toml");
            return;
        }

        if let Err(error) = self.queue.set_max_parallel(next.max_parallel) {
            error!(error = %error, "failed to apply max_parallel");
            return;
        }
        if let Err(error) = self.queue.set_connections(next.connections) {
            error!(error = %error, "failed to apply connections");
            return;
        }
        if let Err(error) = self
            .queue
            .set_fallback_filename(next.fallback_filename.clone())
        {
            error!(error = %error, "failed to apply fallback filename");
            return;
        }

        next.theme.apply(Some(window), cx);
        self.settings_draft = None;
        self.active_screen = AppScreen::Queue;
        cx.notify();
    }

    fn ensure_settings_draft(&mut self, cx: &mut Context<Self>) -> Option<Entity<settings::Draft>> {
        if let Some(draft) = &self.settings_draft {
            return Some(draft.clone());
        }

        let current = match self.settings.current() {
            Ok(current) => current,
            Err(error) => {
                error!(error = %error, "failed to read current settings");
                return None;
            }
        };

        let draft = cx.new(|_| settings::Draft::new(current));
        self.settings_draft = Some(draft.clone());
        Some(draft)
    }

    fn app_tabs(&self, cx: &mut Context<Self>) -> impl IntoElement {
        TabBar::new("app-tabs")
            .segmented()
            .child(
                Tab::new()
                    .label("Queue")
                    .selected(self.active_screen == AppScreen::Queue)
                    .on_click(cx.listener(Self::show_queue)),
            )
            .child(
                Tab::new()
                    .label("Settings")
                    .selected(self.active_screen == AppScreen::Settings)
                    .on_click(cx.listener(Self::show_settings)),
            )
    }

    fn tab_row(&self, cx: &mut Context<Self>) -> Div {
        let row = h_flex().items_center().justify_between().gap_2();

        if self.active_screen == AppScreen::Queue {
            row.child(self.app_tabs(cx)).child(titlebar::add_button(
                Arc::clone(&self.queue),
                Arc::clone(&self.settings),
            ))
        } else {
            row.child(self.app_tabs(cx))
        }
    }

    fn active_content(&self, window: &mut Window, cx: &mut Context<Self>) -> AnyElement {
        match self.active_screen {
            AppScreen::Queue => div()
                .v_flex()
                .gap_2()
                .flex_1()
                .min_h_0()
                .child(records::section(&self.records_state))
                .into_any_element(),
            AppScreen::Settings => {
                let Some(draft) = self.settings_draft.clone() else {
                    return div().into_any_element();
                };

                div()
                    .v_flex()
                    .gap_4()
                    .flex_1()
                    .min_h_0()
                    .child(
                        div()
                            .flex_1()
                            .min_h_0()
                            .child(settings::create(&draft, window, cx)),
                    )
                    .child(
                        h_flex()
                            .justify_end()
                            .gap_2()
                            .child(
                                Button::new("cancel-settings")
                                    .label("cancel")
                                    .outline()
                                    .on_click(cx.listener(Self::cancel_settings)),
                            )
                            .child(
                                Button::new("save-settings")
                                    .label("save")
                                    .primary()
                                    .on_click(cx.listener(Self::save_settings)),
                            ),
                    )
                    .into_any_element()
            }
        }
    }

    fn create_interface(&self, window: &mut Window, cx: &mut Context<Self>) -> Div {
        records::sync(&self.records_state, &self.queue, cx);

        div()
            .v_flex()
            .size_full()
            .child(titlebar::create())
            .child(
                div()
                    .v_flex()
                    .gap_4()
                    .p_4()
                    .flex_1()
                    .min_h_0()
                    .child(self.tab_row(cx))
                    .child(self.active_content(window, cx)),
            )
            .children(Root::render_dialog_layer(window, cx))
            .children(Root::render_sheet_layer(window, cx))
            .children(Root::render_notification_layer(window, cx))
    }
}

impl Render for View {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.create_interface(window, cx)
    }
}
