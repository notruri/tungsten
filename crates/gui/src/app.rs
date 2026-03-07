use std::path::PathBuf;
use std::sync::Arc;

use gpui::*;
use gpui_component::{button::*, input::*, *};
use tungsten_net::{ConflictPolicy, DownloadRequest, IntegrityRule, QueueService};

pub struct View {
    queue: Arc<QueueService>,
    input_state: Entity<InputState>,
}

impl View {
    pub fn new(window: &mut Window, cx: &mut Context<Self>, queue: Arc<QueueService>) -> Self {
        let input_state = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("url here")
                .default_value("")
        });
        Self { queue, input_state }
    }

    fn create_interface(&self, _: &mut Window, _: &mut Context<Self>) -> Div {
        let queue = Arc::clone(&self.queue);
        let input_state_for_add = self.input_state.clone();

        let queue_text = match self.queue.snapshot() {
            Ok(records) => format_records(&records),
            Err(error) => format!("failed to read queue: {error}"),
        };

        div()
            .v_flex()
            .gap_2()
            .size_full()
            .p_4()
            .child("Tungsten")
            .child(Input::new(&self.input_state))
            .child(
                div().h_flex().gap_2().child(
                    Button::new("add-queue")
                        .primary()
                        .label("add to queue")
                        .on_click(move |_, _, cx| {
                            let url = input_state_for_add.read(cx).value().to_string();
                            if url.trim().is_empty() {
                                eprintln!("url is required");
                                return;
                            }

                            let dest = std::env::current_dir()
                                .unwrap_or_else(|_| PathBuf::from("."))
                                .join("storage/downloads/download.bin");

                            println!("adding to queue: {url} ({dest:?})");

                            let request = DownloadRequest::new(
                                url,
                                dest,
                                ConflictPolicy::AutoRename,
                                IntegrityRule::None,
                            );
                            if let Err(error) = queue.enqueue(request) {
                                eprintln!("failed to enqueue request: {error}");
                            }
                        }),
                ),
            )
            .child(queue_text)
    }
}

impl Render for View {
    fn render(&mut self, w: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.create_interface(w, cx)
    }
}

fn format_records(records: &[tungsten_net::DownloadRecord]) -> String {
    if records.is_empty() {
        return "queue is empty".to_string();
    }

    let mut lines = Vec::with_capacity(records.len() + 1);
    lines.push("id | status | downloaded/total | file".to_string());

    for record in records {
        let total_text = record
            .progress
            .total
            .map(|value| value.to_string())
            .unwrap_or_else(|| "?".to_string());

        let status_text = format!("{:?}", record.status);
        lines.push(format!(
            "{} | {} | {}/{} | {}",
            record.id,
            status_text,
            record.progress.downloaded,
            total_text,
            record
                .request
                .destination
                .file_name()
                .map(|value| value.to_string_lossy().into_owned())
                .unwrap_or_else(|| "unknown".to_string())
        ));

        if let Some(error) = &record.error {
            lines.push(format!("  error: {error}"));
        }
    }

    lines.join("\n")
}
