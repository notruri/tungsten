use std::sync::Arc;

use gpui::{App, AppContext, ParentElement, Styled, Window, div, px};
use gpui_component::dialog::DialogButtonProps;
use gpui_component::{input::*, *};
use tracing::{error, warn};
use tungsten_net::model::{ConflictPolicy, DownloadRequest, IntegrityRule};
use tungsten_net::queue::QueueService;

use crate::components::dialog;
use crate::settings::SettingsStore;

#[derive(Debug, Clone)]
struct QueueEntry {
    url: String,
    integrity: IntegrityRule,
}

pub(crate) fn open_dialog(
    queue: Arc<QueueService>,
    settings: Arc<SettingsStore>,
    window: &mut Window,
    cx: &mut App,
) {
    let input_state = cx.new(|cx| {
        InputState::new(window, cx)
            .multi_line(true)
            .rows(8)
            .default_value("")
    });

    let queue_for_add = Arc::clone(&queue);
    let settings_for_add = Arc::clone(&settings);
    let input_state_for_add = input_state.clone();
    let input_state_for_dialog = input_state.clone();
    window.open_dialog(cx, move |dialog, _, _| {
        dialog
            .title("Add to queue")
            .width(px(580.0))
            .button_props(
                DialogButtonProps::default()
                    .show_cancel(true)
                    .ok_text("add to queue")
                    .cancel_text("cancel"),
            )
            .footer(dialog::dialog_footer("add-queue-dialog", "add to queue"))
            .on_ok({
                let queue_for_add = Arc::clone(&queue_for_add);
                let settings_for_add = Arc::clone(&settings_for_add);
                let input_state_for_add = input_state_for_add.clone();
                move |_, _, cx| {
                    let value = input_state_for_add.read(cx).value().to_string();
                    let lines: Vec<&str> = value
                        .lines()
                        .map(str::trim)
                        .filter(|line| !line.is_empty())
                        .collect();

                    if lines.is_empty() {
                        warn!("at least one URL is required");
                        return false;
                    }

                    let mut entries = Vec::with_capacity(lines.len());
                    for (index, line) in lines.iter().enumerate() {
                        match parse_queue_entry(line) {
                            Ok(entry) => entries.push(entry),
                            Err(message) => {
                                warn!(line_number = index + 1, line = %line, "{message}");
                                return false;
                            }
                        }
                    }

                    let destination = match settings_for_add.current() {
                        Ok(current) => current.download_root,
                        Err(error) => {
                            error!(
                                error = %error,
                                "failed to resolve current download root from settings"
                            );
                            return false;
                        }
                    };

                    let mut enqueued = 0usize;
                    for entry in entries {
                        let request = DownloadRequest::new(
                            entry.url.clone(),
                            destination.clone(),
                            ConflictPolicy::AutoRename,
                            entry.integrity,
                        );

                        match queue_for_add.enqueue(request) {
                            Ok(_) => {
                                enqueued += 1;
                            }
                            Err(error) => {
                                error!(
                                    url = %entry.url,
                                    error = %error,
                                    "failed to enqueue request"
                                );
                            }
                        }
                    }

                    if enqueued == 0 {
                        warn!("no entries were added to queue");
                        return false;
                    }

                    true
                }
            })
            .child(
                div()
                    .v_flex()
                    .gap_2()
                    .child("paste entries below, one per line")
                    .child("format: URL or URL sha256:<64-hex>")
                    .child(Input::new(&input_state_for_dialog).h(px(220.0))),
            )
    });

    input_state.update(cx, |input, input_cx| input.focus(window, input_cx));
}

fn parse_queue_entry(line: &str) -> Result<QueueEntry, String> {
    let mut parts = line.split_whitespace();
    let url = parts
        .next()
        .ok_or_else(|| "entry must include a URL".to_string())?;
    let integrity = match parts.next() {
        Some(value) => parse_integrity(value)?,
        None => IntegrityRule::None,
    };

    if parts.next().is_some() {
        return Err("entry must be `URL` or `URL sha256:<64-hex>`".to_string());
    }

    Ok(QueueEntry {
        url: url.to_string(),
        integrity,
    })
}

fn parse_integrity(value: &str) -> Result<IntegrityRule, String> {
    let hash = value
        .strip_prefix("sha256:")
        .or_else(|| value.strip_prefix("sha256="))
        .unwrap_or(value);

    if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("sha256 must be a 64-character hexadecimal value".to_string());
    }

    Ok(IntegrityRule::Sha256(hash.to_ascii_lowercase()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_queue_entry_without_integrity() {
        let entry = parse_queue_entry("https://example.com/file.bin").expect("entry should parse");
        assert_eq!(entry.url, "https://example.com/file.bin");
        assert!(matches!(entry.integrity, IntegrityRule::None));
    }

    #[test]
    fn parse_queue_entry_with_prefixed_integrity() {
        let entry = parse_queue_entry(
            "https://example.com/file.bin sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
        .expect("entry should parse");

        assert_eq!(entry.url, "https://example.com/file.bin");
        match entry.integrity {
            IntegrityRule::Sha256(hash) => assert_eq!(
                hash,
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            ),
            IntegrityRule::None => panic!("entry should include sha256 integrity"),
        }
    }

    #[test]
    fn parse_queue_entry_with_bare_integrity() {
        let entry = parse_queue_entry(
            "https://example.com/file.bin ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789",
        )
        .expect("entry should parse");

        match entry.integrity {
            IntegrityRule::Sha256(hash) => assert_eq!(
                hash,
                "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
            ),
            IntegrityRule::None => panic!("entry should include sha256 integrity"),
        }
    }

    #[test]
    fn reject_invalid_integrity_value() {
        let error = parse_queue_entry("https://example.com/file.bin sha256:nope")
            .expect_err("invalid hash should fail");
        assert!(error.contains("64-character hexadecimal"));
    }

    #[test]
    fn reject_extra_tokens() {
        let error = parse_queue_entry(
            "https://example.com/file.bin sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef extra",
        )
        .expect_err("extra token should fail");
        assert!(error.contains("URL"));
    }
}
