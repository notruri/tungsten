use std::cmp::Ordering;
use std::ffi::OsString;
use std::io::{Error as IoError, ErrorKind};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use gpui::*;
use gpui_component::{
    Side,
    menu::{ContextMenuExt, PopupMenu, PopupMenuItem},
    table::{Column, ColumnSort, Table, TableDelegate, TableState},
};
use tracing::{debug, error};
use tungsten_net::model::{DownloadId, DownloadRecord, DownloadStatus};
use tungsten_net::queue::QueueService;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueueColumnKey {
    Name,
    Size,
    Total,
    Percentage,
    Status,
    Speed,
    Eta,
}

#[derive(Clone)]
struct QueueColumnConfig {
    key: QueueColumnKey,
    column: Column,
    visible: bool,
}

pub fn new_state<V>(
    queue: Arc<QueueService>,
    window: &mut Window,
    cx: &mut Context<V>,
) -> Entity<TableState<QueueTableDelegate>>
where
    V: 'static,
{
    let state = cx.new(|cx| {
        TableState::new(QueueTableDelegate::new(queue), window, cx).loop_selection(false)
    });
    let weak_state = state.downgrade();

    state.update(cx, move |table, _| {
        table.delegate_mut().bind_table_state(weak_state);
    });

    state
}

pub fn sync<V>(
    state: &Entity<TableState<QueueTableDelegate>>,
    queue: &Arc<QueueService>,
    cx: &mut Context<V>,
) where
    V: 'static,
{
    let rows = queue.snapshot().unwrap_or_else(|_| Vec::new());
    state.update(cx, |table, cx| {
        table.delegate_mut().set_rows(rows);
        cx.notify();
    });
}

pub fn section(state: &Entity<TableState<QueueTableDelegate>>) -> Div {
    div()
        .flex_1()
        .min_h_0()
        .child(Table::new(state).stripe(true).bordered(true))
}

pub struct QueueTableDelegate {
    queue: Arc<QueueService>,
    columns: Vec<QueueColumnConfig>,
    rows: Vec<DownloadRecord>,
    active_sort: Option<(QueueColumnKey, ColumnSort)>,
    table_state: WeakEntity<TableState<QueueTableDelegate>>,
}

impl QueueTableDelegate {
    fn new(queue: Arc<QueueService>) -> Self {
        Self {
            queue,
            columns: vec![
                QueueColumnConfig {
                    key: QueueColumnKey::Name,
                    column: Column::new("name", "name").width(px(240.)).sortable(),
                    visible: true,
                },
                QueueColumnConfig {
                    key: QueueColumnKey::Size,
                    column: Column::new("size", "size").width(px(120.)).sortable(),
                    visible: true,
                },
                QueueColumnConfig {
                    key: QueueColumnKey::Total,
                    column: Column::new("total", "total").width(px(120.)).sortable(),
                    visible: true,
                },
                QueueColumnConfig {
                    key: QueueColumnKey::Percentage,
                    column: Column::new("percentage", "progress")
                        .width(px(110.))
                        .sortable(),
                    visible: true,
                },
                QueueColumnConfig {
                    key: QueueColumnKey::Status,
                    column: Column::new("status", "status").width(px(110.)).sortable(),
                    visible: true,
                },
                QueueColumnConfig {
                    key: QueueColumnKey::Speed,
                    column: Column::new("speed", "speed").width(px(120.)).sortable(),
                    visible: true,
                },
                QueueColumnConfig {
                    key: QueueColumnKey::Eta,
                    column: Column::new("eta", "eta").width(px(96.)).sortable(),
                    visible: true,
                },
            ],
            rows: Vec::new(),
            active_sort: None,
            table_state: WeakEntity::new_invalid(),
        }
    }

    fn bind_table_state(&mut self, table_state: WeakEntity<TableState<QueueTableDelegate>>) {
        self.table_state = table_state;
    }

    fn set_rows(&mut self, rows: Vec<DownloadRecord>) {
        self.rows = rows;
        self.sort_rows();
    }

    fn sort_rows(&mut self) {
        let Some((column_key, sort)) = self.active_sort else {
            return;
        };

        self.rows.sort_by(|left, right| {
            let ordering = compare_rows(left, right, column_key);
            match sort {
                ColumnSort::Ascending => ordering,
                ColumnSort::Descending => ordering.reverse(),
                ColumnSort::Default => Ordering::Equal,
            }
        });
    }

    fn visible_column_count(&self) -> usize {
        self.columns.iter().filter(|column| column.visible).count()
    }

    fn visible_column(&self, col_ix: usize) -> Option<&QueueColumnConfig> {
        self.columns
            .iter()
            .filter(|column| column.visible)
            .nth(col_ix)
    }

    fn visible_column_key(&self, col_ix: usize) -> Option<QueueColumnKey> {
        self.visible_column(col_ix).map(|column| column.key)
    }

    fn column_menu_entries(&self) -> Vec<(QueueColumnKey, SharedString, bool, bool)> {
        let visible_count = self.visible_column_count();
        self.columns
            .iter()
            .map(|column| {
                (
                    column.key,
                    column.column.name.clone(),
                    column.visible,
                    column.visible && visible_count <= 1,
                )
            })
            .collect()
    }

    fn toggle_column(&mut self, key: QueueColumnKey) -> bool {
        let visible_count = self.visible_column_count();
        let Some(column) = self.columns.iter_mut().find(|column| column.key == key) else {
            return false;
        };

        if column.visible && visible_count <= 1 {
            return false;
        }

        column.visible = !column.visible;

        if let Some((active_key, _)) = self.active_sort {
            if active_key == key && !column.visible {
                self.active_sort = None;
            }
        }

        true
    }
}

impl TableDelegate for QueueTableDelegate {
    fn columns_count(&self, _: &App) -> usize {
        self.visible_column_count()
    }

    fn rows_count(&self, _: &App) -> usize {
        self.rows.len()
    }

    fn column(&self, col_ix: usize, _: &App) -> &Column {
        let fallback = self
            .columns
            .iter()
            .find(|column| column.visible)
            .or_else(|| self.columns.first())
            .expect("queue table must have at least one column");

        &self.visible_column(col_ix).unwrap_or(fallback).column
    }

    fn render_th(
        &mut self,
        col_ix: usize,
        _window: &mut Window,
        _: &mut Context<TableState<Self>>,
    ) -> impl IntoElement {
        let Some(column) = self.visible_column(col_ix) else {
            return div().size_full().into_any_element();
        };

        let name = column.column.name.clone();
        let table_state = self.table_state.clone();
        let menu_entries = self.column_menu_entries();

        div()
            .size_full()
            .child(name)
            .context_menu(move |menu: PopupMenu, _, _| {
                let mut menu = menu.check_side(Side::Left).label("columns").separator();
                for (column_key, column_name, checked, disabled) in menu_entries.iter().cloned() {
                    let table_state = table_state.clone();
                    menu = menu.item(
                        PopupMenuItem::new(column_name)
                            .checked(checked)
                            .disabled(disabled)
                            .on_click(move |_, window, cx| {
                                if let Err(error) = table_state.update(cx, |table, cx| {
                                    if table.delegate_mut().toggle_column(column_key) {
                                        table.refresh(cx);
                                        cx.notify();
                                    }
                                }) {
                                    error!(error = %error, "failed to toggle column visibility");
                                }
                                window.refresh();
                            }),
                    );
                }
                menu
            })
            .into_any_element()
    }

    fn perform_sort(
        &mut self,
        col_ix: usize,
        sort: ColumnSort,
        _: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) {
        let Some(column_key) = self.visible_column_key(col_ix) else {
            return;
        };

        self.active_sort = if matches!(sort, ColumnSort::Default) {
            None
        } else {
            Some((column_key, sort))
        };
        self.sort_rows();
        cx.notify();
    }

    fn render_td(
        &mut self,
        row_ix: usize,
        col_ix: usize,
        _window: &mut Window,
        _: &mut Context<TableState<Self>>,
    ) -> impl IntoElement {
        let Some(record) = self.rows.get(row_ix) else {
            return div().into_any_element();
        };
        let Some(column_key) = self.visible_column_key(col_ix) else {
            return div().into_any_element();
        };
        let queue = Arc::clone(&self.queue);
        let download_id = record.id;
        let status = record.status.clone();
        let file_name = file_name_for_display(record);
        let destination = record.destination.clone();
        let menu_anchor_id = ElementId::from(("cell-menu", download_id.0));

        let cell = match column_key {
            QueueColumnKey::Name => div().child(
                record
                    .destination
                    .as_ref()
                    .and_then(|path| path.file_name())
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or_else(|| "resolving".to_string()),
            ),
            QueueColumnKey::Size => div().child(format_bytes(record.progress.downloaded)),
            QueueColumnKey::Total => div().child(
                record
                    .progress
                    .total
                    .map(format_bytes)
                    .unwrap_or_else(|| "-".to_string()),
            ),
            QueueColumnKey::Percentage => div().child(format_percentage(
                record.progress.downloaded,
                record.progress.total,
            )),
            QueueColumnKey::Status => div().child(format!("{status:?}")),
            QueueColumnKey::Speed => div().child(
                record
                    .progress
                    .speed_bps
                    .map(|speed| format!("{}/s", format_bytes(speed)))
                    .unwrap_or_else(|| "-".to_string()),
            ),
            QueueColumnKey::Eta => div().child(
                record
                    .progress
                    .eta_seconds
                    .map(format_eta)
                    .unwrap_or_else(|| "-".to_string()),
            ),
        };

        cell.id((menu_anchor_id, format!("col-{col_ix}")))
            .context_menu(move |menu: PopupMenu, _, _| {
                build_task_menu(
                    menu,
                    Arc::clone(&queue),
                    download_id,
                    status.clone(),
                    file_name.clone(),
                    destination.clone(),
                )
            })
            .into_any_element()
    }

    fn context_menu(
        &mut self,
        _row_ix: usize,
        menu: PopupMenu,
        _window: &mut Window,
        _: &mut Context<TableState<Self>>,
    ) -> PopupMenu {
        // Disable table-level row context menu: we attach row menus to cell elements to avoid
        // stale row context being reused when right-clicking in other parts of the table.
        menu
    }
}

fn build_task_menu(
    menu: PopupMenu,
    queue: Arc<QueueService>,
    download_id: DownloadId,
    status: DownloadStatus,
    file_name: String,
    destination: Option<PathBuf>,
) -> PopupMenu {
    let should_resume = matches!(
        status,
        DownloadStatus::Paused | DownloadStatus::Failed | DownloadStatus::Cancelled
    );
    let pause_label = if should_resume { "resume" } else { "pause" };
    let can_pause_resume = matches!(
        status,
        DownloadStatus::Queued
            | DownloadStatus::Running
            | DownloadStatus::Paused
            | DownloadStatus::Failed
            | DownloadStatus::Cancelled
    );
    let can_cancel = matches!(
        status,
        DownloadStatus::Queued
            | DownloadStatus::Running
            | DownloadStatus::Paused
            | DownloadStatus::Failed
    );
    let can_delete = !matches!(status, DownloadStatus::Running | DownloadStatus::Verifying);
    let can_open_explorer = destination.is_some();

    let queue_for_pause_resume = Arc::clone(&queue);
    let queue_for_cancel = Arc::clone(&queue);
    let queue_for_delete = Arc::clone(&queue);
    let destination_for_open = destination.clone();

    menu.label(truncate_text(&file_name, 28))
        .separator()
        .item(
            PopupMenuItem::new(pause_label)
                .disabled(!can_pause_resume)
                .on_click(move |_, _, _| {
                    let result = if should_resume {
                        queue_for_pause_resume.resume(download_id)
                    } else {
                        queue_for_pause_resume.pause(download_id)
                    };

                    if let Err(error) = result {
                        error!(
                            download_id = %download_id,
                            error = %error,
                            "failed to run pause/resume action"
                        );
                    }
                }),
        )
        .item(
            PopupMenuItem::new("cancel")
                .disabled(!can_cancel)
                .on_click(move |_, _, _| {
                    if let Err(error) = queue_for_cancel.cancel(download_id) {
                        error!(download_id = %download_id, error = %error, "failed to cancel");
                    }
                }),
        )
        .item(
            PopupMenuItem::new("delete")
                .disabled(!can_delete)
                .on_click(move |_, _, _| {
                    if let Err(error) = queue_for_delete.delete(download_id) {
                        error!(download_id = %download_id, error = %error, "failed to delete");
                    }
                }),
        )
        .item(
            PopupMenuItem::new("open in file explorer")
                .disabled(!can_open_explorer)
                .on_click(move |_, _, _| {
                    let Some(destination_path) = destination_for_open.as_ref() else {
                        return;
                    };

                    debug!(
                        download_id = %download_id,
                        destination = %destination_path.display(),
                        exists = destination_path.exists(),
                        "opening destination in file explorer"
                    );

                    if let Err(error) = open_in_file_explorer(destination_path) {
                        error!(
                            download_id = %download_id,
                            destination = %destination_path.display(),
                            error = %error,
                            "failed to open destination in file explorer"
                        );
                    }
                }),
        )
}

fn open_in_file_explorer(path: &Path) -> Result<(), IoError> {
    #[cfg(target_os = "windows")]
    {
        let parent = path.parent().map(|value| value.display().to_string());
        debug!(
            destination = %path.display(),
            exists = path.exists(),
            parent = ?parent,
            "resolved destination path for file explorer"
        );

        if path.exists() {
            debug!("using explorer select mode with split args");
            let split_args = vec![OsString::from("/select,"), path.as_os_str().to_os_string()];
            return run_explorer(split_args);
        }

        if let Some(parent) = path.parent() {
            let parent_path = parent.as_os_str().to_os_string();
            debug!(
                parent = %parent.display(),
                "destination missing; opening parent directory"
            );
            return run_explorer(vec![parent_path]);
        }

        let raw_path = path.as_os_str().to_os_string();
        debug!("destination has no parent; passing destination path directly");
        run_explorer(vec![raw_path])
    }

    #[cfg(not(target_os = "windows"))]
    {
        let _ = path;
        Err(IoError::new(
            ErrorKind::Unsupported,
            "open in file explorer is only supported on Windows",
        ))
    }
}

#[cfg(target_os = "windows")]
fn run_explorer(args: Vec<OsString>) -> Result<(), IoError> {
    let debug_args = args
        .iter()
        .map(|value| value.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    debug!(args = ?debug_args, "launching explorer");

    let status = Command::new("explorer.exe").args(&args).status()?;
    debug!(status = ?status, success = status.success(), "explorer process exited");

    if status.success() {
        Ok(())
    } else {
        Err(IoError::new(
            ErrorKind::Other,
            format!("explorer exited with status {status}"),
        ))
    }
}

fn compare_rows(
    left: &DownloadRecord,
    right: &DownloadRecord,
    column_key: QueueColumnKey,
) -> Ordering {
    match column_key {
        QueueColumnKey::Name => file_name_for_sort(left).cmp(&file_name_for_sort(right)),
        QueueColumnKey::Size => left
            .progress
            .downloaded
            .cmp(&right.progress.downloaded)
            .then_with(|| left.id.0.cmp(&right.id.0)),
        QueueColumnKey::Total => left
            .progress
            .total
            .unwrap_or_default()
            .cmp(&right.progress.total.unwrap_or_default())
            .then_with(|| left.id.0.cmp(&right.id.0)),
        QueueColumnKey::Percentage => percentage_for_sort(left)
            .cmp(&percentage_for_sort(right))
            .then_with(|| left.id.0.cmp(&right.id.0)),
        QueueColumnKey::Status => status_rank(&left.status)
            .cmp(&status_rank(&right.status))
            .then_with(|| left.id.0.cmp(&right.id.0)),
        QueueColumnKey::Speed => left
            .progress
            .speed_bps
            .unwrap_or_default()
            .cmp(&right.progress.speed_bps.unwrap_or_default())
            .then_with(|| left.id.0.cmp(&right.id.0)),
        QueueColumnKey::Eta => left
            .progress
            .eta_seconds
            .unwrap_or(u64::MAX)
            .cmp(&right.progress.eta_seconds.unwrap_or(u64::MAX))
            .then_with(|| left.id.0.cmp(&right.id.0)),
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 7] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];

    if bytes < 1024 {
        return format!("{bytes} {}", UNITS[0]);
    }

    let mut value = bytes as f64;
    let mut unit_ix = 0usize;
    while value >= 1024.0 && unit_ix < UNITS.len() - 1 {
        value /= 1024.0;
        unit_ix += 1;
    }

    let mut text = format!("{value:.2}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }

    format!("{text} {}", UNITS[unit_ix])
}

fn file_name_for_sort(record: &DownloadRecord) -> String {
    record
        .destination
        .as_ref()
        .and_then(|path| path.file_name())
        .map(|name| name.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_default()
}

fn file_name_for_display(record: &DownloadRecord) -> String {
    record
        .destination
        .as_ref()
        .and_then(|path| path.file_name())
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "resolving".to_string())
}

fn truncate_text(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }

    let keep = max_chars.saturating_sub(1);
    let mut result = String::new();
    for ch in text.chars().take(keep) {
        result.push(ch);
    }
    result.push('…');
    result
}

fn format_eta(seconds: u64) -> String {
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let secs = seconds % 60;

    if hours > 0 {
        return format!("{hours}h {minutes:02}m");
    }

    if minutes > 0 {
        return format!("{minutes}m {secs:02}s");
    }

    format!("{secs}s")
}

fn format_percentage(downloaded: u64, total: Option<u64>) -> String {
    let Some(total) = total else {
        return "-".to_string();
    };
    if total == 0 {
        return "-".to_string();
    }

    let percentage = ((downloaded as f64 / total as f64) * 100.0).clamp(0.0, 100.0);
    let mut text = format!("{percentage:.2}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    format!("{text}%")
}

fn percentage_for_sort(record: &DownloadRecord) -> u64 {
    let Some(total) = record.progress.total else {
        return 0;
    };
    if total == 0 {
        return 0;
    }

    // Keep integer math for stable sorting and avoid float edge cases.
    record
        .progress
        .downloaded
        .saturating_mul(10_000)
        .saturating_div(total)
}

fn status_rank(status: &DownloadStatus) -> u8 {
    match status {
        DownloadStatus::Queued => 0,
        DownloadStatus::Running => 1,
        DownloadStatus::Paused => 2,
        DownloadStatus::Verifying => 3,
        DownloadStatus::Completed => 4,
        DownloadStatus::Failed => 5,
        DownloadStatus::Cancelled => 6,
    }
}
