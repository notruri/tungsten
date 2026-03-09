use std::cmp::Ordering;
use std::sync::Arc;

use gpui::*;
use gpui_component::{
    menu::{PopupMenu, PopupMenuItem},
    table::{Column, ColumnSort, Table, TableDelegate, TableState},
};
use tungsten_net::{DownloadRecord, DownloadStatus, QueueService};

const COL_NAME: usize = 0;
const COL_SIZE: usize = 1;
const COL_TOTAL: usize = 2;
const COL_PERCENTAGE: usize = 3;
const COL_STATUS: usize = 4;
const COL_SPEED: usize = 5;
const COL_ETA: usize = 6;

pub fn new_state<V>(
    queue: Arc<QueueService>,
    window: &mut Window,
    cx: &mut Context<V>,
) -> Entity<TableState<QueueTableDelegate>>
where
    V: 'static,
{
    cx.new(|cx| {
        TableState::new(QueueTableDelegate::new(queue), window, cx)
            .loop_selection(false)
    })
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
    columns: Vec<Column>,
    rows: Vec<DownloadRecord>,
    active_sort: Option<(usize, ColumnSort)>,
}

impl QueueTableDelegate {
    fn new(queue: Arc<QueueService>) -> Self {
        Self {
            queue,
            columns: vec![
                Column::new("name", "name")
                    .width(px(240.))
                    .sortable(),
                Column::new("size", "size")
                    .width(px(120.))
                    .sortable(),
                Column::new("total", "total")
                    .width(px(120.))
                    .sortable(),
                Column::new("percentage", "progress")
                    .width(px(110.))
                    .sortable(),
                Column::new("status", "status")
                    .width(px(110.))
                    .sortable(),
                Column::new("speed", "speed")
                    .width(px(120.))
                    .sortable(),
                Column::new("eta", "eta")
                    .width(px(96.))
                    .sortable(),
            ],
            rows: Vec::new(),
            active_sort: None,
        }
    }

    fn set_rows(&mut self, rows: Vec<DownloadRecord>) {
        self.rows = rows;
        self.sort_rows();
    }

    fn sort_rows(&mut self) {
        let Some((col_ix, sort)) = self.active_sort else {
            return;
        };

        self.rows.sort_by(|left, right| {
            let ordering = compare_rows(left, right, col_ix);
            match sort {
                ColumnSort::Ascending => ordering,
                ColumnSort::Descending => ordering.reverse(),
                ColumnSort::Default => Ordering::Equal,
            }
        });
    }
}

impl TableDelegate for QueueTableDelegate {
    fn columns_count(&self, _: &App) -> usize {
        self.columns.len()
    }

    fn rows_count(&self, _: &App) -> usize {
        self.rows.len()
    }

    fn column(&self, col_ix: usize, _: &App) -> &Column {
        &self.columns[col_ix]
    }

    fn perform_sort(
        &mut self,
        col_ix: usize,
        sort: ColumnSort,
        _: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) {
        self.active_sort = if matches!(sort, ColumnSort::Default) {
            None
        } else {
            Some((col_ix, sort))
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
        let status = record.status.clone();

        match col_ix {
            COL_NAME => div()
                .child(
                    record
                        .request
                        .destination
                        .file_name()
                        .map(|name| name.to_string_lossy().into_owned())
                        .unwrap_or_else(String::new),
                )
                .into_any_element(),
            COL_STATUS => div().child(format!("{status:?}")).into_any_element(),
            COL_SIZE => div()
                .child(format_bytes(record.progress.downloaded))
                .into_any_element(),
            COL_TOTAL => div()
                .child(
                    record
                        .progress
                        .total
                        .map(format_bytes)
                        .unwrap_or_else(|| "-".to_string()),
                )
                .into_any_element(),
            COL_SPEED => div()
                .child(
                    record
                        .progress
                        .speed_bps
                        .map(|speed| format!("{}/s", format_bytes(speed)))
                        .unwrap_or_else(|| "-".to_string()),
                )
                .into_any_element(),
            COL_ETA => div()
                .child(
                    record
                        .progress
                        .eta_seconds
                        .map(format_eta)
                        .unwrap_or_else(|| "-".to_string()),
                )
                .into_any_element(),
            COL_PERCENTAGE => div()
                .child(format_percentage(record.progress.downloaded, record.progress.total))
                .into_any_element(),
            _ => div().into_any_element(),
        }
    }

    fn context_menu(
        &mut self,
        row_ix: usize,
        menu: PopupMenu,
        _window: &mut Window,
        _: &mut Context<TableState<Self>>,
    ) -> PopupMenu {
        let Some(record) = self.rows.get(row_ix) else {
            return menu;
        };

        let download_id = record.id;
        let status = record.status.clone();
        let should_resume = matches!(
            status,
            DownloadStatus::Paused | DownloadStatus::Failed | DownloadStatus::Cancelled
        );
        let pause_label = if should_resume { "resume" } else { "pause" };

        let queue_for_pause_resume = Arc::clone(&self.queue);
        let queue_for_cancel = Arc::clone(&self.queue);
        let queue_for_delete = Arc::clone(&self.queue);

        menu
            .label(format!("task {}", download_id))
            .separator()
            .item(PopupMenuItem::new(pause_label).on_click(move |_, _, _| {
                let result = if should_resume {
                    queue_for_pause_resume.resume(download_id)
                } else {
                    queue_for_pause_resume.pause(download_id)
                };

                if let Err(error) = result {
                    eprintln!(
                        "failed to run pause/resume action for {}: {error}",
                        download_id
                    );
                }
            }))
            .item(PopupMenuItem::new("cancel").on_click(move |_, _, _| {
                if let Err(error) = queue_for_cancel.cancel(download_id) {
                    eprintln!("failed to cancel {}: {error}", download_id);
                }
            }))
            .item(PopupMenuItem::new("delete").on_click(move |_, _, _| {
                if let Err(error) = queue_for_delete.delete(download_id) {
                    eprintln!("failed to delete {}: {error}", download_id);
                }
            }))
    }
}

fn compare_rows(left: &DownloadRecord, right: &DownloadRecord, col_ix: usize) -> Ordering {
    match col_ix {
        COL_NAME => file_name_for_sort(left).cmp(&file_name_for_sort(right)),
        COL_STATUS => status_rank(&left.status)
            .cmp(&status_rank(&right.status))
            .then_with(|| left.id.0.cmp(&right.id.0)),
        COL_SIZE => left
            .progress
            .downloaded
            .cmp(&right.progress.downloaded)
            .then_with(|| left.id.0.cmp(&right.id.0)),
        COL_TOTAL => left
            .progress
            .total
            .unwrap_or_default()
            .cmp(&right.progress.total.unwrap_or_default())
            .then_with(|| left.id.0.cmp(&right.id.0)),
        COL_SPEED => left
            .progress
            .speed_bps
            .unwrap_or_default()
            .cmp(&right.progress.speed_bps.unwrap_or_default())
            .then_with(|| left.id.0.cmp(&right.id.0)),
        COL_ETA => left
            .progress
            .eta_seconds
            .unwrap_or(u64::MAX)
            .cmp(&right.progress.eta_seconds.unwrap_or(u64::MAX))
            .then_with(|| left.id.0.cmp(&right.id.0)),
        COL_PERCENTAGE => percentage_for_sort(left)
            .cmp(&percentage_for_sort(right))
            .then_with(|| left.id.0.cmp(&right.id.0)),
        _ => left.id.0.cmp(&right.id.0),
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
        .request
        .destination
        .file_name()
        .map(|name| name.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_default()
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
