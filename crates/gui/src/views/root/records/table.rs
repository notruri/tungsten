use std::cmp::Ordering;
use std::sync::Arc;

use gpui::*;
use gpui_component::{
    Side,
    menu::{ContextMenuExt, PopupMenu, PopupMenuItem},
    table::{Column, ColumnSort, Table, TableDelegate, TableState},
};
use tracing::error;
use tungsten_net::model::{DownloadRecord, DownloadStatus};
use tungsten_net::queue::QueueService;

use super::format::{
    file_name_for_display, file_name_for_sort, format_bytes, format_eta, format_percentage,
};
use super::menu::build_task_menu;

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

pub struct QueueTableDelegate {
    queue: Arc<QueueService>,
    columns: Vec<QueueColumnConfig>,
    rows: Vec<DownloadRecord>,
    active_sort: Option<(QueueColumnKey, ColumnSort)>,
    table_state: WeakEntity<TableState<QueueTableDelegate>>,
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

    fn visible_column_index(&self, col_ix: usize) -> Option<usize> {
        self.columns
            .iter()
            .enumerate()
            .filter(|(_, column)| column.visible)
            .nth(col_ix)
            .map(|(ix, _)| ix)
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

    fn move_column(
        &mut self,
        col_ix: usize,
        to_ix: usize,
        _: &mut Window,
        _: &mut Context<TableState<Self>>,
    ) {
        let Some(from) = self.visible_column_index(col_ix) else {
            return;
        };
        let Some(to) = self.visible_column_index(to_ix) else {
            return;
        };

        let column = self.columns.remove(from);
        self.columns.insert(to, column);
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
            QueueColumnKey::Status => div().child(format!("{status:?}").to_lowercase()),
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
