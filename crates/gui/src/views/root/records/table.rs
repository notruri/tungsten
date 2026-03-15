use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

use gpui::*;
use gpui_component::{
    ActiveTheme as _, Side,
    menu::{ContextMenuExt, PopupMenu, PopupMenuItem},
    table::{Column, ColumnSort, DataTable, TableDelegate, TableEvent, TableState},
};
use tracing::error;
use tungsten_runtime::{DownloadId, DownloadRecord, DownloadStatus, QueueService};

use super::format::{
    file_name_for_display, file_name_for_sort, format_bytes, format_eta, format_percentage,
};
use super::menu::{GroupMenuTarget, build_group_task_menu, build_task_menu};

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
    selected_ids: HashSet<DownloadId>,
    anchor_id: Option<DownloadId>,
    focused_id: Option<DownloadId>,
    suspend_row_sync: bool,
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

    window
        .subscribe(
            &state,
            cx,
            move |table_state: Entity<TableState<QueueTableDelegate>>,
                  event: &TableEvent,
                  window,
                  cx| {
                let TableEvent::SelectRow(row_ix) = event else {
                    return;
                };

                table_state.update(cx, |table, cx| {
                    if table.delegate_mut().consume_row_sync_suspension() {
                        return;
                    }

                    let previous = table
                        .delegate()
                        .focused_id
                        .and_then(|focused_id| table.delegate().row_index_by_id(focused_id));

                    table.delegate_mut().update_focused_row(*row_ix);
                    if window.modifiers().shift {
                        table
                            .delegate_mut()
                            .extend_selection_to_row(*row_ix, previous);
                    } else {
                        table.delegate_mut().select_single(*row_ix);
                    }
                    cx.notify();
                });

                window.refresh();
            },
        )
        .detach();

    let state_for_clear_sync = state.clone();
    cx.observe(&state, move |_, _, cx| {
        state_for_clear_sync.update(cx, |table, cx| {
            if table.selected_row().is_none() && !table.delegate().selected_ids.is_empty() {
                table.delegate_mut().clear_multi_selection();
                cx.notify();
            }
        });
    })
    .detach();

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
    let state_for_keys = state.clone();
    div()
        .flex_1()
        .min_h_0()
        .capture_key_down(move |event, window, cx| {
            handle_key_down(&state_for_keys, event, window, cx);
        })
        .child(DataTable::new(state).stripe(true).bordered(true))
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
            selected_ids: HashSet::new(),
            anchor_id: None,
            focused_id: None,
            suspend_row_sync: false,
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
        self.prune_selection();
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

        if let Some((active_key, _)) = self.active_sort
            && active_key == key
            && !column.visible
        {
            self.active_sort = None;
        }

        true
    }

    fn row_id(&self, row_ix: usize) -> Option<DownloadId> {
        self.rows.get(row_ix).map(|record| record.id)
    }

    fn row_index_by_id(&self, id: DownloadId) -> Option<usize> {
        self.rows.iter().position(|record| record.id == id)
    }

    fn row_range_ids(&self, start: usize, end: usize) -> HashSet<DownloadId> {
        let (from, to) = if start <= end {
            (start, end)
        } else {
            (end, start)
        };

        self.rows
            .iter()
            .skip(from)
            .take(to.saturating_sub(from) + 1)
            .map(|record| record.id)
            .collect()
    }

    fn row_targets(&self, ids: &HashSet<DownloadId>) -> Vec<GroupMenuTarget> {
        self.rows
            .iter()
            .filter(|record| ids.contains(&record.id))
            .map(|record| GroupMenuTarget {
                id: record.id,
                status: record.status.clone(),
            })
            .collect()
    }

    fn is_selected_row(&self, row_ix: usize) -> bool {
        self.row_id(row_ix)
            .map(|id| self.selected_ids.contains(&id))
            .unwrap_or(false)
    }

    fn select_single(&mut self, row_ix: usize) {
        let Some(row_id) = self.row_id(row_ix) else {
            return;
        };

        self.selected_ids.clear();
        self.selected_ids.insert(row_id);
        self.anchor_id = Some(row_id);
    }

    fn toggle_row_selection(&mut self, row_ix: usize) {
        let Some(row_id) = self.row_id(row_ix) else {
            return;
        };

        if !self.selected_ids.insert(row_id) {
            self.selected_ids.remove(&row_id);
            if self.selected_ids.is_empty() {
                self.anchor_id = None;
            } else if self.anchor_id == Some(row_id) {
                self.anchor_id = self.selected_ids.iter().next().copied();
            }
            return;
        }
        self.anchor_id = Some(row_id);
    }

    fn move_cursor(&self, current_row: Option<usize>, direction: i8) -> Option<usize> {
        if self.rows.is_empty() {
            return None;
        }

        if current_row.is_none() {
            return Some(0);
        }

        let current = current_row.unwrap_or(0);
        let last_row = self.rows.len().saturating_sub(1);
        let next = if direction < 0 {
            current.saturating_sub(1)
        } else {
            (current + 1).min(last_row)
        };
        Some(next)
    }

    fn select_all(&mut self) {
        self.selected_ids = self.rows.iter().map(|record| record.id).collect();
        if self.anchor_id.is_none() {
            self.anchor_id = self.rows.first().map(|record| record.id);
        }
    }

    fn update_focused_row(&mut self, row_ix: usize) {
        self.focused_id = self.row_id(row_ix);
    }

    fn clear_focused_row(&mut self) {
        self.focused_id = None;
    }

    fn clear_multi_selection(&mut self) {
        self.selected_ids.clear();
        self.anchor_id = None;
        self.focused_id = None;
        self.suspend_row_sync = false;
    }

    fn suspend_row_sync(&mut self) {
        self.suspend_row_sync = true;
    }

    fn consume_row_sync_suspension(&mut self) -> bool {
        let was_suspended = self.suspend_row_sync;
        self.suspend_row_sync = false;
        was_suspended
    }

    fn extend_selection_to_row(&mut self, row_ix: usize, current_row: Option<usize>) {
        let Some(target_id) = self.row_id(row_ix) else {
            return;
        };
        let anchor_ix = self
            .anchor_id
            .and_then(|anchor_id| self.row_index_by_id(anchor_id))
            .or(current_row)
            .unwrap_or(row_ix);

        if self.anchor_id.is_none() {
            self.anchor_id = self.row_id(anchor_ix);
        }
        self.selected_ids = self.row_range_ids(anchor_ix, row_ix);
        self.selected_ids.insert(target_id);
    }

    fn selected_group_targets_for_row(&self, row_id: DownloadId) -> Option<Vec<GroupMenuTarget>> {
        if self.selected_ids.len() <= 1 || !self.selected_ids.contains(&row_id) {
            return None;
        }

        let targets = self.row_targets(&self.selected_ids);
        if targets.len() <= 1 {
            return None;
        }

        Some(targets)
    }

    fn prune_selection(&mut self) {
        let row_ids: HashSet<DownloadId> = self.rows.iter().map(|record| record.id).collect();
        self.selected_ids.retain(|id| row_ids.contains(id));

        if self
            .anchor_id
            .is_some_and(|anchor_id| !row_ids.contains(&anchor_id))
        {
            self.anchor_id = None;
        }
        if self
            .focused_id
            .is_some_and(|focused_id| !row_ids.contains(&focused_id))
        {
            self.focused_id = None;
        }
        if self.selected_ids.len() <= 1 {
            self.suspend_row_sync = false;
        }
    }
}

impl TableDelegate for QueueTableDelegate {
    fn columns_count(&self, _: &App) -> usize {
        self.visible_column_count()
    }

    fn rows_count(&self, _: &App) -> usize {
        self.rows.len()
    }

    fn column(&self, col_ix: usize, _: &App) -> Column {
        let fallback = self
            .columns
            .iter()
            .find(|column| column.visible)
            .or_else(|| self.columns.first())
            .expect("queue table must have at least one column");

        self.visible_column(col_ix)
            .unwrap_or(fallback)
            .column
            .clone()
    }

    fn render_tr(
        &mut self,
        row_ix: usize,
        _window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) -> Stateful<Div> {
        let mut row = div().id(("row", row_ix));
        let row_id = self.row_id(row_ix);
        let is_multi_selected_row = self.selected_ids.len() > 1
            && self.is_selected_row(row_ix)
            && row_id != self.focused_id;
        if is_multi_selected_row {
            row = row.border_color(gpui::transparent_white()).child(
                div()
                    .top(if row_ix == 0 { px(0.) } else { px(-1.) })
                    .left(px(0.))
                    .right(px(0.))
                    .bottom(px(-1.))
                    .absolute()
                    .bg(cx.theme().table_active)
                    .border_1()
                    .border_color(cx.theme().table_active_border),
            );
        }
        row
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
            .text_sm()
            .child(name)
            .context_menu(move |menu: PopupMenu, _, _| {
                let mut menu = menu.check_side(Side::Left).separator();
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
        let table_state = self.table_state.clone();
        let download_id = record.id;
        let status = record.status.clone();
        let file_name = file_name_for_display(record);
        let destination = record.destination.clone();
        let speed_limit_kbps = record.request.speed_limit_kbps;
        let group_targets = self.selected_group_targets_for_row(download_id);
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

        cell.text_sm()
            .id((menu_anchor_id, format!("col-{col_ix}")))
            .on_click(move |event, window, cx| {
                if !event.standard_click() {
                    return;
                }

                let is_secondary = event.modifiers().secondary();
                cx.stop_propagation();

                if let Err(error) = table_state.update(cx, |table, cx| {
                    if is_secondary {
                        table.delegate_mut().toggle_row_selection(row_ix);
                        if table.delegate().selected_ids.is_empty() {
                            table.clear_selection(cx);
                            table.delegate_mut().clear_focused_row();
                        } else {
                            table.delegate_mut().suspend_row_sync();
                            table.set_selected_row(row_ix, cx);
                            table.delegate_mut().update_focused_row(row_ix);
                        }
                    } else {
                        table.delegate_mut().select_single(row_ix);
                        table.delegate_mut().suspend_row_sync();
                        table.set_selected_row(row_ix, cx);
                        table.delegate_mut().update_focused_row(row_ix);
                    }
                    cx.notify();
                }) {
                    error!(error = %error, "failed to update row selection");
                }

                window.refresh();
            })
            .context_menu(move |menu: PopupMenu, _, _| {
                if let Some(group_targets) = group_targets.clone() {
                    build_group_task_menu(menu, Arc::clone(&queue), group_targets)
                } else {
                    build_task_menu(
                        menu,
                        Arc::clone(&queue),
                        download_id,
                        status.clone(),
                        file_name.clone(),
                        destination.clone(),
                        speed_limit_kbps,
                    )
                }
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

fn handle_key_down(
    state: &Entity<TableState<QueueTableDelegate>>,
    event: &KeyDownEvent,
    window: &mut Window,
    cx: &mut App,
) {
    if event.is_held {
        return;
    }

    let key = event.keystroke.key.as_str();
    let modifiers = event.keystroke.modifiers;
    if key.eq_ignore_ascii_case("escape") || key.eq_ignore_ascii_case("esc") {
        state.update(cx, |table, cx| {
            table.clear_selection(cx);
            table.delegate_mut().clear_multi_selection();
            cx.notify();
        });

        cx.stop_propagation();
        window.refresh();
        return;
    }

    let is_select_all = key.eq_ignore_ascii_case("a")
        && modifiers.secondary()
        && !modifiers.alt
        && !modifiers.function;
    if is_select_all {
        state.update(cx, |table, cx| {
            if table.delegate().rows.is_empty() {
                return;
            }

            table.delegate_mut().select_all();
            if table.selected_row().is_none() {
                table.delegate_mut().suspend_row_sync();
                table.set_selected_row(0, cx);
                table.delegate_mut().update_focused_row(0);
            } else if let Some(current_row) = table.selected_row() {
                table.delegate_mut().update_focused_row(current_row);
            }
            cx.notify();
        });

        cx.stop_propagation();
        window.refresh();
        return;
    }

    if key != "up" && key != "down" {
        return;
    }

    let direction = if key == "up" { -1 } else { 1 };
    let shift = modifiers.shift;
    state.update(cx, |table, cx| {
        let current = table.selected_row();
        let Some(target_row) = table.delegate().move_cursor(current, direction) else {
            return;
        };

        table.delegate_mut().suspend_row_sync();
        table.set_selected_row(target_row, cx);
        table.delegate_mut().update_focused_row(target_row);
        if shift {
            table
                .delegate_mut()
                .extend_selection_to_row(target_row, current);
        } else {
            table.delegate_mut().select_single(target_row);
        }
        cx.notify();
    });

    cx.stop_propagation();
    window.refresh();
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
        DownloadStatus::Preparing => 1,
        DownloadStatus::Running => 2,
        DownloadStatus::Finalizing => 3,
        DownloadStatus::Paused => 4,
        DownloadStatus::Verifying => 5,
        DownloadStatus::Completed => 6,
        DownloadStatus::Failed => 7,
        DownloadStatus::Cancelled => 8,
    }
}
