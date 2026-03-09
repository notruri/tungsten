use std::cmp::Ordering;
use std::sync::Arc;

use gpui::*;
use gpui_component::{
    button::*,
    table::{Column, ColumnSort, Table, TableDelegate, TableState},
};
use tungsten_net::{DownloadRecord, DownloadStatus, QueueService};

const COL_ID: usize = 0;
const COL_FILE: usize = 1;
const COL_STATUS: usize = 2;
const COL_PROGRESS: usize = 3;
const COL_PAUSE: usize = 4;
const COL_CANCEL: usize = 5;
const COL_DELETE: usize = 6;

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
                Column::new("id", "id")
                    .width(px(64.))
                    .sortable(),
                Column::new("file", "file")
                    .width(px(240.))
                    .sortable(),
                Column::new("status", "status")
                    .width(px(110.))
                    .sortable(),
                Column::new("progress", "progress")
                    .width(px(140.))
                    .sortable(),
                Column::new("pause", "").width(px(90.)).resizable(false),
                Column::new("cancel", "").width(px(90.)).resizable(false),
                Column::new("delete", "").width(px(90.)).resizable(false),
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
        let download_id = record.id;
        let status = record.status.clone();
        let should_resume = matches!(
            status,
            DownloadStatus::Paused | DownloadStatus::Failed | DownloadStatus::Cancelled
        );
        let pause_label = if should_resume { "resume" } else { "pause" };

        match col_ix {
            COL_ID => div().child(download_id.to_string()).into_any_element(),
            COL_FILE => div()
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
            COL_PROGRESS => div()
                .child(format!(
                    "{} / {}",
                    record.progress.downloaded,
                    record.progress.total.unwrap_or_default()
                ))
                .into_any_element(),
            COL_PAUSE => {
                let queue_for_pause_resume = Arc::clone(&self.queue);
                div()
                    .child(
                        Button::new(("pause-resume", download_id.0))
                            .primary()
                            .label(pause_label)
                            .on_click(move |_, _, _| {
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
                            }),
                    )
                    .into_any_element()
            }
            COL_CANCEL => {
                let queue_for_cancel = Arc::clone(&self.queue);
                div()
                    .child(
                        Button::new(("cancel", download_id.0))
                            .primary()
                            .label("cancel")
                            .on_click(move |_, _, _| {
                                if let Err(error) = queue_for_cancel.cancel(download_id) {
                                    eprintln!("failed to cancel {}: {error}", download_id);
                                }
                            }),
                    )
                    .into_any_element()
            }
            COL_DELETE => {
                let queue_for_delete = Arc::clone(&self.queue);
                div()
                    .child(Button::new(("delete", download_id.0)).label("delete").on_click(
                        move |_, _, _| {
                            if let Err(error) = queue_for_delete.delete(download_id) {
                                eprintln!("failed to delete {}: {error}", download_id);
                            }
                        },
                    ))
                    .into_any_element()
            }
            _ => div().into_any_element(),
        }
    }
}

fn compare_rows(left: &DownloadRecord, right: &DownloadRecord, col_ix: usize) -> Ordering {
    match col_ix {
        COL_ID => left.id.0.cmp(&right.id.0),
        COL_FILE => file_name_for_sort(left).cmp(&file_name_for_sort(right)),
        COL_STATUS => status_rank(&left.status)
            .cmp(&status_rank(&right.status))
            .then_with(|| left.id.0.cmp(&right.id.0)),
        COL_PROGRESS => left
            .progress
            .downloaded
            .cmp(&right.progress.downloaded)
            .then_with(|| left.id.0.cmp(&right.id.0)),
        _ => left.id.0.cmp(&right.id.0),
    }
}

fn file_name_for_sort(record: &DownloadRecord) -> String {
    record
        .request
        .destination
        .file_name()
        .map(|name| name.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_default()
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
