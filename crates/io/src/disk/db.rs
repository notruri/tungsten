use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use tungsten_net::NetError;
use tungsten_net::model::{DownloadId, DownloadRequest, DownloadStatus, ProgressSnapshot};
use tungsten_net::store::{PersistedDownload, PersistedQueue};
use tungsten_net::transfer::{MultipartPart, TempLayout};

use super::codec::{FromDatabase, RequestValue, TempLayoutValue, ToDatabase};

pub(super) fn read_queue(path: &Path) -> Result<PersistedQueue, NetError> {
    let connection = Connection::open(path)
        .map_err(|error| NetError::State(format!("failed to open state db: {error}")))?;
    init_schema(&connection)?;
    read_queue_with_connection(&connection)
}

pub(super) fn write_queue(path: &Path, state: &PersistedQueue) -> Result<(), NetError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut connection = Connection::open(path)
        .map_err(|error| NetError::State(format!("failed to open state db: {error}")))?;
    init_schema(&connection)?;
    let transaction = connection.transaction().map_err(|error| {
        NetError::State(format!("failed to start state db transaction: {error}"))
    })?;

    transaction
        .execute(
            "
            INSERT INTO queue_meta(key, value)
            VALUES('next_id', ?1)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            ",
            [state.next_id.to_db("queue_meta.value")?],
        )
        .map_err(|error| NetError::State(format!("failed to persist next_id: {error}")))?;

    transaction
        .execute("DELETE FROM multipart_parts", [])
        .map_err(|error| NetError::State(format!("failed to clear multipart parts: {error}")))?;
    transaction
        .execute("DELETE FROM downloads", [])
        .map_err(|error| NetError::State(format!("failed to clear downloads: {error}")))?;
    transaction
        .execute("DELETE FROM requests", [])
        .map_err(|error| NetError::State(format!("failed to clear requests: {error}")))?;

    {
        let mut request_statement = transaction
            .prepare(
                "
                INSERT INTO requests(
                    id,
                    url,
                    destination,
                    conflict,
                    integrity_kind,
                    integrity_value,
                    speed_limit_kbps
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                ",
            )
            .map_err(|error| {
                NetError::State(format!(
                    "failed to prepare request insert statement: {error}"
                ))
            })?;

        let mut download_statement = transaction
            .prepare(
                "
                INSERT INTO downloads(
                    id,
                    request,
                    destination,
                    loaded_from_store,
                    temp_path,
                    temp_layout_kind,
                    temp_layout_total_size,
                    supports_resume,
                    status,
                    progress_downloaded,
                    progress_total,
                    progress_speed_bps,
                    progress_eta_seconds,
                    error,
                    etag,
                    last_modified,
                    created_at,
                    updated_at
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17,
                    ?18
                )
                ",
            )
            .map_err(|error| {
                NetError::State(format!(
                    "failed to prepare download insert statement: {error}"
                ))
            })?;

        let mut part_statement = transaction
            .prepare(
                "
                INSERT INTO multipart_parts(
                    download_id,
                    part_index,
                    part_start,
                    part_end,
                    path
                ) VALUES (?1, ?2, ?3, ?4, ?5)
                ",
            )
            .map_err(|error| {
                NetError::State(format!(
                    "failed to prepare multipart part insert statement: {error}"
                ))
            })?;

        for download in &state.downloads {
            let request_id = download.id.0.to_db("requests.id")?;
            let request = download.request.to_db("requests")?;
            let temp_layout = download.temp_layout.to_db("downloads.temp_layout")?;
            let status = download.status.to_db("downloads.status")?;

            request_statement
                .execute(params![
                    request_id,
                    request.url,
                    request.destination,
                    request.conflict,
                    request.integrity_kind,
                    request.integrity_value,
                    request.speed_limit_kbps,
                ])
                .map_err(|error| {
                    NetError::State(format!(
                        "failed to insert request for download {} into state db: {error}",
                        download.id
                    ))
                })?;

            download_statement
                .execute(params![
                    download.id.0.to_db("downloads.id")?,
                    request_id,
                    download
                        .destination
                        .as_ref()
                        .map(|value| value.to_string_lossy().into_owned()),
                    download
                        .loaded_from_store
                        .to_db("downloads.loaded_from_store")?,
                    download.temp_path.to_string_lossy().into_owned(),
                    temp_layout.kind,
                    temp_layout.total_size,
                    download
                        .supports_resume
                        .to_db("downloads.supports_resume")?,
                    status,
                    download
                        .progress
                        .downloaded
                        .to_db("downloads.progress_downloaded")?,
                    download
                        .progress
                        .total
                        .map(|value| value.to_db("downloads.progress_total"))
                        .transpose()?,
                    download
                        .progress
                        .speed_bps
                        .map(|value| value.to_db("downloads.progress_speed_bps"))
                        .transpose()?,
                    download
                        .progress
                        .eta_seconds
                        .map(|value| value.to_db("downloads.progress_eta_seconds"))
                        .transpose()?,
                    download.error.as_deref(),
                    download.etag.as_deref(),
                    download.last_modified.as_deref(),
                    download.created_at.to_db("downloads.created_at")?,
                    download.updated_at.to_db("downloads.updated_at")?,
                ])
                .map_err(|error| {
                    NetError::State(format!(
                        "failed to insert download {} into state db: {error}",
                        download.id
                    ))
                })?;

            if let TempLayout::Multipart(layout) = &download.temp_layout {
                for part in &layout.parts {
                    part_statement
                        .execute(params![
                            download.id.0.to_db("multipart_parts.download_id")?,
                            part.index.to_db("multipart_parts.part_index")?,
                            part.start.to_db("multipart_parts.part_start")?,
                            part.end.to_db("multipart_parts.part_end")?,
                            part.path.to_string_lossy().into_owned(),
                        ])
                        .map_err(|error| {
                            NetError::State(format!(
                                "failed to insert multipart part for download {}: {error}",
                                download.id
                            ))
                        })?;
                }
            }
        }
    }

    transaction.commit().map_err(|error| {
        NetError::State(format!("failed to commit state db transaction: {error}"))
    })?;
    Ok(())
}

fn init_schema(connection: &Connection) -> Result<(), NetError> {
    connection
        .execute_batch(
            "
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS queue_meta (
                key TEXT PRIMARY KEY NOT NULL,
                value INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS requests (
                id INTEGER PRIMARY KEY NOT NULL,
                url TEXT NOT NULL,
                destination TEXT NOT NULL,
                conflict TEXT NOT NULL,
                integrity_kind TEXT NOT NULL,
                integrity_value TEXT,
                speed_limit_kbps INTEGER
            );
            CREATE TABLE IF NOT EXISTS downloads (
                id INTEGER PRIMARY KEY NOT NULL,
                request INTEGER NOT NULL,
                destination TEXT,
                loaded_from_store INTEGER NOT NULL,
                temp_path TEXT NOT NULL,
                temp_layout_kind TEXT NOT NULL,
                temp_layout_total_size INTEGER,
                supports_resume INTEGER NOT NULL,
                status TEXT NOT NULL,
                progress_downloaded INTEGER NOT NULL,
                progress_total INTEGER,
                progress_speed_bps INTEGER,
                progress_eta_seconds INTEGER,
                error TEXT,
                etag TEXT,
                last_modified TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(request) REFERENCES requests(id)
            );
            CREATE TABLE IF NOT EXISTS multipart_parts (
                download_id INTEGER NOT NULL,
                part_index INTEGER NOT NULL,
                part_start INTEGER NOT NULL,
                part_end INTEGER NOT NULL,
                path TEXT NOT NULL,
                PRIMARY KEY(download_id, part_index),
                FOREIGN KEY(download_id) REFERENCES downloads(id) ON DELETE CASCADE
            );
            ",
        )
        .map_err(|error| {
            NetError::State(format!("failed to initialize state db schema: {error}"))
        })?;
    ensure_request_column(connection, "speed_limit_kbps", "INTEGER")
}

fn read_queue_with_connection(connection: &Connection) -> Result<PersistedQueue, NetError> {
    let next_id = connection
        .query_row(
            "SELECT value FROM queue_meta WHERE key = 'next_id'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(|error| NetError::State(format!("failed to read next_id from state db: {error}")))?
        .map_or(Ok(0_u64), |value| u64::from_db(value, "queue_meta.value"))?;

    let mut statement = connection
        .prepare(
            "
            SELECT
                downloads.id,
                requests.url,
                requests.destination,
                requests.conflict,
                requests.integrity_kind,
                requests.integrity_value,
                requests.speed_limit_kbps,
                downloads.destination,
                downloads.loaded_from_store,
                downloads.temp_path,
                downloads.temp_layout_kind,
                downloads.temp_layout_total_size,
                downloads.supports_resume,
                downloads.status,
                downloads.progress_downloaded,
                downloads.progress_total,
                downloads.progress_speed_bps,
                downloads.progress_eta_seconds,
                downloads.error,
                downloads.etag,
                downloads.last_modified,
                downloads.created_at,
                downloads.updated_at
            FROM downloads
            INNER JOIN requests ON requests.id = downloads.request
            ORDER BY downloads.id ASC
            ",
        )
        .map_err(|error| NetError::State(format!("failed to prepare downloads query: {error}")))?;

    let mut rows = statement
        .query([])
        .map_err(|error| NetError::State(format!("failed to query downloads: {error}")))?;

    let mut downloads = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|error| NetError::State(format!("failed to read download row: {error}")))?
    {
        let id = u64::from_db(
            row.get::<_, i64>(0).map_err(|error| {
                NetError::State(format!("failed to read downloads.id: {error}"))
            })?,
            "downloads.id",
        )?;
        let loaded_from_store = bool::from_db(
            row.get::<_, i64>(8).map_err(|error| {
                NetError::State(format!(
                    "failed to read downloads.loaded_from_store: {error}"
                ))
            })?,
            "downloads.loaded_from_store",
        )?;
        let supports_resume = bool::from_db(
            row.get::<_, i64>(12).map_err(|error| {
                NetError::State(format!("failed to read downloads.supports_resume: {error}"))
            })?,
            "downloads.supports_resume",
        )?;
        let status = DownloadStatus::from_db(
            row.get::<_, String>(13).map_err(|error| {
                NetError::State(format!("failed to read downloads.status: {error}"))
            })?,
            "downloads.status",
        )?;
        let temp_layout = TempLayout::from_db(
            (
                TempLayoutValue {
                    kind: row.get::<_, String>(10).map_err(|error| {
                        NetError::State(format!(
                            "failed to read downloads.temp_layout_kind: {error}"
                        ))
                    })?,
                    total_size: row.get::<_, Option<i64>>(11).map_err(|error| {
                        NetError::State(format!(
                            "failed to read downloads.temp_layout_total_size: {error}"
                        ))
                    })?,
                },
                read_multipart_parts(connection, id)?,
            ),
            "downloads.temp_layout",
        )?;
        let request = DownloadRequest::from_db(
            RequestValue {
                url: row.get::<_, String>(1).map_err(|error| {
                    NetError::State(format!("failed to read requests.url: {error}"))
                })?,
                destination: row.get::<_, String>(2).map_err(|error| {
                    NetError::State(format!("failed to read requests.destination: {error}"))
                })?,
                conflict: row.get::<_, String>(3).map_err(|error| {
                    NetError::State(format!("failed to read requests.conflict: {error}"))
                })?,
                integrity_kind: row.get::<_, String>(4).map_err(|error| {
                    NetError::State(format!("failed to read requests.integrity_kind: {error}"))
                })?,
                integrity_value: row.get::<_, Option<String>>(5).map_err(|error| {
                    NetError::State(format!("failed to read requests.integrity_value: {error}"))
                })?,
                speed_limit_kbps: row.get::<_, Option<i64>>(6).map_err(|error| {
                    NetError::State(format!("failed to read requests.speed_limit_kbps: {error}"))
                })?,
            },
            "requests",
        )?;

        downloads.push(PersistedDownload {
            id: DownloadId(id),
            request,
            destination: row
                .get::<_, Option<String>>(7)
                .map_err(|error| {
                    NetError::State(format!("failed to read downloads.destination: {error}"))
                })?
                .map(PathBuf::from),
            loaded_from_store,
            temp_path: PathBuf::from(row.get::<_, String>(9).map_err(|error| {
                NetError::State(format!("failed to read downloads.temp_path: {error}"))
            })?),
            temp_layout,
            supports_resume,
            status,
            progress: ProgressSnapshot {
                downloaded: u64::from_db(
                    row.get::<_, i64>(14).map_err(|error| {
                        NetError::State(format!(
                            "failed to read downloads.progress_downloaded: {error}"
                        ))
                    })?,
                    "downloads.progress_downloaded",
                )?,
                total: row
                    .get::<_, Option<i64>>(15)
                    .map_err(|error| {
                        NetError::State(format!("failed to read downloads.progress_total: {error}"))
                    })?
                    .map(|value| u64::from_db(value, "downloads.progress_total"))
                    .transpose()?,
                speed_bps: row
                    .get::<_, Option<i64>>(16)
                    .map_err(|error| {
                        NetError::State(format!(
                            "failed to read downloads.progress_speed_bps: {error}"
                        ))
                    })?
                    .map(|value| u64::from_db(value, "downloads.progress_speed_bps"))
                    .transpose()?,
                eta_seconds: row
                    .get::<_, Option<i64>>(17)
                    .map_err(|error| {
                        NetError::State(format!(
                            "failed to read downloads.progress_eta_seconds: {error}"
                        ))
                    })?
                    .map(|value| u64::from_db(value, "downloads.progress_eta_seconds"))
                    .transpose()?,
            },
            error: row.get::<_, Option<String>>(18).map_err(|error| {
                NetError::State(format!("failed to read downloads.error: {error}"))
            })?,
            etag: row.get::<_, Option<String>>(19).map_err(|error| {
                NetError::State(format!("failed to read downloads.etag: {error}"))
            })?,
            last_modified: row.get::<_, Option<String>>(20).map_err(|error| {
                NetError::State(format!("failed to read downloads.last_modified: {error}"))
            })?,
            created_at: DateTime::<Utc>::from_db(
                row.get::<_, String>(21).map_err(|error| {
                    NetError::State(format!("failed to read downloads.created_at: {error}"))
                })?,
                "downloads.created_at",
            )?,
            updated_at: DateTime::<Utc>::from_db(
                row.get::<_, String>(22).map_err(|error| {
                    NetError::State(format!("failed to read downloads.updated_at: {error}"))
                })?,
                "downloads.updated_at",
            )?,
        });
    }

    Ok(PersistedQueue { next_id, downloads })
}

fn ensure_request_column(
    connection: &Connection,
    column: &str,
    definition: &str,
) -> Result<(), NetError> {
    let mut statement = connection
        .prepare("PRAGMA table_info(requests)")
        .map_err(|error| NetError::State(format!("failed to inspect requests schema: {error}")))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|error| NetError::State(format!("failed to query requests schema: {error}")))?;

    for existing in columns {
        let existing = existing
            .map_err(|error| NetError::State(format!("failed to read requests schema: {error}")))?;
        if existing == column {
            return Ok(());
        }
    }

    connection
        .execute(
            &format!("ALTER TABLE requests ADD COLUMN {column} {definition}"),
            [],
        )
        .map_err(|error| NetError::State(format!("failed to migrate requests schema: {error}")))?;
    Ok(())
}

fn read_multipart_parts(
    connection: &Connection,
    download_id: u64,
) -> Result<Vec<MultipartPart>, NetError> {
    let mut statement = connection
        .prepare(
            "
            SELECT part_index, part_start, part_end, path
            FROM multipart_parts
            WHERE download_id = ?1
            ORDER BY part_index ASC
            ",
        )
        .map_err(|error| {
            NetError::State(format!("failed to prepare multipart parts query: {error}"))
        })?;

    let mut rows = statement
        .query([download_id.to_db("multipart_parts.download_id")?])
        .map_err(|error| NetError::State(format!("failed to query multipart parts: {error}")))?;

    let mut parts = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|error| NetError::State(format!("failed to read multipart part row: {error}")))?
    {
        let raw_index = u64::from_db(
            row.get::<_, i64>(0).map_err(|error| {
                NetError::State(format!(
                    "failed to read multipart_parts.part_index: {error}"
                ))
            })?,
            "multipart_parts.part_index",
        )?;
        let index = usize::try_from(raw_index).map_err(|error| {
            NetError::State(format!(
                "invalid multipart_parts.part_index for download {download_id}: {error}"
            ))
        })?;
        parts.push(MultipartPart {
            index,
            start: u64::from_db(
                row.get::<_, i64>(1).map_err(|error| {
                    NetError::State(format!(
                        "failed to read multipart_parts.part_start: {error}"
                    ))
                })?,
                "multipart_parts.part_start",
            )?,
            end: u64::from_db(
                row.get::<_, i64>(2).map_err(|error| {
                    NetError::State(format!("failed to read multipart_parts.part_end: {error}"))
                })?,
                "multipart_parts.part_end",
            )?,
            path: PathBuf::from(row.get::<_, String>(3).map_err(|error| {
                NetError::State(format!("failed to read multipart_parts.path: {error}"))
            })?),
        });
    }

    Ok(parts)
}
