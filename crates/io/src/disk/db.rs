use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, params};
use tungsten_net::NetError;
use tungsten_net::model::{DownloadId, DownloadRequest, ProgressSnapshot};
use tungsten_net::store::{PersistedDownload, PersistedQueue};
use tungsten_net::transfer::{MultipartPart, TempLayout};

use super::codec::{
    bool_to_i64, decode_conflict_policy, decode_download_status, decode_integrity_rule,
    decode_temp_layout, encode_conflict_policy, encode_download_status, encode_integrity_rule,
    encode_temp_layout, i64_to_bool, i64_to_u64, u64_to_i64, usize_to_i64,
};

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
            [u64_to_i64(state.next_id, "queue_meta.value")?],
        )
        .map_err(|error| NetError::State(format!("failed to persist next_id: {error}")))?;

    transaction
        .execute("DELETE FROM multipart_parts", [])
        .map_err(|error| NetError::State(format!("failed to clear multipart parts: {error}")))?;
    transaction
        .execute("DELETE FROM downloads", [])
        .map_err(|error| NetError::State(format!("failed to clear downloads: {error}")))?;

    {
        let mut download_statement = transaction
            .prepare(
                "
                INSERT INTO downloads(
                    id,
                    request_url,
                    request_destination,
                    request_conflict,
                    request_integrity_kind,
                    request_integrity_value,
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
                    ?18, ?19, ?20, ?21, ?22
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
            let (integrity_kind, integrity_value) =
                encode_integrity_rule(&download.request.integrity);
            let (temp_layout_kind, temp_layout_total_size) =
                encode_temp_layout(&download.temp_layout)?;
            let conflict = encode_conflict_policy(&download.request.conflict);
            let status = encode_download_status(&download.status);
            let download_id = u64_to_i64(download.id.0, "downloads.id")?;

            download_statement
                .execute(params![
                    download_id,
                    download.request.url.as_str(),
                    download.request.destination.to_string_lossy().into_owned(),
                    conflict,
                    integrity_kind,
                    integrity_value,
                    download
                        .destination
                        .as_ref()
                        .map(|value| value.to_string_lossy().into_owned()),
                    bool_to_i64(download.loaded_from_store),
                    download.temp_path.to_string_lossy().into_owned(),
                    temp_layout_kind,
                    temp_layout_total_size,
                    bool_to_i64(download.supports_resume),
                    status,
                    u64_to_i64(
                        download.progress.downloaded,
                        "downloads.progress_downloaded"
                    )?,
                    download
                        .progress
                        .total
                        .map(|value| u64_to_i64(value, "downloads.progress_total"))
                        .transpose()?,
                    download
                        .progress
                        .speed_bps
                        .map(|value| u64_to_i64(value, "downloads.progress_speed_bps"))
                        .transpose()?,
                    download
                        .progress
                        .eta_seconds
                        .map(|value| u64_to_i64(value, "downloads.progress_eta_seconds"))
                        .transpose()?,
                    download.error.as_deref(),
                    download.etag.as_deref(),
                    download.last_modified.as_deref(),
                    u64_to_i64(download.created_at, "downloads.created_at")?,
                    u64_to_i64(download.updated_at, "downloads.updated_at")?
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
                            download_id,
                            usize_to_i64(part.index, "multipart_parts.part_index")?,
                            u64_to_i64(part.start, "multipart_parts.part_start")?,
                            u64_to_i64(part.end, "multipart_parts.part_end")?,
                            part.path.to_string_lossy().into_owned()
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
            CREATE TABLE IF NOT EXISTS downloads (
                id INTEGER PRIMARY KEY NOT NULL,
                request_url TEXT NOT NULL,
                request_destination TEXT NOT NULL,
                request_conflict TEXT NOT NULL,
                request_integrity_kind TEXT NOT NULL,
                request_integrity_value TEXT,
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
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
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
        .map_err(|error| NetError::State(format!("failed to initialize state db schema: {error}")))
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
        .map_or(Ok(0_u64), |value| i64_to_u64(value, "queue_meta.value"))?;

    let mut statement = connection
        .prepare(
            "
            SELECT
                id,
                request_url,
                request_destination,
                request_conflict,
                request_integrity_kind,
                request_integrity_value,
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
            FROM downloads
            ORDER BY id ASC
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
        let id = i64_to_u64(
            row.get::<_, i64>(0).map_err(|error| {
                NetError::State(format!("failed to read downloads.id: {error}"))
            })?,
            "downloads.id",
        )?;
        let request_conflict = row.get::<_, String>(3).map_err(|error| {
            NetError::State(format!(
                "failed to read downloads.request_conflict: {error}"
            ))
        })?;
        let request_integrity_kind = row.get::<_, String>(4).map_err(|error| {
            NetError::State(format!(
                "failed to read downloads.request_integrity_kind: {error}"
            ))
        })?;
        let request_integrity_value = row.get::<_, Option<String>>(5).map_err(|error| {
            NetError::State(format!(
                "failed to read downloads.request_integrity_value: {error}"
            ))
        })?;
        let loaded_from_store = row
            .get::<_, i64>(7)
            .map_err(|error| {
                NetError::State(format!(
                    "failed to read downloads.loaded_from_store: {error}"
                ))
            })
            .and_then(|value| i64_to_bool(value, "downloads.loaded_from_store"))?;
        let temp_layout_kind = row.get::<_, String>(9).map_err(|error| {
            NetError::State(format!(
                "failed to read downloads.temp_layout_kind: {error}"
            ))
        })?;
        let temp_layout_total_size = row.get::<_, Option<i64>>(10).map_err(|error| {
            NetError::State(format!(
                "failed to read downloads.temp_layout_total_size: {error}"
            ))
        })?;
        let supports_resume = row
            .get::<_, i64>(11)
            .map_err(|error| {
                NetError::State(format!("failed to read downloads.supports_resume: {error}"))
            })
            .and_then(|value| i64_to_bool(value, "downloads.supports_resume"))?;
        let status = row.get::<_, String>(12).map_err(|error| {
            NetError::State(format!("failed to read downloads.status: {error}"))
        })?;

        let temp_layout = decode_temp_layout(
            &temp_layout_kind,
            temp_layout_total_size,
            read_multipart_parts(connection, id)?,
        )?;

        downloads.push(PersistedDownload {
            id: DownloadId(id),
            request: DownloadRequest {
                url: row.get::<_, String>(1).map_err(|error| {
                    NetError::State(format!("failed to read downloads.request_url: {error}"))
                })?,
                destination: PathBuf::from(row.get::<_, String>(2).map_err(|error| {
                    NetError::State(format!(
                        "failed to read downloads.request_destination: {error}"
                    ))
                })?),
                conflict: decode_conflict_policy(&request_conflict)?,
                integrity: decode_integrity_rule(
                    &request_integrity_kind,
                    request_integrity_value.as_deref(),
                )?,
            },
            destination: row
                .get::<_, Option<String>>(6)
                .map_err(|error| {
                    NetError::State(format!("failed to read downloads.destination: {error}"))
                })?
                .map(PathBuf::from),
            loaded_from_store,
            temp_path: PathBuf::from(row.get::<_, String>(8).map_err(|error| {
                NetError::State(format!("failed to read downloads.temp_path: {error}"))
            })?),
            temp_layout,
            supports_resume,
            status: decode_download_status(&status)?,
            progress: ProgressSnapshot {
                downloaded: i64_to_u64(
                    row.get::<_, i64>(13).map_err(|error| {
                        NetError::State(format!(
                            "failed to read downloads.progress_downloaded: {error}"
                        ))
                    })?,
                    "downloads.progress_downloaded",
                )?,
                total: row
                    .get::<_, Option<i64>>(14)
                    .map_err(|error| {
                        NetError::State(format!("failed to read downloads.progress_total: {error}"))
                    })?
                    .map(|value| i64_to_u64(value, "downloads.progress_total"))
                    .transpose()?,
                speed_bps: row
                    .get::<_, Option<i64>>(15)
                    .map_err(|error| {
                        NetError::State(format!(
                            "failed to read downloads.progress_speed_bps: {error}"
                        ))
                    })?
                    .map(|value| i64_to_u64(value, "downloads.progress_speed_bps"))
                    .transpose()?,
                eta_seconds: row
                    .get::<_, Option<i64>>(16)
                    .map_err(|error| {
                        NetError::State(format!(
                            "failed to read downloads.progress_eta_seconds: {error}"
                        ))
                    })?
                    .map(|value| i64_to_u64(value, "downloads.progress_eta_seconds"))
                    .transpose()?,
            },
            error: row.get::<_, Option<String>>(17).map_err(|error| {
                NetError::State(format!("failed to read downloads.error: {error}"))
            })?,
            etag: row.get::<_, Option<String>>(18).map_err(|error| {
                NetError::State(format!("failed to read downloads.etag: {error}"))
            })?,
            last_modified: row.get::<_, Option<String>>(19).map_err(|error| {
                NetError::State(format!("failed to read downloads.last_modified: {error}"))
            })?,
            created_at: i64_to_u64(
                row.get::<_, i64>(20).map_err(|error| {
                    NetError::State(format!("failed to read downloads.created_at: {error}"))
                })?,
                "downloads.created_at",
            )?,
            updated_at: i64_to_u64(
                row.get::<_, i64>(21).map_err(|error| {
                    NetError::State(format!("failed to read downloads.updated_at: {error}"))
                })?,
                "downloads.updated_at",
            )?,
        });
    }

    Ok(PersistedQueue { next_id, downloads })
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
        .query([u64_to_i64(download_id, "multipart_parts.download_id")?])
        .map_err(|error| NetError::State(format!("failed to query multipart parts: {error}")))?;

    let mut parts = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|error| NetError::State(format!("failed to read multipart part row: {error}")))?
    {
        let raw_index = i64_to_u64(
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
            start: i64_to_u64(
                row.get::<_, i64>(1).map_err(|error| {
                    NetError::State(format!(
                        "failed to read multipart_parts.part_start: {error}"
                    ))
                })?,
                "multipart_parts.part_start",
            )?,
            end: i64_to_u64(
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
