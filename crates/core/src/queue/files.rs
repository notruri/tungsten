use std::collections::HashMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use crate::model::{ConflictPolicy, DownloadId};
use crate::store::PersistedDownload;

use super::DEFAULT_DOWNLOAD_FILE_NAME;

const TEMP_DIR_NAME: &str = "Tungsten";

pub(crate) fn resolve_destination(
    requested: &Path,
    downloads: &HashMap<DownloadId, PersistedDownload>,
    conflict: &ConflictPolicy,
) -> PathBuf {
    if !matches!(conflict, ConflictPolicy::AutoRename) {
        return requested.to_path_buf();
    }

    if !path_conflicts(requested, downloads) {
        return requested.to_path_buf();
    }

    let parent = requested
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let stem = requested
        .file_stem()
        .map(OsString::from)
        .unwrap_or_else(|| OsString::from("download"));
    let extension = requested.extension().map(OsString::from);

    let mut index = 1u64;
    loop {
        let mut candidate_name = OsString::from(&stem);
        candidate_name.push(format!(" ({index})"));

        let candidate = if let Some(ext) = &extension {
            let mut with_ext = candidate_name.clone();
            with_ext.push(".");
            with_ext.push(ext);
            parent.join(with_ext)
        } else {
            parent.join(candidate_name)
        };

        if !path_conflicts(&candidate, downloads) {
            return candidate;
        }

        index = index.saturating_add(1);
    }
}

pub(crate) fn destination_from_request(
    requested: &Path,
    source_url: &str,
    remote_file_name: Option<&str>,
    fallback_file_name: &str,
) -> PathBuf {
    if !looks_like_directory_path(requested) {
        return requested.to_path_buf();
    }

    let file_name = remote_file_name
        .and_then(sanitize_file_name)
        .or_else(|| file_name_from_url_path(source_url))
        .unwrap_or_else(|| {
            sanitize_file_name(fallback_file_name)
                .unwrap_or_else(|| DEFAULT_DOWNLOAD_FILE_NAME.to_string())
        });
    requested.join(file_name)
}

pub(crate) fn fallback_destination(requested: &Path, fallback_file_name: &str) -> PathBuf {
    if looks_like_directory_path(requested) {
        let fallback = sanitize_file_name(fallback_file_name)
            .unwrap_or_else(|| DEFAULT_DOWNLOAD_FILE_NAME.to_string());
        return requested.join(fallback);
    }

    requested.to_path_buf()
}

pub(crate) fn looks_like_directory_path(path: &Path) -> bool {
    if path.is_dir() {
        return true;
    }

    let raw = path.to_string_lossy();
    if raw.ends_with('/') || raw.ends_with('\\') {
        return true;
    }

    path.extension().is_none()
}

pub(crate) fn temp_path_for(destination: &Path, download_id: DownloadId) -> PathBuf {
    let file_name = destination
        .file_name()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_else(|| "download".to_string());
    let temp_name = format!("{file_name}.{download_id}.part");
    std::env::temp_dir().join(TEMP_DIR_NAME).join(temp_name)
}

fn sanitize_file_name(value: &str) -> Option<String> {
    let trimmed = value.trim().trim_matches('.');
    if trimmed.is_empty() {
        return None;
    }

    let mut output = String::with_capacity(trimmed.len());
    for ch in trimmed.chars() {
        match ch {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => output.push('_'),
            '\u{0000}'..='\u{001F}' => output.push('_'),
            _ => output.push(ch),
        }
    }

    if output.trim().is_empty() {
        None
    } else {
        Some(output)
    }
}

fn file_name_from_url_path(value: &str) -> Option<String> {
    let value = value.split('#').next()?;
    let value = value.split('?').next()?;
    let path = if let Some((_, remainder)) = value.split_once("://") {
        let (_, path) = remainder.split_once('/')?;
        path
    } else {
        value
    };

    let segment = path.rsplit('/').find(|part| !part.is_empty())?;
    let decoded = percent_decode(segment).unwrap_or_else(|| segment.to_string());
    sanitize_file_name(&decoded)
}

fn percent_decode(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0usize;

    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return None;
            }
            let hi = from_hex(bytes[index + 1])?;
            let lo = from_hex(bytes[index + 2])?;
            decoded.push((hi << 4) | lo);
            index += 3;
            continue;
        }

        decoded.push(bytes[index]);
        index += 1;
    }

    String::from_utf8(decoded).ok()
}

fn from_hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn path_conflicts(path: &Path, downloads: &HashMap<DownloadId, PersistedDownload>) -> bool {
    if path.exists() {
        return true;
    }

    downloads.values().any(|record| {
        record
            .destination
            .as_ref()
            .is_some_and(|destination| destination == path)
    })
}
