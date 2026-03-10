use std::collections::HashMap;
use std::ffi::OsString;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use reqwest::Url;
use sha2::{Digest, Sha256};

use crate::error::NetError;
use crate::model::{ConflictPolicy, DownloadId, DownloadRequest};
use crate::store::PersistedDownload;
use crate::transfer::{TempLayout, temp};

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

pub(crate) fn apply_inferred_destination_file_name(
    request: &mut DownloadRequest,
    remote_file_name: Option<&str>,
) {
    let inferred_file_name = remote_file_name
        .and_then(sanitize_file_name)
        .or_else(|| infer_file_name_from_url(&request.url));
    let looks_like_directory = destination_looks_like_directory(&request.destination);

    if looks_like_directory {
        let file_name =
            inferred_file_name.unwrap_or_else(|| DEFAULT_DOWNLOAD_FILE_NAME.to_string());
        request.destination = request.destination.join(file_name);
        return;
    }

    if !should_infer_destination_file_name(&request.destination, &request.url) {
        return;
    }

    if let Some(file_name) = inferred_file_name {
        request.destination = request.destination.with_file_name(file_name);
    }
}

pub(crate) fn temp_path_for(destination: &Path, download_id: DownloadId) -> PathBuf {
    temp_path_in(&std::env::temp_dir(), destination, download_id)
}

fn temp_path_in(temp_root: &Path, destination: &Path, download_id: DownloadId) -> PathBuf {
    let file_name = destination
        .file_name()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_else(|| "download".to_string());
    let temp_name = format!("{file_name}.{download_id}.part");
    temp_root.join(TEMP_DIR_NAME).join(temp_name)
}

pub(crate) fn remove_file_if_exists(path: &Path) -> Result<(), NetError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(NetError::Io(error)),
    }
}

pub(crate) fn remove_temp_layout_files(layout: &TempLayout) -> Result<(), NetError> {
    temp::cleanup_layout(layout)
}

pub(crate) fn sha256_file(path: &Path) -> Result<String, NetError> {
    let file = fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];

    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(hex::encode(hasher.finalize()))
}

fn destination_looks_like_directory(destination: &Path) -> bool {
    if destination.is_dir() {
        return true;
    }

    let raw = destination.to_string_lossy();
    if raw.ends_with('/') || raw.ends_with('\\') {
        return true;
    }

    destination.extension().is_none()
}

fn should_infer_destination_file_name(destination: &Path, url: &str) -> bool {
    let file_name = destination
        .file_name()
        .and_then(|value| value.to_str())
        .map(str::trim)
        .unwrap_or_default();

    if file_name.eq_ignore_ascii_case(DEFAULT_DOWNLOAD_FILE_NAME) {
        return true;
    }

    infer_file_name_from_url(url)
        .map(|inferred| inferred == file_name)
        .unwrap_or(false)
}

fn infer_file_name_from_url(url: &str) -> Option<String> {
    let parsed = Url::parse(url).ok()?;
    let segment = parsed
        .path_segments()?
        .filter(|value| !value.is_empty())
        .last()?;
    sanitize_file_name(segment)
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

fn path_conflicts(path: &Path, downloads: &HashMap<DownloadId, PersistedDownload>) -> bool {
    if path.exists() {
        return true;
    }

    downloads
        .values()
        .any(|record| record.request.destination == path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temp_path_uses_temp_root_instead_of_destination_parent() {
        let temp_path = temp_path_in(
            Path::new(r"C:\Users\Name\AppData\Local\Temp"),
            Path::new(r"D:\Downloads\file.bin"),
            DownloadId(7),
        );

        assert_eq!(
            temp_path,
            PathBuf::from(r"C:\Users\Name\AppData\Local\Temp\Tungsten\file.bin.7.part")
        );
    }

    #[test]
    fn temp_path_falls_back_to_download_name_when_destination_has_no_file_name() {
        let temp_path = temp_path_in(Path::new("/tmp"), Path::new(""), DownloadId(3));

        assert_eq!(temp_path, PathBuf::from("/tmp/Tungsten/download.3.part"));
    }
}
