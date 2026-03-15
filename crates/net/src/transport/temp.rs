//! Multipart temp-layout helpers.
//!
//! The transport layer persists multipart state in the queue model, but it
//! still needs filesystem-specific helpers to:
//! - derive the set of part files for a payload temp path
//! - restore per-part progress from persisted cursors or legacy part files
//! - clean up obsolete multipart files when layouts change

use std::collections::HashSet;
use std::ffi::OsString;
use std::fs as stdfs;
use std::path::{Path, PathBuf};

use crate::error::NetError;

use super::{MultipartPart, MultipartState, TempLayout};

pub(crate) fn prepare_layout(
    temp_path: &Path,
    current: &TempLayout,
    total_size: u64,
    connections: usize,
) -> Result<MultipartState, NetError> {
    match current {
        TempLayout::Multipart(layout)
            if layout.total_size == total_size && !layout.parts.is_empty() =>
        {
            let mut normalized = layout.clone();
            for part in &mut normalized.parts {
                normalize_cursor(part);
            }
            Ok(normalized)
        }
        TempLayout::Multipart(layout) => {
            cleanup_parts(layout)?;
            Ok(build_layout(temp_path, total_size, connections))
        }
        TempLayout::Single => Ok(build_layout(temp_path, total_size, connections)),
    }
}

pub(crate) fn build_layout(
    temp_path: &Path,
    total_size: u64,
    connections: usize,
) -> MultipartState {
    let part_count = connections.max(1).min(total_size as usize).max(1);
    let base = total_size / part_count as u64;
    let extra = total_size % part_count as u64;
    let mut parts = Vec::with_capacity(part_count);
    let mut start = 0u64;

    for index in 0..part_count {
        let len = base + u64::from(index < extra as usize);
        let end = start + len.saturating_sub(1);
        parts.push(MultipartPart {
            index,
            start,
            end,
            cursor: start,
            path: part_path_for(temp_path, index),
        });
        start = end.saturating_add(1);
    }

    MultipartState { total_size, parts }
}

pub(crate) fn load_part_progress(layout: &MultipartState) -> Result<Vec<u64>, NetError> {
    let mut progress = Vec::with_capacity(layout.parts.len());
    for part in &layout.parts {
        let expected = part_len(part);
        let cursor = part.cursor.clamp(part.start, part.end.saturating_add(1));
        let downloaded_from_cursor = cursor.saturating_sub(part.start);
        if downloaded_from_cursor > 0 {
            progress.push(downloaded_from_cursor.min(expected));
            continue;
        }

        // Compatibility path for older persisted multipart layouts that tracked
        // progress using per-part files on disk.
        let size = match stdfs::metadata(&part.path) {
            Ok(metadata) if metadata.len() <= expected => metadata.len(),
            Ok(_) => {
                stdfs::remove_file(&part.path)?;
                0
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(NetError::Io(error)),
        };
        progress.push(size);
    }
    Ok(progress)
}

pub(crate) fn cleanup_parts(layout: &MultipartState) -> Result<(), NetError> {
    let mut seen = HashSet::with_capacity(layout.parts.len());
    for part in &layout.parts {
        if !seen.insert(part.path.clone()) {
            continue;
        }
        match stdfs::remove_file(&part.path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(NetError::Io(error)),
        }
    }
    Ok(())
}

pub(crate) fn part_len(part: &MultipartPart) -> u64 {
    part.end.saturating_sub(part.start).saturating_add(1)
}

fn part_path_for(temp_path: &Path, index: usize) -> PathBuf {
    let mut name = OsString::from(temp_path.as_os_str());
    name.push(format!(".p{index}"));
    PathBuf::from(name)
}

fn normalize_cursor(part: &mut MultipartPart) {
    if part.cursor < part.start || part.cursor > part.end.saturating_add(1) {
        part.cursor = part.start;
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;
    use tungsten_core::{MultipartState, TempLayout};

    use super::*;

    #[test]
    fn build_layout_splits_total_size_across_parts() {
        let layout = build_layout(Path::new("download.part"), 10, 3);
        let lengths = layout.parts.iter().map(part_len).collect::<Vec<_>>();

        assert_eq!(layout.total_size, 10);
        assert_eq!(layout.parts.len(), 3);
        assert_eq!(lengths, vec![4, 3, 3]);
    }

    #[test]
    fn prepare_layout_reuses_matching_layout_and_normalizes_cursor() {
        let existing = MultipartState {
            total_size: 8,
            parts: vec![MultipartPart {
                index: 0,
                start: 0,
                end: 7,
                cursor: 99,
                path: PathBuf::from("download.part.p0"),
            }],
        };

        let prepared = prepare_layout(
            Path::new("download.part"),
            &TempLayout::Multipart(existing),
            8,
            1,
        )
        .unwrap_or_else(|error| panic!("layout should prepare: {error}"));

        assert_eq!(prepared.parts[0].cursor, 0);
    }

    #[test]
    fn load_part_progress_prefers_cursor_and_cleans_oversized_legacy_file() {
        let root = tempdir().unwrap_or_else(|error| panic!("tempdir should be created: {error}"));
        let oversized = root.path().join("download.part.p1");
        fs::write(&oversized, vec![1u8; 8])
            .unwrap_or_else(|error| panic!("legacy part file should be written: {error}"));

        let layout = MultipartState {
            total_size: 8,
            parts: vec![
                MultipartPart {
                    index: 0,
                    start: 0,
                    end: 3,
                    cursor: 2,
                    path: root.path().join("download.part.p0"),
                },
                MultipartPart {
                    index: 1,
                    start: 4,
                    end: 7,
                    cursor: 4,
                    path: oversized.clone(),
                },
            ],
        };

        let progress = load_part_progress(&layout)
            .unwrap_or_else(|error| panic!("part progress should load: {error}"));

        assert_eq!(progress, vec![2, 0]);
        assert!(!oversized.exists());
    }
}
