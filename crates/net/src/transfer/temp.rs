use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::error::NetError;

use super::{DOWNLOAD_BUFFER_SIZE, MultipartPart, MultipartState, TempLayout};

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
            Ok(layout.clone())
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
        let size = match fs::metadata(&part.path) {
            Ok(metadata) if metadata.len() <= expected => metadata.len(),
            Ok(_) => {
                fs::remove_file(&part.path)?;
                0
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(NetError::Io(error)),
        };
        progress.push(size);
    }
    Ok(progress)
}

pub(crate) fn merge_parts(temp_path: &Path, layout: &MultipartState) -> Result<(), NetError> {
    if let Some(parent) = temp_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let merge_result = (|| -> Result<(), NetError> {
        let mut output = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(temp_path)?;
        let mut buffer = [0u8; DOWNLOAD_BUFFER_SIZE];

        for part in &layout.parts {
            let mut input = fs::File::open(&part.path)?;
            loop {
                let read = input.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                output.write_all(&buffer[..read])?;
            }
        }

        output.flush()?;
        Ok(())
    })();

    if let Err(error) = &merge_result {
        let _ = fs::remove_file(temp_path);
        return Err(NetError::Backend(format!(
            "failed to merge multipart download: {error}"
        )));
    }

    cleanup_parts(layout)
}

pub(crate) fn cleanup_layout(layout: &TempLayout) -> Result<(), NetError> {
    if let TempLayout::Multipart(layout) = layout {
        cleanup_parts(layout)?;
    }
    Ok(())
}

pub(crate) fn cleanup_parts(layout: &MultipartState) -> Result<(), NetError> {
    for part in &layout.parts {
        match fs::remove_file(&part.path) {
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
