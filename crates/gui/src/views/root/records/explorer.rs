use std::io::Error as IoError;
use std::path::{Path, PathBuf};

use tracing::debug;

pub(super) fn open_in_file_explorer(path: &Path) -> Result<(), IoError> {
    let target = explorer_target(path);
    debug!(
        destination = %path.display(),
        target = %target.display(),
        exists = path.exists(),
        "opening destination with opener"
    );

    opener::open(&target).map_err(|error| {
        IoError::other(format!(
            "failed to open {} in file explorer: {error}",
            target.display()
        ))
    })
}

fn explorer_target(path: &Path) -> PathBuf {
    if path.is_dir() {
        return path.to_path_buf();
    }

    if let Some(parent) = path.parent() {
        parent.to_path_buf()
    } else {
        path.to_path_buf()
    }
}
