use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::{ChunkFilesystem, ChunkLayout, ChunkPart, ChunkSession, FilesystemError};

const APP_DIR_NAME: &str = "Tungsten";
const CHUNKS_DIR_NAME: &str = "chunks";
const BUFFER_SIZE: usize = 64 * 1024;

/// Local-disk [`ChunkFilesystem`] implementation.
///
/// Sessions are stored under `<root>/<session-key>/` and use:
/// - `payload.part` for the merged temp payload
/// - `part-<index>.chunk` for individual chunk files
#[derive(Debug, Clone)]
pub struct LocalFilesystem {
    root: PathBuf,
}

impl LocalFilesystem {
    /// Creates a filesystem rooted at `root`.
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// Builds a filesystem under `root/Tungsten/chunks`.
    pub fn with_temp_root(root: &Path) -> Self {
        Self::new(root.join(APP_DIR_NAME).join(CHUNKS_DIR_NAME))
    }

    /// Returns the default centralized temp root.
    pub fn default_root() -> PathBuf {
        std::env::temp_dir()
            .join(APP_DIR_NAME)
            .join(CHUNKS_DIR_NAME)
    }

    fn session_root(&self, key: &str) -> Result<PathBuf, FilesystemError> {
        Ok(self.root.join(sanitize_key(key)?))
    }

    fn metadata_len(path: &Path) -> Result<u64, FilesystemError> {
        match fs::metadata(path) {
            Ok(metadata) => Ok(metadata.len()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(error) => Err(FilesystemError::Io(error)),
        }
    }

    fn open_writer(path: &Path, downloaded: u64) -> Result<File, FilesystemError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        file.set_len(downloaded)?;
        file.seek(SeekFrom::Start(downloaded))?;
        Ok(file)
    }
}

impl Default for LocalFilesystem {
    fn default() -> Self {
        Self::new(Self::default_root())
    }
}

impl ChunkFilesystem for LocalFilesystem {
    fn root(&self) -> &Path {
        &self.root
    }

    fn session(&self, key: &str) -> Result<ChunkSession, FilesystemError> {
        Ok(ChunkSession::new(key.to_string(), self.session_root(key)?))
    }

    fn create_session(&self, key: &str) -> Result<ChunkSession, FilesystemError> {
        let session = self.session(key)?;
        fs::create_dir_all(session.root())?;
        Ok(session)
    }

    fn payload_len(&self, session: &ChunkSession) -> Result<u64, FilesystemError> {
        Self::metadata_len(session.payload_path())
    }

    fn part_progress(
        &self,
        session: &ChunkSession,
        layout: &ChunkLayout,
    ) -> Result<Vec<u64>, FilesystemError> {
        let mut progress = Vec::with_capacity(layout.parts().len());

        for part in layout.parts() {
            let path = session.part_path(part.index());
            let size = Self::metadata_len(&path)?;
            if size > part.len() {
                fs::remove_file(path)?;
                progress.push(0);
            } else {
                progress.push(size);
            }
        }

        Ok(progress)
    }

    fn open_part_writer(
        &self,
        session: &ChunkSession,
        part: &ChunkPart,
        downloaded: u64,
    ) -> Result<File, FilesystemError> {
        if downloaded > part.len() {
            return Err(FilesystemError::InvalidState(format!(
                "part {} resume offset {} exceeds part length {}",
                part.index(),
                downloaded,
                part.len()
            )));
        }

        Self::open_writer(&session.part_path(part.index()), downloaded)
    }

    fn open_payload_writer(
        &self,
        session: &ChunkSession,
        downloaded: u64,
    ) -> Result<File, FilesystemError> {
        Self::open_writer(session.payload_path(), downloaded)
    }

    fn merge_parts(
        &self,
        session: &ChunkSession,
        layout: &ChunkLayout,
    ) -> Result<(), FilesystemError> {
        let merge_result = (|| -> Result<(), FilesystemError> {
            let mut output = Self::open_writer(session.payload_path(), 0)?;
            let mut buffer = [0u8; BUFFER_SIZE];

            for part in layout.parts() {
                let path = session.part_path(part.index());
                let mut input = File::open(&path)?;
                let mut copied = 0u64;

                loop {
                    let read = input.read(&mut buffer)?;
                    if read == 0 {
                        break;
                    }

                    output.write_all(&buffer[..read])?;
                    copied += read as u64;
                }

                if copied != part.len() {
                    return Err(FilesystemError::InvalidState(format!(
                        "part {} size mismatch: expected {}, got {}",
                        part.index(),
                        part.len(),
                        copied
                    )));
                }
            }

            output.flush()?;
            Ok(())
        })();

        if let Err(error) = &merge_result {
            match fs::remove_file(session.payload_path()) {
                Ok(()) => {}
                Err(remove_error) if remove_error.kind() == std::io::ErrorKind::NotFound => {}
                Err(remove_error) => return Err(FilesystemError::Io(remove_error)),
            }
            return Err(FilesystemError::InvalidState(format!(
                "failed to merge parts: {error}"
            )));
        }

        for part in layout.parts() {
            match fs::remove_file(session.part_path(part.index())) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(FilesystemError::Io(error)),
            }
        }

        Ok(())
    }

    fn cleanup_session(&self, session: &ChunkSession) -> Result<(), FilesystemError> {
        match fs::remove_dir_all(session.root()) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(FilesystemError::Io(error)),
        }
    }
}

fn sanitize_key(key: &str) -> Result<String, FilesystemError> {
    let trimmed = key.trim();
    if trimmed.is_empty() {
        return Err(FilesystemError::InvalidKey(
            "key must not be empty".to_string(),
        ));
    }

    let mut output = String::with_capacity(trimmed.len());
    for ch in trimmed.chars() {
        match ch {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => output.push('_'),
            '\u{0000}'..='\u{001F}' => output.push('_'),
            _ => output.push(ch),
        }
    }

    let normalized = output.trim().trim_matches('.').to_string();
    if normalized.is_empty() || normalized == "." || normalized == ".." {
        return Err(FilesystemError::InvalidKey(format!(
            "key `{trimmed}` resolves to an empty path component"
        )));
    }

    Ok(normalized)
}
