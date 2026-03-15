use std::fs::File;
use std::path::{Path, PathBuf};

use super::*;

const PAYLOAD_FILE_NAME: &str = "payload.part";

/// One chunk-storage session rooted at a deterministic directory on disk.
///
/// A session owns the temporary payload file and any per-part chunk files for
/// one logical download.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkSession {
    key: String,
    root: PathBuf,
    payload: PathBuf,
}

impl ChunkSession {
    /// Builds a session from a caller-provided key and root path.
    pub fn new(key: String, root: PathBuf) -> Self {
        Self {
            key,
            payload: root.join(PAYLOAD_FILE_NAME),
            root,
        }
    }

    /// Returns the stable session key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the directory that contains the session's temporary files.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Returns the merged payload path for this session.
    pub fn payload_path(&self) -> &Path {
        &self.payload
    }

    /// Returns the path for a numbered chunk file inside this session.
    pub fn part_path(&self, index: usize) -> PathBuf {
        self.root.join(format!("part-{index}.chunk"))
    }
}

/// Ordered chunk ranges for one target file.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ChunkLayout {
    total_size: u64,
    parts: Vec<ChunkPart>,
}

impl ChunkLayout {
    /// Splits `total_size` into at most `chunks` contiguous byte ranges.
    pub fn split(total_size: u64, chunks: usize) -> Self {
        if total_size == 0 {
            return Self::default();
        }

        let max_parts = match usize::try_from(total_size) {
            Ok(value) => value,
            Err(_) => usize::MAX,
        };
        let part_count = chunks.max(1).min(max_parts);
        let base = total_size / part_count as u64;
        let extra = total_size % part_count as u64;
        let mut parts = Vec::with_capacity(part_count);
        let mut start = 0u64;

        for index in 0..part_count {
            let len = base + u64::from(index < extra as usize);
            let end = start + len.saturating_sub(1);
            parts.push(ChunkPart { index, start, end });
            start = end.saturating_add(1);
        }

        Self { total_size, parts }
    }

    /// Returns the total size represented by this layout.
    pub fn total_size(&self) -> u64 {
        self.total_size
    }

    /// Returns the ordered chunk ranges.
    pub fn parts(&self) -> &[ChunkPart] {
        &self.parts
    }
}

/// One contiguous byte range inside a [`ChunkLayout`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkPart {
    index: usize,
    start: u64,
    end: u64,
}

impl ChunkPart {
    /// Returns the zero-based part index.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Returns the inclusive byte offset where this part starts.
    pub fn start(&self) -> u64 {
        self.start
    }

    /// Returns the inclusive byte offset where this part ends.
    pub fn end(&self) -> u64 {
        self.end
    }

    /// Returns the number of bytes expected for this part.
    pub fn len(&self) -> u64 {
        self.end.saturating_sub(self.start).saturating_add(1)
    }

    /// Returns whether this part covers zero bytes.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Storage interface for chunked temporary files.
///
/// Implementations are responsible only for filesystem concerns: session
/// layout, resumable file access, merge operations, and cleanup.
pub trait ChunkFilesystem: Send + Sync {
    /// Returns the filesystem root used for all chunk sessions.
    fn root(&self) -> &Path;

    /// Resolves a session for `key` without creating files on disk.
    fn session(&self, key: &str) -> Result<ChunkSession, FilesystemError>;

    /// Resolves a session for `key` and ensures its directory exists.
    fn create_session(&self, key: &str) -> Result<ChunkSession, FilesystemError>;

    /// Returns the current length of the merged payload file.
    fn payload_len(&self, session: &ChunkSession) -> Result<u64, FilesystemError>;

    /// Returns the downloaded byte count for each part in `layout`.
    ///
    /// Invalid oversized part files are treated as corrupt and reset.
    fn part_progress(
        &self,
        session: &ChunkSession,
        layout: &ChunkLayout,
    ) -> Result<Vec<u64>, FilesystemError>;

    /// Opens a part file positioned at `downloaded` bytes for resume.
    fn open_part_writer(
        &self,
        session: &ChunkSession,
        part: &ChunkPart,
        downloaded: u64,
    ) -> Result<File, FilesystemError>;

    /// Opens the merged payload file positioned at `downloaded` bytes.
    fn open_payload_writer(
        &self,
        session: &ChunkSession,
        downloaded: u64,
    ) -> Result<File, FilesystemError>;

    /// Concatenates all parts in layout order into the payload file.
    fn merge_parts(
        &self,
        session: &ChunkSession,
        layout: &ChunkLayout,
    ) -> Result<(), FilesystemError>;

    /// Removes all temporary files owned by the session.
    fn cleanup_session(&self, session: &ChunkSession) -> Result<(), FilesystemError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_session_builds_payload_and_part_paths() {
        let session = ChunkSession::new("abc".to_string(), PathBuf::from("/tmp/session"));

        assert_eq!(session.key(), "abc");
        assert_eq!(session.root(), Path::new("/tmp/session"));
        assert_eq!(
            session.payload_path(),
            Path::new("/tmp/session/payload.part")
        );
        assert_eq!(
            session.part_path(2),
            PathBuf::from("/tmp/session/part-2.chunk")
        );
    }

    #[test]
    fn chunk_layout_split_balances_parts() {
        let layout = ChunkLayout::split(10, 3);
        let lengths = layout
            .parts()
            .iter()
            .map(ChunkPart::len)
            .collect::<Vec<_>>();

        assert_eq!(layout.total_size(), 10);
        assert_eq!(layout.parts().len(), 3);
        assert_eq!(lengths, vec![4, 3, 3]);
        assert_eq!(layout.parts()[0].start(), 0);
        assert_eq!(layout.parts()[0].end(), 3);
        assert_eq!(layout.parts()[2].start(), 7);
        assert_eq!(layout.parts()[2].end(), 9);
    }

    #[test]
    fn chunk_layout_split_zero_size_is_empty() {
        let layout = ChunkLayout::split(0, 4);

        assert_eq!(layout.total_size(), 0);
        assert!(layout.parts().is_empty());
    }
}
