use std::path::PathBuf;
use std::time::Instant;

use async_trait::async_trait;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tracing::debug;

use super::{WriteStream, Writer, WriterError};

/// Prepared single-file temp session.
#[derive(Debug)]
pub struct SingleSession {
    path: PathBuf,
    file: Option<File>,
    existing_size: u64,
}

impl SingleSession {
    /// Opens the temp file once and captures trusted resume state from that handle.
    pub async fn open(path: PathBuf) -> Result<Self, WriterError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .await?;
        let existing_size = file.metadata().await?.len();

        Ok(Self {
            path,
            file: Some(file),
            existing_size,
        })
    }

    pub fn existing_size(&self) -> u64 {
        self.existing_size
    }

    fn file(&self) -> Result<&File, WriterError> {
        self.file
            .as_ref()
            .ok_or_else(|| WriterError::State("single session file is unavailable".to_string()))
    }

    fn close(&mut self) {
        self.file = None;
    }
}

/// Sequential payload writer backed by one temp file.
#[derive(Debug)]
pub struct SingleWriter {
    session: SingleSession,
    total_size: Option<u64>,
    start_offset: u64,
    created: bool,
    opened: bool,
}

impl SingleWriter {
    /// Builds a writer for one temp payload file.
    pub fn new(session: SingleSession, total_size: Option<u64>, start_offset: u64) -> Self {
        Self {
            session,
            total_size,
            start_offset,
            created: false,
            opened: false,
        }
    }
}

#[async_trait]
impl Writer for SingleWriter {
    type Stream = SingleStream;

    async fn create(&mut self) -> Result<(), WriterError> {
        if self.created {
            return Ok(());
        }

        if let Some(total_size) = self.total_size {
            let previous_size = self.session.existing_size();
            let file = self.session.file()?;
            let started_at = Instant::now();
            debug!(
                path = %self.session.path.display(),
                previous_size,
                target_size = total_size,
                "starting temp file preallocation"
            );
            file.set_len(total_size).await?;
            debug!(
                path = %self.session.path.display(),
                previous_size,
                target_size = total_size,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "finished temp file preallocation"
            );
        }

        self.created = true;
        Ok(())
    }

    async fn stream(&mut self, index: usize) -> Result<Self::Stream, WriterError> {
        if index != 0 {
            return Err(WriterError::InvalidStream(index));
        }
        if self.opened {
            return Err(WriterError::State(
                "single writer stream already opened".to_string(),
            ));
        }
        if !self.created {
            return Err(WriterError::State(
                "single writer must be created before opening a stream".to_string(),
            ));
        }

        let mut file = self.session.file()?.try_clone().await?;
        if self.total_size.is_none() && self.start_offset == 0 {
            file.set_len(0).await?;
        }
        file.seek(std::io::SeekFrom::Start(self.start_offset))
            .await?;

        self.opened = true;
        Ok(SingleStream { file })
    }

    async fn flush(&mut self) -> Result<(), WriterError> {
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), WriterError> {
        Ok(())
    }

    async fn cleanup(&mut self) -> Result<(), WriterError> {
        self.session.close();
        match fs::remove_file(&self.session.path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(WriterError::Io(error)),
        }
    }
}

/// Owned sequential payload stream.
#[derive(Debug)]
pub struct SingleStream {
    file: File,
}

#[async_trait]
impl WriteStream for SingleStream {
    async fn send(&mut self, data: &[u8]) -> Result<(), WriterError> {
        self.file.write_all(data).await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), WriterError> {
        self.file.flush().await?;
        Ok(())
    }
}
