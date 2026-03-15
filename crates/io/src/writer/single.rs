use std::path::PathBuf;
use std::time::Instant;

use async_trait::async_trait;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tracing::debug;

use super::{WriteStream, Writer, WriterError};

/// Sequential payload writer backed by one temp file.
#[derive(Debug)]
pub struct SingleWriter {
    path: PathBuf,
    total_size: Option<u64>,
    existing_size: u64,
    created: bool,
    opened: bool,
}

impl SingleWriter {
    /// Builds a writer for one temp payload file.
    pub fn new(path: PathBuf, total_size: Option<u64>, existing_size: u64) -> Self {
        Self {
            path,
            total_size,
            existing_size,
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

        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).await?;
        }

        if let Some(total_size) = self.total_size {
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(&self.path)
                .await?;
            let previous_size = file.metadata().await?.len();
            let started_at = Instant::now();
            debug!(
                path = %self.path.display(),
                previous_size,
                target_size = total_size,
                "starting temp file preallocation"
            );
            file.set_len(total_size).await?;
            debug!(
                path = %self.path.display(),
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

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(self.total_size.is_none() && self.existing_size == 0)
            .open(&self.path)
            .await?;
        file.seek(std::io::SeekFrom::Start(self.existing_size))
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
        match fs::remove_file(&self.path).await {
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
