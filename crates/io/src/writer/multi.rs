use std::path::PathBuf;

use async_trait::async_trait;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use super::{WriteStream, Writer, WriterError};

const BUFFER_SIZE: usize = 64 * 1024;

/// One logical part owned by a [`MultiWriter`].
#[derive(Debug, Clone)]
pub struct MultiPart {
    /// Stream index used by the transport layer.
    pub index: usize,
    /// Part file path for this stream.
    pub path: PathBuf,
    /// Expected byte length of this part.
    pub len: u64,
    /// Already downloaded byte count used for resume.
    pub downloaded: u64,
}

/// Construction parameters for [`MultiWriter`].
#[derive(Debug)]
pub struct MultiConfig {
    /// Final payload temp path produced during [`Writer::finish`].
    pub payload_path: PathBuf,
    /// Ordered stream parts.
    pub parts: Vec<MultiPart>,
}

/// Multipart writer backed by one file per stream.
#[derive(Debug)]
pub struct MultiWriter {
    payload_path: PathBuf,
    parts: Vec<MultiPart>,
    created: bool,
    opened: Vec<bool>,
}

impl MultiWriter {
    /// Builds a multipart writer.
    pub fn new(config: MultiConfig) -> Self {
        let opened = vec![false; config.parts.len()];
        Self {
            payload_path: config.payload_path,
            parts: config.parts,
            created: false,
            opened,
        }
    }
}

#[async_trait]
impl Writer for MultiWriter {
    type Stream = MultiStream;

    async fn create(&mut self) -> Result<(), WriterError> {
        if self.created {
            return Ok(());
        }

        if let Some(parent) = self.payload_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        for part in &self.parts {
            if let Some(parent) = part.path.parent() {
                fs::create_dir_all(parent).await?;
            }
        }

        self.created = true;
        Ok(())
    }

    async fn stream(&mut self, index: usize) -> Result<Self::Stream, WriterError> {
        if !self.created {
            return Err(WriterError::State(
                "multi writer must be created before opening a stream".to_string(),
            ));
        }

        let Some(part) = self.parts.get(index).cloned() else {
            return Err(WriterError::InvalidStream(index));
        };
        let Some(opened) = self.opened.get_mut(index) else {
            return Err(WriterError::InvalidStream(index));
        };
        if *opened {
            return Err(WriterError::State(format!(
                "multi writer stream {index} already opened"
            )));
        }
        if part.downloaded > part.len {
            return Err(WriterError::State(format!(
                "stream {index} resume offset {} exceeds part length {}",
                part.downloaded, part.len
            )));
        }

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&part.path)
            .await?;
        file.set_len(part.downloaded).await?;
        file.seek(std::io::SeekFrom::Start(part.downloaded)).await?;
        *opened = true;

        Ok(MultiStream { file })
    }

    async fn flush(&mut self) -> Result<(), WriterError> {
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), WriterError> {
        if let Some(parent) = self.payload_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let mut output = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.payload_path)
            .await?;
        let mut buffer = vec![0u8; BUFFER_SIZE];

        for part in &self.parts {
            let mut input = File::open(&part.path).await?;
            let mut copied = 0u64;

            loop {
                let read = input.read(&mut buffer).await?;
                if read == 0 {
                    break;
                }
                output.write_all(&buffer[..read]).await?;
                copied += read as u64;
            }

            if copied != part.len {
                return Err(WriterError::State(format!(
                    "stream {} size mismatch: expected {}, got {}",
                    part.index, part.len, copied
                )));
            }
        }

        output.flush().await?;

        for part in &self.parts {
            match fs::remove_file(&part.path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(WriterError::Io(error)),
            }
        }

        Ok(())
    }

    async fn cleanup(&mut self) -> Result<(), WriterError> {
        for part in &self.parts {
            match fs::remove_file(&part.path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(WriterError::Io(error)),
            }
        }
        match fs::remove_file(&self.payload_path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(WriterError::Io(error)),
        }
    }
}

/// Owned sequential part stream.
#[derive(Debug)]
pub struct MultiStream {
    file: File,
}

#[async_trait]
impl WriteStream for MultiStream {
    async fn send(&mut self, data: &[u8]) -> Result<(), WriterError> {
        self.file.write_all(data).await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), WriterError> {
        self.file.flush().await?;
        Ok(())
    }
}
