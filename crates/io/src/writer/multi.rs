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

#[derive(Debug)]
struct SessionPart {
    part: MultiPart,
    file: Option<File>,
}

/// Prepared multipart temp session.
#[derive(Debug)]
pub struct MultiSession {
    payload_path: PathBuf,
    payload: Option<File>,
    parts: Vec<SessionPart>,
}

impl MultiSession {
    /// Opens all temp files once and derives resume state from those handles.
    pub async fn open(payload_path: PathBuf, parts: Vec<MultiPart>) -> Result<Self, WriterError> {
        if let Some(parent) = payload_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let payload = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&payload_path)
            .await?;

        let mut prepared_parts = Vec::with_capacity(parts.len());
        for mut part in parts {
            if let Some(parent) = part.path.parent() {
                fs::create_dir_all(parent).await?;
            }

            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(&part.path)
                .await?;
            if part.downloaded == 0 {
                let size = file.metadata().await?.len();
                part.downloaded = if size <= part.len {
                    size
                } else {
                    file.set_len(0).await?;
                    0
                };
            } else {
                part.downloaded = part.downloaded.min(part.len);
            }

            prepared_parts.push(SessionPart {
                part,
                file: Some(file),
            });
        }

        Ok(Self {
            payload_path,
            payload: Some(payload),
            parts: prepared_parts,
        })
    }

    pub fn parts(&self) -> Vec<MultiPart> {
        self.parts.iter().map(|part| part.part.clone()).collect()
    }

    fn close(&mut self) {
        self.payload = None;
        for part in &mut self.parts {
            part.file = None;
        }
    }
}

/// Multipart writer backed by one file per stream.
#[derive(Debug)]
pub struct MultiWriter {
    session: MultiSession,
    created: bool,
    opened: Vec<bool>,
}

impl MultiWriter {
    /// Builds a multipart writer.
    pub fn new(session: MultiSession) -> Self {
        let opened = vec![false; session.parts.len()];
        Self {
            session,
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

        self.created = true;
        Ok(())
    }

    async fn stream(&mut self, index: usize) -> Result<Self::Stream, WriterError> {
        if !self.created {
            return Err(WriterError::State(
                "multi writer must be created before opening a stream".to_string(),
            ));
        }

        let Some(part) = self.session.parts.get(index).map(|part| part.part.clone()) else {
            return Err(WriterError::InvalidStream(index));
        };
        let Some(opened) = self.opened.get(index).copied() else {
            return Err(WriterError::InvalidStream(index));
        };
        if opened {
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

        let file = self
            .session
            .parts
            .get(index)
            .and_then(|part| part.file.as_ref())
            .ok_or_else(|| {
                WriterError::State(format!("multi session stream {index} file is unavailable"))
            })?
            .try_clone()
            .await?;
        let mut file = file;
        file.set_len(part.downloaded).await?;
        file.seek(std::io::SeekFrom::Start(part.downloaded)).await?;
        self.opened[index] = true;

        Ok(MultiStream { file })
    }

    async fn flush(&mut self) -> Result<(), WriterError> {
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), WriterError> {
        let mut output = self.session.payload.take().ok_or_else(|| {
            WriterError::State("multipart payload file is unavailable".to_string())
        })?;
        output.set_len(0).await?;
        output.seek(std::io::SeekFrom::Start(0)).await?;
        let mut buffer = vec![0u8; BUFFER_SIZE];

        for part in &mut self.session.parts {
            let input = part.file.as_mut().ok_or_else(|| {
                WriterError::State(format!("stream {} file is unavailable", part.part.index))
            })?;
            input.seek(std::io::SeekFrom::Start(0)).await?;
            let mut copied = 0u64;

            loop {
                let read = input.read(&mut buffer).await?;
                if read == 0 {
                    break;
                }
                output.write_all(&buffer[..read]).await?;
                copied += read as u64;
            }

            if copied != part.part.len {
                return Err(WriterError::State(format!(
                    "stream {} size mismatch: expected {}, got {}",
                    part.part.index, part.part.len, copied
                )));
            }
        }

        output.flush().await?;
        self.session.payload = Some(output);

        for part in &mut self.session.parts {
            part.file = None;
            match fs::remove_file(&part.part.path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(WriterError::Io(error)),
            }
        }

        Ok(())
    }

    async fn cleanup(&mut self) -> Result<(), WriterError> {
        self.session.close();
        for part in &self.session.parts {
            match fs::remove_file(&part.part.path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(WriterError::Io(error)),
            }
        }
        match fs::remove_file(&self.session.payload_path).await {
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
