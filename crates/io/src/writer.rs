//! High-level write backends used by download transports.
//!
//! A [`Writer`] owns the filesystem state for one logical download session.
//! Callers obtain logical producer streams via [`Writer::stream`] and send
//! byte chunks into those streams without caring how they are persisted.
//!
//! The current implementations are:
//! - [`SingleWriter`] for sequential writes into one payload temp file
//! - [`MultiWriter`] for sequential writes into per-stream part files followed
//!   by a merge step during [`Writer::finish`]

mod error;
mod multi;
mod single;

use async_trait::async_trait;

pub use error::WriterError;
pub use multi::{MultiPart, MultiSession, MultiStream, MultiWriter};
pub use single::{SingleSession, SingleStream, SingleWriter};

/// High-level storage backend for one download session.
#[async_trait]
pub trait Writer: Send {
    /// Owned stream type returned to callers that want to send bytes.
    type Stream: WriteStream + Send;

    /// Prepares any filesystem state required before opening streams.
    async fn create(&mut self) -> Result<(), WriterError>;

    /// Opens a logical producer stream identified by `index`.
    async fn stream(&mut self, index: usize) -> Result<Self::Stream, WriterError>;

    /// Flushes any session-level buffers.
    async fn flush(&mut self) -> Result<(), WriterError>;

    /// Finalizes the session.
    ///
    /// For multipart writers this merges part files into the payload path.
    async fn finish(&mut self) -> Result<(), WriterError>;

    /// Removes any temp files owned by the session.
    async fn cleanup(&mut self) -> Result<(), WriterError>;
}

/// Owned byte sink returned by [`Writer::stream`].
#[async_trait]
pub trait WriteStream: Send {
    /// Sends one chunk into the stream.
    async fn send(&mut self, data: &[u8]) -> Result<(), WriterError>;

    /// Flushes the underlying stream.
    async fn flush(&mut self) -> Result<(), WriterError>;
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{
        MultiPart, MultiSession, MultiWriter, SingleSession, SingleWriter, WriteStream, Writer,
    };

    #[tokio::test]
    async fn single_writer_preallocates_and_writes_sequentially() {
        let temp = tempdir().unwrap_or_else(|error| panic!("tempdir should be created: {error}"));
        let path = temp.path().join("single.part");
        let session = SingleSession::open(path.clone())
            .await
            .unwrap_or_else(|error| panic!("single session should open: {error}"));
        let mut writer = SingleWriter::new(session, Some(16), 0);
        writer
            .create()
            .await
            .unwrap_or_else(|error| panic!("single writer should create: {error}"));

        let len = fs::metadata(&path)
            .unwrap_or_else(|error| panic!("single writer metadata should be readable: {error}"))
            .len();
        assert_eq!(len, 16);

        let mut stream = writer
            .stream(0)
            .await
            .unwrap_or_else(|error| panic!("single writer stream should open: {error}"));
        stream
            .send(b"hello world")
            .await
            .unwrap_or_else(|error| panic!("single stream send should succeed: {error}"));
        stream
            .flush()
            .await
            .unwrap_or_else(|error| panic!("single stream flush should succeed: {error}"));

        let data = fs::read(&path)
            .unwrap_or_else(|error| panic!("single writer output should be readable: {error}"));
        assert_eq!(&data[..11], b"hello world");
    }

    #[tokio::test]
    async fn multi_writer_merges_part_streams() {
        let temp = tempdir().unwrap_or_else(|error| panic!("tempdir should be created: {error}"));
        let payload = temp.path().join("payload.part");
        let session = MultiSession::open(
            payload.clone(),
            vec![
                MultiPart {
                    index: 0,
                    path: temp.path().join("payload.part.p0"),
                    len: 5,
                    downloaded: 0,
                },
                MultiPart {
                    index: 1,
                    path: temp.path().join("payload.part.p1"),
                    len: 5,
                    downloaded: 0,
                },
            ],
        )
        .await
        .unwrap_or_else(|error| panic!("multi session should open: {error}"));
        let mut writer = MultiWriter::new(session);

        writer
            .create()
            .await
            .unwrap_or_else(|error| panic!("multi writer should create: {error}"));
        let mut first = writer
            .stream(0)
            .await
            .unwrap_or_else(|error| panic!("first stream should open: {error}"));
        let mut second = writer
            .stream(1)
            .await
            .unwrap_or_else(|error| panic!("second stream should open: {error}"));

        first
            .send(b"hello")
            .await
            .unwrap_or_else(|error| panic!("first stream send should succeed: {error}"));
        second
            .send(b"world")
            .await
            .unwrap_or_else(|error| panic!("second stream send should succeed: {error}"));
        first
            .flush()
            .await
            .unwrap_or_else(|error| panic!("first stream flush should succeed: {error}"));
        second
            .flush()
            .await
            .unwrap_or_else(|error| panic!("second stream flush should succeed: {error}"));

        writer
            .finish()
            .await
            .unwrap_or_else(|error| panic!("multi writer finish should succeed: {error}"));

        let data = fs::read(&payload)
            .unwrap_or_else(|error| panic!("merged payload should be readable: {error}"));
        assert_eq!(data, b"helloworld");
        assert!(!temp.path().join("payload.part.p0").exists());
        assert!(!temp.path().join("payload.part.p1").exists());
    }
}
