//! Filesystem primitives for Tungsten-managed storage.
//!
//! The [`ChunkFilesystem`] API centralizes how chunked download data is laid
//! out on disk. The current implementation, [`LocalFilesystem`], stores each
//! download in its own session directory under a centralized temp root.
//!
//! Typical flow:
//! 1. Create or resolve a [`ChunkSession`] for a stable key.
//! 2. Build a [`ChunkLayout`] for the target file size and chunk count.
//! 3. Reopen part writers with previously downloaded byte counts.
//! 4. Merge completed parts into the session payload file.
//! 5. Remove the session when the caller no longer needs temp data.

mod chunk;
mod error;
mod local;

pub use chunk::*;
pub use error::*;
pub use local::*;
