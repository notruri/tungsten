pub mod error;
pub mod model;
pub mod queue;
pub mod store;
pub mod transfer;

pub use error::CoreError;
pub use model::{
    ConflictPolicy, DownloadId, DownloadRecord, DownloadRequest, DownloadStatus, IntegrityRule,
    ProgressSnapshot, QueueEvent,
};
pub use queue::{DEFAULT_DOWNLOAD_FILE_NAME, QueueConfig, QueueService};
pub use store::{PersistedDownload, PersistedQueue, QueueStore};
pub use transfer::{
    ControlSignal, MultipartPart, MultipartState, ProbeInfo, TempLayout, Transfer, TransferOutcome,
    TransferTask, TransferUpdate,
};
