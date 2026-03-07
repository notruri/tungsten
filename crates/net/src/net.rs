pub mod backend;
pub mod error;
pub mod queue;
pub mod state;
pub mod types;

pub use backend::{ControlSignal, DownloadBackend, DownloadTask, ProbeInfo, ReqwestBackend};
pub use error::NetError;
pub use queue::QueueService;
pub use state::{PersistedState, StateStore};
pub use types::{
    ConflictPolicy, DownloadId, DownloadRecord, DownloadRequest, DownloadStatus, IntegrityRule,
    ProgressSnapshot, QueueEvent,
};
