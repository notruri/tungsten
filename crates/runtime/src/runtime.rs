use std::path::PathBuf;
use std::sync::Arc;

use tungsten_io::DiskStateStore;
use tungsten_net::queue::QueueConfig;
use tungsten_net::transfer::ReqwestTransfer;

pub use tungsten_net::NetError as RuntimeError;
pub use tungsten_net::QueueService;
pub use tungsten_net::model::{
    ConflictPolicy, DownloadId, DownloadRecord, DownloadRequest, DownloadStatus, IntegrityRule,
    ProgressSnapshot, QueueEvent,
};
pub use tungsten_net::queue::DEFAULT_DOWNLOAD_FILE_NAME;

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub state_path: PathBuf,
    pub max_parallel: usize,
    pub connections: usize,
    pub download_limit_kbps: u64,
    pub fallback_filename: String,
}

impl RuntimeConfig {
    pub fn new(state_path: PathBuf, max_parallel: usize, connections: usize) -> Self {
        Self {
            state_path,
            max_parallel,
            connections,
            download_limit_kbps: 0,
            fallback_filename: DEFAULT_DOWNLOAD_FILE_NAME.to_string(),
        }
    }

    pub fn download_limit_kbps(mut self, download_limit_kbps: u64) -> Self {
        self.download_limit_kbps = download_limit_kbps;
        self
    }

    pub fn fallback_filename(mut self, fallback_filename: impl Into<String>) -> Self {
        let fallback_filename = fallback_filename.into().trim().to_string();
        if fallback_filename.is_empty() {
            return self;
        }

        self.fallback_filename = fallback_filename;
        self
    }
}

#[derive(Clone)]
pub struct Runtime {
    queue: Arc<tungsten_net::QueueService>,
}

impl Runtime {
    pub fn new(config: RuntimeConfig) -> Result<Self, RuntimeError> {
        let store = Arc::new(DiskStateStore::new(config.state_path));
        let transfer = Arc::new(ReqwestTransfer::new(config.connections));
        let queue_config = QueueConfig::new(config.max_parallel, config.connections)
            .download_limit_kbps(config.download_limit_kbps)
            .fallback_filename(config.fallback_filename);
        let queue = tungsten_net::QueueService::with_transfer(queue_config, transfer, store)?;

        Ok(Self {
            queue: Arc::new(queue),
        })
    }

    pub fn queue(&self) -> Arc<tungsten_net::QueueService> {
        Arc::clone(&self.queue)
    }
}
