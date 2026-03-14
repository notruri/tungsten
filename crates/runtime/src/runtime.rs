use std::path::PathBuf;
use std::sync::Arc;

use tungsten_io::DiskStateStore;
use tungsten_net::transport::ReqwestTransfer;

pub use tungsten_core::CoreError as RuntimeError;
pub use tungsten_core::*;

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub state_path: PathBuf,
    pub max_parallel: usize,
    pub connections: usize,
    pub download_limit_kbps: u64,
    pub fallback_filename: String,
    pub temp_root: PathBuf,
}

impl RuntimeConfig {
    pub fn new(state_path: PathBuf, max_parallel: usize, connections: usize) -> Self {
        Self {
            state_path,
            max_parallel,
            connections,
            download_limit_kbps: 0,
            fallback_filename: DEFAULT_DOWNLOAD_FILE_NAME.to_string(),
            temp_root: std::env::temp_dir().join("Tungsten"),
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

    pub fn temp_root(mut self, temp_root: PathBuf) -> Self {
        if temp_root.as_os_str().is_empty() {
            return self;
        }

        self.temp_root = temp_root;
        self
    }
}

#[derive(Clone)]
pub struct Runtime {
    queue: Arc<QueueService>,
}

impl Runtime {
    pub fn new(config: RuntimeConfig) -> Result<Self, CoreError> {
        let store = Arc::new(DiskStateStore::new(config.state_path));
        let transfer = Arc::new(ReqwestTransfer::new(config.connections));
        let queue_config = QueueConfig::new(config.max_parallel, config.connections)
            .download_limit_kbps(config.download_limit_kbps)
            .fallback_filename(config.fallback_filename)
            .temp_root(config.temp_root);
        
        let tokio = tokio::runtime::Runtime::new()?;
        let queue = QueueService::new(queue_config, transfer, store, tokio)?;

        Ok(Self {
            queue: Arc::new(queue),
        })
    }

    pub fn queue(&self) -> Arc<QueueService> {
        Arc::clone(&self.queue)
    }
}
