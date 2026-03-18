use std::path::PathBuf;
use std::sync::{Arc, mpsc};

use tungsten_io::DiskStateStore;
use tungsten_net::transport::Transport;

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
        let transfer = Arc::new(Transport::new(config.connections));
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

    pub fn enqueue(&self, request: DownloadRequest) -> Result<DownloadId, RuntimeError> {
        self.queue.enqueue(request)
    }

    pub fn pause(&self, download_id: DownloadId) -> Result<(), RuntimeError> {
        self.queue.pause(download_id)
    }

    pub fn resume(&self, download_id: DownloadId) -> Result<(), RuntimeError> {
        self.queue.resume(download_id)
    }

    pub fn cancel(&self, download_id: DownloadId) -> Result<(), RuntimeError> {
        self.queue.cancel(download_id)
    }

    pub fn remove(&self, download_id: DownloadId) -> Result<(), RuntimeError> {
        self.queue.remove(download_id)
    }

    pub fn queue(&self) -> Arc<QueueService> {
        Arc::clone(&self.queue)
    }

    pub fn set_connections(&self, connections: usize) -> Result<(), RuntimeError> {
        self.queue.set_connections(connections)
    }

    pub fn set_download_limit(&self, download_limit_kbps: u64) -> Result<(), RuntimeError> {
        self.queue.set_download_limit(download_limit_kbps)
    }

    pub fn set_speed_limit(
        &self,
        download_id: DownloadId,
        speed_limit_kbps: Option<u64>,
    ) -> Result<(), RuntimeError> {
        self.queue.set_speed_limit(download_id, speed_limit_kbps)
    }

    pub fn set_fallback_filename(
        &self,
        fallback_filename: impl Into<String>,
    ) -> Result<(), RuntimeError> {
        self.queue.set_fallback_filename(fallback_filename)
    }

    pub fn set_max_parallel(&self, max_parallel: usize) -> Result<(), RuntimeError> {
        self.queue.set_max_parallel(max_parallel)
    }

    pub fn set_temp_root(&self, temp_root: PathBuf) -> Result<(), RuntimeError> {
        self.queue.set_temp_root(temp_root)
    }

    pub fn snapshot(&self) -> Result<Vec<DownloadRecord>, RuntimeError> {
        self.queue.snapshot()
    }

    pub fn subscribe(&self) -> Result<mpsc::Receiver<QueueEvent>, RuntimeError> {
        self.queue.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_config_uses_defaults() {
        let config = RuntimeConfig::new(PathBuf::from("state.db"), 2, 4);

        assert_eq!(config.state_path, PathBuf::from("state.db"));
        assert_eq!(config.max_parallel, 2);
        assert_eq!(config.connections, 4);
        assert_eq!(config.download_limit_kbps, 0);
        assert_eq!(config.fallback_filename, DEFAULT_DOWNLOAD_FILE_NAME);
        assert_eq!(config.temp_root, std::env::temp_dir().join("Tungsten"));
    }

    #[test]
    fn runtime_config_builders_ignore_empty_values() {
        let config = RuntimeConfig::new(PathBuf::from("state.db"), 1, 1)
            .fallback_filename("   ")
            .temp_root(PathBuf::new());

        assert_eq!(config.fallback_filename, DEFAULT_DOWNLOAD_FILE_NAME);
        assert_eq!(config.temp_root, std::env::temp_dir().join("Tungsten"));
    }
}
