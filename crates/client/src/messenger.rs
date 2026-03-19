pub use tungsten_core::*;
pub use tungsten_ipc::{BackendConfig, ClientError};

#[derive(Debug)]
pub struct Client {
    inner: tungsten_ipc::Client,
}

impl Client {
    pub fn new() -> Result<Self, ClientError> {
        Ok(Self {
            inner: tungsten_ipc::Client::new()?,
        })
    }

    pub fn enqueue(&self, request: DownloadRequest) -> Result<DownloadId, ClientError> {
        self.inner.enqueue(request)
    }

    pub fn pause(&self, download_id: DownloadId) -> Result<(), ClientError> {
        self.inner.pause(download_id)
    }

    pub fn resume(&self, download_id: DownloadId) -> Result<(), ClientError> {
        self.inner.resume(download_id)
    }

    pub fn cancel(&self, download_id: DownloadId) -> Result<(), ClientError> {
        self.inner.cancel(download_id)
    }

    pub fn remove(&self, download_id: DownloadId) -> Result<(), ClientError> {
        self.inner.remove(download_id)
    }

    pub fn set_connections(&self, connections: usize) -> Result<(), ClientError> {
        self.inner.set_connections(connections)
    }

    pub fn set_download_limit(&self, download_limit_kbps: u64) -> Result<(), ClientError> {
        self.inner.set_download_limit(download_limit_kbps)
    }

    pub fn set_speed_limit(
        &self,
        download_id: DownloadId,
        speed_limit_kbps: Option<u64>,
    ) -> Result<(), ClientError> {
        self.inner.set_speed_limit(download_id, speed_limit_kbps)
    }

    pub fn set_fallback_filename(
        &self,
        fallback_filename: impl Into<String>,
    ) -> Result<(), ClientError> {
        self.inner.set_fallback_filename(fallback_filename)
    }

    pub fn set_max_parallel(&self, max_parallel: usize) -> Result<(), ClientError> {
        self.inner.set_max_parallel(max_parallel)
    }

    pub fn get_config(&self) -> Result<BackendConfig, ClientError> {
        self.inner.get_config()
    }

    pub fn set_config(&self, config: BackendConfig) -> Result<(), ClientError> {
        self.inner.set_config(config)
    }

    pub fn set_temp_root(&self, temp_root: std::path::PathBuf) -> Result<(), ClientError> {
        self.inner.set_temp_root(temp_root)
    }

    pub fn snapshot(&self) -> Result<Vec<DownloadRecord>, ClientError> {
        self.inner.snapshot()
    }

    pub fn subscribe(&self) -> Result<std::sync::mpsc::Receiver<QueueEvent>, ClientError> {
        self.inner.subscribe()
    }
}
