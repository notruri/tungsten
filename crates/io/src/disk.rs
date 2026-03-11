use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use tracing::debug;
use tungsten_net::NetError;
use tungsten_net::store::{PersistedQueue, QueueStore};

mod codec;
mod db;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone)]
pub struct DiskStateStore {
    path: PathBuf,
    io_lock: Arc<Mutex<()>>,
}

impl DiskStateStore {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            io_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn lock_io(&self) -> Result<MutexGuard<'_, ()>, NetError> {
        self.io_lock
            .lock()
            .map_err(|error| NetError::State(format!("state db lock poisoned: {error}")))
    }

    fn load_queue_unlocked(&self) -> Result<PersistedQueue, NetError> {
        if !self.path.exists() {
            return Ok(PersistedQueue::default());
        }

        db::read_queue(&self.path)
    }

    fn write_queue_db(&self, state: &PersistedQueue) -> Result<(), NetError> {
        db::write_queue(&self.path, state)
    }
}

impl QueueStore for DiskStateStore {
    fn load_queue(&self) -> Result<PersistedQueue, NetError> {
        let _guard = self.lock_io()?;
        let state = self.load_queue_unlocked()?;
        debug!(
            path = %self.path.display(),
            downloads = state.downloads.len(),
            "loaded queue state from disk"
        );
        Ok(state)
    }

    fn save_queue(&self, state: &PersistedQueue) -> Result<(), NetError> {
        let _guard = self.lock_io()?;
        debug!(
            path = %self.path.display(),
            downloads = state.downloads.len(),
            "saving queue state to sqlite"
        );
        self.write_queue_db(state)
    }
}
