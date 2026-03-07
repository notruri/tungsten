use serde::{Deserialize, Serialize};

use crate::error::NetError;
use crate::types::{DownloadId, DownloadRecord, ProgressSnapshot};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PersistedState {
    pub next_id: u64,
    pub downloads: Vec<DownloadRecord>,
}

pub trait StateStore: Send + Sync {
    fn load_state(&self) -> Result<PersistedState, NetError>;
    fn save_state(&self, state: &PersistedState) -> Result<(), NetError>;
    fn checkpoint_progress(
        &self,
        download_id: DownloadId,
        progress: &ProgressSnapshot,
    ) -> Result<(), NetError>;
}
