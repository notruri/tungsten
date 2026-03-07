use std::fs;
use std::path::{Path, PathBuf};

use tungsten_net::{DownloadId, NetError, PersistedState, ProgressSnapshot, StateStore};

#[derive(Debug, Clone)]
pub struct DiskStateStore {
    path: PathBuf,
}

impl DiskStateStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn write_state_file(&self, state: &PersistedState) -> Result<(), NetError> {
        let serialized = serde_json::to_string_pretty(state)
            .map_err(|error| NetError::State(format!("failed to serialize state: {error}")))?;

        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }

        let temp_path = self.path.with_extension("tmp");
        fs::write(&temp_path, serialized)?;
        fs::rename(temp_path, &self.path)?;
        Ok(())
    }
}

impl StateStore for DiskStateStore {
    fn load_state(&self) -> Result<PersistedState, NetError> {
        if !self.path.exists() {
            return Ok(PersistedState::default());
        }

        let content = fs::read_to_string(&self.path)?;
        let parsed = serde_json::from_str::<PersistedState>(&content)
            .map_err(|error| NetError::State(format!("failed to parse state json: {error}")))?;

        Ok(parsed)
    }

    fn save_state(&self, state: &PersistedState) -> Result<(), NetError> {
        self.write_state_file(state)
    }

    fn checkpoint_progress(
        &self,
        download_id: DownloadId,
        progress: &ProgressSnapshot,
    ) -> Result<(), NetError> {
        let mut state = self.load_state()?;
        for record in &mut state.downloads {
            if record.id == download_id {
                record.progress = progress.clone();
                record.touch();
                break;
            }
        }

        self.write_state_file(&state)
    }
}
