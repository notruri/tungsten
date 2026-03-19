use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::ConfigError;

pub fn ensure_directory(path: &Path) -> Result<(), ConfigError> {
    fs::create_dir_all(path)?;
    Ok(())
}

pub fn ensure_parent(path: &Path) -> Result<(), ConfigError> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };

    ensure_directory(parent)
}

pub fn load_toml<T>(path: &Path) -> Result<T, ConfigError>
where
    T: DeserializeOwned,
{
    let content = fs::read_to_string(path)?;
    Ok(toml::from_str(&content)?)
}

pub fn load_optional_toml<T>(path: &Path) -> Result<Option<T>, ConfigError>
where
    T: DeserializeOwned,
{
    match fs::read_to_string(path) {
        Ok(content) => Ok(Some(toml::from_str(&content)?)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub fn save_toml<T>(path: &Path, value: &T) -> Result<(), ConfigError>
where
    T: Serialize,
{
    let content = toml::to_string_pretty(value)?;
    ensure_parent(path)?;
    fs::write(path, content)?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct Store<T> {
    path: PathBuf,
    current: Arc<Mutex<T>>,
}

impl<T> Store<T> {
    pub fn new(path: PathBuf, initial: T) -> Self {
        Self {
            path,
            current: Arc::new(Mutex::new(initial)),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl<T> Store<T>
where
    T: Clone,
{
    pub fn current(&self) -> Result<T, ConfigError> {
        self.current
            .lock()
            .map(|guard| guard.clone())
            .map_err(|error| ConfigError::State(format!("config lock poisoned: {error}")))
    }
}

impl<T> Store<T>
where
    T: Clone + Serialize,
{
    pub fn save(&self, value: T) -> Result<(), ConfigError> {
        save_toml(&self.path, &value)?;

        let mut guard = self
            .current
            .lock()
            .map_err(|error| ConfigError::State(format!("config lock poisoned: {error}")))?;
        *guard = value;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestConfig {
        value: String,
    }

    #[test]
    fn store_save_persists_toml() {
        let dir = TempDir::new().expect("temp dir should be created");
        let path = dir.path().join("config.toml");
        let store = Store::new(
            path.clone(),
            TestConfig {
                value: "before".to_string(),
            },
        );

        store
            .save(TestConfig {
                value: "after".to_string(),
            })
            .expect("store save should succeed");

        let current = store.current().expect("store current should succeed");
        let persisted: TestConfig = load_toml(&path).expect("config file should be readable");

        assert_eq!(current.value, "after");
        assert_eq!(persisted.value, "after");
    }
}
