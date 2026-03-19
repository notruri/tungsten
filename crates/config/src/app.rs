use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tungsten_core::{CoreError, DEFAULT_DOWNLOAD_FILE_NAME};

use crate::{ConfigError, default_download_dir, load_optional_toml};

pub const APP_DIR: &str = "Tungsten";
pub const DEFAULT_DOWNLOADS_DIR: &str = "Tungsten Downloads";
pub const DEFAULT_BACKEND_CONFIG_FILE: &str = "backend.toml";
pub const DEFAULT_CLIENT_CONFIG_FILE: &str = "client.toml";
pub const DEFAULT_STATE_FILE: &str = "appstate.db";
pub const DEFAULT_SOCKET_FILE: &str = "tungsten.sock";
pub const DEFAULT_MAX_PARALLEL: usize = 3;
pub const DEFAULT_CONNECTIONS: usize = 4;
pub const DEFAULT_DOWNLOAD_LIMIT_KBPS: u64 = 0;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThemePreference {
    #[default]
    System,
    Light,
    Dark,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(flatten)]
    pub backend: BackendConfig,
    #[serde(flatten)]
    pub client: ClientPreferences,
}

impl AppConfig {
    pub fn new(download_root: PathBuf) -> Self {
        Self {
            backend: BackendConfig::new(download_root),
            client: ClientPreferences::default(),
        }
    }

    pub fn defaults() -> Result<Self, ConfigError> {
        Ok(Self::new(default_download_dir()?))
    }

    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let defaults = Self::defaults()?;
        let loaded = match load_optional_toml::<AppConfigFile>(path)? {
            Some(file) => file.into_config(&defaults),
            None => defaults,
        };

        loaded
            .validate()
            .map_err(|error| ConfigError::Invalid(error.to_string()))?;
        Ok(loaded)
    }

    pub fn normalize(mut self) -> Self {
        self.backend = self.backend.normalize();
        self
    }

    pub fn validate(&self) -> Result<(), CoreError> {
        self.backend.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendConfig {
    pub download_root: PathBuf,
    pub temp_dir: PathBuf,
    pub fallback_filename: String,
    pub max_parallel: usize,
    pub connections: usize,
    pub download_limit_kbps: u64,
}

impl BackendConfig {
    pub fn new(download_root: PathBuf) -> Self {
        Self {
            temp_dir: download_root.join("tmp"),
            download_root,
            fallback_filename: DEFAULT_DOWNLOAD_FILE_NAME.to_string(),
            max_parallel: DEFAULT_MAX_PARALLEL,
            connections: DEFAULT_CONNECTIONS,
            download_limit_kbps: DEFAULT_DOWNLOAD_LIMIT_KBPS,
        }
    }

    pub fn defaults() -> Result<Self, ConfigError> {
        Ok(Self::new(default_download_dir()?))
    }

    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let defaults = Self::defaults()?;
        let loaded = match load_optional_toml::<BackendConfigFile>(path)? {
            Some(file) => file.into_config(&defaults),
            None => defaults,
        };

        loaded
            .validate()
            .map_err(|error| ConfigError::Invalid(error.to_string()))?;
        Ok(loaded)
    }

    pub fn normalize(mut self) -> Self {
        if self.temp_dir.as_os_str().is_empty() {
            self.temp_dir = self.download_root.join("tmp");
        }

        self.fallback_filename = self.fallback_filename.trim().to_string();
        if self.fallback_filename.is_empty() {
            self.fallback_filename = DEFAULT_DOWNLOAD_FILE_NAME.to_string();
        }

        self.max_parallel = self.max_parallel.max(1);
        self.connections = self.connections.max(1);
        self
    }

    pub fn validate(&self) -> Result<(), CoreError> {
        if self.download_root.as_os_str().is_empty() {
            return Err(CoreError::InvalidRequest(
                "download root must not be empty".to_string(),
            ));
        }
        if self.temp_dir.as_os_str().is_empty() {
            return Err(CoreError::InvalidRequest(
                "temp dir must not be empty".to_string(),
            ));
        }

        let fallback = self.fallback_filename.trim();
        if fallback.is_empty() {
            return Err(CoreError::InvalidRequest(
                "fallback filename must not be empty".to_string(),
            ));
        }
        if fallback.contains('/') || fallback.contains('\\') {
            return Err(CoreError::InvalidRequest(
                "fallback filename must not contain path separators".to_string(),
            ));
        }

        if self.max_parallel == 0 {
            return Err(CoreError::InvalidRequest(
                "max_parallel must be at least 1".to_string(),
            ));
        }
        if self.connections == 0 {
            return Err(CoreError::InvalidRequest(
                "connections must be at least 1".to_string(),
            ));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientPreferences {
    pub minimize_to_tray: bool,
    pub theme: ThemePreference,
}

impl ClientPreferences {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let defaults = Self::default();
        let loaded = match load_optional_toml::<ClientPreferencesFile>(path)? {
            Some(file) => file.into_config(&defaults),
            None => defaults,
        };
        Ok(loaded)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppConfigFile {
    #[serde(flatten, default)]
    pub backend: BackendConfigFile,
    #[serde(flatten, default)]
    pub client: ClientPreferencesFile,
}

impl AppConfigFile {
    pub fn into_config(self, defaults: &AppConfig) -> AppConfig {
        AppConfig {
            backend: self.backend.into_config(&defaults.backend),
            client: self.client.into_config(&defaults.client),
        }
        .normalize()
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendConfigFile {
    pub download_root: Option<PathBuf>,
    pub temp_dir: Option<PathBuf>,
    pub fallback_filename: Option<String>,
    pub max_parallel: Option<usize>,
    pub connections: Option<usize>,
    pub download_limit_kbps: Option<u64>,
}

impl BackendConfigFile {
    pub fn into_config(self, defaults: &BackendConfig) -> BackendConfig {
        let download_root = self
            .download_root
            .unwrap_or_else(|| defaults.download_root.clone());

        BackendConfig {
            temp_dir: self.temp_dir.unwrap_or_else(|| download_root.join("tmp")),
            download_root,
            fallback_filename: self
                .fallback_filename
                .unwrap_or_else(|| defaults.fallback_filename.clone()),
            max_parallel: self.max_parallel.unwrap_or(defaults.max_parallel),
            connections: self.connections.unwrap_or(defaults.connections),
            download_limit_kbps: self
                .download_limit_kbps
                .unwrap_or(defaults.download_limit_kbps),
        }
        .normalize()
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientPreferencesFile {
    pub minimize_to_tray: Option<bool>,
    pub theme: Option<ThemePreference>,
}

impl ClientPreferencesFile {
    pub fn into_config(self, defaults: &ClientPreferences) -> ClientPreferences {
        ClientPreferences {
            minimize_to_tray: self.minimize_to_tray.unwrap_or(defaults.minimize_to_tray),
            theme: self.theme.unwrap_or(defaults.theme),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_config_file_overrides_defaults() {
        let defaults = AppConfig::new(PathBuf::from("downloads/default"));
        let config = AppConfigFile {
            backend: BackendConfigFile {
                download_root: Some(PathBuf::from("downloads/custom")),
                temp_dir: Some(PathBuf::from("tmp/custom")),
                fallback_filename: Some("custom.bin".to_string()),
                max_parallel: Some(7),
                connections: Some(8),
                download_limit_kbps: Some(1024),
            },
            client: ClientPreferencesFile {
                minimize_to_tray: Some(true),
                theme: Some(ThemePreference::Dark),
            },
        }
        .into_config(&defaults);

        assert_eq!(
            config.backend.download_root,
            PathBuf::from("downloads/custom")
        );
        assert_eq!(config.backend.temp_dir, PathBuf::from("tmp/custom"));
        assert_eq!(config.backend.fallback_filename, "custom.bin");
        assert_eq!(config.backend.max_parallel, 7);
        assert_eq!(config.backend.connections, 8);
        assert_eq!(config.backend.download_limit_kbps, 1024);
        assert!(config.client.minimize_to_tray);
        assert_eq!(config.client.theme, ThemePreference::Dark);
    }

    #[test]
    fn normalize_fills_backend_defaults() {
        let config = AppConfig {
            backend: BackendConfig {
                download_root: PathBuf::from("downloads"),
                temp_dir: PathBuf::new(),
                fallback_filename: "   ".to_string(),
                max_parallel: 0,
                connections: 0,
                download_limit_kbps: 0,
            },
            client: ClientPreferences::default(),
        }
        .normalize();

        assert_eq!(config.backend.fallback_filename, DEFAULT_DOWNLOAD_FILE_NAME);
        assert_eq!(config.backend.max_parallel, 1);
        assert_eq!(config.backend.connections, 1);
        assert_eq!(
            config.backend.temp_dir,
            PathBuf::from("downloads").join("tmp")
        );
    }
}
