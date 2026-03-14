use std::fs;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow};
use gpui::{App, Window};
use gpui_component::{Theme, ThemeMode};
use serde::{Deserialize, Serialize};
use tungsten_runtime::DEFAULT_DOWNLOAD_FILE_NAME;

use crate::paths::resolve_download_dir;

const DEFAULT_MAX_PARALLEL: usize = 3;
const DEFAULT_CONNECTIONS: usize = 4;
const DEFAULT_DOWNLOAD_LIMIT_KBPS: u64 = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppSettings {
    pub download_root: PathBuf,
    pub temp_dir: PathBuf,
    pub fallback_filename: String,
    pub max_parallel: usize,
    pub connections: usize,
    pub download_limit_kbps: u64,
    pub minimize_to_tray: bool,
    pub theme: ThemePreference,
}

impl AppSettings {
    pub fn defaults() -> Result<Self> {
        let download_root = resolve_download_dir()?;
        Ok(Self {
            temp_dir: download_root.join("tmp"),
            download_root,
            fallback_filename: DEFAULT_DOWNLOAD_FILE_NAME.to_string(),
            max_parallel: DEFAULT_MAX_PARALLEL,
            connections: DEFAULT_CONNECTIONS,
            download_limit_kbps: DEFAULT_DOWNLOAD_LIMIT_KBPS,
            minimize_to_tray: false,
            theme: ThemePreference::default(),
        })
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

    pub fn validate(&self) -> Result<()> {
        if self.download_root.as_os_str().is_empty() {
            return Err(anyhow!("download root must not be empty"));
        }
        if self.temp_dir.as_os_str().is_empty() {
            return Err(anyhow!("temp dir must not be empty"));
        }

        let fallback = self.fallback_filename.trim();
        if fallback.is_empty() {
            return Err(anyhow!("fallback filename must not be empty"));
        }
        if fallback.contains('/') || fallback.contains('\\') {
            return Err(anyhow!(
                "fallback filename must not contain path separators"
            ));
        }

        if self.max_parallel == 0 {
            return Err(anyhow!("max_parallel must be at least 1"));
        }
        if self.connections == 0 {
            return Err(anyhow!("connections must be at least 1"));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ThemePreference {
    #[default]
    System,
    Light,
    Dark,
}

impl ThemePreference {
    pub fn apply(self, window: Option<&mut Window>, cx: &mut App) {
        let current = Theme::global(cx);
        let font_family = current.font_family.clone();
        let font_size = current.font_size;
        let mono_font_family = current.mono_font_family.clone();
        let mono_font_size = current.mono_font_size;

        match self {
            Self::System => Theme::sync_system_appearance(window, cx),
            Self::Light => Theme::change(ThemeMode::Light, window, cx),
            Self::Dark => Theme::change(ThemeMode::Dark, window, cx),
        }

        let theme = Theme::global_mut(cx);
        theme.font_family = font_family;
        theme.font_size = font_size;
        theme.mono_font_family = mono_font_family;
        theme.mono_font_size = mono_font_size;
    }

    pub fn all() -> [Self; 3] {
        [Self::System, Self::Light, Self::Dark]
    }

    pub fn key(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Light => "light",
            Self::Dark => "dark",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::System => "System",
            Self::Light => "Light",
            Self::Dark => "Dark",
        }
    }

    pub fn from_key(value: &str) -> Option<Self> {
        match value {
            "system" => Some(Self::System),
            "light" => Some(Self::Light),
            "dark" => Some(Self::Dark),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SettingsStore {
    path: PathBuf,
    current: Arc<Mutex<AppSettings>>,
}

impl SettingsStore {
    pub fn load(path: PathBuf) -> Result<Self> {
        let defaults = AppSettings::defaults()?;
        let loaded = read_settings(&path, &defaults)?;
        loaded.validate()?;
        Ok(Self {
            path,
            current: Arc::new(Mutex::new(loaded)),
        })
    }

    pub fn with_defaults(path: PathBuf) -> Result<Self> {
        let defaults = AppSettings::defaults()?;
        defaults.validate()?;
        Ok(Self {
            path,
            current: Arc::new(Mutex::new(defaults)),
        })
    }

    pub fn current(&self) -> Result<AppSettings> {
        self.current
            .lock()
            .map(|guard| guard.clone())
            .map_err(|error| anyhow!("settings lock poisoned: {error}"))
    }

    pub fn save(&self, settings: AppSettings) -> Result<()> {
        let settings = settings.normalize();
        settings.validate()?;
        write_settings(&self.path, &settings)?;

        let mut guard = self
            .current
            .lock()
            .map_err(|error| anyhow!("settings lock poisoned: {error}"))?;
        *guard = settings;
        Ok(())
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct AppSettingsFile {
    download_root: Option<PathBuf>,
    temp_dir: Option<PathBuf>,
    fallback_filename: Option<String>,
    max_parallel: Option<usize>,
    connections: Option<usize>,
    download_limit_kbps: Option<u64>,
    minimize_to_tray: Option<bool>,
    theme: Option<ThemePreference>,
}

impl AppSettingsFile {
    fn into_settings(self, defaults: &AppSettings) -> AppSettings {
        let download_root = self
            .download_root
            .unwrap_or_else(|| defaults.download_root.clone());

        AppSettings {
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
            minimize_to_tray: self.minimize_to_tray.unwrap_or(defaults.minimize_to_tray),
            theme: self.theme.unwrap_or(defaults.theme),
        }
        .normalize()
    }

    fn from_settings(settings: &AppSettings) -> Self {
        Self {
            download_root: Some(settings.download_root.clone()),
            temp_dir: Some(settings.temp_dir.clone()),
            fallback_filename: Some(settings.fallback_filename.clone()),
            max_parallel: Some(settings.max_parallel),
            connections: Some(settings.connections),
            download_limit_kbps: Some(settings.download_limit_kbps),
            minimize_to_tray: Some(settings.minimize_to_tray),
            theme: Some(settings.theme),
        }
    }
}

fn read_settings(path: &PathBuf, defaults: &AppSettings) -> Result<AppSettings> {
    let content = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(defaults.clone()),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to read {}", path.display()));
        }
    };

    let parsed: AppSettingsFile =
        toml::from_str(&content).with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(parsed.into_settings(defaults))
}

fn write_settings(path: &PathBuf, settings: &AppSettings) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let file = AppSettingsFile::from_settings(settings);
    let content = toml::to_string_pretty(&file).context("failed to serialize settings")?;
    fs::write(path, content).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_round_trip() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let path = temp.path().join("config.toml");
        let store = SettingsStore::with_defaults(path.clone()).expect("defaults should load");

        let settings = AppSettings {
            download_root: temp.path().join("downloads"),
            temp_dir: temp.path().join("downloads").join("tmp"),
            fallback_filename: "fallback.bin".to_string(),
            max_parallel: 5,
            connections: 6,
            download_limit_kbps: 512,
            minimize_to_tray: true,
            theme: ThemePreference::Dark,
        };
        store.save(settings.clone()).expect("settings should save");

        let loaded = SettingsStore::load(path)
            .expect("settings should load")
            .current()
            .expect("settings should read");
        assert_eq!(loaded, settings);
    }

    #[test]
    fn partial_file_uses_defaults() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let path = temp.path().join("config.toml");
        fs::write(&path, "connections = 8\n").expect("partial file should be writable");

        let store = SettingsStore::load(path).expect("settings should load");
        let settings = store.current().expect("settings should read");
        assert_eq!(settings.connections, 8);
        assert!(!settings.download_root.as_os_str().is_empty());
        assert_eq!(settings.temp_dir, settings.download_root.join("tmp"));
        assert_eq!(settings.fallback_filename, DEFAULT_DOWNLOAD_FILE_NAME);
        assert_eq!(settings.max_parallel, DEFAULT_MAX_PARALLEL);
        assert_eq!(settings.download_limit_kbps, DEFAULT_DOWNLOAD_LIMIT_KBPS);
        assert_eq!(settings.theme, ThemePreference::System);
    }

    #[test]
    fn reject_separator_in_fallback_filename() {
        let settings = AppSettings {
            download_root: PathBuf::from("/tmp"),
            temp_dir: PathBuf::from("/tmp/tmp"),
            fallback_filename: "bad/name.bin".to_string(),
            max_parallel: 1,
            connections: 1,
            download_limit_kbps: 0,
            minimize_to_tray: false,
            theme: ThemePreference::System,
        };

        let error = settings
            .validate()
            .expect_err("filename should be rejected");
        assert!(error.to_string().contains("path separators"));
    }

    #[test]
    fn reject_invalid_theme_value() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let path = temp.path().join("config.toml");
        fs::write(&path, "theme = \"neon\"\n").expect("config should be writable");

        let error = SettingsStore::load(path).expect_err("invalid theme should fail");
        assert!(error.to_string().contains("failed to parse"));
    }

    #[test]
    fn theme_keys_round_trip() {
        for theme in ThemePreference::all() {
            assert_eq!(ThemePreference::from_key(theme.key()), Some(theme));
        }
    }
}
