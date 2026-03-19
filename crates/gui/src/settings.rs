use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Result, anyhow};
use gpui::{App, Window};
use gpui_component::{Theme, ThemeMode};
use serde::{Deserialize, Serialize};
use tungsten_config::{ClientPreferences, client_config_path, save_toml};
use tungsten_ipc::{BackendConfig, Client, ThemePreference as RemoteThemePreference};

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
    pub fn normalize(self) -> Self {
        Self::from_parts(self.backend_config().normalize(), self.client_preferences())
    }

    pub fn validate(&self) -> Result<()> {
        self.backend_config()
            .validate()
            .map_err(|error| anyhow!(error.to_string()))
    }

    pub fn from_parts(config: BackendConfig, preferences: ClientPreferences) -> Self {
        Self {
            download_root: config.download_root,
            temp_dir: config.temp_dir,
            fallback_filename: config.fallback_filename,
            max_parallel: config.max_parallel,
            connections: config.connections,
            download_limit_kbps: config.download_limit_kbps,
            minimize_to_tray: preferences.minimize_to_tray,
            theme: preferences.theme.into(),
        }
    }

    pub fn backend_config(&self) -> BackendConfig {
        BackendConfig {
            download_root: self.download_root.clone(),
            temp_dir: self.temp_dir.clone(),
            fallback_filename: self.fallback_filename.clone(),
            max_parallel: self.max_parallel,
            connections: self.connections,
            download_limit_kbps: self.download_limit_kbps,
        }
    }

    pub fn client_preferences(&self) -> ClientPreferences {
        ClientPreferences {
            minimize_to_tray: self.minimize_to_tray,
            theme: self.theme.into(),
        }
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

impl From<RemoteThemePreference> for ThemePreference {
    fn from(value: RemoteThemePreference) -> Self {
        match value {
            RemoteThemePreference::System => Self::System,
            RemoteThemePreference::Light => Self::Light,
            RemoteThemePreference::Dark => Self::Dark,
        }
    }
}

impl From<ThemePreference> for RemoteThemePreference {
    fn from(value: ThemePreference) -> Self {
        match value {
            ThemePreference::System => Self::System,
            ThemePreference::Light => Self::Light,
            ThemePreference::Dark => Self::Dark,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SettingsStore {
    client: Arc<Client>,
    client_path: PathBuf,
    current: Arc<Mutex<AppSettings>>,
}

impl SettingsStore {
    pub fn load(client: Arc<Client>) -> Result<Self> {
        let client_path = client_config_path()?;
        let current =
            AppSettings::from_parts(client.get_config()?, ClientPreferences::load(&client_path)?)
                .normalize();
        current.validate()?;

        Ok(Self {
            client,
            client_path,
            current: Arc::new(Mutex::new(current)),
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
        self.client.set_config(settings.backend_config())?;
        save_toml(&self.client_path, &settings.client_preferences())?;

        let mut guard = self
            .current
            .lock()
            .map_err(|error| anyhow!("settings lock poisoned: {error}"))?;
        *guard = settings;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_settings_round_trip_with_remote_config() {
        let settings = AppSettings {
            download_root: PathBuf::from("downloads"),
            temp_dir: PathBuf::from("downloads/tmp"),
            fallback_filename: "fallback.bin".to_string(),
            max_parallel: 5,
            connections: 6,
            download_limit_kbps: 512,
            minimize_to_tray: true,
            theme: ThemePreference::Dark,
        };

        let restored =
            AppSettings::from_parts(settings.backend_config(), settings.client_preferences());
        assert_eq!(restored, settings);
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
    fn theme_keys_round_trip() {
        for theme in ThemePreference::all() {
            assert_eq!(ThemePreference::from_key(theme.key()), Some(theme));
        }
    }
}
