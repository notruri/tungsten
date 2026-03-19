use std::ffi::OsString;
use std::path::PathBuf;

use crate::{
    APP_DIR, ConfigError, DEFAULT_BACKEND_CONFIG_FILE, DEFAULT_CLIENT_CONFIG_FILE,
    DEFAULT_DOWNLOADS_DIR, DEFAULT_SOCKET_FILE, DEFAULT_STATE_FILE,
};

pub fn state_root() -> Result<PathBuf, ConfigError> {
    state_root_impl()
}

pub fn app_state_dir() -> Result<PathBuf, ConfigError> {
    Ok(state_root()?.join(APP_DIR))
}

pub fn app_state_path() -> Result<PathBuf, ConfigError> {
    Ok(app_state_dir()?.join(DEFAULT_STATE_FILE))
}

pub fn backend_config_path() -> Result<PathBuf, ConfigError> {
    Ok(app_state_dir()?.join(DEFAULT_BACKEND_CONFIG_FILE))
}

pub fn client_config_path() -> Result<PathBuf, ConfigError> {
    Ok(app_state_dir()?.join(DEFAULT_CLIENT_CONFIG_FILE))
}

pub fn app_socket_path() -> Result<PathBuf, ConfigError> {
    Ok(app_state_dir()?.join(DEFAULT_SOCKET_FILE))
}

pub fn downloads_root() -> Result<PathBuf, ConfigError> {
    downloads_root_impl()
}

pub fn default_download_dir() -> Result<PathBuf, ConfigError> {
    Ok(downloads_root()?.join(DEFAULT_DOWNLOADS_DIR))
}

fn path_from_var(name: &'static str, value: Option<OsString>) -> Result<PathBuf, ConfigError> {
    match value {
        Some(value) if !value.is_empty() => Ok(PathBuf::from(value)),
        _ => Err(ConfigError::MissingEnv { name }),
    }
}

#[cfg(target_os = "windows")]
fn state_root_impl() -> Result<PathBuf, ConfigError> {
    path_from_var("APPDATA", std::env::var_os("APPDATA"))
}

#[cfg(not(target_os = "windows"))]
fn state_root_impl() -> Result<PathBuf, ConfigError> {
    match std::env::var_os("XDG_STATE_HOME") {
        Some(value) if !value.is_empty() => Ok(PathBuf::from(value)),
        _ => {
            let home = path_from_var("HOME", std::env::var_os("HOME"))?;
            Ok(home.join(".local").join("state"))
        }
    }
}

#[cfg(target_os = "windows")]
fn downloads_root_impl() -> Result<PathBuf, ConfigError> {
    let userprofile = path_from_var("USERPROFILE", std::env::var_os("USERPROFILE"))?;
    Ok(userprofile.join("Downloads"))
}

#[cfg(not(target_os = "windows"))]
fn downloads_root_impl() -> Result<PathBuf, ConfigError> {
    path_from_var("HOME", std::env::var_os("HOME"))
}
