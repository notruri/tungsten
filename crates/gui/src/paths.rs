use std::ffi::OsString;
use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};

const APP_DIR: &str = "Tungsten";
const DEFAULT_DOWNLOADS_DIR: &str = "Tungsten Downloads";
const STATE_FILE: &str = "appstate.db";

pub fn resolve_state_path() -> Result<PathBuf> {
    let state_root = resolve_state_root()?;
    Ok(state_path_from(&state_root))
}

pub fn resolve_download_dir() -> Result<PathBuf> {
    let downloads_root = resolve_downloads_root()?;
    Ok(download_dir_from(&downloads_root))
}

fn path_from_var(name: &str, value: Option<OsString>) -> Result<PathBuf> {
    match value {
        Some(value) if !value.is_empty() => Ok(PathBuf::from(value)),
        _ => Err(anyhow!("{name} is not set")),
    }
}

fn state_path_from(appdata: &Path) -> PathBuf {
    appdata.join(APP_DIR).join(STATE_FILE)
}

fn download_dir_from(downloads_root: &Path) -> PathBuf {
    downloads_root.join(DEFAULT_DOWNLOADS_DIR)
}

#[cfg(target_os = "windows")]
fn resolve_state_root() -> Result<PathBuf> {
    path_from_var("APPDATA", std::env::var_os("APPDATA"))
}

#[cfg(not(target_os = "windows"))]
fn resolve_state_root() -> Result<PathBuf> {
    match std::env::var_os("XDG_STATE_HOME") {
        Some(value) if !value.is_empty() => Ok(PathBuf::from(value)),
        _ => {
            let home = path_from_var("HOME", std::env::var_os("HOME"))?;
            Ok(home.join(".local").join("state"))
        }
    }
}

#[cfg(target_os = "windows")]
fn resolve_downloads_root() -> Result<PathBuf> {
    let userprofile = path_from_var("USERPROFILE", std::env::var_os("USERPROFILE"))?;
    Ok(userprofile.join("Downloads"))
}

#[cfg(not(target_os = "windows"))]
fn resolve_downloads_root() -> Result<PathBuf> {
    path_from_var("HOME", std::env::var_os("HOME"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(target_os = "windows")]
    fn state_path_uses_appdata_root() {
        let path = state_path_from(Path::new(r"C:\Users\Name\AppData\Roaming"));
        assert_eq!(
            path,
            PathBuf::from(r"C:\Users\Name\AppData\Roaming\Tungsten\appstate.db")
        );
    }

    #[test]
    #[cfg(not(target_os = "windows"))]
    fn state_path_uses_xdg_state_root() {
        let path = state_path_from(Path::new("/home/name/.local/state"));
        assert_eq!(
            path,
            PathBuf::from("/home/name/.local/state/Tungsten/appstate.db")
        );
    }

    #[test]
    #[cfg(target_os = "windows")]
    fn download_dir_uses_userprofile_downloads_root() {
        let path = download_dir_from(Path::new(r"C:\Users\Name\Downloads"));
        assert_eq!(
            path,
            PathBuf::from(r"C:\Users\Name\Downloads\Tungsten Downloads")
        );
    }

    #[test]
    #[cfg(not(target_os = "windows"))]
    fn download_dir_uses_home_root() {
        let path = download_dir_from(Path::new("/home/name"));
        assert_eq!(path, PathBuf::from("/home/name/Tungsten Downloads"));
    }

    #[test]
    #[cfg(target_os = "windows")]
    fn missing_appdata_returns_error() {
        let error = path_from_var("APPDATA", None).unwrap_err();
        assert_eq!(error.to_string(), "APPDATA is not set");
    }

    #[test]
    #[cfg(target_os = "windows")]
    fn missing_userprofile_returns_error() {
        let error = path_from_var("USERPROFILE", None).unwrap_err();
        assert_eq!(error.to_string(), "USERPROFILE is not set");
    }

    #[test]
    #[cfg(not(target_os = "windows"))]
    fn missing_home_returns_error() {
        let error = path_from_var("HOME", None).unwrap_err();
        assert_eq!(error.to_string(), "HOME is not set");
    }
}
