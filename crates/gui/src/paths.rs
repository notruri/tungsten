use std::ffi::OsString;
use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};

const APP_DIR: &str = "Tungsten";
const DOWNLOADS_DIR: &str = "Downloads";
const DEFAULT_DOWNLOADS_DIR: &str = "Tungsten Downloads";
const STATE_FILE: &str = "state.json";

pub fn resolve_state_path() -> Result<PathBuf> {
    let appdata = path_from_var("APPDATA", std::env::var_os("APPDATA"))?;
    Ok(state_path_from(&appdata))
}

pub fn resolve_download_dir() -> Result<PathBuf> {
    let userprofile = path_from_var("USERPROFILE", std::env::var_os("USERPROFILE"))?;
    Ok(download_dir_from(&userprofile))
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

fn download_dir_from(userprofile: &Path) -> PathBuf {
    userprofile.join(DOWNLOADS_DIR).join(DEFAULT_DOWNLOADS_DIR)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_path_uses_appdata_root() {
        let path = state_path_from(Path::new(r"C:\Users\Name\AppData\Roaming"));
        assert_eq!(
            path,
            PathBuf::from(r"C:\Users\Name\AppData\Roaming\Tungsten\state.json")
        );
    }

    #[test]
    fn download_dir_uses_userprofile_downloads_root() {
        let path = download_dir_from(Path::new(r"C:\Users\Name"));
        assert_eq!(
            path,
            PathBuf::from(r"C:\Users\Name\Downloads\Tungsten Downloads")
        );
    }

    #[test]
    fn missing_appdata_returns_error() {
        let error = path_from_var("APPDATA", None).unwrap_err();
        assert_eq!(error.to_string(), "APPDATA is not set");
    }

    #[test]
    fn missing_userprofile_returns_error() {
        let error = path_from_var("USERPROFILE", None).unwrap_err();
        assert_eq!(error.to_string(), "USERPROFILE is not set");
    }
}
