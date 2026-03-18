use std::ffi::OsString;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use anyhow::{Context, Result, anyhow};
use interprocess::local_socket::{
    GenericFilePath, GenericNamespaced, ListenerOptions, Name, prelude::*,
};
use serde::Deserialize;
use tokio::signal;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use tungsten_daemon::{ConfigStore, Daemon};
use tungsten_ipc::{
    AppConfig, DEFAULT_SOCKET_NAME, IpcError, RequestMessage, ThemePreference, read_frame,
    write_frame,
};
use tungsten_runtime::{DEFAULT_DOWNLOAD_FILE_NAME, Runtime, RuntimeConfig};

const APP_DIR: &str = "Tungsten";
const DEFAULT_DOWNLOADS_DIR: &str = "Tungsten Downloads";
const STATE_FILE: &str = "appstate.db";
const CONFIG_FILE: &str = "config.toml";
const DEFAULT_MAX_PARALLEL: usize = 3;
const DEFAULT_CONNECTIONS: usize = 4;
const DEFAULT_DOWNLOAD_LIMIT_KBPS: u64 = 0;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let config_path = resolve_config_path()?;
    let settings = load_settings(&config_path)?;
    let state_path = resolve_state_path()?;

    initialize_parent(&state_path)?;
    initialize_directory(&settings.temp_dir)?;

    let runtime = Runtime::new(
        RuntimeConfig::new(
            state_path.clone(),
            settings.max_parallel,
            settings.connections,
        )
        .download_limit_kbps(settings.download_limit_kbps)
        .fallback_filename(settings.fallback_filename.clone())
        .temp_root(settings.temp_dir.clone()),
    )
    .with_context(|| format!("failed to initialize runtime at {}", state_path.display()))?;

    let _daemon = Daemon::new(
        Arc::new(runtime),
        ConfigStore::new(config_path, settings.clone()),
    );
    let socket_name = resolve_socket_name()?;
    let listener = ListenerOptions::new()
        .name(socket_name.borrow())
        .try_overwrite(true)
        .create_sync()
        .with_context(|| {
            format!(
                "failed to bind local socket {}",
                socket_display(&socket_name)
            )
        })?;

    info!(
        state_path = %state_path.display(),
        socket = %socket_display(&socket_name),
        max_parallel = settings.max_parallel,
        connections = settings.connections,
        "daemon initialized"
    );

    let daemon = _daemon.clone();
    let accept_loop = thread::spawn(move || run_listener(listener, daemon));

    signal::ctrl_c()
        .await
        .context("failed while waiting for shutdown signal")?;

    info!("shutdown signal received");
    drop(accept_loop);
    Ok(())
}

fn init_tracing() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,tungsten=debug"));

    if let Err(error) = tracing_subscriber::fmt().with_env_filter(filter).try_init() {
        warn!(error = %error, "failed to initialize tracing subscriber");
    }
}

fn default_config() -> Result<AppConfig> {
    let download_root = resolve_download_dir()?;
    Ok(AppConfig {
        download_root: download_root.clone(),
        temp_dir: download_root.join("tmp"),
        fallback_filename: DEFAULT_DOWNLOAD_FILE_NAME.to_string(),
        max_parallel: DEFAULT_MAX_PARALLEL,
        connections: DEFAULT_CONNECTIONS,
        download_limit_kbps: DEFAULT_DOWNLOAD_LIMIT_KBPS,
        minimize_to_tray: false,
        theme: ThemePreference::System,
    })
}

#[derive(Debug, Default, Deserialize)]
struct SettingsFile {
    download_root: Option<PathBuf>,
    temp_dir: Option<PathBuf>,
    fallback_filename: Option<String>,
    max_parallel: Option<usize>,
    connections: Option<usize>,
    download_limit_kbps: Option<u64>,
    minimize_to_tray: Option<bool>,
    theme: Option<ThemePreference>,
}

impl SettingsFile {
    fn into_settings(self, defaults: &AppConfig) -> AppConfig {
        let download_root = self
            .download_root
            .unwrap_or_else(|| defaults.download_root.clone());

        AppConfig {
            download_root: download_root.clone(),
            temp_dir: self.temp_dir.unwrap_or_else(|| download_root.join("tmp")),
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
}

fn load_settings(path: &Path) -> Result<AppConfig> {
    let defaults = default_config()?;
    let loaded = match fs::read_to_string(path) {
        Ok(content) => {
            let parsed: SettingsFile = toml::from_str(&content)
                .with_context(|| format!("failed to parse {}", path.display()))?;
            parsed.into_settings(&defaults)
        }
        Err(error) if error.kind() == ErrorKind::NotFound => defaults,
        Err(error) => {
            return Err(error).with_context(|| format!("failed to read {}", path.display()));
        }
    };

    loaded
        .validate()
        .map_err(|error| anyhow!(error.to_string()))?;
    Ok(loaded)
}

fn initialize_parent(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };

    initialize_directory(parent)
}

fn initialize_directory(path: &Path) -> Result<()> {
    fs::create_dir_all(path).with_context(|| format!("failed to create {}", path.display()))
}

fn run_listener(listener: interprocess::local_socket::Listener, daemon: Daemon) {
    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let daemon = daemon.clone();
                thread::spawn(move || {
                    if let Err(error) = handle_connection(stream, daemon) {
                        error!(error = %error, "connection handler failed");
                    }
                });
            }
            Err(error) => {
                error!(error = %error, "failed to accept incoming connection");
            }
        }
    }
}

fn handle_connection(mut stream: interprocess::local_socket::Stream, daemon: Daemon) -> Result<()> {
    loop {
        let message = match read_frame::<_, RequestMessage>(&mut stream) {
            Ok(message) => message,
            Err(IpcError::Io(error)) if error.kind() == ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error).context("failed to read request frame"),
        };

        let handled = daemon.handle(message);
        write_frame(&mut stream, &handled.response).context("failed to write response frame")?;

        let Some(subscription) = handled.subscription else {
            continue;
        };

        loop {
            let event = match subscription.recv() {
                Ok(event) => event,
                Err(error) => return Err(error).context("subscription closed"),
            };

            write_frame(&mut stream, &event).context("failed to write event frame")?;
        }
    }
}

fn resolve_state_path() -> Result<PathBuf> {
    let state_root = resolve_state_root()?;
    Ok(state_root.join(APP_DIR).join(STATE_FILE))
}

fn resolve_config_path() -> Result<PathBuf> {
    let state_root = resolve_state_root()?;
    Ok(state_root.join(APP_DIR).join(CONFIG_FILE))
}

fn resolve_download_dir() -> Result<PathBuf> {
    let downloads_root = resolve_downloads_root()?;
    Ok(downloads_root.join(DEFAULT_DOWNLOADS_DIR))
}

fn resolve_socket_name() -> Result<Name<'static>> {
    if GenericNamespaced::is_supported() {
        return DEFAULT_SOCKET_NAME
            .to_ns_name::<GenericNamespaced>()
            .map(Name::into_owned)
            .context("failed to build namespaced local socket name");
    }

    let state_root = resolve_state_root()?;
    let socket_path = state_root.join(APP_DIR).join("daemon.sock");
    initialize_parent(&socket_path)?;
    socket_path
        .to_string_lossy()
        .as_ref()
        .to_fs_name::<GenericFilePath>()
        .map(Name::into_owned)
        .with_context(|| {
            format!(
                "failed to build filesystem local socket name for {}",
                socket_path.display()
            )
        })
}

fn socket_display(name: &Name<'_>) -> String {
    format!("{name:?}")
}

fn path_from_var(name: &str, value: Option<OsString>) -> Result<PathBuf> {
    match value {
        Some(value) if !value.is_empty() => Ok(PathBuf::from(value)),
        _ => Err(anyhow!("{name} is not set")),
    }
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
    fn settings_normalize_fills_defaults() {
        let settings = AppConfig {
            download_root: PathBuf::from("downloads"),
            temp_dir: PathBuf::new(),
            fallback_filename: "   ".to_string(),
            max_parallel: 0,
            connections: 0,
            download_limit_kbps: 0,
            minimize_to_tray: false,
            theme: ThemePreference::System,
        }
        .normalize();

        assert_eq!(settings.fallback_filename, DEFAULT_DOWNLOAD_FILE_NAME);
        assert_eq!(settings.max_parallel, 1);
        assert_eq!(settings.connections, 1);
        assert!(!settings.temp_dir.as_os_str().is_empty());
    }

    #[test]
    fn settings_file_overrides_defaults() {
        let defaults = AppConfig {
            download_root: PathBuf::from("downloads/default"),
            temp_dir: PathBuf::from("tmp/default"),
            fallback_filename: "download.bin".to_string(),
            max_parallel: 3,
            connections: 4,
            download_limit_kbps: 0,
            minimize_to_tray: false,
            theme: ThemePreference::System,
        };
        let settings = SettingsFile {
            download_root: Some(PathBuf::from("downloads/custom")),
            temp_dir: Some(PathBuf::from("tmp/custom")),
            fallback_filename: Some("custom.bin".to_string()),
            max_parallel: Some(7),
            connections: Some(8),
            download_limit_kbps: Some(1024),
            minimize_to_tray: Some(true),
            theme: Some(ThemePreference::Dark),
        }
        .into_settings(&defaults);

        assert_eq!(settings.download_root, PathBuf::from("downloads/custom"));
        assert_eq!(settings.temp_dir, PathBuf::from("tmp/custom"));
        assert_eq!(settings.fallback_filename, "custom.bin");
        assert_eq!(settings.max_parallel, 7);
        assert_eq!(settings.connections, 8);
        assert_eq!(settings.download_limit_kbps, 1024);
        assert!(settings.minimize_to_tray);
        assert_eq!(settings.theme, ThemePreference::Dark);
    }

    #[test]
    fn socket_display_is_not_empty() {
        let socket_name = if GenericNamespaced::is_supported() {
            DEFAULT_SOCKET_NAME
                .to_ns_name::<GenericNamespaced>()
                .expect("namespaced socket name should be valid")
        } else {
            "/tmp/tungsten-daemon.sock"
                .to_fs_name::<GenericFilePath>()
                .expect("filesystem socket name should be valid")
        };

        assert!(!socket_display(&socket_name).is_empty());
    }
}
