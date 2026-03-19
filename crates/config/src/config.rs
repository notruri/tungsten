pub mod app;
pub mod error;
pub mod file;
pub mod path;

pub use app::{
    APP_DIR, AppConfig, AppConfigFile, BackendConfig, BackendConfigFile, ClientPreferences,
    ClientPreferencesFile, DEFAULT_BACKEND_CONFIG_FILE, DEFAULT_CLIENT_CONFIG_FILE,
    DEFAULT_CONNECTIONS, DEFAULT_DOWNLOAD_LIMIT_KBPS, DEFAULT_DOWNLOADS_DIR, DEFAULT_MAX_PARALLEL,
    DEFAULT_SOCKET_FILE, DEFAULT_STATE_FILE, ThemePreference,
};
pub use error::ConfigError;
pub use file::{Store, ensure_directory, ensure_parent, load_optional_toml, load_toml, save_toml};
pub use path::{
    app_socket_path, app_state_dir, app_state_path, backend_config_path, client_config_path,
    default_download_dir, downloads_root, state_root,
};
