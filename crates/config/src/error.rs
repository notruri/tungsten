use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to decode toml: {0}")]
    Decode(#[from] toml::de::Error),
    #[error("failed to encode toml: {0}")]
    Encode(#[from] toml::ser::Error),
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    State(String),
    #[error("{name} is not set")]
    MissingEnv { name: &'static str },
}
