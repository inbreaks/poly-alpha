use thiserror::Error;

pub type Result<T> = std::result::Result<T, CoreError>;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("configuration error: {0}")]
    Config(#[from] config::ConfigError),
    #[error("failed to convert floating point value for {context}")]
    InvalidFloatConversion { context: &'static str },
    #[error("channel operation failed: {0}")]
    Channel(String),
}
