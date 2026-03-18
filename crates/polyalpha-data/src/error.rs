use polyalpha_core::{Exchange, Symbol};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, DataError>;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DataError {
    #[error("unknown polymarket asset id: {asset_id}")]
    UnknownPolymarketAsset { asset_id: String },
    #[error("unknown cex symbol mapping: {exchange:?}/{venue_symbol}")]
    UnknownCexSymbol {
        exchange: Exchange,
        venue_symbol: String,
    },
    #[error("unknown symbol in registry: {symbol}")]
    UnknownSymbol { symbol: String },
    #[error("market data source is not connected: {exchange:?}")]
    NotConnected { exchange: Exchange },
    #[error("market data channel is closed")]
    ChannelClosed,
    #[error("http request failed: {0}")]
    Http(String),
    #[error("json parsing failed: {0}")]
    Json(String),
    #[error("decimal parsing failed: {0}")]
    Decimal(String),
    #[error("invalid response payload: {0}")]
    InvalidResponse(String),
}

impl From<Symbol> for DataError {
    fn from(symbol: Symbol) -> Self {
        Self::UnknownSymbol { symbol: symbol.0 }
    }
}

impl From<reqwest::Error> for DataError {
    fn from(value: reqwest::Error) -> Self {
        Self::Http(value.to_string())
    }
}

impl From<serde_json::Error> for DataError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value.to_string())
    }
}
