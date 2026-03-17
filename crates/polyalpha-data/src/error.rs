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
}

impl From<Symbol> for DataError {
    fn from(symbol: Symbol) -> Self {
        Self::UnknownSymbol { symbol: symbol.0 }
    }
}
