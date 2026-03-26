use serde::{Deserialize, Serialize};

use super::{Exchange, Price, Symbol, VenueQuantity};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum InstrumentKind {
    PolyYes,
    PolyNo,
    CexPerp,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PriceLevel {
    pub price: Price,
    pub quantity: VenueQuantity,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrderBookSnapshot {
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub instrument: InstrumentKind,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub exchange_timestamp_ms: u64,
    pub received_at_ms: u64,
    pub sequence: u64,
    /// Last trade price from the API, useful when orderbook spread is wide
    pub last_trade_price: Option<Price>,
}
