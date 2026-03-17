use serde::{Deserialize, Serialize};

use super::{Exchange, InstrumentKind, Price, Symbol, UsdNotional, VenueQuantity};

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum PositionSide {
    Long,
    Short,
    #[default]
    Flat,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PositionKey {
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub instrument: InstrumentKind,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Position {
    pub key: PositionKey,
    pub side: PositionSide,
    pub quantity: VenueQuantity,
    pub entry_price: Price,
    pub entry_notional: UsdNotional,
    pub unrealized_pnl: UsdNotional,
    pub realized_pnl: UsdNotional,
}
