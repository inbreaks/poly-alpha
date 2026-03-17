use serde::{Deserialize, Serialize};

use super::{Exchange, InstrumentKind, OrderId, OrderSide, Price, Symbol, UsdNotional, VenueQuantity};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Fill {
    pub fill_id: String,
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub instrument: InstrumentKind,
    pub order_id: OrderId,
    pub side: OrderSide,
    pub price: Price,
    pub quantity: VenueQuantity,
    pub notional_usd: UsdNotional,
    pub fee: UsdNotional,
    pub is_maker: bool,
    pub timestamp_ms: u64,
}
