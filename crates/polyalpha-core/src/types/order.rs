use serde::{Deserialize, Serialize};

use super::{
    CexBaseQty, Exchange, PolyShares, Price, Symbol, TokenSide, UsdNotional, VenueQuantity,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct OrderId(pub String);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct ClientOrderId(pub String);

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum OrderType {
    Limit,
    Market,
    PostOnly,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum TimeInForce {
    Gtc,
    Ioc,
    Fok,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum OrderStatus {
    Pending,
    Open,
    PartialFill,
    Filled,
    Cancelled,
    Rejected,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum OrderRequest {
    Poly(PolyOrderRequest),
    Cex(CexOrderRequest),
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum PolySizingInstruction {
    BuyBudgetCap {
        max_cost_usd: UsdNotional,
        max_avg_price: Price,
        max_shares: PolyShares,
    },
    SellExactShares {
        shares: PolyShares,
        min_avg_price: Price,
    },
    SellMinProceeds {
        shares: PolyShares,
        min_proceeds_usd: UsdNotional,
    },
}

impl PolySizingInstruction {
    pub fn requested_shares(self) -> PolyShares {
        match self {
            Self::BuyBudgetCap { max_shares, .. } => max_shares,
            Self::SellExactShares { shares, .. } | Self::SellMinProceeds { shares, .. } => shares,
        }
    }

    pub fn boundary_price(self) -> Option<Price> {
        match self {
            Self::BuyBudgetCap { max_avg_price, .. } => Some(max_avg_price),
            Self::SellExactShares { min_avg_price, .. } => Some(min_avg_price),
            Self::SellMinProceeds { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PolyOrderRequest {
    pub client_order_id: ClientOrderId,
    pub symbol: Symbol,
    pub token_side: TokenSide,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub sizing: PolySizingInstruction,
    pub time_in_force: TimeInForce,
    pub post_only: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CexOrderRequest {
    pub client_order_id: ClientOrderId,
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub venue_symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub price: Option<Price>,
    pub base_qty: CexBaseQty,
    pub time_in_force: TimeInForce,
    pub reduce_only: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrderResponse {
    pub client_order_id: ClientOrderId,
    pub exchange_order_id: OrderId,
    pub status: OrderStatus,
    pub filled_quantity: VenueQuantity,
    pub average_price: Option<Price>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejection_reason: Option<String>,
    pub timestamp_ms: u64,
}
