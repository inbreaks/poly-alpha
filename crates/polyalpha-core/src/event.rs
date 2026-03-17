use std::collections::HashMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::types::{
    CircuitBreakerStatus, Exchange, Fill, HedgeState, InstrumentKind, OrderId, OrderResponse,
    OrderSide, Position, PositionKey, Price, Symbol, UsdNotional, VenueQuantity,
};

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConnectionStatus {
    Connecting,
    #[default]
    Connected,
    Disconnected,
    Reconnecting,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum MarketDataEvent {
    OrderBookUpdate {
        snapshot: crate::types::OrderBookSnapshot,
    },
    TradeUpdate {
        exchange: Exchange,
        symbol: Symbol,
        instrument: InstrumentKind,
        price: Price,
        quantity: VenueQuantity,
        side: OrderSide,
        timestamp_ms: u64,
    },
    FundingRate {
        exchange: Exchange,
        symbol: Symbol,
        rate: Decimal,
        next_funding_time_ms: u64,
    },
    MarketLifecycle {
        symbol: Symbol,
        phase: crate::types::MarketPhase,
        timestamp_ms: u64,
    },
    ConnectionEvent {
        exchange: Exchange,
        status: ConnectionStatus,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExecutionEvent {
    OrderSubmitted {
        symbol: Symbol,
        exchange: Exchange,
        response: OrderResponse,
    },
    OrderFilled(Fill),
    OrderCancelled {
        symbol: Symbol,
        order_id: OrderId,
        exchange: Exchange,
    },
    HedgeStateChanged {
        symbol: Symbol,
        session_id: Uuid,
        old_state: HedgeState,
        new_state: HedgeState,
        timestamp_ms: u64,
    },
    ReconcileRequired {
        symbol: Option<Symbol>,
        reason: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SystemCommand {
    Shutdown,
    PauseStrategy(Symbol),
    ResumeStrategy(Symbol),
    TriggerCircuitBreaker(String),
    ResetCircuitBreaker,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RiskStateSnapshot {
    pub circuit_breaker: CircuitBreakerStatus,
    pub total_exposure_usd: UsdNotional,
    pub positions: HashMap<PositionKey, Position>,
    pub daily_pnl: UsdNotional,
    pub max_drawdown_pct: Decimal,
    pub persistence_lag_secs: u64,
    pub timestamp_ms: u64,
}
