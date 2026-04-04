//! Core type exports intentionally exclude the legacy executable signal surface.
//!
//! ```compile_fail
//! use polyalpha_core::LegacyExecutableSignalSurface;
//! ```

pub mod decimal;
pub mod engine;
pub mod fill;
pub mod hedge;
pub mod market;
pub mod order;
pub mod orderbook;
pub mod planning;
pub mod position;
pub mod position_tracker;
pub mod risk;
pub mod signal;

pub use decimal::{CexBaseQty, PolyShares, Price, Probability, UsdNotional, VenueQuantity};
pub use engine::EngineParams;
pub use fill::Fill;
pub use hedge::{HedgeState, LegStatus, SessionType};
pub use market::{
    cex_venue_symbol, Exchange, MarketConfig, MarketPhase, MarketRule, MarketRuleKind,
    PolymarketIds, SettlementRules, Symbol, SymbolRegistry, TokenSide,
};
pub use order::{
    CexOrderRequest, ClientOrderId, OrderId, OrderRequest, OrderResponse, OrderSide, OrderStatus,
    OrderType, PolyOrderRequest, PolySizingInstruction, TimeInForce,
};
pub use orderbook::{InstrumentKind, OrderBookSnapshot, PriceLevel, Side};
pub use planning::{
    ExecutionResult, OpenCandidate, OrderLedgerEntry, PlanRejectionReason, PlanningBookLevel,
    PlanningDiagnostics, PlanningIntent, RecoveryDecisionReason, ResidualSnapshot,
    RevalidationFailureReason, TradePlan, PLANNING_SCHEMA_VERSION,
};
pub use position::{Position, PositionKey, PositionSide};
pub use position_tracker::{FillEffect, PositionTracker};
pub use risk::{CircuitBreakerStatus, PersistenceLag, RiskRejection};
pub use signal::{
    AlphaEngineOutput, DmmQuoteSlot, DmmQuoteState, DmmQuoteUpdate, EngineWarning, SignalStrength,
};
