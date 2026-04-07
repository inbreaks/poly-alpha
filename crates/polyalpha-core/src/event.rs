use std::collections::HashMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::types::{
    CircuitBreakerStatus, Exchange, ExecutionResult, Fill, HedgeState, InstrumentKind, OrderId,
    OrderResponse, OrderSide, Position, PositionKey, Price, PriceLevel, Symbol, SymbolRegistry,
    TradePlan, UsdNotional, VenueQuantity,
};

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConnectionStatus {
    #[default]
    Connecting,
    Connected,
    Disconnected,
    Reconnecting,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum MarketDataEvent {
    OrderBookUpdate {
        snapshot: crate::types::OrderBookSnapshot,
    },
    CexVenueOrderBookUpdate {
        exchange: Exchange,
        venue_symbol: String,
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
        exchange_timestamp_ms: u64,
        received_at_ms: u64,
        sequence: u64,
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
    CexVenueTradeUpdate {
        exchange: Exchange,
        venue_symbol: String,
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
    CexVenueFundingRate {
        exchange: Exchange,
        venue_symbol: String,
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

impl MarketDataEvent {
    pub fn expand_for_registry(&self, registry: &SymbolRegistry) -> Vec<Self> {
        self.clone().into_expanded_for_registry(registry)
    }

    pub fn into_expanded_for_registry(self, registry: &SymbolRegistry) -> Vec<Self> {
        match self {
            Self::CexVenueOrderBookUpdate {
                exchange,
                venue_symbol,
                bids,
                asks,
                exchange_timestamp_ms,
                received_at_ms,
                sequence,
            } => {
                let symbols = registry.lookup_cex_symbols(exchange, &venue_symbol);
                debug_assert!(
                    symbols.is_some(),
                    "missing registry mapping for CEX venue event: {:?} {}",
                    exchange,
                    venue_symbol
                );
                symbols
                    .unwrap_or(&[])
                    .iter()
                    .map(|symbol| Self::OrderBookUpdate {
                        snapshot: crate::types::OrderBookSnapshot {
                            exchange,
                            symbol: symbol.clone(),
                            instrument: InstrumentKind::CexPerp,
                            bids: bids.clone(),
                            asks: asks.clone(),
                            exchange_timestamp_ms,
                            received_at_ms,
                            sequence,
                            last_trade_price: None,
                        },
                    })
                    .collect()
            }
            Self::CexVenueTradeUpdate {
                exchange,
                venue_symbol,
                price,
                quantity,
                side,
                timestamp_ms,
            } => {
                let symbols = registry.lookup_cex_symbols(exchange, &venue_symbol);
                debug_assert!(
                    symbols.is_some(),
                    "missing registry mapping for CEX venue event: {:?} {}",
                    exchange,
                    venue_symbol
                );
                symbols
                    .unwrap_or(&[])
                    .iter()
                    .map(|symbol| Self::TradeUpdate {
                        exchange,
                        symbol: symbol.clone(),
                        instrument: InstrumentKind::CexPerp,
                        price,
                        quantity,
                        side,
                        timestamp_ms,
                    })
                    .collect()
            }
            Self::CexVenueFundingRate {
                exchange,
                venue_symbol,
                rate,
                next_funding_time_ms,
            } => {
                let symbols = registry.lookup_cex_symbols(exchange, &venue_symbol);
                debug_assert!(
                    symbols.is_some(),
                    "missing registry mapping for CEX venue event: {:?} {}",
                    exchange,
                    venue_symbol
                );
                symbols
                    .unwrap_or(&[])
                    .iter()
                    .map(|symbol| Self::FundingRate {
                        exchange,
                        symbol: symbol.clone(),
                        rate,
                        next_funding_time_ms,
                    })
                    .collect()
            }
            other => vec![other],
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ExecutionEvent {
    TradePlanCreated {
        plan: TradePlan,
    },
    PlanSuperseded {
        symbol: Symbol,
        superseded_plan_id: String,
        next_plan_id: String,
    },
    RecoveryPlanCreated {
        plan: TradePlan,
    },
    ExecutionResultRecorded {
        result: ExecutionResult,
    },
    OrderSubmitted {
        symbol: Symbol,
        exchange: Exchange,
        response: OrderResponse,
        correlation_id: String,
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
    TradeClosed {
        symbol: Symbol,
        correlation_id: String,
        realized_pnl: UsdNotional,
        timestamp_ms: u64,
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

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;
    use crate::types::{CexBaseQty, MarketConfig, PolymarketIds, PriceLevel, SymbolRegistry};

    fn shared_cex_registry() -> SymbolRegistry {
        SymbolRegistry::new(vec![
            MarketConfig {
                symbol: Symbol::new("btc-90k-mar-2026"),
                poly_ids: PolymarketIds {
                    condition_id: "condition-1".to_owned(),
                    yes_token_id: "yes-1".to_owned(),
                    no_token_id: "no-1".to_owned(),
                },
                market_question: None,
                market_rule: None,
                cex_symbol: "BTCUSDT".to_owned(),
                hedge_exchange: Exchange::Binance,
                strike_price: Some(Price(Decimal::new(90_000, 0))),
                settlement_timestamp: 1_775_001_600,
                min_tick_size: Price(Decimal::new(1, 2)),
                neg_risk: false,
                cex_price_tick: Decimal::new(1, 1),
                cex_qty_step: Decimal::new(1, 3),
                cex_contract_multiplier: Decimal::ONE,
            },
            MarketConfig {
                symbol: Symbol::new("btc-100k-mar-2026"),
                poly_ids: PolymarketIds {
                    condition_id: "condition-2".to_owned(),
                    yes_token_id: "yes-2".to_owned(),
                    no_token_id: "no-2".to_owned(),
                },
                market_question: None,
                market_rule: None,
                cex_symbol: "BTCUSDT".to_owned(),
                hedge_exchange: Exchange::Binance,
                strike_price: Some(Price(Decimal::new(100_000, 0))),
                settlement_timestamp: 1_775_001_600,
                min_tick_size: Price(Decimal::new(1, 2)),
                neg_risk: false,
                cex_price_tick: Decimal::new(1, 1),
                cex_qty_step: Decimal::new(1, 3),
                cex_contract_multiplier: Decimal::ONE,
            },
        ])
    }

    #[test]
    fn cex_venue_orderbook_expands_to_all_attached_symbols() {
        let registry = shared_cex_registry();
        let events = MarketDataEvent::CexVenueOrderBookUpdate {
            exchange: Exchange::Binance,
            venue_symbol: "BTCUSDT".to_owned(),
            bids: vec![PriceLevel {
                price: Price(Decimal::new(100_000, 0)),
                quantity: VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(2, 0))),
            }],
            asks: vec![PriceLevel {
                price: Price(Decimal::new(100_010, 0)),
                quantity: VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(3, 0))),
            }],
            exchange_timestamp_ms: 10,
            received_at_ms: 11,
            sequence: 12,
        }
        .expand_for_registry(&registry);

        let mut symbols = events
            .into_iter()
            .map(|event| match event {
                MarketDataEvent::OrderBookUpdate { snapshot } => snapshot.symbol,
                other => panic!("unexpected event after expansion: {other:?}"),
            })
            .collect::<Vec<_>>();
        symbols.sort_by(|left, right| left.0.cmp(&right.0));

        assert_eq!(
            symbols,
            vec![
                Symbol::new("btc-100k-mar-2026"),
                Symbol::new("btc-90k-mar-2026"),
            ]
        );
    }
}
