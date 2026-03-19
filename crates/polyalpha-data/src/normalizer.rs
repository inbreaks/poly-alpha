use rust_decimal::Decimal;

use polyalpha_core::{
    CexBaseQty, Exchange, InstrumentKind, MarketDataEvent, MarketPhase, OrderBookSnapshot,
    OrderSide, PolyShares, Price, PriceLevel, SettlementRules, Symbol, SymbolRegistry, TokenSide,
    VenueQuantity,
};

use crate::error::{DataError, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolyBookLevel {
    pub price: Price,
    pub shares: PolyShares,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolyBookUpdate {
    pub asset_id: String,
    pub bids: Vec<PolyBookLevel>,
    pub asks: Vec<PolyBookLevel>,
    pub exchange_timestamp_ms: u64,
    pub received_at_ms: u64,
    pub sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CexBookLevel {
    pub price: Price,
    pub base_qty: CexBaseQty,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CexBookUpdate {
    pub exchange: Exchange,
    pub venue_symbol: String,
    pub bids: Vec<CexBookLevel>,
    pub asks: Vec<CexBookLevel>,
    pub exchange_timestamp_ms: u64,
    pub received_at_ms: u64,
    pub sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CexTradeUpdate {
    pub exchange: Exchange,
    pub venue_symbol: String,
    pub price: Price,
    pub quantity: CexBaseQty,
    pub side: OrderSide,
    pub timestamp_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CexFundingUpdate {
    pub exchange: Exchange,
    pub venue_symbol: String,
    pub rate: Decimal,
    pub next_funding_time_ms: u64,
}

#[derive(Clone, Debug)]
pub struct MarketDataNormalizer {
    registry: SymbolRegistry,
}

impl MarketDataNormalizer {
    pub fn new(registry: SymbolRegistry) -> Self {
        Self { registry }
    }

    pub fn registry(&self) -> &SymbolRegistry {
        &self.registry
    }

    pub fn normalize_poly_orderbook(&self, update: PolyBookUpdate) -> Result<MarketDataEvent> {
        let (symbol, token_side) = self
            .registry
            .lookup_poly_asset(&update.asset_id)
            .ok_or_else(|| DataError::UnknownPolymarketAsset {
                asset_id: update.asset_id.clone(),
            })?;

        Ok(MarketDataEvent::OrderBookUpdate {
            snapshot: OrderBookSnapshot {
                exchange: Exchange::Polymarket,
                symbol,
                instrument: instrument_from_token_side(token_side),
                bids: update
                    .bids
                    .into_iter()
                    .map(|level| PriceLevel {
                        price: level.price,
                        quantity: VenueQuantity::PolyShares(level.shares),
                    })
                    .collect(),
                asks: update
                    .asks
                    .into_iter()
                    .map(|level| PriceLevel {
                        price: level.price,
                        quantity: VenueQuantity::PolyShares(level.shares),
                    })
                    .collect(),
                exchange_timestamp_ms: update.exchange_timestamp_ms,
                received_at_ms: update.received_at_ms,
                sequence: update.sequence,
            },
        })
    }

    pub fn normalize_cex_orderbook(&self, update: CexBookUpdate) -> Result<MarketDataEvent> {
        let symbol = self.lookup_cex_symbol(update.exchange, &update.venue_symbol)?;

        Ok(MarketDataEvent::OrderBookUpdate {
            snapshot: OrderBookSnapshot {
                exchange: update.exchange,
                symbol,
                instrument: InstrumentKind::CexPerp,
                bids: update
                    .bids
                    .into_iter()
                    .map(|level| PriceLevel {
                        price: level.price,
                        quantity: VenueQuantity::CexBaseQty(level.base_qty),
                    })
                    .collect(),
                asks: update
                    .asks
                    .into_iter()
                    .map(|level| PriceLevel {
                        price: level.price,
                        quantity: VenueQuantity::CexBaseQty(level.base_qty),
                    })
                    .collect(),
                exchange_timestamp_ms: update.exchange_timestamp_ms,
                received_at_ms: update.received_at_ms,
                sequence: update.sequence,
            },
        })
    }

    pub fn normalize_cex_trade(&self, update: CexTradeUpdate) -> Result<MarketDataEvent> {
        let symbol = self.lookup_cex_symbol(update.exchange, &update.venue_symbol)?;

        Ok(MarketDataEvent::TradeUpdate {
            exchange: update.exchange,
            symbol,
            instrument: InstrumentKind::CexPerp,
            price: update.price,
            quantity: VenueQuantity::CexBaseQty(update.quantity),
            side: update.side,
            timestamp_ms: update.timestamp_ms,
        })
    }

    pub fn normalize_cex_funding(&self, update: CexFundingUpdate) -> Result<MarketDataEvent> {
        let symbol = self.lookup_cex_symbol(update.exchange, &update.venue_symbol)?;

        Ok(MarketDataEvent::FundingRate {
            exchange: update.exchange,
            symbol,
            rate: update.rate,
            next_funding_time_ms: update.next_funding_time_ms,
        })
    }

    pub fn market_lifecycle(
        &self,
        symbol: &Symbol,
        now_timestamp_secs: u64,
        emitted_at_ms: u64,
        rules: &SettlementRules,
    ) -> Result<MarketDataEvent> {
        let config = self
            .registry
            .get_config(symbol)
            .ok_or_else(|| DataError::UnknownSymbol {
                symbol: symbol.0.clone(),
            })?;

        Ok(MarketDataEvent::MarketLifecycle {
            symbol: symbol.clone(),
            phase: MarketPhase::from_settlement(
                config.settlement_timestamp,
                now_timestamp_secs,
                rules,
            ),
            timestamp_ms: emitted_at_ms,
        })
    }

    fn lookup_cex_symbol(&self, exchange: Exchange, venue_symbol: &str) -> Result<Symbol> {
        self.registry
            .lookup_cex_symbol(exchange, venue_symbol)
            .cloned()
            .ok_or_else(|| DataError::UnknownCexSymbol {
                exchange,
                venue_symbol: venue_symbol.to_owned(),
            })
    }
}

fn instrument_from_token_side(token_side: TokenSide) -> InstrumentKind {
    match token_side {
        TokenSide::Yes => InstrumentKind::PolyYes,
        TokenSide::No => InstrumentKind::PolyNo,
    }
}

#[cfg(test)]
mod tests {
    use polyalpha_core::{MarketConfig, PolymarketIds};

    use super::*;

    fn sample_registry() -> SymbolRegistry {
        SymbolRegistry::new(vec![MarketConfig {
            symbol: Symbol::new("btc-100k-mar-2026"),
            poly_ids: PolymarketIds {
                condition_id: "condition-1".to_owned(),
                yes_token_id: "yes-1".to_owned(),
                no_token_id: "no-1".to_owned(),
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
        }])
    }

    #[test]
    fn polymarket_books_resolve_symbol_and_token_side() {
        let normalizer = MarketDataNormalizer::new(sample_registry());
        let event = normalizer
            .normalize_poly_orderbook(PolyBookUpdate {
                asset_id: "yes-1".to_owned(),
                bids: vec![PolyBookLevel {
                    price: Price(Decimal::new(49, 2)),
                    shares: PolyShares(Decimal::new(10, 0)),
                }],
                asks: vec![PolyBookLevel {
                    price: Price(Decimal::new(51, 2)),
                    shares: PolyShares(Decimal::new(12, 0)),
                }],
                exchange_timestamp_ms: 10,
                received_at_ms: 11,
                sequence: 7,
            })
            .expect("poly orderbook should normalize");

        match event {
            MarketDataEvent::OrderBookUpdate { snapshot } => {
                assert_eq!(snapshot.exchange, Exchange::Polymarket);
                assert_eq!(snapshot.symbol, Symbol::new("btc-100k-mar-2026"));
                assert_eq!(snapshot.instrument, InstrumentKind::PolyYes);
                assert_eq!(snapshot.bids.len(), 1);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn cex_trade_uses_symbol_registry_reverse_lookup() {
        let normalizer = MarketDataNormalizer::new(sample_registry());
        let event = normalizer
            .normalize_cex_trade(CexTradeUpdate {
                exchange: Exchange::Binance,
                venue_symbol: "BTCUSDT".to_owned(),
                price: Price(Decimal::new(91_250, 0)),
                quantity: CexBaseQty(Decimal::new(25, 3)),
                side: OrderSide::Buy,
                timestamp_ms: 12,
            })
            .expect("cex trade should normalize");

        match event {
            MarketDataEvent::TradeUpdate {
                exchange,
                symbol,
                quantity,
                ..
            } => {
                assert_eq!(exchange, Exchange::Binance);
                assert_eq!(symbol, Symbol::new("btc-100k-mar-2026"));
                assert_eq!(
                    quantity,
                    VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(25, 3)))
                );
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn lifecycle_events_are_derived_from_registry_config() {
        let normalizer = MarketDataNormalizer::new(sample_registry());
        let symbol = Symbol::new("btc-100k-mar-2026");
        let event = normalizer
            .market_lifecycle(
                &symbol,
                1_775_001_600 - 8 * 3600,
                1_775_001_600_000 - 8 * 3_600_000,
                &SettlementRules::default(),
            )
            .expect("lifecycle event should normalize");

        match event {
            MarketDataEvent::MarketLifecycle { phase, .. } => {
                assert!(matches!(phase, MarketPhase::ForceReduce { .. }));
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
}
