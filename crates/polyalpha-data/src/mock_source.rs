use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use polyalpha_core::{
    ConnectionStatus, Exchange, MarketDataEvent, MarketDataSource, SettlementRules, Symbol,
};

use crate::error::{DataError, Result};
use crate::manager::DataManager;
use crate::normalizer::{CexBookUpdate, CexFundingUpdate, CexTradeUpdate, PolyBookUpdate};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MockTick {
    PolyOrderBook(PolyBookUpdate),
    CexOrderBook(CexBookUpdate),
    CexTrade(CexTradeUpdate),
    CexFunding(CexFundingUpdate),
    Lifecycle {
        symbol: Symbol,
        now_timestamp_secs: u64,
        emitted_at_ms: u64,
    },
    Connection {
        exchange: Exchange,
        status: ConnectionStatus,
    },
}

#[derive(Clone, Debug)]
pub struct MockMarketDataSource {
    manager: DataManager,
    source_exchange: Exchange,
    settlement_rules: SettlementRules,
    ticks: Arc<Vec<MockTick>>,
    cursor: Arc<Mutex<usize>>,
    status: Arc<Mutex<ConnectionStatus>>,
    subscriptions: Arc<Mutex<HashSet<Symbol>>>,
}

impl MockMarketDataSource {
    pub fn new(
        manager: DataManager,
        source_exchange: Exchange,
        settlement_rules: SettlementRules,
        ticks: Vec<MockTick>,
    ) -> Self {
        Self {
            manager,
            source_exchange,
            settlement_rules,
            ticks: Arc::new(ticks),
            cursor: Arc::new(Mutex::new(0)),
            status: Arc::new(Mutex::new(ConnectionStatus::Disconnected)),
            subscriptions: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn with_defaults(manager: DataManager, ticks: Vec<MockTick>) -> Self {
        Self::new(
            manager,
            Exchange::Polymarket,
            SettlementRules::default(),
            ticks,
        )
    }

    pub fn reset(&self) {
        *self.cursor.lock().expect("cursor lock poisoned") = 0;
    }

    pub fn tick_count(&self) -> usize {
        self.ticks.len()
    }

    pub fn remaining_ticks(&self) -> usize {
        let cursor = *self.cursor.lock().expect("cursor lock poisoned");
        self.tick_count().saturating_sub(cursor)
    }

    pub fn is_subscribed(&self, symbol: &Symbol) -> bool {
        self.subscriptions
            .lock()
            .expect("subscriptions lock poisoned")
            .contains(symbol)
    }

    pub fn replay_all(&self) -> Result<usize> {
        self.ensure_connected()?;

        let mut published = 0usize;
        while let Some(sent) = self.emit_next()? {
            published += sent;
        }
        Ok(published)
    }

    pub fn emit_next(&self) -> Result<Option<usize>> {
        self.ensure_connected()?;

        let next_tick = {
            let mut cursor = self.cursor.lock().expect("cursor lock poisoned");
            if *cursor >= self.ticks.len() {
                None
            } else {
                let tick = self.ticks[*cursor].clone();
                *cursor += 1;
                Some(tick)
            }
        };

        next_tick.map(|tick| self.emit_tick(tick)).transpose()
    }

    fn emit_tick(&self, tick: MockTick) -> Result<usize> {
        match tick {
            MockTick::PolyOrderBook(update) => {
                let symbol = self
                    .manager
                    .normalizer()
                    .registry()
                    .lookup_poly_asset(&update.asset_id)
                    .map(|(symbol, _)| symbol)
                    .ok_or_else(|| DataError::UnknownPolymarketAsset {
                        asset_id: update.asset_id.clone(),
                    })?;
                if !self.is_subscribed(&symbol) {
                    return Ok(0);
                }
                self.manager
                    .normalize_and_publish_poly_orderbook(update)
                    .map(|_| 1)
            }
            MockTick::CexOrderBook(update) => {
                let symbols = self
                    .manager
                    .normalizer()
                    .registry()
                    .lookup_cex_symbols(update.exchange, &update.venue_symbol)
                    .map(|s| s.to_vec())
                    .ok_or_else(|| DataError::UnknownCexSymbol {
                        exchange: update.exchange,
                        venue_symbol: update.venue_symbol.clone(),
                    })?;
                if !symbols.iter().any(|s| self.is_subscribed(s)) {
                    return Ok(0);
                }
                self.manager
                    .normalize_and_publish_cex_orderbook(update)
                    .map(|_| 1)
            }
            MockTick::CexTrade(update) => {
                let symbols = self
                    .manager
                    .normalizer()
                    .registry()
                    .lookup_cex_symbols(update.exchange, &update.venue_symbol)
                    .map(|s| s.to_vec())
                    .ok_or_else(|| DataError::UnknownCexSymbol {
                        exchange: update.exchange,
                        venue_symbol: update.venue_symbol.clone(),
                    })?;
                if !symbols.iter().any(|s| self.is_subscribed(s)) {
                    return Ok(0);
                }
                self.manager
                    .normalize_and_publish_cex_trade(update)
                    .map(|_| 1)
            }
            MockTick::CexFunding(update) => {
                let symbols = self
                    .manager
                    .normalizer()
                    .registry()
                    .lookup_cex_symbols(update.exchange, &update.venue_symbol)
                    .map(|s| s.to_vec())
                    .ok_or_else(|| DataError::UnknownCexSymbol {
                        exchange: update.exchange,
                        venue_symbol: update.venue_symbol.clone(),
                    })?;
                if !symbols.iter().any(|s| self.is_subscribed(s)) {
                    return Ok(0);
                }
                self.manager
                    .normalize_and_publish_cex_funding(update)
                    .map(|_| 1)
            }
            MockTick::Lifecycle {
                symbol,
                now_timestamp_secs,
                emitted_at_ms,
            } => {
                if !self.is_subscribed(&symbol) {
                    return Ok(0);
                }
                self.manager
                    .publish_market_lifecycle(
                        &symbol,
                        now_timestamp_secs,
                        emitted_at_ms,
                        &self.settlement_rules,
                    )
                    .map(|_| 1)
            }
            MockTick::Connection { exchange, status } => self
                .manager
                .publish(MarketDataEvent::ConnectionEvent { exchange, status })
                .map(|_| 1),
        }
    }

    fn ensure_connected(&self) -> Result<()> {
        if self.connection_status() == ConnectionStatus::Connected {
            Ok(())
        } else {
            Err(DataError::NotConnected {
                exchange: self.source_exchange,
            })
        }
    }
}

#[async_trait]
impl MarketDataSource for MockMarketDataSource {
    async fn connect(&mut self) -> polyalpha_core::Result<()> {
        *self.status.lock().expect("status lock poisoned") = ConnectionStatus::Connected;
        Ok(())
    }

    async fn subscribe_market(&self, symbol: &Symbol) -> polyalpha_core::Result<()> {
        self.subscriptions
            .lock()
            .expect("subscriptions lock poisoned")
            .insert(symbol.clone());
        Ok(())
    }

    fn connection_status(&self) -> ConnectionStatus {
        *self.status.lock().expect("status lock poisoned")
    }

    fn exchange(&self) -> Exchange {
        self.source_exchange
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use polyalpha_core::{
        create_channels, CexBaseQty, Exchange, MarketConfig, MarketDataEvent, MarketDataSource,
        OrderSide, PolyShares, PolymarketIds, Price, SettlementRules, Symbol, SymbolRegistry,
    };

    use super::*;
    use crate::manager::DataManager;
    use crate::normalizer::{
        CexFundingUpdate, CexTradeUpdate, MarketDataNormalizer, PolyBookLevel, PolyBookUpdate,
    };

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

    #[tokio::test]
    async fn mock_source_replays_deterministic_mixed_ticks() {
        let symbol = Symbol::new("btc-100k-mar-2026");
        let channels = create_channels(std::slice::from_ref(&symbol));
        let manager = DataManager::new(
            MarketDataNormalizer::new(sample_registry()),
            channels.market_data_tx.clone(),
        );
        let mut source = MockMarketDataSource::new(
            manager,
            Exchange::Polymarket,
            SettlementRules::default(),
            vec![
                MockTick::PolyOrderBook(PolyBookUpdate {
                    asset_id: "yes-1".to_owned(),
                    bids: vec![PolyBookLevel {
                        price: Price(Decimal::new(49, 2)),
                        shares: PolyShares(Decimal::new(20, 0)),
                    }],
                    asks: vec![],
                    exchange_timestamp_ms: 10,
                    received_at_ms: 11,
                    sequence: 1,
                    last_trade_price: None,
                }),
                MockTick::CexTrade(CexTradeUpdate {
                    exchange: Exchange::Binance,
                    venue_symbol: "BTCUSDT".to_owned(),
                    price: Price(Decimal::new(91_500, 0)),
                    quantity: CexBaseQty(Decimal::new(3, 2)),
                    side: OrderSide::Buy,
                    timestamp_ms: 12,
                }),
                MockTick::CexFunding(CexFundingUpdate {
                    exchange: Exchange::Binance,
                    venue_symbol: "BTCUSDT".to_owned(),
                    rate: Decimal::new(5, 4),
                    next_funding_time_ms: 13,
                }),
                MockTick::Lifecycle {
                    symbol: symbol.clone(),
                    now_timestamp_secs: 1_775_001_600 - 8 * 3600,
                    emitted_at_ms: 14,
                },
            ],
        );

        let mut rx = channels.market_data_tx.subscribe();
        source.connect().await.expect("connect should succeed");
        source
            .subscribe_market(&symbol)
            .await
            .expect("subscribe should succeed");

        let sent = source.replay_all().expect("replay should succeed");
        assert_eq!(sent, 4);
        assert_eq!(source.remaining_ticks(), 0);

        assert!(matches!(
            rx.try_recv().expect("expected poly book event"),
            MarketDataEvent::OrderBookUpdate { .. }
        ));
        assert!(matches!(
            rx.try_recv().expect("expected cex trade event"),
            MarketDataEvent::CexVenueTradeUpdate { .. }
        ));
        assert!(matches!(
            rx.try_recv().expect("expected cex funding event"),
            MarketDataEvent::CexVenueFundingRate { .. }
        ));
        assert!(matches!(
            rx.try_recv().expect("expected lifecycle event"),
            MarketDataEvent::MarketLifecycle { .. }
        ));
    }

    #[tokio::test]
    async fn mock_source_requires_connection_and_subscription_for_symbol_ticks() {
        let symbol = Symbol::new("btc-100k-mar-2026");
        let channels = create_channels(std::slice::from_ref(&symbol));
        let manager = DataManager::new(
            MarketDataNormalizer::new(sample_registry()),
            channels.market_data_tx.clone(),
        );
        let mut source = MockMarketDataSource::with_defaults(
            manager,
            vec![
                MockTick::PolyOrderBook(PolyBookUpdate {
                    asset_id: "yes-1".to_owned(),
                    bids: vec![],
                    asks: vec![],
                    exchange_timestamp_ms: 1,
                    received_at_ms: 2,
                    sequence: 1,
                    last_trade_price: None,
                }),
                MockTick::Connection {
                    exchange: Exchange::Binance,
                    status: ConnectionStatus::Connected,
                },
            ],
        );

        let mut rx = channels.market_data_tx.subscribe();
        let err = source
            .replay_all()
            .expect_err("replay should fail before connect");
        assert!(matches!(err, DataError::NotConnected { .. }));

        source.connect().await.expect("connect should succeed");
        let sent = source
            .replay_all()
            .expect("replay should work after connect");
        // orderbook skipped due to missing subscription; connection event is still published
        assert_eq!(sent, 1);
        assert!(matches!(
            rx.try_recv().expect("expected connection event"),
            MarketDataEvent::ConnectionEvent { .. }
        ));
    }
}
