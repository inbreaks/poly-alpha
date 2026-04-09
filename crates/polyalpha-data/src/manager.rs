use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use polyalpha_core::{
    Exchange, InstrumentKind, MarketDataEvent, MarketDataTx, SettlementRules, Symbol,
};

use crate::error::{DataError, Result};
use crate::normalizer::{
    CexBookUpdate, CexFundingUpdate, CexTradeUpdate, MarketDataNormalizer, PolyBookUpdate,
};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum OrderbookSequenceKey {
    Symbol {
        exchange: Exchange,
        symbol: Symbol,
        instrument: InstrumentKind,
    },
    CexVenue {
        exchange: Exchange,
        venue_symbol: String,
    },
}

#[derive(Clone, Debug)]
pub struct DataManager {
    normalizer: MarketDataNormalizer,
    market_data_tx: MarketDataTx,
    last_orderbook_sequences: Arc<Mutex<HashMap<OrderbookSequenceKey, u64>>>,
}

impl DataManager {
    pub fn new(normalizer: MarketDataNormalizer, market_data_tx: MarketDataTx) -> Self {
        Self {
            normalizer,
            market_data_tx,
            last_orderbook_sequences: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn normalizer(&self) -> &MarketDataNormalizer {
        &self.normalizer
    }

    pub fn publish(&self, event: MarketDataEvent) -> Result<usize> {
        if self.should_suppress_orderbook_event(&event) {
            return Ok(0);
        }
        self.market_data_tx
            .send(event)
            .map_err(|_| DataError::ChannelClosed)
    }

    fn should_suppress_orderbook_event(&self, event: &MarketDataEvent) -> bool {
        let Some((key, sequence)) = orderbook_sequence_key(event) else {
            return false;
        };
        let mut guard = self
            .last_orderbook_sequences
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match guard.get(&key) {
            Some(previous_sequence) if sequence < *previous_sequence => true,
            _ => {
                guard.insert(key, sequence);
                false
            }
        }
    }

    pub fn normalize_and_publish_poly_orderbook(&self, update: PolyBookUpdate) -> Result<usize> {
        let event = self.normalizer.normalize_poly_orderbook(update)?;
        self.publish(event)
    }

    pub fn normalize_and_publish_cex_orderbook(&self, update: CexBookUpdate) -> Result<usize> {
        let event = self.normalizer.normalize_cex_orderbook(update)?;
        self.publish(event)
    }

    pub fn normalize_and_publish_cex_trade(&self, update: CexTradeUpdate) -> Result<usize> {
        let event = self.normalizer.normalize_cex_trade(update)?;
        self.publish(event)
    }

    pub fn normalize_and_publish_cex_funding(&self, update: CexFundingUpdate) -> Result<usize> {
        let event = self.normalizer.normalize_cex_funding(update)?;
        self.publish(event)
    }

    pub fn publish_market_lifecycle(
        &self,
        symbol: &Symbol,
        now_timestamp_secs: u64,
        emitted_at_ms: u64,
        rules: &SettlementRules,
    ) -> Result<usize> {
        let event =
            self.normalizer
                .market_lifecycle(symbol, now_timestamp_secs, emitted_at_ms, rules)?;
        self.publish(event)
    }
}

fn orderbook_sequence_key(event: &MarketDataEvent) -> Option<(OrderbookSequenceKey, u64)> {
    match event {
        MarketDataEvent::OrderBookUpdate { snapshot } => Some((
            OrderbookSequenceKey::Symbol {
                exchange: snapshot.exchange,
                symbol: snapshot.symbol.clone(),
                instrument: snapshot.instrument,
            },
            snapshot.sequence,
        )),
        MarketDataEvent::CexVenueOrderBookUpdate {
            exchange,
            venue_symbol,
            sequence,
            ..
        } => Some((
            OrderbookSequenceKey::CexVenue {
                exchange: *exchange,
                venue_symbol: venue_symbol.clone(),
            },
            *sequence,
        )),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use tokio::sync::broadcast::error::TryRecvError;

    use polyalpha_core::{
        create_channels, CexBaseQty, Exchange, MarketConfig, MarketDataEvent, PolyShares,
        PolymarketIds, Price, SymbolRegistry,
    };

    use super::*;
    use crate::normalizer::{CexBookLevel, CexBookUpdate, PolyBookLevel, PolyBookUpdate};

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

    fn sample_poly_update(sequence: u64, price_cents: i64) -> PolyBookUpdate {
        PolyBookUpdate {
            asset_id: "yes-1".to_owned(),
            bids: vec![PolyBookLevel {
                price: Price(Decimal::new(price_cents - 1, 2)),
                shares: PolyShares(Decimal::new(5, 0)),
            }],
            asks: vec![PolyBookLevel {
                price: Price(Decimal::new(price_cents + 1, 2)),
                shares: PolyShares(Decimal::new(5, 0)),
            }],
            exchange_timestamp_ms: sequence,
            received_at_ms: sequence + 1,
            sequence,
            last_trade_price: None,
        }
    }

    fn sample_cex_update(sequence: u64, price_tenths: i64) -> CexBookUpdate {
        CexBookUpdate {
            exchange: Exchange::Binance,
            venue_symbol: "BTCUSDT".to_owned(),
            bids: vec![CexBookLevel {
                price: Price(Decimal::new(price_tenths - 1, 1)),
                base_qty: CexBaseQty(Decimal::new(5, 0)),
            }],
            asks: vec![CexBookLevel {
                price: Price(Decimal::new(price_tenths + 1, 1)),
                base_qty: CexBaseQty(Decimal::new(5, 0)),
            }],
            exchange_timestamp_ms: sequence,
            received_at_ms: sequence + 1,
            sequence,
        }
    }

    #[test]
    fn normalized_events_are_published_to_market_data_channel() {
        let channels = create_channels(&[Symbol::new("btc-100k-mar-2026")]);
        let manager = DataManager::new(
            MarketDataNormalizer::new(sample_registry()),
            channels.market_data_tx.clone(),
        );
        let mut rx = channels.market_data_tx.subscribe();

        let subscribers = manager
            .normalize_and_publish_poly_orderbook(PolyBookUpdate {
                asset_id: "yes-1".to_owned(),
                bids: vec![PolyBookLevel {
                    price: Price(Decimal::new(48, 2)),
                    shares: PolyShares(Decimal::new(5, 0)),
                }],
                asks: vec![],
                exchange_timestamp_ms: 10,
                received_at_ms: 11,
                sequence: 1,
                last_trade_price: None,
            })
            .expect("publish should succeed");

        assert!(subscribers >= 1);
        match rx.try_recv().expect("event should be available") {
            MarketDataEvent::OrderBookUpdate { snapshot } => {
                assert_eq!(snapshot.exchange, Exchange::Polymarket);
                assert_eq!(snapshot.symbol, Symbol::new("btc-100k-mar-2026"));
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn stale_poly_orderbook_update_is_not_published() {
        let channels = create_channels(&[Symbol::new("btc-100k-mar-2026")]);
        let manager = DataManager::new(
            MarketDataNormalizer::new(sample_registry()),
            channels.market_data_tx.clone(),
        );
        let mut rx = channels.market_data_tx.subscribe();

        let first = manager
            .normalize_and_publish_poly_orderbook(sample_poly_update(10, 50))
            .expect("first poly update should publish");
        let second = manager
            .normalize_and_publish_poly_orderbook(sample_poly_update(9, 40))
            .expect("stale poly update should be ignored cleanly");

        assert!(first >= 1);
        assert_eq!(second, 0);

        match rx.try_recv().expect("fresh poly event should be available") {
            MarketDataEvent::OrderBookUpdate { snapshot } => {
                assert_eq!(snapshot.sequence, 10);
                assert_eq!(snapshot.asks[0].price, Price(Decimal::new(51, 2)));
            }
            other => panic!("unexpected event: {other:?}"),
        }
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn stale_cex_orderbook_update_is_not_published() {
        let channels = create_channels(&[Symbol::new("btc-100k-mar-2026")]);
        let manager = DataManager::new(
            MarketDataNormalizer::new(sample_registry()),
            channels.market_data_tx.clone(),
        );
        let mut rx = channels.market_data_tx.subscribe();

        let first = manager
            .normalize_and_publish_cex_orderbook(sample_cex_update(10, 1_000))
            .expect("first cex update should publish");
        let second = manager
            .normalize_and_publish_cex_orderbook(sample_cex_update(9, 900))
            .expect("stale cex update should be ignored cleanly");

        assert!(first >= 1);
        assert_eq!(second, 0);

        match rx.try_recv().expect("fresh cex event should be available") {
            MarketDataEvent::CexVenueOrderBookUpdate {
                sequence,
                asks,
                venue_symbol,
                ..
            } => {
                assert_eq!(sequence, 10);
                assert_eq!(venue_symbol, "BTCUSDT");
                assert_eq!(asks[0].price, Price(Decimal::new(1001, 1)));
            }
            other => panic!("unexpected event: {other:?}"),
        }
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    }
}
