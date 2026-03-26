use polyalpha_core::{MarketDataEvent, MarketDataTx, SettlementRules, Symbol};

use crate::error::{DataError, Result};
use crate::normalizer::{
    CexBookUpdate, CexFundingUpdate, CexTradeUpdate, MarketDataNormalizer, PolyBookUpdate,
};

#[derive(Clone, Debug)]
pub struct DataManager {
    normalizer: MarketDataNormalizer,
    market_data_tx: MarketDataTx,
}

impl DataManager {
    pub fn new(normalizer: MarketDataNormalizer, market_data_tx: MarketDataTx) -> Self {
        Self {
            normalizer,
            market_data_tx,
        }
    }

    pub fn normalizer(&self) -> &MarketDataNormalizer {
        &self.normalizer
    }

    pub fn publish(&self, event: MarketDataEvent) -> Result<usize> {
        self.market_data_tx
            .send(event)
            .map_err(|_| DataError::ChannelClosed)
    }

    pub fn normalize_and_publish_poly_orderbook(&self, update: PolyBookUpdate) -> Result<usize> {
        let event = self.normalizer.normalize_poly_orderbook(update)?;
        self.publish(event)
    }

    pub fn normalize_and_publish_cex_orderbook(&self, update: CexBookUpdate) -> Result<usize> {
        let events = self.normalizer.normalize_cex_orderbook(update)?;
        let mut total = 0;
        for event in events {
            total += self.publish(event)?;
        }
        Ok(total)
    }

    pub fn normalize_and_publish_cex_trade(&self, update: CexTradeUpdate) -> Result<usize> {
        let events = self.normalizer.normalize_cex_trade(update)?;
        let mut total = 0;
        for event in events {
            total += self.publish(event)?;
        }
        Ok(total)
    }

    pub fn normalize_and_publish_cex_funding(&self, update: CexFundingUpdate) -> Result<usize> {
        let events = self.normalizer.normalize_cex_funding(update)?;
        let mut total = 0;
        for event in events {
            total += self.publish(event)?;
        }
        Ok(total)
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

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use polyalpha_core::{
        create_channels, Exchange, MarketConfig, MarketDataEvent, PolyShares, PolymarketIds, Price,
        SymbolRegistry,
    };

    use super::*;
    use crate::normalizer::{PolyBookLevel, PolyBookUpdate};

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
}
