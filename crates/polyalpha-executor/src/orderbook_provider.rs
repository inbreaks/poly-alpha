use polyalpha_core::{Exchange, InstrumentKind, OrderBookSnapshot, Symbol};

/// Trait for providing orderbook data to executors.
/// Implementations can fetch from live data, cached snapshots, or mock data.
pub trait OrderbookProvider: Send + Sync {
    /// Get the latest orderbook snapshot for a given exchange/symbol/instrument.
    /// Returns None if no orderbook is available.
    fn get_orderbook(
        &self,
        exchange: Exchange,
        symbol: &Symbol,
        instrument: InstrumentKind,
    ) -> Option<OrderBookSnapshot>;

    /// Get CEX orderbook by venue symbol (e.g., "BTCUSDT").
    /// This is useful when multiple markets share the same CEX symbol.
    fn get_cex_orderbook(
        &self,
        exchange: Exchange,
        venue_symbol: &str,
    ) -> Option<OrderBookSnapshot>;
}

/// A simple in-memory orderbook provider that stores the latest snapshots.
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct OrderbookKey {
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub instrument: InstrumentKind,
}

impl From<(Exchange, Symbol, InstrumentKind)> for OrderbookKey {
    fn from((exchange, symbol, instrument): (Exchange, Symbol, InstrumentKind)) -> Self {
        Self {
            exchange,
            symbol,
            instrument,
        }
    }
}

/// In-memory orderbook provider that can be updated with new snapshots.
pub struct InMemoryOrderbookProvider {
    /// Non-CEX orderbooks keyed by (exchange, symbol, instrument)
    orderbooks: Arc<RwLock<HashMap<OrderbookKey, OrderBookSnapshot>>>,
    /// CEX orderbooks keyed by (exchange, venue_symbol)
    /// This is the canonical storage for CEX snapshots.
    cex_orderbooks: Arc<RwLock<HashMap<(Exchange, String), OrderBookSnapshot>>>,
    /// Maps a symbol-keyed CEX lookup back to the canonical venue symbol.
    cex_symbol_index: Arc<RwLock<HashMap<(Exchange, Symbol), String>>>,
}

impl Default for InMemoryOrderbookProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryOrderbookProvider {
    pub fn new() -> Self {
        Self {
            orderbooks: Arc::new(RwLock::new(HashMap::new())),
            cex_orderbooks: Arc::new(RwLock::new(HashMap::new())),
            cex_symbol_index: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Update the orderbook for a given key.
    pub fn update(&self, snapshot: OrderBookSnapshot) {
        let key = OrderbookKey {
            exchange: snapshot.exchange,
            symbol: snapshot.symbol.clone(),
            instrument: snapshot.instrument,
        };
        if let Ok(mut map) = self.orderbooks.write() {
            map.insert(key, snapshot);
        }
    }

    /// Update CEX orderbook with venue_symbol for lookup.
    /// This allows looking up CEX orderbook by venue_symbol (e.g., "BTCUSDT")
    /// which is needed because multiple markets may share the same CEX symbol.
    pub fn update_cex(&self, snapshot: OrderBookSnapshot, venue_symbol: String) {
        let exchange = snapshot.exchange;
        let symbol = snapshot.symbol.clone();
        if let Ok(mut cex_map) = self.cex_orderbooks.write() {
            cex_map.insert((exchange, venue_symbol.clone()), snapshot);
        }

        if let Ok(mut symbol_index) = self.cex_symbol_index.write() {
            symbol_index.insert((exchange, symbol), venue_symbol);
        }
    }

    /// Update the canonical CEX snapshot once and refresh all market-symbol aliases that share it.
    pub fn update_cex_shared(
        &self,
        snapshot: OrderBookSnapshot,
        venue_symbol: String,
        symbols: &[Symbol],
    ) {
        let exchange = snapshot.exchange;
        if let Ok(mut cex_map) = self.cex_orderbooks.write() {
            cex_map.insert((exchange, venue_symbol.clone()), snapshot);
        }

        if let Ok(mut symbol_index) = self.cex_symbol_index.write() {
            for symbol in symbols {
                symbol_index.insert((exchange, symbol.clone()), venue_symbol.clone());
            }
        }
    }

    /// Get a clone of the internal storage for use in other contexts.
    pub fn clone_storage(&self) -> Arc<RwLock<HashMap<OrderbookKey, OrderBookSnapshot>>> {
        Arc::clone(&self.orderbooks)
    }
}

impl OrderbookProvider for InMemoryOrderbookProvider {
    fn get_orderbook(
        &self,
        exchange: Exchange,
        symbol: &Symbol,
        instrument: InstrumentKind,
    ) -> Option<OrderBookSnapshot> {
        if instrument == InstrumentKind::CexPerp {
            let venue_symbol = {
                let index = self.cex_symbol_index.read().ok()?;
                index.get(&(exchange, symbol.clone())).cloned()?
            };
            let mut snapshot = {
                let cex_books = self.cex_orderbooks.read().ok()?;
                cex_books.get(&(exchange, venue_symbol)).cloned()?
            };
            snapshot.symbol = symbol.clone();
            return Some(snapshot);
        }

        let key = OrderbookKey {
            exchange,
            symbol: symbol.clone(),
            instrument,
        };
        if let Ok(map) = self.orderbooks.read() {
            map.get(&key).cloned()
        } else {
            None
        }
    }

    fn get_cex_orderbook(
        &self,
        exchange: Exchange,
        venue_symbol: &str,
    ) -> Option<OrderBookSnapshot> {
        if let Ok(map) = self.cex_orderbooks.read() {
            map.get(&(exchange, venue_symbol.to_string())).cloned()
        } else {
            None
        }
    }
}

/// A no-op orderbook provider that always returns None.
/// Used when orderbook-based fill simulation is disabled.
pub struct NoOpOrderbookProvider;

impl OrderbookProvider for NoOpOrderbookProvider {
    fn get_orderbook(
        &self,
        _exchange: Exchange,
        _symbol: &Symbol,
        _instrument: InstrumentKind,
    ) -> Option<OrderBookSnapshot> {
        None
    }

    fn get_cex_orderbook(
        &self,
        _exchange: Exchange,
        _venue_symbol: &str,
    ) -> Option<OrderBookSnapshot> {
        None
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;
    use polyalpha_core::{CexBaseQty, Price, PriceLevel, VenueQuantity};

    fn sample_cex_snapshot(symbol: &str, sequence: u64) -> OrderBookSnapshot {
        OrderBookSnapshot {
            exchange: Exchange::Binance,
            symbol: Symbol::new(symbol),
            instrument: InstrumentKind::CexPerp,
            bids: vec![PriceLevel {
                price: Price(Decimal::new(1000, 1)),
                quantity: VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(5, 0))),
            }],
            asks: vec![PriceLevel {
                price: Price(Decimal::new(1010, 1)),
                quantity: VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(5, 0))),
            }],
            exchange_timestamp_ms: sequence,
            received_at_ms: sequence,
            sequence,
            last_trade_price: None,
        }
    }

    #[test]
    fn cex_symbol_lookup_reads_latest_shared_venue_snapshot() {
        let provider = InMemoryOrderbookProvider::new();
        provider.update_cex(sample_cex_snapshot("market-a", 1), "BTCUSDT".to_owned());
        provider.update_cex(sample_cex_snapshot("market-b", 2), "BTCUSDT".to_owned());

        let symbol_book = provider
            .get_orderbook(
                Exchange::Binance,
                &Symbol::new("market-a"),
                InstrumentKind::CexPerp,
            )
            .expect("symbol lookup should resolve through venue alias");
        let venue_book = provider
            .get_cex_orderbook(Exchange::Binance, "BTCUSDT")
            .expect("venue lookup should return latest canonical CEX snapshot");

        assert_eq!(symbol_book.sequence, 2);
        assert_eq!(symbol_book.symbol, Symbol::new("market-a"));
        assert_eq!(venue_book.sequence, 2);
    }
}
