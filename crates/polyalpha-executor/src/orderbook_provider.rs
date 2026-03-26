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
    /// Orderbooks keyed by (exchange, symbol, instrument)
    orderbooks: Arc<RwLock<HashMap<OrderbookKey, OrderBookSnapshot>>>,
    /// CEX orderbooks keyed by (exchange, venue_symbol)
    /// This allows looking up CEX orderbook by venue symbol (e.g., "BTCUSDT")
    cex_orderbooks: Arc<RwLock<HashMap<(Exchange, String), OrderBookSnapshot>>>,
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
        // Store by symbol (for Poly compatibility)
        let key = OrderbookKey {
            exchange: snapshot.exchange,
            symbol: snapshot.symbol.clone(),
            instrument: snapshot.instrument,
        };
        if let Ok(mut map) = self.orderbooks.write() {
            map.insert(key, snapshot.clone());
        }

        // Also store by venue_symbol for CEX lookup
        if let Ok(mut cex_map) = self.cex_orderbooks.write() {
            cex_map.insert((snapshot.exchange, venue_symbol), snapshot);
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
