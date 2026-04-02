use std::collections::HashSet;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{json, Value};

use polyalpha_core::{
    CexBaseQty, ConnectionStatus, CoreError, Exchange, MarketDataSource, OrderSide, Symbol,
};

use crate::error::{DataError, Result};
use crate::manager::DataManager;
use crate::normalizer::{CexBookLevel, CexBookUpdate, CexFundingUpdate, CexTradeUpdate};

const DEFAULT_BINANCE_FUTURES_REST_URL: &str = "https://fapi.binance.com";

/// Historical kline data point
#[derive(Clone, Debug)]
pub struct BinanceKline {
    pub open_time_ms: u64,
    pub close: f64,
}

#[derive(Clone, Debug)]
pub struct BinanceFuturesDataSource {
    manager: DataManager,
    client: Client,
    rest_url: String,
    status: Arc<Mutex<ConnectionStatus>>,
    subscriptions: Arc<Mutex<HashSet<Symbol>>>,
}

impl BinanceFuturesDataSource {
    fn set_status_if_changed(&self, new_status: ConnectionStatus) {
        let mut guard = self.status.lock().expect("binance status lock poisoned");
        if *guard == new_status {
            return;
        }
        *guard = new_status;
    }

    fn mark_connected(&self) {
        self.set_status_if_changed(ConnectionStatus::Connected);
    }

    fn mark_connecting(&self) {
        self.set_status_if_changed(ConnectionStatus::Connecting);
    }

    fn mark_failure(&self) {
        let mut guard = self.status.lock().expect("binance status lock poisoned");
        let next = match *guard {
            ConnectionStatus::Connected => ConnectionStatus::Reconnecting,
            ConnectionStatus::Reconnecting => ConnectionStatus::Disconnected,
            ConnectionStatus::Connecting => ConnectionStatus::Reconnecting,
            _ => ConnectionStatus::Disconnected,
        };
        if next == *guard {
            return;
        }
        *guard = next;
    }
    pub fn new(manager: DataManager, rest_url: impl Into<String>) -> Self {
        Self {
            manager,
            client: Client::new(),
            rest_url: rest_url.into(),
            status: Arc::new(Mutex::new(ConnectionStatus::Disconnected)),
            subscriptions: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn with_default_url(manager: DataManager) -> Self {
        Self::new(manager, DEFAULT_BINANCE_FUTURES_REST_URL)
    }

    pub async fn fetch_orderbook(
        &self,
        venue_symbol: &str,
        depth_limit: u16,
    ) -> Result<CexBookUpdate> {
        let endpoint = format!("{}/fapi/v1/depth", self.rest_url.trim_end_matches('/'));
        let payload = self
            .client
            .get(endpoint)
            .query(&[
                ("symbol", venue_symbol.to_owned()),
                ("limit", depth_limit.to_string()),
            ])
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        Self::parse_depth_payload(&payload, venue_symbol)
    }

    pub async fn fetch_funding_and_mark(
        &self,
        venue_symbol: &str,
    ) -> Result<(CexFundingUpdate, CexTradeUpdate)> {
        let endpoint = format!(
            "{}/fapi/v1/premiumIndex",
            self.rest_url.trim_end_matches('/')
        );
        let payload = self
            .client
            .get(endpoint)
            .query(&[("symbol", venue_symbol)])
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        Self::parse_premium_index_payload(&payload, venue_symbol)
    }

    pub async fn fetch_and_publish_orderbook(
        &self,
        venue_symbol: &str,
        depth_limit: u16,
    ) -> Result<usize> {
        let update = match self.fetch_orderbook(venue_symbol, depth_limit).await {
            Ok(u) => {
                self.mark_connected();
                u
            }
            Err(e) => {
                self.mark_failure();
                return Err(e);
            }
        };

        self.manager.normalize_and_publish_cex_orderbook(update)
    }

    pub async fn fetch_and_publish_funding_and_mark(&self, venue_symbol: &str) -> Result<usize> {
        let (funding, mark) = match self.fetch_funding_and_mark(venue_symbol).await {
            Ok(pair) => {
                self.mark_connected();
                pair
            }
            Err(e) => {
                self.mark_failure();
                return Err(e);
            }
        };
        let mut published = 0usize;
        published += usize::from(self.manager.normalize_and_publish_cex_funding(funding)? > 0);
        published += usize::from(self.manager.normalize_and_publish_cex_trade(mark)? > 0);
        Ok(published)
    }

    pub async fn fetch_and_publish_orderbook_by_symbol(
        &self,
        symbol: &Symbol,
        depth_limit: u16,
    ) -> Result<usize> {
        let venue_symbol = self
            .manager
            .normalizer()
            .registry()
            .get_cex_symbol(symbol)
            .ok_or_else(|| DataError::UnknownSymbol {
                symbol: symbol.0.clone(),
            })?;
        self.fetch_and_publish_orderbook(venue_symbol, depth_limit)
            .await
    }

    pub async fn fetch_and_publish_funding_and_mark_by_symbol(
        &self,
        symbol: &Symbol,
    ) -> Result<usize> {
        let venue_symbol = self
            .manager
            .normalizer()
            .registry()
            .get_cex_symbol(symbol)
            .ok_or_else(|| DataError::UnknownSymbol {
                symbol: symbol.0.clone(),
            })?;
        self.fetch_and_publish_funding_and_mark(venue_symbol).await
    }

    /// Fetch recent klines (1-minute candles) for warmup
    /// Returns up to `limit` most recent klines
    pub async fn fetch_klines(&self, venue_symbol: &str, limit: u16) -> Result<Vec<BinanceKline>> {
        let endpoint = format!("{}/fapi/v1/klines", self.rest_url.trim_end_matches('/'));
        let payload = self
            .client
            .get(endpoint)
            .query(&[
                ("symbol", venue_symbol.to_owned()),
                ("interval", "1m".to_owned()),
                ("limit", limit.to_string()),
            ])
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        Self::parse_klines_payload(&payload)
    }

    pub async fn fetch_klines_by_symbol(
        &self,
        symbol: &Symbol,
        limit: u16,
    ) -> Result<Vec<BinanceKline>> {
        let venue_symbol = self
            .manager
            .normalizer()
            .registry()
            .get_cex_symbol(symbol)
            .ok_or_else(|| DataError::UnknownSymbol {
                symbol: symbol.0.clone(),
            })?;
        self.fetch_klines(venue_symbol, limit).await
    }

    fn parse_klines_payload(payload: &str) -> Result<Vec<BinanceKline>> {
        let raw: Vec<Vec<serde_json::Value>> = serde_json::from_str(payload)?;
        let mut klines = Vec::with_capacity(raw.len());
        for kline in raw {
            if kline.len() < 5 {
                continue;
            }
            let open_time_ms = match &kline[0] {
                serde_json::Value::Number(n) => n.as_u64().unwrap_or(0),
                serde_json::Value::String(s) => s.parse().unwrap_or(0),
                _ => 0,
            };
            let close = match &kline[4] {
                serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0),
                serde_json::Value::String(s) => s.parse().unwrap_or(0.0),
                _ => 0.0,
            };
            if open_time_ms > 0 && close > 0.0 {
                klines.push(BinanceKline {
                    open_time_ms,
                    close,
                });
            }
        }
        Ok(klines)
    }

    pub fn build_depth_subscribe_message(venue_symbol: &str, id: u64) -> Value {
        let stream = format!("{}@depth@100ms", venue_symbol.to_ascii_lowercase());
        json!({
            "method": "SUBSCRIBE",
            "params": [stream],
            "id": id
        })
    }

    pub fn build_partial_depth_stream_name(venue_symbol: &str, depth_limit: u16) -> String {
        let depth = depth_limit.clamp(5, 20);
        format!("{}@depth{}@100ms", venue_symbol.to_ascii_lowercase(), depth)
    }

    pub fn parse_depth_payload(payload: &str, venue_symbol: &str) -> Result<CexBookUpdate> {
        let response: BinanceDepthResponse = serde_json::from_str(payload)?;
        Ok(CexBookUpdate {
            exchange: Exchange::Binance,
            venue_symbol: venue_symbol.to_owned(),
            bids: response
                .bids
                .into_iter()
                .map(|[price, qty]| -> Result<CexBookLevel> {
                    Ok(CexBookLevel {
                        price: polyalpha_core::Price(parse_decimal(&price)?),
                        base_qty: CexBaseQty(parse_decimal(&qty)?),
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            asks: response
                .asks
                .into_iter()
                .map(|[price, qty]| -> Result<CexBookLevel> {
                    Ok(CexBookLevel {
                        price: polyalpha_core::Price(parse_decimal(&price)?),
                        base_qty: CexBaseQty(parse_decimal(&qty)?),
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            exchange_timestamp_ms: response.event_time.unwrap_or_else(now_millis),
            received_at_ms: now_millis(),
            sequence: response.last_update_id.unwrap_or(0),
        })
    }

    pub fn parse_depth_ws_payload(payload: &str) -> Result<CexBookUpdate> {
        Self::parse_partial_depth_ws_payload(payload, "")?.ok_or_else(|| {
            DataError::InvalidResponse("binance ws payload did not contain depth data".to_owned())
        })
    }

    pub fn parse_partial_depth_ws_payload(
        payload: &str,
        expected_symbol: &str,
    ) -> Result<Option<CexBookUpdate>> {
        let value: Value = serde_json::from_str(payload)?;
        let root = value.get("data").unwrap_or(&value);

        if root.get("result").is_some()
            || root.get("id").is_some()
            || root.get("code").is_some()
            || root.get("ping").is_some()
        {
            return Ok(None);
        }

        let Some(bids_value) = root.get("bids").or_else(|| root.get("b")) else {
            return Ok(None);
        };
        let Some(asks_value) = root.get("asks").or_else(|| root.get("a")) else {
            return Ok(None);
        };

        let parsed_symbol = root
            .get("s")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_ascii_uppercase())
            .or_else(|| parse_binance_stream_symbol(&value));
        let Some(venue_symbol) = parsed_symbol else {
            return Err(DataError::InvalidResponse(
                "binance depth payload missing symbol".to_owned(),
            ));
        };
        if !expected_symbol.is_empty() && venue_symbol != expected_symbol.to_ascii_uppercase() {
            return Err(DataError::InvalidResponse(format!(
                "binance depth payload symbol mismatch: expected {}, got {}",
                expected_symbol, venue_symbol
            )));
        }

        Ok(Some(CexBookUpdate {
            exchange: Exchange::Binance,
            venue_symbol,
            bids: parse_depth_levels(bids_value)?,
            asks: parse_depth_levels(asks_value)?,
            exchange_timestamp_ms: root
                .get("E")
                .and_then(Value::as_u64)
                .or_else(|| root.get("T").and_then(Value::as_u64))
                .unwrap_or_else(now_millis),
            received_at_ms: now_millis(),
            sequence: root
                .get("lastUpdateId")
                .and_then(Value::as_u64)
                .or_else(|| root.get("u").and_then(Value::as_u64))
                .unwrap_or(0),
        }))
    }

    pub fn parse_premium_index_payload(
        payload: &str,
        venue_symbol: &str,
    ) -> Result<(CexFundingUpdate, CexTradeUpdate)> {
        let response: BinancePremiumIndexResponse = serde_json::from_str(payload)?;
        let mark_price = parse_decimal(&response.mark_price)?;
        let funding_rate = parse_decimal(&response.last_funding_rate)?;
        let ts = response.time.unwrap_or_else(now_millis);

        Ok((
            CexFundingUpdate {
                exchange: Exchange::Binance,
                venue_symbol: venue_symbol.to_owned(),
                rate: funding_rate,
                next_funding_time_ms: response.next_funding_time,
            },
            CexTradeUpdate {
                exchange: Exchange::Binance,
                venue_symbol: venue_symbol.to_owned(),
                price: polyalpha_core::Price(mark_price),
                quantity: CexBaseQty(Decimal::ZERO),
                side: OrderSide::Buy,
                timestamp_ms: ts,
            },
        ))
    }
}

#[async_trait]
impl MarketDataSource for BinanceFuturesDataSource {
    async fn connect(&mut self) -> polyalpha_core::Result<()> {
        self.mark_connecting();
        Ok(())
    }

    async fn subscribe_market(&self, symbol: &Symbol) -> polyalpha_core::Result<()> {
        self.subscriptions
            .lock()
            .map_err(|_| CoreError::Channel("binance subscriptions lock poisoned".to_owned()))?
            .insert(symbol.clone());
        Ok(())
    }

    fn connection_status(&self) -> ConnectionStatus {
        *self.status.lock().expect("binance status lock poisoned")
    }

    fn exchange(&self) -> Exchange {
        Exchange::Binance
    }
}

#[derive(Debug, Deserialize)]
struct BinanceDepthResponse {
    #[serde(rename = "E")]
    event_time: Option<u64>,
    #[serde(rename = "lastUpdateId")]
    last_update_id: Option<u64>,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

fn parse_depth_levels(value: &Value) -> Result<Vec<CexBookLevel>> {
    let levels = value.as_array().ok_or_else(|| {
        DataError::InvalidResponse("binance depth levels must be an array".to_owned())
    })?;

    levels
        .iter()
        .map(|level| {
            let items = level.as_array().ok_or_else(|| {
                DataError::InvalidResponse("binance depth level must be [price, qty]".to_owned())
            })?;
            if items.len() < 2 {
                return Err(DataError::InvalidResponse(
                    "binance depth level requires [price, qty]".to_owned(),
                ));
            }
            let price = items[0]
                .as_str()
                .ok_or_else(|| {
                    DataError::InvalidResponse("binance level price must be string".to_owned())
                })
                .and_then(parse_decimal)?;
            let qty = items[1]
                .as_str()
                .ok_or_else(|| {
                    DataError::InvalidResponse("binance level qty must be string".to_owned())
                })
                .and_then(parse_decimal)?;
            Ok(CexBookLevel {
                price: polyalpha_core::Price(price),
                base_qty: CexBaseQty(qty),
            })
        })
        .collect()
}

#[derive(Debug, Deserialize)]
struct BinancePremiumIndexResponse {
    #[serde(rename = "markPrice")]
    mark_price: String,
    #[serde(rename = "lastFundingRate")]
    last_funding_rate: String,
    #[serde(rename = "nextFundingTime")]
    next_funding_time: u64,
    time: Option<u64>,
}

fn parse_binance_stream_symbol(value: &Value) -> Option<String> {
    value
        .get("stream")
        .and_then(Value::as_str)
        .and_then(|stream| stream.split('@').next())
        .filter(|symbol| !symbol.is_empty())
        .map(|symbol| symbol.to_ascii_uppercase())
}

fn parse_decimal(raw: &str) -> Result<Decimal> {
    Decimal::from_str(raw).map_err(|err| DataError::Decimal(err.to_string()))
}

fn now_millis() -> u64 {
    let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    duration.as_millis() as u64
}

#[cfg(test)]
mod tests {
    use tokio::sync::broadcast::error::TryRecvError;

    use polyalpha_core::{
        create_channels, ConnectionStatus, Exchange, MarketConfig, PolymarketIds, Price, Symbol,
        SymbolRegistry,
    };

    use super::*;
    use crate::{DataManager, MarketDataNormalizer};

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
    fn parse_binance_depth_payload() {
        let payload = r#"{
            "lastUpdateId": 105,
            "bids": [["100000.1","0.5"]],
            "asks": [["100000.2","0.4"]]
        }"#;
        let update =
            BinanceFuturesDataSource::parse_depth_payload(payload, "BTCUSDT").expect("parse ok");
        assert_eq!(update.exchange, Exchange::Binance);
        assert_eq!(update.venue_symbol, "BTCUSDT");
        assert_eq!(update.bids.len(), 1);
        assert_eq!(update.asks.len(), 1);
        assert_eq!(update.sequence, 105);
    }

    #[test]
    fn parse_binance_premium_index_payload() {
        let payload = r#"{
            "markPrice":"100123.45",
            "lastFundingRate":"0.00010000",
            "nextFundingTime":1715000000000,
            "time":1714999999000
        }"#;
        let (funding, mark) =
            BinanceFuturesDataSource::parse_premium_index_payload(payload, "BTCUSDT")
                .expect("parse ok");
        assert_eq!(funding.exchange, Exchange::Binance);
        assert_eq!(funding.venue_symbol, "BTCUSDT");
        assert_eq!(funding.next_funding_time_ms, 1_715_000_000_000);
        assert_eq!(
            mark.price.0,
            Decimal::from_str("100123.45").expect("decimal parse")
        );
    }

    #[test]
    fn build_binance_subscribe_message() {
        let msg = BinanceFuturesDataSource::build_depth_subscribe_message("BTCUSDT", 7);
        assert_eq!(msg["method"], "SUBSCRIBE");
        assert_eq!(msg["params"][0], "btcusdt@depth@100ms");
        assert_eq!(msg["id"], 7);
    }

    #[test]
    fn parse_binance_partial_depth_ws_payload_with_wrapper() {
        let payload = r#"{
            "stream":"btcusdt@depth20@100ms",
            "data":{
                "lastUpdateId": 201,
                "E": 1715000002000,
                "bids":[["100000.1","0.8"]],
                "asks":[["100000.3","0.6"]]
            }
        }"#;
        let update = BinanceFuturesDataSource::parse_partial_depth_ws_payload(payload, "BTCUSDT")
            .expect("parse ok")
            .expect("depth update");
        assert_eq!(update.venue_symbol, "BTCUSDT");
        assert_eq!(update.sequence, 201);
        assert_eq!(update.bids.len(), 1);
        assert_eq!(update.asks.len(), 1);
    }

    #[test]
    fn parse_binance_partial_depth_ws_payload_rejects_missing_symbol_metadata() {
        let payload = r#"{
            "data":{
                "lastUpdateId": 201,
                "E": 1715000002000,
                "bids":[["100000.1","0.8"]],
                "asks":[["100000.3","0.6"]]
            }
        }"#;

        let err = BinanceFuturesDataSource::parse_partial_depth_ws_payload(payload, "BTCUSDT")
            .expect_err("payload without stream or symbol must be rejected");
        assert!(
            matches!(err, DataError::InvalidResponse(message) if message.contains("missing symbol"))
        );
    }

    #[test]
    fn parse_binance_partial_depth_ws_payload_rejects_mismatched_symbol() {
        let payload = r#"{
            "stream":"ethusdt@depth20@100ms",
            "data":{
                "lastUpdateId": 201,
                "E": 1715000002000,
                "bids":[["100000.1","0.8"]],
                "asks":[["100000.3","0.6"]]
            }
        }"#;

        let err = BinanceFuturesDataSource::parse_partial_depth_ws_payload(payload, "BTCUSDT")
            .expect_err("payload with mismatched stream symbol must be rejected");
        assert!(
            matches!(err, DataError::InvalidResponse(message) if message.contains("symbol mismatch"))
        );
    }

    #[test]
    fn rest_status_changes_do_not_publish_connection_events() {
        let symbol = Symbol::new("btc-100k-mar-2026");
        let channels = create_channels(std::slice::from_ref(&symbol));
        let manager = DataManager::new(
            MarketDataNormalizer::new(sample_registry()),
            channels.market_data_tx.clone(),
        );
        let source = BinanceFuturesDataSource::new(manager, "https://fapi.binance.com");
        let mut rx = channels.market_data_tx.subscribe();

        source.mark_connected();
        assert_eq!(source.connection_status(), ConnectionStatus::Connected);
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

        source.mark_failure();
        assert_eq!(source.connection_status(), ConnectionStatus::Reconnecting);
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    }
}
