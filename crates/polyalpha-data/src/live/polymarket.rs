use std::collections::HashSet;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use reqwest::Client;
use rust_decimal::Decimal;
use serde_json::{json, Value};

use polyalpha_core::{
    ConnectionStatus, CoreError, Exchange, MarketDataEvent, MarketDataSource, SettlementRules,
    Symbol, TokenSide,
};

use crate::error::{DataError, Result};
use crate::manager::DataManager;
use crate::normalizer::{PolyBookLevel, PolyBookUpdate};

const DEFAULT_POLYMARKET_CLOB_URL: &str = "https://clob.polymarket.com";

/// Historical price data point from Polymarket
#[derive(Clone, Debug)]
pub struct PolyPricePoint {
    pub ts_ms: u64,
    pub price: f64,
}

#[derive(Clone, Debug)]
pub struct PolymarketLiveDataSource {
    manager: DataManager,
    client: Client,
    clob_api_url: String,
    settlement_rules: SettlementRules,
    status: Arc<Mutex<ConnectionStatus>>,
    subscriptions: Arc<Mutex<HashSet<Symbol>>>,
}

impl PolymarketLiveDataSource {
    fn publish_connection_event(&self, status: ConnectionStatus) {
        let _ = self.manager.publish(MarketDataEvent::ConnectionEvent {
            exchange: Exchange::Polymarket,
            status,
        });
    }

    fn set_status_if_changed(&self, new_status: ConnectionStatus) {
        let mut guard = self.status.lock().expect("polymarket status lock poisoned");
        if *guard == new_status {
            return;
        }
        *guard = new_status;
        drop(guard);
        self.publish_connection_event(new_status);
    }

    fn mark_connected(&self) {
        self.set_status_if_changed(ConnectionStatus::Connected);
    }

    fn mark_connecting(&self) {
        self.set_status_if_changed(ConnectionStatus::Connecting);
    }

    fn mark_failure(&self) {
        let mut guard = self.status.lock().expect("polymarket status lock poisoned");
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
        drop(guard);
        self.publish_connection_event(next);
    }
    pub fn new(
        manager: DataManager,
        clob_api_url: impl Into<String>,
        settlement_rules: SettlementRules,
    ) -> Self {
        Self {
            manager,
            client: Client::new(),
            clob_api_url: clob_api_url.into(),
            settlement_rules,
            status: Arc::new(Mutex::new(ConnectionStatus::Disconnected)),
            subscriptions: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn with_default_url(manager: DataManager, settlement_rules: SettlementRules) -> Self {
        Self::new(manager, DEFAULT_POLYMARKET_CLOB_URL, settlement_rules)
    }

    pub fn settlement_rules(&self) -> &SettlementRules {
        &self.settlement_rules
    }

    pub async fn fetch_orderbook_by_token_id(&self, token_id: &str) -> Result<PolyBookUpdate> {
        let endpoint = format!("{}/book", self.clob_api_url.trim_end_matches('/'));
        let payload = self
            .client
            .get(endpoint)
            .query(&[("token_id", token_id)])
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        Self::parse_orderbook_payload(&payload, token_id)
    }

    pub async fn fetch_orderbook_by_symbol(
        &self,
        symbol: &Symbol,
        token_side: TokenSide,
    ) -> Result<PolyBookUpdate> {
        let ids = self
            .manager
            .normalizer()
            .registry()
            .get_poly_ids(symbol)
            .ok_or_else(|| DataError::UnknownSymbol {
                symbol: symbol.0.clone(),
            })?;

        let token_id = match token_side {
            TokenSide::Yes => ids.yes_token_id.as_str(),
            TokenSide::No => ids.no_token_id.as_str(),
        };
        self.fetch_orderbook_by_token_id(token_id).await
    }

    pub async fn fetch_and_publish_orderbook_by_token_id(&self, token_id: &str) -> Result<usize> {
        let update = match self.fetch_orderbook_by_token_id(token_id).await {
            Ok(u) => {
                self.mark_connected();
                u
            }
            Err(e) => {
                self.mark_failure();
                return Err(e);
            }
        };
        self.manager.normalize_and_publish_poly_orderbook(update)
    }

    pub async fn fetch_and_publish_orderbook_by_symbol(
        &self,
        symbol: &Symbol,
        token_side: TokenSide,
    ) -> Result<usize> {
        let update = match self.fetch_orderbook_by_symbol(symbol, token_side).await {
            Ok(u) => {
                self.mark_connected();
                u
            }
            Err(e) => {
                self.mark_failure();
                return Err(e);
            }
        };

        self.manager.normalize_and_publish_poly_orderbook(update)
    }

    /// Fetch historical price data from prices-history API
    /// Returns price points for the given time range (last 6 hours by default)
    pub async fn fetch_price_history(
        &self,
        token_id: &str,
        start_ts: u64,
        end_ts: u64,
    ) -> Result<Vec<PolyPricePoint>> {
        let endpoint = format!("{}/prices-history", self.clob_api_url.trim_end_matches('/'));
        let payload = self
            .client
            .get(endpoint)
            .query(&[
                ("market", token_id),
                ("startTs", &start_ts.to_string()),
                ("endTs", &end_ts.to_string()),
                ("fidelity", "1"),
            ])
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        Self::parse_price_history_payload(&payload)
    }

    pub async fn fetch_price_history_by_symbol(
        &self,
        symbol: &Symbol,
        token_side: TokenSide,
        start_ts: u64,
        end_ts: u64,
    ) -> Result<Vec<PolyPricePoint>> {
        let ids = self
            .manager
            .normalizer()
            .registry()
            .get_poly_ids(symbol)
            .ok_or_else(|| DataError::UnknownSymbol {
                symbol: symbol.0.clone(),
            })?;

        let token_id = match token_side {
            TokenSide::Yes => ids.yes_token_id.as_str(),
            TokenSide::No => ids.no_token_id.as_str(),
        };
        self.fetch_price_history(token_id, start_ts, end_ts).await
    }

    fn parse_price_history_payload(payload: &str) -> Result<Vec<PolyPricePoint>> {
        let value: Value = serde_json::from_str(payload)?;
        let history = value
            .get("history")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                DataError::InvalidResponse(
                    "missing history array in price history response".to_owned(),
                )
            })?;

        let mut points = Vec::with_capacity(history.len());
        for item in history {
            let ts = item
                .get("t")
                .and_then(|v| v.as_u64())
                .or_else(|| item.get("t").and_then(|v| v.as_i64()).map(|v| v as u64));
            let price = item.get("p").and_then(|v| v.as_f64()).or_else(|| {
                item.get("p")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse().ok())
            });

            if let (Some(ts), Some(price)) = (ts, price) {
                // Convert seconds to milliseconds
                let ts_ms = if ts < 1_000_000_000_000 {
                    ts * 1000
                } else {
                    ts
                };
                points.push(PolyPricePoint { ts_ms, price });
            }
        }

        // Sort by timestamp
        points.sort_by_key(|p| p.ts_ms);
        Ok(points)
    }

    pub fn build_orderbook_subscribe_message(asset_ids: &[String]) -> Value {
        json!({
            "type": "market",
            "assets_ids": asset_ids,
            "custom_feature_enabled": true,
        })
    }

    pub fn parse_ws_orderbook_payload(
        payload: &str,
        fallback_token_id: &str,
    ) -> Result<PolyBookUpdate> {
        Self::parse_orderbook_payload(payload, fallback_token_id)
    }

    pub fn parse_orderbook_payload(
        payload: &str,
        fallback_token_id: &str,
    ) -> Result<PolyBookUpdate> {
        let value: Value = serde_json::from_str(payload)?;
        let root = if let Some(book) = value.get("book") {
            book
        } else {
            &value
        };

        let asset_id = root
            .get("asset_id")
            .and_then(Value::as_str)
            .or_else(|| root.get("token_id").and_then(Value::as_str))
            .unwrap_or(fallback_token_id)
            .to_owned();

        let bids = parse_poly_levels(root.get("bids"))?;
        let asks = parse_poly_levels(root.get("asks"))?;
        let exchange_timestamp_ms =
            parse_ts_millis(root.get("timestamp").or_else(|| root.get("ts")));
        let sequence =
            parse_u64_any(root.get("sequence").or_else(|| root.get("hash"))).unwrap_or(0);

        // Extract last_trade_price from the API response
        let last_trade_price = value
            .get("last_trade_price")
            .and_then(|v| match v {
                Value::String(s) => Decimal::from_str(s).ok(),
                Value::Number(n) => Decimal::from_str(&n.to_string()).ok(),
                _ => None,
            })
            .map(polyalpha_core::Price);

        Ok(PolyBookUpdate {
            asset_id,
            bids,
            asks,
            exchange_timestamp_ms,
            received_at_ms: now_millis(),
            sequence,
            last_trade_price,
        })
    }
}

#[async_trait]
impl MarketDataSource for PolymarketLiveDataSource {
    async fn connect(&mut self) -> polyalpha_core::Result<()> {
        self.mark_connecting();
        Ok(())
    }

    async fn subscribe_market(&self, symbol: &Symbol) -> polyalpha_core::Result<()> {
        self.subscriptions
            .lock()
            .map_err(|_| CoreError::Channel("polymarket subscriptions lock poisoned".to_owned()))?
            .insert(symbol.clone());
        Ok(())
    }

    fn connection_status(&self) -> ConnectionStatus {
        *self.status.lock().expect("polymarket status lock poisoned")
    }

    fn exchange(&self) -> Exchange {
        Exchange::Polymarket
    }
}

fn parse_poly_levels(value: Option<&Value>) -> Result<Vec<PolyBookLevel>> {
    let mut out = Vec::new();
    let Some(Value::Array(levels)) = value else {
        return Ok(out);
    };

    for level in levels {
        match level {
            Value::Array(items) if items.len() >= 2 => {
                let price = parse_decimal_any(Some(&items[0]))?;
                let size = parse_decimal_any(Some(&items[1]))?;
                out.push(PolyBookLevel {
                    price: polyalpha_core::Price(price),
                    shares: polyalpha_core::PolyShares(size),
                });
            }
            Value::Object(map) => {
                let price = parse_decimal_any(
                    map.get("price")
                        .or_else(|| map.get("px"))
                        .or_else(|| map.get("p")),
                )?;
                let size = parse_decimal_any(
                    map.get("size")
                        .or_else(|| map.get("qty"))
                        .or_else(|| map.get("quantity"))
                        .or_else(|| map.get("s")),
                )?;
                out.push(PolyBookLevel {
                    price: polyalpha_core::Price(price),
                    shares: polyalpha_core::PolyShares(size),
                });
            }
            _ => {
                return Err(DataError::InvalidResponse(
                    "polymarket level must be [price, size] or {price,size}".to_owned(),
                ));
            }
        }
    }

    Ok(out)
}

fn parse_decimal_any(value: Option<&Value>) -> Result<Decimal> {
    let Some(value) = value else {
        return Err(DataError::InvalidResponse(
            "missing decimal field".to_owned(),
        ));
    };

    match value {
        Value::String(raw) => {
            Decimal::from_str(raw).map_err(|err| DataError::Decimal(err.to_string()))
        }
        Value::Number(number) => Decimal::from_str(&number.to_string())
            .map_err(|err| DataError::Decimal(err.to_string())),
        _ => Err(DataError::InvalidResponse(
            "decimal field must be string or number".to_owned(),
        )),
    }
}

fn parse_u64_any(value: Option<&Value>) -> Option<u64> {
    match value {
        Some(Value::Number(number)) => number.as_u64(),
        Some(Value::String(raw)) => raw.parse::<u64>().ok(),
        _ => None,
    }
}

fn parse_ts_millis(value: Option<&Value>) -> u64 {
    parse_u64_any(value).unwrap_or_else(now_millis)
}

fn now_millis() -> u64 {
    let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    duration.as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_polymarket_orderbook_object_levels() {
        let payload = r#"
        {
          "asset_id": "yes-1",
          "timestamp": "1715000000000",
          "sequence": "42",
          "bids": [{"price":"0.49","size":"12"}],
          "asks": [{"price":"0.51","size":"13"}]
        }
        "#;

        let update = PolymarketLiveDataSource::parse_orderbook_payload(payload, "fallback")
            .expect("payload should parse");
        assert_eq!(update.asset_id, "yes-1");
        assert_eq!(update.bids.len(), 1);
        assert_eq!(update.asks.len(), 1);
        assert_eq!(update.exchange_timestamp_ms, 1_715_000_000_000);
        assert_eq!(update.sequence, 42);
    }

    #[test]
    fn parse_polymarket_orderbook_array_levels() {
        let payload = r#"
        {
          "book": {
            "token_id": "no-1",
            "ts": "1715000000123",
            "hash": "9",
            "bids": [["0.61", "8"]],
            "asks": [["0.62", "7"]]
          }
        }
        "#;

        let update = PolymarketLiveDataSource::parse_orderbook_payload(payload, "fallback")
            .expect("payload should parse");
        assert_eq!(update.asset_id, "no-1");
        assert_eq!(update.bids.len(), 1);
        assert_eq!(update.asks.len(), 1);
        assert_eq!(update.exchange_timestamp_ms, 1_715_000_000_123);
        assert_eq!(update.sequence, 9);
    }

    #[test]
    fn build_subscribe_message_contains_asset_ids() {
        let message = PolymarketLiveDataSource::build_orderbook_subscribe_message(&[
            "yes-1".to_owned(),
            "no-1".to_owned(),
        ]);
        assert_eq!(message["type"], "market");
        assert_eq!(message["assets_ids"][0], "yes-1");
        assert_eq!(message["assets_ids"][1], "no-1");
        assert_eq!(message["custom_feature_enabled"], true);
    }
}
