use std::collections::HashSet;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use reqwest::Client;
use rust_decimal::Decimal;
use serde_json::{json, Value};

use polyalpha_core::{
    ConnectionStatus, CoreError, Exchange, MarketDataSource, SettlementRules, Symbol, TokenSide,
};

use crate::error::{DataError, Result};
use crate::manager::DataManager;
use crate::normalizer::{PolyBookLevel, PolyBookUpdate};

const DEFAULT_POLYMARKET_CLOB_URL: &str = "https://clob.polymarket.com";

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
        let update = self.fetch_orderbook_by_token_id(token_id).await?;
        self.manager.normalize_and_publish_poly_orderbook(update)
    }

    pub async fn fetch_and_publish_orderbook_by_symbol(
        &self,
        symbol: &Symbol,
        token_side: TokenSide,
    ) -> Result<usize> {
        let update = self.fetch_orderbook_by_symbol(symbol, token_side).await?;
        self.manager.normalize_and_publish_poly_orderbook(update)
    }

    pub fn build_orderbook_subscribe_message(asset_ids: &[String]) -> Value {
        json!({
            "type": "subscribe",
            "channel": "market",
            "asset_ids": asset_ids,
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

        Ok(PolyBookUpdate {
            asset_id,
            bids,
            asks,
            exchange_timestamp_ms,
            received_at_ms: now_millis(),
            sequence,
        })
    }
}

#[async_trait]
impl MarketDataSource for PolymarketLiveDataSource {
    async fn connect(&mut self) -> polyalpha_core::Result<()> {
        *self
            .status
            .lock()
            .map_err(|_| CoreError::Channel("polymarket status lock poisoned".to_owned()))? =
            ConnectionStatus::Connected;
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
        assert_eq!(message["type"], "subscribe");
        assert_eq!(message["asset_ids"][0], "yes-1");
        assert_eq!(message["asset_ids"][1], "no-1");
    }
}
