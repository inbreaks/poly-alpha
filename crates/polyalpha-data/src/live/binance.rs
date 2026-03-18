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

#[derive(Clone, Debug)]
pub struct BinanceFuturesDataSource {
    manager: DataManager,
    client: Client,
    rest_url: String,
    status: Arc<Mutex<ConnectionStatus>>,
    subscriptions: Arc<Mutex<HashSet<Symbol>>>,
}

impl BinanceFuturesDataSource {
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
        let update = self.fetch_orderbook(venue_symbol, depth_limit).await?;
        self.manager.normalize_and_publish_cex_orderbook(update)
    }

    pub async fn fetch_and_publish_funding_and_mark(&self, venue_symbol: &str) -> Result<usize> {
        let (funding, mark) = self.fetch_funding_and_mark(venue_symbol).await?;
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

    pub fn build_depth_subscribe_message(venue_symbol: &str, id: u64) -> Value {
        let stream = format!("{}@depth@100ms", venue_symbol.to_ascii_lowercase());
        json!({
            "method": "SUBSCRIBE",
            "params": [stream],
            "id": id
        })
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
        let ws: BinanceDepthWsEvent = serde_json::from_str(payload)?;
        Ok(CexBookUpdate {
            exchange: Exchange::Binance,
            venue_symbol: ws.symbol,
            bids: ws
                .bids
                .into_iter()
                .map(|[price, qty]| -> Result<CexBookLevel> {
                    Ok(CexBookLevel {
                        price: polyalpha_core::Price(parse_decimal(&price)?),
                        base_qty: CexBaseQty(parse_decimal(&qty)?),
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            asks: ws
                .asks
                .into_iter()
                .map(|[price, qty]| -> Result<CexBookLevel> {
                    Ok(CexBookLevel {
                        price: polyalpha_core::Price(parse_decimal(&price)?),
                        base_qty: CexBaseQty(parse_decimal(&qty)?),
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            exchange_timestamp_ms: ws.event_time,
            received_at_ms: now_millis(),
            sequence: ws.final_update_id,
        })
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
        *self
            .status
            .lock()
            .map_err(|_| CoreError::Channel("binance status lock poisoned".to_owned()))? =
            ConnectionStatus::Connected;
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

#[derive(Debug, Deserialize)]
struct BinanceDepthWsEvent {
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "u")]
    final_update_id: u64,
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
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
    use super::*;

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
}
