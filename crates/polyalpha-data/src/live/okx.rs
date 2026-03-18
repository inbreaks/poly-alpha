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

const DEFAULT_OKX_REST_URL: &str = "https://www.okx.com";

#[derive(Clone, Debug)]
pub struct OkxMarketDataSource {
    manager: DataManager,
    client: Client,
    rest_url: String,
    status: Arc<Mutex<ConnectionStatus>>,
    subscriptions: Arc<Mutex<HashSet<Symbol>>>,
}

impl OkxMarketDataSource {
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
        Self::new(manager, DEFAULT_OKX_REST_URL)
    }

    pub async fn fetch_orderbook(&self, venue_symbol: &str, depth: u16) -> Result<CexBookUpdate> {
        let inst_id = okx_inst_id(venue_symbol);
        let endpoint = format!(
            "{}/api/v5/market/books",
            self.rest_url.trim_end_matches('/')
        );
        let payload = self
            .client
            .get(endpoint)
            .query(&[("instId", inst_id.as_str()), ("sz", &depth.to_string())])
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        Self::parse_orderbook_payload(&payload, venue_symbol)
    }

    pub async fn fetch_funding_and_mark(
        &self,
        venue_symbol: &str,
    ) -> Result<(CexFundingUpdate, CexTradeUpdate)> {
        let inst_id = okx_inst_id(venue_symbol);
        let funding_endpoint = format!(
            "{}/api/v5/public/funding-rate",
            self.rest_url.trim_end_matches('/')
        );
        let funding_payload = self
            .client
            .get(funding_endpoint)
            .query(&[("instId", inst_id.as_str())])
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;

        let mark_endpoint = format!(
            "{}/api/v5/public/mark-price",
            self.rest_url.trim_end_matches('/')
        );
        let mark_payload = self
            .client
            .get(mark_endpoint)
            .query(&[("instType", "SWAP"), ("instId", inst_id.as_str())])
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;

        Self::parse_funding_and_mark_payloads(&funding_payload, &mark_payload, venue_symbol)
    }

    pub async fn fetch_and_publish_orderbook(
        &self,
        venue_symbol: &str,
        depth: u16,
    ) -> Result<usize> {
        let update = self.fetch_orderbook(venue_symbol, depth).await?;
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
        depth: u16,
    ) -> Result<usize> {
        let venue_symbol = self
            .manager
            .normalizer()
            .registry()
            .get_cex_symbol(symbol)
            .ok_or_else(|| DataError::UnknownSymbol {
                symbol: symbol.0.clone(),
            })?;
        self.fetch_and_publish_orderbook(venue_symbol, depth).await
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

    pub fn build_books_subscribe_message(inst_id: &str) -> Value {
        json!({
            "op": "subscribe",
            "args": [{
                "channel": "books",
                "instId": inst_id
            }]
        })
    }

    pub fn parse_orderbook_ws_payload(payload: &str, venue_symbol: &str) -> Result<CexBookUpdate> {
        let ws: OkxWsBooksEvent = serde_json::from_str(payload)?;
        let first = ws
            .data
            .first()
            .ok_or_else(|| DataError::InvalidResponse("okx ws books data is empty".to_owned()))?;
        Ok(CexBookUpdate {
            exchange: Exchange::Okx,
            venue_symbol: venue_symbol.to_owned(),
            bids: parse_okx_levels(&first.bids)?,
            asks: parse_okx_levels(&first.asks)?,
            exchange_timestamp_ms: parse_ts(&first.ts),
            received_at_ms: now_millis(),
            sequence: 0,
        })
    }

    pub fn parse_orderbook_payload(payload: &str, venue_symbol: &str) -> Result<CexBookUpdate> {
        let response: OkxEnvelope<OkxBooksData> = serde_json::from_str(payload)?;
        if response.code != "0" {
            return Err(DataError::InvalidResponse(format!(
                "okx books response code {}",
                response.code
            )));
        }
        let first = response
            .data
            .first()
            .ok_or_else(|| DataError::InvalidResponse("okx books data is empty".to_owned()))?;

        Ok(CexBookUpdate {
            exchange: Exchange::Okx,
            venue_symbol: venue_symbol.to_owned(),
            bids: parse_okx_levels(&first.bids)?,
            asks: parse_okx_levels(&first.asks)?,
            exchange_timestamp_ms: parse_ts(&first.ts),
            received_at_ms: now_millis(),
            sequence: 0,
        })
    }

    pub fn parse_funding_and_mark_payloads(
        funding_payload: &str,
        mark_payload: &str,
        venue_symbol: &str,
    ) -> Result<(CexFundingUpdate, CexTradeUpdate)> {
        let funding_response: OkxEnvelope<OkxFundingData> = serde_json::from_str(funding_payload)?;
        if funding_response.code != "0" {
            return Err(DataError::InvalidResponse(format!(
                "okx funding response code {}",
                funding_response.code
            )));
        }
        let funding = funding_response
            .data
            .first()
            .ok_or_else(|| DataError::InvalidResponse("okx funding data is empty".to_owned()))?;

        let mark_response: OkxEnvelope<OkxMarkPriceData> = serde_json::from_str(mark_payload)?;
        if mark_response.code != "0" {
            return Err(DataError::InvalidResponse(format!(
                "okx mark response code {}",
                mark_response.code
            )));
        }
        let mark = mark_response
            .data
            .first()
            .ok_or_else(|| DataError::InvalidResponse("okx mark data is empty".to_owned()))?;

        Ok((
            CexFundingUpdate {
                exchange: Exchange::Okx,
                venue_symbol: venue_symbol.to_owned(),
                rate: parse_decimal(&funding.funding_rate)?,
                next_funding_time_ms: parse_ts(&funding.next_funding_time),
            },
            CexTradeUpdate {
                exchange: Exchange::Okx,
                venue_symbol: venue_symbol.to_owned(),
                price: polyalpha_core::Price(parse_decimal(&mark.mark_px)?),
                quantity: CexBaseQty(Decimal::ZERO),
                side: OrderSide::Buy,
                timestamp_ms: mark.ts.as_deref().map(parse_ts).unwrap_or_else(now_millis),
            },
        ))
    }
}

#[async_trait]
impl MarketDataSource for OkxMarketDataSource {
    async fn connect(&mut self) -> polyalpha_core::Result<()> {
        *self
            .status
            .lock()
            .map_err(|_| CoreError::Channel("okx status lock poisoned".to_owned()))? =
            ConnectionStatus::Connected;
        Ok(())
    }

    async fn subscribe_market(&self, symbol: &Symbol) -> polyalpha_core::Result<()> {
        self.subscriptions
            .lock()
            .map_err(|_| CoreError::Channel("okx subscriptions lock poisoned".to_owned()))?
            .insert(symbol.clone());
        Ok(())
    }

    fn connection_status(&self) -> ConnectionStatus {
        *self.status.lock().expect("okx status lock poisoned")
    }

    fn exchange(&self) -> Exchange {
        Exchange::Okx
    }
}

#[derive(Debug, Deserialize)]
struct OkxEnvelope<T> {
    code: String,
    data: Vec<T>,
}

#[derive(Debug, Deserialize)]
struct OkxBooksData {
    bids: Vec<Vec<String>>,
    asks: Vec<Vec<String>>,
    ts: String,
}

#[derive(Debug, Deserialize)]
struct OkxFundingData {
    #[serde(rename = "fundingRate")]
    funding_rate: String,
    #[serde(rename = "nextFundingTime")]
    next_funding_time: String,
}

#[derive(Debug, Deserialize)]
struct OkxMarkPriceData {
    #[serde(rename = "markPx")]
    mark_px: String,
    ts: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OkxWsBooksEvent {
    data: Vec<OkxBooksData>,
}

fn parse_okx_levels(levels: &[Vec<String>]) -> Result<Vec<CexBookLevel>> {
    levels
        .iter()
        .map(|level| {
            if level.len() < 2 {
                return Err(DataError::InvalidResponse(
                    "okx level needs at least [price, size]".to_owned(),
                ));
            }
            Ok(CexBookLevel {
                price: polyalpha_core::Price(parse_decimal(&level[0])?),
                base_qty: CexBaseQty(parse_decimal(&level[1])?),
            })
        })
        .collect()
}

fn parse_decimal(raw: &str) -> Result<Decimal> {
    Decimal::from_str(raw).map_err(|err| DataError::Decimal(err.to_string()))
}

fn parse_ts(raw: &str) -> u64 {
    raw.parse::<u64>().unwrap_or_else(|_| now_millis())
}

fn now_millis() -> u64 {
    let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    duration.as_millis() as u64
}

fn okx_inst_id(venue_symbol: &str) -> String {
    if venue_symbol.contains('-') {
        return venue_symbol.to_owned();
    }
    if let Some(base) = venue_symbol.strip_suffix("USDT") {
        return format!("{base}-USDT-SWAP");
    }
    venue_symbol.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_okx_books_payload() {
        let payload = r#"{
            "code":"0",
            "data":[{"bids":[["100.1","2.3","0","1"]],"asks":[["100.2","1.8","0","1"]],"ts":"1715000001000"}]
        }"#;
        let update =
            OkxMarketDataSource::parse_orderbook_payload(payload, "BTCUSDT").expect("parse ok");
        assert_eq!(update.exchange, Exchange::Okx);
        assert_eq!(update.venue_symbol, "BTCUSDT");
        assert_eq!(update.bids.len(), 1);
        assert_eq!(update.asks.len(), 1);
        assert_eq!(update.exchange_timestamp_ms, 1_715_000_001_000);
    }

    #[test]
    fn parse_okx_funding_and_mark_payloads() {
        let funding_payload = r#"{
            "code":"0",
            "data":[{"fundingRate":"0.0002","nextFundingTime":"1715000800000"}]
        }"#;
        let mark_payload = r#"{
            "code":"0",
            "data":[{"markPx":"100.25","ts":"1715000000001"}]
        }"#;
        let (funding, mark) = OkxMarketDataSource::parse_funding_and_mark_payloads(
            funding_payload,
            mark_payload,
            "BTCUSDT",
        )
        .expect("parse ok");
        assert_eq!(funding.exchange, Exchange::Okx);
        assert_eq!(funding.next_funding_time_ms, 1_715_000_800_000);
        assert_eq!(
            mark.price.0,
            Decimal::from_str("100.25").expect("decimal parse")
        );
    }

    #[test]
    fn build_okx_subscribe_message() {
        let msg = OkxMarketDataSource::build_books_subscribe_message("BTC-USDT-SWAP");
        assert_eq!(msg["op"], "subscribe");
        assert_eq!(msg["args"][0]["channel"], "books");
        assert_eq!(msg["args"][0]["instId"], "BTC-USDT-SWAP");
    }

    #[test]
    fn convert_plain_symbol_to_okx_inst_id() {
        assert_eq!(okx_inst_id("BTCUSDT"), "BTC-USDT-SWAP");
        assert_eq!(okx_inst_id("BTC-USDT-SWAP"), "BTC-USDT-SWAP");
    }
}
