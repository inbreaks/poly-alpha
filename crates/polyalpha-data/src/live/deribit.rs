use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use reqwest::Client;
use serde::Deserialize;

use crate::error::{DataError, Result};

const DEFAULT_REPRUNE_INDEX_DRIFT_BPS: f64 = 500.0;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DeribitAsset {
    Btc,
    Eth,
}

impl DeribitAsset {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Btc => "BTC",
            Self::Eth => "ETH",
        }
    }

    pub fn from_symbol(value: &str) -> Result<Self> {
        match value {
            "BTC" => Ok(Self::Btc),
            "ETH" => Ok(Self::Eth),
            other => Err(DataError::InvalidResponse(format!(
                "unsupported Deribit asset `{other}`"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeribitOptionType {
    Call,
    Put,
}

impl DeribitOptionType {
    fn from_code(value: &str) -> Result<Self> {
        match value {
            "C" => Ok(Self::Call),
            "P" => Ok(Self::Put),
            other => Err(DataError::InvalidResponse(format!(
                "unsupported Deribit option type `{other}`"
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DeribitInstrument {
    pub instrument_name: String,
    pub asset: DeribitAsset,
    pub strike: f64,
    pub expiry_ts_ms: u64,
    pub option_type: DeribitOptionType,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DeribitTickerMessage {
    pub instrument_name: String,
    pub asset: DeribitAsset,
    pub expiry_ts_ms: u64,
    pub strike: f64,
    pub option_type: DeribitOptionType,
    pub timestamp_ms: u64,
    pub mark_price: f64,
    pub mark_iv: f64,
    pub best_bid_price: Option<f64>,
    pub best_ask_price: Option<f64>,
    pub bid_iv: Option<f64>,
    pub ask_iv: Option<f64>,
    pub delta: Option<f64>,
    pub gamma: Option<f64>,
    pub index_price: Option<f64>,
}

impl DeribitTickerMessage {
    pub fn from_text(payload: &str) -> Result<Self> {
        let envelope: DeribitSubscriptionEnvelope = serde_json::from_str(payload)?;
        let Some(data) = envelope.params.map(|params| params.data) else {
            return Err(DataError::InvalidResponse(
                "missing Deribit subscription params.data".to_owned(),
            ));
        };
        let parsed = ParsedInstrumentName::parse(&data.instrument_name)?;
        Ok(Self {
            instrument_name: data.instrument_name,
            asset: parsed.asset,
            expiry_ts_ms: parsed.expiry_ts_ms,
            strike: parsed.strike,
            option_type: parsed.option_type,
            timestamp_ms: data.timestamp,
            mark_price: data.mark_price,
            mark_iv: data.mark_iv,
            best_bid_price: data.best_bid_price,
            best_ask_price: data.best_ask_price,
            bid_iv: data.bid_iv,
            ask_iv: data.ask_iv,
            delta: data.greeks.as_ref().and_then(|greeks| greeks.delta),
            gamma: data.greeks.as_ref().and_then(|greeks| greeks.gamma),
            index_price: data.index_price.or(data.underlying_price),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DiscoveryFilter {
    pub max_relative_strike_distance: f64,
    pub max_expiry_days: u32,
    pub reprune_interval_secs: u64,
}

impl DiscoveryFilter {
    pub fn new(
        max_relative_strike_distance: f64,
        max_expiry_days: u32,
        reprune_interval_secs: u64,
    ) -> Self {
        Self {
            max_relative_strike_distance,
            max_expiry_days,
            reprune_interval_secs,
        }
    }

    pub fn allows(&self, instrument: &DeribitInstrument, index_price: f64, now_ts_ms: u64) -> bool {
        if !index_price.is_finite() || index_price <= 0.0 {
            return false;
        }
        let relative_distance = (instrument.strike - index_price).abs() / index_price;
        if relative_distance > self.max_relative_strike_distance {
            return false;
        }
        if instrument.expiry_ts_ms <= now_ts_ms {
            return false;
        }
        let expiry_days =
            (instrument.expiry_ts_ms - now_ts_ms) as f64 / (24.0 * 60.0 * 60.0 * 1000.0);
        expiry_days <= self.max_expiry_days as f64
    }

    pub fn should_reprime(
        &self,
        last_prune_ts_ms: Option<u64>,
        last_prune_index_price: Option<f64>,
        new_index_price: f64,
        now_ts_ms: u64,
    ) -> bool {
        let Some(last_prune_ts_ms) = last_prune_ts_ms else {
            return true;
        };
        if now_ts_ms.saturating_sub(last_prune_ts_ms) >= self.reprune_interval_secs * 1000 {
            return true;
        }
        let Some(last_prune_index_price) = last_prune_index_price else {
            return true;
        };
        if !last_prune_index_price.is_finite() || last_prune_index_price <= 0.0 {
            return true;
        }
        ((new_index_price - last_prune_index_price).abs() / last_prune_index_price)
            >= DEFAULT_REPRUNE_INDEX_DRIFT_BPS / 10_000.0
    }
}

impl Default for DiscoveryFilter {
    fn default() -> Self {
        Self {
            max_relative_strike_distance: 0.15,
            max_expiry_days: 30,
            reprune_interval_secs: 600,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DeribitOptionsClient {
    #[allow(dead_code)]
    client: Client,
    #[allow(dead_code)]
    rest_url: String,
    #[allow(dead_code)]
    ws_url: String,
    discovery_filter: DiscoveryFilter,
    connected: Arc<Mutex<bool>>,
    instruments: Arc<Mutex<HashMap<DeribitAsset, Vec<DeribitInstrument>>>>,
    tickers: Arc<Mutex<HashMap<DeribitAsset, HashMap<String, DeribitTickerMessage>>>>,
    subscribed_instruments: Arc<Mutex<HashMap<DeribitAsset, HashSet<String>>>>,
    prune_state: Arc<Mutex<HashMap<DeribitAsset, DiscoveryState>>>,
}

impl DeribitOptionsClient {
    pub fn new(
        rest_url: impl Into<String>,
        ws_url: impl Into<String>,
        discovery_filter: DiscoveryFilter,
    ) -> Self {
        Self {
            client: Client::new(),
            rest_url: rest_url.into(),
            ws_url: ws_url.into(),
            discovery_filter,
            connected: Arc::new(Mutex::new(false)),
            instruments: Arc::new(Mutex::new(HashMap::new())),
            tickers: Arc::new(Mutex::new(HashMap::new())),
            subscribed_instruments: Arc::new(Mutex::new(HashMap::new())),
            prune_state: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn connect(&mut self) -> Result<()> {
        *self
            .connected
            .lock()
            .expect("deribit connected lock poisoned") = true;
        Ok(())
    }

    pub async fn prime_asset(&self, asset: DeribitAsset) -> Result<()> {
        self.ensure_connected()?;
        self.instruments
            .lock()
            .expect("deribit instruments lock poisoned")
            .entry(asset)
            .or_default();
        self.tickers
            .lock()
            .expect("deribit tickers lock poisoned")
            .entry(asset)
            .or_default();
        Ok(())
    }

    pub fn latest_tickers(&self, asset: DeribitAsset) -> Vec<DeribitTickerMessage> {
        let mut values = self
            .tickers
            .lock()
            .expect("deribit tickers lock poisoned")
            .get(&asset)
            .map(|by_instrument| by_instrument.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        values.sort_by(|left, right| left.instrument_name.cmp(&right.instrument_name));
        values
    }

    pub async fn refresh_subscriptions(&self, asset: DeribitAsset) -> Result<()> {
        self.ensure_connected()?;
        let index_price = self
            .latest_tickers(asset)
            .into_iter()
            .find_map(|ticker| ticker.index_price)
            .ok_or_else(|| {
                DataError::InvalidResponse(format!(
                    "missing index price for Deribit asset {}",
                    asset.as_str()
                ))
            })?;
        let now_ts_ms = current_time_ms()?;
        let selected = self.selected_instruments(asset, index_price, now_ts_ms);
        self.subscribed_instruments
            .lock()
            .expect("deribit subscriptions lock poisoned")
            .insert(
                asset,
                selected
                    .iter()
                    .map(|instrument| instrument.instrument_name.clone())
                    .collect(),
            );
        self.prune_state
            .lock()
            .expect("deribit prune state lock poisoned")
            .insert(
                asset,
                DiscoveryState {
                    last_prune_ts_ms: Some(now_ts_ms),
                    last_prune_index_price: Some(index_price),
                },
            );
        Ok(())
    }

    pub fn replace_instruments(&self, asset: DeribitAsset, instruments: Vec<DeribitInstrument>) {
        self.instruments
            .lock()
            .expect("deribit instruments lock poisoned")
            .insert(asset, instruments);
    }

    pub fn ingest_ticker(&self, ticker: DeribitTickerMessage) {
        self.tickers
            .lock()
            .expect("deribit tickers lock poisoned")
            .entry(ticker.asset)
            .or_default()
            .insert(ticker.instrument_name.clone(), ticker);
    }

    pub fn selected_instruments(
        &self,
        asset: DeribitAsset,
        index_price: f64,
        now_ts_ms: u64,
    ) -> Vec<DeribitInstrument> {
        let mut selected = self
            .instruments
            .lock()
            .expect("deribit instruments lock poisoned")
            .get(&asset)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|instrument| {
                self.discovery_filter
                    .allows(instrument, index_price, now_ts_ms)
            })
            .collect::<Vec<_>>();
        selected.sort_by(|left, right| left.instrument_name.cmp(&right.instrument_name));
        selected
    }

    pub fn discovery_filter(&self) -> DiscoveryFilter {
        self.discovery_filter
    }

    pub fn should_refresh_subscriptions(
        &self,
        asset: DeribitAsset,
        index_price: f64,
        now_ts_ms: u64,
    ) -> bool {
        let state = self
            .prune_state
            .lock()
            .expect("deribit prune state lock poisoned")
            .get(&asset)
            .cloned()
            .unwrap_or_default();
        self.discovery_filter.should_reprime(
            state.last_prune_ts_ms,
            state.last_prune_index_price,
            index_price,
            now_ts_ms,
        )
    }

    fn ensure_connected(&self) -> Result<()> {
        if *self
            .connected
            .lock()
            .expect("deribit connected lock poisoned")
        {
            Ok(())
        } else {
            Err(DataError::NotConnected {
                exchange: polyalpha_core::Exchange::Deribit,
            })
        }
    }
}

#[derive(Clone, Debug, Default)]
struct DiscoveryState {
    last_prune_ts_ms: Option<u64>,
    last_prune_index_price: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct DeribitSubscriptionEnvelope {
    #[allow(dead_code)]
    jsonrpc: Option<String>,
    #[allow(dead_code)]
    method: Option<String>,
    params: Option<DeribitSubscriptionParams>,
}

#[derive(Debug, Deserialize)]
struct DeribitSubscriptionParams {
    #[allow(dead_code)]
    channel: String,
    data: DeribitTickerPayload,
}

#[derive(Debug, Deserialize)]
struct DeribitTickerPayload {
    instrument_name: String,
    timestamp: u64,
    mark_price: f64,
    mark_iv: f64,
    #[serde(default)]
    best_bid_price: Option<f64>,
    #[serde(default)]
    best_ask_price: Option<f64>,
    #[serde(default)]
    bid_iv: Option<f64>,
    #[serde(default)]
    ask_iv: Option<f64>,
    #[serde(default)]
    underlying_price: Option<f64>,
    #[serde(default)]
    index_price: Option<f64>,
    #[serde(default)]
    greeks: Option<DeribitGreeksPayload>,
}

#[derive(Debug, Deserialize)]
struct DeribitGreeksPayload {
    #[serde(default)]
    delta: Option<f64>,
    #[serde(default)]
    gamma: Option<f64>,
}

#[derive(Clone, Copy, Debug)]
struct ParsedInstrumentName {
    asset: DeribitAsset,
    expiry_ts_ms: u64,
    strike: f64,
    option_type: DeribitOptionType,
}

impl ParsedInstrumentName {
    fn parse(value: &str) -> Result<Self> {
        let mut parts = value.split('-');
        let asset = parts
            .next()
            .ok_or_else(|| DataError::InvalidResponse(format!("invalid instrument `{value}`")))?;
        let expiry_code = parts
            .next()
            .ok_or_else(|| DataError::InvalidResponse(format!("invalid instrument `{value}`")))?;
        let strike = parts
            .next()
            .ok_or_else(|| DataError::InvalidResponse(format!("invalid instrument `{value}`")))?;
        let option_type = parts
            .next()
            .ok_or_else(|| DataError::InvalidResponse(format!("invalid instrument `{value}`")))?;
        if parts.next().is_some() {
            return Err(DataError::InvalidResponse(format!(
                "invalid instrument `{value}`"
            )));
        }
        Ok(Self {
            asset: DeribitAsset::from_symbol(asset)?,
            expiry_ts_ms: parse_deribit_expiry_ts_ms(expiry_code)?,
            strike: f64::from_str(strike).map_err(|err| {
                DataError::InvalidResponse(format!(
                    "invalid Deribit strike `{strike}` in `{value}`: {err}"
                ))
            })?,
            option_type: DeribitOptionType::from_code(option_type)?,
        })
    }
}

fn parse_deribit_expiry_ts_ms(value: &str) -> Result<u64> {
    if value.len() != 7 {
        return Err(DataError::InvalidResponse(format!(
            "invalid Deribit expiry code `{value}`"
        )));
    }
    let day = value[0..2].parse::<u32>().map_err(|err| {
        DataError::InvalidResponse(format!("invalid Deribit expiry day `{value}`: {err}"))
    })?;
    let month = match &value[2..5] {
        "JAN" => 1,
        "FEB" => 2,
        "MAR" => 3,
        "APR" => 4,
        "MAY" => 5,
        "JUN" => 6,
        "JUL" => 7,
        "AUG" => 8,
        "SEP" => 9,
        "OCT" => 10,
        "NOV" => 11,
        "DEC" => 12,
        other => {
            return Err(DataError::InvalidResponse(format!(
                "invalid Deribit expiry month `{other}` in `{value}`"
            )))
        }
    };
    let year = 2000
        + value[5..7].parse::<i32>().map_err(|err| {
            DataError::InvalidResponse(format!("invalid Deribit expiry year `{value}`: {err}"))
        })?;
    let days_since_epoch = days_from_civil(year, month, day);
    if days_since_epoch < 0 {
        return Err(DataError::InvalidResponse(format!(
            "invalid Deribit expiry code `{value}` before unix epoch"
        )));
    }
    Ok(((days_since_epoch as u64) * 86_400 + 8 * 3_600) * 1_000)
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let adjusted_year = year - i32::from(month <= 2);
    let era = if adjusted_year >= 0 {
        adjusted_year
    } else {
        adjusted_year - 399
    } / 400;
    let year_of_era = adjusted_year - era * 400;
    let month_index = month as i32 + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_index + 2) / 5 + day as i32 - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    (era as i64) * 146_097 + (day_of_era as i64) - 719_468
}

fn current_time_ms() -> Result<u64> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|err| DataError::InvalidResponse(format!("system clock error: {err}")))?;
    Ok(now.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_deribit_ticker_payload_extracts_iv_and_greeks() {
        let payload = r#"{
            "jsonrpc":"2.0",
            "method":"subscription",
            "params":{
                "channel":"ticker.BTC-27SEP24-100000-C.raw",
                "data":{
                    "instrument_name":"BTC-27SEP24-100000-C",
                    "timestamp":1725000000000,
                    "mark_price":0.1435,
                    "mark_iv":63.4,
                    "best_bid_price":0.141,
                    "best_ask_price":0.146,
                    "bid_iv":62.8,
                    "ask_iv":64.1,
                    "greeks":{"delta":0.47,"gamma":0.00019}
                }
            }
        }"#;

        let ticker = DeribitTickerMessage::from_text(payload).expect("parse ticker payload");

        assert_eq!(ticker.instrument_name, "BTC-27SEP24-100000-C");
        assert_eq!(ticker.mark_iv, 63.4);
        assert_eq!(ticker.bid_iv, Some(62.8));
        assert_eq!(ticker.ask_iv, Some(64.1));
        assert_eq!(ticker.delta, Some(0.47));
        assert_eq!(ticker.gamma, Some(0.00019));
    }

    #[test]
    fn discovery_filter_prunes_far_otm_and_long_dated_contracts() {
        let filter = DiscoveryFilter::default();
        let now_ts_ms = 1_715_000_000_000;
        let near = DeribitInstrument {
            instrument_name: "BTC-27SEP24-100000-C".to_owned(),
            asset: DeribitAsset::Btc,
            strike: 100_000.0,
            expiry_ts_ms: now_ts_ms + 7 * 24 * 60 * 60 * 1000,
            option_type: DeribitOptionType::Call,
        };
        let far_strike = DeribitInstrument {
            instrument_name: "BTC-27SEP24-130000-C".to_owned(),
            asset: DeribitAsset::Btc,
            strike: 130_000.0,
            expiry_ts_ms: near.expiry_ts_ms,
            option_type: DeribitOptionType::Call,
        };
        let far_expiry = DeribitInstrument {
            instrument_name: "BTC-27DEC24-100000-C".to_owned(),
            asset: DeribitAsset::Btc,
            strike: 100_000.0,
            expiry_ts_ms: now_ts_ms + 45 * 24 * 60 * 60 * 1000,
            option_type: DeribitOptionType::Call,
        };

        assert!(filter.allows(&near, 100_000.0, now_ts_ms));
        assert!(!filter.allows(&far_strike, 100_000.0, now_ts_ms));
        assert!(!filter.allows(&far_expiry, 100_000.0, now_ts_ms));
    }
}
