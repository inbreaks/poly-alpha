use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_decimal::Decimal;

use polyalpha_core::{DeribitConfig, PulseProviderConfig};
use polyalpha_data::{DeribitOptionsClient, DeribitTickerMessage, DiscoveryFilter};

use crate::anchor::provider::{AnchorError, AnchorProvider, Result};
use crate::model::{AnchorQualityMetrics, AnchorSnapshot, LocalSurfacePoint, PulseAsset};

#[derive(Clone)]
pub struct DeribitAnchorProvider {
    provider_id: String,
    #[allow(dead_code)]
    config: PulseProviderConfig,
    client: Arc<DeribitOptionsClient>,
}

impl DeribitAnchorProvider {
    pub fn new(
        provider_id: impl Into<String>,
        venue: &DeribitConfig,
        config: PulseProviderConfig,
    ) -> Self {
        let client = DeribitOptionsClient::new(
            venue.rest_url.clone(),
            venue.ws_url.clone(),
            DiscoveryFilter::default(),
        );
        Self {
            provider_id: provider_id.into(),
            config,
            client: Arc::new(client),
        }
    }

    pub fn with_client(
        provider_id: impl Into<String>,
        config: PulseProviderConfig,
        client: Arc<DeribitOptionsClient>,
    ) -> Self {
        Self {
            provider_id: provider_id.into(),
            config,
            client,
        }
    }
}

impl AnchorProvider for DeribitAnchorProvider {
    fn provider_id(&self) -> &str {
        &self.provider_id
    }

    fn snapshot_for_target(
        &self,
        asset: PulseAsset,
        target_event_expiry_ts_ms: Option<u64>,
    ) -> Result<Option<AnchorSnapshot>> {
        let deribit_asset =
            asset
                .as_deribit_asset()
                .ok_or_else(|| AnchorError::UnsupportedAsset {
                    asset: asset.as_str().to_owned(),
                })?;
        let tickers = self.client.latest_tickers(deribit_asset);
        if tickers.is_empty() {
            return Ok(None);
        }

        let selected_expiry_ts_ms = tickers
            .iter()
            .map(|ticker| ticker.expiry_ts_ms)
            .min_by_key(|expiry_ts_ms| match target_event_expiry_ts_ms {
                Some(target) => expiry_ts_ms.abs_diff(target),
                None => *expiry_ts_ms,
            })
            .ok_or_else(|| AnchorError::InvalidSnapshot("missing Deribit expiry".to_owned()))?;
        let surface = tickers
            .into_iter()
            .filter(|ticker| ticker.expiry_ts_ms == selected_expiry_ts_ms)
            .collect::<Vec<_>>();
        let latest_ts_ms = surface
            .iter()
            .map(|ticker| ticker.timestamp_ms)
            .max()
            .ok_or_else(|| AnchorError::InvalidSnapshot("missing Deribit timestamp".to_owned()))?;
        let latest_received_at_ms = surface
            .iter()
            .map(|ticker| ticker.received_at_ms)
            .max()
            .ok_or_else(|| {
                AnchorError::InvalidSnapshot("missing Deribit receive timestamp".to_owned())
            })?;
        let index_price = surface
            .iter()
            .find_map(|ticker| ticker.index_price)
            .ok_or_else(|| {
                AnchorError::InvalidSnapshot("missing Deribit index price".to_owned())
            })?;
        let index_price_decimal = decimal_from_f64(index_price)
            .ok_or_else(|| AnchorError::InvalidSnapshot("invalid index price".to_owned()))?;
        let local_surface_points = surface
            .iter()
            .map(local_surface_point_from_ticker)
            .collect::<Result<Vec<_>>>()?;
        let atm_point = surface
            .iter()
            .min_by(|left, right| {
                let left_distance = (left.strike - index_price).abs();
                let right_distance = (right.strike - index_price).abs();
                left_distance
                    .partial_cmp(&right_distance)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .ok_or_else(|| AnchorError::InvalidSnapshot("missing ATM point".to_owned()))?;

        Ok(Some(AnchorSnapshot {
            asset,
            provider_id: self.provider_id.clone(),
            ts_ms: latest_ts_ms,
            index_price: index_price_decimal,
            expiry_ts_ms: selected_expiry_ts_ms,
            atm_iv: atm_point.mark_iv,
            local_surface_points: local_surface_points.clone(),
            quality: AnchorQualityMetrics {
                anchor_age_ms: current_time_ms().saturating_sub(latest_received_at_ms),
                max_quote_spread_bps: max_quote_spread_bps(&surface),
                has_strike_coverage: !local_surface_points.is_empty(),
                has_liquidity: local_surface_points
                    .iter()
                    .any(|point| point.best_bid.is_some() && point.best_ask.is_some()),
                expiry_mismatch_minutes: target_event_expiry_ts_ms
                    .map(|target| signed_expiry_mismatch_minutes(selected_expiry_ts_ms, target))
                    .unwrap_or(0),
                greeks_complete: local_surface_points
                    .iter()
                    .all(|point| point.delta.is_some() && point.gamma.is_some()),
            },
        }))
    }
}

fn local_surface_point_from_ticker(ticker: &DeribitTickerMessage) -> Result<LocalSurfacePoint> {
    Ok(LocalSurfacePoint {
        instrument_name: ticker.instrument_name.clone(),
        strike: decimal_from_f64(ticker.strike)
            .ok_or_else(|| AnchorError::InvalidSnapshot("invalid Deribit strike".to_owned()))?,
        expiry_ts_ms: ticker.expiry_ts_ms,
        bid_iv: ticker.bid_iv,
        ask_iv: ticker.ask_iv,
        mark_iv: ticker.mark_iv,
        delta: ticker.delta,
        gamma: ticker.gamma,
        best_bid: ticker.best_bid_price.and_then(decimal_from_f64),
        best_ask: ticker.best_ask_price.and_then(decimal_from_f64),
    })
}

fn max_quote_spread_bps(tickers: &[DeribitTickerMessage]) -> Option<Decimal> {
    tickers
        .iter()
        .filter_map(|ticker| {
            let best_bid = ticker.best_bid_price?;
            let best_ask = ticker.best_ask_price?;
            if !best_bid.is_finite()
                || !best_ask.is_finite()
                || best_bid <= 0.0
                || best_ask < best_bid
            {
                return None;
            }
            let mid = (best_bid + best_ask) / 2.0;
            if mid <= 0.0 {
                return None;
            }
            decimal_from_f64(((best_ask - best_bid) / mid) * 10_000.0)
        })
        .max()
}

fn decimal_from_f64(value: f64) -> Option<Decimal> {
    if value.is_finite() {
        Decimal::from_f64_retain(value)
    } else {
        None
    }
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn signed_expiry_mismatch_minutes(selected_expiry_ts_ms: u64, target_event_expiry_ts_ms: u64) -> i64 {
    let diff_ms = selected_expiry_ts_ms as i128 - target_event_expiry_ts_ms as i128;
    (diff_ms / 60_000) as i64
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use polyalpha_core::PulseProviderConfig;
    use polyalpha_data::{DeribitAsset, DeribitOptionType, DeribitOptionsClient, DeribitTickerMessage, DiscoveryFilter};

    use super::*;

    #[test]
    fn snapshot_quality_uses_local_receive_time_for_anchor_age() {
        let now_ms = current_time_ms();
        let expiry_ts_ms = now_ms + 24 * 60 * 60 * 1_000;
        let client = Arc::new(DeribitOptionsClient::new(
            "https://www.deribit.com/api/v2",
            "wss://www.deribit.com/ws/api/v2",
            DiscoveryFilter::default(),
        ));
        for strike in [100_000_i64, 102_000_i64] {
            client.ingest_ticker(DeribitTickerMessage {
                instrument_name: format!("BTC-TEST-{strike}-C"),
                asset: DeribitAsset::Btc,
                expiry_ts_ms,
                strike: strike as f64,
                option_type: DeribitOptionType::Call,
                timestamp_ms: now_ms.saturating_sub(1_000),
                received_at_ms: now_ms,
                mark_price: 0.12,
                mark_iv: 55.0,
                best_bid_price: Some(0.11),
                best_ask_price: Some(0.13),
                bid_iv: Some(54.5),
                ask_iv: Some(55.5),
                delta: Some(0.5),
                gamma: Some(0.0002),
                index_price: Some(100_000.0),
            });
        }

        let provider = DeribitAnchorProvider::with_client(
            "deribit_primary",
            PulseProviderConfig {
                kind: "deribit".to_owned(),
                enabled: true,
                max_anchor_age_ms: 250,
                max_anchor_latency_delta_ms: 5_000,
                soft_mismatch_window_minutes: 360,
                hard_expiry_mismatch_minutes: 720,
            },
            client,
        );

        let snapshot = provider
            .snapshot_for_target(PulseAsset::Btc, Some(expiry_ts_ms))
            .expect("provider snapshot")
            .expect("anchor snapshot");

        assert!(snapshot.quality.anchor_age_ms < 200);
    }
}
