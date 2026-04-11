use std::sync::Arc;

use rust_decimal::Decimal;
use thiserror::Error;

use polyalpha_core::Settings;

use crate::anchor::provider::AnchorProvider;
use crate::anchor::router::AnchorRouter;
use crate::hedge::GlobalHedgeAggregator;
use crate::model::PulseAsset;

pub type Result<T> = std::result::Result<T, PulseRuntimeBuildError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PulseExecutionMode {
    Paper,
    LiveMock,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PulseBookFeedStub {
    pub asset: PulseAsset,
    pub depth: u16,
}

#[derive(Clone)]
pub struct PulseRuntimeFixture {
    pub settings: Settings,
    pub anchor_provider: Arc<dyn AnchorProvider>,
    pub poly_books: Vec<PulseBookFeedStub>,
    pub binance_books: Vec<PulseBookFeedStub>,
}

pub struct PulseRuntimeBuilder {
    settings: Settings,
    anchor_provider: Option<Arc<dyn AnchorProvider>>,
    poly_books: Vec<PulseBookFeedStub>,
    binance_books: Vec<PulseBookFeedStub>,
    execution_mode: PulseExecutionMode,
}

pub struct PulseRuntime {
    execution_mode: PulseExecutionMode,
    enabled_assets: Vec<PulseAsset>,
    #[allow(dead_code)]
    anchor_provider: Arc<dyn AnchorProvider>,
    #[allow(dead_code)]
    poly_books: Vec<PulseBookFeedStub>,
    #[allow(dead_code)]
    binance_books: Vec<PulseBookFeedStub>,
    #[allow(dead_code)]
    hedge_aggregator: GlobalHedgeAggregator,
}

#[derive(Debug, Error)]
pub enum PulseRuntimeBuildError {
    #[error("pulse runtime has no enabled assets")]
    NoEnabledAssets,
    #[error("pulse runtime is missing an anchor provider")]
    MissingAnchorProvider,
    #[error("pulse runtime failed to build anchor router: {0}")]
    AnchorRouter(String),
}

impl PulseRuntimeBuilder {
    pub fn new(settings: Settings) -> Self {
        Self {
            settings,
            anchor_provider: None,
            poly_books: Vec::new(),
            binance_books: Vec::new(),
            execution_mode: PulseExecutionMode::Paper,
        }
    }

    pub fn with_anchor_provider(mut self, anchor_provider: Arc<dyn AnchorProvider>) -> Self {
        self.anchor_provider = Some(anchor_provider);
        self
    }

    pub fn with_poly_books(mut self, poly_books: Vec<PulseBookFeedStub>) -> Self {
        self.poly_books = poly_books;
        self
    }

    pub fn with_binance_books(mut self, binance_books: Vec<PulseBookFeedStub>) -> Self {
        self.binance_books = binance_books;
        self
    }

    pub fn with_execution_mode(mut self, execution_mode: PulseExecutionMode) -> Self {
        self.execution_mode = execution_mode;
        self
    }

    pub async fn build(self) -> Result<PulseRuntime> {
        let enabled_assets = enabled_assets_from_settings(&self.settings);
        if enabled_assets.is_empty() {
            return Err(PulseRuntimeBuildError::NoEnabledAssets);
        }

        let anchor_provider = match self.anchor_provider {
            Some(provider) => provider,
            None => {
                let router = AnchorRouter::from_settings(&self.settings)
                    .map_err(|err| PulseRuntimeBuildError::AnchorRouter(err.to_string()))?;
                router
                    .provider_for_asset(enabled_assets[0])
                    .map_err(|err| PulseRuntimeBuildError::AnchorRouter(err.to_string()))?
            }
        };

        Ok(PulseRuntime {
            execution_mode: self.execution_mode,
            enabled_assets,
            anchor_provider,
            poly_books: self.poly_books,
            binance_books: self.binance_books,
            hedge_aggregator: GlobalHedgeAggregator::new(Decimal::new(1, 3), 100),
        })
    }
}

impl PulseRuntime {
    pub fn execution_mode(&self) -> PulseExecutionMode {
        self.execution_mode
    }

    pub fn enabled_assets(&self) -> Vec<PulseAsset> {
        self.enabled_assets.clone()
    }

    pub fn uses_global_hedge_aggregator(&self) -> bool {
        true
    }
}

pub fn runtime_fixture_for_asset(asset: PulseAsset) -> PulseRuntimeFixture {
    let settings = fixture_settings_for_asset(asset);
    let router = AnchorRouter::from_settings(&settings).expect("build fixture anchor router");
    let anchor_provider = router
        .provider_for_asset(asset)
        .expect("fixture anchor provider");

    PulseRuntimeFixture {
        settings,
        anchor_provider,
        poly_books: vec![PulseBookFeedStub { asset, depth: 5 }],
        binance_books: vec![PulseBookFeedStub { asset, depth: 5 }],
    }
}

fn enabled_assets_from_settings(settings: &Settings) -> Vec<PulseAsset> {
    let mut assets = settings
        .strategy
        .pulse_arb
        .routing
        .iter()
        .filter(|(_, route)| route.enabled)
        .filter_map(|(asset_key, _)| PulseAsset::from_routing_key(asset_key))
        .collect::<Vec<_>>();
    assets.sort_by_key(|asset| asset.as_str().to_owned());
    assets
}

fn fixture_settings_for_asset(asset: PulseAsset) -> Settings {
    let (btc_enabled, eth_enabled) = match asset {
        PulseAsset::Btc => (true, false),
        PulseAsset::Eth => (false, true),
        PulseAsset::Sol | PulseAsset::Xrp => (false, false),
    };

    serde_json::from_value(serde_json::json!({
        "general": {
            "log_level": "info",
            "data_dir": "./data",
            "monitor_socket_path": "/tmp/polyalpha.sock"
        },
        "polymarket": {
            "clob_api_url": "https://clob.polymarket.com",
            "ws_url": "wss://ws-subscriptions-clob.polymarket.com/ws/market",
            "chain_id": 137
        },
        "binance": {
            "rest_url": "https://fapi.binance.com",
            "ws_url": "wss://fstream.binance.com"
        },
        "deribit": {
            "rest_url": "https://www.deribit.com/api/v2",
            "ws_url": "wss://www.deribit.com/ws/api/v2"
        },
        "okx": {
            "rest_url": "https://www.okx.com",
            "ws_public_url": "wss://ws.okx.com:8443/ws/v5/public",
            "ws_private_url": "wss://ws.okx.com:8443/ws/v5/private"
        },
        "markets": [],
        "strategy": {
            "basis": {
                "entry_z_score_threshold": "4.0",
                "exit_z_score_threshold": "0.5",
                "rolling_window_secs": 36000,
                "min_warmup_samples": 600,
                "min_basis_bps": "50.0",
                "max_position_usd": "200",
                "delta_rebalance_threshold": "0.05",
                "delta_rebalance_interval_secs": 60
            },
            "dmm": {
                "gamma": "0.1",
                "sigma_window_secs": 300,
                "max_inventory": "5000",
                "order_refresh_secs": 10,
                "num_levels": 3,
                "level_spacing_bps": "10.0",
                "min_spread_bps": "20.0"
            },
            "negrisk": {
                "min_arb_bps": "30.0",
                "max_legs": 8,
                "enable_inventory_backed_short": false
            },
            "pulse_arb": {
                "runtime": { "enabled": true, "max_concurrent_sessions_per_asset": 2 },
                "session": { "max_holding_secs": 900, "min_opening_notional_usd": "250" },
                "entry": { "min_net_session_edge_bps": "25" },
                "rehedge": {
                    "delta_drift_threshold": "0.03",
                    "delta_bump_mode": "relative_with_clamp",
                    "delta_bump_ratio_bps": 1,
                    "min_abs_bump": "5",
                    "max_abs_bump": "25"
                },
                "pin_risk": {
                    "gamma_cap_mode": "delta_clamp",
                    "max_abs_event_delta": "0.75",
                    "pin_risk_zone_bps": 15,
                    "pin_risk_time_window_secs": 1800
                },
                "providers": {
                    "deribit_primary": {
                        "kind": "deribit",
                        "enabled": true,
                        "max_anchor_age_ms": 250,
                        "soft_mismatch_window_minutes": 360,
                        "hard_expiry_mismatch_minutes": 720
                    }
                },
                "routing": {
                    "btc": {
                        "enabled": btc_enabled,
                        "anchor_provider": "deribit_primary",
                        "hedge_venue": "binance_perp"
                    },
                    "eth": {
                        "enabled": eth_enabled,
                        "anchor_provider": "deribit_primary",
                        "hedge_venue": "binance_perp"
                    }
                }
            },
            "settlement": {
                "stop_new_position_hours": 24,
                "force_reduce_hours": 12,
                "force_reduce_target_ratio": "0.5",
                "close_only_hours": 6,
                "emergency_close_hours": 1,
                "dispute_close_only": true
            }
        },
        "risk": {
            "max_total_exposure_usd": "10000",
            "max_single_position_usd": "200",
            "max_daily_loss_usd": "500",
            "max_drawdown_pct": "10.0",
            "max_open_orders": 50,
            "circuit_breaker_cooldown_secs": 300,
            "rate_limit_orders_per_sec": 5,
            "max_persistence_lag_secs": 10
        }
    }))
    .expect("pulse runtime fixture settings")
}

#[cfg(test)]
mod tests {
    use crate::model::PulseAsset;

    use super::*;

    #[tokio::test]
    async fn runtime_builds_live_mock_stack_for_btc_and_uses_mock_execution() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(fixture.anchor_provider.clone())
            .with_poly_books(fixture.poly_books.clone())
            .with_binance_books(fixture.binance_books.clone())
            .with_execution_mode(PulseExecutionMode::LiveMock)
            .build()
            .await
            .expect("build pulse runtime");

        assert_eq!(runtime.execution_mode(), PulseExecutionMode::LiveMock);
        assert_eq!(runtime.enabled_assets(), vec![PulseAsset::Btc]);
        assert!(runtime.uses_global_hedge_aggregator());
    }
}
