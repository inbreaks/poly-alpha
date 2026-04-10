use std::collections::HashMap;
use std::sync::Arc;

use polyalpha_core::Settings;

use crate::anchor::deribit::DeribitAnchorProvider;
use crate::anchor::provider::{AnchorError, AnchorProvider, Result};
use crate::model::PulseAsset;

pub struct AnchorRouter {
    providers: HashMap<String, Arc<dyn AnchorProvider>>,
    routing: HashMap<PulseAsset, String>,
}

impl AnchorRouter {
    pub fn from_settings(settings: &Settings) -> Result<Self> {
        let mut providers: HashMap<String, Arc<dyn AnchorProvider>> = HashMap::new();
        let mut routing = HashMap::new();

        for (asset_key, route) in &settings.strategy.pulse_arb.routing {
            if !route.enabled {
                continue;
            }
            let asset = PulseAsset::from_routing_key(asset_key).ok_or_else(|| {
                AnchorError::UnsupportedAsset {
                    asset: asset_key.clone(),
                }
            })?;
            let provider_id = route.anchor_provider.clone();
            let provider_config = settings
                .strategy
                .pulse_arb
                .providers
                .get(&provider_id)
                .ok_or_else(|| AnchorError::MissingProvider {
                    provider_id: provider_id.clone(),
                })?;
            if !provider_config.enabled {
                return Err(AnchorError::ProviderDisabled {
                    provider_id: provider_id.clone(),
                });
            }
            if !providers.contains_key(&provider_id) {
                let provider: Arc<dyn AnchorProvider> = match provider_config.kind.as_str() {
                    "deribit" => Arc::new(DeribitAnchorProvider::new(
                        provider_id.clone(),
                        &settings.deribit,
                        provider_config.clone(),
                    )),
                    other => {
                        return Err(AnchorError::UnsupportedProviderKind {
                            provider_id: provider_id.clone(),
                            kind: other.to_owned(),
                        })
                    }
                };
                providers.insert(provider_id.clone(), provider);
            }
            routing.insert(asset, provider_id);
        }

        Ok(Self { providers, routing })
    }

    pub fn provider_for_asset(&self, asset: PulseAsset) -> Result<Arc<dyn AnchorProvider>> {
        let provider_id = self
            .routing
            .get(&asset)
            .ok_or_else(|| AnchorError::MissingRoute {
                asset: asset.as_str().to_owned(),
            })?;
        self.providers
            .get(provider_id)
            .cloned()
            .ok_or_else(|| AnchorError::MissingProvider {
                provider_id: provider_id.clone(),
            })
    }
}

#[cfg(test)]
mod tests {
    use polyalpha_core::Settings;

    use super::*;

    fn pulse_test_settings() -> Settings {
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
                        },
                        "binance_options_primary": {
                            "kind": "binance_options",
                            "enabled": false,
                            "max_anchor_age_ms": 250,
                            "soft_mismatch_window_minutes": 360,
                            "hard_expiry_mismatch_minutes": 720
                        }
                    },
                    "routing": {
                        "btc": {
                            "enabled": true,
                            "anchor_provider": "deribit_primary",
                            "hedge_venue": "binance_perp"
                        },
                        "eth": {
                            "enabled": true,
                            "anchor_provider": "deribit_primary",
                            "hedge_venue": "binance_perp"
                        },
                        "sol": {
                            "enabled": false,
                            "anchor_provider": "binance_options_primary",
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
        .expect("pulse test settings")
    }

    #[tokio::test]
    async fn anchor_router_uses_deribit_primary_for_btc_and_eth() {
        let settings = pulse_test_settings();
        let router = AnchorRouter::from_settings(&settings).expect("build anchor router");

        let btc = router
            .provider_for_asset(PulseAsset::Btc)
            .expect("btc provider");
        let eth = router
            .provider_for_asset(PulseAsset::Eth)
            .expect("eth provider");

        assert_eq!(btc.provider_id(), "deribit_primary");
        assert_eq!(eth.provider_id(), "deribit_primary");
        assert!(router.provider_for_asset(PulseAsset::Sol).is_err());
    }
}
