use ::config::{Config, Environment, File};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::types::{Exchange, MarketConfig, SettlementRules, UsdNotional};
use crate::Result;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GeneralConfig {
    pub log_level: String,
    pub data_dir: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PolymarketConfig {
    pub clob_api_url: String,
    pub ws_url: String,
    pub chain_id: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BinanceConfig {
    pub rest_url: String,
    pub ws_url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OkxConfig {
    pub rest_url: String,
    pub ws_public_url: String,
    pub ws_private_url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BasisStrategyConfig {
    pub entry_z_score_threshold: Decimal,
    pub exit_z_score_threshold: Decimal,
    pub rolling_window_secs: u64,
    pub min_warmup_samples: usize,
    pub min_basis_bps: Decimal,
    pub max_position_usd: UsdNotional,
    pub delta_rebalance_threshold: Decimal,
    pub delta_rebalance_interval_secs: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DmmStrategyConfig {
    pub gamma: Decimal,
    pub sigma_window_secs: u64,
    pub max_inventory: UsdNotional,
    pub order_refresh_secs: u64,
    pub num_levels: usize,
    pub level_spacing_bps: Decimal,
    pub min_spread_bps: Decimal,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NegRiskStrategyConfig {
    pub min_arb_bps: Decimal,
    pub max_legs: usize,
    pub enable_inventory_backed_short: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StrategyConfig {
    pub basis: BasisStrategyConfig,
    pub dmm: DmmStrategyConfig,
    pub negrisk: NegRiskStrategyConfig,
    pub settlement: SettlementRules,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RiskConfig {
    pub max_total_exposure_usd: UsdNotional,
    pub max_single_position_usd: UsdNotional,
    pub max_daily_loss_usd: UsdNotional,
    pub max_drawdown_pct: Decimal,
    pub max_open_orders: usize,
    pub circuit_breaker_cooldown_secs: u64,
    pub rate_limit_orders_per_sec: u64,
    pub max_persistence_lag_secs: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Settings {
    pub general: GeneralConfig,
    pub polymarket: PolymarketConfig,
    pub binance: BinanceConfig,
    pub okx: OkxConfig,
    pub markets: Vec<MarketConfig>,
    pub strategy: StrategyConfig,
    pub risk: RiskConfig,
}

impl Settings {
    pub fn load(environment: &str) -> Result<Self> {
        let builder = Config::builder()
            .add_source(File::with_name("config/default").required(false))
            .add_source(File::with_name(&format!("config/{environment}")).required(false))
            .add_source(Environment::with_prefix("POLYALPHA").separator("__"));

        let config = builder.build()?;
        Ok(config.try_deserialize()?)
    }

    pub fn hedge_exchange_for(&self, symbol: &crate::types::Symbol) -> Option<Exchange> {
        self.markets
            .iter()
            .find(|market| &market.symbol == symbol)
            .map(|market| market.hedge_exchange)
    }
}
