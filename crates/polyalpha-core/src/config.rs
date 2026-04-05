use ::config::{Config, Environment, File};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use crate::types::{Exchange, MarketConfig, SettlementRules, UsdNotional};
use crate::Result;

fn default_enable_freshness_check() -> bool {
    true
}

fn default_reject_on_disconnect() -> bool {
    true
}

fn default_max_data_age_minutes() -> u64 {
    1
}

fn default_max_time_diff_minutes() -> u64 {
    1
}

fn default_initial_capital() -> f64 {
    10_000.0
}

fn default_monitor_socket_path() -> String {
    "/tmp/polyalpha.sock".to_owned()
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum BandPolicyMode {
    #[default]
    ConfiguredBand,
    Disabled,
}

impl BandPolicyMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ConfiguredBand => "configured_band",
            Self::Disabled => "disabled",
        }
    }

    pub fn uses_configured_band(self) -> bool {
        matches!(self, Self::ConfiguredBand)
    }
}

impl fmt::Display for BandPolicyMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for BandPolicyMode {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "configured_band" => Ok(Self::ConfiguredBand),
            "disabled" => Ok(Self::Disabled),
            other => Err(format!("unsupported band policy: {other}")),
        }
    }
}

fn default_market_data_mode() -> MarketDataMode {
    MarketDataMode::Poll
}

fn default_max_stale_ms() -> u64 {
    5_000
}

fn default_reconnect_backoff_ms() -> u64 {
    1_000
}

fn default_snapshot_refresh_secs() -> u64 {
    300
}

fn default_planner_depth_levels() -> usize {
    5
}

fn default_funding_poll_secs() -> u64 {
    60
}

fn default_use_server_time() -> bool {
    true
}

fn default_audit_enabled() -> bool {
    true
}

fn default_audit_snapshot_interval_secs() -> u64 {
    15
}

fn default_audit_checkpoint_interval_secs() -> u64 {
    60
}

fn default_audit_warehouse_sync_interval_secs() -> u64 {
    300
}

fn default_audit_raw_segment_max_bytes() -> u64 {
    64 * 1024 * 1024
}

fn default_execution_cost_poly_fee_bps() -> u32 {
    20
}

fn default_execution_cost_cex_taker_fee_bps() -> u32 {
    5
}

fn default_execution_cost_cex_maker_fee_bps() -> u32 {
    5
}

fn default_execution_cost_fallback_funding_bps_per_day() -> u32 {
    1
}

fn default_max_open_instant_loss_pct_of_budget() -> Decimal {
    Decimal::new(4, 2)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GeneralConfig {
    pub log_level: String,
    pub data_dir: String,
    #[serde(default = "default_monitor_socket_path")]
    pub monitor_socket_path: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PolymarketSignatureType {
    Eoa,
    Proxy,
    GnosisSafe,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PolymarketConfig {
    pub clob_api_url: String,
    pub ws_url: String,
    pub chain_id: u64,
    #[serde(default)]
    pub private_key: Option<String>,
    #[serde(default)]
    pub signature_type: Option<PolymarketSignatureType>,
    #[serde(default)]
    pub funder: Option<String>,
    #[serde(default)]
    pub api_key_nonce: Option<u32>,
    #[serde(default = "default_use_server_time")]
    pub use_server_time: bool,
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

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum MarketDataMode {
    #[default]
    Poll,
    Ws,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketDataConfig {
    #[serde(default = "default_market_data_mode")]
    pub mode: MarketDataMode,
    #[serde(default = "default_max_stale_ms")]
    pub max_stale_ms: u64,
    #[serde(default = "default_planner_depth_levels")]
    pub planner_depth_levels: usize,
    #[serde(default)]
    pub poly_open_max_quote_age_ms: Option<u64>,
    #[serde(default)]
    pub cex_open_max_quote_age_ms: Option<u64>,
    #[serde(default)]
    pub close_max_quote_age_ms: Option<u64>,
    #[serde(default)]
    pub max_cross_leg_skew_ms: Option<u64>,
    #[serde(default)]
    pub borderline_poly_quote_age_ms: Option<u64>,
    #[serde(default = "default_reconnect_backoff_ms")]
    pub reconnect_backoff_ms: u64,
    #[serde(default = "default_snapshot_refresh_secs")]
    pub snapshot_refresh_secs: u64,
    #[serde(default = "default_funding_poll_secs")]
    pub funding_poll_secs: u64,
}

impl Default for MarketDataConfig {
    fn default() -> Self {
        Self {
            mode: default_market_data_mode(),
            max_stale_ms: default_max_stale_ms(),
            planner_depth_levels: default_planner_depth_levels(),
            poly_open_max_quote_age_ms: None,
            cex_open_max_quote_age_ms: None,
            close_max_quote_age_ms: None,
            max_cross_leg_skew_ms: None,
            borderline_poly_quote_age_ms: None,
            reconnect_backoff_ms: default_reconnect_backoff_ms(),
            snapshot_refresh_secs: default_snapshot_refresh_secs(),
            funding_poll_secs: default_funding_poll_secs(),
        }
    }
}

impl MarketDataConfig {
    pub fn resolved_poly_open_max_quote_age_ms(&self) -> u64 {
        self.poly_open_max_quote_age_ms.unwrap_or(self.max_stale_ms)
    }

    pub fn resolved_cex_open_max_quote_age_ms(&self) -> u64 {
        self.cex_open_max_quote_age_ms.unwrap_or(self.max_stale_ms)
    }

    pub fn resolved_close_max_quote_age_ms(&self) -> u64 {
        self.close_max_quote_age_ms
            .unwrap_or(self.max_stale_ms.saturating_mul(2))
    }

    pub fn resolved_max_cross_leg_skew_ms(&self) -> u64 {
        self.max_cross_leg_skew_ms.unwrap_or(self.max_stale_ms)
    }

    pub fn resolved_borderline_poly_quote_age_ms(&self) -> u64 {
        self.borderline_poly_quote_age_ms
            .unwrap_or(self.max_stale_ms.saturating_mul(8) / 10)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuditConfig {
    #[serde(default = "default_audit_enabled")]
    pub enabled: bool,
    #[serde(default = "default_audit_snapshot_interval_secs")]
    pub snapshot_interval_secs: u64,
    #[serde(default = "default_audit_checkpoint_interval_secs")]
    pub checkpoint_interval_secs: u64,
    #[serde(default = "default_audit_warehouse_sync_interval_secs")]
    pub warehouse_sync_interval_secs: u64,
    #[serde(default = "default_audit_raw_segment_max_bytes")]
    pub raw_segment_max_bytes: u64,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: default_audit_enabled(),
            snapshot_interval_secs: default_audit_snapshot_interval_secs(),
            checkpoint_interval_secs: default_audit_checkpoint_interval_secs(),
            warehouse_sync_interval_secs: default_audit_warehouse_sync_interval_secs(),
            raw_segment_max_bytes: default_audit_raw_segment_max_bytes(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct BasisOverrideConfig {
    /// Override entry Z-score threshold for specific asset
    #[serde(default)]
    pub entry_z_score_threshold: Option<Decimal>,
    /// Override exit Z-score threshold for specific asset
    #[serde(default)]
    pub exit_z_score_threshold: Option<Decimal>,
    /// Override rolling window in seconds
    #[serde(default)]
    pub rolling_window_secs: Option<u64>,
    /// Override minimum warmup samples
    #[serde(default)]
    pub min_warmup_samples: Option<usize>,
    /// Override minimum basis in basis points
    #[serde(default)]
    pub min_basis_bps: Option<Decimal>,
    /// Override maximum position size in USD
    #[serde(default)]
    pub max_position_usd: Option<UsdNotional>,
    /// Override maximum allowed instant open loss as pct of budget
    #[serde(default)]
    pub max_open_instant_loss_pct_of_budget: Option<Decimal>,
    /// Override delta rebalance threshold
    #[serde(default)]
    pub delta_rebalance_threshold: Option<Decimal>,
    /// Override delta rebalance interval in seconds
    #[serde(default)]
    pub delta_rebalance_interval_secs: Option<u64>,
    /// Override minimum Polymarket price for trading
    #[serde(default)]
    pub min_poly_price: Option<Decimal>,
    /// Override maximum Polymarket price for trading
    #[serde(default)]
    pub max_poly_price: Option<Decimal>,
    /// Override maximum data age in minutes
    #[serde(default)]
    pub max_data_age_minutes: Option<u64>,
    /// Override maximum poly/cex time diff in minutes
    #[serde(default)]
    pub max_time_diff_minutes: Option<u64>,
    /// Override freshness guard enablement
    #[serde(default)]
    pub enable_freshness_check: Option<bool>,
    /// Override whether to reject on disconnect
    #[serde(default)]
    pub reject_on_disconnect: Option<bool>,
    /// Override band policy for specific asset
    #[serde(default)]
    pub band_policy: Option<BandPolicyMode>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct EffectiveBasisConfig {
    pub asset_key: String,
    pub entry_z_score_threshold: Decimal,
    pub exit_z_score_threshold: Decimal,
    pub rolling_window_secs: u64,
    pub min_warmup_samples: usize,
    pub min_basis_bps: Decimal,
    pub max_position_usd: UsdNotional,
    pub max_open_instant_loss_pct_of_budget: Decimal,
    pub delta_rebalance_threshold: Decimal,
    pub delta_rebalance_interval_secs: u64,
    pub band_policy: BandPolicyMode,
    pub min_poly_price: Option<Decimal>,
    pub max_poly_price: Option<Decimal>,
    pub max_data_age_minutes: u64,
    pub max_time_diff_minutes: u64,
    pub enable_freshness_check: bool,
    pub reject_on_disconnect: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BasisStrategyConfig {
    pub entry_z_score_threshold: Decimal,
    pub exit_z_score_threshold: Decimal,
    pub rolling_window_secs: u64,
    pub min_warmup_samples: usize,
    pub min_basis_bps: Decimal,
    pub max_position_usd: UsdNotional,
    #[serde(default = "default_max_open_instant_loss_pct_of_budget")]
    pub max_open_instant_loss_pct_of_budget: Decimal,
    pub delta_rebalance_threshold: Decimal,
    pub delta_rebalance_interval_secs: u64,
    #[serde(default)]
    pub band_policy: BandPolicyMode,
    #[serde(default)]
    pub min_poly_price: Option<Decimal>,
    #[serde(default)]
    pub max_poly_price: Option<Decimal>,
    #[serde(default = "default_max_data_age_minutes")]
    pub max_data_age_minutes: u64,
    #[serde(default = "default_max_time_diff_minutes")]
    pub max_time_diff_minutes: u64,
    #[serde(default = "default_enable_freshness_check")]
    pub enable_freshness_check: bool,
    #[serde(default = "default_reject_on_disconnect")]
    pub reject_on_disconnect: bool,
    /// Per-asset parameter overrides, keyed by asset prefix (e.g., "btc", "eth")
    #[serde(default)]
    pub overrides: HashMap<String, BasisOverrideConfig>,
}

pub fn asset_key_from_cex_symbol(cex_symbol: &str) -> String {
    let normalized = cex_symbol
        .trim()
        .to_ascii_lowercase()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>();
    for suffix in [
        "usdtswap", "usdcswap", "usdswap", "usdtperp", "usdcperp", "usdperp", "usdt", "usdc",
        "usd", "perp", "swap",
    ] {
        if normalized.ends_with(suffix) && normalized.len() > suffix.len() {
            return normalized[..normalized.len() - suffix.len()].to_owned();
        }
    }
    normalized
}

impl BasisStrategyConfig {
    pub fn effective_for_asset_key(&self, asset_key: &str) -> EffectiveBasisConfig {
        let asset_key = asset_key.trim().to_ascii_lowercase();
        let override_config = self.overrides.get(&asset_key);
        EffectiveBasisConfig {
            asset_key,
            entry_z_score_threshold: override_config
                .and_then(|config| config.entry_z_score_threshold)
                .unwrap_or(self.entry_z_score_threshold),
            exit_z_score_threshold: override_config
                .and_then(|config| config.exit_z_score_threshold)
                .unwrap_or(self.exit_z_score_threshold),
            rolling_window_secs: override_config
                .and_then(|config| config.rolling_window_secs)
                .unwrap_or(self.rolling_window_secs),
            min_warmup_samples: override_config
                .and_then(|config| config.min_warmup_samples)
                .unwrap_or(self.min_warmup_samples),
            min_basis_bps: override_config
                .and_then(|config| config.min_basis_bps)
                .unwrap_or(self.min_basis_bps),
            max_position_usd: override_config
                .and_then(|config| config.max_position_usd)
                .unwrap_or(self.max_position_usd),
            max_open_instant_loss_pct_of_budget: override_config
                .and_then(|config| config.max_open_instant_loss_pct_of_budget)
                .unwrap_or(self.max_open_instant_loss_pct_of_budget),
            delta_rebalance_threshold: override_config
                .and_then(|config| config.delta_rebalance_threshold)
                .unwrap_or(self.delta_rebalance_threshold),
            delta_rebalance_interval_secs: override_config
                .and_then(|config| config.delta_rebalance_interval_secs)
                .unwrap_or(self.delta_rebalance_interval_secs),
            band_policy: override_config
                .and_then(|config| config.band_policy)
                .unwrap_or(self.band_policy),
            min_poly_price: override_config
                .and_then(|config| config.min_poly_price)
                .or(self.min_poly_price),
            max_poly_price: override_config
                .and_then(|config| config.max_poly_price)
                .or(self.max_poly_price),
            max_data_age_minutes: override_config
                .and_then(|config| config.max_data_age_minutes)
                .unwrap_or(self.max_data_age_minutes),
            max_time_diff_minutes: override_config
                .and_then(|config| config.max_time_diff_minutes)
                .unwrap_or(self.max_time_diff_minutes),
            enable_freshness_check: override_config
                .and_then(|config| config.enable_freshness_check)
                .unwrap_or(self.enable_freshness_check),
            reject_on_disconnect: override_config
                .and_then(|config| config.reject_on_disconnect)
                .unwrap_or(self.reject_on_disconnect),
        }
    }

    pub fn effective_for_cex_symbol(&self, cex_symbol: &str) -> EffectiveBasisConfig {
        self.effective_for_asset_key(&asset_key_from_cex_symbol(cex_symbol))
    }
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
    #[serde(default)]
    pub market_data: MarketDataConfig,
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PaperTradingConfig {
    #[serde(default = "default_initial_capital")]
    pub initial_capital: f64,
    #[serde(default)]
    pub timezone: Option<String>,
}

impl Default for PaperTradingConfig {
    fn default() -> Self {
        Self {
            initial_capital: default_initial_capital(),
            timezone: None,
        }
    }
}

/// Configuration for slippage and liquidity simulation in paper trading.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaperSlippageConfig {
    /// Additional slippage for Polymarket orders (in basis points).
    /// E.g., 50 means 0.5% extra slippage on top of orderbook spread.
    pub poly_slippage_bps: u64,
    /// Additional slippage for CEX orders (in basis points).
    pub cex_slippage_bps: u64,
    /// Minimum liquidity required (in shares for Poly, base qty for CEX).
    /// Orders with less available liquidity will be rejected.
    pub min_liquidity: Decimal,
    /// Whether to allow partial fills when liquidity is insufficient.
    pub allow_partial_fill: bool,
}

impl Default for PaperSlippageConfig {
    fn default() -> Self {
        Self {
            poly_slippage_bps: 50,
            cex_slippage_bps: 2,
            min_liquidity: Decimal::new(100, 0),
            allow_partial_fill: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FundingCostMode {
    #[serde(alias = "fallback")]
    FallbackBps,
    ObservedRate,
}

impl Default for FundingCostMode {
    fn default() -> Self {
        Self::FallbackBps
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionCostConfig {
    #[serde(default = "default_execution_cost_poly_fee_bps")]
    pub poly_fee_bps: u32,
    #[serde(default = "default_execution_cost_cex_taker_fee_bps")]
    pub cex_taker_fee_bps: u32,
    #[serde(default = "default_execution_cost_cex_maker_fee_bps")]
    pub cex_maker_fee_bps: u32,
    #[serde(default)]
    pub funding_mode: FundingCostMode,
    #[serde(default = "default_execution_cost_fallback_funding_bps_per_day")]
    pub fallback_funding_bps_per_day: u32,
}

impl Default for ExecutionCostConfig {
    fn default() -> Self {
        Self {
            poly_fee_bps: default_execution_cost_poly_fee_bps(),
            cex_taker_fee_bps: default_execution_cost_cex_taker_fee_bps(),
            cex_maker_fee_bps: default_execution_cost_cex_maker_fee_bps(),
            funding_mode: FundingCostMode::default(),
            fallback_funding_bps_per_day: default_execution_cost_fallback_funding_bps_per_day(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Settings {
    pub general: GeneralConfig,
    pub polymarket: PolymarketConfig,
    pub binance: BinanceConfig,
    pub okx: OkxConfig,
    pub markets: Vec<MarketConfig>,
    pub strategy: StrategyConfig,
    pub risk: RiskConfig,
    #[serde(default)]
    pub paper: PaperTradingConfig,
    #[serde(default)]
    pub paper_slippage: PaperSlippageConfig,
    #[serde(default)]
    pub execution_costs: ExecutionCostConfig,
    #[serde(default)]
    pub audit: AuditConfig,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basis_strategy_config_defaults_open_instant_loss_pct() {
        let config: BasisStrategyConfig = serde_json::from_value(serde_json::json!({
            "entry_z_score_threshold": "4.0",
            "exit_z_score_threshold": "0.5",
            "rolling_window_secs": 36000,
            "min_warmup_samples": 600,
            "min_basis_bps": "50.0",
            "max_position_usd": "200",
            "delta_rebalance_threshold": "0.05",
            "delta_rebalance_interval_secs": 60
        }))
        .expect("config should deserialize");

        assert_eq!(
            config.max_open_instant_loss_pct_of_budget,
            Decimal::new(4, 2)
        );
    }

    #[test]
    fn per_asset_basis_override_resolution() {
        let config: BasisStrategyConfig = serde_json::from_value(serde_json::json!({
            "entry_z_score_threshold": "4.0",
            "exit_z_score_threshold": "0.5",
            "rolling_window_secs": 36000,
            "min_warmup_samples": 600,
            "min_basis_bps": "50.0",
            "max_position_usd": "200",
            "max_open_instant_loss_pct_of_budget": "0.01",
            "delta_rebalance_threshold": "0.05",
            "delta_rebalance_interval_secs": 60,
            "band_policy": "configured_band",
            "min_poly_price": "0.2",
            "max_poly_price": "0.5",
            "max_data_age_minutes": 1,
            "max_time_diff_minutes": 1,
            "enable_freshness_check": true,
            "reject_on_disconnect": true,
            "overrides": {
                "btc": {
                    "min_poly_price": "0.10",
                    "max_poly_price": "0.45",
                    "max_open_instant_loss_pct_of_budget": "0.02",
                    "delta_rebalance_threshold": "0.08",
                    "delta_rebalance_interval_secs": 120,
                    "max_data_age_minutes": 2,
                    "max_time_diff_minutes": 3,
                    "enable_freshness_check": false,
                    "reject_on_disconnect": false
                }
            }
        }))
        .expect("config should deserialize");

        let btc = config.effective_for_asset_key("btc");
        assert_eq!(btc.min_poly_price, Some(Decimal::new(10, 2)));
        assert_eq!(btc.max_poly_price, Some(Decimal::new(45, 2)));
        assert_eq!(btc.max_open_instant_loss_pct_of_budget, Decimal::new(2, 2));
        assert_eq!(btc.delta_rebalance_threshold, Decimal::new(8, 2));
        assert_eq!(btc.delta_rebalance_interval_secs, 120);
        assert_eq!(btc.max_data_age_minutes, 2);
        assert_eq!(btc.max_time_diff_minutes, 3);
        assert!(!btc.enable_freshness_check);
        assert!(!btc.reject_on_disconnect);

        let eth = config.effective_for_asset_key("eth");
        assert_eq!(eth.min_poly_price, Some(Decimal::new(20, 2)));
        assert_eq!(eth.max_poly_price, Some(Decimal::new(50, 2)));
        assert_eq!(eth.max_open_instant_loss_pct_of_budget, Decimal::new(1, 2));
        assert_eq!(eth.delta_rebalance_threshold, Decimal::new(5, 2));
        assert_eq!(eth.delta_rebalance_interval_secs, 60);
        assert_eq!(eth.max_data_age_minutes, 1);
        assert_eq!(eth.max_time_diff_minutes, 1);
        assert!(eth.enable_freshness_check);
        assert!(eth.reject_on_disconnect);
    }

    #[test]
    fn polymarket_config_deserializes_optional_live_auth_fields() {
        let config: PolymarketConfig = serde_json::from_str(
            r#"{
                "clob_api_url":"https://clob.polymarket.com",
                "ws_url":"wss://ws-subscriptions-clob.polymarket.com/ws/market",
                "chain_id":137,
                "private_key":"0xabc",
                "signature_type":"proxy",
                "funder":"0x0000000000000000000000000000000000000001",
                "api_key_nonce":7,
                "use_server_time":false
            }"#,
        )
        .expect("polymarket config with live auth");

        assert_eq!(config.private_key.as_deref(), Some("0xabc"));
        assert_eq!(config.signature_type, Some(PolymarketSignatureType::Proxy));
        assert_eq!(
            config.funder.as_deref(),
            Some("0x0000000000000000000000000000000000000001")
        );
        assert_eq!(config.api_key_nonce, Some(7));
        assert!(!config.use_server_time);
    }

    #[test]
    fn market_data_config_defaults_to_poll_mode() {
        let config: MarketDataConfig =
            serde_json::from_str("{}").expect("default market data config");
        assert_eq!(config.mode, MarketDataMode::Poll);
        assert_eq!(config.max_stale_ms, 5_000);
        assert_eq!(config.planner_depth_levels, 5);
        assert_eq!(config.resolved_poly_open_max_quote_age_ms(), 5_000);
        assert_eq!(config.resolved_cex_open_max_quote_age_ms(), 5_000);
        assert_eq!(config.resolved_close_max_quote_age_ms(), 10_000);
        assert_eq!(config.resolved_max_cross_leg_skew_ms(), 5_000);
        assert_eq!(config.resolved_borderline_poly_quote_age_ms(), 4_000);
        assert_eq!(config.reconnect_backoff_ms, 1_000);
        assert_eq!(config.snapshot_refresh_secs, 300);
        assert_eq!(config.funding_poll_secs, 60);
    }

    #[test]
    fn market_data_config_deserializes_ws_mode() {
        let config: MarketDataConfig = serde_json::from_str(
            r#"{
                "mode":"ws",
                "max_stale_ms":1500,
                "poly_open_max_quote_age_ms":2500,
                "cex_open_max_quote_age_ms":1200,
                "close_max_quote_age_ms":3000,
                "max_cross_leg_skew_ms":900,
                "borderline_poly_quote_age_ms":1800,
                "reconnect_backoff_ms":2000,
                "snapshot_refresh_secs":45,
                "funding_poll_secs":30
            }"#,
        )
        .expect("ws market data config");
        assert_eq!(config.mode, MarketDataMode::Ws);
        assert_eq!(config.max_stale_ms, 1_500);
        assert_eq!(config.planner_depth_levels, 5);
        assert_eq!(config.resolved_poly_open_max_quote_age_ms(), 2_500);
        assert_eq!(config.resolved_cex_open_max_quote_age_ms(), 1_200);
        assert_eq!(config.resolved_close_max_quote_age_ms(), 3_000);
        assert_eq!(config.resolved_max_cross_leg_skew_ms(), 900);
        assert_eq!(config.resolved_borderline_poly_quote_age_ms(), 1_800);
        assert_eq!(config.reconnect_backoff_ms, 2_000);
        assert_eq!(config.snapshot_refresh_secs, 45);
        assert_eq!(config.funding_poll_secs, 30);
    }

    #[test]
    fn general_config_defaults_monitor_socket_path() {
        let config: GeneralConfig = serde_json::from_str(
            r#"{
                "log_level":"info",
                "data_dir":"./data"
            }"#,
        )
        .expect("general config with default socket path");
        assert_eq!(config.monitor_socket_path, "/tmp/polyalpha.sock");
    }

    #[test]
    fn audit_config_defaults_are_forward_compatible() {
        let config: AuditConfig = serde_json::from_str("{}").expect("default audit config");
        assert!(config.enabled);
        assert_eq!(config.snapshot_interval_secs, 15);
        assert_eq!(config.checkpoint_interval_secs, 60);
        assert_eq!(config.warehouse_sync_interval_secs, 300);
        assert_eq!(config.raw_segment_max_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn execution_cost_config_defaults_are_explicit() {
        let config: ExecutionCostConfig =
            serde_json::from_str("{}").expect("default execution cost config");
        assert_eq!(config.poly_fee_bps, 20);
        assert_eq!(config.cex_taker_fee_bps, 5);
        assert_eq!(config.cex_maker_fee_bps, 5);
        assert_eq!(config.funding_mode, FundingCostMode::FallbackBps);
        assert_eq!(config.fallback_funding_bps_per_day, 1);
    }

    #[test]
    fn execution_cost_config_deserializes_observed_rate_mode() {
        let config: ExecutionCostConfig = serde_json::from_str(
            r#"{
                "poly_fee_bps": 17,
                "cex_taker_fee_bps": 9,
                "cex_maker_fee_bps": 2,
                "funding_mode": "observed_rate",
                "fallback_funding_bps_per_day": 4
            }"#,
        )
        .expect("execution cost config");
        assert_eq!(config.poly_fee_bps, 17);
        assert_eq!(config.cex_taker_fee_bps, 9);
        assert_eq!(config.cex_maker_fee_bps, 2);
        assert_eq!(config.funding_mode, FundingCostMode::ObservedRate);
        assert_eq!(config.fallback_funding_bps_per_day, 4);
    }
}
