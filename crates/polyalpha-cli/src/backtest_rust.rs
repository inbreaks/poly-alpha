use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use duckdb::{AccessMode, Config as DuckConfig, Connection};
use regex::Regex;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::Value as JsonValue;

use polyalpha_core::{
    AlphaEngine, BandPolicyMode, CexBaseQty, Exchange, InstrumentKind, MarketConfig,
    MarketDataConfig, MarketDataEvent, MarketRule, MarketRuleKind, OpenCandidate,
    OrderBookSnapshot, OrderSide, PlanningIntent, PolyShares, PolymarketIds, Price, PriceLevel,
    Symbol, TokenSide, TradePlan, UsdNotional, VenueQuantity,
};
use polyalpha_engine::{SimpleAlphaEngine, SimpleEngineConfig};
use polyalpha_executor::{CanonicalPlanningContext, ExecutionPlanner};

use crate::args::RustStressPreset;

const DEFAULT_POSITION_NOTIONAL_PCT: f64 = 0.01;
const RESOLUTION_EPSILON: f64 = 1e-4;
const ONE_MINUTE_MS: u64 = 60_000;
const MONEY_LITERAL_RE: &str = r"([0-9][0-9,]*(?:\.\d+)?(?:[kmb])?)\b";
const REPLAY_ANOMALY_MIN_ABS_EQUITY_JUMP_USD: f64 = 10_000.0;
const REPLAY_ANOMALY_MIN_MARK_SHARE: f64 = 0.80;
const REPLAY_ANOMALY_TOP_LIMIT: usize = 10;
const REPLAY_ANOMALY_TOP_MARKET_LIMIT: usize = 10;
const REPLAY_SYNC_RESET_MIN_MARKETS: usize = 3;
const REPLAY_SYNC_RESET_EXTREME_THRESHOLD: f64 = 0.20;
const REPLAY_SYNC_RESET_NEUTRAL_TARGET: f64 = 0.50;
const REPLAY_SYNC_RESET_NEUTRAL_TOLERANCE: f64 = 0.055;
const REPLAY_SYNC_RESET_MIN_MOVE: f64 = 0.25;
const REPLAY_SYNC_RESET_INTERIOR_MIN: f64 = 0.25;
const REPLAY_SYNC_RESET_INTERIOR_MAX: f64 = 0.75;
const REPLAY_SYNC_RESET_SNAPBACK_TOLERANCE: f64 = 0.05;
const REPLAY_SYNC_RESET_INTERIOR_MIN_TRIGGER_MARKETS: usize = 2;
const REPLAY_SYNC_RESET_INTERIOR_MIN_SANITIZED_MARKETS: usize = 4;
const REPLAY_SYNC_RESET_PAIR_TEMPLATE_TOLERANCE: f64 = 0.05;
const REPLAY_SYNC_RESET_PAIR_COMPLEMENT_TOLERANCE: f64 = 0.08;
const REPLAY_SYNC_RESET_PAIR_SETTLEMENT_TOLERANCE_MS: u64 = 2 * 60 * 60 * 1000;

#[derive(Clone, Debug)]
pub struct RustReplayCommandArgs {
    pub db_path: String,
    pub market_id: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub initial_capital: f64,
    pub rolling_window: usize,
    pub entry_z: f64,
    pub exit_z: f64,
    pub position_notional_usd: Option<f64>,
    pub max_capital_usage: f64,
    pub cex_hedge_ratio: f64,
    pub cex_margin_ratio: f64,
    pub poly_fee_bps: f64,
    pub poly_slippage_bps: f64,
    pub cex_fee_bps: f64,
    pub cex_slippage_bps: f64,
    pub entry_fill_ratio: f64,
    pub planner_depth_levels: usize,
    pub band_policy: BandPolicyMode,
    pub min_poly_price: Option<f64>,
    pub max_poly_price: Option<f64>,
    pub max_holding_bars: Option<usize>,
    pub report_json: Option<String>,
    pub anomaly_report_json: Option<String>,
    pub fail_on_anomaly: bool,
    pub equity_csv: Option<String>,
    pub trades_csv: Option<String>,
    pub snapshots_csv: Option<String>,
}

#[derive(Clone, Debug)]
pub struct RustStressCommandArgs {
    pub db_path: String,
    pub market_id: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub initial_capital: f64,
    pub rolling_window: usize,
    pub entry_z: f64,
    pub exit_z: f64,
    pub position_notional_usd: Option<f64>,
    pub max_capital_usage: f64,
    pub cex_hedge_ratio: f64,
    pub cex_margin_ratio: f64,
    pub planner_depth_levels: usize,
    pub preset: Option<RustStressPreset>,
    pub report_json: Option<String>,
}

#[derive(Clone, Debug)]
struct ReplayFilter {
    market_id: Option<String>,
    start_ts_ms: Option<u64>,
    end_ts_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
struct ExecutionStressConfig {
    label: String,
    poly_fee_bps: f64,
    poly_slippage_bps: f64,
    cex_fee_bps: f64,
    cex_slippage_bps: f64,
    entry_fill_ratio: f64,
}

#[derive(Clone, Debug)]
struct ResolvedReplayConfig {
    db_path: String,
    initial_capital: f64,
    rolling_window: usize,
    entry_z: f64,
    exit_z: f64,
    position_notional_usd: f64,
    max_capital_usage: f64,
    cex_hedge_ratio: f64,
    cex_margin_ratio: f64,
    planner_depth_levels: usize,
    band_policy: BandPolicyMode,
    min_poly_price: Option<f64>,
    max_poly_price: Option<f64>,
    max_holding_bars: Option<usize>,
    stress: ExecutionStressConfig,
}

#[derive(Clone, Debug, Default, Serialize)]
struct EquityPoint {
    ts_ms: u64,
    equity: f64,
    cash: f64,
    available_cash: f64,
    unrealized_pnl: f64,
    gross_exposure: f64,
    reserved_margin: f64,
    capital_in_use: f64,
    open_positions: usize,
}

#[derive(Clone, Debug)]
struct ReplayDataset {
    metadata: HashMap<String, String>,
    markets: HashMap<String, ReplayMarketSpec>,
    rows: Vec<ReplayMinuteRow>,
    cex_close_by_ts: HashMap<u64, f64>,
    unique_minutes: usize,
    poly_observation_count: usize,
    sanitization: ReplaySanitizationDiagnostics,
}

#[derive(Clone, Debug)]
struct ReplayMarketSpec {
    event_id: String,
    market_id: String,
    market_slug: String,
    symbol: Symbol,
    market_config: MarketConfig,
    settlement_ts_ms: u64,
    yes_payout: Option<f64>,
    no_payout: Option<f64>,
}

#[derive(Clone, Debug)]
struct ReplayMinuteRow {
    ts_ms: u64,
    market_id: String,
    yes_price: Option<f64>,
    no_price: Option<f64>,
}

#[derive(Clone, Debug, Default)]
struct ReplaySanitizationOutcome {
    rows: Vec<ReplayMinuteRow>,
    diagnostics: ReplaySanitizationDiagnostics,
}

#[derive(Clone, Debug, Default, Serialize)]
struct ReplaySanitizationDiagnostics {
    sanitized_market_minutes: usize,
    sanitized_poly_observations: usize,
    sync_reset_windows: Vec<ReplaySanitizedSyncResetWindow>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
struct ReplaySanitizedSyncResetWindow {
    event_id: String,
    start_ts_ms: u64,
    end_ts_ms: u64,
    trigger_market_count: usize,
    sanitized_market_count: usize,
}

#[derive(Clone, Debug, Default)]
struct ReplayRuntimeState {
    bootstrapped: bool,
    last_cex_synced_minute_ms: Option<u64>,
}

#[derive(Clone, Debug)]
struct ReplayPosition {
    market_id: String,
    token_side: TokenSide,
    shares: f64,
    poly_entry_notional: f64,
    last_poly_price: f64,
    cex_entry_price: f64,
    cex_qty_btc: f64,
    last_cex_price: f64,
    reserved_margin: f64,
    round_trip_pnl: f64,
    entry_ts_ms: u64,
    entry_poly_price: f64,
    entry_fees_paid: f64,
    entry_slippage_paid: f64,
    entry_z_score: f64,
    entry_basis_bps: f64,
    entry_delta: f64,
}

#[derive(Clone, Debug)]
struct PendingReplayCandidate {
    candidate: OpenCandidate,
    spec: ReplayMarketSpec,
    row: ReplayMinuteRow,
    cex_reference_price: f64,
}

#[derive(Clone, Debug)]
struct PendingReplayClose {
    close_reason: String,
    spec: ReplayMarketSpec,
    row: ReplayMinuteRow,
    cex_reference_price: f64,
}

#[derive(Clone, Debug, Default, Serialize)]
struct ReplayStats {
    grouped_rows_seen: usize,
    grouped_rows_replayed: usize,
    grouped_rows_skipped_missing_cex: usize,
    poly_observations_replayed: usize,
    signals_emitted: usize,
    signals_rejected_capital: usize,
    signals_rejected_missing_price: usize,
    signals_rejected_below_min_poly_price: usize,
    signals_rejected_above_or_equal_max_poly_price: usize,
    signals_rejected_planner: usize,
    signals_ignored_existing_position: usize,
    unsupported_signals: usize,
    entry_count: usize,
    close_count: usize,
    settlement_count: usize,
    trade_count: usize,
    poly_trade_count: usize,
    cex_trade_count: usize,
    round_trip_count: usize,
    winning_round_trips: usize,
    poly_realized_gross: f64,
    cex_realized_gross: f64,
    total_fees_paid: f64,
    total_slippage_paid: f64,
    poly_fees_paid: f64,
    poly_slippage_paid: f64,
    cex_fees_paid: f64,
    cex_slippage_paid: f64,
    max_gross_exposure: f64,
    max_reserved_margin: f64,
    max_capital_in_use: f64,
    max_capital_utilization: f64,
    max_drawdown: f64,
    #[serde(skip_serializing)]
    signals_by_market: HashMap<String, usize>,
    #[serde(skip_serializing)]
    rejected_capital_by_market: HashMap<String, usize>,
    #[serde(skip_serializing)]
    rejected_missing_price_by_market: HashMap<String, usize>,
    #[serde(skip_serializing)]
    rejected_below_min_price_by_market: HashMap<String, usize>,
    #[serde(skip_serializing)]
    rejected_above_or_equal_max_price_by_market: HashMap<String, usize>,
    #[serde(skip_serializing)]
    entries_by_market: HashMap<String, usize>,
    #[serde(skip_serializing)]
    net_pnl_by_market: HashMap<String, f64>,
}

#[derive(Clone, Debug, Serialize)]
struct MarketSignalDebugRow {
    market_id: String,
    signal_count: usize,
    rejected_capital_count: usize,
    rejected_missing_price_count: usize,
    rejected_below_min_price_count: usize,
    rejected_above_or_equal_max_price_count: usize,
    entry_count: usize,
    net_pnl: f64,
}

#[derive(Clone, Debug, Serialize)]
struct RustReplaySummary {
    db_path: String,
    stress: ExecutionStressConfig,
    band_policy: BandPolicyMode,
    initial_capital: f64,
    ending_equity: f64,
    total_pnl: f64,
    realized_pnl: f64,
    unrealized_pnl: f64,
    poly_realized_gross: f64,
    cex_realized_gross: f64,
    max_drawdown: f64,
    max_gross_exposure: f64,
    max_reserved_margin: f64,
    max_capital_in_use: f64,
    max_capital_utilization: f64,
    signal_count: usize,
    signal_rejected_capital: usize,
    signal_rejected_missing_price: usize,
    signal_rejected_below_min_poly_price: usize,
    signal_rejected_above_or_equal_max_poly_price: usize,
    signal_rejected_planner: usize,
    signal_ignored_existing_position: usize,
    entry_count: usize,
    close_count: usize,
    settlement_count: usize,
    trade_count: usize,
    poly_trade_count: usize,
    cex_trade_count: usize,
    round_trip_count: usize,
    winning_round_trips: usize,
    win_rate: f64,
    total_fees_paid: f64,
    total_slippage_paid: f64,
    poly_fees_paid: f64,
    poly_slippage_paid: f64,
    cex_fees_paid: f64,
    cex_slippage_paid: f64,
    event_count: usize,
    market_count: usize,
    minute_count: usize,
    grouped_row_count: usize,
    poly_observation_count: usize,
    sanitized_market_minute_count: usize,
    sanitized_poly_observation_count: usize,
    sanitized_sync_reset_window_count: usize,
    replayed_grouped_rows: usize,
    replayed_poly_observations: usize,
    open_positions_end: usize,
    start_ts_ms: Option<u64>,
    end_ts_ms: Option<u64>,
    anomaly_count: usize,
    max_abs_anomaly_equity_jump_usd: f64,
    max_anomaly_mark_share: f64,
    top_signal_markets: Vec<MarketSignalDebugRow>,
    top_entry_markets: Vec<MarketSignalDebugRow>,
    market_debug_rows: Vec<MarketSignalDebugRow>,
}

#[derive(Clone, Debug, Serialize)]
struct RustReplayReport {
    generated_at_utc: String,
    build_metadata: HashMap<String, String>,
    summary: RustReplaySummary,
    anomaly_diagnostics: ReplayAnomalyDiagnostics,
}

#[derive(Clone, Debug, Serialize)]
struct ReplayAnomalyReport {
    generated_at_utc: String,
    build_metadata: HashMap<String, String>,
    diagnostics: ReplayAnomalyDiagnostics,
}

#[derive(Clone, Debug, Serialize)]
struct RustStressReport {
    generated_at_utc: String,
    build_metadata: HashMap<String, String>,
    results: Vec<RustReplaySummary>,
}

#[derive(Clone, Debug, Serialize)]
struct ReplayTradeRow {
    trade_id: usize,
    strategy: String,
    entry_time: u64,
    event_id: String,
    market_id: String,
    market_slug: String,
    exit_time: u64,
    direction: String,
    entry_poly_price: f64,
    exit_poly_price: f64,
    entry_cex_price: f64,
    exit_cex_price: f64,
    poly_shares: f64,
    cex_qty: f64,
    gross_pnl: f64,
    fees: f64,
    slippage: f64,
    gas: f64,
    funding_cost: f64,
    net_pnl: f64,
    holding_bars: u64,
    z_score_at_entry: f64,
    basis_bps_at_entry: f64,
    delta_at_entry: f64,
}

#[derive(Clone, Debug, Serialize)]
struct ReplaySnapshotRow {
    ts_ms: u64,
    event_id: String,
    market_id: String,
    market_slug: String,
    yes_price: Option<f64>,
    no_price: Option<f64>,
    cex_reference_price: f64,
    cex_advance_price: f64,
    signal_count: usize,
    signal_action: Option<String>,
    signal_token_side: Option<String>,
    signal_basis_bps: Option<f64>,
    signal_z_score: Option<f64>,
    snapshot_token_side: Option<String>,
    snapshot_poly_price: Option<f64>,
    snapshot_fair_value: Option<f64>,
    snapshot_basis_bps: Option<f64>,
    snapshot_z_score: Option<f64>,
    snapshot_delta: Option<f64>,
    snapshot_sigma: Option<f64>,
    snapshot_minutes_to_expiry: Option<f64>,
}

#[derive(Clone, Debug, Default, Serialize, PartialEq)]
struct ReplayAnomalyDiagnostics {
    anomaly_count: usize,
    max_abs_equity_jump_usd: f64,
    max_mark_share: f64,
    top_anomalies: Vec<ReplayAnomaly>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
struct ReplayAnomaly {
    start_ts_ms: u64,
    end_ts_ms: u64,
    equity_change_usd: f64,
    cash_change_usd: f64,
    fill_cash_change_usd: f64,
    mark_change_usd: f64,
    mark_share: f64,
    reason_codes: Vec<String>,
    sync_reset_events: Vec<ReplayAnomalousEvent>,
    top_markets: Vec<ReplayMinuteAttribution>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
struct ReplayAnomalousEvent {
    event_id: String,
    market_count: usize,
}

#[derive(Clone, Debug, Default, Serialize, PartialEq)]
struct ReplayMinuteAttribution {
    event_id: String,
    market_id: String,
    market_slug: String,
    market_question: Option<String>,
    total_contribution_usd: f64,
    fill_cash_change_usd: f64,
    poly_mark_change_usd: f64,
    cex_mark_change_usd: f64,
    open_positions_before: usize,
    open_positions_after: usize,
    entries_at_end_ts: usize,
    exits_at_end_ts: usize,
    yes_price_start: Option<f64>,
    yes_price_end: Option<f64>,
    no_price_start: Option<f64>,
    no_price_end: Option<f64>,
    cex_price_start: Option<f64>,
    cex_price_end: Option<f64>,
}

#[derive(Clone, Debug)]
struct ReplayRunResult {
    summary: RustReplaySummary,
    equity_curve: Vec<EquityPoint>,
    trade_log: Vec<ReplayTradeRow>,
    snapshot_log: Vec<ReplaySnapshotRow>,
}

pub async fn run_rust_replay_command(args: RustReplayCommandArgs) -> Result<()> {
    let filter = build_filter(
        args.market_id.clone(),
        args.start.as_deref(),
        args.end.as_deref(),
    )?;
    let dataset = load_dataset(&args.db_path, &filter)?;
    let config = resolve_replay_config(
        &args.db_path,
        args.initial_capital,
        args.rolling_window,
        args.entry_z,
        args.exit_z,
        args.position_notional_usd,
        args.max_capital_usage,
        args.cex_hedge_ratio,
        args.cex_margin_ratio,
        args.planner_depth_levels,
        args.band_policy,
        args.min_poly_price,
        args.max_poly_price,
        args.max_holding_bars,
        ExecutionStressConfig {
            label: "custom".to_owned(),
            poly_fee_bps: args.poly_fee_bps,
            poly_slippage_bps: args.poly_slippage_bps,
            cex_fee_bps: args.cex_fee_bps,
            cex_slippage_bps: args.cex_slippage_bps,
            entry_fill_ratio: args.entry_fill_ratio,
        },
    )?;
    let run = run_single_replay(&dataset, &config, args.snapshots_csv.is_some()).await?;
    let anomaly_diagnostics =
        detect_replay_anomalies(&dataset, &config, &run.equity_curve, &run.trade_log);
    let mut summary = run.summary.clone();
    summary.anomaly_count = anomaly_diagnostics.anomaly_count;
    summary.max_abs_anomaly_equity_jump_usd = anomaly_diagnostics.max_abs_equity_jump_usd;
    summary.max_anomaly_mark_share = anomaly_diagnostics.max_mark_share;
    print_replay_summary(&summary);
    print_replay_anomaly_summary(&anomaly_diagnostics);

    if let Some(path) = args.report_json.as_deref() {
        write_json(
            path,
            &RustReplayReport {
                generated_at_utc: Utc::now().to_rfc3339(),
                build_metadata: dataset.metadata.clone(),
                summary: summary.clone(),
                anomaly_diagnostics: anomaly_diagnostics.clone(),
            },
        )?;
    }
    if let Some(path) = args.anomaly_report_json.as_deref() {
        write_json(
            path,
            &ReplayAnomalyReport {
                generated_at_utc: Utc::now().to_rfc3339(),
                build_metadata: dataset.metadata.clone(),
                diagnostics: anomaly_diagnostics.clone(),
            },
        )?;
    }
    if let Some(path) = args.equity_csv.as_deref() {
        write_equity_csv(path, &run.equity_curve)?;
    }
    if let Some(path) = args.trades_csv.as_deref() {
        write_trades_csv(path, &run.trade_log)?;
    }
    if let Some(path) = args.snapshots_csv.as_deref() {
        write_snapshots_csv(path, &run.snapshot_log)?;
    }
    if args.fail_on_anomaly && anomaly_diagnostics.anomaly_count > 0 {
        let top = anomaly_diagnostics.top_anomalies.first();
        bail!(
            "replay anomaly detected: count={} largest_jump={:+.4} start_ts_ms={} end_ts_ms={}",
            anomaly_diagnostics.anomaly_count,
            top.map(|item| item.equity_change_usd).unwrap_or_default(),
            top.map(|item| item.start_ts_ms).unwrap_or_default(),
            top.map(|item| item.end_ts_ms).unwrap_or_default(),
        );
    }
    Ok(())
}

pub async fn run_rust_stress_command(args: RustStressCommandArgs) -> Result<()> {
    let filter = build_filter(
        args.market_id.clone(),
        args.start.as_deref(),
        args.end.as_deref(),
    )?;
    let dataset = load_dataset(&args.db_path, &filter)?;
    let presets = resolve_stress_presets(args.preset);
    let mut results = Vec::with_capacity(presets.len());

    for preset in presets {
        let config = resolve_replay_config(
            &args.db_path,
            args.initial_capital,
            args.rolling_window,
            args.entry_z,
            args.exit_z,
            args.position_notional_usd,
            args.max_capital_usage,
            args.cex_hedge_ratio,
            args.cex_margin_ratio,
            args.planner_depth_levels,
            BandPolicyMode::ConfiguredBand,
            None, // min_poly_price - not available in stress test
            None, // max_poly_price - not available in stress test
            None, // max_holding_bars - not available in stress test
            preset,
        )?;
        let run = run_single_replay(&dataset, &config, false).await?;
        results.push(run.summary);
    }

    print_stress_summary(&results);
    if let Some(path) = args.report_json.as_deref() {
        write_json(
            path,
            &RustStressReport {
                generated_at_utc: Utc::now().to_rfc3339(),
                build_metadata: dataset.metadata.clone(),
                results,
            },
        )?;
    }
    Ok(())
}

fn build_filter(
    market_id: Option<String>,
    start: Option<&str>,
    end: Option<&str>,
) -> Result<ReplayFilter> {
    Ok(ReplayFilter {
        market_id,
        start_ts_ms: parse_time_to_ms(start)?,
        end_ts_ms: parse_time_to_ms(end)?,
    })
}

fn resolve_replay_config(
    db_path: &str,
    initial_capital: f64,
    rolling_window: usize,
    entry_z: f64,
    exit_z: f64,
    position_notional_usd: Option<f64>,
    max_capital_usage: f64,
    cex_hedge_ratio: f64,
    cex_margin_ratio: f64,
    planner_depth_levels: usize,
    band_policy: BandPolicyMode,
    min_poly_price: Option<f64>,
    max_poly_price: Option<f64>,
    max_holding_bars: Option<usize>,
    stress: ExecutionStressConfig,
) -> Result<ResolvedReplayConfig> {
    let initial_capital = initial_capital.max(0.0);
    if initial_capital <= 0.0 {
        bail!("initial capital must be positive");
    }

    let entry_z = entry_z.abs().max(0.0);
    let exit_z = exit_z.abs().min(entry_z.max(0.0));
    let position_notional_usd =
        position_notional_usd.unwrap_or(initial_capital * DEFAULT_POSITION_NOTIONAL_PCT);
    let planner_depth_levels = if planner_depth_levels == 0 {
        MarketDataConfig::default().planner_depth_levels
    } else {
        planner_depth_levels
    };

    Ok(ResolvedReplayConfig {
        db_path: db_path.to_owned(),
        initial_capital,
        rolling_window: rolling_window.max(2),
        entry_z,
        exit_z,
        position_notional_usd: position_notional_usd.max(0.0),
        max_capital_usage: max_capital_usage.max(0.0),
        cex_hedge_ratio: cex_hedge_ratio.max(0.0),
        cex_margin_ratio: cex_margin_ratio.max(0.0),
        planner_depth_levels: planner_depth_levels.max(1),
        band_policy,
        min_poly_price,
        max_poly_price,
        max_holding_bars,
        stress: ExecutionStressConfig {
            label: stress.label,
            poly_fee_bps: stress.poly_fee_bps.max(0.0),
            poly_slippage_bps: stress.poly_slippage_bps.max(0.0),
            cex_fee_bps: stress.cex_fee_bps.max(0.0),
            cex_slippage_bps: stress.cex_slippage_bps.max(0.0),
            entry_fill_ratio: stress.entry_fill_ratio.clamp(0.0, 1.0),
        },
    })
}

fn resolve_stress_presets(selected: Option<RustStressPreset>) -> Vec<ExecutionStressConfig> {
    let all = vec![
        ExecutionStressConfig {
            label: "baseline".to_owned(),
            poly_fee_bps: 2.0,
            poly_slippage_bps: 10.0,
            cex_fee_bps: 2.0,
            cex_slippage_bps: 10.0,
            entry_fill_ratio: 1.0,
        },
        ExecutionStressConfig {
            label: "conservative".to_owned(),
            poly_fee_bps: 2.0,
            poly_slippage_bps: 20.0,
            cex_fee_bps: 4.0,
            cex_slippage_bps: 12.0,
            entry_fill_ratio: 0.8,
        },
        ExecutionStressConfig {
            label: "harsh".to_owned(),
            poly_fee_bps: 4.0,
            poly_slippage_bps: 40.0,
            cex_fee_bps: 6.0,
            cex_slippage_bps: 20.0,
            entry_fill_ratio: 0.6,
        },
    ];

    match selected {
        Some(RustStressPreset::Baseline) => vec![all[0].clone()],
        Some(RustStressPreset::Conservative) => vec![all[1].clone()],
        Some(RustStressPreset::Harsh) => vec![all[2].clone()],
        None => all,
    }
}

fn load_dataset(db_path: &str, filter: &ReplayFilter) -> Result<ReplayDataset> {
    let config = DuckConfig::default()
        .access_mode(AccessMode::ReadOnly)
        .context("failed to configure read-only duckdb access")?;
    let conn = Connection::open_with_flags(db_path, config)
        .with_context(|| format!("failed to open duckdb database `{db_path}`"))?;

    let metadata = load_build_metadata(&conn)?;
    let market_resolutions = load_market_resolutions(&conn)?;
    let markets = load_market_specs(&conn, filter, &market_resolutions, &metadata)?;
    let raw_rows = load_price_rows(&conn, filter)?;
    let sanitization = sanitize_sync_neutral_resets(&markets, raw_rows);
    let rows = sanitization.rows;
    let cex_close_by_ts = load_cex_closes(&conn, &metadata)?;

    if markets.is_empty() {
        bail!("no replay markets matched filter");
    }
    if rows.is_empty() {
        bail!("no grouped polymarket rows matched filter");
    }
    if cex_close_by_ts.is_empty() {
        let cex_table = metadata
            .get("cex_table")
            .map(|s| s.as_str())
            .unwrap_or("binance_btc_1m");
        bail!("no {} 1m rows found", cex_table);
    }

    let mut minute_set = HashSet::new();
    let mut poly_observation_count = 0usize;
    for row in &rows {
        minute_set.insert(row.ts_ms);
        poly_observation_count += usize::from(row.yes_price.is_some());
        poly_observation_count += usize::from(row.no_price.is_some());
    }

    Ok(ReplayDataset {
        metadata,
        markets,
        rows,
        cex_close_by_ts,
        unique_minutes: minute_set.len(),
        poly_observation_count,
        sanitization: sanitization.diagnostics,
    })
}

fn load_build_metadata(conn: &Connection) -> Result<HashMap<String, String>> {
    let mut stmt = conn.prepare("select key, value from build_metadata order by key asc")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;

    let mut metadata = HashMap::new();
    for row in rows {
        let (key, value) = row?;
        metadata.insert(key, value);
    }
    Ok(metadata)
}

fn load_market_resolutions(conn: &Connection) -> Result<HashMap<(String, String), f64>> {
    let mut stmt = conn.prepare("select cast(raw_json as varchar) from gamma_events")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut payouts = HashMap::new();

    for row in rows {
        let raw_json = row?;
        let payload: JsonValue =
            serde_json::from_str(&raw_json).context("failed to parse gamma_events.raw_json")?;
        let Some(markets) = payload.get("markets").and_then(JsonValue::as_array) else {
            continue;
        };
        for market in markets {
            let Some(market_id) = market
                .get("id")
                .and_then(JsonValue::as_i64)
                .map(|value| value.to_string())
                .or_else(|| {
                    market
                        .get("id")
                        .and_then(JsonValue::as_str)
                        .map(ToOwned::to_owned)
                })
            else {
                continue;
            };
            let outcome_prices = parse_outcome_prices(market.get("outcomePrices"));
            let status = market
                .get("umaResolutionStatus")
                .and_then(JsonValue::as_str)
                .unwrap_or_default();
            if !is_official_resolution(&outcome_prices, status) || outcome_prices.len() < 2 {
                continue;
            }
            payouts.insert(
                (market_id.clone(), "yes".to_owned()),
                clamp_probability_value(outcome_prices[0]),
            );
            payouts.insert(
                (market_id, "no".to_owned()),
                clamp_probability_value(outcome_prices[1]),
            );
        }
    }
    Ok(payouts)
}

fn load_market_specs(
    conn: &Connection,
    filter: &ReplayFilter,
    market_resolutions: &HashMap<(String, String), f64>,
    metadata: &HashMap<String, String>,
) -> Result<HashMap<String, ReplayMarketSpec>> {
    let sql = format!(
        "
        select
            market_id,
            any_value(event_id) as event_id,
            any_value(condition_id) as condition_id,
            any_value(market_question) as market_question,
            max(case when lower(token_side) = 'yes' then token_id end) as yes_token_id,
            max(case when lower(token_side) = 'no' then token_id end) as no_token_id,
            min(start_ts) as start_ts,
            max(end_ts) as end_ts
        from polymarket_contracts
        {}
        group by market_id
        order by min(start_ts) asc, market_id asc
        ",
        build_contract_filter_sql(filter)
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, i64>(7)?,
        ))
    })?;

    let mut markets = HashMap::new();
    for row in rows {
        let (
            market_id,
            event_id,
            condition_id,
            market_question,
            yes_token_id,
            no_token_id,
            _,
            end_ts,
        ) = row?;
        let yes_token_id = yes_token_id
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow!("market `{market_id}` missing yes token id"))?;
        let no_token_id = no_token_id
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow!("market `{market_id}` missing no token id"))?;
        let rule = parse_market_rule(&market_question)?;
        let symbol = Symbol::new(format!("mkt-{market_id}"));
        let market_slug = format!("mkt-{market_id}");
        let settlement_ts_ms = i64_to_u64(end_ts)?
            .checked_mul(1000)
            .ok_or_else(|| anyhow!("market `{market_id}` settlement timestamp overflow"))?;
        let strike_price = rule.lower_strike.or(rule.upper_strike);
        let cex_symbol = metadata
            .get("cex_symbol")
            .cloned()
            .unwrap_or_else(|| "BTCUSDT".to_owned());
        let market_config = MarketConfig {
            symbol: symbol.clone(),
            poly_ids: PolymarketIds {
                condition_id: condition_id.unwrap_or_else(|| format!("condition-{market_id}")),
                yes_token_id,
                no_token_id,
            },
            market_question: Some(market_question.clone()),
            market_rule: Some(rule.clone()),
            cex_symbol,
            hedge_exchange: Exchange::Binance,
            strike_price,
            settlement_timestamp: settlement_ts_ms / 1000,
            min_tick_size: Price(Decimal::new(1, 2)),
            neg_risk: false,
            cex_price_tick: Decimal::new(1, 1),
            cex_qty_step: Decimal::new(1, 3),
            cex_contract_multiplier: Decimal::ONE,
        };

        markets.insert(
            market_id.clone(),
            ReplayMarketSpec {
                event_id,
                market_id: market_id.clone(),
                market_slug,
                symbol,
                market_config,
                settlement_ts_ms,
                yes_payout: market_resolutions
                    .get(&(market_id.clone(), "yes".to_owned()))
                    .copied(),
                no_payout: market_resolutions
                    .get(&(market_id, "no".to_owned()))
                    .copied(),
            },
        );
    }
    Ok(markets)
}

fn load_price_rows(conn: &Connection, filter: &ReplayFilter) -> Result<Vec<ReplayMinuteRow>> {
    let sql = format!(
        "
        with minute_market as (
            select
                market_id,
                ts_ms,
                max(case when lower(token_side) = 'yes' then poly_price end) as yes_price,
                max(case when lower(token_side) = 'no' then poly_price end) as no_price
            from polymarket_price_history
            {}
            group by market_id, ts_ms
        )
        select market_id, ts_ms, yes_price, no_price
        from minute_market
        order by ts_ms asc, market_id asc
        ",
        build_row_filter_sql(filter)
    );

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, Option<f64>>(2)?,
            row.get::<_, Option<f64>>(3)?,
        ))
    })?;

    let mut out = Vec::new();
    for row in rows {
        let (market_id, ts_ms_raw, yes_price, no_price) = row?;
        out.push(ReplayMinuteRow {
            market_id,
            ts_ms: i64_to_u64(ts_ms_raw)?,
            yes_price,
            no_price,
        });
    }
    Ok(out)
}

fn load_cex_closes(
    conn: &Connection,
    metadata: &HashMap<String, String>,
) -> Result<HashMap<u64, f64>> {
    let cex_table = metadata
        .get("cex_table")
        .map(|s| s.as_str())
        .unwrap_or("binance_btc_1m");
    let sql = format!("select ts_ms, close from {} order by ts_ms asc", cex_table);
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, f64>(1)?)))?;

    let mut out = HashMap::new();
    for row in rows {
        let (ts_ms_raw, close) = row?;
        let ts_ms = i64_to_u64(ts_ms_raw)?;
        out.insert(ts_ms, close);
    }
    Ok(out)
}

fn build_contract_filter_sql(filter: &ReplayFilter) -> String {
    let mut clauses = Vec::new();
    if let Some(market_id) = filter.market_id.as_deref() {
        clauses.push(format!("market_id = {}", sql_quote(market_id)));
    }
    if let Some(start_ts_ms) = filter.start_ts_ms {
        clauses.push(format!("(end_ts * 1000) >= {start_ts_ms}"));
    }
    if let Some(end_ts_ms) = filter.end_ts_ms {
        clauses.push(format!("(start_ts * 1000) <= {end_ts_ms}"));
    }
    if clauses.is_empty() {
        String::new()
    } else {
        format!("where {}", clauses.join(" and "))
    }
}

fn build_row_filter_sql(filter: &ReplayFilter) -> String {
    let mut clauses = Vec::new();
    if let Some(market_id) = filter.market_id.as_deref() {
        clauses.push(format!("market_id = {}", sql_quote(market_id)));
    }
    if let Some(start_ts_ms) = filter.start_ts_ms {
        clauses.push(format!("ts_ms >= {start_ts_ms}"));
    }
    if let Some(end_ts_ms) = filter.end_ts_ms {
        clauses.push(format!("ts_ms <= {end_ts_ms}"));
    }
    if clauses.is_empty() {
        String::new()
    } else {
        format!("where {}", clauses.join(" and "))
    }
}

async fn run_single_replay(
    dataset: &ReplayDataset,
    config: &ResolvedReplayConfig,
    capture_snapshots: bool,
) -> Result<ReplayRunResult> {
    let markets = dataset
        .markets
        .values()
        .map(|spec| spec.market_config.clone())
        .collect::<Vec<_>>();
    let engine_config = SimpleEngineConfig {
        min_signal_samples: 2,
        rolling_window_minutes: config.rolling_window,
        entry_z: config.entry_z,
        exit_z: config.exit_z,
        position_notional_usd: UsdNotional(decimal_from_f64(config.position_notional_usd)?),
        cex_hedge_ratio: config.cex_hedge_ratio,
        dmm_half_spread: Decimal::new(1, 2),
        dmm_quote_size: polyalpha_core::PolyShares::ZERO,
        ..SimpleEngineConfig::default()
    };
    let mut engine = SimpleAlphaEngine::with_markets(engine_config, markets);

    let mut runtime = dataset
        .markets
        .keys()
        .map(|market_id| (market_id.clone(), ReplayRuntimeState::default()))
        .collect::<HashMap<_, _>>();
    let mut positions: HashMap<Symbol, ReplayPosition> = HashMap::new();
    let mut equity_curve = Vec::new();
    let mut trade_log = Vec::new();
    let mut snapshot_log = Vec::new();
    let mut stats = ReplayStats::default();
    let planner = replay_execution_planner(config);
    let mut cash = config.initial_capital;
    let mut peak_equity = config.initial_capital;
    let mut last_batch_ts = None;
    let mut last_cex_reference_price = None;
    let mut replayed_market_ids = HashSet::new();
    let mut replayed_event_ids = HashSet::new();

    let mut idx = 0usize;
    while idx < dataset.rows.len() {
        let ts_ms = dataset.rows[idx].ts_ms;
        let mut batch_end = idx + 1;
        while batch_end < dataset.rows.len() && dataset.rows[batch_end].ts_ms == ts_ms {
            batch_end += 1;
        }

        stats.grouped_rows_seen += batch_end - idx;
        let cex_reference_price = match ts_ms
            .checked_sub(ONE_MINUTE_MS)
            .and_then(|prior_ts| dataset.cex_close_by_ts.get(&prior_ts).copied())
        {
            Some(price) => price,
            None => {
                stats.grouped_rows_skipped_missing_cex += batch_end - idx;
                idx = batch_end;
                continue;
            }
        };
        let cex_advance_price = match dataset.cex_close_by_ts.get(&ts_ms).copied() {
            Some(price) => price,
            None => {
                stats.grouped_rows_skipped_missing_cex += batch_end - idx;
                idx = batch_end;
                continue;
            }
        };

        for position in positions.values_mut() {
            position.last_cex_price = cex_reference_price;
        }

        let mut pending_close_signals = Vec::new();
        let mut pending_entry_signals = Vec::new();
        for row in &dataset.rows[idx..batch_end] {
            let Some(spec) = dataset.markets.get(&row.market_id) else {
                continue;
            };
            replayed_market_ids.insert(spec.market_id.clone());
            replayed_event_ids.insert(spec.event_id.clone());
            let state = runtime
                .get_mut(&row.market_id)
                .ok_or_else(|| anyhow!("runtime state missing for market `{}`", row.market_id))?;

            if !state.bootstrapped {
                bootstrap_symbol_cex_state(
                    &mut engine,
                    &spec.symbol,
                    &dataset.cex_close_by_ts,
                    config.rolling_window,
                    ts_ms,
                )
                .await?;
                state.bootstrapped = true;
                state.last_cex_synced_minute_ms =
                    Some(ts_ms.checked_sub(ONE_MINUTE_MS).unwrap_or_default());
            }

            advance_symbol_cex_state(
                &mut engine,
                &spec.symbol,
                &dataset.cex_close_by_ts,
                state,
                ts_ms,
            )
            .await?;

            if let Some(position) = positions.get_mut(&spec.symbol) {
                match position.token_side {
                    TokenSide::Yes => {
                        if let Some(price) = row.yes_price {
                            position.last_poly_price = price;
                        }
                    }
                    TokenSide::No => {
                        if let Some(price) = row.no_price {
                            position.last_poly_price = price;
                        }
                    }
                }
            }

            if let Some(yes_price) = row.yes_price {
                let _ = engine
                    .on_market_data(&poly_trade_event(
                        &spec.symbol,
                        InstrumentKind::PolyYes,
                        yes_price,
                        ts_ms,
                    ))
                    .await;
                stats.poly_observations_replayed += 1;
            }
            if let Some(no_price) = row.no_price {
                let _ = engine
                    .on_market_data(&poly_trade_event(
                        &spec.symbol,
                        InstrumentKind::PolyNo,
                        no_price,
                        ts_ms,
                    ))
                    .await;
                stats.poly_observations_replayed += 1;
            }

            let output = engine
                .on_market_data(&cex_trade_event(&spec.symbol, cex_advance_price, ts_ms + 1))
                .await;
            stats.grouped_rows_replayed += 1;

            if capture_snapshots {
                let snapshot = engine.basis_snapshot(&spec.symbol);
                let first_candidate = output.open_candidates.first();
                snapshot_log.push(ReplaySnapshotRow {
                    ts_ms,
                    event_id: spec.event_id.clone(),
                    market_id: spec.market_id.clone(),
                    market_slug: spec.market_slug.clone(),
                    yes_price: row.yes_price,
                    no_price: row.no_price,
                    cex_reference_price,
                    cex_advance_price,
                    signal_count: output.open_candidates.len(),
                    signal_action: first_candidate.map(candidate_action_label),
                    signal_token_side: first_candidate
                        .map(|candidate| candidate.token_side)
                        .map(token_side_label)
                        .map(|s| s.to_string()),
                    signal_basis_bps: first_candidate
                        .map(|candidate| candidate.raw_mispricing * 10_000.0),
                    signal_z_score: first_candidate.and_then(|candidate| candidate.z_score),
                    snapshot_token_side: snapshot
                        .as_ref()
                        .map(|item| token_side_label(item.token_side).to_string()),
                    snapshot_poly_price: snapshot.as_ref().map(|item| item.poly_price),
                    snapshot_fair_value: snapshot.as_ref().map(|item| item.fair_value),
                    snapshot_basis_bps: snapshot.as_ref().map(|item| item.signal_basis * 10_000.0),
                    snapshot_z_score: snapshot.as_ref().and_then(|item| item.z_score),
                    snapshot_delta: snapshot.as_ref().map(|item| item.delta),
                    snapshot_sigma: snapshot.as_ref().and_then(|item| item.sigma),
                    snapshot_minutes_to_expiry: snapshot
                        .as_ref()
                        .map(|item| item.minutes_to_expiry),
                });
            }

            for candidate in output.open_candidates {
                let pending = PendingReplayCandidate {
                    candidate,
                    spec: spec.clone(),
                    row: row.clone(),
                    cex_reference_price,
                };
                pending_entry_signals.push(pending);
            }

            if let Some(reason) = engine.close_reason(&spec.symbol) {
                pending_close_signals.push(PendingReplayClose {
                    close_reason: reason,
                    spec: spec.clone(),
                    row: row.clone(),
                    cex_reference_price,
                });
            }
        }

        for pending in pending_close_signals {
            handle_close_signal(
                &pending.close_reason,
                &pending.spec,
                &pending.row,
                pending.cex_reference_price,
                config,
                &planner,
                &mut positions,
                &mut cash,
                &mut stats,
                &mut trade_log,
            )?;
            engine.sync_position_state(
                &pending.spec.symbol,
                positions
                    .get(&pending.spec.symbol)
                    .map(|position| position.token_side),
            );
        }

        let symbols_to_settle = positions
            .iter()
            .filter_map(|(symbol, position)| {
                let spec = dataset.markets.get(&position.market_id)?;
                (spec.settlement_ts_ms <= ts_ms).then(|| symbol.clone())
            })
            .collect::<Vec<_>>();
        for symbol in symbols_to_settle {
            let market_id = positions
                .get(&symbol)
                .map(|position| position.market_id.clone())
                .ok_or_else(|| {
                    anyhow!("position disappeared before settlement for `{}`", symbol.0)
                })?;
            let spec = dataset
                .markets
                .get(&market_id)
                .ok_or_else(|| anyhow!("market spec missing for settlement `{market_id}`"))?;
            close_position(
                &symbol,
                spec,
                None,
                cex_reference_price,
                true,
                "settled",
                config,
                &planner,
                &mut positions,
                &mut cash,
                &mut stats,
                &mut trade_log,
                ts_ms,
            )?;
            engine.sync_position_state(
                &symbol,
                positions.get(&symbol).map(|position| position.token_side),
            );
        }

        // Max holding time check
        if let Some(max_bars) = config.max_holding_bars {
            let symbols_to_force_close: Vec<(Symbol, String)> = positions
                .iter()
                .filter_map(|(symbol, position)| {
                    let holding_bars =
                        (ts_ms.saturating_sub(position.entry_ts_ms) / ONE_MINUTE_MS) as usize;
                    if holding_bars >= max_bars {
                        Some((symbol.clone(), format!("max_holding_{}min", max_bars)))
                    } else {
                        None
                    }
                })
                .collect();

            for (symbol, reason) in symbols_to_force_close {
                let market_id = positions
                    .get(&symbol)
                    .map(|position| position.market_id.clone())
                    .ok_or_else(|| {
                        anyhow!(
                            "position disappeared before max holding close for `{}`",
                            symbol.0
                        )
                    })?;
                let spec = dataset.markets.get(&market_id).ok_or_else(|| {
                    anyhow!("market spec missing for max holding close `{market_id}`")
                })?;
                close_position(
                    &symbol,
                    spec,
                    None,
                    cex_reference_price,
                    false,
                    &reason,
                    config,
                    &planner,
                    &mut positions,
                    &mut cash,
                    &mut stats,
                    &mut trade_log,
                    ts_ms,
                )?;
                engine.sync_position_state(
                    &symbol,
                    positions.get(&symbol).map(|position| position.token_side),
                );
            }
        }

        pending_entry_signals.sort_by(|left, right| {
            let left_z = left.candidate.z_score.unwrap_or(f64::INFINITY);
            let right_z = right.candidate.z_score.unwrap_or(f64::INFINITY);
            left_z
                .partial_cmp(&right_z)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    let left_basis = left.candidate.raw_mispricing.abs();
                    let right_basis = right.candidate.raw_mispricing.abs();
                    right_basis
                        .partial_cmp(&left_basis)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
        });

        for pending in pending_entry_signals {
            handle_candidate(
                pending.candidate,
                &pending.spec,
                &pending.row,
                pending.cex_reference_price,
                config,
                &planner,
                &mut positions,
                &mut cash,
                &mut stats,
                &mut trade_log,
            )?;
            engine.sync_position_state(
                &pending.spec.symbol,
                positions
                    .get(&pending.spec.symbol)
                    .map(|position| position.token_side),
            );
        }

        let equity = record_equity_point(
            ts_ms,
            config,
            &positions,
            cash,
            &mut stats,
            &mut peak_equity,
            &mut equity_curve,
        );
        last_cex_reference_price = Some(cex_reference_price);
        last_batch_ts = Some(ts_ms);
        if !equity.is_finite() {
            bail!("equity became non-finite at {ts_ms}");
        }
        idx = batch_end;
    }

    if !positions.is_empty() {
        let Some(final_cex_price) = last_cex_reference_price else {
            bail!("cannot settle remaining positions without a final CEX reference price");
        };
        let remaining_symbols = positions.keys().cloned().collect::<Vec<_>>();
        for symbol in remaining_symbols {
            let market_id = positions
                .get(&symbol)
                .map(|position| position.market_id.clone())
                .ok_or_else(|| anyhow!("position disappeared during final settlement"))?;
            let spec = dataset
                .markets
                .get(&market_id)
                .ok_or_else(|| anyhow!("market spec missing for final settlement `{market_id}`"))?;
            close_position(
                &symbol,
                spec,
                None,
                final_cex_price,
                true,
                "settled",
                config,
                &planner,
                &mut positions,
                &mut cash,
                &mut stats,
                &mut trade_log,
                last_batch_ts.unwrap_or_default(),
            )?;
            engine.sync_position_state(&symbol, None);
        }

        if let Some(ts_ms) = last_batch_ts {
            record_equity_point(
                ts_ms,
                config,
                &positions,
                cash,
                &mut stats,
                &mut peak_equity,
                &mut equity_curve,
            );
        }
    }

    let ending_equity = equity_curve
        .last()
        .map(|point| point.equity)
        .unwrap_or(config.initial_capital);
    let unrealized_pnl = equity_curve
        .last()
        .map(|point| point.unrealized_pnl)
        .unwrap_or(0.0);
    let realized_pnl = stats.poly_realized_gross + stats.cex_realized_gross
        - stats.total_fees_paid
        - stats.total_slippage_paid;

    let summary = RustReplaySummary {
        db_path: config.db_path.clone(),
        stress: config.stress.clone(),
        band_policy: config.band_policy,
        initial_capital: config.initial_capital,
        ending_equity,
        total_pnl: ending_equity - config.initial_capital,
        realized_pnl,
        unrealized_pnl,
        poly_realized_gross: stats.poly_realized_gross,
        cex_realized_gross: stats.cex_realized_gross,
        max_drawdown: stats.max_drawdown,
        max_gross_exposure: stats.max_gross_exposure,
        max_reserved_margin: stats.max_reserved_margin,
        max_capital_in_use: stats.max_capital_in_use,
        max_capital_utilization: stats.max_capital_utilization,
        signal_count: stats.signals_emitted,
        signal_rejected_capital: stats.signals_rejected_capital,
        signal_rejected_missing_price: stats.signals_rejected_missing_price,
        signal_rejected_below_min_poly_price: stats.signals_rejected_below_min_poly_price,
        signal_rejected_above_or_equal_max_poly_price: stats
            .signals_rejected_above_or_equal_max_poly_price,
        signal_rejected_planner: stats.signals_rejected_planner,
        signal_ignored_existing_position: stats.signals_ignored_existing_position,
        entry_count: stats.entry_count,
        close_count: stats.close_count,
        settlement_count: stats.settlement_count,
        trade_count: stats.trade_count,
        poly_trade_count: stats.poly_trade_count,
        cex_trade_count: stats.cex_trade_count,
        round_trip_count: stats.round_trip_count,
        winning_round_trips: stats.winning_round_trips,
        win_rate: ratio(stats.winning_round_trips, stats.round_trip_count),
        total_fees_paid: stats.total_fees_paid,
        total_slippage_paid: stats.total_slippage_paid,
        poly_fees_paid: stats.poly_fees_paid,
        poly_slippage_paid: stats.poly_slippage_paid,
        cex_fees_paid: stats.cex_fees_paid,
        cex_slippage_paid: stats.cex_slippage_paid,
        event_count: replayed_event_ids.len(),
        market_count: replayed_market_ids.len(),
        minute_count: dataset.unique_minutes,
        grouped_row_count: dataset.rows.len(),
        poly_observation_count: dataset.poly_observation_count,
        sanitized_market_minute_count: dataset.sanitization.sanitized_market_minutes,
        sanitized_poly_observation_count: dataset.sanitization.sanitized_poly_observations,
        sanitized_sync_reset_window_count: dataset.sanitization.sync_reset_windows.len(),
        replayed_grouped_rows: stats.grouped_rows_replayed,
        replayed_poly_observations: stats.poly_observations_replayed,
        open_positions_end: positions.len(),
        start_ts_ms: dataset.rows.first().map(|row| row.ts_ms),
        end_ts_ms: dataset.rows.last().map(|row| row.ts_ms),
        anomaly_count: 0,
        max_abs_anomaly_equity_jump_usd: 0.0,
        max_anomaly_mark_share: 0.0,
        top_signal_markets: top_signal_markets(&stats, 10),
        top_entry_markets: top_entry_markets(&stats, 10),
        market_debug_rows: all_market_debug_rows(&stats),
    };

    Ok(ReplayRunResult {
        summary,
        equity_curve,
        trade_log,
        snapshot_log,
    })
}

async fn bootstrap_symbol_cex_state(
    engine: &mut SimpleAlphaEngine,
    symbol: &Symbol,
    cex_close_by_ts: &HashMap<u64, f64>,
    rolling_window: usize,
    first_observation_minute_ms: u64,
) -> Result<()> {
    let lookback_minutes = rolling_window.max(2) as u64;
    let bootstrap_start_minute_ms =
        first_observation_minute_ms.saturating_sub(lookback_minutes.saturating_mul(ONE_MINUTE_MS));
    let bootstrap_end_minute_ms = first_observation_minute_ms
        .checked_sub(ONE_MINUTE_MS)
        .ok_or_else(|| anyhow!("first observation minute underflow for `{}`", symbol.0))?;

    let mut minute_ms = bootstrap_start_minute_ms;
    while minute_ms <= bootstrap_end_minute_ms {
        if let Some(close) = cex_close_by_ts.get(&minute_ms).copied() {
            let _ = engine
                .on_market_data(&cex_trade_event(symbol, close, minute_ms + 1))
                .await;
        }
        minute_ms = minute_ms
            .checked_add(ONE_MINUTE_MS)
            .ok_or_else(|| anyhow!("bootstrap minute overflow for `{}`", symbol.0))?;
    }
    Ok(())
}

async fn advance_symbol_cex_state(
    engine: &mut SimpleAlphaEngine,
    symbol: &Symbol,
    cex_close_by_ts: &HashMap<u64, f64>,
    state: &mut ReplayRuntimeState,
    target_minute_ms: u64,
) -> Result<()> {
    let mut synced_minute_ms = state
        .last_cex_synced_minute_ms
        .ok_or_else(|| anyhow!("cex state not bootstrapped for `{}`", symbol.0))?;

    while synced_minute_ms < target_minute_ms {
        let next_minute_ms = synced_minute_ms
            .checked_add(ONE_MINUTE_MS)
            .ok_or_else(|| anyhow!("cex minute overflow for `{}`", symbol.0))?;
        let close = cex_close_by_ts
            .get(&next_minute_ms)
            .copied()
            .ok_or_else(|| {
                anyhow!(
                    "missing cex close for `{}` at minute {} while advancing replay",
                    symbol.0,
                    next_minute_ms
                )
            })?;
        let _ = engine
            .on_market_data(&cex_trade_event(symbol, close, next_minute_ms + 1))
            .await;
        synced_minute_ms = next_minute_ms;
    }

    state.last_cex_synced_minute_ms = Some(synced_minute_ms);
    Ok(())
}

fn replay_execution_planner(config: &ResolvedReplayConfig) -> ExecutionPlanner {
    ExecutionPlanner {
        poly_fee_bps: rounded_bps(config.stress.poly_fee_bps),
        cex_taker_fee_bps: rounded_bps(config.stress.cex_fee_bps),
        cex_maker_fee_bps: rounded_bps(config.stress.cex_fee_bps),
        funding_mode: polyalpha_core::FundingCostMode::FallbackBps,
        fallback_funding_bps_per_day: 0,
        poly_slippage_bps: rounded_bps(config.stress.poly_slippage_bps),
        cex_slippage_bps: rounded_bps(config.stress.cex_slippage_bps),
        max_open_instant_loss_pct_of_budget: Decimal::new(4, 2),
    }
}

fn rounded_bps(value: f64) -> u32 {
    value.max(0.0).round() as u32
}

fn replay_open_intent(candidate: &OpenCandidate) -> PlanningIntent {
    PlanningIntent::OpenPosition {
        schema_version: candidate.schema_version,
        intent_id: format!("replay-open-{}", candidate.candidate_id),
        correlation_id: candidate.correlation_id.clone(),
        symbol: candidate.symbol.clone(),
        candidate: candidate.clone(),
        max_budget_usd: candidate.risk_budget_usd,
        max_residual_delta: 0.05,
        max_shock_loss_usd: candidate.risk_budget_usd.max(25.0),
    }
}

fn replay_close_intent(symbol: &Symbol, close_reason: &str, ts_ms: u64) -> PlanningIntent {
    PlanningIntent::ClosePosition {
        schema_version: 1,
        intent_id: format!("replay-close-{}-{ts_ms}", symbol.0),
        correlation_id: format!("replay-close-{}-{ts_ms}", symbol.0),
        symbol: symbol.clone(),
        close_reason: close_reason.to_owned(),
        target_close_ratio: 1.0,
    }
}

fn synthetic_book_quantity() -> Decimal {
    Decimal::new(1_000_000, 0)
}

fn decimal_price(value: f64, label: &str) -> Result<Decimal> {
    Decimal::from_f64(value).ok_or_else(|| anyhow!("invalid {label} price `{value}`"))
}

fn synthetic_poly_book(
    symbol: &Symbol,
    token_side: TokenSide,
    price: f64,
    ts_ms: u64,
) -> Result<OrderBookSnapshot> {
    let price = Price(decimal_price(price, "poly")?);
    Ok(OrderBookSnapshot {
        exchange: Exchange::Polymarket,
        symbol: symbol.clone(),
        instrument: match token_side {
            TokenSide::Yes => InstrumentKind::PolyYes,
            TokenSide::No => InstrumentKind::PolyNo,
        },
        bids: vec![PriceLevel {
            price,
            quantity: VenueQuantity::PolyShares(PolyShares(synthetic_book_quantity())),
        }],
        asks: vec![PriceLevel {
            price,
            quantity: VenueQuantity::PolyShares(PolyShares(synthetic_book_quantity())),
        }],
        exchange_timestamp_ms: ts_ms,
        received_at_ms: ts_ms,
        sequence: ts_ms,
        last_trade_price: Some(price),
    })
}

fn synthetic_cex_book(symbol: &Symbol, price: f64, ts_ms: u64) -> Result<OrderBookSnapshot> {
    let price = Price(decimal_price(price, "cex")?);
    Ok(OrderBookSnapshot {
        exchange: Exchange::Binance,
        symbol: symbol.clone(),
        instrument: InstrumentKind::CexPerp,
        bids: vec![PriceLevel {
            price,
            quantity: VenueQuantity::CexBaseQty(CexBaseQty(synthetic_book_quantity())),
        }],
        asks: vec![PriceLevel {
            price,
            quantity: VenueQuantity::CexBaseQty(CexBaseQty(synthetic_book_quantity())),
        }],
        exchange_timestamp_ms: ts_ms,
        received_at_ms: ts_ms,
        sequence: ts_ms,
        last_trade_price: Some(price),
    })
}

fn replay_planning_context(
    planner_depth_levels: usize,
    symbol: &Symbol,
    row: &ReplayMinuteRow,
    cex_reference_price: f64,
    cex_qty_step: Decimal,
    positions: &HashMap<Symbol, ReplayPosition>,
) -> Result<CanonicalPlanningContext> {
    let yes_price = row
        .yes_price
        .ok_or_else(|| anyhow!("missing yes price for `{}` at {}", symbol.0, row.ts_ms))?;
    let no_price = row
        .no_price
        .ok_or_else(|| anyhow!("missing no price for `{}` at {}", symbol.0, row.ts_ms))?;
    if cex_reference_price <= 0.0 {
        bail!(
            "missing cex reference price for `{}` at {}",
            symbol.0,
            row.ts_ms
        );
    }

    let current_position = positions.get(symbol);
    let (current_poly_yes_shares, current_poly_no_shares, current_cex_net_qty) =
        match current_position {
            Some(position) => match position.token_side {
                TokenSide::Yes => (
                    Decimal::from_f64(position.shares).unwrap_or(Decimal::ZERO),
                    Decimal::ZERO,
                    Decimal::from_f64(position.cex_qty_btc).unwrap_or(Decimal::ZERO),
                ),
                TokenSide::No => (
                    Decimal::ZERO,
                    Decimal::from_f64(position.shares).unwrap_or(Decimal::ZERO),
                    Decimal::from_f64(position.cex_qty_btc).unwrap_or(Decimal::ZERO),
                ),
            },
            None => (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO),
        };

    Ok(CanonicalPlanningContext {
        planner_depth_levels: planner_depth_levels.max(1),
        poly_yes_book: synthetic_poly_book(symbol, TokenSide::Yes, yes_price, row.ts_ms)?,
        poly_no_book: synthetic_poly_book(symbol, TokenSide::No, no_price, row.ts_ms)?,
        cex_book: synthetic_cex_book(symbol, cex_reference_price, row.ts_ms)?,
        cex_qty_step,
        current_poly_yes_shares,
        current_poly_no_shares,
        current_cex_net_qty,
        now_ms: row.ts_ms,
    })
}

fn plan_replay_open_candidate(
    planner: &ExecutionPlanner,
    planner_depth_levels: usize,
    candidate: &OpenCandidate,
    row: &ReplayMinuteRow,
    cex_reference_price: f64,
    cex_qty_step: Decimal,
    positions: &HashMap<Symbol, ReplayPosition>,
) -> Result<TradePlan> {
    let intent = replay_open_intent(candidate);
    let context = replay_planning_context(
        planner_depth_levels,
        &candidate.symbol,
        row,
        cex_reference_price,
        cex_qty_step,
        positions,
    )?;
    planner.plan(&intent, &context).map_err(|rejection| {
        anyhow!(
            "replay planner rejected [{}]: {}",
            rejection.reason.code(),
            rejection.detail
        )
    })
}

fn plan_replay_close_position(
    planner: &ExecutionPlanner,
    planner_depth_levels: usize,
    symbol: &Symbol,
    close_reason: &str,
    row: &ReplayMinuteRow,
    cex_reference_price: f64,
    cex_qty_step: Decimal,
    positions: &HashMap<Symbol, ReplayPosition>,
) -> Result<TradePlan> {
    let intent = replay_close_intent(symbol, close_reason, row.ts_ms);
    let context = replay_planning_context(
        planner_depth_levels,
        symbol,
        row,
        cex_reference_price,
        cex_qty_step,
        positions,
    )?;
    planner.plan(&intent, &context).map_err(|rejection| {
        anyhow!(
            "replay close planner rejected [{}]: {}",
            rejection.reason.code(),
            rejection.detail
        )
    })
}

fn scaled_decimal(value: Decimal, ratio: Decimal) -> Decimal {
    (value * ratio).max(Decimal::ZERO)
}

fn replay_close_row(
    spec: &ReplayMarketSpec,
    position: &ReplayPosition,
    market_exit_price: Option<f64>,
    ts_ms: u64,
) -> ReplayMinuteRow {
    let current_side_price =
        clamp_probability_value(market_exit_price.unwrap_or(position.last_poly_price));
    let complementary_price = clamp_probability_value(1.0 - current_side_price);
    let (yes_price, no_price) = match position.token_side {
        TokenSide::Yes => (Some(current_side_price), Some(complementary_price)),
        TokenSide::No => (Some(complementary_price), Some(current_side_price)),
    };
    ReplayMinuteRow {
        ts_ms,
        market_id: spec.market_id.clone(),
        yes_price,
        no_price,
    }
}

fn replay_slipped_price(
    price: f64,
    side: OrderSide,
    slippage_bps: f64,
    clamp_probability: bool,
) -> f64 {
    let multiplier = 1.0
        + match side {
            OrderSide::Buy => slippage_bps / 10_000.0,
            OrderSide::Sell => -slippage_bps / 10_000.0,
        };
    let slipped = price * multiplier;
    if clamp_probability {
        slipped.clamp(0.0, 1.0)
    } else {
        slipped.max(0.0)
    }
}

fn handle_candidate(
    candidate: OpenCandidate,
    spec: &ReplayMarketSpec,
    row: &ReplayMinuteRow,
    cex_reference_price: f64,
    config: &ResolvedReplayConfig,
    planner: &ExecutionPlanner,
    positions: &mut HashMap<Symbol, ReplayPosition>,
    cash: &mut f64,
    stats: &mut ReplayStats,
    _trade_log: &mut Vec<ReplayTradeRow>,
) -> Result<()> {
    stats.signals_emitted += 1;
    *stats
        .signals_by_market
        .entry(spec.market_id.clone())
        .or_insert(0) += 1;
    let entry_z_score = candidate.z_score.unwrap_or_default();
    let entry_basis_bps = candidate.raw_mispricing * 10_000.0;
    if positions.contains_key(&spec.symbol) {
        stats.signals_ignored_existing_position += 1;
        return Ok(());
    }

    let token_side = candidate.token_side;
    let Some(poly_entry_price) = row.price_for(token_side) else {
        stats.signals_rejected_missing_price += 1;
        *stats
            .rejected_missing_price_by_market
            .entry(spec.market_id.clone())
            .or_insert(0) += 1;
        return Ok(());
    };
    if poly_entry_price <= 0.0 || cex_reference_price <= 0.0 {
        stats.signals_rejected_missing_price += 1;
        *stats
            .rejected_missing_price_by_market
            .entry(spec.market_id.clone())
            .or_insert(0) += 1;
        return Ok(());
    }

    if config.band_policy.uses_configured_band() {
        if let Some(min_price) = config.min_poly_price {
            if poly_entry_price < min_price {
                stats.signals_rejected_below_min_poly_price += 1;
                *stats
                    .rejected_below_min_price_by_market
                    .entry(spec.market_id.clone())
                    .or_insert(0) += 1;
                return Ok(());
            }
        }
        if let Some(max_price) = config.max_poly_price {
            if poly_entry_price >= max_price {
                stats.signals_rejected_above_or_equal_max_poly_price += 1;
                *stats
                    .rejected_above_or_equal_max_price_by_market
                    .entry(spec.market_id.clone())
                    .or_insert(0) += 1;
                return Ok(());
            }
        }
    }

    let plan = match plan_replay_open_candidate(
        planner,
        config.planner_depth_levels,
        &candidate,
        row,
        cex_reference_price,
        spec.market_config.cex_qty_step,
        positions,
    ) {
        Ok(plan) => plan,
        Err(_) => {
            stats.signals_rejected_planner += 1;
            return Ok(());
        }
    };
    let fill_ratio =
        Decimal::from_f64(config.stress.entry_fill_ratio.clamp(0.0, 1.0)).unwrap_or(Decimal::ONE);
    let planned_shares = plan.poly_planned_shares.0;
    let actual_poly_shares = scaled_decimal(planned_shares, fill_ratio);
    if actual_poly_shares <= Decimal::ZERO {
        stats.signals_rejected_planner += 1;
        return Ok(());
    }
    let actual_fill_ratio = if planned_shares > Decimal::ZERO {
        actual_poly_shares / planned_shares
    } else {
        Decimal::ZERO
    };
    let shares = actual_poly_shares.to_f64().unwrap_or_default();

    let poly_entry_price = plan
        .poly_executable_price
        .0
        .to_f64()
        .unwrap_or(poly_entry_price);
    let poly_entry_notional = (actual_poly_shares * plan.poly_executable_price.0)
        .to_f64()
        .unwrap_or_default();
    let actual_cex_qty = scaled_decimal(plan.cex_planned_qty.0, actual_fill_ratio);
    let cex_qty_abs = actual_cex_qty.to_f64().unwrap_or_default();
    let cex_side = plan.cex_side;
    let cex_qty_btc = if matches!(cex_side, OrderSide::Buy) {
        cex_qty_abs
    } else {
        -cex_qty_abs
    };
    let cex_entry_price = plan
        .cex_executable_price
        .0
        .to_f64()
        .unwrap_or(cex_reference_price);
    let cex_entry_notional = (actual_cex_qty * plan.cex_executable_price.0)
        .to_f64()
        .unwrap_or_default();
    let margin_required = cex_entry_notional * config.cex_margin_ratio;
    let (_, capital_in_use) = portfolio_usage(positions);
    let reserved_margin_total = portfolio_usage(positions).0;
    let available_cash = *cash - reserved_margin_total;
    let poly_fee_paid = scaled_decimal(plan.poly_fee_usd.0, actual_fill_ratio)
        .to_f64()
        .unwrap_or_default();
    let poly_slippage_paid = scaled_decimal(plan.poly_friction_cost_usd.0, actual_fill_ratio)
        .to_f64()
        .unwrap_or_default();
    let cex_fee_paid = scaled_decimal(plan.cex_fee_usd.0, actual_fill_ratio)
        .to_f64()
        .unwrap_or_default();
    let cex_slippage_paid = scaled_decimal(plan.cex_friction_cost_usd.0, actual_fill_ratio)
        .to_f64()
        .unwrap_or_default();
    let estimated_entry_costs = poly_fee_paid + cex_fee_paid;
    let required_cash = poly_entry_notional + margin_required + estimated_entry_costs;
    let capital_limit = config.initial_capital * config.max_capital_usage;
    if available_cash + 1e-12 < required_cash
        || capital_in_use + poly_entry_notional + margin_required > capital_limit + 1e-12
    {
        stats.signals_rejected_capital += 1;
        *stats
            .rejected_capital_by_market
            .entry(spec.market_id.clone())
            .or_insert(0) += 1;
        return Ok(());
    }

    *cash -= poly_entry_notional;
    *cash -= poly_fee_paid;
    record_costs(
        stats,
        poly_fee_paid,
        poly_slippage_paid,
        true,
        true,
        poly_entry_notional > 0.0,
    );

    let mut round_trip_pnl = -poly_fee_paid;
    let mut entry_fees_paid = poly_fee_paid;
    let mut entry_slippage_paid = poly_slippage_paid;
    if cex_entry_notional > 0.0 {
        *cash -= cex_fee_paid;
        round_trip_pnl -= cex_fee_paid;
        record_costs(stats, cex_fee_paid, cex_slippage_paid, false, true, true);
        entry_fees_paid += cex_fee_paid;
        entry_slippage_paid += cex_slippage_paid;
    }

    positions.insert(
        spec.symbol.clone(),
        ReplayPosition {
            market_id: spec.market_id.clone(),
            token_side,
            shares,
            poly_entry_notional,
            last_poly_price: poly_entry_price,
            cex_entry_price,
            cex_qty_btc,
            last_cex_price: cex_entry_price,
            reserved_margin: margin_required,
            round_trip_pnl,
            entry_ts_ms: row.ts_ms,
            entry_poly_price: poly_entry_price,
            entry_fees_paid,
            entry_slippage_paid,
            entry_z_score,
            entry_basis_bps,
            entry_delta: candidate.delta_estimate,
        },
    );
    stats.entry_count += 1;
    *stats
        .entries_by_market
        .entry(spec.market_id.clone())
        .or_insert(0) += 1;
    Ok(())
}

fn handle_close_signal(
    close_reason: &str,
    spec: &ReplayMarketSpec,
    row: &ReplayMinuteRow,
    cex_reference_price: f64,
    config: &ResolvedReplayConfig,
    planner: &ExecutionPlanner,
    positions: &mut HashMap<Symbol, ReplayPosition>,
    cash: &mut f64,
    stats: &mut ReplayStats,
    trade_log: &mut Vec<ReplayTradeRow>,
) -> Result<()> {
    if positions.contains_key(&spec.symbol) {
        close_position(
            &spec.symbol,
            spec,
            row.current_price_for_position(positions.get(&spec.symbol)),
            cex_reference_price,
            false,
            close_reason,
            config,
            planner,
            positions,
            cash,
            stats,
            trade_log,
            row.ts_ms,
        )?;
    }
    Ok(())
}

fn top_signal_markets(stats: &ReplayStats, limit: usize) -> Vec<MarketSignalDebugRow> {
    let mut rows = stats
        .signals_by_market
        .iter()
        .map(|(market_id, signal_count)| MarketSignalDebugRow {
            market_id: market_id.clone(),
            signal_count: *signal_count,
            rejected_capital_count: stats
                .rejected_capital_by_market
                .get(market_id)
                .copied()
                .unwrap_or_default(),
            rejected_missing_price_count: stats
                .rejected_missing_price_by_market
                .get(market_id)
                .copied()
                .unwrap_or_default(),
            rejected_below_min_price_count: stats
                .rejected_below_min_price_by_market
                .get(market_id)
                .copied()
                .unwrap_or_default(),
            rejected_above_or_equal_max_price_count: stats
                .rejected_above_or_equal_max_price_by_market
                .get(market_id)
                .copied()
                .unwrap_or_default(),
            entry_count: stats
                .entries_by_market
                .get(market_id)
                .copied()
                .unwrap_or_default(),
            net_pnl: stats
                .net_pnl_by_market
                .get(market_id)
                .copied()
                .unwrap_or_default(),
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .signal_count
            .cmp(&left.signal_count)
            .then_with(|| {
                right
                    .rejected_capital_count
                    .cmp(&left.rejected_capital_count)
            })
            .then_with(|| right.entry_count.cmp(&left.entry_count))
            .then_with(|| left.market_id.cmp(&right.market_id))
    });
    rows.truncate(limit);
    rows
}

fn top_entry_markets(stats: &ReplayStats, limit: usize) -> Vec<MarketSignalDebugRow> {
    let mut rows = stats
        .entries_by_market
        .iter()
        .map(|(market_id, entry_count)| MarketSignalDebugRow {
            market_id: market_id.clone(),
            signal_count: stats
                .signals_by_market
                .get(market_id)
                .copied()
                .unwrap_or_default(),
            rejected_capital_count: stats
                .rejected_capital_by_market
                .get(market_id)
                .copied()
                .unwrap_or_default(),
            rejected_missing_price_count: stats
                .rejected_missing_price_by_market
                .get(market_id)
                .copied()
                .unwrap_or_default(),
            rejected_below_min_price_count: stats
                .rejected_below_min_price_by_market
                .get(market_id)
                .copied()
                .unwrap_or_default(),
            rejected_above_or_equal_max_price_count: stats
                .rejected_above_or_equal_max_price_by_market
                .get(market_id)
                .copied()
                .unwrap_or_default(),
            entry_count: *entry_count,
            net_pnl: stats
                .net_pnl_by_market
                .get(market_id)
                .copied()
                .unwrap_or_default(),
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .entry_count
            .cmp(&left.entry_count)
            .then_with(|| right.signal_count.cmp(&left.signal_count))
            .then_with(|| {
                right
                    .rejected_capital_count
                    .cmp(&left.rejected_capital_count)
            })
            .then_with(|| left.market_id.cmp(&right.market_id))
    });
    rows.truncate(limit);
    rows
}

fn all_market_debug_rows(stats: &ReplayStats) -> Vec<MarketSignalDebugRow> {
    let mut market_ids = stats
        .signals_by_market
        .keys()
        .chain(stats.entries_by_market.keys())
        .chain(stats.rejected_capital_by_market.keys())
        .chain(stats.rejected_missing_price_by_market.keys())
        .chain(stats.rejected_below_min_price_by_market.keys())
        .chain(stats.rejected_above_or_equal_max_price_by_market.keys())
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    market_ids.sort();

    market_ids
        .into_iter()
        .map(|market_id| MarketSignalDebugRow {
            market_id: market_id.clone(),
            signal_count: stats
                .signals_by_market
                .get(&market_id)
                .copied()
                .unwrap_or_default(),
            rejected_capital_count: stats
                .rejected_capital_by_market
                .get(&market_id)
                .copied()
                .unwrap_or_default(),
            rejected_missing_price_count: stats
                .rejected_missing_price_by_market
                .get(&market_id)
                .copied()
                .unwrap_or_default(),
            rejected_below_min_price_count: stats
                .rejected_below_min_price_by_market
                .get(&market_id)
                .copied()
                .unwrap_or_default(),
            rejected_above_or_equal_max_price_count: stats
                .rejected_above_or_equal_max_price_by_market
                .get(&market_id)
                .copied()
                .unwrap_or_default(),
            entry_count: stats
                .entries_by_market
                .get(&market_id)
                .copied()
                .unwrap_or_default(),
            net_pnl: stats
                .net_pnl_by_market
                .get(&market_id)
                .copied()
                .unwrap_or_default(),
        })
        .collect()
}

fn close_position(
    symbol: &Symbol,
    spec: &ReplayMarketSpec,
    market_exit_price: Option<f64>,
    exit_cex_price: f64,
    settle_to_payout: bool,
    close_reason: &str,
    config: &ResolvedReplayConfig,
    planner: &ExecutionPlanner,
    positions: &mut HashMap<Symbol, ReplayPosition>,
    cash: &mut f64,
    stats: &mut ReplayStats,
    trade_log: &mut Vec<ReplayTradeRow>,
    ts_ms: u64,
) -> Result<()> {
    let Some(existing_position) = positions.get(symbol).cloned() else {
        return Ok(());
    };
    let close_plan = if settle_to_payout {
        None
    } else {
        let close_row = replay_close_row(spec, &existing_position, market_exit_price, ts_ms);
        match plan_replay_close_position(
            planner,
            config.planner_depth_levels,
            symbol,
            close_reason,
            &close_row,
            exit_cex_price,
            spec.market_config.cex_qty_step,
            positions,
        ) {
            Ok(plan) => Some(plan),
            Err(_) => return Ok(()),
        }
    };

    let Some(mut position) = positions.remove(symbol) else {
        return Ok(());
    };

    let used_exit_poly_price = if settle_to_payout {
        settlement_payout(spec, position.token_side, exit_cex_price)
    } else {
        close_plan
            .as_ref()
            .and_then(|plan| plan.poly_executable_price.0.to_f64())
            .or(market_exit_price)
            .unwrap_or(position.last_poly_price)
    };
    let sale_value = if settle_to_payout {
        position.shares * used_exit_poly_price
    } else {
        close_plan
            .as_ref()
            .map(|plan| {
                (plan.poly_planned_shares.0 * plan.poly_executable_price.0)
                    .to_f64()
                    .unwrap_or_default()
            })
            .unwrap_or_else(|| position.shares * used_exit_poly_price)
    };
    let poly_gross_pnl = sale_value - position.poly_entry_notional;
    *cash += sale_value;
    stats.poly_realized_gross += poly_gross_pnl;
    position.round_trip_pnl += poly_gross_pnl;
    let exit_poly_fee_paid = close_plan
        .as_ref()
        .and_then(|plan| plan.poly_fee_usd.0.to_f64())
        .unwrap_or_default();
    let exit_poly_slippage_paid = close_plan
        .as_ref()
        .and_then(|plan| plan.poly_friction_cost_usd.0.to_f64())
        .unwrap_or_default();
    let mut exit_fees_paid = exit_poly_fee_paid;
    let mut exit_slippage_paid = exit_poly_slippage_paid;

    if !settle_to_payout && sale_value > 0.0 {
        *cash -= exit_poly_fee_paid;
        position.round_trip_pnl -= exit_poly_fee_paid;
        record_costs(
            stats,
            exit_poly_fee_paid,
            exit_poly_slippage_paid,
            true,
            false,
            true,
        );
    }

    let used_exit_cex_price = if settle_to_payout {
        replay_slipped_price(
            exit_cex_price,
            if position.cex_qty_btc >= 0.0 {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            },
            config.stress.cex_slippage_bps,
            false,
        )
    } else {
        close_plan
            .as_ref()
            .and_then(|plan| plan.cex_executable_price.0.to_f64())
            .unwrap_or(exit_cex_price)
    };
    let cex_gross_pnl = linear_cex_pnl(
        position.cex_entry_price,
        used_exit_cex_price,
        position.cex_qty_btc,
    );
    *cash += cex_gross_pnl;
    stats.cex_realized_gross += cex_gross_pnl;
    position.round_trip_pnl += cex_gross_pnl;

    let exit_cex_notional = position.cex_qty_btc.abs() * used_exit_cex_price;
    if exit_cex_notional > 0.0 {
        let cex_fee_paid = close_plan
            .as_ref()
            .and_then(|plan| plan.cex_fee_usd.0.to_f64())
            .unwrap_or_else(|| exit_cex_notional * config.stress.cex_fee_bps / 10_000.0);
        let cex_slippage_paid = close_plan
            .as_ref()
            .and_then(|plan| plan.cex_friction_cost_usd.0.to_f64())
            .unwrap_or_else(|| {
                (position.cex_qty_btc.abs() * (used_exit_cex_price - exit_cex_price).abs()).max(0.0)
            });
        *cash -= cex_fee_paid;
        position.round_trip_pnl -= cex_fee_paid;
        record_costs(stats, cex_fee_paid, cex_slippage_paid, false, false, true);
        exit_fees_paid += cex_fee_paid;
        exit_slippage_paid += cex_slippage_paid;
    }

    let round_trip_gross_pnl = poly_gross_pnl + cex_gross_pnl;
    let total_fees_paid = position.entry_fees_paid + exit_fees_paid;
    let total_slippage_paid = position.entry_slippage_paid + exit_slippage_paid;
    let round_trip_net_pnl = round_trip_gross_pnl - total_fees_paid;

    trade_log.push(ReplayTradeRow {
        trade_id: trade_log.len() + 1,
        strategy: "arb".to_owned(),
        entry_time: position.entry_ts_ms / 1000,
        event_id: spec.event_id.clone(),
        market_id: spec.market_id.clone(),
        market_slug: spec.market_slug.clone(),
        exit_time: ts_ms / 1000,
        direction: format!("{}_{}", token_side_label(position.token_side), close_reason),
        entry_poly_price: position.entry_poly_price,
        exit_poly_price: used_exit_poly_price,
        entry_cex_price: position.cex_entry_price,
        exit_cex_price: used_exit_cex_price,
        poly_shares: position.shares,
        cex_qty: position.cex_qty_btc.abs(),
        gross_pnl: round_trip_gross_pnl,
        fees: total_fees_paid,
        slippage: total_slippage_paid,
        gas: 0.0,
        funding_cost: 0.0,
        net_pnl: round_trip_net_pnl,
        holding_bars: ts_ms.saturating_sub(position.entry_ts_ms) / ONE_MINUTE_MS,
        z_score_at_entry: position.entry_z_score,
        basis_bps_at_entry: position.entry_basis_bps,
        delta_at_entry: position.entry_delta,
    });

    stats.round_trip_count += 1;
    *stats
        .net_pnl_by_market
        .entry(position.market_id.clone())
        .or_insert(0.0) += round_trip_net_pnl;
    if round_trip_net_pnl > 0.0 {
        stats.winning_round_trips += 1;
    }
    if settle_to_payout {
        stats.settlement_count += 1;
    } else {
        stats.close_count += 1;
    }
    Ok(())
}

fn record_equity_point(
    ts_ms: u64,
    config: &ResolvedReplayConfig,
    positions: &HashMap<Symbol, ReplayPosition>,
    cash: f64,
    stats: &mut ReplayStats,
    peak_equity: &mut f64,
    equity_curve: &mut Vec<EquityPoint>,
) -> f64 {
    let (reserved_margin, capital_in_use) = portfolio_usage(positions);
    let mut unrealized_pnl = 0.0;
    let mut gross_exposure = 0.0;
    let mut market_value = 0.0;
    let mut cex_unrealized_total = 0.0;

    for position in positions.values() {
        let current_poly_value = position.shares * position.last_poly_price;
        let current_poly_unrealized = current_poly_value - position.poly_entry_notional;
        let current_cex_unrealized = linear_cex_pnl(
            position.cex_entry_price,
            position.last_cex_price,
            position.cex_qty_btc,
        );
        unrealized_pnl += current_poly_unrealized + current_cex_unrealized;
        market_value += current_poly_value;
        cex_unrealized_total += current_cex_unrealized;
        gross_exposure += current_poly_value + position.cex_qty_btc.abs() * position.last_cex_price;
    }

    stats.max_gross_exposure = stats.max_gross_exposure.max(gross_exposure);
    stats.max_reserved_margin = stats.max_reserved_margin.max(reserved_margin);
    stats.max_capital_in_use = stats.max_capital_in_use.max(capital_in_use);
    if config.initial_capital > 0.0 {
        stats.max_capital_utilization = stats
            .max_capital_utilization
            .max(capital_in_use / config.initial_capital);
    }

    let available_cash = cash - reserved_margin;
    let equity = cash + market_value + cex_unrealized_total;
    *peak_equity = peak_equity.max(equity);
    stats.max_drawdown = stats.max_drawdown.max(*peak_equity - equity);
    equity_curve.push(EquityPoint {
        ts_ms,
        equity,
        cash,
        available_cash,
        unrealized_pnl,
        gross_exposure,
        reserved_margin,
        capital_in_use,
        open_positions: positions.len(),
    });
    equity
}

fn portfolio_usage(positions: &HashMap<Symbol, ReplayPosition>) -> (f64, f64) {
    let reserved_margin = positions
        .values()
        .map(|position| position.reserved_margin)
        .sum();
    let capital_in_use = positions
        .values()
        .map(|position| position.poly_entry_notional + position.reserved_margin)
        .sum();
    (reserved_margin, capital_in_use)
}

fn parse_time_to_ms(raw: Option<&str>) -> Result<Option<u64>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let text = raw.trim();
    if text.is_empty() {
        return Ok(None);
    }
    if text.chars().all(|ch| ch.is_ascii_digit()) {
        let value = text
            .parse::<u64>()
            .with_context(|| format!("invalid millisecond timestamp `{text}`"))?;
        return Ok(Some(value));
    }

    if let Ok(date) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
        let dt = Utc.from_utc_datetime(
            &date
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| anyhow!("invalid date `{text}`"))?,
        );
        return Ok(Some(dt.timestamp_millis() as u64));
    }

    let normalized = if text.ends_with('Z') {
        format!("{}+00:00", &text[..text.len() - 1])
    } else if text.contains(' ') && !text.contains('T') {
        text.replacen(' ', "T", 1)
    } else {
        text.to_owned()
    };
    let dt = DateTime::parse_from_rfc3339(&normalized)
        .with_context(|| format!("failed to parse time `{text}`"))?
        .with_timezone(&Utc);
    Ok(Some(dt.timestamp_millis() as u64))
}

fn parse_market_rule(market_question: &str) -> Result<MarketRule> {
    let question = market_question
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase();
    let between_re = Regex::new(&format!(
        r"between\s+\${money}\s+and\s+\${money}",
        money = MONEY_LITERAL_RE
    ))?;
    let above_re = Regex::new(&format!(
        r"(above|greater than)\s+\${money}",
        money = MONEY_LITERAL_RE
    ))?;
    let below_re = Regex::new(&format!(
        r"(below|less than)\s+\${money}",
        money = MONEY_LITERAL_RE
    ))?;

    if let Some(captures) = between_re.captures(&question) {
        return Ok(MarketRule {
            kind: MarketRuleKind::Between,
            lower_strike: Some(Price(decimal_from_f64(parse_money(&captures[1])?)?)),
            upper_strike: Some(Price(decimal_from_f64(parse_money(&captures[2])?)?)),
        });
    }
    if let Some(captures) = above_re.captures(&question) {
        return Ok(MarketRule {
            kind: MarketRuleKind::Above,
            lower_strike: Some(Price(decimal_from_f64(parse_money(&captures[2])?)?)),
            upper_strike: None,
        });
    }
    if let Some(captures) = below_re.captures(&question) {
        return Ok(MarketRule {
            kind: MarketRuleKind::Below,
            lower_strike: None,
            upper_strike: Some(Price(decimal_from_f64(parse_money(&captures[2])?)?)),
        });
    }
    bail!("unsupported market question for causal replay: {market_question}")
}

fn parse_money(raw: &str) -> Result<f64> {
    let normalized = raw.replace(',', "").trim().to_lowercase();
    if normalized.is_empty() {
        bail!("invalid money literal `{raw}`");
    }

    let (number_text, multiplier) = match normalized.chars().last() {
        Some('k') => (&normalized[..normalized.len() - 1], 1_000.0),
        Some('m') => (&normalized[..normalized.len() - 1], 1_000_000.0),
        Some('b') => (&normalized[..normalized.len() - 1], 1_000_000_000.0),
        Some(ch) if ch.is_ascii_alphabetic() => {
            bail!("unsupported money suffix `{ch}` in `{raw}`");
        }
        Some(_) => (normalized.as_str(), 1.0),
        None => bail!("invalid money literal `{raw}`"),
    };
    // Polymarket 的 BTC 市场常见 $122K 这类写法，必须还原成真实 strike。
    number_text
        .trim()
        .parse::<f64>()
        .map(|value| value * multiplier)
        .with_context(|| format!("invalid money literal `{raw}`"))
}

fn parse_outcome_prices(raw: Option<&JsonValue>) -> Vec<f64> {
    parse_json_string_array(raw)
        .into_iter()
        .filter_map(|item| item.parse::<f64>().ok())
        .collect()
}

fn parse_json_string_array(raw: Option<&JsonValue>) -> Vec<String> {
    let Some(raw) = raw else {
        return Vec::new();
    };
    if let Some(values) = raw.as_array() {
        return values
            .iter()
            .filter_map(|value| {
                value
                    .as_str()
                    .map(ToOwned::to_owned)
                    .or_else(|| value.as_i64().map(|item| item.to_string()))
                    .or_else(|| value.as_f64().map(|item| item.to_string()))
            })
            .collect();
    }
    if let Some(text) = raw.as_str() {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Vec::new();
        }
        if let Ok(parsed) = serde_json::from_str::<JsonValue>(trimmed) {
            if let Some(values) = parsed.as_array() {
                return values
                    .iter()
                    .filter_map(|value| {
                        value
                            .as_str()
                            .map(ToOwned::to_owned)
                            .or_else(|| value.as_i64().map(|item| item.to_string()))
                            .or_else(|| value.as_f64().map(|item| item.to_string()))
                    })
                    .collect();
            }
        }
        return vec![trimmed.to_owned()];
    }
    Vec::new()
}

fn is_official_resolution(outcome_prices: &[f64], status: &str) -> bool {
    let normalized_status = status.trim().to_lowercase();
    if matches!(
        normalized_status.as_str(),
        "resolved" | "finalized" | "settled"
    ) {
        return true;
    }
    if outcome_prices.len() < 2 {
        return false;
    }
    if (outcome_prices[0] + outcome_prices[1] - 1.0).abs() > RESOLUTION_EPSILON {
        return false;
    }
    outcome_prices[..2]
        .iter()
        .all(|price| price.abs() <= RESOLUTION_EPSILON || (price - 1.0).abs() <= RESOLUTION_EPSILON)
}

fn clamp_probability_value(value: f64) -> f64 {
    value.clamp(0.0, 1.0)
}

fn settlement_payout(spec: &ReplayMarketSpec, token_side: TokenSide, terminal_price: f64) -> f64 {
    match token_side {
        TokenSide::Yes => spec.yes_payout.unwrap_or_else(|| {
            realized_yes_payout(
                spec.market_config.resolved_market_rule().as_ref(),
                terminal_price,
            )
        }),
        TokenSide::No => spec.no_payout.unwrap_or_else(|| {
            1.0 - realized_yes_payout(
                spec.market_config.resolved_market_rule().as_ref(),
                terminal_price,
            )
        }),
    }
}

fn realized_yes_payout(rule: Option<&MarketRule>, terminal_price: f64) -> f64 {
    let Some(rule) = rule else {
        return 0.0;
    };
    match rule.kind {
        MarketRuleKind::Above => {
            if terminal_price > rule.lower_strike.map(Price::to_f64).unwrap_or_default() {
                1.0
            } else {
                0.0
            }
        }
        MarketRuleKind::Below => {
            if terminal_price < rule.upper_strike.map(Price::to_f64).unwrap_or_default() {
                1.0
            } else {
                0.0
            }
        }
        MarketRuleKind::Between => {
            let lower = rule.lower_strike.map(Price::to_f64).unwrap_or_default();
            let upper = rule.upper_strike.map(Price::to_f64).unwrap_or_default();
            if terminal_price >= lower && terminal_price < upper {
                1.0
            } else {
                0.0
            }
        }
    }
}

fn poly_trade_event(
    symbol: &Symbol,
    instrument: InstrumentKind,
    price: f64,
    ts_ms: u64,
) -> MarketDataEvent {
    MarketDataEvent::TradeUpdate {
        exchange: Exchange::Polymarket,
        symbol: symbol.clone(),
        instrument,
        price: Price(decimal_from_f64_lossy(price)),
        quantity: polyalpha_core::VenueQuantity::PolyShares(polyalpha_core::PolyShares::ZERO),
        side: OrderSide::Buy,
        timestamp_ms: ts_ms,
    }
}

fn cex_trade_event(symbol: &Symbol, price: f64, ts_ms: u64) -> MarketDataEvent {
    MarketDataEvent::TradeUpdate {
        exchange: Exchange::Binance,
        symbol: symbol.clone(),
        instrument: InstrumentKind::CexPerp,
        price: Price(decimal_from_f64_lossy(price)),
        quantity: polyalpha_core::VenueQuantity::CexBaseQty(polyalpha_core::CexBaseQty::default()),
        side: OrderSide::Buy,
        timestamp_ms: ts_ms,
    }
}

fn record_costs(
    stats: &mut ReplayStats,
    fee_paid: f64,
    slippage_paid: f64,
    is_poly: bool,
    is_entry: bool,
    should_count_trade: bool,
) {
    stats.total_fees_paid += fee_paid;
    stats.total_slippage_paid += slippage_paid;
    if should_count_trade {
        stats.trade_count += 1;
    }
    if is_poly {
        stats.poly_fees_paid += fee_paid;
        stats.poly_slippage_paid += slippage_paid;
        if should_count_trade {
            stats.poly_trade_count += 1;
        }
    } else {
        stats.cex_fees_paid += fee_paid;
        stats.cex_slippage_paid += slippage_paid;
        if should_count_trade {
            stats.cex_trade_count += 1;
        }
    }
    let _ = is_entry;
}

fn linear_cex_pnl(entry_price: f64, exit_price: f64, qty_btc: f64) -> f64 {
    if entry_price <= 0.0 || qty_btc.abs() <= f64::EPSILON {
        0.0
    } else {
        qty_btc * (exit_price - entry_price)
    }
}

fn ratio(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ReplayPricePoint {
    yes_price: Option<f64>,
    no_price: Option<f64>,
}

#[derive(Clone, Copy, Debug, Default)]
struct ReplayPositionComponent {
    poly_value: f64,
    cex_unrealized: f64,
}

#[derive(Clone, Debug, Default)]
struct ReplayMinuteAttributionAccumulator {
    fill_cash_change_usd: f64,
    poly_mark_change_usd: f64,
    cex_mark_change_usd: f64,
    open_positions_before: usize,
    open_positions_after: usize,
    entries_at_end_ts: usize,
    exits_at_end_ts: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReplaySyncResetFlavor {
    NeutralPlateau,
    InteriorReset,
}

fn build_market_price_series(
    dataset: &ReplayDataset,
) -> HashMap<String, Vec<(u64, ReplayPricePoint)>> {
    build_market_price_series_from_rows(&dataset.rows)
}

fn build_market_price_series_from_rows(
    rows: &[ReplayMinuteRow],
) -> HashMap<String, Vec<(u64, ReplayPricePoint)>> {
    let mut by_market: HashMap<String, Vec<(u64, ReplayPricePoint)>> = HashMap::new();
    for row in rows {
        by_market.entry(row.market_id.clone()).or_default().push((
            row.ts_ms,
            ReplayPricePoint {
                yes_price: row.yes_price,
                no_price: row.no_price,
            },
        ));
    }
    by_market
}

fn sanitize_sync_neutral_resets(
    markets: &HashMap<String, ReplayMarketSpec>,
    rows: Vec<ReplayMinuteRow>,
) -> ReplaySanitizationOutcome {
    if rows.is_empty() || markets.is_empty() {
        return ReplaySanitizationOutcome {
            rows,
            diagnostics: ReplaySanitizationDiagnostics::default(),
        };
    }

    let price_series = build_market_price_series_from_rows(&rows);
    let mut event_market_ids: HashMap<String, Vec<String>> = HashMap::new();
    for (market_id, spec) in markets {
        event_market_ids
            .entry(spec.event_id.clone())
            .or_default()
            .push(market_id.clone());
    }

    let mut neutral_triggers: HashMap<(String, u64), HashSet<String>> = HashMap::new();
    let mut interior_triggers: HashMap<(String, u64), HashSet<String>> = HashMap::new();
    let mut pair_triggers: HashMap<u64, HashSet<String>> = HashMap::new();
    for (market_id, points) in &price_series {
        let Some(spec) = markets.get(market_id) else {
            continue;
        };
        for window in points.windows(3) {
            let before = window[0].1;
            let after = window[1].1;
            let next = window[2].1;
            match sync_reset_flavor(before, after, next) {
                Some(ReplaySyncResetFlavor::NeutralPlateau) => {
                    neutral_triggers
                        .entry((spec.event_id.clone(), window[1].0))
                        .or_default()
                        .insert(market_id.clone());
                    pair_triggers
                        .entry(window[1].0)
                        .or_default()
                        .insert(market_id.clone());
                }
                Some(ReplaySyncResetFlavor::InteriorReset) => {
                    interior_triggers
                        .entry((spec.event_id.clone(), window[1].0))
                        .or_default()
                        .insert(market_id.clone());
                    pair_triggers
                        .entry(window[1].0)
                        .or_default()
                        .insert(market_id.clone());
                }
                None => {}
            }
        }
    }

    let mut trigger_entries = neutral_triggers.into_iter().collect::<Vec<_>>();
    trigger_entries.sort_by(|left, right| {
        left.0
             .0
            .cmp(&right.0 .0)
            .then_with(|| left.0 .1.cmp(&right.0 .1))
    });

    let mut sanitized_cells: HashSet<(String, u64)> = HashSet::new();
    let mut sync_reset_windows = Vec::new();
    for ((event_id, start_ts_ms), trigger_market_ids) in trigger_entries {
        if trigger_market_ids.len() < REPLAY_SYNC_RESET_MIN_MARKETS {
            continue;
        }
        let Some(event_markets) = event_market_ids.get(&event_id) else {
            continue;
        };
        let mut ts_ms = start_ts_ms;
        let mut end_ts_ms = None;
        let mut sanitized_market_ids = HashSet::new();

        loop {
            let neutral_market_ids = event_markets
                .iter()
                .filter(|market_id| {
                    price_point_at(&price_series, market_id, ts_ms).is_some_and(|point| {
                        is_neutral_probability(point.yes_price)
                            && is_neutral_probability(point.no_price)
                    })
                })
                .cloned()
                .collect::<Vec<_>>();
            if neutral_market_ids.len() < REPLAY_SYNC_RESET_MIN_MARKETS {
                break;
            }
            for market_id in neutral_market_ids {
                sanitized_market_ids.insert(market_id.clone());
                sanitized_cells.insert((market_id, ts_ms));
            }
            end_ts_ms = Some(ts_ms);
            let Some(next_ts_ms) = ts_ms.checked_add(ONE_MINUTE_MS) else {
                break;
            };
            ts_ms = next_ts_ms;
        }

        if let Some(end_ts_ms) = end_ts_ms {
            sync_reset_windows.push(ReplaySanitizedSyncResetWindow {
                event_id,
                start_ts_ms,
                end_ts_ms,
                trigger_market_count: trigger_market_ids.len(),
                sanitized_market_count: sanitized_market_ids.len(),
            });
        }
    }

    let mut interior_entries = interior_triggers.into_iter().collect::<Vec<_>>();
    interior_entries.sort_by(|left, right| {
        left.0
             .0
            .cmp(&right.0 .0)
            .then_with(|| left.0 .1.cmp(&right.0 .1))
    });
    for ((event_id, start_ts_ms), trigger_market_ids) in interior_entries {
        let Some(event_markets) = event_market_ids.get(&event_id) else {
            continue;
        };
        let start_interior_market_count = event_markets
            .iter()
            .filter(|market_id| {
                price_point_at(&price_series, market_id, start_ts_ms).is_some_and(|point| {
                    is_interior_sync_reset_probability(point.yes_price)
                        && is_interior_sync_reset_probability(point.no_price)
                })
            })
            .count();
        let meets_trigger_threshold = trigger_market_ids.len() >= REPLAY_SYNC_RESET_MIN_MARKETS;
        let meets_adjacent_template_threshold = trigger_market_ids.len()
            >= REPLAY_SYNC_RESET_INTERIOR_MIN_TRIGGER_MARKETS
            && start_interior_market_count >= REPLAY_SYNC_RESET_INTERIOR_MIN_SANITIZED_MARKETS;
        if !meets_trigger_threshold && !meets_adjacent_template_threshold {
            continue;
        }
        let mut ts_ms = start_ts_ms;
        let mut end_ts_ms = None;
        let mut sanitized_market_ids = HashSet::new();

        loop {
            let interior_market_ids = event_markets
                .iter()
                .filter(|market_id| {
                    price_point_at(&price_series, market_id, ts_ms).is_some_and(|point| {
                        is_interior_sync_reset_probability(point.yes_price)
                            && is_interior_sync_reset_probability(point.no_price)
                    })
                })
                .cloned()
                .collect::<Vec<_>>();
            if interior_market_ids.len() < REPLAY_SYNC_RESET_MIN_MARKETS {
                break;
            }
            for market_id in interior_market_ids {
                sanitized_market_ids.insert(market_id.clone());
                sanitized_cells.insert((market_id, ts_ms));
            }
            end_ts_ms = Some(ts_ms);
            let Some(next_ts_ms) = ts_ms.checked_add(ONE_MINUTE_MS) else {
                break;
            };
            ts_ms = next_ts_ms;
        }

        if let Some(end_ts_ms) = end_ts_ms {
            sync_reset_windows.push(ReplaySanitizedSyncResetWindow {
                event_id,
                start_ts_ms,
                end_ts_ms,
                trigger_market_count: trigger_market_ids.len(),
                sanitized_market_count: sanitized_market_ids.len(),
            });
        }
    }

    let mut pair_entries = pair_triggers.into_iter().collect::<Vec<_>>();
    pair_entries.sort_by_key(|(start_ts_ms, _)| *start_ts_ms);
    for (start_ts_ms, trigger_market_ids) in pair_entries {
        let Some(next_ts_ms) = start_ts_ms.checked_add(ONE_MINUTE_MS) else {
            continue;
        };
        let mut pair_market_ids = HashSet::new();
        let candidates = trigger_market_ids.into_iter().collect::<Vec<_>>();
        for (left_idx, left_market_id) in candidates.iter().enumerate() {
            for right_market_id in candidates.iter().skip(left_idx + 1) {
                let Some(left_spec) = markets.get(left_market_id) else {
                    continue;
                };
                let Some(right_spec) = markets.get(right_market_id) else {
                    continue;
                };
                let Some(left_before) =
                    price_point_at(&price_series, left_market_id, start_ts_ms - ONE_MINUTE_MS)
                else {
                    continue;
                };
                let Some(left_after) = price_point_at(&price_series, left_market_id, start_ts_ms)
                else {
                    continue;
                };
                let Some(left_next) = price_point_at(&price_series, left_market_id, next_ts_ms)
                else {
                    continue;
                };
                let Some(right_before) =
                    price_point_at(&price_series, right_market_id, start_ts_ms - ONE_MINUTE_MS)
                else {
                    continue;
                };
                let Some(right_after) = price_point_at(&price_series, right_market_id, start_ts_ms)
                else {
                    continue;
                };
                let Some(right_next) = price_point_at(&price_series, right_market_id, next_ts_ms)
                else {
                    continue;
                };
                if !is_pair_sync_reset_candidate(
                    left_spec,
                    right_spec,
                    left_before,
                    left_after,
                    left_next,
                    right_before,
                    right_after,
                    right_next,
                ) {
                    continue;
                }
                pair_market_ids.insert(left_market_id.clone());
                pair_market_ids.insert(right_market_id.clone());
            }
        }

        pair_market_ids
            .retain(|market_id| !sanitized_cells.contains(&(market_id.clone(), start_ts_ms)));
        if pair_market_ids.len() < 2 {
            continue;
        }
        for market_id in &pair_market_ids {
            sanitized_cells.insert((market_id.clone(), start_ts_ms));
        }
        sync_reset_windows.push(ReplaySanitizedSyncResetWindow {
            event_id: pair_sync_reset_scope(markets, &pair_market_ids),
            start_ts_ms,
            end_ts_ms: start_ts_ms,
            trigger_market_count: pair_market_ids.len(),
            sanitized_market_count: pair_market_ids.len(),
        });
    }

    let mut diagnostics = ReplaySanitizationDiagnostics {
        sanitized_market_minutes: 0,
        sanitized_poly_observations: 0,
        sync_reset_windows,
    };
    let rows = rows
        .into_iter()
        .map(|mut row| {
            if sanitized_cells.contains(&(row.market_id.clone(), row.ts_ms)) {
                diagnostics.sanitized_market_minutes += 1;
                diagnostics.sanitized_poly_observations += usize::from(row.yes_price.is_some());
                diagnostics.sanitized_poly_observations += usize::from(row.no_price.is_some());
                row.yes_price = None;
                row.no_price = None;
            }
            row
        })
        .collect::<Vec<_>>();

    ReplaySanitizationOutcome { rows, diagnostics }
}

fn price_point_at(
    price_series: &HashMap<String, Vec<(u64, ReplayPricePoint)>>,
    market_id: &str,
    ts_ms: u64,
) -> Option<ReplayPricePoint> {
    let points = price_series.get(market_id)?;
    let idx = points
        .binary_search_by_key(&ts_ms, |(point_ts_ms, _)| *point_ts_ms)
        .ok()?;
    Some(points[idx].1)
}

fn replay_reference_cex_price_at(dataset: &ReplayDataset, ts_ms: u64) -> Option<f64> {
    ts_ms
        .checked_sub(ONE_MINUTE_MS)
        .and_then(|prior_ts_ms| dataset.cex_close_by_ts.get(&prior_ts_ms).copied())
}

fn trade_token_side(trade: &ReplayTradeRow) -> TokenSide {
    if trade.direction.starts_with("yes_") {
        TokenSide::Yes
    } else {
        TokenSide::No
    }
}

fn signed_cex_qty_from_trade(trade: &ReplayTradeRow) -> f64 {
    if trade.delta_at_entry >= 0.0 {
        -trade.cex_qty
    } else {
        trade.cex_qty
    }
}

fn replay_trade_component_at(
    dataset: &ReplayDataset,
    price_series: &HashMap<String, Vec<(u64, ReplayPricePoint)>>,
    trade: &ReplayTradeRow,
    ts_ms: u64,
) -> Option<ReplayPositionComponent> {
    let ts = ts_ms / 1000;
    if !(trade.entry_time <= ts && trade.exit_time > ts) {
        return None;
    }
    if trade.entry_time == ts {
        return Some(ReplayPositionComponent {
            poly_value: trade.poly_shares * trade.entry_poly_price,
            cex_unrealized: 0.0,
        });
    }

    let prices = price_point_at(price_series, &trade.market_id, ts_ms)?;
    let poly_price = match trade_token_side(trade) {
        TokenSide::Yes => prices.yes_price?,
        TokenSide::No => prices.no_price?,
    };
    let cex_price = replay_reference_cex_price_at(dataset, ts_ms)?;
    Some(ReplayPositionComponent {
        poly_value: trade.poly_shares * poly_price,
        cex_unrealized: linear_cex_pnl(
            trade.entry_cex_price,
            cex_price,
            signed_cex_qty_from_trade(trade),
        ),
    })
}

fn replay_entry_fee_usd(trade: &ReplayTradeRow, config: &ResolvedReplayConfig) -> f64 {
    trade.poly_shares * trade.entry_poly_price * config.stress.poly_fee_bps / 10_000.0
        + trade.cex_qty * trade.entry_cex_price * config.stress.cex_fee_bps / 10_000.0
}

fn replay_exit_fee_usd(trade: &ReplayTradeRow, config: &ResolvedReplayConfig) -> f64 {
    trade.poly_shares * trade.exit_poly_price * config.stress.poly_fee_bps / 10_000.0
        + trade.cex_qty * trade.exit_cex_price * config.stress.cex_fee_bps / 10_000.0
}

fn build_minute_attributions(
    dataset: &ReplayDataset,
    config: &ResolvedReplayConfig,
    price_series: &HashMap<String, Vec<(u64, ReplayPricePoint)>>,
    trade_log: &[ReplayTradeRow],
    start_ts_ms: u64,
    end_ts_ms: u64,
) -> Vec<ReplayMinuteAttribution> {
    let start_ts = start_ts_ms / 1000;
    let end_ts = end_ts_ms / 1000;
    let mut by_market: HashMap<String, ReplayMinuteAttributionAccumulator> = HashMap::new();

    for trade in trade_log
        .iter()
        .filter(|trade| trade.entry_time <= end_ts && trade.exit_time > start_ts)
    {
        let accumulator = by_market.entry(trade.market_id.clone()).or_default();
        let open_before = trade.entry_time <= start_ts && trade.exit_time > start_ts;
        let open_after = trade.entry_time <= end_ts && trade.exit_time > end_ts;
        if open_before {
            accumulator.open_positions_before += 1;
        }
        if open_after {
            accumulator.open_positions_after += 1;
        }

        let before = replay_trade_component_at(dataset, price_series, trade, start_ts_ms);
        let after = replay_trade_component_at(dataset, price_series, trade, end_ts_ms);
        accumulator.poly_mark_change_usd += after.map(|item| item.poly_value).unwrap_or_default()
            - before.map(|item| item.poly_value).unwrap_or_default();
        accumulator.cex_mark_change_usd +=
            after.map(|item| item.cex_unrealized).unwrap_or_default()
                - before.map(|item| item.cex_unrealized).unwrap_or_default();

        if trade.entry_time == end_ts {
            accumulator.entries_at_end_ts += 1;
            accumulator.fill_cash_change_usd -=
                trade.poly_shares * trade.entry_poly_price + replay_entry_fee_usd(trade, config);
        }
        if trade.exit_time == end_ts {
            accumulator.exits_at_end_ts += 1;
            accumulator.fill_cash_change_usd += trade.poly_shares * trade.exit_poly_price
                + linear_cex_pnl(
                    trade.entry_cex_price,
                    trade.exit_cex_price,
                    signed_cex_qty_from_trade(trade),
                )
                - replay_exit_fee_usd(trade, config);
        }
    }

    let mut rows = by_market
        .into_iter()
        .filter_map(|(market_id, accumulator)| {
            let spec = dataset.markets.get(&market_id)?;
            let start_prices = price_point_at(price_series, &market_id, start_ts_ms);
            let end_prices = price_point_at(price_series, &market_id, end_ts_ms);
            Some(ReplayMinuteAttribution {
                event_id: spec.event_id.clone(),
                market_id: market_id.clone(),
                market_slug: spec.market_slug.clone(),
                market_question: spec.market_config.market_question.clone(),
                total_contribution_usd: accumulator.fill_cash_change_usd
                    + accumulator.poly_mark_change_usd
                    + accumulator.cex_mark_change_usd,
                fill_cash_change_usd: accumulator.fill_cash_change_usd,
                poly_mark_change_usd: accumulator.poly_mark_change_usd,
                cex_mark_change_usd: accumulator.cex_mark_change_usd,
                open_positions_before: accumulator.open_positions_before,
                open_positions_after: accumulator.open_positions_after,
                entries_at_end_ts: accumulator.entries_at_end_ts,
                exits_at_end_ts: accumulator.exits_at_end_ts,
                yes_price_start: start_prices.and_then(|item| item.yes_price),
                yes_price_end: end_prices.and_then(|item| item.yes_price),
                no_price_start: start_prices.and_then(|item| item.no_price),
                no_price_end: end_prices.and_then(|item| item.no_price),
                cex_price_start: replay_reference_cex_price_at(dataset, start_ts_ms),
                cex_price_end: replay_reference_cex_price_at(dataset, end_ts_ms),
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .total_contribution_usd
            .abs()
            .partial_cmp(&left.total_contribution_usd.abs())
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.market_id.cmp(&right.market_id))
    });
    rows
}

fn is_extreme_probability(value: Option<f64>) -> bool {
    value.is_some_and(|item| {
        item <= REPLAY_SYNC_RESET_EXTREME_THRESHOLD
            || item >= 1.0 - REPLAY_SYNC_RESET_EXTREME_THRESHOLD
    })
}

fn is_neutral_probability(value: Option<f64>) -> bool {
    value.is_some_and(|item| {
        (item - REPLAY_SYNC_RESET_NEUTRAL_TARGET).abs() <= REPLAY_SYNC_RESET_NEUTRAL_TOLERANCE
    })
}

fn is_interior_sync_reset_probability(value: Option<f64>) -> bool {
    value.is_some_and(|item| {
        (REPLAY_SYNC_RESET_INTERIOR_MIN..=REPLAY_SYNC_RESET_INTERIOR_MAX).contains(&item)
    })
}

fn approx_same_probability(lhs: Option<f64>, rhs: Option<f64>) -> bool {
    lhs.zip(rhs)
        .map(|(lhs, rhs)| (lhs - rhs).abs() <= REPLAY_SYNC_RESET_SNAPBACK_TOLERANCE)
        .unwrap_or(false)
}

fn approx_same_probability_with_tolerance(
    lhs: Option<f64>,
    rhs: Option<f64>,
    tolerance: f64,
) -> bool {
    lhs.zip(rhs)
        .map(|(lhs, rhs)| (lhs - rhs).abs() <= tolerance)
        .unwrap_or(false)
}

fn approx_probability_sum(lhs: Option<f64>, rhs: Option<f64>, target: f64, tolerance: f64) -> bool {
    lhs.zip(rhs)
        .map(|(lhs, rhs)| ((lhs + rhs) - target).abs() <= tolerance)
        .unwrap_or(false)
}

fn same_strike(lhs: Option<Price>, rhs: Option<Price>) -> bool {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => lhs == rhs,
        _ => false,
    }
}

fn is_adjacent_ladder_pair(lhs: &MarketRule, rhs: &MarketRule) -> bool {
    match (&lhs.kind, &rhs.kind) {
        (MarketRuleKind::Below, MarketRuleKind::Between) => {
            same_strike(lhs.upper_strike, rhs.lower_strike)
        }
        (MarketRuleKind::Between, MarketRuleKind::Below) => {
            same_strike(lhs.lower_strike, rhs.upper_strike)
        }
        (MarketRuleKind::Between, MarketRuleKind::Between) => {
            same_strike(lhs.upper_strike, rhs.lower_strike)
                || same_strike(lhs.lower_strike, rhs.upper_strike)
        }
        (MarketRuleKind::Between, MarketRuleKind::Above) => {
            same_strike(lhs.upper_strike, rhs.lower_strike)
        }
        (MarketRuleKind::Above, MarketRuleKind::Between) => {
            same_strike(lhs.lower_strike, rhs.upper_strike)
        }
        _ => false,
    }
}

fn is_complementary_threshold_pair(lhs: &MarketRule, rhs: &MarketRule) -> bool {
    match (&lhs.kind, &rhs.kind) {
        (MarketRuleKind::Above, MarketRuleKind::Below) => {
            same_strike(lhs.lower_strike, rhs.upper_strike)
        }
        (MarketRuleKind::Below, MarketRuleKind::Above) => {
            same_strike(lhs.upper_strike, rhs.lower_strike)
        }
        _ => false,
    }
}

fn pair_sync_reset_scope(
    markets: &HashMap<String, ReplayMarketSpec>,
    market_ids: &HashSet<String>,
) -> String {
    let mut event_ids = market_ids
        .iter()
        .filter_map(|market_id| markets.get(market_id).map(|spec| spec.event_id.clone()))
        .collect::<Vec<_>>();
    event_ids.sort();
    event_ids.dedup();
    if event_ids.is_empty() {
        "pair-sync-reset".to_owned()
    } else {
        event_ids.join("|")
    }
}

fn is_pair_sync_reset_candidate(
    lhs_spec: &ReplayMarketSpec,
    rhs_spec: &ReplayMarketSpec,
    lhs_before: ReplayPricePoint,
    lhs_after: ReplayPricePoint,
    lhs_next: ReplayPricePoint,
    rhs_before: ReplayPricePoint,
    rhs_after: ReplayPricePoint,
    rhs_next: ReplayPricePoint,
) -> bool {
    if lhs_spec
        .settlement_ts_ms
        .abs_diff(rhs_spec.settlement_ts_ms)
        > REPLAY_SYNC_RESET_PAIR_SETTLEMENT_TOLERANCE_MS
    {
        return false;
    }
    let Some(lhs_rule) = lhs_spec.market_config.market_rule.as_ref() else {
        return false;
    };
    let Some(rhs_rule) = rhs_spec.market_config.market_rule.as_ref() else {
        return false;
    };
    let lhs_extreme =
        is_extreme_probability(lhs_before.yes_price) || is_extreme_probability(lhs_before.no_price);
    let rhs_extreme =
        is_extreme_probability(rhs_before.yes_price) || is_extreme_probability(rhs_before.no_price);
    let lhs_next_extreme =
        is_extreme_probability(lhs_next.yes_price) || is_extreme_probability(lhs_next.no_price);
    let rhs_next_extreme =
        is_extreme_probability(rhs_next.yes_price) || is_extreme_probability(rhs_next.no_price);
    if !lhs_extreme || !rhs_extreme || !lhs_next_extreme || !rhs_next_extreme {
        return false;
    }

    if is_adjacent_ladder_pair(lhs_rule, rhs_rule) {
        return approx_same_probability_with_tolerance(
            lhs_before.yes_price,
            rhs_before.yes_price,
            REPLAY_SYNC_RESET_PAIR_TEMPLATE_TOLERANCE,
        ) && approx_same_probability_with_tolerance(
            lhs_after.yes_price,
            rhs_after.yes_price,
            REPLAY_SYNC_RESET_PAIR_TEMPLATE_TOLERANCE,
        ) && approx_same_probability_with_tolerance(
            lhs_next.yes_price,
            rhs_next.yes_price,
            REPLAY_SYNC_RESET_PAIR_TEMPLATE_TOLERANCE,
        );
    }

    if is_complementary_threshold_pair(lhs_rule, rhs_rule) {
        return approx_probability_sum(
            lhs_before.yes_price,
            rhs_before.yes_price,
            1.0,
            REPLAY_SYNC_RESET_PAIR_COMPLEMENT_TOLERANCE,
        ) && approx_probability_sum(
            lhs_after.yes_price,
            rhs_after.yes_price,
            1.0,
            REPLAY_SYNC_RESET_PAIR_COMPLEMENT_TOLERANCE,
        ) && approx_probability_sum(
            lhs_next.yes_price,
            rhs_next.yes_price,
            1.0,
            REPLAY_SYNC_RESET_PAIR_COMPLEMENT_TOLERANCE,
        );
    }

    false
}

fn sync_reset_flavor(
    before: ReplayPricePoint,
    after: ReplayPricePoint,
    next: ReplayPricePoint,
) -> Option<ReplaySyncResetFlavor> {
    let moved_into_neutral = before
        .yes_price
        .zip(after.yes_price)
        .map(|(lhs, rhs)| (lhs - rhs).abs() >= REPLAY_SYNC_RESET_MIN_MOVE)
        .unwrap_or(false)
        || before
            .no_price
            .zip(after.no_price)
            .map(|(lhs, rhs)| (lhs - rhs).abs() >= REPLAY_SYNC_RESET_MIN_MOVE)
            .unwrap_or(false);
    let moved_out_of_neutral = after
        .yes_price
        .zip(next.yes_price)
        .map(|(lhs, rhs)| (lhs - rhs).abs() >= REPLAY_SYNC_RESET_MIN_MOVE)
        .unwrap_or(false)
        || after
            .no_price
            .zip(next.no_price)
            .map(|(lhs, rhs)| (lhs - rhs).abs() >= REPLAY_SYNC_RESET_MIN_MOVE)
            .unwrap_or(false);
    let next_is_neutral =
        is_neutral_probability(next.yes_price) && is_neutral_probability(next.no_price);
    let next_snaps_back_to_extreme = moved_out_of_neutral
        && (is_extreme_probability(next.yes_price) || is_extreme_probability(next.no_price));
    if moved_into_neutral
        && (is_extreme_probability(before.yes_price) || is_extreme_probability(before.no_price))
        && is_neutral_probability(after.yes_price)
        && is_neutral_probability(after.no_price)
        && (next_is_neutral || next_snaps_back_to_extreme)
    {
        return Some(ReplaySyncResetFlavor::NeutralPlateau);
    }

    let moved_into_interior = before
        .yes_price
        .zip(after.yes_price)
        .map(|(lhs, rhs)| (lhs - rhs).abs() >= REPLAY_SYNC_RESET_MIN_MOVE)
        .unwrap_or(false)
        || before
            .no_price
            .zip(after.no_price)
            .map(|(lhs, rhs)| (lhs - rhs).abs() >= REPLAY_SYNC_RESET_MIN_MOVE)
            .unwrap_or(false);
    let snapped_back_to_before = (approx_same_probability(before.yes_price, next.yes_price)
        && approx_same_probability(before.no_price, next.no_price))
        || approx_same_probability(before.yes_price, next.yes_price)
        || approx_same_probability(before.no_price, next.no_price);
    let next_is_interior = is_interior_sync_reset_probability(next.yes_price)
        && is_interior_sync_reset_probability(next.no_price);
    if moved_into_interior
        && is_interior_sync_reset_probability(after.yes_price)
        && is_interior_sync_reset_probability(after.no_price)
        && (next_is_interior || snapped_back_to_before)
    {
        return Some(ReplaySyncResetFlavor::InteriorReset);
    }

    None
}

fn is_sync_probability_reset(
    before: ReplayPricePoint,
    after: ReplayPricePoint,
    next: ReplayPricePoint,
) -> bool {
    sync_reset_flavor(before, after, next).is_some()
}

fn detect_sync_reset_events(
    dataset: &ReplayDataset,
    price_series: &HashMap<String, Vec<(u64, ReplayPricePoint)>>,
    attributions: &[ReplayMinuteAttribution],
    start_ts_ms: u64,
    end_ts_ms: u64,
) -> Vec<ReplayAnomalousEvent> {
    let Some(next_ts_ms) = end_ts_ms.checked_add(ONE_MINUTE_MS) else {
        return Vec::new();
    };
    let impacted_market_ids = attributions
        .iter()
        .filter(|item| item.total_contribution_usd.abs() > 1.0)
        .map(|item| item.market_id.as_str())
        .collect::<HashSet<_>>();
    let mut counts: HashMap<String, usize> = HashMap::new();

    for market_id in impacted_market_ids {
        let Some(before) = price_point_at(price_series, market_id, start_ts_ms) else {
            continue;
        };
        let Some(after) = price_point_at(price_series, market_id, end_ts_ms) else {
            continue;
        };
        let Some(next) = price_point_at(price_series, market_id, next_ts_ms) else {
            continue;
        };
        if !is_sync_probability_reset(before, after, next) {
            continue;
        }
        let Some(spec) = dataset.markets.get(market_id) else {
            continue;
        };
        *counts.entry(spec.event_id.clone()).or_insert(0) += 1;
    }

    let mut events = counts
        .into_iter()
        .filter(|(_, market_count)| *market_count >= REPLAY_SYNC_RESET_MIN_MARKETS)
        .map(|(event_id, market_count)| ReplayAnomalousEvent {
            event_id,
            market_count,
        })
        .collect::<Vec<_>>();
    events.sort_by(|left, right| {
        right
            .market_count
            .cmp(&left.market_count)
            .then_with(|| left.event_id.cmp(&right.event_id))
    });
    events
}

fn detect_replay_anomalies(
    dataset: &ReplayDataset,
    config: &ResolvedReplayConfig,
    equity_curve: &[EquityPoint],
    trade_log: &[ReplayTradeRow],
) -> ReplayAnomalyDiagnostics {
    if equity_curve.len() < 2 {
        return ReplayAnomalyDiagnostics::default();
    }

    let price_series = build_market_price_series(dataset);
    let mut anomalies = Vec::new();

    for window in equity_curve.windows(2) {
        let start = &window[0];
        let end = &window[1];
        let equity_change_usd = end.equity - start.equity;
        if equity_change_usd.abs() < REPLAY_ANOMALY_MIN_ABS_EQUITY_JUMP_USD {
            continue;
        }
        let cash_change_usd = end.cash - start.cash;
        let formula_mark_change_usd = equity_change_usd - cash_change_usd;
        let mark_share = if equity_change_usd.abs() <= f64::EPSILON {
            0.0
        } else {
            formula_mark_change_usd.abs() / equity_change_usd.abs()
        };
        if mark_share < REPLAY_ANOMALY_MIN_MARK_SHARE {
            continue;
        }

        let attributions = build_minute_attributions(
            dataset,
            config,
            &price_series,
            trade_log,
            start.ts_ms,
            end.ts_ms,
        );
        let fill_cash_change_usd = attributions
            .iter()
            .map(|item| item.fill_cash_change_usd)
            .sum::<f64>();
        let detailed_mark_change_usd = attributions
            .iter()
            .map(|item| item.poly_mark_change_usd + item.cex_mark_change_usd)
            .sum::<f64>();
        let sync_reset_events = detect_sync_reset_events(
            dataset,
            &price_series,
            &attributions,
            start.ts_ms,
            end.ts_ms,
        );
        let mut reason_codes = vec!["mark_dominated_jump".to_owned()];
        if !sync_reset_events.is_empty() {
            reason_codes.push("sync_probability_reset".to_owned());
        }
        anomalies.push(ReplayAnomaly {
            start_ts_ms: start.ts_ms,
            end_ts_ms: end.ts_ms,
            equity_change_usd,
            cash_change_usd,
            fill_cash_change_usd,
            mark_change_usd: if attributions.is_empty() {
                formula_mark_change_usd
            } else {
                detailed_mark_change_usd
            },
            mark_share,
            reason_codes,
            sync_reset_events,
            top_markets: attributions
                .into_iter()
                .take(REPLAY_ANOMALY_TOP_MARKET_LIMIT)
                .collect(),
        });
    }

    anomalies.sort_by(|left, right| {
        right
            .equity_change_usd
            .abs()
            .partial_cmp(&left.equity_change_usd.abs())
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.end_ts_ms.cmp(&right.end_ts_ms))
    });

    ReplayAnomalyDiagnostics {
        anomaly_count: anomalies.len(),
        max_abs_equity_jump_usd: anomalies
            .iter()
            .map(|item| item.equity_change_usd.abs())
            .fold(0.0, f64::max),
        max_mark_share: anomalies
            .iter()
            .map(|item| item.mark_share)
            .fold(0.0, f64::max),
        top_anomalies: anomalies
            .into_iter()
            .take(REPLAY_ANOMALY_TOP_LIMIT)
            .collect(),
    }
}

fn print_replay_summary(summary: &RustReplaySummary) {
    println!("==============================================================================");
    println!("Rust 历史回放");
    println!("==============================================================================");
    println!("数据库: {}", summary.db_path);
    println!("压力档位: {}", summary.stress.label);
    println!(
        "收益: 初始 {:.4} -> 结束 {:.4} | 净收益 {:+.4}",
        summary.initial_capital, summary.ending_equity, summary.total_pnl
    );
    println!(
        "风控: 最大回撤 {:.4} | 峰值资金占用 {:.4} | 峰值资金使用率 {:.2}%",
        summary.max_drawdown,
        summary.max_capital_in_use,
        summary.max_capital_utilization * 100.0
    );
    println!(
        "交易: 信号 {} | 开仓 {} | 平仓 {} | 到期结算 {} | round trip {} | 胜率 {:.2}%",
        summary.signal_count,
        summary.entry_count,
        summary.close_count,
        summary.settlement_count,
        summary.round_trip_count,
        summary.win_rate * 100.0
    );
    println!(
        "成本: fee {:.4} | slippage {:.4} | Poly rows {} | group rows {}",
        summary.total_fees_paid,
        summary.total_slippage_paid,
        summary.replayed_poly_observations,
        summary.replayed_grouped_rows
    );
    if summary.sanitized_market_minute_count > 0 || summary.sanitized_poly_observation_count > 0 {
        println!(
            "净化: sync windows {} | sanitized market-minutes {} | sanitized poly obs {}",
            summary.sanitized_sync_reset_window_count,
            summary.sanitized_market_minute_count,
            summary.sanitized_poly_observation_count,
        );
    }
    println!(
        "异常: {} | 最大异常跳变 {:.4} | 最大 mark 占比 {:.2}%",
        summary.anomaly_count,
        summary.max_abs_anomaly_equity_jump_usd,
        summary.max_anomaly_mark_share * 100.0,
    );
}

fn print_replay_anomaly_summary(diagnostics: &ReplayAnomalyDiagnostics) {
    if diagnostics.anomaly_count == 0 {
        return;
    }
    println!("------------------------------------------------------------------------------");
    println!(
        "Anomaly: {} 个 | 最大跳变 {:.4} | 最大 mark 占比 {:.2}%",
        diagnostics.anomaly_count,
        diagnostics.max_abs_equity_jump_usd,
        diagnostics.max_mark_share * 100.0,
    );
    if let Some(top) = diagnostics.top_anomalies.first() {
        println!(
            "Top anomaly: {} -> {} | equity {:+.4} | fill {:+.4} | mark {:+.4} | reasons {}",
            top.start_ts_ms,
            top.end_ts_ms,
            top.equity_change_usd,
            top.fill_cash_change_usd,
            top.mark_change_usd,
            top.reason_codes.join(","),
        );
    }
}

fn print_stress_summary(results: &[RustReplaySummary]) {
    println!("==============================================================================");
    println!("Rust 压力回放对比");
    println!("==============================================================================");
    for summary in results {
        println!(
            "{:<13} pnl {:+12.4} | end {:>12.4} | maxdd {:>10.4} | win {:>7.2}% | fills {:>4.0}%",
            summary.stress.label,
            summary.total_pnl,
            summary.ending_equity,
            summary.max_drawdown,
            summary.win_rate * 100.0,
            summary.stress.entry_fill_ratio * 100.0,
        );
    }
}

fn token_side_label(token_side: TokenSide) -> &'static str {
    match token_side {
        TokenSide::Yes => "yes",
        TokenSide::No => "no",
    }
}

fn candidate_action_label(candidate: &OpenCandidate) -> String {
    match candidate.direction.as_str() {
        "long" => "open_candidate".to_string(),
        "short" => "reverse_candidate".to_string(),
        _ => "candidate".to_string(),
    }
}

fn write_json<T: Serialize>(path: &str, value: &T) -> Result<()> {
    let path_ref = Path::new(path);
    if let Some(parent) = path_ref.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create report directory `{}`", parent.display()))?;
    }
    let payload = serde_json::to_vec_pretty(value).context("failed to serialize json report")?;
    fs::write(path_ref, payload)
        .with_context(|| format!("failed to write json report `{}`", path_ref.display()))
}

fn write_equity_csv(path: &str, equity_curve: &[EquityPoint]) -> Result<()> {
    let path_ref = Path::new(path);
    if let Some(parent) = path_ref.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create equity directory `{}`", parent.display()))?;
    }

    let mut out = String::from(
        "ts_ms,equity,cash,available_cash,unrealized_pnl,gross_exposure,reserved_margin,capital_in_use,open_positions\n",
    );
    for point in equity_curve {
        let _ = writeln!(
            out,
            "{},{:.8},{:.8},{:.8},{:.8},{:.8},{:.8},{:.8},{}",
            point.ts_ms,
            point.equity,
            point.cash,
            point.available_cash,
            point.unrealized_pnl,
            point.gross_exposure,
            point.reserved_margin,
            point.capital_in_use,
            point.open_positions
        );
    }
    fs::write(path_ref, out)
        .with_context(|| format!("failed to write equity csv `{}`", path_ref.display()))
}

fn write_trades_csv(path: &str, trade_log: &[ReplayTradeRow]) -> Result<()> {
    let path_ref = Path::new(path);
    if let Some(parent) = path_ref.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create trades directory `{}`", parent.display()))?;
    }

    let mut out = String::from(
        "trade_id,strategy,entry_time,event_id,market_id,market_slug,exit_time,direction,entry_poly_price,exit_poly_price,entry_cex_price,exit_cex_price,poly_shares,cex_qty,gross_pnl,fees,slippage,gas,funding_cost,net_pnl,holding_bars,z_score_at_entry,basis_bps_at_entry,delta_at_entry\n",
    );
    for trade in trade_log {
        let _ = writeln!(
            out,
            "{},{},{},{},{},{},{},{},{:.8},{:.8},{:.8},{:.8},{:.8},{:.8},{:.8},{:.8},{:.8},{:.8},{:.8},{:.8},{},{:.8},{:.8},{:.8}",
            trade.trade_id,
            csv_escape(&trade.strategy),
            trade.entry_time,
            csv_escape(&trade.event_id),
            csv_escape(&trade.market_id),
            csv_escape(&trade.market_slug),
            trade.exit_time,
            csv_escape(&trade.direction),
            trade.entry_poly_price,
            trade.exit_poly_price,
            trade.entry_cex_price,
            trade.exit_cex_price,
            trade.poly_shares,
            trade.cex_qty,
            trade.gross_pnl,
            trade.fees,
            trade.slippage,
            trade.gas,
            trade.funding_cost,
            trade.net_pnl,
            trade.holding_bars,
            trade.z_score_at_entry,
            trade.basis_bps_at_entry,
            trade.delta_at_entry,
        );
    }
    fs::write(path_ref, out)
        .with_context(|| format!("failed to write trades csv `{}`", path_ref.display()))
}

fn write_snapshots_csv(path: &str, snapshot_log: &[ReplaySnapshotRow]) -> Result<()> {
    let path_ref = Path::new(path);
    if let Some(parent) = path_ref.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create snapshots directory `{}`",
                parent.display()
            )
        })?;
    }

    let mut out = String::from(
        "ts_ms,event_id,market_id,market_slug,yes_price,no_price,cex_reference_price,cex_advance_price,signal_count,signal_action,signal_token_side,signal_basis_bps,signal_z_score,snapshot_token_side,snapshot_poly_price,snapshot_fair_value,snapshot_basis_bps,snapshot_z_score,snapshot_delta,snapshot_sigma,snapshot_minutes_to_expiry\n",
    );
    for snap in snapshot_log {
        let _ = writeln!(
            out,
            "{},{},{},{},{},{},{:.8},{:.8},{},{},{},{},{},{},{:.8},{:.8},{:.4},{:.4},{:.6},{:.6},{:.2}",
            snap.ts_ms,
            csv_escape(&snap.event_id),
            csv_escape(&snap.market_id),
            csv_escape(&snap.market_slug),
            snap.yes_price.map_or(String::new(), |v| format!("{:.8}", v)),
            snap.no_price.map_or(String::new(), |v| format!("{:.8}", v)),
            snap.cex_reference_price,
            snap.cex_advance_price,
            snap.signal_count,
            snap.signal_action.as_deref().unwrap_or(""),
            snap.signal_token_side.as_deref().unwrap_or(""),
            snap.signal_basis_bps.map_or(String::new(), |v| format!("{:.4}", v)),
            snap.signal_z_score.map_or(String::new(), |v| format!("{:.4}", v)),
            snap.snapshot_token_side.as_deref().unwrap_or(""),
            snap.snapshot_poly_price.map_or(String::new(), |v| format!("{:.8}", v)),
            snap.snapshot_fair_value.map_or(String::new(), |v| format!("{:.8}", v)),
            snap.snapshot_basis_bps.unwrap_or(0.0),
            snap.snapshot_z_score.unwrap_or(0.0),
            snap.snapshot_delta.unwrap_or(0.0),
            snap.snapshot_sigma.unwrap_or(0.0),
            snap.snapshot_minutes_to_expiry.unwrap_or(0.0),
        );
    }
    fs::write(path_ref, out)
        .with_context(|| format!("failed to write snapshots csv `{}`", path_ref.display()))
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

fn sql_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn i64_to_u64(value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| anyhow!("negative value cannot convert to u64: {value}"))
}

fn decimal_from_f64(value: f64) -> Result<Decimal> {
    Decimal::from_f64(value).ok_or_else(|| anyhow!("failed to convert `{value}` into Decimal"))
}

fn decimal_from_f64_lossy(value: f64) -> Decimal {
    Decimal::from_f64(value).unwrap_or_default()
}

impl ReplayMinuteRow {
    fn price_for(&self, token_side: TokenSide) -> Option<f64> {
        match token_side {
            TokenSide::Yes => self.yes_price,
            TokenSide::No => self.no_price,
        }
    }

    fn current_price_for_position(&self, position: Option<&ReplayPosition>) -> Option<f64> {
        position.and_then(|position| self.price_for(position.token_side))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn sample_replay_spec() -> ReplayMarketSpec {
        ReplayMarketSpec {
            event_id: "evt-1".to_owned(),
            market_id: "mkt-1".to_owned(),
            market_slug: "btc-above-100k".to_owned(),
            symbol: Symbol::new("btc-above-100k"),
            market_config: MarketConfig {
                symbol: Symbol::new("btc-above-100k"),
                poly_ids: PolymarketIds {
                    condition_id: "cond-1".to_owned(),
                    yes_token_id: "yes-1".to_owned(),
                    no_token_id: "no-1".to_owned(),
                },
                market_question: Some("Will Bitcoin be above $100,000 on Friday?".to_owned()),
                market_rule: Some(MarketRule {
                    kind: MarketRuleKind::Above,
                    lower_strike: Some(Price(Decimal::new(100_000, 0))),
                    upper_strike: None,
                }),
                cex_symbol: "BTCUSDT".to_owned(),
                hedge_exchange: Exchange::Binance,
                strike_price: Some(Price(Decimal::new(100_000, 0))),
                settlement_timestamp: 1_750_000_000,
                min_tick_size: Price(Decimal::new(1, 2)),
                neg_risk: false,
                cex_price_tick: Decimal::new(1, 1),
                cex_qty_step: Decimal::new(1, 3),
                cex_contract_multiplier: Decimal::ONE,
            },
            settlement_ts_ms: 1_750_000_000_000,
            yes_payout: None,
            no_payout: None,
        }
    }

    fn replay_spec_for_question(
        event_id: &str,
        market_id: &str,
        question: &str,
    ) -> ReplayMarketSpec {
        ReplayMarketSpec {
            event_id: event_id.to_owned(),
            market_id: market_id.to_owned(),
            market_slug: format!("mkt-{market_id}"),
            symbol: Symbol::new(format!("mkt-{market_id}")),
            market_config: MarketConfig {
                symbol: Symbol::new(format!("mkt-{market_id}")),
                poly_ids: PolymarketIds {
                    condition_id: format!("condition-{market_id}"),
                    yes_token_id: format!("yes-{market_id}"),
                    no_token_id: format!("no-{market_id}"),
                },
                market_question: Some(question.to_owned()),
                market_rule: Some(parse_market_rule(question).expect("market rule")),
                cex_symbol: "BTCUSDT".to_owned(),
                hedge_exchange: Exchange::Binance,
                strike_price: None,
                settlement_timestamp: 10_000,
                min_tick_size: Price(Decimal::new(1, 2)),
                neg_risk: false,
                cex_price_tick: Decimal::new(1, 1),
                cex_qty_step: Decimal::new(1, 3),
                cex_contract_multiplier: Decimal::ONE,
            },
            settlement_ts_ms: 10_000 * 1000,
            yes_payout: None,
            no_payout: None,
        }
    }

    fn sample_replay_row(spec: &ReplayMarketSpec) -> ReplayMinuteRow {
        ReplayMinuteRow {
            ts_ms: 1_749_999_940_000,
            market_id: spec.market_id.clone(),
            yes_price: Some(0.50),
            no_price: Some(0.50),
        }
    }

    fn sample_open_candidate(spec: &ReplayMarketSpec, row: &ReplayMinuteRow) -> OpenCandidate {
        OpenCandidate {
            schema_version: 1,
            candidate_id: "cand-1".to_owned(),
            correlation_id: "corr-1".to_owned(),
            symbol: spec.symbol.clone(),
            token_side: TokenSide::Yes,
            direction: "long".to_owned(),
            fair_value: 0.69,
            raw_mispricing: 0.20,
            delta_estimate: 0.00012,
            risk_budget_usd: 100.0,
            strength: polyalpha_core::SignalStrength::Normal,
            z_score: Some(-2.4),
            timestamp_ms: row.ts_ms,
        }
    }

    fn sample_replay_config(
        entry_fill_ratio: f64,
        poly_fee_bps: f64,
        poly_slippage_bps: f64,
        cex_fee_bps: f64,
        cex_slippage_bps: f64,
    ) -> ResolvedReplayConfig {
        ResolvedReplayConfig {
            db_path: ":memory:".to_owned(),
            initial_capital: 10_000.0,
            rolling_window: 60,
            entry_z: 2.0,
            exit_z: 0.5,
            position_notional_usd: 100.0,
            max_capital_usage: 1.0,
            cex_hedge_ratio: 1.0,
            cex_margin_ratio: 0.1,
            planner_depth_levels: MarketDataConfig::default().planner_depth_levels,
            band_policy: BandPolicyMode::ConfiguredBand,
            min_poly_price: None,
            max_poly_price: None,
            max_holding_bars: None,
            stress: ExecutionStressConfig {
                label: "stress".to_owned(),
                poly_fee_bps,
                poly_slippage_bps,
                cex_fee_bps,
                cex_slippage_bps,
                entry_fill_ratio,
            },
        }
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct PlannerRejectionSample {
        ts_ms: u64,
        market_id: String,
        token_side: TokenSide,
        yes_price: Option<f64>,
        no_price: Option<f64>,
        fair_value: f64,
        raw_mispricing: f64,
        z_score: Option<f64>,
        delta_estimate: f64,
        reason_code: String,
        detail: String,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct PlannerWindowDiagnostic {
        market_id: String,
        start: String,
        end: String,
        signal_count: usize,
        entry_count: usize,
        planner_rejections: usize,
        planner_reason_counts: BTreeMap<String, usize>,
        samples: Vec<PlannerRejectionSample>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct SpikeRawPriceRow {
        event_id: String,
        market_id: String,
        token_id: String,
        token_side: String,
        ts_ms: u64,
        poly_price: f64,
        market_question: String,
    }

    fn replay_fixture_db_path() -> PathBuf {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        for ancestor in manifest_dir.ancestors() {
            let candidate = ancestor.join("data/btc_basis_backtest_price_only_2025_2026.duckdb");
            if candidate.exists() {
                return candidate;
            }
        }
        manifest_dir.join("../../../../data/btc_basis_backtest_price_only_2025_2026.duckdb")
    }

    fn sample_replay_stress() -> ExecutionStressConfig {
        ExecutionStressConfig {
            label: "custom".to_owned(),
            poly_fee_bps: 2.0,
            poly_slippage_bps: 50.0,
            cex_fee_bps: 2.0,
            cex_slippage_bps: 2.0,
            entry_fill_ratio: 1.0,
        }
    }

    async fn diagnose_planner_rejections_for_window(
        market_id: &str,
        start: &str,
        end: &str,
    ) -> PlannerWindowDiagnostic {
        let db_path = replay_fixture_db_path();
        assert!(
            db_path.exists(),
            "expected replay fixture database at {}",
            db_path.display()
        );
        let db_path_str = db_path.to_string_lossy().to_string();
        let filter = build_filter(Some(market_id.to_owned()), Some(start), Some(end))
            .expect("window filter");
        let dataset = load_dataset(&db_path_str, &filter).expect("load dataset");
        let config = resolve_replay_config(
            &db_path_str,
            100_000.0,
            360,
            2.0,
            0.5,
            None,
            0.25,
            1.0,
            0.1,
            5,
            BandPolicyMode::ConfiguredBand,
            None,
            None,
            None,
            sample_replay_stress(),
        )
        .expect("resolve config");
        let markets = dataset
            .markets
            .values()
            .map(|spec| spec.market_config.clone())
            .collect::<Vec<_>>();
        let engine_config = SimpleEngineConfig {
            min_signal_samples: 2,
            rolling_window_minutes: config.rolling_window,
            entry_z: config.entry_z,
            exit_z: config.exit_z,
            position_notional_usd: UsdNotional(
                decimal_from_f64(config.position_notional_usd).expect("position notional"),
            ),
            cex_hedge_ratio: config.cex_hedge_ratio,
            dmm_half_spread: Decimal::new(1, 2),
            dmm_quote_size: polyalpha_core::PolyShares::ZERO,
            ..SimpleEngineConfig::default()
        };
        let mut engine = SimpleAlphaEngine::with_markets(engine_config, markets);
        let mut runtime = dataset
            .markets
            .keys()
            .map(|id| (id.clone(), ReplayRuntimeState::default()))
            .collect::<HashMap<_, _>>();
        let mut positions: HashMap<Symbol, ReplayPosition> = HashMap::new();
        let mut trade_log = Vec::new();
        let mut stats = ReplayStats::default();
        let planner = replay_execution_planner(&config);
        let mut cash = config.initial_capital;
        let mut reason_counts = BTreeMap::new();
        let mut samples = Vec::new();

        let mut idx = 0usize;
        while idx < dataset.rows.len() {
            let ts_ms = dataset.rows[idx].ts_ms;
            let mut batch_end = idx + 1;
            while batch_end < dataset.rows.len() && dataset.rows[batch_end].ts_ms == ts_ms {
                batch_end += 1;
            }

            let cex_reference_price = match ts_ms
                .checked_sub(ONE_MINUTE_MS)
                .and_then(|prior_ts| dataset.cex_close_by_ts.get(&prior_ts).copied())
            {
                Some(price) => price,
                None => {
                    idx = batch_end;
                    continue;
                }
            };
            let cex_advance_price = match dataset.cex_close_by_ts.get(&ts_ms).copied() {
                Some(price) => price,
                None => {
                    idx = batch_end;
                    continue;
                }
            };

            for position in positions.values_mut() {
                position.last_cex_price = cex_reference_price;
            }

            let mut pending_close_signals = Vec::new();
            let mut pending_entry_signals = Vec::new();
            for row in &dataset.rows[idx..batch_end] {
                let Some(spec) = dataset.markets.get(&row.market_id) else {
                    continue;
                };
                let state = runtime
                    .get_mut(&row.market_id)
                    .expect("runtime state for market");

                if !state.bootstrapped {
                    bootstrap_symbol_cex_state(
                        &mut engine,
                        &spec.symbol,
                        &dataset.cex_close_by_ts,
                        config.rolling_window,
                        ts_ms,
                    )
                    .await
                    .expect("bootstrap cex state");
                    state.bootstrapped = true;
                    state.last_cex_synced_minute_ms =
                        Some(ts_ms.checked_sub(ONE_MINUTE_MS).unwrap_or_default());
                }

                advance_symbol_cex_state(
                    &mut engine,
                    &spec.symbol,
                    &dataset.cex_close_by_ts,
                    state,
                    ts_ms,
                )
                .await
                .expect("advance cex state");

                if let Some(position) = positions.get_mut(&spec.symbol) {
                    match position.token_side {
                        TokenSide::Yes => {
                            if let Some(price) = row.yes_price {
                                position.last_poly_price = price;
                            }
                        }
                        TokenSide::No => {
                            if let Some(price) = row.no_price {
                                position.last_poly_price = price;
                            }
                        }
                    }
                }

                if let Some(yes_price) = row.yes_price {
                    let _ = engine
                        .on_market_data(&poly_trade_event(
                            &spec.symbol,
                            InstrumentKind::PolyYes,
                            yes_price,
                            ts_ms,
                        ))
                        .await;
                }
                if let Some(no_price) = row.no_price {
                    let _ = engine
                        .on_market_data(&poly_trade_event(
                            &spec.symbol,
                            InstrumentKind::PolyNo,
                            no_price,
                            ts_ms,
                        ))
                        .await;
                }

                let output = engine
                    .on_market_data(&cex_trade_event(&spec.symbol, cex_advance_price, ts_ms + 1))
                    .await;
                for candidate in output.open_candidates {
                    pending_entry_signals.push(PendingReplayCandidate {
                        candidate,
                        spec: spec.clone(),
                        row: row.clone(),
                        cex_reference_price,
                    });
                }
                if let Some(reason) = engine.close_reason(&spec.symbol) {
                    pending_close_signals.push(PendingReplayClose {
                        close_reason: reason,
                        spec: spec.clone(),
                        row: row.clone(),
                        cex_reference_price,
                    });
                }
            }

            for pending in pending_close_signals {
                handle_close_signal(
                    &pending.close_reason,
                    &pending.spec,
                    &pending.row,
                    pending.cex_reference_price,
                    &config,
                    &planner,
                    &mut positions,
                    &mut cash,
                    &mut stats,
                    &mut trade_log,
                )
                .expect("close handling");
                engine.sync_position_state(
                    &pending.spec.symbol,
                    positions
                        .get(&pending.spec.symbol)
                        .map(|position| position.token_side),
                );
            }

            pending_entry_signals.sort_by(|left, right| {
                let left_z = left.candidate.z_score.unwrap_or(f64::INFINITY);
                let right_z = right.candidate.z_score.unwrap_or(f64::INFINITY);
                left_z
                    .partial_cmp(&right_z)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| {
                        let left_basis = left.candidate.raw_mispricing.abs();
                        let right_basis = right.candidate.raw_mispricing.abs();
                        right_basis
                            .partial_cmp(&left_basis)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
            });

            for pending in pending_entry_signals {
                if !positions.contains_key(&pending.spec.symbol) {
                    let context = replay_planning_context(
                        config.planner_depth_levels,
                        &pending.candidate.symbol,
                        &pending.row,
                        pending.cex_reference_price,
                        pending.spec.market_config.cex_qty_step,
                        &positions,
                    )
                    .expect("planning context");
                    let intent = replay_open_intent(&pending.candidate);
                    if let Err(rejection) = planner.plan(&intent, &context) {
                        *reason_counts
                            .entry(rejection.reason.code().to_owned())
                            .or_insert(0) += 1;
                        if samples.len() < 24 {
                            samples.push(PlannerRejectionSample {
                                ts_ms: pending.row.ts_ms,
                                market_id: pending.spec.market_id.clone(),
                                token_side: pending.candidate.token_side,
                                yes_price: pending.row.yes_price,
                                no_price: pending.row.no_price,
                                fair_value: pending.candidate.fair_value,
                                raw_mispricing: pending.candidate.raw_mispricing,
                                z_score: pending.candidate.z_score,
                                delta_estimate: pending.candidate.delta_estimate,
                                reason_code: rejection.reason.code().to_owned(),
                                detail: rejection.detail,
                            });
                        }
                    }
                }

                handle_candidate(
                    pending.candidate,
                    &pending.spec,
                    &pending.row,
                    pending.cex_reference_price,
                    &config,
                    &planner,
                    &mut positions,
                    &mut cash,
                    &mut stats,
                    &mut trade_log,
                )
                .expect("candidate handling");
                engine.sync_position_state(
                    &pending.spec.symbol,
                    positions
                        .get(&pending.spec.symbol)
                        .map(|position| position.token_side),
                );
            }

            idx = batch_end;
        }

        PlannerWindowDiagnostic {
            market_id: market_id.to_owned(),
            start: start.to_owned(),
            end: end.to_owned(),
            signal_count: stats.signals_emitted,
            entry_count: stats.entry_count,
            planner_rejections: stats.signals_rejected_planner,
            planner_reason_counts: reason_counts,
            samples,
        }
    }

    fn load_spike_raw_price_rows(
        db_path: &str,
        start_ts_ms: u64,
        end_ts_ms: u64,
    ) -> Result<Vec<SpikeRawPriceRow>> {
        let config = DuckConfig::default()
            .access_mode(AccessMode::ReadOnly)
            .context("failed to configure read-only duckdb access")?;
        let conn = Connection::open_with_flags(db_path, config)
            .with_context(|| format!("failed to open duckdb database `{db_path}`"))?;

        let sql = format!(
            "
            select
                p.event_id,
                p.market_id,
                p.token_id,
                lower(p.token_side) as token_side,
                p.ts_ms,
                p.poly_price,
                c.market_question
            from polymarket_price_history p
            join polymarket_contracts c
              on c.market_id = p.market_id
             and c.token_id = p.token_id
            where p.event_id in ('261279', '261299')
              and p.ts_ms between {start_ts_ms} and {end_ts_ms}
            order by p.ts_ms asc, p.event_id asc, p.market_id asc, token_side asc
            "
        );

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            Ok(SpikeRawPriceRow {
                event_id: row.get::<_, String>(0)?,
                market_id: row.get::<_, String>(1)?,
                token_id: row.get::<_, String>(2)?,
                token_side: row.get::<_, String>(3)?,
                ts_ms: row.get::<_, i64>(4)? as u64,
                poly_price: row.get::<_, f64>(5)?,
                market_question: row.get::<_, String>(6)?,
            })
        })?;

        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    fn print_spike_grouped_rows(db_path: &str, start_ts_ms: u64, end_ts_ms: u64) -> Result<()> {
        let filter = ReplayFilter {
            market_id: None,
            start_ts_ms: Some(start_ts_ms),
            end_ts_ms: Some(end_ts_ms),
        };
        let dataset = load_dataset(db_path, &filter)?;
        for row in dataset.rows.iter().filter(|row| {
            matches!(
                row.market_id.as_str(),
                "1559492" | "1559491" | "1559402" | "1559400" | "1559404" | "1559403"
            )
        }) {
            println!(
                "GROUPED ts_ms={} market_id={} yes_price={:?} no_price={:?}",
                row.ts_ms, row.market_id, row.yes_price, row.no_price
            );
        }
        Ok(())
    }

    fn assert_f64_close(actual: f64, expected: f64) {
        let diff = (actual - expected).abs();
        assert!(
            diff <= 1e-9,
            "expected {expected:.12}, got {actual:.12}, diff {diff:.12}"
        );
    }

    fn sample_close_intent(spec: &ReplayMarketSpec) -> PlanningIntent {
        PlanningIntent::ClosePosition {
            schema_version: 1,
            intent_id: format!("replay-close-{}", spec.symbol.0),
            correlation_id: "corr-close-1".to_owned(),
            symbol: spec.symbol.clone(),
            close_reason: "basis_reverted".to_owned(),
            target_close_ratio: 1.0,
        }
    }

    #[test]
    fn parses_supported_market_rules() {
        let above = parse_market_rule("Will Bitcoin be above $100,000 on Friday?").unwrap();
        assert_eq!(above.kind, MarketRuleKind::Above);
        assert_eq!(
            above.lower_strike.map(|value| value.to_f64()),
            Some(100_000.0)
        );

        let below = parse_market_rule("Will Bitcoin be below $90,000?").unwrap();
        assert_eq!(below.kind, MarketRuleKind::Below);
        assert_eq!(
            below.upper_strike.map(|value| value.to_f64()),
            Some(90_000.0)
        );

        let between = parse_market_rule(
            "Will the price of Bitcoin be between $62,000 and $64,000 on March 18?",
        )
        .unwrap();
        assert_eq!(between.kind, MarketRuleKind::Between);
        assert_eq!(
            between.lower_strike.map(|value| value.to_f64()),
            Some(62_000.0)
        );
        assert_eq!(
            between.upper_strike.map(|value| value.to_f64()),
            Some(64_000.0)
        );
    }

    #[test]
    fn parses_market_rules_with_k_suffix() {
        let above = parse_market_rule("Bitcoin above $122K on September 10?").unwrap();
        assert_eq!(above.kind, MarketRuleKind::Above);
        assert_eq!(
            above.lower_strike.map(|value| value.to_f64()),
            Some(122_000.0)
        );

        let between =
            parse_market_rule("Will Bitcoin be between $118K and $120K on Friday?").unwrap();
        assert_eq!(between.kind, MarketRuleKind::Between);
        assert_eq!(
            between.lower_strike.map(|value| value.to_f64()),
            Some(118_000.0)
        );
        assert_eq!(
            between.upper_strike.map(|value| value.to_f64()),
            Some(120_000.0)
        );
    }

    #[test]
    fn rejects_unsupported_market_rule() {
        let error = parse_market_rule("Will MicroStrategy announce another purchase?").unwrap_err();
        assert!(error
            .to_string()
            .contains("unsupported market question for causal replay"));
    }

    #[test]
    fn identifies_official_resolution() {
        assert!(is_official_resolution(&[1.0, 0.0], "resolved"));
        assert!(is_official_resolution(&[0.0, 1.0], "finalized"));
        assert!(!is_official_resolution(&[0.6, 0.4], "pending"));
    }

    #[test]
    fn writes_trade_csv_with_expected_header_and_fields() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = env::temp_dir().join(format!(
            "polyalpha-trades-{}-{nonce}.csv",
            std::process::id()
        ));
        let trades = vec![ReplayTradeRow {
            trade_id: 1,
            strategy: "arb".to_owned(),
            entry_time: 1_750_000_000,
            event_id: "evt-1".to_owned(),
            market_id: "mkt-1".to_owned(),
            market_slug: "btc-above-100k".to_owned(),
            exit_time: 1_750_000_600,
            direction: "yes_settled".to_owned(),
            entry_poly_price: 0.42,
            exit_poly_price: 1.0,
            entry_cex_price: 100_000.0,
            exit_cex_price: 101_000.0,
            poly_shares: 2_380.95238095,
            cex_qty: 0.12345678,
            gross_pnl: 123.45,
            fees: 4.56,
            slippage: 7.89,
            gas: 0.0,
            funding_cost: 0.0,
            net_pnl: 111.0,
            holding_bars: 10,
            z_score_at_entry: -2.05,
            basis_bps_at_entry: -123.45,
            delta_at_entry: 0.12,
        }];

        write_trades_csv(path.to_str().unwrap_or_default(), &trades).unwrap();
        let payload = fs::read_to_string(&path).unwrap();
        assert!(payload.starts_with(
            "trade_id,strategy,entry_time,event_id,market_id,market_slug,exit_time,direction,"
        ));
        assert!(payload.contains("evt-1,mkt-1,btc-above-100k"));
        assert!(payload.contains("yes_settled"));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn handle_candidate_rejects_non_positive_planned_edge() {
        let spec = sample_replay_spec();
        let row = sample_replay_row(&spec);
        let candidate = OpenCandidate {
            schema_version: 1,
            candidate_id: "cand-1".to_owned(),
            correlation_id: "corr-1".to_owned(),
            symbol: spec.symbol.clone(),
            token_side: TokenSide::Yes,
            direction: "long".to_owned(),
            fair_value: 0.5001,
            raw_mispricing: 0.0001,
            delta_estimate: 0.10,
            risk_budget_usd: 100.0,
            strength: polyalpha_core::SignalStrength::Weak,
            z_score: Some(-2.0),
            timestamp_ms: row.ts_ms,
        };
        let config = sample_replay_config(1.0, 100.0, 100.0, 100.0, 100.0);
        let planner = replay_execution_planner(&config);
        let mut positions = HashMap::new();
        let mut cash = config.initial_capital;
        let mut stats = ReplayStats::default();
        let mut trade_log = Vec::new();

        handle_candidate(
            candidate,
            &spec,
            &row,
            100_000.0,
            &config,
            &planner,
            &mut positions,
            &mut cash,
            &mut stats,
            &mut trade_log,
        )
        .expect("candidate handling should not error");

        assert!(
            positions.is_empty(),
            "planner-rejected candidate must not open"
        );
        assert_eq!(stats.entry_count, 0);
        assert_eq!(stats.signals_rejected_planner, 1);
        assert_eq!(cash, config.initial_capital);
    }

    #[test]
    fn separates_price_band_rejections_from_true_missing_quotes() {
        let spec = sample_replay_spec();
        let mut row = sample_replay_row(&spec);
        row.yes_price = Some(0.05);
        row.no_price = Some(0.95);
        let mut candidate = sample_open_candidate(&spec, &row);
        candidate.token_side = TokenSide::Yes;

        let mut config = sample_replay_config(1.0, 20.0, 50.0, 5.0, 2.0);
        config.min_poly_price = Some(0.2);
        config.max_poly_price = Some(0.5);

        let planner = replay_execution_planner(&config);
        let mut positions = HashMap::new();
        let mut cash = config.initial_capital;
        let mut stats = ReplayStats::default();
        let mut trade_log = Vec::new();

        handle_candidate(
            candidate,
            &spec,
            &row,
            100_000.0,
            &config,
            &planner,
            &mut positions,
            &mut cash,
            &mut stats,
            &mut trade_log,
        )
        .expect("candidate handling should not error");

        assert_eq!(stats.signals_rejected_missing_price, 0);
        assert_eq!(stats.signals_rejected_below_min_poly_price, 1);
        assert_eq!(stats.signals_rejected_above_or_equal_max_poly_price, 0);
    }

    #[test]
    fn market_debug_rows_include_reason_breakdown() {
        let mut stats = ReplayStats::default();
        stats.signals_by_market.insert("mkt-1".to_owned(), 3);
        stats
            .rejected_below_min_price_by_market
            .insert("mkt-1".to_owned(), 2);

        let rows = all_market_debug_rows(&stats);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].signal_count, 3);
        assert_eq!(rows[0].rejected_below_min_price_count, 2);
        assert_eq!(rows[0].rejected_missing_price_count, 0);
        assert_eq!(rows[0].rejected_above_or_equal_max_price_count, 0);
    }

    #[test]
    fn disabled_band_policy_keeps_candidate_tradable_inside_old_price_filter() {
        let spec = sample_replay_spec();
        let mut row = sample_replay_row(&spec);
        row.yes_price = Some(0.05);
        row.no_price = Some(0.95);
        let mut candidate = sample_open_candidate(&spec, &row);
        candidate.token_side = TokenSide::Yes;

        let mut config = sample_replay_config(1.0, 20.0, 50.0, 5.0, 2.0);
        config.band_policy = BandPolicyMode::Disabled;
        config.min_poly_price = Some(0.2);
        config.max_poly_price = Some(0.5);

        let planner = replay_execution_planner(&config);
        let mut positions = HashMap::new();
        let mut cash = config.initial_capital;
        let mut stats = ReplayStats::default();
        let mut trade_log = Vec::new();

        handle_candidate(
            candidate,
            &spec,
            &row,
            100_000.0,
            &config,
            &planner,
            &mut positions,
            &mut cash,
            &mut stats,
            &mut trade_log,
        )
        .expect("candidate handling should not error");

        assert_eq!(stats.entry_count, 1);
        assert_eq!(stats.signals_rejected_below_min_poly_price, 0);
        assert!(positions.contains_key(&spec.symbol));
    }

    #[test]
    fn handle_candidate_uses_executable_notional_without_double_counting_friction() {
        let spec = sample_replay_spec();
        let row = sample_replay_row(&spec);
        let candidate = sample_open_candidate(&spec, &row);
        let config = sample_replay_config(1.0, 20.0, 50.0, 5.0, 2.0);
        let planner = replay_execution_planner(&config);
        let plan = plan_replay_open_candidate(
            &planner,
            config.planner_depth_levels,
            &candidate,
            &row,
            100_000.0,
            spec.market_config.cex_qty_step,
            &HashMap::new(),
        )
        .expect("plan should succeed");
        let poly_exec_notional = (plan.poly_planned_shares.0 * plan.poly_executable_price.0)
            .to_f64()
            .expect("poly notional");
        let total_fees = (plan.poly_fee_usd.0 + plan.cex_fee_usd.0)
            .to_f64()
            .expect("total fees");
        let expected_cash = config.initial_capital - poly_exec_notional - total_fees;
        let expected_round_trip_pnl = -total_fees;

        let mut positions = HashMap::new();
        let mut cash = config.initial_capital;
        let mut stats = ReplayStats::default();
        let mut trade_log = Vec::new();
        handle_candidate(
            candidate,
            &spec,
            &row,
            100_000.0,
            &config,
            &planner,
            &mut positions,
            &mut cash,
            &mut stats,
            &mut trade_log,
        )
        .expect("candidate handling should not error");

        let position = positions
            .get(&spec.symbol)
            .expect("entry should open a replay position");
        assert_eq!(stats.entry_count, 1);
        assert_f64_close(cash, expected_cash);
        assert_f64_close(position.poly_entry_notional, poly_exec_notional);
        assert_f64_close(position.round_trip_pnl, expected_round_trip_pnl);
    }

    #[test]
    fn handle_candidate_scales_hedge_and_fees_with_entry_fill_ratio() {
        let spec = sample_replay_spec();
        let row = sample_replay_row(&spec);
        let candidate = sample_open_candidate(&spec, &row);
        let config = sample_replay_config(0.5, 20.0, 50.0, 5.0, 2.0);
        let planner = replay_execution_planner(&config);
        let plan = plan_replay_open_candidate(
            &planner,
            config.planner_depth_levels,
            &candidate,
            &row,
            100_000.0,
            spec.market_config.cex_qty_step,
            &HashMap::new(),
        )
        .expect("plan should succeed");
        let fill_ratio = Decimal::from_f64(config.stress.entry_fill_ratio).expect("fill ratio");
        let actual_shares = (plan.poly_planned_shares.0 * fill_ratio)
            .to_f64()
            .expect("actual shares");
        let actual_cex_qty = (plan.cex_planned_qty.0 * fill_ratio)
            .to_f64()
            .expect("actual cex qty");
        let poly_exec_notional =
            (plan.poly_planned_shares.0 * fill_ratio * plan.poly_executable_price.0)
                .to_f64()
                .expect("poly notional");
        let cex_exec_notional = (plan.cex_planned_qty.0 * fill_ratio * plan.cex_executable_price.0)
            .to_f64()
            .expect("cex notional");
        let expected_poly_fee = poly_exec_notional * config.stress.poly_fee_bps / 10_000.0;
        let expected_cex_fee = cex_exec_notional * config.stress.cex_fee_bps / 10_000.0;
        let expected_cash =
            config.initial_capital - poly_exec_notional - expected_poly_fee - expected_cex_fee;

        let mut positions = HashMap::new();
        let mut cash = config.initial_capital;
        let mut stats = ReplayStats::default();
        let mut trade_log = Vec::new();
        handle_candidate(
            candidate,
            &spec,
            &row,
            100_000.0,
            &config,
            &planner,
            &mut positions,
            &mut cash,
            &mut stats,
            &mut trade_log,
        )
        .expect("candidate handling should not error");

        let position = positions
            .get(&spec.symbol)
            .expect("entry should open a replay position");
        assert_eq!(stats.entry_count, 1);
        assert_f64_close(position.shares, actual_shares);
        assert_f64_close(position.cex_qty_btc, -actual_cex_qty);
        assert_f64_close(
            position.entry_fees_paid,
            expected_poly_fee + expected_cex_fee,
        );
        assert_f64_close(cash, expected_cash);
    }

    #[test]
    fn close_position_net_pnl_matches_cash_without_double_counting_slippage() {
        let spec = sample_replay_spec();
        let row = sample_replay_row(&spec);
        let candidate = sample_open_candidate(&spec, &row);
        let config = sample_replay_config(1.0, 20.0, 50.0, 5.0, 2.0);
        let planner = replay_execution_planner(&config);
        let open_plan = plan_replay_open_candidate(
            &planner,
            config.planner_depth_levels,
            &candidate,
            &row,
            100_000.0,
            spec.market_config.cex_qty_step,
            &HashMap::new(),
        )
        .expect("open plan should succeed");
        let open_poly_exec_notional = (open_plan.poly_planned_shares.0
            * open_plan.poly_executable_price.0)
            .to_f64()
            .expect("poly notional");
        let open_total_fees = (open_plan.poly_fee_usd.0 + open_plan.cex_fee_usd.0)
            .to_f64()
            .expect("open fees");
        let open_total_slippage = (open_plan.poly_friction_cost_usd.0
            + open_plan.cex_friction_cost_usd.0)
            .to_f64()
            .expect("open slippage");
        let cex_qty_abs = open_plan.cex_planned_qty.0.to_f64().expect("cex qty");
        let cex_qty_btc = if matches!(open_plan.cex_side, OrderSide::Buy) {
            cex_qty_abs
        } else {
            -cex_qty_abs
        };
        let cex_entry_price = open_plan
            .cex_executable_price
            .0
            .to_f64()
            .expect("cex entry price");
        let reserved_margin = cex_qty_abs * cex_entry_price * config.cex_margin_ratio;
        let cash_after_entry = config.initial_capital - open_poly_exec_notional - open_total_fees;

        let mut positions = HashMap::from([(
            spec.symbol.clone(),
            ReplayPosition {
                market_id: spec.market_id.clone(),
                token_side: candidate.token_side,
                shares: open_plan
                    .poly_planned_shares
                    .0
                    .to_f64()
                    .expect("poly shares"),
                poly_entry_notional: open_poly_exec_notional,
                last_poly_price: open_plan
                    .poly_executable_price
                    .0
                    .to_f64()
                    .expect("poly entry price"),
                cex_entry_price,
                cex_qty_btc,
                last_cex_price: cex_entry_price,
                reserved_margin,
                round_trip_pnl: -open_total_fees,
                entry_ts_ms: row.ts_ms,
                entry_poly_price: open_plan
                    .poly_executable_price
                    .0
                    .to_f64()
                    .expect("poly entry price"),
                entry_fees_paid: open_total_fees,
                entry_slippage_paid: open_total_slippage,
                entry_z_score: candidate.z_score.unwrap_or_default(),
                entry_basis_bps: candidate.raw_mispricing * 10_000.0,
                entry_delta: candidate.delta_estimate,
            },
        )]);
        let close_context = replay_planning_context(
            config.planner_depth_levels,
            &spec.symbol,
            &row,
            100_000.0,
            spec.market_config.cex_qty_step,
            &positions,
        )
        .unwrap();
        let close_plan = planner
            .plan(&sample_close_intent(&spec), &close_context)
            .expect("close plan should succeed");
        let close_poly_exec_notional = (close_plan.poly_planned_shares.0
            * close_plan.poly_executable_price.0)
            .to_f64()
            .expect("close poly notional");
        let close_total_fees = (close_plan.poly_fee_usd.0 + close_plan.cex_fee_usd.0)
            .to_f64()
            .expect("close fees");
        let close_cex_price = close_plan
            .cex_executable_price
            .0
            .to_f64()
            .expect("close cex price");
        let close_cex_gross = linear_cex_pnl(cex_entry_price, close_cex_price, cex_qty_btc);
        let expected_final_cash =
            cash_after_entry + close_poly_exec_notional + close_cex_gross - close_total_fees;
        let expected_round_trip_net = expected_final_cash - config.initial_capital;

        let mut cash = cash_after_entry;
        let mut stats = ReplayStats::default();
        let mut trade_log = Vec::new();
        close_position(
            &spec.symbol,
            &spec,
            row.current_price_for_position(positions.get(&spec.symbol)),
            100_000.0,
            false,
            "basis_reverted",
            &config,
            &planner,
            &mut positions,
            &mut cash,
            &mut stats,
            &mut trade_log,
            row.ts_ms + ONE_MINUTE_MS,
        )
        .expect("close should succeed");

        let trade = trade_log
            .first()
            .expect("close should emit one trade log row");
        assert!(
            positions.is_empty(),
            "close should remove the replay position"
        );
        assert_f64_close(cash, expected_final_cash);
        assert_f64_close(trade.net_pnl, expected_round_trip_net);
    }

    #[test]
    fn detects_sync_probability_reset_anomaly_from_mark_dominated_jump() {
        let event_id = "evt-reset".to_owned();
        let event_markets = [
            (
                "1559492",
                "Will the price of Bitcoin be above $80,000 on March 18?",
                0.045,
                0.500,
                22_200.0222,
                0.538,
            ),
            (
                "1559402",
                "Will the price of Bitcoin be between $62,000 and $64,000 on March 18?",
                0.050,
                0.495,
                19_980.0200,
                0.174,
            ),
            (
                "1559400",
                "Will the price of Bitcoin be less than $62,000 on March 18?",
                0.065,
                0.495,
                15_369.2461,
                0.441,
            ),
        ];
        let mut markets = HashMap::new();
        let mut rows = Vec::new();
        let ts0 = 0_u64;
        let ts1 = ONE_MINUTE_MS;
        let ts2 = ONE_MINUTE_MS * 2;
        for (market_id, question, yes_t0, yes_t1, _, _) in event_markets {
            let spec = ReplayMarketSpec {
                event_id: event_id.clone(),
                market_id: market_id.to_owned(),
                market_slug: format!("mkt-{market_id}"),
                symbol: Symbol::new(format!("mkt-{market_id}")),
                market_config: MarketConfig {
                    symbol: Symbol::new(format!("mkt-{market_id}")),
                    poly_ids: PolymarketIds {
                        condition_id: format!("condition-{market_id}"),
                        yes_token_id: format!("yes-{market_id}"),
                        no_token_id: format!("no-{market_id}"),
                    },
                    market_question: Some(question.to_owned()),
                    market_rule: Some(parse_market_rule(question).expect("market rule")),
                    cex_symbol: "BTCUSDT".to_owned(),
                    hedge_exchange: Exchange::Binance,
                    strike_price: None,
                    settlement_timestamp: 10_000,
                    min_tick_size: Price(Decimal::new(1, 2)),
                    neg_risk: false,
                    cex_price_tick: Decimal::new(1, 1),
                    cex_qty_step: Decimal::new(1, 3),
                    cex_contract_multiplier: Decimal::ONE,
                },
                settlement_ts_ms: 10_000 * 1000,
                yes_payout: None,
                no_payout: None,
            };
            markets.insert(market_id.to_owned(), spec);
            rows.push(ReplayMinuteRow {
                ts_ms: ts0,
                market_id: market_id.to_owned(),
                yes_price: Some(yes_t0),
                no_price: Some(1.0 - yes_t0),
            });
            rows.push(ReplayMinuteRow {
                ts_ms: ts1,
                market_id: market_id.to_owned(),
                yes_price: Some(yes_t1),
                no_price: Some(1.0 - yes_t1),
            });
            rows.push(ReplayMinuteRow {
                ts_ms: ts2,
                market_id: market_id.to_owned(),
                yes_price: Some(yes_t1),
                no_price: Some(1.0 - yes_t1),
            });
        }

        let dataset = ReplayDataset {
            metadata: HashMap::new(),
            markets,
            rows,
            cex_close_by_ts: HashMap::from([(ts0, 70_250.0), (ts1, 70_300.0), (ts2, 70_320.0)]),
            unique_minutes: 3,
            poly_observation_count: 18,
            sanitization: ReplaySanitizationDiagnostics::default(),
        };
        let config = sample_replay_config(1.0, 2.0, 10.0, 2.0, 10.0);
        let trade_log = event_markets
            .into_iter()
            .enumerate()
            .map(
                |(idx, (market_id, _, yes_t0, yes_t1, shares, cex_qty))| ReplayTradeRow {
                    trade_id: idx + 1,
                    strategy: "arb".to_owned(),
                    entry_time: ts0 / 1000,
                    event_id: event_id.clone(),
                    market_id: market_id.to_owned(),
                    market_slug: format!("mkt-{market_id}"),
                    exit_time: (ONE_MINUTE_MS * 3) / 1000,
                    direction: "yes_signal basis reverted inside exit band".to_owned(),
                    entry_poly_price: yes_t0,
                    exit_poly_price: yes_t1,
                    entry_cex_price: 70_250.0,
                    exit_cex_price: 70_300.0,
                    poly_shares: shares,
                    cex_qty,
                    gross_pnl: 0.0,
                    fees: 0.0,
                    slippage: 0.0,
                    gas: 0.0,
                    funding_cost: 0.0,
                    net_pnl: 0.0,
                    holding_bars: 3,
                    z_score_at_entry: -2.0,
                    basis_bps_at_entry: 400.0,
                    delta_at_entry: 0.001,
                },
            )
            .collect::<Vec<_>>();
        let equity_curve = vec![
            EquityPoint {
                ts_ms: ts0,
                equity: 100_000.0,
                cash: 90_000.0,
                available_cash: 90_000.0,
                unrealized_pnl: 0.0,
                gross_exposure: 0.0,
                reserved_margin: 0.0,
                capital_in_use: 0.0,
                open_positions: 3,
            },
            EquityPoint {
                ts_ms: ts1,
                equity: 125_000.0,
                cash: 90_500.0,
                available_cash: 90_500.0,
                unrealized_pnl: 24_500.0,
                gross_exposure: 0.0,
                reserved_margin: 0.0,
                capital_in_use: 0.0,
                open_positions: 3,
            },
            EquityPoint {
                ts_ms: ts2,
                equity: 125_100.0,
                cash: 90_550.0,
                available_cash: 90_550.0,
                unrealized_pnl: 24_550.0,
                gross_exposure: 0.0,
                reserved_margin: 0.0,
                capital_in_use: 0.0,
                open_positions: 3,
            },
        ];

        let diagnostics = detect_replay_anomalies(&dataset, &config, &equity_curve, &trade_log);

        assert_eq!(diagnostics.anomaly_count, 1);
        let anomaly = diagnostics
            .top_anomalies
            .first()
            .expect("expected one anomaly");
        assert_eq!(anomaly.start_ts_ms, ts0);
        assert_eq!(anomaly.end_ts_ms, ts1);
        assert!(anomaly
            .reason_codes
            .iter()
            .any(|code| code == "mark_dominated_jump"));
        assert!(anomaly
            .reason_codes
            .iter()
            .any(|code| code == "sync_probability_reset"));
        assert!(
            anomaly.mark_change_usd > anomaly.fill_cash_change_usd.abs(),
            "expected mark contribution to dominate fill cash"
        );
        assert_eq!(
            anomaly.sync_reset_events,
            vec![ReplayAnomalousEvent {
                event_id,
                market_count: 3,
            }]
        );
        assert_eq!(anomaly.top_markets.len(), 3);
        assert_eq!(anomaly.top_markets[0].market_id, "1559492");
        assert!(anomaly.top_markets[0].total_contribution_usd > 9_000.0);
        assert_eq!(
            anomaly.top_markets[0].market_question.as_deref(),
            Some("Will the price of Bitcoin be above $80,000 on March 18?")
        );
    }

    #[test]
    fn sanitize_sync_reset_plateau_nulls_event_wide_neutral_rows() {
        let reset_event_id = "evt-reset".to_owned();
        let stable_event_id = "evt-stable".to_owned();
        let ts0 = 0_u64;
        let ts1 = ONE_MINUTE_MS;
        let ts2 = ONE_MINUTE_MS * 2;
        let ts3 = ONE_MINUTE_MS * 3;
        let impacted = [
            (
                "1559492",
                "Will the price of Bitcoin be above $80,000 on March 18?",
                0.045,
                0.500,
                0.047,
            ),
            (
                "1559402",
                "Will the price of Bitcoin be between $62,000 and $64,000 on March 18?",
                0.050,
                0.495,
                0.052,
            ),
            (
                "1559400",
                "Will the price of Bitcoin be less than $62,000 on March 18?",
                0.065,
                0.495,
                0.068,
            ),
        ];
        let stable_market = (
            "2559400",
            "Will the price of Bitcoin be above $90,000 on March 18?",
        );

        let mut markets = HashMap::new();
        let mut rows = Vec::new();
        for (market_id, question, yes_t0, yes_neutral, yes_t3) in impacted {
            markets.insert(
                market_id.to_owned(),
                replay_spec_for_question(&reset_event_id, market_id, question),
            );
            rows.push(ReplayMinuteRow {
                ts_ms: ts0,
                market_id: market_id.to_owned(),
                yes_price: Some(yes_t0),
                no_price: Some(1.0 - yes_t0),
            });
            rows.push(ReplayMinuteRow {
                ts_ms: ts1,
                market_id: market_id.to_owned(),
                yes_price: Some(yes_neutral),
                no_price: Some(1.0 - yes_neutral),
            });
            rows.push(ReplayMinuteRow {
                ts_ms: ts2,
                market_id: market_id.to_owned(),
                yes_price: Some(yes_neutral),
                no_price: Some(1.0 - yes_neutral),
            });
            rows.push(ReplayMinuteRow {
                ts_ms: ts3,
                market_id: market_id.to_owned(),
                yes_price: Some(yes_t3),
                no_price: Some(1.0 - yes_t3),
            });
        }

        markets.insert(
            stable_market.0.to_owned(),
            replay_spec_for_question(&stable_event_id, stable_market.0, stable_market.1),
        );
        rows.push(ReplayMinuteRow {
            ts_ms: ts0,
            market_id: stable_market.0.to_owned(),
            yes_price: Some(0.48),
            no_price: Some(0.52),
        });
        rows.push(ReplayMinuteRow {
            ts_ms: ts1,
            market_id: stable_market.0.to_owned(),
            yes_price: Some(0.50),
            no_price: Some(0.50),
        });
        rows.push(ReplayMinuteRow {
            ts_ms: ts2,
            market_id: stable_market.0.to_owned(),
            yes_price: Some(0.50),
            no_price: Some(0.50),
        });
        rows.push(ReplayMinuteRow {
            ts_ms: ts3,
            market_id: stable_market.0.to_owned(),
            yes_price: Some(0.51),
            no_price: Some(0.49),
        });

        let outcome = sanitize_sync_neutral_resets(&markets, rows);

        assert_eq!(outcome.diagnostics.sync_reset_windows.len(), 1);
        assert_eq!(
            outcome.diagnostics.sync_reset_windows[0],
            ReplaySanitizedSyncResetWindow {
                event_id: reset_event_id,
                start_ts_ms: ts1,
                end_ts_ms: ts2,
                trigger_market_count: 3,
                sanitized_market_count: 3,
            }
        );
        assert_eq!(outcome.diagnostics.sanitized_market_minutes, 6);
        assert_eq!(outcome.diagnostics.sanitized_poly_observations, 12);

        for row in outcome
            .rows
            .iter()
            .filter(|row| row.ts_ms == ts1 || row.ts_ms == ts2)
        {
            if row.market_id == stable_market.0 {
                assert_eq!(row.yes_price, Some(0.50));
                assert_eq!(row.no_price, Some(0.50));
            } else {
                assert_eq!(row.yes_price, None);
                assert_eq!(row.no_price, None);
            }
        }
        for row in outcome
            .rows
            .iter()
            .filter(|row| row.ts_ms == ts0 || row.ts_ms == ts3)
        {
            assert!(row.yes_price.is_some());
            assert!(row.no_price.is_some());
        }
    }

    #[test]
    fn sanitize_sync_reset_single_minute_spike_nulls_event_wide_neutral_rows() {
        let reset_event_id = "evt-reset".to_owned();
        let ts0 = 0_u64;
        let ts1 = ONE_MINUTE_MS;
        let ts2 = ONE_MINUTE_MS * 2;
        let impacted = [
            (
                "1559492",
                "Will the price of Bitcoin be above $80,000 on March 18?",
                0.045,
                0.500,
                0.047,
            ),
            (
                "1559402",
                "Will the price of Bitcoin be between $62,000 and $64,000 on March 18?",
                0.050,
                0.495,
                0.052,
            ),
            (
                "1559400",
                "Will the price of Bitcoin be less than $62,000 on March 18?",
                0.065,
                0.495,
                0.068,
            ),
        ];

        let mut markets = HashMap::new();
        let mut rows = Vec::new();
        for (market_id, question, yes_t0, yes_t1, yes_t2) in impacted {
            markets.insert(
                market_id.to_owned(),
                replay_spec_for_question(&reset_event_id, market_id, question),
            );
            rows.push(ReplayMinuteRow {
                ts_ms: ts0,
                market_id: market_id.to_owned(),
                yes_price: Some(yes_t0),
                no_price: Some(1.0 - yes_t0),
            });
            rows.push(ReplayMinuteRow {
                ts_ms: ts1,
                market_id: market_id.to_owned(),
                yes_price: Some(yes_t1),
                no_price: Some(1.0 - yes_t1),
            });
            rows.push(ReplayMinuteRow {
                ts_ms: ts2,
                market_id: market_id.to_owned(),
                yes_price: Some(yes_t2),
                no_price: Some(1.0 - yes_t2),
            });
        }

        let outcome = sanitize_sync_neutral_resets(&markets, rows);

        assert_eq!(outcome.diagnostics.sync_reset_windows.len(), 1);
        assert_eq!(
            outcome.diagnostics.sync_reset_windows[0],
            ReplaySanitizedSyncResetWindow {
                event_id: reset_event_id,
                start_ts_ms: ts1,
                end_ts_ms: ts1,
                trigger_market_count: 3,
                sanitized_market_count: 3,
            }
        );
        assert_eq!(outcome.diagnostics.sanitized_market_minutes, 3);
        assert_eq!(outcome.diagnostics.sanitized_poly_observations, 6);

        for row in &outcome.rows {
            if row.ts_ms == ts1 {
                assert_eq!(row.yes_price, None);
                assert_eq!(row.no_price, None);
            } else {
                assert!(row.yes_price.is_some());
                assert!(row.no_price.is_some());
            }
        }
    }

    #[test]
    fn sanitize_sync_reset_single_minute_template_spike_nulls_event_wide_rows() {
        let reset_event_id = "evt-reset-template".to_owned();
        let ts0 = 0_u64;
        let ts1 = ONE_MINUTE_MS;
        let ts2 = ONE_MINUTE_MS * 2;
        let impacted = [
            (
                "1414993",
                "Will the price of Solana be less than $40 on March 1?",
                0.016,
                0.3445,
                0.016,
            ),
            (
                "1415025",
                "Will the price of Solana be between $120 and $130 on March 1?",
                0.0155,
                0.3445,
                0.0155,
            ),
            (
                "1415016",
                "Will the price of Solana be between $110 and $120 on March 1?",
                0.0205,
                0.3395,
                0.0205,
            ),
        ];

        let mut markets = HashMap::new();
        let mut rows = Vec::new();
        for (market_id, question, yes_t0, yes_t1, yes_t2) in impacted {
            markets.insert(
                market_id.to_owned(),
                replay_spec_for_question(&reset_event_id, market_id, question),
            );
            rows.push(ReplayMinuteRow {
                ts_ms: ts0,
                market_id: market_id.to_owned(),
                yes_price: Some(yes_t0),
                no_price: Some(1.0 - yes_t0),
            });
            rows.push(ReplayMinuteRow {
                ts_ms: ts1,
                market_id: market_id.to_owned(),
                yes_price: Some(yes_t1),
                no_price: Some(1.0 - yes_t1),
            });
            rows.push(ReplayMinuteRow {
                ts_ms: ts2,
                market_id: market_id.to_owned(),
                yes_price: Some(yes_t2),
                no_price: Some(1.0 - yes_t2),
            });
        }

        let outcome = sanitize_sync_neutral_resets(&markets, rows);

        assert_eq!(outcome.diagnostics.sync_reset_windows.len(), 1);
        assert_eq!(
            outcome.diagnostics.sync_reset_windows[0],
            ReplaySanitizedSyncResetWindow {
                event_id: reset_event_id,
                start_ts_ms: ts1,
                end_ts_ms: ts1,
                trigger_market_count: 3,
                sanitized_market_count: 3,
            }
        );
        assert_eq!(outcome.diagnostics.sanitized_market_minutes, 3);
        assert_eq!(outcome.diagnostics.sanitized_poly_observations, 6);

        for row in &outcome.rows {
            if row.ts_ms == ts1 {
                assert_eq!(row.yes_price, None);
                assert_eq!(row.no_price, None);
            } else {
                assert!(row.yes_price.is_some());
                assert!(row.no_price.is_some());
            }
        }
    }

    #[test]
    fn sanitize_sync_reset_template_plateau_nulls_event_wide_rows_until_exit() {
        let reset_event_id = "evt-reset-template-plateau".to_owned();
        let ts0 = 0_u64;
        let ts1 = ONE_MINUTE_MS;
        let ts2 = ONE_MINUTE_MS * 2;
        let ts3 = ONE_MINUTE_MS * 3;
        let impacted = [
            (
                "1415039",
                "Will the price of XRP be less than $0.90 on March 1?",
                0.0160,
                0.3445,
                0.4695,
                0.0160,
            ),
            (
                "1415041",
                "Will the price of XRP be between $0.90 and $1.00 on March 1?",
                0.0165,
                0.3445,
                0.4645,
                0.0165,
            ),
            (
                "1408395",
                "Will the price of XRP be between $1.00 and $1.10 on February 28?",
                0.0295,
                0.3475,
                0.4650,
                0.0290,
            ),
        ];

        let mut markets = HashMap::new();
        let mut rows = Vec::new();
        for (market_id, question, yes_t0, yes_t1, yes_t2, yes_t3) in impacted {
            markets.insert(
                market_id.to_owned(),
                replay_spec_for_question(&reset_event_id, market_id, question),
            );
            for (ts_ms, yes_price) in [(ts0, yes_t0), (ts1, yes_t1), (ts2, yes_t2), (ts3, yes_t3)] {
                rows.push(ReplayMinuteRow {
                    ts_ms,
                    market_id: market_id.to_owned(),
                    yes_price: Some(yes_price),
                    no_price: Some(1.0 - yes_price),
                });
            }
        }

        let outcome = sanitize_sync_neutral_resets(&markets, rows);

        assert_eq!(outcome.diagnostics.sync_reset_windows.len(), 1);
        assert_eq!(
            outcome.diagnostics.sync_reset_windows[0],
            ReplaySanitizedSyncResetWindow {
                event_id: reset_event_id,
                start_ts_ms: ts1,
                end_ts_ms: ts2,
                trigger_market_count: 3,
                sanitized_market_count: 3,
            }
        );
        assert_eq!(outcome.diagnostics.sanitized_market_minutes, 6);
        assert_eq!(outcome.diagnostics.sanitized_poly_observations, 12);

        for row in &outcome.rows {
            if row.ts_ms == ts1 || row.ts_ms == ts2 {
                assert_eq!(row.yes_price, None);
                assert_eq!(row.no_price, None);
            } else {
                assert!(row.yes_price.is_some());
                assert!(row.no_price.is_some());
            }
        }
    }

    #[test]
    fn sanitize_sync_reset_two_trigger_markets_still_nulls_event_wide_interior_template_reset() {
        let reset_event_id = "evt-reset-template-adjacent".to_owned();
        let ts0 = 0_u64;
        let ts1 = ONE_MINUTE_MS;
        let ts2 = ONE_MINUTE_MS * 2;
        let impacted = [
            (
                "1403023",
                "Will the price of Solana be between $60 and $70 on February 27?",
                0.0580,
                0.3495,
                0.0530,
            ),
            (
                "1403024",
                "Will the price of Solana be between $70 and $80 on February 27?",
                0.4500,
                0.4600,
                0.4500,
            ),
            (
                "1403025",
                "Will the price of Solana be between $80 and $90 on February 27?",
                0.4150,
                0.4550,
                0.4150,
            ),
            (
                "1403026",
                "Will the price of Solana be between $90 and $100 on February 27?",
                0.0850,
                0.3700,
                0.0700,
            ),
        ];
        let unaffected = (
            "1403027",
            "Will the price of Solana be between $100 and $110 on February 27?",
            0.0135,
            0.0135,
            0.0135,
        );

        let mut markets = HashMap::new();
        let mut rows = Vec::new();
        for (market_id, question, yes_t0, yes_t1, yes_t2) in
            impacted.into_iter().chain([unaffected])
        {
            markets.insert(
                market_id.to_owned(),
                replay_spec_for_question(&reset_event_id, market_id, question),
            );
            for (ts_ms, yes_price) in [(ts0, yes_t0), (ts1, yes_t1), (ts2, yes_t2)] {
                rows.push(ReplayMinuteRow {
                    ts_ms,
                    market_id: market_id.to_owned(),
                    yes_price: Some(yes_price),
                    no_price: Some(1.0 - yes_price),
                });
            }
        }
        let outcome = sanitize_sync_neutral_resets(&markets, rows);

        assert_eq!(outcome.diagnostics.sync_reset_windows.len(), 1);
        assert_eq!(
            outcome.diagnostics.sync_reset_windows[0],
            ReplaySanitizedSyncResetWindow {
                event_id: reset_event_id,
                start_ts_ms: ts1,
                end_ts_ms: ts1,
                trigger_market_count: 2,
                sanitized_market_count: 4,
            }
        );
        assert_eq!(outcome.diagnostics.sanitized_market_minutes, 4);
        assert_eq!(outcome.diagnostics.sanitized_poly_observations, 8);

        for row in &outcome.rows {
            if row.ts_ms == ts1 && row.market_id != unaffected.0 {
                assert_eq!(row.yes_price, None);
                assert_eq!(row.no_price, None);
            } else {
                assert!(row.yes_price.is_some());
                assert!(row.no_price.is_some());
            }
        }
    }

    #[test]
    fn sanitize_sync_reset_adjacent_pair_template_spike_nulls_two_market_pair() {
        let reset_event_id = "evt-reset-pair".to_owned();
        let ts0 = 0_u64;
        let ts1 = ONE_MINUTE_MS;
        let ts2 = ONE_MINUTE_MS * 2;
        let impacted = [
            (
                "1415039",
                "Will the price of XRP be less than $0.90 on March 1?",
                0.0160,
                0.3400,
                0.0160,
            ),
            (
                "1415041",
                "Will the price of XRP be between $0.90 and $1.00 on March 1?",
                0.0165,
                0.3445,
                0.0165,
            ),
        ];
        let unaffected = (
            "1415044",
            "Will the price of XRP be between $1.00 and $1.10 on March 1?",
            0.0300,
            0.0300,
            0.0300,
        );

        let mut markets = HashMap::new();
        let mut rows = Vec::new();
        for (market_id, question, yes_t0, yes_t1, yes_t2) in
            impacted.into_iter().chain([unaffected])
        {
            markets.insert(
                market_id.to_owned(),
                replay_spec_for_question(&reset_event_id, market_id, question),
            );
            for (ts_ms, yes_price) in [(ts0, yes_t0), (ts1, yes_t1), (ts2, yes_t2)] {
                rows.push(ReplayMinuteRow {
                    ts_ms,
                    market_id: market_id.to_owned(),
                    yes_price: Some(yes_price),
                    no_price: Some(1.0 - yes_price),
                });
            }
        }
        let pair_spec = markets
            .get_mut("1415041")
            .expect("pair market spec should exist");
        pair_spec.settlement_ts_ms += 5_000;
        pair_spec.market_config.settlement_timestamp += 5;

        let outcome = sanitize_sync_neutral_resets(&markets, rows);

        assert_eq!(outcome.diagnostics.sync_reset_windows.len(), 1);
        assert_eq!(outcome.diagnostics.sanitized_market_minutes, 2);
        assert_eq!(outcome.diagnostics.sanitized_poly_observations, 4);

        for row in &outcome.rows {
            if row.ts_ms == ts1 && row.market_id != unaffected.0 {
                assert_eq!(row.yes_price, None);
                assert_eq!(row.no_price, None);
            } else {
                assert!(row.yes_price.is_some());
                assert!(row.no_price.is_some());
            }
        }
    }

    #[test]
    fn sanitize_sync_reset_complementary_threshold_pair_nulls_cross_event_pair() {
        let above_event_id = "evt-above-threshold".to_owned();
        let below_event_id = "evt-below-threshold".to_owned();
        let ts0 = 0_u64;
        let ts1 = ONE_MINUTE_MS;
        let ts2 = ONE_MINUTE_MS * 2;
        let impacted = [
            (
                &above_event_id,
                "1220909",
                "Will the price of Ethereum be above $2,800 on January 26?",
                0.9715,
                0.5490,
                0.9710,
            ),
            (
                &below_event_id,
                "1220942",
                "Will the price of Ethereum be less than $2,800 on January 26?",
                0.0285,
                0.4570,
                0.0290,
            ),
        ];
        let unaffected = [
            (
                &above_event_id,
                "1220911",
                "Will the price of Ethereum be above $2,900 on January 26?",
                0.9400,
                0.9400,
                0.9400,
            ),
            (
                &below_event_id,
                "1220944",
                "Will the price of Ethereum be between $2,900 and $3,000 on January 26?",
                0.0400,
                0.0400,
                0.0400,
            ),
        ];

        let mut markets = HashMap::new();
        let mut rows = Vec::new();
        for (event_id, market_id, question, yes_t0, yes_t1, yes_t2) in
            impacted.into_iter().chain(unaffected)
        {
            markets.insert(
                market_id.to_owned(),
                replay_spec_for_question(event_id, market_id, question),
            );
            for (ts_ms, yes_price) in [(ts0, yes_t0), (ts1, yes_t1), (ts2, yes_t2)] {
                rows.push(ReplayMinuteRow {
                    ts_ms,
                    market_id: market_id.to_owned(),
                    yes_price: Some(yes_price),
                    no_price: Some(1.0 - yes_price),
                });
            }
        }
        let pair_spec = markets
            .get_mut("1220942")
            .expect("pair market spec should exist");
        pair_spec.settlement_ts_ms += 90 * ONE_MINUTE_MS;
        pair_spec.market_config.settlement_timestamp += 90 * 60;

        let outcome = sanitize_sync_neutral_resets(&markets, rows);

        assert_eq!(outcome.diagnostics.sync_reset_windows.len(), 1);
        assert_eq!(outcome.diagnostics.sanitized_market_minutes, 2);
        assert_eq!(outcome.diagnostics.sanitized_poly_observations, 4);

        for row in &outcome.rows {
            if row.ts_ms == ts1 && (row.market_id == "1220909" || row.market_id == "1220942") {
                assert_eq!(row.yes_price, None);
                assert_eq!(row.no_price, None);
            } else {
                assert!(row.yes_price.is_some());
                assert!(row.no_price.is_some());
            }
        }
    }

    #[test]
    fn detects_sync_probability_reset_anomaly_from_single_minute_mark_spike() {
        let event_id = "evt-reset".to_owned();
        let event_markets = [
            (
                "1559492",
                "Will the price of Bitcoin be above $80,000 on March 18?",
                0.045,
                0.500,
                0.047,
                22_200.0222,
                0.538,
            ),
            (
                "1559402",
                "Will the price of Bitcoin be between $62,000 and $64,000 on March 18?",
                0.050,
                0.495,
                0.052,
                19_980.0200,
                0.174,
            ),
            (
                "1559400",
                "Will the price of Bitcoin be less than $62,000 on March 18?",
                0.065,
                0.495,
                0.068,
                15_369.2461,
                0.441,
            ),
        ];
        let mut markets = HashMap::new();
        let mut rows = Vec::new();
        let ts0 = 0_u64;
        let ts1 = ONE_MINUTE_MS;
        let ts2 = ONE_MINUTE_MS * 2;
        for (market_id, question, yes_t0, yes_t1, yes_t2, _, _) in event_markets {
            let spec = ReplayMarketSpec {
                event_id: event_id.clone(),
                market_id: market_id.to_owned(),
                market_slug: format!("mkt-{market_id}"),
                symbol: Symbol::new(format!("mkt-{market_id}")),
                market_config: MarketConfig {
                    symbol: Symbol::new(format!("mkt-{market_id}")),
                    poly_ids: PolymarketIds {
                        condition_id: format!("condition-{market_id}"),
                        yes_token_id: format!("yes-{market_id}"),
                        no_token_id: format!("no-{market_id}"),
                    },
                    market_question: Some(question.to_owned()),
                    market_rule: Some(parse_market_rule(question).expect("market rule")),
                    cex_symbol: "BTCUSDT".to_owned(),
                    hedge_exchange: Exchange::Binance,
                    strike_price: None,
                    settlement_timestamp: 10_000,
                    min_tick_size: Price(Decimal::new(1, 2)),
                    neg_risk: false,
                    cex_price_tick: Decimal::new(1, 1),
                    cex_qty_step: Decimal::new(1, 3),
                    cex_contract_multiplier: Decimal::ONE,
                },
                settlement_ts_ms: 10_000 * 1000,
                yes_payout: None,
                no_payout: None,
            };
            markets.insert(market_id.to_owned(), spec);
            rows.push(ReplayMinuteRow {
                ts_ms: ts0,
                market_id: market_id.to_owned(),
                yes_price: Some(yes_t0),
                no_price: Some(1.0 - yes_t0),
            });
            rows.push(ReplayMinuteRow {
                ts_ms: ts1,
                market_id: market_id.to_owned(),
                yes_price: Some(yes_t1),
                no_price: Some(1.0 - yes_t1),
            });
            rows.push(ReplayMinuteRow {
                ts_ms: ts2,
                market_id: market_id.to_owned(),
                yes_price: Some(yes_t2),
                no_price: Some(1.0 - yes_t2),
            });
        }

        let dataset = ReplayDataset {
            metadata: HashMap::new(),
            markets,
            rows,
            cex_close_by_ts: HashMap::from([(ts0, 70_250.0), (ts1, 70_300.0), (ts2, 70_320.0)]),
            unique_minutes: 3,
            poly_observation_count: 18,
            sanitization: ReplaySanitizationDiagnostics::default(),
        };
        let config = sample_replay_config(1.0, 2.0, 10.0, 2.0, 10.0);
        let trade_log = event_markets
            .into_iter()
            .enumerate()
            .map(
                |(idx, (market_id, _, yes_t0, yes_t1, _yes_t2, shares, cex_qty))| ReplayTradeRow {
                    trade_id: idx + 1,
                    strategy: "arb".to_owned(),
                    entry_time: ts0 / 1000,
                    event_id: event_id.clone(),
                    market_id: market_id.to_owned(),
                    market_slug: format!("mkt-{market_id}"),
                    exit_time: (ONE_MINUTE_MS * 3) / 1000,
                    direction: "yes_signal basis reverted inside exit band".to_owned(),
                    entry_poly_price: yes_t0,
                    exit_poly_price: yes_t1,
                    entry_cex_price: 70_250.0,
                    exit_cex_price: 70_300.0,
                    poly_shares: shares,
                    cex_qty,
                    gross_pnl: 0.0,
                    fees: 0.0,
                    slippage: 0.0,
                    gas: 0.0,
                    funding_cost: 0.0,
                    net_pnl: 0.0,
                    holding_bars: 3,
                    z_score_at_entry: -2.0,
                    basis_bps_at_entry: 400.0,
                    delta_at_entry: 0.001,
                },
            )
            .collect::<Vec<_>>();
        let equity_curve = vec![
            EquityPoint {
                ts_ms: ts0,
                equity: 100_000.0,
                cash: 90_000.0,
                available_cash: 90_000.0,
                unrealized_pnl: 0.0,
                gross_exposure: 0.0,
                reserved_margin: 0.0,
                capital_in_use: 0.0,
                open_positions: 3,
            },
            EquityPoint {
                ts_ms: ts1,
                equity: 125_000.0,
                cash: 90_500.0,
                available_cash: 90_500.0,
                unrealized_pnl: 24_500.0,
                gross_exposure: 0.0,
                reserved_margin: 0.0,
                capital_in_use: 0.0,
                open_positions: 3,
            },
            EquityPoint {
                ts_ms: ts2,
                equity: 100_100.0,
                cash: 90_550.0,
                available_cash: 90_550.0,
                unrealized_pnl: 24_550.0,
                gross_exposure: 0.0,
                reserved_margin: 0.0,
                capital_in_use: 0.0,
                open_positions: 3,
            },
        ];

        let diagnostics = detect_replay_anomalies(&dataset, &config, &equity_curve, &trade_log);
        let anomaly = diagnostics
            .top_anomalies
            .first()
            .expect("expected one anomaly");
        assert!(anomaly
            .reason_codes
            .iter()
            .any(|code| code == "sync_probability_reset"));
        assert_eq!(
            anomaly.sync_reset_events,
            vec![ReplayAnomalousEvent {
                event_id,
                market_count: 3,
            }]
        );
    }

    #[test]
    fn detects_sync_probability_reset_anomaly_from_single_minute_template_spike() {
        let event_id = "evt-reset-template".to_owned();
        let event_markets = [
            (
                "1414993",
                "Will the price of Solana be less than $40 on March 1?",
                0.016,
                0.3445,
                0.016,
                20_000.0,
                0.250,
            ),
            (
                "1415025",
                "Will the price of Solana be between $120 and $130 on March 1?",
                0.0155,
                0.3445,
                0.0155,
                18_000.0,
                0.220,
            ),
            (
                "1415016",
                "Will the price of Solana be between $110 and $120 on March 1?",
                0.0205,
                0.3395,
                0.0205,
                15_000.0,
                0.180,
            ),
        ];
        let mut markets = HashMap::new();
        let mut rows = Vec::new();
        let ts0 = 0_u64;
        let ts1 = ONE_MINUTE_MS;
        let ts2 = ONE_MINUTE_MS * 2;
        for (market_id, question, yes_t0, yes_t1, yes_t2, _, _) in event_markets {
            let spec = ReplayMarketSpec {
                event_id: event_id.clone(),
                market_id: market_id.to_owned(),
                market_slug: format!("mkt-{market_id}"),
                symbol: Symbol::new(format!("mkt-{market_id}")),
                market_config: MarketConfig {
                    symbol: Symbol::new(format!("mkt-{market_id}")),
                    poly_ids: PolymarketIds {
                        condition_id: format!("condition-{market_id}"),
                        yes_token_id: format!("yes-{market_id}"),
                        no_token_id: format!("no-{market_id}"),
                    },
                    market_question: Some(question.to_owned()),
                    market_rule: Some(parse_market_rule(question).expect("market rule")),
                    cex_symbol: "SOLUSDT".to_owned(),
                    hedge_exchange: Exchange::Binance,
                    strike_price: None,
                    settlement_timestamp: 10_000,
                    min_tick_size: Price(Decimal::new(1, 2)),
                    neg_risk: false,
                    cex_price_tick: Decimal::new(1, 1),
                    cex_qty_step: Decimal::new(1, 3),
                    cex_contract_multiplier: Decimal::ONE,
                },
                settlement_ts_ms: 10_000 * 1000,
                yes_payout: None,
                no_payout: None,
            };
            markets.insert(market_id.to_owned(), spec);
            rows.push(ReplayMinuteRow {
                ts_ms: ts0,
                market_id: market_id.to_owned(),
                yes_price: Some(yes_t0),
                no_price: Some(1.0 - yes_t0),
            });
            rows.push(ReplayMinuteRow {
                ts_ms: ts1,
                market_id: market_id.to_owned(),
                yes_price: Some(yes_t1),
                no_price: Some(1.0 - yes_t1),
            });
            rows.push(ReplayMinuteRow {
                ts_ms: ts2,
                market_id: market_id.to_owned(),
                yes_price: Some(yes_t2),
                no_price: Some(1.0 - yes_t2),
            });
        }

        let dataset = ReplayDataset {
            metadata: HashMap::new(),
            markets,
            rows,
            cex_close_by_ts: HashMap::from([(ts0, 80.15), (ts1, 80.09), (ts2, 79.81)]),
            unique_minutes: 3,
            poly_observation_count: 18,
            sanitization: ReplaySanitizationDiagnostics::default(),
        };
        let config = sample_replay_config(1.0, 2.0, 10.0, 2.0, 10.0);
        let trade_log = event_markets
            .into_iter()
            .enumerate()
            .map(
                |(idx, (market_id, _, yes_t0, yes_t1, _yes_t2, shares, cex_qty))| ReplayTradeRow {
                    trade_id: idx + 1,
                    strategy: "arb".to_owned(),
                    entry_time: ts0 / 1000,
                    event_id: event_id.clone(),
                    market_id: market_id.to_owned(),
                    market_slug: format!("mkt-{market_id}"),
                    exit_time: (ONE_MINUTE_MS * 3) / 1000,
                    direction: "yes_signal basis reverted inside exit band".to_owned(),
                    entry_poly_price: yes_t0,
                    exit_poly_price: yes_t1,
                    entry_cex_price: 80.15,
                    exit_cex_price: 80.09,
                    poly_shares: shares,
                    cex_qty,
                    gross_pnl: 0.0,
                    fees: 0.0,
                    slippage: 0.0,
                    gas: 0.0,
                    funding_cost: 0.0,
                    net_pnl: 0.0,
                    holding_bars: 3,
                    z_score_at_entry: -2.0,
                    basis_bps_at_entry: 400.0,
                    delta_at_entry: 0.001,
                },
            )
            .collect::<Vec<_>>();
        let equity_curve = vec![
            EquityPoint {
                ts_ms: ts0,
                equity: 100_000.0,
                cash: 90_000.0,
                available_cash: 90_000.0,
                unrealized_pnl: 0.0,
                gross_exposure: 0.0,
                reserved_margin: 0.0,
                capital_in_use: 0.0,
                open_positions: 3,
            },
            EquityPoint {
                ts_ms: ts1,
                equity: 124_000.0,
                cash: 90_400.0,
                available_cash: 90_400.0,
                unrealized_pnl: 23_600.0,
                gross_exposure: 0.0,
                reserved_margin: 0.0,
                capital_in_use: 0.0,
                open_positions: 3,
            },
            EquityPoint {
                ts_ms: ts2,
                equity: 100_200.0,
                cash: 90_450.0,
                available_cash: 90_450.0,
                unrealized_pnl: 23_650.0,
                gross_exposure: 0.0,
                reserved_margin: 0.0,
                capital_in_use: 0.0,
                open_positions: 3,
            },
        ];

        let diagnostics = detect_replay_anomalies(&dataset, &config, &equity_curve, &trade_log);
        let anomaly = diagnostics
            .top_anomalies
            .first()
            .expect("expected one anomaly");
        assert!(anomaly
            .reason_codes
            .iter()
            .any(|code| code == "sync_probability_reset"));
        assert_eq!(
            anomaly.sync_reset_events,
            vec![ReplayAnomalousEvent {
                event_id,
                market_count: 3,
            }]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "diagnostic: uses local replay fixture database"]
    async fn diagnose_576713_tail_window_planner_rejections() {
        let diagnostic =
            diagnose_planner_rejections_for_window("576713", "2025-08-22", "2025-08-23").await;
        println!("{diagnostic:#?}");
        assert!(diagnostic.signal_count > 0);
        assert!(diagnostic.planner_rejections > 0);
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "diagnostic: uses local replay fixture database"]
    async fn diagnose_1385950_tail_window_planner_rejections() {
        let diagnostic =
            diagnose_planner_rejections_for_window("1385950", "2026-02-22", "2026-02-24").await;
        println!("{diagnostic:#?}");
        assert!(diagnostic.signal_count > 0);
        assert!(diagnostic.planner_rejections > 0);
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "diagnostic: print raw polymarket price rows around 2026-03-11 spike"]
    async fn diagnose_20260311_spike_raw_polymarket_rows() {
        let db_path = replay_fixture_db_path();
        let db_path_str = db_path.to_string_lossy().to_string();
        let rows = load_spike_raw_price_rows(&db_path_str, 1_773_246_000_000, 1_773_246_120_000)
            .expect("load raw spike rows");
        assert!(!rows.is_empty(), "expected raw rows around spike");
        for row in rows {
            println!(
                "RAW ts_ms={} event_id={} market_id={} side={} token_id={} poly_price={:.6} question={}",
                row.ts_ms,
                row.event_id,
                row.market_id,
                row.token_side,
                row.token_id,
                row.poly_price,
                row.market_question,
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "diagnostic: print grouped replay rows around 2026-03-11 spike"]
    async fn diagnose_20260311_spike_grouped_rows() {
        let db_path = replay_fixture_db_path();
        let db_path_str = db_path.to_string_lossy().to_string();
        print_spike_grouped_rows(&db_path_str, 1_773_246_000_000, 1_773_246_120_000)
            .expect("print grouped spike rows");
    }
}
