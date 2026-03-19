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
    AlphaEngine, ArbSignalAction, Exchange, InstrumentKind, MarketConfig, MarketDataEvent,
    MarketRule, MarketRuleKind, OrderSide, PolymarketIds, Price, Symbol, TokenSide, UsdNotional,
};
use polyalpha_engine::{SimpleAlphaEngine, SimpleEngineConfig};

use crate::args::RustStressPreset;

const DEFAULT_POSITION_NOTIONAL_PCT: f64 = 0.01;
const RESOLUTION_EPSILON: f64 = 1e-4;
const ONE_MINUTE_MS: u64 = 60_000;
const MONEY_LITERAL_RE: &str = r"([0-9][0-9,]*(?:\.\d+)?(?:[kmb])?)\b";

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
    pub report_json: Option<String>,
    pub equity_csv: Option<String>,
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
}

#[derive(Clone, Debug)]
struct ReplayMarketSpec {
    event_id: String,
    market_id: String,
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
}

#[derive(Clone, Debug)]
struct PendingReplaySignal {
    signal: polyalpha_core::ArbSignalEvent,
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
    entries_by_market: HashMap<String, usize>,
    #[serde(skip_serializing)]
    net_pnl_by_market: HashMap<String, f64>,
}

#[derive(Clone, Debug, Serialize)]
struct MarketSignalDebugRow {
    market_id: String,
    signal_count: usize,
    rejected_capital_count: usize,
    entry_count: usize,
    net_pnl: f64,
}

#[derive(Clone, Debug, Serialize)]
struct RustReplaySummary {
    db_path: String,
    stress: ExecutionStressConfig,
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
    replayed_grouped_rows: usize,
    replayed_poly_observations: usize,
    open_positions_end: usize,
    start_ts_ms: Option<u64>,
    end_ts_ms: Option<u64>,
    top_signal_markets: Vec<MarketSignalDebugRow>,
    top_entry_markets: Vec<MarketSignalDebugRow>,
    market_debug_rows: Vec<MarketSignalDebugRow>,
}

#[derive(Clone, Debug, Serialize)]
struct RustReplayReport {
    generated_at_utc: String,
    build_metadata: HashMap<String, String>,
    summary: RustReplaySummary,
}

#[derive(Clone, Debug, Serialize)]
struct RustStressReport {
    generated_at_utc: String,
    build_metadata: HashMap<String, String>,
    results: Vec<RustReplaySummary>,
}

#[derive(Clone, Debug)]
struct ReplayRunResult {
    summary: RustReplaySummary,
    equity_curve: Vec<EquityPoint>,
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
        ExecutionStressConfig {
            label: "custom".to_owned(),
            poly_fee_bps: args.poly_fee_bps,
            poly_slippage_bps: args.poly_slippage_bps,
            cex_fee_bps: args.cex_fee_bps,
            cex_slippage_bps: args.cex_slippage_bps,
            entry_fill_ratio: args.entry_fill_ratio,
        },
    )?;
    let run = run_single_replay(&dataset, &config).await?;
    print_replay_summary(&run.summary);

    if let Some(path) = args.report_json.as_deref() {
        write_json(
            path,
            &RustReplayReport {
                generated_at_utc: Utc::now().to_rfc3339(),
                build_metadata: dataset.metadata.clone(),
                summary: run.summary.clone(),
            },
        )?;
    }
    if let Some(path) = args.equity_csv.as_deref() {
        write_equity_csv(path, &run.equity_curve)?;
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
            preset,
        )?;
        let run = run_single_replay(&dataset, &config).await?;
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
    let markets = load_market_specs(&conn, filter, &market_resolutions)?;
    let rows = load_price_rows(&conn, filter)?;
    let cex_close_by_ts = load_cex_closes(&conn)?;

    if markets.is_empty() {
        bail!("no replay markets matched filter");
    }
    if rows.is_empty() {
        bail!("no grouped polymarket rows matched filter");
    }
    if cex_close_by_ts.is_empty() {
        bail!("no Binance BTC 1m rows found");
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
        let settlement_ts_ms = i64_to_u64(end_ts)?
            .checked_mul(1000)
            .ok_or_else(|| anyhow!("market `{market_id}` settlement timestamp overflow"))?;
        let strike_price = rule.lower_strike.or(rule.upper_strike);
        let market_config = MarketConfig {
            symbol: symbol.clone(),
            poly_ids: PolymarketIds {
                condition_id: condition_id.unwrap_or_else(|| format!("condition-{market_id}")),
                yes_token_id,
                no_token_id,
            },
            market_question: Some(market_question.clone()),
            market_rule: Some(rule.clone()),
            cex_symbol: "BTCUSDT".to_owned(),
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

fn load_cex_closes(conn: &Connection) -> Result<HashMap<u64, f64>> {
    let mut stmt = conn.prepare("select ts_ms, close from binance_btc_1m order by ts_ms asc")?;
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
    };
    let mut engine = SimpleAlphaEngine::with_markets(engine_config, markets);

    let mut runtime = dataset
        .markets
        .keys()
        .map(|market_id| (market_id.clone(), ReplayRuntimeState::default()))
        .collect::<HashMap<_, _>>();
    let mut positions: HashMap<Symbol, ReplayPosition> = HashMap::new();
    let mut equity_curve = Vec::new();
    let mut stats = ReplayStats::default();
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
        let mut pending_other_signals = Vec::new();
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

            for signal in output.arb_signals {
                let pending = PendingReplaySignal {
                    signal,
                    spec: spec.clone(),
                    row: row.clone(),
                    cex_reference_price,
                };
                match pending.signal.action {
                    ArbSignalAction::ClosePosition { .. } => pending_close_signals.push(pending),
                    ArbSignalAction::BasisLong { .. } => pending_entry_signals.push(pending),
                    _ => pending_other_signals.push(pending),
                }
            }
        }

        for pending in pending_close_signals {
            handle_signal(
                pending.signal,
                &pending.spec,
                &pending.row,
                pending.cex_reference_price,
                config,
                &mut positions,
                &mut cash,
                &mut stats,
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
                config,
                &mut positions,
                &mut cash,
                &mut stats,
            )?;
            engine.sync_position_state(
                &symbol,
                positions.get(&symbol).map(|position| position.token_side),
            );
        }

        pending_entry_signals.sort_by(|left, right| {
            let left_z = left
                .signal
                .z_score
                .as_ref()
                .and_then(Decimal::to_f64)
                .unwrap_or(f64::INFINITY);
            let right_z = right
                .signal
                .z_score
                .as_ref()
                .and_then(Decimal::to_f64)
                .unwrap_or(f64::INFINITY);
            left_z
                .partial_cmp(&right_z)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    let left_basis = left
                        .signal
                        .basis_value
                        .as_ref()
                        .and_then(Decimal::to_f64)
                        .unwrap_or_default()
                        .abs();
                    let right_basis = right
                        .signal
                        .basis_value
                        .as_ref()
                        .and_then(Decimal::to_f64)
                        .unwrap_or_default()
                        .abs();
                    right_basis
                        .partial_cmp(&left_basis)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
        });

        for pending in pending_entry_signals {
            handle_signal(
                pending.signal,
                &pending.spec,
                &pending.row,
                pending.cex_reference_price,
                config,
                &mut positions,
                &mut cash,
                &mut stats,
            )?;
            engine.sync_position_state(
                &pending.spec.symbol,
                positions
                    .get(&pending.spec.symbol)
                    .map(|position| position.token_side),
            );
        }

        for pending in pending_other_signals {
            handle_signal(
                pending.signal,
                &pending.spec,
                &pending.row,
                pending.cex_reference_price,
                config,
                &mut positions,
                &mut cash,
                &mut stats,
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
                config,
                &mut positions,
                &mut cash,
                &mut stats,
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
        replayed_grouped_rows: stats.grouped_rows_replayed,
        replayed_poly_observations: stats.poly_observations_replayed,
        open_positions_end: positions.len(),
        start_ts_ms: dataset.rows.first().map(|row| row.ts_ms),
        end_ts_ms: dataset.rows.last().map(|row| row.ts_ms),
        top_signal_markets: top_signal_markets(&stats, 10),
        top_entry_markets: top_entry_markets(&stats, 10),
        market_debug_rows: all_market_debug_rows(&stats),
    };

    Ok(ReplayRunResult {
        summary,
        equity_curve,
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

fn handle_signal(
    signal: polyalpha_core::ArbSignalEvent,
    spec: &ReplayMarketSpec,
    row: &ReplayMinuteRow,
    cex_reference_price: f64,
    config: &ResolvedReplayConfig,
    positions: &mut HashMap<Symbol, ReplayPosition>,
    cash: &mut f64,
    stats: &mut ReplayStats,
) -> Result<()> {
    stats.signals_emitted += 1;
    *stats
        .signals_by_market
        .entry(spec.market_id.clone())
        .or_insert(0) += 1;
    match signal.action {
        ArbSignalAction::BasisLong {
            token_side,
            poly_target_shares,
            cex_side,
            cex_hedge_qty,
            ..
        } => {
            if positions.contains_key(&spec.symbol) {
                stats.signals_ignored_existing_position += 1;
                return Ok(());
            }

            let Some(poly_entry_price) = row.price_for(token_side) else {
                stats.signals_rejected_missing_price += 1;
                return Ok(());
            };

            let shares =
                poly_target_shares.0.to_f64().unwrap_or_default() * config.stress.entry_fill_ratio;
            if shares <= 0.0 || poly_entry_price <= 0.0 || cex_reference_price <= 0.0 {
                stats.signals_rejected_missing_price += 1;
                return Ok(());
            }

            let poly_entry_notional = shares * poly_entry_price;
            let cex_qty_abs =
                cex_hedge_qty.0.to_f64().unwrap_or_default() * config.stress.entry_fill_ratio;
            let cex_qty_btc = if matches!(cex_side, OrderSide::Buy) {
                cex_qty_abs
            } else {
                -cex_qty_abs
            };
            let cex_entry_notional = cex_qty_abs * cex_reference_price;
            let margin_required = cex_entry_notional * config.cex_margin_ratio;
            let (_, capital_in_use) = portfolio_usage(positions);
            let reserved_margin_total = portfolio_usage(positions).0;
            let available_cash = *cash - reserved_margin_total;
            let estimated_entry_costs = trade_cost(
                poly_entry_notional,
                config.stress.poly_fee_bps,
                config.stress.poly_slippage_bps,
            )
            .0 + trade_cost(
                cex_entry_notional,
                config.stress.cex_fee_bps,
                config.stress.cex_slippage_bps,
            )
            .0;
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
            let (poly_costs, poly_fee_paid, poly_slippage_paid) = trade_cost(
                poly_entry_notional,
                config.stress.poly_fee_bps,
                config.stress.poly_slippage_bps,
            );
            *cash -= poly_costs;
            record_costs(
                stats,
                poly_fee_paid,
                poly_slippage_paid,
                true,
                true,
                poly_entry_notional > 0.0,
            );

            let mut round_trip_pnl = -(poly_costs);
            if cex_entry_notional > 0.0 {
                let (cex_costs, cex_fee_paid, cex_slippage_paid) = trade_cost(
                    cex_entry_notional,
                    config.stress.cex_fee_bps,
                    config.stress.cex_slippage_bps,
                );
                *cash -= cex_costs;
                round_trip_pnl -= cex_costs;
                record_costs(stats, cex_fee_paid, cex_slippage_paid, false, true, true);
            }

            positions.insert(
                spec.symbol.clone(),
                ReplayPosition {
                    market_id: spec.market_id.clone(),
                    token_side,
                    shares,
                    poly_entry_notional,
                    last_poly_price: poly_entry_price,
                    cex_entry_price: cex_reference_price,
                    cex_qty_btc,
                    last_cex_price: cex_reference_price,
                    reserved_margin: margin_required,
                    round_trip_pnl,
                },
            );
            stats.entry_count += 1;
            *stats
                .entries_by_market
                .entry(spec.market_id.clone())
                .or_insert(0) += 1;
        }
        ArbSignalAction::ClosePosition { .. } => {
            if positions.contains_key(&spec.symbol) {
                close_position(
                    &spec.symbol,
                    spec,
                    row.current_price_for_position(positions.get(&spec.symbol)),
                    cex_reference_price,
                    false,
                    config,
                    positions,
                    cash,
                    stats,
                )?;
            }
        }
        _ => {
            stats.unsupported_signals += 1;
        }
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
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    market_ids.sort();

    market_ids
        .into_iter()
        .map(|market_id| MarketSignalDebugRow {
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
            market_id,
        })
        .collect()
}

fn close_position(
    symbol: &Symbol,
    spec: &ReplayMarketSpec,
    market_exit_price: Option<f64>,
    exit_cex_price: f64,
    settle_to_payout: bool,
    config: &ResolvedReplayConfig,
    positions: &mut HashMap<Symbol, ReplayPosition>,
    cash: &mut f64,
    stats: &mut ReplayStats,
) -> Result<()> {
    let Some(mut position) = positions.remove(symbol) else {
        return Ok(());
    };

    let sale_value = if settle_to_payout {
        let payout = settlement_payout(spec, position.token_side, exit_cex_price);
        position.shares * payout
    } else {
        let exit_price = market_exit_price.unwrap_or(position.last_poly_price);
        position.shares * exit_price
    };
    let poly_gross_pnl = sale_value - position.poly_entry_notional;
    *cash += sale_value;
    stats.poly_realized_gross += poly_gross_pnl;
    position.round_trip_pnl += poly_gross_pnl;

    if !settle_to_payout && sale_value > 0.0 {
        let (poly_costs, poly_fee_paid, poly_slippage_paid) = trade_cost(
            sale_value,
            config.stress.poly_fee_bps,
            config.stress.poly_slippage_bps,
        );
        *cash -= poly_costs;
        position.round_trip_pnl -= poly_costs;
        record_costs(stats, poly_fee_paid, poly_slippage_paid, true, false, true);
    }

    let cex_gross_pnl = linear_cex_pnl(
        position.cex_entry_price,
        exit_cex_price,
        position.cex_qty_btc,
    );
    *cash += cex_gross_pnl;
    stats.cex_realized_gross += cex_gross_pnl;
    position.round_trip_pnl += cex_gross_pnl;

    let exit_cex_notional = position.cex_qty_btc.abs() * exit_cex_price;
    if exit_cex_notional > 0.0 {
        let (cex_costs, cex_fee_paid, cex_slippage_paid) = trade_cost(
            exit_cex_notional,
            config.stress.cex_fee_bps,
            config.stress.cex_slippage_bps,
        );
        *cash -= cex_costs;
        position.round_trip_pnl -= cex_costs;
        record_costs(stats, cex_fee_paid, cex_slippage_paid, false, false, true);
    }

    stats.round_trip_count += 1;
    *stats
        .net_pnl_by_market
        .entry(position.market_id.clone())
        .or_insert(0.0) += position.round_trip_pnl;
    if position.round_trip_pnl > 0.0 {
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

fn trade_cost(notional: f64, fee_bps: f64, slippage_bps: f64) -> (f64, f64, f64) {
    let abs_notional = notional.abs();
    let fee_paid = abs_notional * fee_bps / 10_000.0;
    let slippage_paid = abs_notional * slippage_bps / 10_000.0;
    (fee_paid + slippage_paid, fee_paid, slippage_paid)
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
    use super::*;

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
}
