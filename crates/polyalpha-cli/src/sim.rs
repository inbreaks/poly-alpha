use anyhow::{anyhow, Context, Result};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::PathBuf;
use tokio::sync::broadcast::error::TryRecvError;
use tokio::time::{sleep, Duration};

use polyalpha_core::{
    create_channels, AlphaEngine, CexBaseQty, ConnectionStatus, EngineParams, EngineWarning,
    Exchange, ExecutionEvent, HedgeState, InstrumentKind, MarketConfig, MarketDataEvent,
    MarketDataSource, MarketPhase, OpenCandidate, OrderStatus, PlanningIntent, Position, Price,
    RiskManager, Settings, SymbolRegistry, TokenSide, UsdNotional, VenueQuantity,
};
use polyalpha_data::{
    CexBookLevel, CexBookUpdate, MockMarketDataSource, MockTick, PolyBookLevel, PolyBookUpdate,
};
use polyalpha_engine::{SimpleAlphaEngine, SimpleEngineConfig};
use polyalpha_executor::dry_run::DryRunOrderSnapshot;
use polyalpha_risk::{InMemoryRiskManager, RiskLimits};

use crate::args::{SimInspectFormat, SimScenario};
use crate::commands::{open_plan_risk_rejection_reason, preview_open_plan, select_market};
use crate::market_pool::{
    active_open_cooldown_reason, active_open_cooldown_remaining_ms, clear_expired_open_cooldown,
    extract_plan_rejection_reason_code, maybe_start_open_cooldown, note_tradeable_activity,
    MarketPoolState,
};
use crate::runtime::{
    apply_orderbook_snapshot, build_data_manager, build_execution_stack,
    close_intent_for_symbol as runtime_close_intent_for_symbol, RuntimeExecutionMode,
    RuntimeExecutor,
};

const MAX_EVENT_LOGS: usize = 32;
const REPORT_WIDTH: usize = 78;

fn format_exchange_label(exchange: Exchange) -> &'static str {
    match exchange {
        Exchange::Polymarket => "Polymarket",
        Exchange::Binance => "Binance",
        Exchange::Okx => "OKX",
    }
}

fn format_order_side_label(side: polyalpha_core::OrderSide) -> &'static str {
    match side {
        polyalpha_core::OrderSide::Buy => "买入",
        polyalpha_core::OrderSide::Sell => "卖出",
    }
}

fn format_instrument_label(instrument: InstrumentKind) -> &'static str {
    match instrument {
        InstrumentKind::PolyYes => "YES",
        InstrumentKind::PolyNo => "NO",
        InstrumentKind::CexPerp => "交易所永续",
    }
}

fn format_market_phase_label(phase: &MarketPhase) -> &'static str {
    match phase {
        MarketPhase::Trading => "交易中",
        MarketPhase::PreSettlement { .. } => "预结算",
        MarketPhase::ForceReduce { .. } => "强制降仓",
        MarketPhase::CloseOnly { .. } => "只平仓",
        MarketPhase::SettlementPending => "待结算",
        MarketPhase::Disputed => "争议中",
        MarketPhase::Resolved => "已结算",
    }
}

fn format_connection_status_label(status: ConnectionStatus) -> &'static str {
    match status {
        ConnectionStatus::Connecting => "连接中",
        ConnectionStatus::Connected => "已连接",
        ConnectionStatus::Disconnected => "已断开",
        ConnectionStatus::Reconnecting => "重连中",
    }
}

fn format_hedge_state_label(state: HedgeState) -> &'static str {
    match state {
        HedgeState::Idle => "空闲",
        HedgeState::SubmittingLegs => "提交腿单中",
        HedgeState::Hedging => "对冲中",
        HedgeState::Hedged => "已对冲",
        HedgeState::Rebalancing => "再平衡中",
        HedgeState::Closing => "平仓中",
    }
}

fn display_exchange_text(value: &str) -> &str {
    match value {
        "Polymarket" => "Polymarket",
        "Binance" => "Binance",
        "Okx" | "OKX" => "OKX",
        _ => value,
    }
}

fn display_market_phase_text(value: &str) -> &str {
    if value.starts_with("Trading") {
        "交易中"
    } else if value.starts_with("PreSettlement") {
        "预结算"
    } else if value.starts_with("ForceReduce") {
        "强制降仓"
    } else if value.starts_with("CloseOnly") {
        "只平仓"
    } else if value.starts_with("SettlementPending") {
        "待结算"
    } else if value.starts_with("Disputed") {
        "争议中"
    } else if value.starts_with("Resolved") {
        "已结算"
    } else if value.eq_ignore_ascii_case("unknown") {
        "未知"
    } else {
        value
    }
}

fn display_hedge_state_text(value: &str) -> &str {
    match value {
        "Idle" => "空闲",
        "SubmittingLegs" => "提交腿单中",
        "Hedging" => "对冲中",
        "Hedged" => "已对冲",
        "Rebalancing" => "再平衡中",
        "Closing" => "平仓中",
        _ => value,
    }
}

fn display_connection_status_text(value: &str) -> &str {
    match value {
        "Connecting" => "连接中",
        "Connected" => "已连接",
        "Disconnected" => "已断开",
        "Reconnecting" => "重连中",
        _ => value,
    }
}

fn display_circuit_breaker_text(value: &str) -> &str {
    match value {
        "Closed" => "关闭",
        "HalfOpen" => "半开",
        "Open" => "打开",
        _ => value,
    }
}

fn display_order_status_text(value: &str) -> &str {
    match value {
        "Pending" => "待处理",
        "Open" => "已挂单",
        "PartialFill" => "部分成交",
        "Filled" => "已成交",
        "Cancelled" => "已撤销",
        "Rejected" => "已拒绝",
        _ => value,
    }
}

fn display_position_side_text(value: &str) -> &str {
    match value {
        "Long" | "LONG" => "多",
        "Short" | "SHORT" => "空",
        "Flat" | "FLAT" => "平",
        "Buy" | "BUY" => "买",
        "Sell" | "SELL" => "卖",
        _ => value,
    }
}

fn display_instrument_text(value: &str) -> &str {
    match value {
        "PolyYes" => "Polymarket YES",
        "PolyNo" => "Polymarket NO",
        "CexPerp" => "交易所永续",
        _ => value,
    }
}

fn display_connection_entry(value: &str) -> String {
    match value.split_once('=') {
        Some((exchange, status)) => format!(
            "{}={}",
            display_exchange_text(exchange),
            display_connection_status_text(status)
        ),
        None => value.to_owned(),
    }
}

fn display_event_kind_text(value: &str) -> &str {
    match value {
        "market_data" => "行情",
        "signal" => "信号",
        "fill" | "execution" => "成交",
        "trade_closed" => "平仓",
        "state" => "状态",
        "warning" => "警告",
        "skip" => "跳过",
        "risk" => "风控",
        "reconcile" => "对账",
        "error" | "poll_error" => "错误",
        _ => value,
    }
}

fn display_sim_scenario_text(value: &str) -> &str {
    match value {
        "Basic" => "基础场景",
        "BasisEntry" => "基差入场场景",
        "PhaseCloseOnly" => "只平仓阶段场景",
        _ => value,
    }
}

#[derive(Default)]
struct SimStats {
    ticks_processed: usize,
    market_events: usize,
    signals_seen: usize,
    signals_rejected: usize,
    order_submitted: usize,
    order_cancelled: usize,
    fills: usize,
    state_changes: usize,
    trades_closed: usize,
    total_pnl_usd: f64,
}

#[derive(Clone, Debug, Default)]
struct ObservedState {
    poly_yes_mid: Option<Decimal>,
    poly_no_mid: Option<Decimal>,
    cex_mid: Option<Decimal>,
    market_phase: Option<MarketPhase>,
    connections: HashMap<String, String>,
    market_pool: MarketPoolState,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SimEventLog {
    kind: String,
    summary: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SimPositionView {
    exchange: String,
    instrument: String,
    side: String,
    quantity: String,
    entry_price: String,
    entry_notional: String,
    realized_pnl: String,
    unrealized_pnl: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SimOrderView {
    exchange_order_id: String,
    exchange: String,
    symbol: String,
    status: String,
    timestamp_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SimSnapshot {
    tick_index: usize,
    market: String,
    hedge_symbol: String,
    scenario: String,
    market_phase: String,
    #[serde(default)]
    connections: Vec<String>,
    poly_yes_mid: Option<f64>,
    poly_no_mid: Option<f64>,
    cex_mid: Option<f64>,
    current_basis: Option<f64>,
    signal_token_side: Option<String>,
    current_zscore: Option<f64>,
    current_fair_value: Option<f64>,
    current_delta: Option<f64>,
    cex_reference_price: Option<f64>,
    hedge_state: String,
    last_signal_id: Option<String>,
    open_orders: usize,
    active_dmm_orders: usize,
    signals_seen: usize,
    signals_rejected: usize,
    order_submitted: usize,
    order_cancelled: usize,
    fills: usize,
    state_changes: usize,
    total_exposure_usd: String,
    daily_pnl_usd: String,
    breaker: String,
    positions: Vec<SimPositionView>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SimArtifact {
    env: String,
    market_index: usize,
    market: String,
    scenario: String,
    tick_interval_ms: u64,
    ticks_processed: usize,
    final_snapshot: SimSnapshot,
    open_orders: Vec<SimOrderView>,
    recent_events: Vec<SimEventLog>,
    snapshots: Vec<SimSnapshot>,
}

fn build_sim_engine_config(settings: &Settings) -> SimpleEngineConfig {
    let configured_entry = settings
        .strategy
        .basis
        .entry_z_score_threshold
        .to_f64()
        .unwrap_or(2.0);
    let configured_exit = settings
        .strategy
        .basis
        .exit_z_score_threshold
        .to_f64()
        .unwrap_or(0.5);

    SimpleEngineConfig {
        min_signal_samples: settings.strategy.basis.min_warmup_samples.max(2),
        rolling_window_minutes: ((settings.strategy.basis.rolling_window_secs / 60) as usize)
            .max(2),
        entry_z: configured_entry.abs().max(0.1),
        exit_z: configured_exit
            .abs()
            .min(configured_entry.abs().max(0.1))
            .max(0.05),
        position_notional_usd: settings.strategy.basis.max_position_usd,
        cex_hedge_ratio: 1.0,
        dmm_half_spread: Decimal::new(1, 2),
        dmm_quote_size: polyalpha_core::PolyShares::ZERO,
        enable_freshness_check: settings.strategy.basis.enable_freshness_check,
        reject_on_disconnect: settings.strategy.basis.reject_on_disconnect,
        max_poly_data_age_ms: settings
            .strategy
            .basis
            .max_data_age_minutes
            .saturating_mul(60_000),
        max_cex_data_age_ms: settings
            .strategy
            .basis
            .max_data_age_minutes
            .saturating_mul(60_000),
        max_time_diff_ms: settings
            .strategy
            .basis
            .max_time_diff_minutes
            .saturating_mul(60_000),
    }
}

fn close_intent_for_symbol(
    symbol: &polyalpha_core::Symbol,
    reason: &str,
    now_ms: u64,
) -> PlanningIntent {
    runtime_close_intent_for_symbol(
        symbol,
        reason,
        &format!("corr-close-{}-{now_ms}", symbol.0),
        now_ms,
    )
}

fn sync_engine_position_state(
    engine: &mut SimpleAlphaEngine,
    risk: &InMemoryRiskManager,
    symbol: &polyalpha_core::Symbol,
) {
    let active_token_side = if risk
        .position_tracker()
        .net_symbol_qty(symbol, InstrumentKind::PolyYes)
        > Decimal::ZERO
    {
        Some(TokenSide::Yes)
    } else if risk
        .position_tracker()
        .net_symbol_qty(symbol, InstrumentKind::PolyNo)
        > Decimal::ZERO
    {
        Some(TokenSide::No)
    } else {
        None
    };
    engine.sync_position_state(symbol, active_token_side);
}

fn event_timestamp_ms(event: &MarketDataEvent) -> u64 {
    match event {
        MarketDataEvent::OrderBookUpdate { snapshot } => snapshot.received_at_ms,
        MarketDataEvent::TradeUpdate { timestamp_ms, .. } => *timestamp_ms,
        MarketDataEvent::FundingRate {
            next_funding_time_ms,
            ..
        } => *next_funding_time_ms,
        MarketDataEvent::MarketLifecycle { timestamp_ms, .. } => *timestamp_ms,
        MarketDataEvent::ConnectionEvent { .. } => 0,
    }
}

async fn run_sim_with_mock_ticks(
    env: &str,
    settings: &Settings,
    market_index: usize,
    scenario_label: &str,
    ticks: Vec<MockTick>,
    tick_interval_ms: u64,
    print_every: usize,
    max_ticks: usize,
    json: bool,
) -> Result<SimArtifact> {
    let market = select_market(settings, market_index)?;
    let registry = SymbolRegistry::new(settings.markets.clone());
    let channels = create_channels(std::slice::from_ref(&market.symbol));
    let manager = build_data_manager(&registry, channels.market_data_tx.clone());

    let mut source = MockMarketDataSource::new(
        manager,
        Exchange::Polymarket,
        settings.strategy.settlement.clone(),
        ticks,
    );
    source.connect().await?;
    source.subscribe_market(&market.symbol).await?;

    let mut market_data_rx = channels.market_data_tx.subscribe();
    let engine_config = build_sim_engine_config(settings);
    let mut engine =
        SimpleAlphaEngine::with_markets(engine_config.clone(), settings.markets.clone());
    engine.update_params(EngineParams {
        basis_entry_zscore: None,
        basis_exit_zscore: None,
        rolling_window_secs: None,
        max_position_usd: Some(settings.strategy.basis.max_position_usd),
    });

    let (orderbook_provider, executor, mut execution) = build_execution_stack(
        settings,
        &registry,
        RuntimeExecutionMode::Paper,
        1_700_000_000_000,
        None,
    )
    .await?;
    let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
    let mut observed = ObservedState::default();
    let mut stats = SimStats::default();
    let mut recent_events = VecDeque::with_capacity(MAX_EVENT_LOGS);
    let mut snapshots = Vec::new();

    while source.emit_next()?.is_some() {
        stats.ticks_processed += 1;

        loop {
            match market_data_rx.try_recv() {
                Ok(event) => {
                    stats.market_events += 1;
                    if let MarketDataEvent::OrderBookUpdate { snapshot } = &event {
                        apply_orderbook_snapshot(&orderbook_provider, &registry, snapshot);
                    }
                    observe_market_event(&mut observed, &event);
                    push_event(
                        &mut recent_events,
                        "market_data",
                        summarize_market_event(&event),
                    );

                    if let MarketDataEvent::MarketLifecycle { symbol, phase, .. } = &event {
                        risk.update_market_phase(symbol.clone(), phase.clone());
                    }

                    let output = engine.on_market_data(&event).await;
                    for warning in &output.warnings {
                        push_event(
                            &mut recent_events,
                            "warning",
                            summarize_engine_warning(warning),
                        );
                    }
                    for update in output.dmm_updates {
                        let events = execution.apply_dmm_quote_update(update).await?;
                        apply_execution_events(&mut risk, &mut stats, &mut recent_events, events)
                            .await?;
                        sync_engine_position_state(&mut engine, &risk, &market.symbol);
                    }

                    for candidate in output.open_candidates {
                        stats.signals_seen += 1;
                        push_event(
                            &mut recent_events,
                            "signal",
                            summarize_candidate(&candidate),
                        );

                        clear_expired_open_cooldown(
                            &mut observed.market_pool,
                            candidate.timestamp_ms,
                        );
                        if let Some(reason) = active_open_cooldown_reason(
                            &observed.market_pool,
                            candidate.timestamp_ms,
                        ) {
                            stats.signals_rejected += 1;
                            let remaining_ms = active_open_cooldown_remaining_ms(
                                &observed.market_pool,
                                candidate.timestamp_ms,
                            )
                            .unwrap_or_default();
                            push_event(
                                &mut recent_events,
                                "risk",
                                format!(
                                    "候选被拒绝：市场处于冷却期 {}（{}，剩余冷却 {}ms）",
                                    candidate.candidate_id, reason, remaining_ms
                                ),
                            );
                            continue;
                        }

                        let plan = match preview_open_plan(&mut execution, &risk, &candidate) {
                            Ok(plan) => plan,
                            Err(err) => {
                                stats.signals_rejected += 1;
                                let reason_code =
                                    extract_plan_rejection_reason_code(&err.to_string())
                                        .unwrap_or_else(|| "trade_plan_rejected".to_owned());
                                let _ = maybe_start_market_pool_cooldown(
                                    settings,
                                    &mut observed,
                                    candidate.timestamp_ms,
                                    &reason_code,
                                );
                                push_event(
                                    &mut recent_events,
                                    "risk",
                                    format!(
                                        "候选被交易规划拒绝 {}（{}）",
                                        candidate.candidate_id, err
                                    ),
                                );
                                continue;
                            }
                        };

                        if let Some(risk_rejection) = open_plan_risk_rejection_reason(&risk, &plan)
                        {
                            stats.signals_rejected += 1;
                            push_event(
                                &mut recent_events,
                                "risk",
                                format!(
                                    "候选被拒绝：未通过风控检查 {}（{}）",
                                    candidate.candidate_id, risk_rejection
                                ),
                            );
                            continue;
                        }

                        match execution.process_plan(plan).await {
                            Ok(events) => {
                                if execution_events_include_trade_plan_created(&events) {
                                    note_tradeable_activity(
                                        &mut observed.market_pool,
                                        candidate.timestamp_ms,
                                    );
                                }
                                let rejection_reasons = opening_signal_rejection_reasons(&events);
                                if !rejection_reasons.is_empty() {
                                    let _ = maybe_start_market_pool_cooldown_from_rejections(
                                        settings,
                                        &mut observed,
                                        candidate.timestamp_ms,
                                        &rejection_reasons,
                                    );
                                }
                                apply_execution_events(
                                    &mut risk,
                                    &mut stats,
                                    &mut recent_events,
                                    events,
                                )
                                .await?;
                                sync_engine_position_state(&mut engine, &risk, &market.symbol);
                            }
                            Err(err) => {
                                stats.signals_rejected += 1;
                                let rejection_reasons = vec![err.to_string()];
                                let _ = maybe_start_market_pool_cooldown_from_rejections(
                                    settings,
                                    &mut observed,
                                    candidate.timestamp_ms,
                                    &rejection_reasons,
                                );
                                push_event(
                                    &mut recent_events,
                                    "risk",
                                    format!(
                                        "候选被执行规划拒绝 {}（{}）",
                                        candidate.candidate_id, err
                                    ),
                                );
                            }
                        }
                    }

                    if let Some(reason) = engine.close_reason(&market.symbol) {
                        let events = execution
                            .process_intent(close_intent_for_symbol(
                                &market.symbol,
                                &reason,
                                event_timestamp_ms(&event),
                            ))
                            .await?;
                        apply_execution_events(&mut risk, &mut stats, &mut recent_events, events)
                            .await?;
                        sync_engine_position_state(&mut engine, &risk, &market.symbol);
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Lagged(_)) => continue,
                Err(TryRecvError::Closed) => break,
            }
        }

        let snapshot = build_snapshot(
            &market,
            scenario_label,
            stats.ticks_processed,
            &engine,
            &observed,
            &stats,
            &risk,
            &execution,
            &executor,
        )?;
        if stats.ticks_processed % print_every.max(1) == 0 {
            print_snapshot(&snapshot, json);
            if !json {
                if let Some(last_event) = recent_events.back() {
                    println!(
                        "最近事件=[{}] {}",
                        display_event_kind_text(&last_event.kind),
                        last_event.summary
                    );
                }
            }
        }
        snapshots.push(snapshot);

        if max_ticks > 0 && stats.ticks_processed >= max_ticks {
            break;
        }
        if tick_interval_ms > 0 {
            sleep(Duration::from_millis(tick_interval_ms)).await;
        }
    }

    let final_snapshot = snapshots
        .last()
        .cloned()
        .ok_or_else(|| anyhow!("模拟盘会话未生成任何快照"))?;
    let open_orders = executor
        .open_order_snapshots()?
        .into_iter()
        .map(order_view_from_snapshot)
        .collect();

    Ok(SimArtifact {
        env: env.to_owned(),
        market_index,
        market: market.symbol.0.clone(),
        scenario: scenario_label.to_owned(),
        tick_interval_ms,
        ticks_processed: stats.ticks_processed,
        final_snapshot,
        open_orders,
        recent_events: recent_events.into_iter().collect(),
        snapshots,
    })
}

pub async fn run_sim(
    env: &str,
    market_index: usize,
    scenario: SimScenario,
    tick_interval_ms: u64,
    print_every: usize,
    max_ticks: usize,
    json: bool,
) -> Result<()> {
    let settings = Settings::load(env).with_context(|| format!("加载配置环境 `{env}` 失败"))?;
    let market = select_market(&settings, market_index)?;
    let ticks = scenario_ticks(&market, scenario);
    let artifact = run_sim_with_mock_ticks(
        env,
        &settings,
        market_index,
        &format!("{scenario:?}"),
        ticks,
        tick_interval_ms,
        print_every,
        max_ticks,
        json,
    )
    .await?;
    let artifact_path = artifact_path(&settings, env)?;
    persist_artifact(&artifact_path, &artifact)?;
    println!("模拟结果已写入：{}", artifact_path.display());

    Ok(())
}

pub fn inspect_sim(env: &str, format: SimInspectFormat) -> Result<()> {
    let settings = Settings::load(env).with_context(|| format!("加载配置环境 `{env}` 失败"))?;
    let artifact_path = resolve_artifact_for_read(&settings, env)?;
    let raw = fs::read_to_string(&artifact_path)
        .with_context(|| format!("读取模拟结果 `{}` 失败", artifact_path.display()))?;
    let artifact: SimArtifact = serde_json::from_str(&raw)
        .with_context(|| format!("解析模拟结果 `{}` 失败", artifact_path.display()))?;

    match format {
        SimInspectFormat::Table => print_artifact_table(&artifact, &artifact_path),
        SimInspectFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&artifact)?);
        }
    }

    Ok(())
}

fn scenario_ticks(market: &MarketConfig, scenario: SimScenario) -> Vec<MockTick> {
    match scenario {
        SimScenario::Basic => basic_ticks(market),
        SimScenario::BasisEntry => basis_entry_ticks(market),
        SimScenario::PhaseCloseOnly => phase_close_only_ticks(market),
    }
}

fn basic_ticks(market: &MarketConfig) -> Vec<MockTick> {
    let mut ticks = Vec::new();
    let symbol = market.symbol.clone();
    let trading_now = market.settlement_timestamp.saturating_sub(48 * 3600);
    push_connections(&mut ticks, market);
    ticks.push(MockTick::Lifecycle {
        symbol: symbol.clone(),
        now_timestamp_secs: trading_now,
        emitted_at_ms: 1,
    });
    push_cex_book(&mut ticks, market, 0, 100_000, 100_000);
    push_poly_yes_book(&mut ticks, market, 60_100, 50, 52);
    push_cex_book(&mut ticks, market, 60_200, 103_800, 103_800);
    push_poly_yes_book(&mut ticks, market, 120_100, 38, 40);
    push_cex_book(&mut ticks, market, 120_200, 101_200, 101_200);
    push_poly_yes_book(&mut ticks, market, 180_100, 57, 59);
    push_cex_book(&mut ticks, market, 180_200, 101_400, 101_400);
    ticks.push(MockTick::Lifecycle {
        symbol,
        now_timestamp_secs: market.settlement_timestamp.saturating_sub(1800),
        emitted_at_ms: 240_000,
    });
    ticks
}

fn basis_entry_ticks(market: &MarketConfig) -> Vec<MockTick> {
    let mut ticks = Vec::new();
    let symbol = market.symbol.clone();
    let trading_now = market.settlement_timestamp.saturating_sub(72 * 3600);
    push_connections(&mut ticks, market);
    ticks.push(MockTick::Lifecycle {
        symbol,
        now_timestamp_secs: trading_now,
        emitted_at_ms: 1,
    });
    push_cex_book(&mut ticks, market, 0, 100_000, 100_000);
    push_poly_yes_book(&mut ticks, market, 60_100, 53, 53);
    push_poly_no_book(&mut ticks, market, 60_120, 47, 47);
    push_cex_book(&mut ticks, market, 60_200, 99_500, 99_500);
    push_poly_yes_book(&mut ticks, market, 120_100, 54, 54);
    push_poly_no_book(&mut ticks, market, 120_120, 46, 46);
    push_cex_book(&mut ticks, market, 120_200, 99_200, 99_200);
    push_poly_yes_book(&mut ticks, market, 180_100, 72, 72);
    push_poly_no_book(&mut ticks, market, 180_120, 28, 28);
    push_cex_book(&mut ticks, market, 180_200, 99_000, 99_000);
    push_poly_yes_book(&mut ticks, market, 240_100, 58, 58);
    push_poly_no_book(&mut ticks, market, 240_120, 42, 42);
    push_cex_book(&mut ticks, market, 240_200, 99_100, 99_100);
    push_poly_yes_book(&mut ticks, market, 300_100, 55, 55);
    push_poly_no_book(&mut ticks, market, 300_120, 45, 45);
    push_cex_book(&mut ticks, market, 300_200, 99_200, 99_200);
    ticks
}

fn phase_close_only_ticks(market: &MarketConfig) -> Vec<MockTick> {
    let mut ticks = Vec::new();
    let symbol = market.symbol.clone();
    push_connections(&mut ticks, market);
    push_cex_book(&mut ticks, market, 0, 100_000, 100_000);
    push_poly_yes_book(&mut ticks, market, 60_100, 53, 53);
    push_poly_no_book(&mut ticks, market, 60_120, 47, 47);
    push_cex_book(&mut ticks, market, 60_200, 99_500, 99_500);
    push_poly_yes_book(&mut ticks, market, 120_100, 54, 54);
    push_poly_no_book(&mut ticks, market, 120_120, 46, 46);
    push_cex_book(&mut ticks, market, 120_200, 99_200, 99_200);
    push_poly_yes_book(&mut ticks, market, 180_100, 72, 72);
    push_poly_no_book(&mut ticks, market, 180_120, 28, 28);
    push_cex_book(&mut ticks, market, 180_200, 99_000, 99_000);
    ticks.push(MockTick::Lifecycle {
        symbol,
        now_timestamp_secs: market.settlement_timestamp.saturating_sub(1800),
        emitted_at_ms: 180_300,
    });
    ticks
}

fn push_connections(ticks: &mut Vec<MockTick>, market: &MarketConfig) {
    ticks.push(MockTick::Connection {
        exchange: Exchange::Polymarket,
        status: ConnectionStatus::Connected,
    });
    ticks.push(MockTick::Connection {
        exchange: market.hedge_exchange,
        status: ConnectionStatus::Connected,
    });
}

fn push_cex_book(
    ticks: &mut Vec<MockTick>,
    market: &MarketConfig,
    ts_ms: u64,
    best_bid: i64,
    best_ask: i64,
) {
    ticks.push(MockTick::CexOrderBook(CexBookUpdate {
        exchange: market.hedge_exchange,
        venue_symbol: market.cex_symbol.clone(),
        bids: vec![CexBookLevel {
            price: Price(Decimal::new(best_bid, 0)),
            base_qty: CexBaseQty(Decimal::new(5, 1)),
        }],
        asks: vec![CexBookLevel {
            price: Price(Decimal::new(best_ask, 0)),
            base_qty: CexBaseQty(Decimal::new(5, 1)),
        }],
        exchange_timestamp_ms: ts_ms,
        received_at_ms: ts_ms,
        sequence: ts_ms,
    }));
}

fn push_poly_yes_book(
    ticks: &mut Vec<MockTick>,
    market: &MarketConfig,
    ts_ms: u64,
    best_bid_cents: i64,
    best_ask_cents: i64,
) {
    ticks.push(MockTick::PolyOrderBook(PolyBookUpdate {
        asset_id: market.poly_ids.yes_token_id.clone(),
        bids: vec![PolyBookLevel {
            price: Price(Decimal::new(best_bid_cents, 2)),
            shares: polyalpha_core::PolyShares(Decimal::new(25, 0)),
        }],
        asks: vec![PolyBookLevel {
            price: Price(Decimal::new(best_ask_cents, 2)),
            shares: polyalpha_core::PolyShares(Decimal::new(25, 0)),
        }],
        exchange_timestamp_ms: ts_ms,
        received_at_ms: ts_ms,
        sequence: ts_ms,
        last_trade_price: None,
    }));
}

fn push_poly_no_book(
    ticks: &mut Vec<MockTick>,
    market: &MarketConfig,
    ts_ms: u64,
    best_bid_cents: i64,
    best_ask_cents: i64,
) {
    ticks.push(MockTick::PolyOrderBook(PolyBookUpdate {
        asset_id: market.poly_ids.no_token_id.clone(),
        bids: vec![PolyBookLevel {
            price: Price(Decimal::new(best_bid_cents, 2)),
            shares: polyalpha_core::PolyShares(Decimal::new(25, 0)),
        }],
        asks: vec![PolyBookLevel {
            price: Price(Decimal::new(best_ask_cents, 2)),
            shares: polyalpha_core::PolyShares(Decimal::new(25, 0)),
        }],
        exchange_timestamp_ms: ts_ms,
        received_at_ms: ts_ms,
        sequence: ts_ms,
        last_trade_price: None,
    }));
}

fn observe_market_event(observed: &mut ObservedState, event: &MarketDataEvent) {
    match event {
        MarketDataEvent::OrderBookUpdate { snapshot } => {
            if let Some(mid) = mid_from_book(snapshot) {
                match snapshot.instrument {
                    InstrumentKind::PolyYes => observed.poly_yes_mid = Some(mid),
                    InstrumentKind::PolyNo => observed.poly_no_mid = Some(mid),
                    InstrumentKind::CexPerp => observed.cex_mid = Some(mid),
                }
            }
        }
        MarketDataEvent::TradeUpdate {
            instrument, price, ..
        } => match instrument {
            InstrumentKind::PolyYes => observed.poly_yes_mid = Some(price.0),
            InstrumentKind::PolyNo => observed.poly_no_mid = Some(price.0),
            InstrumentKind::CexPerp => observed.cex_mid = Some(price.0),
        },
        MarketDataEvent::FundingRate { .. } => {}
        MarketDataEvent::MarketLifecycle { phase, .. } => {
            observed.market_phase = Some(phase.clone());
        }
        MarketDataEvent::ConnectionEvent { exchange, status } => {
            observed
                .connections
                .insert(format!("{exchange:?}"), format!("{status:?}"));
        }
    }
}

fn mid_from_book(snapshot: &polyalpha_core::OrderBookSnapshot) -> Option<Decimal> {
    let best_bid = snapshot.bids.first()?.price.0;
    let best_ask = snapshot.asks.first()?.price.0;
    Some((best_bid + best_ask) / Decimal::new(2, 0))
}

fn maybe_start_market_pool_cooldown(
    settings: &Settings,
    observed: &mut ObservedState,
    now_ms: u64,
    reason: &str,
) -> bool {
    maybe_start_open_cooldown(
        &mut observed.market_pool,
        settings.strategy.market_data.max_stale_ms,
        reason,
        now_ms,
    )
}

fn maybe_start_market_pool_cooldown_from_rejections(
    settings: &Settings,
    observed: &mut ObservedState,
    now_ms: u64,
    rejection_reasons: &[String],
) -> Option<String> {
    rejection_reasons.iter().find_map(|reason| {
        let code = extract_plan_rejection_reason_code(reason).unwrap_or_else(|| reason.clone());
        maybe_start_market_pool_cooldown(settings, observed, now_ms, &code).then_some(code)
    })
}

fn opening_signal_rejection_reasons(events: &[ExecutionEvent]) -> Vec<String> {
    let mut reasons = Vec::new();
    let mut seen = HashSet::new();

    for event in events {
        if let ExecutionEvent::OrderSubmitted { response, .. } = event {
            if matches!(response.status, OrderStatus::Rejected) {
                let reason = response
                    .rejection_reason
                    .clone()
                    .unwrap_or_else(|| "order_rejected".to_owned());
                if seen.insert(reason.clone()) {
                    reasons.push(reason);
                }
            }
        }
    }

    reasons
}

fn execution_events_include_trade_plan_created(events: &[ExecutionEvent]) -> bool {
    events
        .iter()
        .any(|event| matches!(event, ExecutionEvent::TradePlanCreated { .. }))
}

fn build_snapshot(
    market: &MarketConfig,
    scenario: &str,
    tick_index: usize,
    engine: &SimpleAlphaEngine,
    observed: &ObservedState,
    stats: &SimStats,
    risk: &InMemoryRiskManager,
    execution: &polyalpha_executor::ExecutionManager<RuntimeExecutor>,
    executor: &RuntimeExecutor,
) -> Result<SimSnapshot> {
    let snapshot = risk.build_snapshot(tick_index as u64);
    let positions = snapshot
        .positions
        .values()
        .map(position_view_from_position)
        .collect::<Vec<_>>();
    let signal_snapshot = engine.basis_snapshot(&market.symbol);
    Ok(SimSnapshot {
        tick_index,
        market: market.symbol.0.clone(),
        hedge_symbol: market.cex_symbol.clone(),
        scenario: scenario.to_owned(),
        market_phase: observed
            .market_phase
            .clone()
            .map(|phase| format!("{phase:?}"))
            .unwrap_or_else(|| "Unknown".to_owned()),
        connections: sorted_connections(&observed.connections),
        poly_yes_mid: observed.poly_yes_mid.and_then(|value| value.to_f64()),
        poly_no_mid: observed.poly_no_mid.and_then(|value| value.to_f64()),
        cex_mid: observed.cex_mid.and_then(|value| value.to_f64()),
        current_basis: signal_snapshot.as_ref().map(|item| item.signal_basis),
        signal_token_side: signal_snapshot
            .as_ref()
            .map(|item| format!("{:?}", item.token_side)),
        current_zscore: signal_snapshot.as_ref().and_then(|item| item.z_score),
        current_fair_value: signal_snapshot.as_ref().map(|item| item.fair_value),
        current_delta: signal_snapshot.as_ref().map(|item| item.delta),
        cex_reference_price: signal_snapshot
            .as_ref()
            .map(|item| item.cex_reference_price),
        hedge_state: execution
            .hedge_state(&market.symbol)
            .map(|state| format!("{state:?}"))
            .unwrap_or_else(|| "Idle".to_owned()),
        last_signal_id: execution
            .last_signal_id(&market.symbol)
            .map(|value| value.to_owned()),
        open_orders: executor.open_order_count()?,
        active_dmm_orders: execution.active_dmm_order_count(&market.symbol),
        signals_seen: stats.signals_seen,
        signals_rejected: stats.signals_rejected,
        order_submitted: stats.order_submitted,
        order_cancelled: stats.order_cancelled,
        fills: stats.fills,
        state_changes: stats.state_changes,
        total_exposure_usd: snapshot.total_exposure_usd.0.normalize().to_string(),
        daily_pnl_usd: snapshot.daily_pnl.0.normalize().to_string(),
        breaker: format!("{:?}", snapshot.circuit_breaker),
        positions,
    })
}

async fn apply_execution_events(
    risk: &mut InMemoryRiskManager,
    stats: &mut SimStats,
    recent_events: &mut VecDeque<SimEventLog>,
    events: Vec<ExecutionEvent>,
) -> Result<()> {
    for event in events {
        match &event {
            ExecutionEvent::TradePlanCreated { .. } => {
                push_event(recent_events, "plan", summarize_execution_event(&event));
            }
            ExecutionEvent::PlanSuperseded { .. } => {
                push_event(recent_events, "plan", summarize_execution_event(&event));
            }
            ExecutionEvent::RecoveryPlanCreated { .. } => {
                push_event(recent_events, "recovery", summarize_execution_event(&event));
            }
            ExecutionEvent::ExecutionResultRecorded { result } => {
                if !result.actual_funding_cost_usd.0.is_zero() {
                    let funding_adjustment =
                        UsdNotional(Decimal::ZERO - result.actual_funding_cost_usd.0);
                    risk.apply_realized_pnl_adjustment(UsdNotional(funding_adjustment.0))
                        .await?;
                    stats.total_pnl_usd += funding_adjustment.0.to_f64().unwrap_or_default();
                }
                push_event(recent_events, "result", summarize_execution_event(&event));
            }
            ExecutionEvent::OrderSubmitted { .. } => {
                stats.order_submitted += 1;
                push_event(
                    recent_events,
                    "execution",
                    summarize_execution_event(&event),
                );
            }
            ExecutionEvent::OrderFilled(fill) => {
                stats.fills += 1;
                risk.on_fill(fill).await?;
                push_event(
                    recent_events,
                    "execution",
                    summarize_execution_event(&event),
                );
            }
            ExecutionEvent::OrderCancelled { .. } => {
                stats.order_cancelled += 1;
                push_event(
                    recent_events,
                    "execution",
                    summarize_execution_event(&event),
                );
            }
            ExecutionEvent::HedgeStateChanged { .. } => {
                stats.state_changes += 1;
                push_event(recent_events, "state", summarize_execution_event(&event));
            }
            ExecutionEvent::ReconcileRequired { .. } => {
                push_event(
                    recent_events,
                    "reconcile",
                    summarize_execution_event(&event),
                );
            }
            ExecutionEvent::TradeClosed { .. } => {
                stats.trades_closed += 1;
                push_event(
                    recent_events,
                    "trade_closed",
                    summarize_execution_event(&event),
                );
            }
        }
    }
    Ok(())
}

fn summarize_execution_event(event: &ExecutionEvent) -> String {
    match event {
        ExecutionEvent::TradePlanCreated { plan } => format!(
            "交易计划已生成 {} {} {} / {}",
            plan.symbol.0, plan.plan_id, plan.intent_type, plan.priority
        ),
        ExecutionEvent::PlanSuperseded {
            symbol,
            superseded_plan_id,
            next_plan_id,
        } => format!(
            "{} 计划已抢占 {} -> {}",
            symbol.0, superseded_plan_id, next_plan_id
        ),
        ExecutionEvent::RecoveryPlanCreated { plan } => {
            format!("恢复计划已生成 {} {}", plan.symbol.0, plan.plan_id)
        }
        ExecutionEvent::ExecutionResultRecorded { result } => format!(
            "执行结果已记录 {} {} {} / 实际边际={}",
            result.symbol.0,
            result.plan_id,
            result.status,
            result.realized_edge_usd.0.normalize()
        ),
        ExecutionEvent::OrderSubmitted {
            exchange, response, ..
        } => {
            if matches!(response.status, OrderStatus::Rejected) {
                let reason = response
                    .rejection_reason
                    .as_deref()
                    .unwrap_or("order_rejected");
                format!(
                    "订单已拒绝 {} {}（原因：{}）",
                    format_exchange_label(*exchange),
                    response.exchange_order_id.0,
                    reason
                )
            } else {
                format!(
                    "订单已提交 {} {}",
                    format_exchange_label(*exchange),
                    response.exchange_order_id.0
                )
            }
        }
        ExecutionEvent::OrderFilled(fill) => format!(
            "订单已成交 {} {} {} @ {}",
            format_exchange_label(fill.exchange),
            format_order_side_label(fill.side),
            quantity_to_string(fill.quantity),
            fill.price.0.normalize()
        ),
        ExecutionEvent::OrderCancelled {
            exchange, order_id, ..
        } => format!(
            "订单已撤销 {} {}",
            format_exchange_label(*exchange),
            order_id.0
        ),
        ExecutionEvent::HedgeStateChanged {
            old_state,
            new_state,
            ..
        } => format!(
            "对冲状态变更 {} -> {}",
            format_hedge_state_label(*old_state),
            format_hedge_state_label(*new_state)
        ),
        ExecutionEvent::ReconcileRequired { symbol, reason } => match symbol {
            Some(symbol) => format!("{} 需要对账：{}", symbol.0, reason),
            None => format!("需要对账：{}", reason),
        },
        ExecutionEvent::TradeClosed {
            symbol,
            realized_pnl,
            ..
        } => format!(
            "{} 已平仓，已实现盈亏={}",
            symbol.0,
            realized_pnl.0.normalize()
        ),
    }
}

fn summarize_market_event(event: &MarketDataEvent) -> String {
    match event {
        MarketDataEvent::OrderBookUpdate { snapshot } => format!(
            "订单簿更新 {} {}（{}）",
            format_exchange_label(snapshot.exchange),
            snapshot.symbol.0,
            format_instrument_label(snapshot.instrument)
        ),
        MarketDataEvent::TradeUpdate {
            exchange,
            symbol,
            instrument,
            price,
            ..
        } => format!(
            "成交更新 {} {}（{}）@ {}",
            format_exchange_label(*exchange),
            symbol.0,
            format_instrument_label(*instrument),
            price.0.normalize()
        ),
        MarketDataEvent::FundingRate {
            exchange,
            symbol,
            rate,
            ..
        } => format!(
            "资金费更新 {} {} {}",
            format_exchange_label(*exchange),
            symbol.0,
            rate.normalize()
        ),
        MarketDataEvent::MarketLifecycle { symbol, phase, .. } => format!(
            "市场阶段更新 {} {}",
            symbol.0,
            format_market_phase_label(phase)
        ),
        MarketDataEvent::ConnectionEvent { exchange, status } => {
            format!(
                "连接状态更新 {} {}",
                format_exchange_label(*exchange),
                format_connection_status_label(*status)
            )
        }
    }
}

fn summarize_engine_warning(warning: &EngineWarning) -> String {
    match warning {
        EngineWarning::ConnectionLost {
            symbol,
            poly_connected,
            cex_connected,
        } => format!(
            "{} 本轮跳过：行情连接异常（Polymarket={}, 交易所={}）",
            symbol.0, poly_connected, cex_connected
        ),
        EngineWarning::NoCexData { symbol } => {
            format!("{} 本轮跳过：缺少可用的交易所参考价", symbol.0)
        }
        EngineWarning::NoPolyData { symbol } => {
            format!("{} 本轮跳过：暂无新的 Polymarket 行情样本", symbol.0)
        }
        EngineWarning::CexPriceStale {
            symbol,
            cex_age_ms,
            max_age_ms,
        } => format!(
            "{} 本轮跳过：交易所行情已超时（当前 {}ms，阈值 {}ms）",
            symbol.0, cex_age_ms, max_age_ms
        ),
        EngineWarning::PolyPriceStale {
            symbol,
            poly_age_ms,
            max_age_ms,
        } => format!(
            "{} 本轮跳过：Polymarket 行情已超时（当前 {}ms，阈值 {}ms）",
            symbol.0, poly_age_ms, max_age_ms
        ),
        EngineWarning::DataMisaligned {
            symbol,
            poly_time_ms,
            cex_time_ms,
            diff_ms,
        } => format!(
            "{} 本轮跳过：Polymarket 与交易所时间未对齐（Poly={}，交易所={}，差值 {}ms）",
            symbol.0, poly_time_ms, cex_time_ms, diff_ms
        ),
    }
}

fn summarize_candidate(candidate: &OpenCandidate) -> String {
    let action = match candidate.direction.as_str() {
        "long" => "开仓候选",
        "short" => "反向候选",
        _ => "候选机会",
    };
    format!("{action} {}", candidate.candidate_id)
}

fn push_event(events: &mut VecDeque<SimEventLog>, kind: &str, summary: String) {
    if events.len() == MAX_EVENT_LOGS {
        events.pop_front();
    }
    events.push_back(SimEventLog {
        kind: kind.to_owned(),
        summary,
    });
}

fn print_snapshot(snapshot: &SimSnapshot, json: bool) {
    if json {
        match serde_json::to_string(snapshot) {
            Ok(line) => println!("{line}"),
            Err(_) => println!("{{\"tick_index\":{}}}", snapshot.tick_index),
        }
        return;
    }

    println!(
        "轮次={} 市场={} 阶段={} 连接={} Polymarket-YES={:?} Polymarket-NO={:?} 交易所中价={:?} 信号腿={:?} 基差={:?} Z值={:?} 公允值={:?} Delta={:?} 交易所参考价={:?} 对冲={} 最近信号={:?} 挂单={} DMM挂单={} 信号={}/{} 提交={} 撤单={} 成交={} 持仓={} 敞口={} 盈亏={} 熔断={}",
        snapshot.tick_index,
        snapshot.market,
        display_market_phase_text(&snapshot.market_phase),
        format_connections(&snapshot.connections),
        snapshot.poly_yes_mid,
        snapshot.poly_no_mid,
        snapshot.cex_mid,
        snapshot.signal_token_side,
        snapshot.current_basis,
        snapshot.current_zscore,
        snapshot.current_fair_value,
        snapshot.current_delta,
        snapshot.cex_reference_price,
        display_hedge_state_text(&snapshot.hedge_state),
        snapshot.last_signal_id,
        snapshot.open_orders,
        snapshot.active_dmm_orders,
        snapshot.signals_seen,
        snapshot.signals_rejected,
        snapshot.order_submitted,
        snapshot.order_cancelled,
        snapshot.fills,
        snapshot.positions.len(),
        snapshot.total_exposure_usd,
        snapshot.daily_pnl_usd,
        display_circuit_breaker_text(&snapshot.breaker),
    );
}

fn print_artifact_table(artifact: &SimArtifact, artifact_path: &PathBuf) {
    print_report_title("模拟盘会话回看");

    print_report_section("会话概览");
    print_report_kv("结果文件", artifact_path.display().to_string());
    print_report_kv("环境", artifact.env.as_str());
    print_report_kv("市场", artifact.market.as_str());
    print_report_kv("对冲标的", artifact.final_snapshot.hedge_symbol.as_str());
    print_report_kv(
        "场景",
        display_sim_scenario_text(artifact.scenario.as_str()),
    );
    print_report_kv("已处理轮次", artifact.ticks_processed.to_string());
    print_report_kv("轮次间隔", format!("{} ms", artifact.tick_interval_ms));
    print_report_kv("快照数量", artifact.snapshots.len().to_string());

    print_report_section("最终状态");
    print_report_kv(
        "市场阶段",
        display_market_phase_text(artifact.final_snapshot.market_phase.as_str()),
    );
    print_report_kv(
        "连接状态",
        format_connections(&artifact.final_snapshot.connections),
    );
    print_report_kv(
        "对冲状态",
        display_hedge_state_text(artifact.final_snapshot.hedge_state.as_str()),
    );
    print_report_kv(
        "最后信号",
        artifact
            .final_snapshot
            .last_signal_id
            .as_deref()
            .unwrap_or("无"),
    );
    print_report_kv(
        "熔断状态",
        display_circuit_breaker_text(artifact.final_snapshot.breaker.as_str()),
    );

    print_report_section("行情观察");
    print_report_kv(
        "Polymarket YES 中间价",
        format_option_f64(artifact.final_snapshot.poly_yes_mid),
    );
    print_report_kv(
        "Polymarket NO 中间价",
        format_option_f64(artifact.final_snapshot.poly_no_mid),
    );
    print_report_kv(
        "交易所中间价",
        format_option_f64(artifact.final_snapshot.cex_mid),
    );
    print_report_kv(
        "当前信号腿",
        artifact
            .final_snapshot
            .signal_token_side
            .as_deref()
            .unwrap_or("无"),
    );
    print_report_kv(
        "信号基差",
        format_option_f64(artifact.final_snapshot.current_basis),
    );
    print_report_kv(
        "当前 Z 值",
        format_option_f64(artifact.final_snapshot.current_zscore),
    );
    print_report_kv(
        "当前公允值",
        format_option_f64(artifact.final_snapshot.current_fair_value),
    );
    print_report_kv(
        "当前 Delta",
        format_option_f64(artifact.final_snapshot.current_delta),
    );
    print_report_kv(
        "交易所参考收盘价",
        format_option_f64(artifact.final_snapshot.cex_reference_price),
    );

    print_report_section("执行统计");
    print_report_kv(
        "信号通过/拒绝",
        format!(
            "{}/{}",
            artifact.final_snapshot.signals_seen, artifact.final_snapshot.signals_rejected
        ),
    );
    print_report_kv(
        "已提交订单",
        artifact.final_snapshot.order_submitted.to_string(),
    );
    print_report_kv(
        "已撤销订单",
        artifact.final_snapshot.order_cancelled.to_string(),
    );
    print_report_kv("成交数", artifact.final_snapshot.fills.to_string());
    print_report_kv(
        "状态变更数",
        artifact.final_snapshot.state_changes.to_string(),
    );
    print_report_kv(
        "当前挂单数",
        artifact.final_snapshot.open_orders.to_string(),
    );
    print_report_kv(
        "当前 DMM 挂单数",
        artifact.final_snapshot.active_dmm_orders.to_string(),
    );

    print_report_section("风险与盈亏");
    print_report_kv(
        "总敞口",
        artifact.final_snapshot.total_exposure_usd.as_str(),
    );
    print_report_kv("当日盈亏", artifact.final_snapshot.daily_pnl_usd.as_str());
    print_report_kv(
        "持仓数量",
        artifact.final_snapshot.positions.len().to_string(),
    );

    print_report_section("持仓明细");
    if artifact.final_snapshot.positions.is_empty() {
        println!("  （无）");
    } else {
        for (idx, position) in artifact.final_snapshot.positions.iter().enumerate() {
            println!(
                "  {}. {} / {} / {} | 数量={} | 入场={} | 已实现={} | 未实现={}",
                idx + 1,
                display_exchange_text(&position.exchange),
                display_instrument_text(&position.instrument),
                display_position_side_text(&position.side),
                position.quantity,
                position.entry_price,
                position.realized_pnl,
                position.unrealized_pnl,
            );
        }
    }

    print_report_section("挂单明细");
    if artifact.open_orders.is_empty() {
        println!("  （无）");
    } else {
        for (idx, order) in artifact.open_orders.iter().enumerate() {
            println!(
                "  {}. {} | {} | {} | {} | 时间戳={}",
                idx + 1,
                order.exchange_order_id,
                display_exchange_text(&order.exchange),
                order.symbol,
                display_order_status_text(&order.status),
                order.timestamp_ms,
            );
        }
    }

    print_report_section("最近事件");
    if artifact.recent_events.is_empty() {
        println!("  （无）");
    } else {
        for (idx, event) in artifact.recent_events.iter().enumerate() {
            println!(
                "  {}. [{}] {}",
                idx + 1,
                display_event_kind_text(&event.kind),
                event.summary
            );
        }
    }
}

fn print_report_title(title: &str) {
    println!("{}", "=".repeat(REPORT_WIDTH));
    println!("{title}");
    println!("{}", "=".repeat(REPORT_WIDTH));
}

fn print_report_section(title: &str) {
    println!();
    println!("[{title}]");
}

fn print_report_kv(label: &str, value: impl AsRef<str>) {
    println!("  {label}: {}", value.as_ref());
}

fn format_option_f64(value: Option<f64>) -> String {
    match value {
        Some(value) => format!("{value:.6}"),
        None => "无".to_owned(),
    }
}

fn sorted_connections(connections: &HashMap<String, String>) -> Vec<String> {
    let mut items = connections
        .iter()
        .map(|(exchange, status)| format!("{exchange}={status}"))
        .collect::<Vec<_>>();
    items.sort();
    items
}

fn artifact_path(settings: &Settings, env: &str) -> Result<PathBuf> {
    let mut path = PathBuf::from(&settings.general.data_dir);
    path.push("sim");
    fs::create_dir_all(&path)
        .with_context(|| format!("创建模拟结果目录 `{}` 失败", path.display()))?;
    path.push(format!("{env}-last-run.json"));
    Ok(path)
}

fn resolve_artifact_for_read(settings: &Settings, env: &str) -> Result<PathBuf> {
    let current = artifact_path(settings, env)?;
    if current.exists() {
        return Ok(current);
    }

    let mut legacy = PathBuf::from(&settings.general.data_dir);
    legacy.push("sim");
    legacy.push(format!("{env}-last-session.json"));
    if legacy.exists() {
        return Ok(legacy);
    }

    Ok(current)
}

fn persist_artifact(path: &PathBuf, artifact: &SimArtifact) -> Result<()> {
    let payload = serde_json::to_string_pretty(artifact)?;
    fs::write(path, payload).with_context(|| format!("写入模拟结果 `{}` 失败", path.display()))
}

fn format_connections(connections: &[String]) -> String {
    if connections.is_empty() {
        "无".to_owned()
    } else {
        connections
            .iter()
            .map(|item| display_connection_entry(item))
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn position_view_from_position(position: &Position) -> SimPositionView {
    SimPositionView {
        exchange: format!("{:?}", position.key.exchange),
        instrument: format!("{:?}", position.key.instrument),
        side: format!("{:?}", position.side),
        quantity: quantity_to_string(position.quantity),
        entry_price: position.entry_price.0.normalize().to_string(),
        entry_notional: position.entry_notional.0.normalize().to_string(),
        realized_pnl: position.realized_pnl.0.normalize().to_string(),
        unrealized_pnl: position.unrealized_pnl.0.normalize().to_string(),
    }
}

fn order_view_from_snapshot(order: DryRunOrderSnapshot) -> SimOrderView {
    SimOrderView {
        exchange_order_id: order.exchange_order_id.0,
        exchange: format!("{:?}", order.exchange),
        symbol: order.symbol.0,
        status: format!("{:?}", order.status),
        timestamp_ms: order.timestamp_ms,
    }
}

fn quantity_to_string(quantity: VenueQuantity) -> String {
    match quantity {
        VenueQuantity::PolyShares(shares) => shares.0.normalize().to_string(),
        VenueQuantity::CexBaseQty(qty) => qty.0.normalize().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use polyalpha_core::{
        AuditConfig, BasisStrategyConfig, BinanceConfig, ClientOrderId, DmmStrategyConfig,
        Exchange, ExecutionEvent, GeneralConfig, MarketDataConfig, MarketRule,
        NegRiskStrategyConfig, OkxConfig, OrderId, OrderResponse, OrderStatus, PaperSlippageConfig,
        PaperTradingConfig, PolyShares, PolymarketConfig, PolymarketIds, RiskConfig,
        SettlementRules, StrategyConfig, Symbol, UsdNotional, VenueQuantity,
    };

    fn test_market(cex_qty_step: Decimal) -> MarketConfig {
        MarketConfig {
            symbol: Symbol::new("e2e-btc-reach-99500"),
            poly_ids: PolymarketIds {
                condition_id: "e2e-cond".to_owned(),
                yes_token_id: "e2e-yes".to_owned(),
                no_token_id: "e2e-no".to_owned(),
            },
            market_question: Some("E2E: Will Bitcoin reach $100,000?".to_owned()),
            market_rule: Some(MarketRule::fallback_above(Price(Decimal::new(100_000, 0)))),
            cex_symbol: "BTCUSDT".to_owned(),
            hedge_exchange: Exchange::Binance,
            strike_price: Some(Price(Decimal::new(100_000, 0))),
            settlement_timestamp: 1_775_016_000,
            min_tick_size: Price(Decimal::new(1, 2)),
            neg_risk: false,
            cex_price_tick: Decimal::new(1, 1),
            cex_qty_step,
            cex_contract_multiplier: Decimal::ONE,
        }
    }

    fn test_settings(market: MarketConfig, max_position_usd: Decimal) -> Settings {
        Settings {
            general: GeneralConfig {
                log_level: "info".to_owned(),
                data_dir: "./data".to_owned(),
                monitor_socket_path: "/tmp/polyalpha.sock".to_owned(),
            },
            polymarket: PolymarketConfig {
                clob_api_url: "https://example.com".to_owned(),
                ws_url: "wss://example.com/ws".to_owned(),
                chain_id: 137,
                private_key: None,
                signature_type: None,
                funder: None,
                api_key_nonce: None,
                use_server_time: true,
            },
            binance: BinanceConfig {
                rest_url: "https://example.com".to_owned(),
                ws_url: "wss://example.com/ws".to_owned(),
            },
            okx: OkxConfig {
                rest_url: "https://example.com".to_owned(),
                ws_public_url: "wss://example.com/public".to_owned(),
                ws_private_url: "wss://example.com/private".to_owned(),
            },
            markets: vec![market],
            strategy: StrategyConfig {
                basis: BasisStrategyConfig {
                    entry_z_score_threshold: Decimal::new(5, 1),
                    exit_z_score_threshold: Decimal::new(2, 1),
                    rolling_window_secs: 240,
                    min_warmup_samples: 2,
                    min_basis_bps: Decimal::ZERO,
                    max_position_usd: UsdNotional(max_position_usd),
                    max_open_instant_loss_pct_of_budget: Decimal::new(4, 2),
                    delta_rebalance_threshold: Decimal::new(5, 2),
                    delta_rebalance_interval_secs: 60,
                    band_policy: polyalpha_core::BandPolicyMode::ConfiguredBand,
                    min_poly_price: Some(Decimal::ZERO),
                    max_poly_price: Some(Decimal::ONE),
                    max_data_age_minutes: 1,
                    max_time_diff_minutes: 1,
                    enable_freshness_check: true,
                    reject_on_disconnect: true,
                    overrides: HashMap::new(),
                },
                dmm: DmmStrategyConfig {
                    gamma: Decimal::new(1, 1),
                    sigma_window_secs: 300,
                    max_inventory: UsdNotional(Decimal::new(50, 0)),
                    order_refresh_secs: 10,
                    num_levels: 3,
                    level_spacing_bps: Decimal::new(10, 0),
                    min_spread_bps: Decimal::new(20, 0),
                },
                negrisk: NegRiskStrategyConfig {
                    min_arb_bps: Decimal::new(30, 0),
                    max_legs: 8,
                    enable_inventory_backed_short: false,
                },
                settlement: SettlementRules::default(),
                market_data: MarketDataConfig::default(),
            },
            risk: RiskConfig {
                max_total_exposure_usd: UsdNotional(Decimal::new(100_000, 0)),
                max_single_position_usd: UsdNotional(Decimal::new(100_000, 0)),
                max_daily_loss_usd: UsdNotional(Decimal::new(50_000, 0)),
                max_drawdown_pct: Decimal::new(50, 0),
                max_open_orders: 100,
                circuit_breaker_cooldown_secs: 60,
                rate_limit_orders_per_sec: 20,
                max_persistence_lag_secs: 10,
            },
            paper: PaperTradingConfig::default(),
            paper_slippage: PaperSlippageConfig {
                poly_slippage_bps: 0,
                cex_slippage_bps: 0,
                min_liquidity: Decimal::new(1, 0),
                allow_partial_fill: false,
            },
            execution_costs: polyalpha_core::ExecutionCostConfig::default(),
            audit: AuditConfig::default(),
        }
    }

    fn push_cex_book_with_qty(
        ticks: &mut Vec<MockTick>,
        market: &MarketConfig,
        ts_ms: u64,
        best_bid: i64,
        best_ask: i64,
        base_qty: Decimal,
    ) {
        ticks.push(MockTick::CexOrderBook(CexBookUpdate {
            exchange: market.hedge_exchange,
            venue_symbol: market.cex_symbol.clone(),
            bids: vec![CexBookLevel {
                price: Price(Decimal::new(best_bid, 0)),
                base_qty: CexBaseQty(base_qty),
            }],
            asks: vec![CexBookLevel {
                price: Price(Decimal::new(best_ask, 0)),
                base_qty: CexBaseQty(base_qty),
            }],
            exchange_timestamp_ms: ts_ms,
            received_at_ms: ts_ms,
            sequence: ts_ms,
        }));
    }

    fn push_poly_yes_book_with_shares(
        ticks: &mut Vec<MockTick>,
        market: &MarketConfig,
        ts_ms: u64,
        best_bid_cents: i64,
        best_ask_cents: i64,
        shares: Decimal,
    ) {
        ticks.push(MockTick::PolyOrderBook(PolyBookUpdate {
            asset_id: market.poly_ids.yes_token_id.clone(),
            bids: vec![PolyBookLevel {
                price: Price(Decimal::new(best_bid_cents, 2)),
                shares: polyalpha_core::PolyShares(shares),
            }],
            asks: vec![PolyBookLevel {
                price: Price(Decimal::new(best_ask_cents, 2)),
                shares: polyalpha_core::PolyShares(shares),
            }],
            exchange_timestamp_ms: ts_ms,
            received_at_ms: ts_ms,
            sequence: ts_ms,
            last_trade_price: None,
        }));
    }

    fn push_poly_no_book_with_shares(
        ticks: &mut Vec<MockTick>,
        market: &MarketConfig,
        ts_ms: u64,
        best_bid_cents: i64,
        best_ask_cents: i64,
        shares: Decimal,
    ) {
        ticks.push(MockTick::PolyOrderBook(PolyBookUpdate {
            asset_id: market.poly_ids.no_token_id.clone(),
            bids: vec![PolyBookLevel {
                price: Price(Decimal::new(best_bid_cents, 2)),
                shares: polyalpha_core::PolyShares(shares),
            }],
            asks: vec![PolyBookLevel {
                price: Price(Decimal::new(best_ask_cents, 2)),
                shares: polyalpha_core::PolyShares(shares),
            }],
            exchange_timestamp_ms: ts_ms,
            received_at_ms: ts_ms,
            sequence: ts_ms,
            last_trade_price: None,
        }));
    }

    fn acceptance_round_trip_ticks(market: &MarketConfig) -> Vec<MockTick> {
        let mut ticks = Vec::new();
        let trading_now = market.settlement_timestamp.saturating_sub(72 * 3600);
        push_connections(&mut ticks, market);
        ticks.push(MockTick::Lifecycle {
            symbol: market.symbol.clone(),
            now_timestamp_secs: trading_now,
            emitted_at_ms: 1,
        });

        let deep_cex_qty = Decimal::new(10_000, 0);
        let deep_poly_shares = Decimal::new(10_000, 0);
        push_cex_book_with_qty(&mut ticks, market, 0, 100_000, 100_000, deep_cex_qty);

        push_poly_yes_book_with_shares(&mut ticks, market, 60_100, 30, 30, deep_poly_shares);
        push_poly_no_book_with_shares(&mut ticks, market, 60_120, 70, 70, deep_poly_shares);
        push_cex_book_with_qty(&mut ticks, market, 60_200, 100_000, 100_000, deep_cex_qty);

        push_poly_yes_book_with_shares(&mut ticks, market, 120_100, 35, 35, deep_poly_shares);
        push_poly_no_book_with_shares(&mut ticks, market, 120_120, 65, 65, deep_poly_shares);
        push_cex_book_with_qty(&mut ticks, market, 120_200, 100_000, 100_000, deep_cex_qty);

        push_poly_yes_book_with_shares(&mut ticks, market, 180_100, 54, 54, deep_poly_shares);
        push_poly_no_book_with_shares(&mut ticks, market, 180_120, 46, 46, deep_poly_shares);
        push_cex_book_with_qty(&mut ticks, market, 180_200, 99_500, 99_500, deep_cex_qty);

        ticks.push(MockTick::Lifecycle {
            symbol: market.symbol.clone(),
            now_timestamp_secs: market.settlement_timestamp.saturating_sub(1_800),
            emitted_at_ms: 180_300,
        });
        ticks
    }

    fn snapshot_positions_are_flat(snapshot: &SimSnapshot) -> bool {
        snapshot.positions.iter().all(|position| {
            position.side == "Flat"
                || position.side == "FLAT"
                || position.quantity == "0"
                || position.quantity == "0.0"
        })
    }

    #[test]
    fn summarize_execution_event_keeps_band_policy_rejection_code_visible() {
        let text = summarize_execution_event(&ExecutionEvent::OrderSubmitted {
            symbol: Symbol::new("btc-test"),
            exchange: Exchange::Binance,
            response: OrderResponse {
                client_order_id: ClientOrderId("client-1".to_owned()),
                exchange_order_id: OrderId("ord-1".to_owned()),
                status: OrderStatus::Rejected,
                filled_quantity: VenueQuantity::PolyShares(PolyShares(Decimal::ZERO)),
                average_price: None,
                rejection_reason: Some("below_min_poly_price".to_owned()),
                timestamp_ms: 1,
            },
            correlation_id: "corr-1".to_owned(),
        });
        assert!(text.contains("below_min_poly_price"));
    }

    #[tokio::test]
    async fn sim_rejects_non_executable_candidate_without_aborting() {
        let market = test_market(Decimal::new(1, 3));
        let settings = test_settings(market.clone(), Decimal::new(1_000, 0));
        let artifact = run_sim_with_mock_ticks(
            "sim-e2e-round-trip",
            &settings,
            0,
            "AcceptanceRoundTrip",
            acceptance_round_trip_ticks(&market),
            0,
            usize::MAX,
            0,
            false,
        )
        .await
        .expect("sim candidate rejection should not abort runtime");

        assert_eq!(
            artifact.final_snapshot.signals_seen, 1,
            "unexpected snapshots: {:#?}",
            artifact.snapshots
        );
        assert_eq!(artifact.final_snapshot.signals_rejected, 1);
        assert_eq!(artifact.final_snapshot.order_submitted, 0);
        assert_eq!(artifact.final_snapshot.fills, 0);
        assert!(
            snapshot_positions_are_flat(&artifact.final_snapshot),
            "unexpected final snapshot: {:#?}\nrecent events: {:#?}",
            artifact.final_snapshot,
            artifact.recent_events
        );
        assert_eq!(artifact.final_snapshot.hedge_state, "Idle");
        assert!(
            artifact
                .recent_events
                .iter()
                .any(|event| event.summary.contains("候选被交易规划拒绝")),
            "planner rejection should be visible in sim event log"
        );
    }

    #[tokio::test]
    async fn sim_blocks_entry_when_cex_hedge_qty_floors_to_zero() {
        let market = test_market(Decimal::ONE);
        let settings = test_settings(market.clone(), Decimal::ONE);
        let artifact = run_sim_with_mock_ticks(
            "sim-e2e-zero-hedge",
            &settings,
            0,
            "AcceptanceZeroHedge",
            acceptance_round_trip_ticks(&market),
            0,
            usize::MAX,
            0,
            false,
        )
        .await
        .expect("sim zero-hedge scenario should succeed");

        assert!(
            artifact.snapshots.iter().any(|snapshot| {
                snapshot
                    .current_zscore
                    .is_some_and(|z_score| z_score <= -0.5)
                    && snapshot
                        .current_delta
                        .is_some_and(|delta| delta.abs() > 0.0 && delta.abs() < 0.05)
            }),
            "scenario should produce an entry-quality basis snapshot with a tiny hedge delta before step rounding blocks entry"
        );
        assert_eq!(artifact.final_snapshot.signals_seen, 1);
        assert_eq!(artifact.final_snapshot.signals_rejected, 1);
        assert_eq!(artifact.final_snapshot.order_submitted, 0);
        assert_eq!(artifact.final_snapshot.fills, 0);
        assert!(snapshot_positions_are_flat(&artifact.final_snapshot));
        assert!(
            artifact
                .recent_events
                .iter()
                .any(|event| { event.summary.contains("zero_cex_hedge_qty") }),
            "zero-hedge rejection should stay visible in the sim event log"
        );
        assert!(
            artifact
                .recent_events
                .iter()
                .all(|event| event.kind != "trade_closed"),
            "no position should mean no TradeClosed event"
        );
    }

    #[test]
    fn sim_market_pool_cooldown_starts_from_zero_hedge_rejection_reason() {
        let market = test_market(Decimal::ONE);
        let settings = test_settings(market, Decimal::ONE);
        let mut observed = ObservedState::default();

        let reason = maybe_start_market_pool_cooldown_from_rejections(
            &settings,
            &mut observed,
            1_000,
            &[String::from(
                "plan rejected [zero_cex_hedge_qty]: open plan cannot produce a non-zero cex hedge after step rounding",
            )],
        );

        assert_eq!(reason.as_deref(), Some("zero_cex_hedge_qty"));
        assert_eq!(
            active_open_cooldown_reason(&observed.market_pool, 1_001),
            Some("zero_cex_hedge_qty")
        );
    }
}
