use anyhow::{anyhow, Context, Result};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::PathBuf;
use tokio::sync::broadcast::error::TryRecvError;
use tokio::time::{sleep, Duration};

use polyalpha_core::{
    create_channels, AlphaEngine, ArbSignalAction, ArbSignalEvent, CexBaseQty, ConnectionStatus,
    EngineParams, Exchange, ExecutionEvent, InstrumentKind, MarketConfig, MarketDataEvent,
    MarketDataSource, MarketPhase, Position, Price, RiskManager, Settings, SymbolRegistry,
    VenueQuantity,
};
use polyalpha_data::{
    CexBookLevel, CexBookUpdate, DataManager, MarketDataNormalizer, MockMarketDataSource, MockTick,
    PolyBookLevel, PolyBookUpdate,
};
use polyalpha_engine::{SimpleAlphaEngine, SimpleEngineConfig};
use polyalpha_executor::{dry_run::DryRunOrderSnapshot, DryRunExecutor, ExecutionManager};
use polyalpha_risk::{InMemoryRiskManager, RiskLimits};

use crate::args::{SimInspectFormat, SimScenario};
use crate::commands::{select_market, signal_allowed};

const MAX_EVENT_LOGS: usize = 32;
const REPORT_WIDTH: usize = 78;

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
}

#[derive(Clone, Debug, Default)]
struct ObservedState {
    poly_yes_mid: Option<Decimal>,
    cex_mid: Option<Decimal>,
    previous_cex_mid: Option<Decimal>,
    funding_rate: Option<f64>,
    market_phase: Option<MarketPhase>,
    connections: HashMap<String, String>,
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
    cex_mid: Option<f64>,
    current_basis: Option<f64>,
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

pub async fn run_sim(
    env: &str,
    market_index: usize,
    scenario: SimScenario,
    tick_interval_ms: u64,
    print_every: usize,
    max_ticks: usize,
    json: bool,
) -> Result<()> {
    let settings =
        Settings::load(env).with_context(|| format!("failed to load config env `{env}`"))?;
    let market = select_market(&settings, market_index)?;
    let registry = SymbolRegistry::new(settings.markets.clone());
    let channels = create_channels(std::slice::from_ref(&market.symbol));
    let manager = DataManager::new(
        MarketDataNormalizer::new(registry.clone()),
        channels.market_data_tx.clone(),
    );

    let ticks = scenario_ticks(&market, scenario);
    let mut source = MockMarketDataSource::new(
        manager,
        Exchange::Polymarket,
        settings.strategy.settlement.clone(),
        ticks,
    );
    source.connect().await?;
    source.subscribe_market(&market.symbol).await?;

    let mut market_data_rx = channels.market_data_tx.subscribe();
    let engine_config = SimpleEngineConfig {
        warmup_samples: 1,
        basis_entry_threshold: 0.05,
        basis_exit_threshold: 0.02,
        ..SimpleEngineConfig::default()
    };
    let mut engine = SimpleAlphaEngine::new(engine_config.clone());
    engine.update_params(EngineParams {
        basis_entry_zscore: None,
        basis_exit_zscore: None,
        max_position_usd: Some(settings.strategy.basis.max_position_usd),
    });

    let executor = DryRunExecutor::new();
    let mut execution = ExecutionManager::with_symbol_registry(executor.clone(), registry.clone());
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
                    for update in output.dmm_updates {
                        let events = execution.apply_dmm_quote_update(update).await?;
                        apply_execution_events(&mut risk, &mut stats, &mut recent_events, events)
                            .await?;
                    }

                    for signal in output.arb_signals {
                        stats.signals_seen += 1;
                        push_event(&mut recent_events, "signal", summarize_signal(&signal));

                        if !signal_allowed(&risk, &registry, &signal).await? {
                            stats.signals_rejected += 1;
                            push_event(
                                &mut recent_events,
                                "risk",
                                format!("signal rejected: {}", signal.signal_id),
                            );
                            continue;
                        }

                        let events = execution.process_arb_signal(signal).await?;
                        apply_execution_events(&mut risk, &mut stats, &mut recent_events, events)
                            .await?;
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Lagged(_)) => continue,
                Err(TryRecvError::Closed) => break,
            }
        }

        let snapshot = build_snapshot(
            &market,
            scenario,
            stats.ticks_processed,
            &engine_config,
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
                    println!("last_event=[{}] {}", last_event.kind, last_event.summary);
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
        .ok_or_else(|| anyhow!("sim session did not produce snapshots"))?;
    let open_orders = executor
        .open_order_snapshots()?
        .into_iter()
        .map(order_view_from_snapshot)
        .collect();
    let artifact = SimArtifact {
        env: env.to_owned(),
        market_index,
        market: market.symbol.0.clone(),
        scenario: format!("{scenario:?}"),
        tick_interval_ms,
        ticks_processed: stats.ticks_processed,
        final_snapshot,
        open_orders,
        recent_events: recent_events.into_iter().collect(),
        snapshots,
    };
    let artifact_path = artifact_path(&settings, env)?;
    persist_artifact(&artifact_path, &artifact)?;
    println!("sim artifact written: {}", artifact_path.display());

    Ok(())
}

pub fn inspect_sim(env: &str, format: SimInspectFormat) -> Result<()> {
    let settings =
        Settings::load(env).with_context(|| format!("failed to load config env `{env}`"))?;
    let artifact_path = resolve_artifact_for_read(&settings, env)?;
    let raw = fs::read_to_string(&artifact_path)
        .with_context(|| format!("failed to read sim artifact `{}`", artifact_path.display()))?;
    let artifact: SimArtifact = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse sim artifact `{}`", artifact_path.display()))?;

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
    push_cex_book(&mut ticks, market, 2, 100_000, 100_010);
    push_poly_yes_book(&mut ticks, market, 3, 50, 52);
    push_cex_book(&mut ticks, market, 4, 103_800, 103_820);
    push_poly_yes_book(&mut ticks, market, 5, 38, 40);
    push_cex_book(&mut ticks, market, 6, 101_200, 101_220);
    push_poly_yes_book(&mut ticks, market, 7, 57, 59);
    ticks.push(MockTick::Lifecycle {
        symbol,
        now_timestamp_secs: market.settlement_timestamp.saturating_sub(1800),
        emitted_at_ms: 8,
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
    push_cex_book(&mut ticks, market, 2, 100_200, 100_210);
    push_poly_yes_book(&mut ticks, market, 3, 47, 49);
    push_cex_book(&mut ticks, market, 4, 104_500, 104_520);
    push_poly_yes_book(&mut ticks, market, 5, 33, 35);
    push_cex_book(&mut ticks, market, 6, 104_900, 104_930);
    push_poly_yes_book(&mut ticks, market, 7, 34, 36);
    push_cex_book(&mut ticks, market, 8, 101_000, 101_020);
    push_poly_yes_book(&mut ticks, market, 9, 55, 57);
    ticks
}

fn phase_close_only_ticks(market: &MarketConfig) -> Vec<MockTick> {
    let mut ticks = Vec::new();
    let symbol = market.symbol.clone();
    push_connections(&mut ticks, market);
    ticks.push(MockTick::Lifecycle {
        symbol,
        now_timestamp_secs: market.settlement_timestamp.saturating_sub(1800),
        emitted_at_ms: 1,
    });
    push_cex_book(&mut ticks, market, 2, 104_000, 104_020);
    push_poly_yes_book(&mut ticks, market, 3, 35, 37);
    push_cex_book(&mut ticks, market, 4, 104_600, 104_620);
    push_poly_yes_book(&mut ticks, market, 5, 34, 36);
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
    }));
}

fn observe_market_event(observed: &mut ObservedState, event: &MarketDataEvent) {
    match event {
        MarketDataEvent::OrderBookUpdate { snapshot } => {
            if let Some(mid) = mid_from_book(snapshot) {
                match snapshot.instrument {
                    InstrumentKind::PolyYes => observed.poly_yes_mid = Some(mid),
                    InstrumentKind::PolyNo => observed.poly_yes_mid = Some(Decimal::ONE - mid),
                    InstrumentKind::CexPerp => {
                        observed.previous_cex_mid = observed.cex_mid;
                        observed.cex_mid = Some(mid);
                    }
                }
            }
        }
        MarketDataEvent::TradeUpdate {
            instrument, price, ..
        } => match instrument {
            InstrumentKind::PolyYes => observed.poly_yes_mid = Some(price.0),
            InstrumentKind::PolyNo => observed.poly_yes_mid = Some(Decimal::ONE - price.0),
            InstrumentKind::CexPerp => {
                observed.previous_cex_mid = observed.cex_mid;
                observed.cex_mid = Some(price.0);
            }
        },
        MarketDataEvent::FundingRate { rate, .. } => {
            observed.funding_rate = rate.to_f64();
        }
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

fn build_snapshot(
    market: &MarketConfig,
    scenario: SimScenario,
    tick_index: usize,
    engine_config: &SimpleEngineConfig,
    observed: &ObservedState,
    stats: &SimStats,
    risk: &InMemoryRiskManager,
    execution: &ExecutionManager<DryRunExecutor>,
    executor: &DryRunExecutor,
) -> Result<SimSnapshot> {
    let snapshot = risk.build_snapshot(tick_index as u64);
    let positions = snapshot
        .positions
        .values()
        .map(position_view_from_position)
        .collect::<Vec<_>>();
    Ok(SimSnapshot {
        tick_index,
        market: market.symbol.0.clone(),
        hedge_symbol: market.cex_symbol.clone(),
        scenario: format!("{scenario:?}"),
        market_phase: observed
            .market_phase
            .clone()
            .map(|phase| format!("{phase:?}"))
            .unwrap_or_else(|| "Unknown".to_owned()),
        connections: sorted_connections(&observed.connections),
        poly_yes_mid: observed.poly_yes_mid.and_then(|value| value.to_f64()),
        cex_mid: observed.cex_mid.and_then(|value| value.to_f64()),
        current_basis: current_basis(observed, engine_config),
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

fn current_basis(observed: &ObservedState, engine_config: &SimpleEngineConfig) -> Option<f64> {
    let poly = observed.poly_yes_mid?.to_f64()?;
    let cex_mid = observed.cex_mid?.to_f64()?;
    // 这里输出的是策略信号基差，不是原始 `poly_price - cex_price`。
    let mut proxy =
        0.5 + (cex_mid - engine_config.cex_anchor_price) / engine_config.cex_price_scale.max(1.0);
    if let Some(rate) = observed.funding_rate {
        proxy += rate * engine_config.funding_rate_weight;
    }
    if let Some(prev) = observed.previous_cex_mid.and_then(|value| value.to_f64()) {
        if prev.abs() > f64::EPSILON {
            proxy += ((cex_mid - prev) / prev) * engine_config.momentum_weight;
        }
    }
    Some((poly - proxy.clamp(0.0, 1.0)).clamp(-1.0, 1.0))
}

async fn apply_execution_events(
    risk: &mut InMemoryRiskManager,
    stats: &mut SimStats,
    recent_events: &mut VecDeque<SimEventLog>,
    events: Vec<ExecutionEvent>,
) -> Result<()> {
    for event in events {
        match &event {
            ExecutionEvent::OrderSubmitted {
                exchange, response, ..
            } => {
                stats.order_submitted += 1;
                push_event(
                    recent_events,
                    "execution",
                    format!(
                        "submitted {:?} {:?}",
                        exchange, response.exchange_order_id.0
                    ),
                );
            }
            ExecutionEvent::OrderFilled(fill) => {
                stats.fills += 1;
                risk.on_fill(fill).await?;
                push_event(
                    recent_events,
                    "execution",
                    format!(
                        "filled {:?} {:?} {} @ {}",
                        fill.exchange,
                        fill.side,
                        quantity_to_string(fill.quantity),
                        fill.price.0.normalize()
                    ),
                );
            }
            ExecutionEvent::OrderCancelled {
                exchange, order_id, ..
            } => {
                stats.order_cancelled += 1;
                push_event(
                    recent_events,
                    "execution",
                    format!("cancelled {:?} {}", exchange, order_id.0),
                );
            }
            ExecutionEvent::HedgeStateChanged {
                old_state,
                new_state,
                ..
            } => {
                stats.state_changes += 1;
                push_event(
                    recent_events,
                    "state",
                    format!("hedge {:?} -> {:?}", old_state, new_state),
                );
            }
            ExecutionEvent::ReconcileRequired { reason, .. } => {
                push_event(recent_events, "reconcile", reason.clone());
            }
        }
    }
    Ok(())
}

fn summarize_market_event(event: &MarketDataEvent) -> String {
    match event {
        MarketDataEvent::OrderBookUpdate { snapshot } => {
            format!(
                "orderbook {:?} {:?}",
                snapshot.exchange, snapshot.instrument
            )
        }
        MarketDataEvent::TradeUpdate {
            exchange,
            instrument,
            price,
            ..
        } => format!(
            "trade {:?} {:?} @ {}",
            exchange,
            instrument,
            price.0.normalize()
        ),
        MarketDataEvent::FundingRate { exchange, rate, .. } => {
            format!("funding {:?} {}", exchange, rate.normalize())
        }
        MarketDataEvent::MarketLifecycle { phase, .. } => format!("phase {:?}", phase),
        MarketDataEvent::ConnectionEvent { exchange, status } => {
            format!("connection {:?} {:?}", exchange, status)
        }
    }
}

fn summarize_signal(signal: &ArbSignalEvent) -> String {
    let action = match &signal.action {
        ArbSignalAction::BasisLong { .. } => "basis_long",
        ArbSignalAction::BasisShort { .. } => "basis_short",
        ArbSignalAction::DeltaRebalance { .. } => "delta_rebalance",
        ArbSignalAction::NegRiskArb { .. } => "neg_risk_arb",
        ArbSignalAction::ClosePosition { .. } => "close_position",
    };
    format!("{action} {}", signal.signal_id)
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
        "tick={} market={} phase={} connections={} poly_yes={:?} cex_mid={:?} signal_basis={:?} hedge={} last_signal={:?} open_orders={} active_dmm_orders={} signals={}/{} submitted={} cancelled={} fills={} positions={} exposure={} pnl={} breaker={}",
        snapshot.tick_index,
        snapshot.market,
        snapshot.market_phase,
        format_connections(&snapshot.connections),
        snapshot.poly_yes_mid,
        snapshot.cex_mid,
        snapshot.current_basis,
        snapshot.hedge_state,
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
        snapshot.breaker,
    );
}

fn print_artifact_table(artifact: &SimArtifact, artifact_path: &PathBuf) {
    print_report_title("模拟盘会话回看");

    print_report_section("会话概览");
    print_report_kv("结果文件", artifact_path.display().to_string());
    print_report_kv("环境", artifact.env.as_str());
    print_report_kv("市场", artifact.market.as_str());
    print_report_kv("对冲标的", artifact.final_snapshot.hedge_symbol.as_str());
    print_report_kv("场景", artifact.scenario.as_str());
    print_report_kv("已处理 ticks", artifact.ticks_processed.to_string());
    print_report_kv("tick 间隔", format!("{} ms", artifact.tick_interval_ms));
    print_report_kv("快照数量", artifact.snapshots.len().to_string());

    print_report_section("最终状态");
    print_report_kv("市场阶段", artifact.final_snapshot.market_phase.as_str());
    print_report_kv(
        "连接状态",
        format_connections(&artifact.final_snapshot.connections),
    );
    print_report_kv("对冲状态", artifact.final_snapshot.hedge_state.as_str());
    print_report_kv(
        "最后信号",
        artifact
            .final_snapshot
            .last_signal_id
            .as_deref()
            .unwrap_or("-"),
    );
    print_report_kv("熔断状态", artifact.final_snapshot.breaker.as_str());

    print_report_section("行情观察");
    print_report_kv(
        "Poly Yes 中间价",
        format_option_f64(artifact.final_snapshot.poly_yes_mid),
    );
    print_report_kv(
        "CEX 中间价",
        format_option_f64(artifact.final_snapshot.cex_mid),
    );
    print_report_kv(
        "信号基差",
        format_option_f64(artifact.final_snapshot.current_basis),
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
        println!("  (none)");
    } else {
        for (idx, position) in artifact.final_snapshot.positions.iter().enumerate() {
            println!(
                "  {}. {} / {} / {} | qty={} | entry={} | realized={} | unrealized={}",
                idx + 1,
                position.exchange,
                position.instrument,
                position.side,
                position.quantity,
                position.entry_price,
                position.realized_pnl,
                position.unrealized_pnl,
            );
        }
    }

    print_report_section("挂单明细");
    if artifact.open_orders.is_empty() {
        println!("  (none)");
    } else {
        for (idx, order) in artifact.open_orders.iter().enumerate() {
            println!(
                "  {}. {} | {} | {} | {} | ts_ms={}",
                idx + 1,
                order.exchange_order_id,
                order.exchange,
                order.symbol,
                order.status,
                order.timestamp_ms,
            );
        }
    }

    print_report_section("最近事件");
    if artifact.recent_events.is_empty() {
        println!("  (none)");
    } else {
        for (idx, event) in artifact.recent_events.iter().enumerate() {
            println!("  {}. [{}] {}", idx + 1, event.kind, event.summary);
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
        None => "-".to_owned(),
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
        .with_context(|| format!("failed to create sim artifact dir `{}`", path.display()))?;
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
    fs::write(path, payload)
        .with_context(|| format!("failed to write sim artifact `{}`", path.display()))
}

fn format_connections(connections: &[String]) -> String {
    if connections.is_empty() {
        "n/a".to_owned()
    } else {
        connections.join(",")
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
