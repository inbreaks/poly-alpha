use anyhow::{anyhow, Context, Result};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast::error::TryRecvError;
use tokio::time::{sleep, Duration};

use polyalpha_core::{
    create_channels, AlphaEngine, ArbSignalAction, ArbSignalEvent, ConnectionStatus, EngineParams,
    Exchange, ExecutionEvent, InstrumentKind, MarketConfig, MarketDataEvent, MarketDataSource,
    MarketPhase, PolyShares, Position, RiskManager, Settings, Symbol, SymbolRegistry, TokenSide,
    VenueQuantity,
};
use polyalpha_data::{
    BinanceFuturesDataSource, DataManager, MarketDataNormalizer, OkxMarketDataSource,
    PolymarketLiveDataSource,
};
use polyalpha_engine::{SimpleAlphaEngine, SimpleEngineConfig};
use polyalpha_executor::{dry_run::DryRunOrderSnapshot, DryRunExecutor, ExecutionManager};
use polyalpha_risk::{InMemoryRiskManager, RiskLimits};

use crate::args::PaperInspectFormat;
use crate::commands::{select_market, signal_allowed};

const MAX_EVENT_LOGS: usize = 48;
const REPORT_WIDTH: usize = 78;

#[derive(Default)]
struct PaperStats {
    ticks_processed: usize,
    market_events: usize,
    signals_seen: usize,
    signals_rejected: usize,
    order_submitted: usize,
    order_cancelled: usize,
    fills: usize,
    state_changes: usize,
    poll_errors: usize,
}

#[derive(Clone, Debug, Default)]
struct ObservedState {
    poly_yes_mid: Option<Decimal>,
    poly_no_mid: Option<Decimal>,
    cex_mid: Option<Decimal>,
    market_phase: Option<MarketPhase>,
    connections: HashMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PaperEventLog {
    kind: String,
    summary: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PaperPositionView {
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
struct PaperOrderView {
    exchange_order_id: String,
    exchange: String,
    symbol: String,
    status: String,
    timestamp_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PaperSnapshot {
    tick_index: usize,
    market: String,
    hedge_exchange: String,
    hedge_symbol: String,
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
    poll_errors: usize,
    total_exposure_usd: String,
    daily_pnl_usd: String,
    breaker: String,
    positions: Vec<PaperPositionView>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PaperArtifact {
    env: String,
    market_index: usize,
    market: String,
    hedge_exchange: String,
    poll_interval_ms: u64,
    depth: u16,
    include_funding: bool,
    ticks_processed: usize,
    final_snapshot: PaperSnapshot,
    open_orders: Vec<PaperOrderView>,
    recent_events: Vec<PaperEventLog>,
    snapshots: Vec<PaperSnapshot>,
}

enum CexLiveSource {
    Binance(BinanceFuturesDataSource),
    Okx(OkxMarketDataSource),
}

impl CexLiveSource {
    async fn connect(&mut self) -> Result<()> {
        match self {
            Self::Binance(source) => source.connect().await?,
            Self::Okx(source) => source.connect().await?,
        }
        Ok(())
    }

    async fn subscribe_market(&self, symbol: &Symbol) -> Result<()> {
        match self {
            Self::Binance(source) => source.subscribe_market(symbol).await?,
            Self::Okx(source) => source.subscribe_market(symbol).await?,
        }
        Ok(())
    }

    fn connection_status(&self) -> ConnectionStatus {
        match self {
            Self::Binance(source) => source.connection_status(),
            Self::Okx(source) => source.connection_status(),
        }
    }

    async fn poll_symbol(
        &self,
        symbol: &Symbol,
        depth: u16,
        include_funding: bool,
    ) -> Result<usize> {
        let mut published = 0usize;
        match self {
            Self::Binance(source) => {
                published += source
                    .fetch_and_publish_orderbook_by_symbol(symbol, depth)
                    .await?;
                if include_funding {
                    published += source
                        .fetch_and_publish_funding_and_mark_by_symbol(symbol)
                        .await?;
                }
            }
            Self::Okx(source) => {
                published += source
                    .fetch_and_publish_orderbook_by_symbol(symbol, depth)
                    .await?;
                if include_funding {
                    published += source
                        .fetch_and_publish_funding_and_mark_by_symbol(symbol)
                        .await?;
                }
            }
        }
        Ok(published)
    }
}

pub async fn run_paper(
    env: &str,
    market_index: usize,
    poll_interval_ms: u64,
    print_every: usize,
    max_ticks: usize,
    depth: u16,
    include_funding: bool,
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

    let mut poly_source = PolymarketLiveDataSource::new(
        manager.clone(),
        settings.polymarket.clob_api_url.clone(),
        settings.strategy.settlement.clone(),
    );
    let mut cex_source = build_cex_source(manager.clone(), &market, &settings);
    poly_source.connect().await?;
    cex_source.connect().await?;
    poly_source.subscribe_market(&market.symbol).await?;
    cex_source.subscribe_market(&market.symbol).await?;

    let mut market_data_rx = channels.market_data_tx.subscribe();
    let engine_config = build_paper_engine_config(&settings);
    let mut engine =
        SimpleAlphaEngine::with_markets(engine_config.clone(), settings.markets.clone());
    engine.update_params(EngineParams {
        basis_entry_zscore: None,
        basis_exit_zscore: None,
        max_position_usd: Some(settings.strategy.basis.max_position_usd),
    });

    let executor = DryRunExecutor::new();
    let mut execution = ExecutionManager::with_symbol_registry(executor.clone(), registry.clone());
    let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
    let mut observed = ObservedState::default();
    let mut stats = PaperStats::default();
    let mut recent_events = VecDeque::with_capacity(MAX_EVENT_LOGS);
    let mut snapshots = Vec::new();
    let mut printed_placeholder_warning = false;

    loop {
        stats.ticks_processed += 1;
        let tick_index = stats.ticks_processed;
        let now_ms = now_millis();
        let now_secs = now_ms / 1000;

        if let Err(err) = manager.publish_market_lifecycle(
            &market.symbol,
            now_secs,
            now_ms,
            &settings.strategy.settlement,
        ) {
            stats.poll_errors += 1;
            push_event(
                &mut recent_events,
                "poll_error",
                format!("publish lifecycle failed: {err}"),
            );
        }

        if looks_like_placeholder_id(&market.poly_ids.yes_token_id)
            || looks_like_placeholder_id(&market.poly_ids.no_token_id)
        {
            if !printed_placeholder_warning {
                printed_placeholder_warning = true;
                push_event(
                    &mut recent_events,
                    "warning",
                    "Polymarket token_id 还是占位值，已跳过双边拉取；先执行 markets discover-btc 生成 live overlay".to_owned(),
                );
            }
        } else {
            for token_side in [TokenSide::Yes, TokenSide::No] {
                if let Err(err) = poly_source
                    .fetch_and_publish_orderbook_by_symbol(&market.symbol, token_side)
                    .await
                {
                    stats.poll_errors += 1;
                    push_event(
                        &mut recent_events,
                        "poll_error",
                        format!("polymarket {:?} poll failed: {err}", token_side),
                    );
                }
            }
        }

        if let Err(err) = cex_source
            .poll_symbol(&market.symbol, depth, include_funding)
            .await
        {
            stats.poll_errors += 1;
            push_event(
                &mut recent_events,
                "poll_error",
                format!("cex poll failed: {err}"),
            );
        }

        update_connection_state(
            &mut observed,
            Exchange::Polymarket,
            poly_source.connection_status(),
        );
        update_connection_state(
            &mut observed,
            market.hedge_exchange,
            cex_source.connection_status(),
        );

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
            &market, tick_index, &engine, &observed, &stats, &risk, &execution, &executor,
        )?;
        if tick_index % print_every.max(1) == 0 {
            print_snapshot(&snapshot, json);
            if !json {
                if let Some(last_event) = recent_events.back() {
                    println!("last_event=[{}] {}", last_event.kind, last_event.summary);
                }
            }
        }
        snapshots.push(snapshot);

        if max_ticks > 0 && tick_index >= max_ticks {
            break;
        }
        if poll_interval_ms > 0 {
            sleep(Duration::from_millis(poll_interval_ms)).await;
        }
    }

    let final_snapshot = snapshots
        .last()
        .cloned()
        .ok_or_else(|| anyhow!("paper session did not produce snapshots"))?;
    let open_orders = executor
        .open_order_snapshots()?
        .into_iter()
        .map(order_view_from_snapshot)
        .collect();
    let artifact = PaperArtifact {
        env: env.to_owned(),
        market_index,
        market: market.symbol.0.clone(),
        hedge_exchange: format!("{:?}", market.hedge_exchange),
        poll_interval_ms,
        depth,
        include_funding,
        ticks_processed: stats.ticks_processed,
        final_snapshot,
        open_orders,
        recent_events: recent_events.into_iter().collect(),
        snapshots,
    };
    let artifact_path = artifact_path(&settings, env)?;
    persist_artifact(&artifact_path, &artifact)?;
    println!("paper artifact written: {}", artifact_path.display());

    Ok(())
}

pub fn inspect_paper(env: &str, format: PaperInspectFormat) -> Result<()> {
    let settings =
        Settings::load(env).with_context(|| format!("failed to load config env `{env}`"))?;
    let artifact_path = resolve_artifact_for_read(&settings, env)?;
    let raw = fs::read_to_string(&artifact_path).with_context(|| {
        format!(
            "failed to read paper artifact `{}`",
            artifact_path.display()
        )
    })?;
    let artifact: PaperArtifact = serde_json::from_str(&raw).with_context(|| {
        format!(
            "failed to parse paper artifact `{}`",
            artifact_path.display()
        )
    })?;

    match format {
        PaperInspectFormat::Table => print_artifact_table(&artifact, &artifact_path),
        PaperInspectFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&artifact)?);
        }
    }

    Ok(())
}

fn build_paper_engine_config(settings: &Settings) -> SimpleEngineConfig {
    let mut config = SimpleEngineConfig::default();
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

    config.min_signal_samples = settings.strategy.basis.min_warmup_samples.max(2);
    config.rolling_window_minutes =
        ((settings.strategy.basis.rolling_window_secs / 60) as usize).max(2);
    config.entry_z = configured_entry.abs().max(0.1);
    config.exit_z = configured_exit.abs().min(config.entry_z).max(0.05);
    config.position_notional_usd = settings.strategy.basis.max_position_usd;
    config.cex_hedge_ratio = 1.0;
    config.dmm_quote_size = PolyShares::ZERO;
    config
}

fn build_cex_source(
    manager: DataManager,
    market: &MarketConfig,
    settings: &Settings,
) -> CexLiveSource {
    match market.hedge_exchange {
        Exchange::Binance => CexLiveSource::Binance(BinanceFuturesDataSource::new(
            manager,
            settings.binance.rest_url.clone(),
        )),
        Exchange::Okx => CexLiveSource::Okx(OkxMarketDataSource::new(
            manager,
            settings.okx.rest_url.clone(),
        )),
        _ => CexLiveSource::Binance(BinanceFuturesDataSource::new(
            manager,
            settings.binance.rest_url.clone(),
        )),
    }
}

fn looks_like_placeholder_id(value: &str) -> bool {
    value.trim().is_empty() || value.contains("...")
}

fn update_connection_state(
    observed: &mut ObservedState,
    exchange: Exchange,
    status: ConnectionStatus,
) {
    observed
        .connections
        .insert(format!("{exchange:?}"), format!("{status:?}"));
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

fn build_snapshot(
    market: &MarketConfig,
    tick_index: usize,
    engine: &SimpleAlphaEngine,
    observed: &ObservedState,
    stats: &PaperStats,
    risk: &InMemoryRiskManager,
    execution: &ExecutionManager<DryRunExecutor>,
    executor: &DryRunExecutor,
) -> Result<PaperSnapshot> {
    let risk_snapshot = risk.build_snapshot(now_millis());
    let positions = risk_snapshot
        .positions
        .values()
        .map(position_view_from_position)
        .collect::<Vec<_>>();
    let signal_snapshot = engine.basis_snapshot(&market.symbol);
    Ok(PaperSnapshot {
        tick_index,
        market: market.symbol.0.clone(),
        hedge_exchange: format!("{:?}", market.hedge_exchange),
        hedge_symbol: market.cex_symbol.clone(),
        market_phase: observed
            .market_phase
            .clone()
            .map(|phase| format!("{phase:?}"))
            .unwrap_or_else(|| "Unknown".to_owned()),
        connections: sorted_connections(&observed.connections),
        poly_yes_mid: observed.poly_yes_mid.and_then(|value| value.to_f64()),
        poly_no_mid: observed.poly_no_mid.and_then(|value| value.to_f64()),
        cex_mid: observed.cex_mid.and_then(|value| value.to_f64()),
        current_basis: signal_snapshot
            .as_ref()
            .map(|snapshot| snapshot.signal_basis),
        signal_token_side: signal_snapshot
            .as_ref()
            .map(|snapshot| format!("{:?}", snapshot.token_side)),
        current_zscore: signal_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.z_score),
        current_fair_value: signal_snapshot.as_ref().map(|snapshot| snapshot.fair_value),
        current_delta: signal_snapshot.as_ref().map(|snapshot| snapshot.delta),
        cex_reference_price: signal_snapshot
            .as_ref()
            .map(|snapshot| snapshot.cex_reference_price),
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
        poll_errors: stats.poll_errors,
        total_exposure_usd: risk_snapshot.total_exposure_usd.0.normalize().to_string(),
        daily_pnl_usd: risk_snapshot.daily_pnl.0.normalize().to_string(),
        breaker: format!("{:?}", risk_snapshot.circuit_breaker),
        positions,
    })
}

async fn apply_execution_events(
    risk: &mut InMemoryRiskManager,
    stats: &mut PaperStats,
    recent_events: &mut VecDeque<PaperEventLog>,
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

fn push_event(events: &mut VecDeque<PaperEventLog>, kind: &str, summary: String) {
    if events.len() == MAX_EVENT_LOGS {
        events.pop_front();
    }
    events.push_back(PaperEventLog {
        kind: kind.to_owned(),
        summary,
    });
}

fn print_snapshot(snapshot: &PaperSnapshot, json: bool) {
    if json {
        match serde_json::to_string(snapshot) {
            Ok(line) => println!("{line}"),
            Err(_) => println!("{{\"tick_index\":{}}}", snapshot.tick_index),
        }
        return;
    }

    println!(
        "tick={} market={} phase={} connections={} poly_yes={:?} poly_no={:?} cex_mid={:?} token_side={:?} signal_basis={:?} z={:?} fair={:?} delta={:?} cex_ref={:?} hedge={} last_signal={:?} open_orders={} active_dmm_orders={} signals={}/{} submitted={} cancelled={} fills={} poll_errors={} positions={} exposure={} pnl={} breaker={}",
        snapshot.tick_index,
        snapshot.market,
        snapshot.market_phase,
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
        snapshot.hedge_state,
        snapshot.last_signal_id,
        snapshot.open_orders,
        snapshot.active_dmm_orders,
        snapshot.signals_seen,
        snapshot.signals_rejected,
        snapshot.order_submitted,
        snapshot.order_cancelled,
        snapshot.fills,
        snapshot.poll_errors,
        snapshot.positions.len(),
        snapshot.total_exposure_usd,
        snapshot.daily_pnl_usd,
        snapshot.breaker,
    );
}

fn print_artifact_table(artifact: &PaperArtifact, artifact_path: &PathBuf) {
    print_report_title("纸面盘会话回看");

    print_report_section("会话概览");
    print_report_kv("结果文件", artifact_path.display().to_string());
    print_report_kv("环境", artifact.env.as_str());
    print_report_kv("市场", artifact.market.as_str());
    print_report_kv("对冲交易所", artifact.hedge_exchange.as_str());
    print_report_kv("轮询间隔", format!("{} ms", artifact.poll_interval_ms));
    print_report_kv("订单簿深度", artifact.depth.to_string());
    print_report_kv(
        "轮询 funding/mark",
        if artifact.include_funding {
            "是"
        } else {
            "否"
        },
    );
    print_report_kv("已处理 ticks", artifact.ticks_processed.to_string());
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
        "Poly No 中间价",
        format_option_f64(artifact.final_snapshot.poly_no_mid),
    );
    print_report_kv(
        "CEX 中间价",
        format_option_f64(artifact.final_snapshot.cex_mid),
    );
    print_report_kv(
        "当前信号腿",
        artifact
            .final_snapshot
            .signal_token_side
            .as_deref()
            .unwrap_or("-"),
    );
    print_report_kv(
        "信号基差",
        format_option_f64(artifact.final_snapshot.current_basis),
    );
    print_report_kv(
        "当前 z-score",
        format_option_f64(artifact.final_snapshot.current_zscore),
    );
    print_report_kv(
        "当前 fair value",
        format_option_f64(artifact.final_snapshot.current_fair_value),
    );
    print_report_kv(
        "当前 delta",
        format_option_f64(artifact.final_snapshot.current_delta),
    );
    print_report_kv(
        "上一根已完成 CEX close",
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
        "轮询错误数",
        artifact.final_snapshot.poll_errors.to_string(),
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
    path.push("paper");
    fs::create_dir_all(&path)
        .with_context(|| format!("failed to create paper artifact dir `{}`", path.display()))?;
    path.push(format!("{env}-last-run.json"));
    Ok(path)
}

fn resolve_artifact_for_read(settings: &Settings, env: &str) -> Result<PathBuf> {
    let current = artifact_path(settings, env)?;
    if current.exists() {
        return Ok(current);
    }

    let mut legacy = PathBuf::from(&settings.general.data_dir);
    legacy.push("paper");
    legacy.push(format!("{env}-last-session.json"));
    if legacy.exists() {
        return Ok(legacy);
    }

    Ok(current)
}

fn persist_artifact(path: &PathBuf, artifact: &PaperArtifact) -> Result<()> {
    let payload = serde_json::to_string_pretty(artifact)?;
    fs::write(path, payload)
        .with_context(|| format!("failed to write paper artifact `{}`", path.display()))
}

fn format_connections(connections: &[String]) -> String {
    if connections.is_empty() {
        "n/a".to_owned()
    } else {
        connections.join(",")
    }
}

fn position_view_from_position(position: &Position) -> PaperPositionView {
    PaperPositionView {
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

fn order_view_from_snapshot(order: DryRunOrderSnapshot) -> PaperOrderView {
    PaperOrderView {
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

fn now_millis() -> u64 {
    let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    duration.as_millis() as u64
}
