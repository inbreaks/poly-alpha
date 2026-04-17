use std::collections::{BTreeMap, HashMap, HashSet};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{interval, Instant, MissedTickBehavior};
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use polyalpha_audit::{
    AuditAnomalyEvent, AuditCheckpoint, AuditCounterSnapshot, AuditEventKind, AuditEventPayload,
    AuditSessionManifest, AuditSessionStatus, AuditSessionSummary, AuditSeverity, AuditWarehouse,
    AuditWriter, NewAuditEvent, PulseBookLevelAuditRow, PulseExecutionContext,
    PulseExecutionRouteContext, PulseMarketTapeAuditEvent,
};
use polyalpha_core::{
    asset_key_from_cex_symbol, AsyncClassification, ConnectionInfo, ConnectionStatus,
    EvaluableStatus, Exchange, InstrumentKind, MarketDataEvent, MarketRetentionReason, MarketTier,
    MarketView, Message as MonitorMessage, MonitorAssetStrategyConfig, MonitorSocketServer,
    MonitorState, MonitorStrategyConfig, OrderBookSnapshot, PolyShares, Price, PriceLevel,
    Settings, SymbolRegistry, TradingMode, VenueQuantity,
};
use polyalpha_data::{
    BinanceFuturesDataSource, DataManager, DeribitOptionsClient, DeribitTickerMessage,
    PolyBookLevel, PolyBookUpdate, PolymarketLiveDataSource,
};
use polyalpha_pulse::anchor::deribit::DeribitAnchorProvider;
use polyalpha_pulse::audit::{PulseAuditEmitter, PulseAuditRecord};
use polyalpha_pulse::model::PulseAsset;
use polyalpha_pulse::runtime::{
    ActualPositionSync, PulseBookFeedStub, PulseExecutionMode, PulseRuntimeBuilder,
};

use crate::args::LiveExecutorMode;
use crate::paper::{
    connect_websocket, log_ws_warning, run_ws_future_with_timeout, WsProxySettings,
};
use crate::runtime::{
    apply_orderbook_event, build_data_manager, build_execution_stack, RuntimeExecutionMode,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PulseInspectRouteView {
    asset: String,
    anchor_provider: String,
    provider_kind: String,
    hedge_venue: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PulseInspectView {
    env: String,
    runtime_enabled: bool,
    max_concurrent_sessions_per_asset: usize,
    max_holding_secs: u64,
    min_opening_notional_usd: String,
    supported_modes: Vec<String>,
    enabled_assets: Vec<String>,
    routes: Vec<PulseInspectRouteView>,
}

const PULSE_RUNTIME_AUDIT_BUFFER_CAPACITY: usize = 4_096;
const PULSE_MARKET_TAPE_BUFFER_CAPACITY: usize = 8_192;

struct PulseAuditRuntime {
    data_dir: String,
    writer: AuditWriter,
    warehouse: AuditWarehouse,
    summary: AuditSessionSummary,
    tick_index: u64,
    market_events: u64,
    snapshot_interval_ms: u64,
    checkpoint_interval_ms: u64,
    warehouse_sync_interval_ms: u64,
    last_summary_write_ms: u64,
    last_checkpoint_ms: u64,
    last_warehouse_sync_ms: u64,
    warehouse_sync_degraded: bool,
    runtime_events_rx: mpsc::Receiver<PulseAuditRecord>,
    market_tape_rx: mpsc::Receiver<NewAuditEvent>,
}

impl PulseAuditRuntime {
    fn start(
        settings: &Settings,
        env: &str,
        mode: PulseExecutionMode,
        now_ms: u64,
        runtime_events_rx: mpsc::Receiver<PulseAuditRecord>,
        market_tape_rx: mpsc::Receiver<NewAuditEvent>,
    ) -> Result<Option<Self>> {
        if !settings.audit.enabled {
            return Ok(None);
        }

        let manifest = AuditSessionManifest {
            version: 1,
            session_id: Uuid::new_v4().to_string(),
            env: env.to_owned(),
            mode: match mode {
                PulseExecutionMode::Paper => TradingMode::Paper,
                PulseExecutionMode::LiveMock => TradingMode::Live,
            },
            started_at_ms: now_ms,
            market_count: settings.markets.len(),
            markets: settings
                .markets
                .iter()
                .map(|market| market.symbol.0.clone())
                .collect(),
        };
        let warehouse = AuditWarehouse::new(&settings.general.data_dir)?;
        let mut writer = AuditWriter::create(
            &settings.general.data_dir,
            manifest.clone(),
            settings.audit.raw_segment_max_bytes,
        )?;
        writer.write_execution_context(&build_pulse_execution_context(
            &manifest.session_id,
            env,
            mode,
            settings,
        ))?;
        let summary = AuditSessionSummary::new_running(&manifest);

        Ok(Some(Self {
            data_dir: settings.general.data_dir.clone(),
            writer,
            warehouse,
            summary,
            tick_index: 0,
            market_events: 0,
            snapshot_interval_ms: settings.audit.snapshot_interval_secs.max(1) * 1_000,
            checkpoint_interval_ms: settings.audit.checkpoint_interval_secs.max(1) * 1_000,
            warehouse_sync_interval_ms: settings.audit.warehouse_sync_interval_secs.max(1) * 1_000,
            last_summary_write_ms: now_ms,
            last_checkpoint_ms: now_ms,
            last_warehouse_sync_ms: now_ms,
            warehouse_sync_degraded: false,
            runtime_events_rx,
            market_tape_rx,
        }))
    }

    fn note_market_event(&mut self) {
        self.market_events = self.market_events.saturating_add(1);
    }

    fn sync_runtime_state(
        &mut self,
        runtime: &polyalpha_pulse::runtime::PulseRuntime,
        monitor_state: &MonitorState,
        now_ms: u64,
    ) -> Result<()> {
        self.tick_index = self.tick_index.saturating_add(1);
        self.append_new_pulse_records()?;
        self.refresh_summary(runtime, monitor_state, now_ms);

        let checkpoint_due =
            now_ms.saturating_sub(self.last_checkpoint_ms) >= self.checkpoint_interval_ms;
        let summary_due =
            now_ms.saturating_sub(self.last_summary_write_ms) >= self.snapshot_interval_ms;
        if checkpoint_due {
            self.summary.latest_checkpoint_ms = Some(now_ms);
            self.writer.write_checkpoint(&AuditCheckpoint {
                session_id: self.summary.session_id.clone(),
                updated_at_ms: now_ms,
                tick_index: self.tick_index,
                counters: self.summary.counters.clone(),
                monitor_state: monitor_state.clone(),
            })?;
            self.writer.write_summary(&self.summary)?;
            self.last_checkpoint_ms = now_ms;
            self.last_summary_write_ms = now_ms;
        } else if summary_due {
            self.writer.write_summary(&self.summary)?;
            self.last_summary_write_ms = now_ms;
        }

        if now_ms.saturating_sub(self.last_warehouse_sync_ms) >= self.warehouse_sync_interval_ms {
            self.sync_warehouse_best_effort(now_ms)?;
            self.last_warehouse_sync_ms = now_ms;
        }

        Ok(())
    }

    fn finalize(
        &mut self,
        runtime: &polyalpha_pulse::runtime::PulseRuntime,
        monitor_state: &MonitorState,
        now_ms: u64,
        status: AuditSessionStatus,
    ) -> Result<()> {
        self.append_new_pulse_records()?;
        self.refresh_summary(runtime, monitor_state, now_ms);
        self.summary.status = status;
        self.summary.updated_at_ms = now_ms;
        self.summary.ended_at_ms = Some(now_ms);
        self.summary.latest_checkpoint_ms = Some(now_ms);

        self.writer.write_checkpoint(&AuditCheckpoint {
            session_id: self.summary.session_id.clone(),
            updated_at_ms: now_ms,
            tick_index: self.tick_index,
            counters: self.summary.counters.clone(),
            monitor_state: monitor_state.clone(),
        })?;
        self.writer.write_summary(&self.summary)?;
        self.sync_warehouse_best_effort(now_ms)?;
        self.last_checkpoint_ms = now_ms;
        self.last_summary_write_ms = now_ms;
        self.last_warehouse_sync_ms = now_ms;
        Ok(())
    }

    fn append_new_pulse_records(&mut self) -> Result<()> {
        loop {
            match self.runtime_events_rx.try_recv() {
                Ok(record) => {
                    if pulse_audit_record_should_persist(&record) {
                        self.writer.append_event(pulse_audit_event(&record))?;
                    }
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
                | Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
            }
        }
        loop {
            match self.market_tape_rx.try_recv() {
                Ok(event) => {
                    if pulse_new_event_should_persist(&event) {
                        self.writer.append_event(event)?;
                    }
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
                | Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
            }
        }
        Ok(())
    }

    fn refresh_summary(
        &mut self,
        runtime: &polyalpha_pulse::runtime::PulseRuntime,
        monitor_state: &MonitorState,
        now_ms: u64,
    ) {
        let closed_session_count = runtime
            .pulse_session_rows()
            .iter()
            .filter(|row| row.closed_at_ms.is_some())
            .count() as u64;

        self.summary.status = AuditSessionStatus::Running;
        self.summary.updated_at_ms = now_ms;
        self.summary.latest_tick_index = self.tick_index;
        self.summary.latest_equity = Some(monitor_state.performance.equity);
        self.summary.latest_total_pnl_usd = Some(monitor_state.performance.total_pnl_usd);
        self.summary.latest_today_pnl_usd = Some(monitor_state.performance.today_pnl_usd);
        self.summary.latest_total_exposure_usd = Some(
            monitor_state
                .positions
                .iter()
                .map(|position| {
                    (position.poly_current_price * position.poly_quantity.abs())
                        + (position.cex_current_price * position.cex_quantity.abs())
                })
                .sum(),
        );
        let signal_stats = runtime.signal_stats();
        let evaluation_stats = runtime.evaluation_stats();
        self.summary.counters = AuditCounterSnapshot {
            ticks_processed: self.tick_index,
            market_events: self.market_events,
            signals_seen: signal_stats.seen,
            signals_rejected: signal_stats.rejected,
            evaluation_attempts: evaluation_stats.attempts,
            state_changes: self.writer.event_count(),
            trades_closed: closed_session_count,
            ..AuditCounterSnapshot::default()
        };
        self.summary.rejection_reasons = signal_stats.rejection_reasons.clone();
        self.summary.pulse_session_summaries = runtime.pulse_session_rows();
    }

    fn sync_warehouse(&self) -> Result<()> {
        self.warehouse
            .sync_session(&self.data_dir, &self.summary.session_id)
            .with_context(|| format!("同步审计会话 `{}` 到仓库失败", self.summary.session_id))
    }

    fn sync_warehouse_best_effort(&mut self, now_ms: u64) -> Result<()> {
        match self.sync_warehouse() {
            Ok(()) => {
                self.warehouse_sync_degraded = false;
                Ok(())
            }
            Err(err) => {
                let message = format!("审计仓库同步失败，已保留原始审计文件：{err}");
                if !self.warehouse_sync_degraded {
                    self.writer.append_event(NewAuditEvent {
                        timestamp_ms: now_ms,
                        kind: AuditEventKind::Anomaly,
                        symbol: None,
                        signal_id: None,
                        correlation_id: None,
                        gate: None,
                        result: None,
                        reason: Some("warehouse_sync_degraded".to_owned()),
                        summary: message.clone(),
                        payload: AuditEventPayload::Anomaly(AuditAnomalyEvent {
                            severity: AuditSeverity::Warning,
                            code: "warehouse_sync_degraded".to_owned(),
                            message: message.clone(),
                            symbol: None,
                            signal_id: None,
                            correlation_id: None,
                        }),
                    })?;
                }
                eprintln!("{message}");
                self.warehouse_sync_degraded = true;
                Ok(())
            }
        }
    }
}

fn pulse_audit_event(record: &PulseAuditRecord) -> NewAuditEvent {
    let summary = match &record.payload {
        polyalpha_audit::AuditEventPayload::PulseLifecycle(event) => {
            format!("pulse {} {}", event.asset, event.state)
        }
        polyalpha_audit::AuditEventPayload::PulseBookTape(event) => {
            format!("pulse tape {} {}", event.asset, event.book.instrument)
        }
        polyalpha_audit::AuditEventPayload::PulseMarketTape(event) => {
            format!("pulse market tape {} {}", event.asset, event.instrument)
        }
        polyalpha_audit::AuditEventPayload::PulseSignalSnapshot(event) => format!(
            "pulse signal {} {}",
            event.asset,
            event.admission_result.as_str()
        ),
        polyalpha_audit::AuditEventPayload::PulseSessionSummary(event) => {
            format!("pulse summary {} {}", event.asset, event.state)
        }
        polyalpha_audit::AuditEventPayload::PulseAssetHealth(event) => format!(
            "pulse health {} {}",
            event.asset,
            event.status.as_deref().unwrap_or("unknown")
        ),
        _ => "pulse".to_owned(),
    };

    NewAuditEvent {
        timestamp_ms: record.timestamp_ms,
        kind: record.kind,
        symbol: None,
        signal_id: None,
        correlation_id: None,
        gate: None,
        result: None,
        reason: None,
        summary,
        payload: record.payload.clone(),
    }
}

fn pulse_audit_record_should_persist(record: &PulseAuditRecord) -> bool {
    pulse_payload_should_persist(&record.payload)
}

fn pulse_new_event_should_persist(event: &NewAuditEvent) -> bool {
    pulse_payload_should_persist(&event.payload)
}

fn pulse_payload_should_persist(payload: &AuditEventPayload) -> bool {
    !matches!(
        payload,
        AuditEventPayload::PulseAssetHealth(_)
            | AuditEventPayload::PulseMarketTape(_)
            | AuditEventPayload::PulseSignalSnapshot(_)
    )
}

fn pulse_audit_channels() -> (
    mpsc::Sender<PulseAuditRecord>,
    mpsc::Receiver<PulseAuditRecord>,
    mpsc::Sender<NewAuditEvent>,
    mpsc::Receiver<NewAuditEvent>,
) {
    let (runtime_events_tx, runtime_events_rx) = mpsc::channel(PULSE_RUNTIME_AUDIT_BUFFER_CAPACITY);
    let (market_tape_tx, market_tape_rx) = mpsc::channel(PULSE_MARKET_TAPE_BUFFER_CAPACITY);
    (
        runtime_events_tx,
        runtime_events_rx,
        market_tape_tx,
        market_tape_rx,
    )
}

fn build_runtime_audit_emitter(
    runtime_events_tx: mpsc::Sender<PulseAuditRecord>,
) -> PulseAuditEmitter {
    Arc::new(move |record| {
        let _ = runtime_events_tx.try_send(record);
    })
}

fn noop_runtime_audit_emitter() -> PulseAuditEmitter {
    Arc::new(|_record| {})
}

#[allow(dead_code)]
fn spawn_pulse_market_tape_task(
    mut market_data_rx: broadcast::Receiver<MarketDataEvent>,
    registry: SymbolRegistry,
    market_tape_tx: mpsc::Sender<NewAuditEvent>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match market_data_rx.recv().await {
                Ok(event) => {
                    if forward_market_event_to_pulse_audit(&registry, &market_tape_tx, &event)
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    })
}

async fn forward_market_event_to_pulse_audit(
    registry: &SymbolRegistry,
    market_tape_tx: &mpsc::Sender<NewAuditEvent>,
    event: &MarketDataEvent,
) -> Result<()> {
    for audit_event in pulse_market_tape_events(registry, event) {
        market_tape_tx
            .send(audit_event)
            .await
            .map_err(|_| anyhow!("pulse market tape channel closed"))?;
    }
    Ok(())
}

fn pulse_market_tape_events(
    registry: &SymbolRegistry,
    event: &MarketDataEvent,
) -> Vec<NewAuditEvent> {
    event
        .expand_for_registry(registry)
        .into_iter()
        .filter_map(|expanded| match expanded {
            MarketDataEvent::OrderBookUpdate { snapshot } => {
                pulse_market_tape_event_from_snapshot(registry, &snapshot)
            }
            _ => None,
        })
        .collect()
}

fn pulse_market_tape_event_from_snapshot(
    registry: &SymbolRegistry,
    snapshot: &OrderBookSnapshot,
) -> Option<NewAuditEvent> {
    let market = registry.get_config(&snapshot.symbol)?;
    let asset = PulseAsset::from_routing_key(&asset_key_from_cex_symbol(&market.cex_symbol))?;
    let bids = audit_book_levels(&snapshot.bids);
    let asks = audit_book_levels(&snapshot.asks);
    let payload = PulseMarketTapeAuditEvent {
        asset: asset.as_str().to_owned(),
        symbol: snapshot.symbol.0.clone(),
        venue: pulse_market_tape_venue_label(snapshot.exchange).to_owned(),
        instrument: pulse_market_tape_instrument_label(snapshot.instrument).to_owned(),
        token_side: pulse_market_tape_token_side(snapshot.instrument),
        ts_exchange_ms: snapshot.exchange_timestamp_ms,
        ts_recv_ms: snapshot.received_at_ms,
        sequence: snapshot.sequence,
        update_kind: "snapshot".to_owned(),
        top_n_depth: bids.len().max(asks.len()),
        best_bid: bids.first().map(|level| level.price.clone()),
        best_ask: asks.first().map(|level| level.price.clone()),
        mid: pulse_market_tape_mid(snapshot).map(pulse_decimal_to_string),
        last_trade_price: snapshot
            .last_trade_price
            .map(|price| pulse_decimal_to_string(price.0)),
        expiry_ts_ms: None,
        strike: None,
        option_type: None,
        mark_iv: None,
        bid_iv: None,
        ask_iv: None,
        delta: None,
        gamma: None,
        index_price: None,
        delta_bids: Vec::new(),
        delta_asks: Vec::new(),
        snapshot_bids: bids,
        snapshot_asks: asks,
    };

    Some(NewAuditEvent {
        timestamp_ms: snapshot.received_at_ms.max(snapshot.exchange_timestamp_ms),
        kind: AuditEventKind::PulseMarketTape,
        symbol: Some(snapshot.symbol.0.clone()),
        signal_id: None,
        correlation_id: None,
        gate: None,
        result: None,
        reason: None,
        summary: format!("pulse market tape {} {}", payload.asset, payload.instrument),
        payload: AuditEventPayload::PulseMarketTape(payload),
    })
}

fn audit_book_levels(levels: &[PriceLevel]) -> Vec<PulseBookLevelAuditRow> {
    levels
        .iter()
        .take(5)
        .map(|level| PulseBookLevelAuditRow {
            price: pulse_decimal_to_string(level.price.0),
            quantity: match level.quantity {
                VenueQuantity::PolyShares(shares) => pulse_decimal_to_string(shares.0),
                VenueQuantity::CexBaseQty(base_qty) => pulse_decimal_to_string(base_qty.0),
            },
        })
        .collect()
}

fn pulse_market_tape_mid(snapshot: &OrderBookSnapshot) -> Option<Decimal> {
    let bid = snapshot.bids.first()?.price.0;
    let ask = snapshot.asks.first()?.price.0;
    Some((bid + ask) / Decimal::TWO)
}

fn pulse_market_tape_venue_label(exchange: Exchange) -> &'static str {
    match exchange {
        Exchange::Polymarket => "Polymarket",
        Exchange::Binance => "Binance",
        Exchange::Okx => "Okx",
        Exchange::Deribit => "Deribit",
    }
}

fn pulse_market_tape_instrument_label(instrument: InstrumentKind) -> &'static str {
    match instrument {
        InstrumentKind::PolyYes => "poly_yes",
        InstrumentKind::PolyNo => "poly_no",
        InstrumentKind::CexPerp => "cex_perp",
    }
}

fn pulse_market_tape_token_side(instrument: InstrumentKind) -> Option<String> {
    match instrument {
        InstrumentKind::PolyYes => Some("yes".to_owned()),
        InstrumentKind::PolyNo => Some("no".to_owned()),
        InstrumentKind::CexPerp => None,
    }
}

fn pulse_decimal_to_string(value: Decimal) -> String {
    value.normalize().to_string()
}

pub async fn run_pulse_paper(env: &str, assets: &[PulseAsset]) -> Result<()> {
    run_pulse(env, assets, PulseExecutionMode::Paper, None).await
}

pub async fn run_pulse_live_mock(
    env: &str,
    assets: &[PulseAsset],
    executor_mode: LiveExecutorMode,
) -> Result<()> {
    if executor_mode != LiveExecutorMode::Mock {
        return Err(anyhow!(
            "pulse live mode currently only supports --executor-mode mock"
        ));
    }

    run_pulse(
        env,
        assets,
        PulseExecutionMode::LiveMock,
        Some(executor_mode),
    )
    .await
}

pub fn inspect_pulse(env: &str) -> Result<()> {
    let settings =
        Settings::load(env).with_context(|| format!("failed to load config env `{env}`"))?;
    let inspect = build_pulse_inspect_view(env, &settings);

    println!("pulse runtime inspect");
    println!("env: {}", inspect.env);
    println!("runtime_enabled: {}", inspect.runtime_enabled);
    println!(
        "session_limits: max_concurrent_per_asset={}, max_holding_secs={}, min_opening_notional_usd={}",
        inspect.max_concurrent_sessions_per_asset,
        inspect.max_holding_secs,
        inspect.min_opening_notional_usd
    );
    println!("supported_modes: {}", inspect.supported_modes.join(","));
    println!("enabled_assets: {}", inspect.enabled_assets.join(","));

    if inspect.routes.is_empty() {
        println!("routes: none");
    } else {
        for route in inspect.routes {
            println!(
                "route: asset={} anchor_provider={} provider_kind={} hedge_venue={}",
                route.asset, route.anchor_provider, route.provider_kind, route.hedge_venue
            );
        }
    }

    Ok(())
}

pub fn parse_pulse_assets(raw: &str) -> Result<Vec<PulseAsset>> {
    let assets = raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            PulseAsset::from_routing_key(value)
                .ok_or_else(|| anyhow!("unsupported pulse asset `{value}`"))
        })
        .collect::<Result<Vec<_>>>()?;

    if assets.is_empty() {
        return Err(anyhow!("pulse runtime requires at least one asset"));
    }

    Ok(assets)
}

async fn run_pulse(
    env: &str,
    assets: &[PulseAsset],
    mode: PulseExecutionMode,
    _executor_mode: Option<LiveExecutorMode>,
) -> Result<()> {
    let settings = filtered_pulse_settings(env, assets)?;
    if settings.markets.is_empty() {
        return Err(anyhow!(
            "pulse runtime env `{env}` has no eligible BTC/ETH markets after filtering"
        ));
    }

    let registry = SymbolRegistry::new(settings.markets.clone());
    let (market_data_tx, _) = broadcast::channel(4_096);
    let mut market_data_rx = market_data_tx.subscribe();
    let manager = build_data_manager(&registry, market_data_tx.clone());
    let executor_start_ms = current_time_ms();
    let runtime_mode = match mode {
        PulseExecutionMode::Paper => RuntimeExecutionMode::Paper,
        PulseExecutionMode::LiveMock => RuntimeExecutionMode::LiveMock,
    };
    let asset_feeds = assets
        .iter()
        .copied()
        .map(|asset| PulseBookFeedStub { asset, depth: 20 })
        .collect::<Vec<_>>();
    let (orderbook_provider, executor, _execution_manager) =
        build_execution_stack(&settings, &registry, runtime_mode, executor_start_ms, None)
            .await
            .context("build pulse execution stack")?;

    let (runtime_audit_emitter, runtime_events_rx, market_tape_rx) = if settings.audit.enabled {
        let (runtime_events_tx, runtime_events_rx, _market_tape_tx, market_tape_rx) =
            pulse_audit_channels();
        (
            build_runtime_audit_emitter(runtime_events_tx),
            Some(runtime_events_rx),
            Some(market_tape_rx),
        )
    } else {
        (noop_runtime_audit_emitter(), None, None)
    };

    let deribit_provider = build_deribit_anchor_provider(&settings, assets)
        .await
        .context("build Deribit anchor provider")?;
    let position_sync = build_pulse_position_sync(executor.clone());
    let runtime_executor: Arc<dyn polyalpha_core::OrderExecutor> = Arc::new(executor.clone());
    let mut runtime = PulseRuntimeBuilder::new(settings.clone())
        .with_anchor_provider(deribit_provider.provider.clone())
        .with_markets(settings.markets.clone())
        .with_executor(runtime_executor)
        .with_position_sync(position_sync)
        .with_poly_books(asset_feeds.clone())
        .with_binance_books(asset_feeds)
        .with_audit_emitter(runtime_audit_emitter)
        .with_execution_mode(mode)
        .build()
        .await
        .context("build pulse runtime")?;

    let (state_broadcaster, _socket_handle) = spawn_pulse_monitor_server(&settings);
    let _data_tasks =
        spawn_pulse_market_data_tasks(manager, &settings, &settings.markets, 20, deribit_provider)
            .await
            .context("spawn pulse market data tasks")?;
    let mut audit_runtime = match (runtime_events_rx, market_tape_rx) {
        (Some(runtime_events_rx), Some(market_tape_rx)) => PulseAuditRuntime::start(
            &settings,
            env,
            mode,
            executor_start_ms,
            runtime_events_rx,
            market_tape_rx,
        )
        .context("start pulse audit runtime")?,
        _ => None,
    };

    println!(
        "pulse {:?} runtime running: assets={:?}, markets={}, socket={}",
        mode,
        assets,
        settings.markets.len(),
        settings.general.monitor_socket_path
    );

    let started_at = Instant::now();
    let mut tick = interval(Duration::from_millis(250));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut last_monitor_state = MonitorState::default();
    last_monitor_state.timestamp_ms = executor_start_ms;
    last_monitor_state.mode = match mode {
        PulseExecutionMode::Paper => TradingMode::Paper,
        PulseExecutionMode::LiveMock => TradingMode::Live,
    };

    let runtime_result: Result<()> = loop {
        tokio::select! {
            recv = market_data_rx.recv() => {
                match recv {
                    Ok(event) => {
                        if let Some(audit) = audit_runtime.as_mut() {
                            audit.note_market_event();
                        }
                        apply_orderbook_event(&orderbook_provider, &registry, &event);
                        runtime.observe_market_event(event);
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => {
                        break Err(anyhow!("pulse market data channel closed"));
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                break Ok(());
            }
            _ = tick.tick() => {
                let now_ms = current_time_ms();
                if let Err(err) = runtime.run_tick(now_ms).await {
                    break Err(anyhow!("pulse runtime tick failed: {err}"));
                }
                if let Some(view) = runtime.last_monitor_view().cloned() {
                    let mut state = MonitorState::default();
                    state.timestamp_ms = now_ms;
                    state.uptime_secs = started_at.elapsed().as_secs();
                    state.mode = match mode {
                        PulseExecutionMode::Paper => TradingMode::Paper,
                        PulseExecutionMode::LiveMock => TradingMode::Live,
                    };
                    state.signals = runtime.signal_stats();
                    state.evaluation = runtime.evaluation_stats();
                    state.markets = runtime.market_views(now_ms);
                    state.pulse = Some(view);
                    state = decorate_pulse_monitor_state(
                        state,
                        &settings,
                        now_ms,
                        started_at.elapsed().as_secs(),
                    );
                    if let Some(audit) = audit_runtime.as_mut() {
                        if let Err(err) = audit.sync_runtime_state(&runtime, &state, now_ms) {
                            break Err(err.context("sync pulse audit runtime"));
                        }
                    }
                    last_monitor_state = state.clone();
                    let _ = state_broadcaster.send(state);
                }
            }
        }
    };

    last_monitor_state.timestamp_ms = current_time_ms();
    last_monitor_state.uptime_secs = started_at.elapsed().as_secs();
    last_monitor_state.mode = match mode {
        PulseExecutionMode::Paper => TradingMode::Paper,
        PulseExecutionMode::LiveMock => TradingMode::Live,
    };
    last_monitor_state.signals = runtime.signal_stats();
    last_monitor_state.evaluation = runtime.evaluation_stats();
    if let Some(view) = runtime.last_monitor_view().cloned() {
        last_monitor_state.markets = runtime.market_views(last_monitor_state.timestamp_ms);
        last_monitor_state.pulse = Some(view);
    }
    let final_timestamp_ms = last_monitor_state.timestamp_ms;
    let final_uptime_secs = last_monitor_state.uptime_secs;
    last_monitor_state = decorate_pulse_monitor_state(
        last_monitor_state,
        &settings,
        final_timestamp_ms,
        final_uptime_secs,
    );
    if let Some(audit) = audit_runtime.as_mut() {
        audit
            .finalize(
                &runtime,
                &last_monitor_state,
                last_monitor_state.timestamp_ms,
                if runtime_result.is_ok() {
                    AuditSessionStatus::Completed
                } else {
                    AuditSessionStatus::Failed
                },
            )
            .context("finalize pulse audit runtime")?;
    }

    runtime_result
}

fn build_pulse_position_sync(executor: crate::runtime::RuntimeExecutor) -> Arc<ActualPositionSync> {
    Arc::new(move |asset, market| {
        executor
            .cex_net_position(market.hedge_exchange, &market.cex_symbol)
            .map(Some)
            .map_err(|err| {
                format!(
                    "sync pulse actual hedge position failed for asset={} symbol={} venue={:?}: {err}",
                    asset.as_str(),
                    market.cex_symbol,
                    market.hedge_exchange
                )
            })
    })
}

fn filtered_pulse_settings(env: &str, assets: &[PulseAsset]) -> Result<Settings> {
    let settings =
        Settings::load(env).with_context(|| format!("failed to load config env `{env}`"))?;
    Ok(apply_pulse_asset_filter(settings, assets))
}

fn build_pulse_execution_context(
    session_id: &str,
    env: &str,
    mode: PulseExecutionMode,
    settings: &Settings,
) -> PulseExecutionContext {
    let mut routes = settings
        .strategy
        .pulse_arb
        .routing
        .iter()
        .filter(|(_, route)| route.enabled)
        .map(|(asset, route)| PulseExecutionRouteContext {
            asset: asset.to_owned(),
            anchor_provider: route.anchor_provider.clone(),
            hedge_venue: route.hedge_venue.clone(),
        })
        .collect::<Vec<_>>();
    routes.sort_by(|left, right| left.asset.cmp(&right.asset));

    let enabled_assets = routes
        .iter()
        .map(|route| route.asset.clone())
        .collect::<Vec<_>>();
    let mut market_symbols = settings
        .markets
        .iter()
        .map(|market| market.symbol.0.clone())
        .collect::<Vec<_>>();
    market_symbols.sort();
    market_symbols.dedup();

    PulseExecutionContext {
        session_id: session_id.to_owned(),
        env: env.to_owned(),
        mode: match mode {
            PulseExecutionMode::Paper => "paper".to_owned(),
            PulseExecutionMode::LiveMock => "live_mock".to_owned(),
        },
        enabled_assets,
        market_symbols,
        routes,
        poly_fee_bps: settings.execution_costs.poly_fee_bps,
        cex_taker_fee_bps: settings.execution_costs.cex_taker_fee_bps,
        cex_slippage_bps: settings.paper_slippage.cex_slippage_bps as u32,
        maker_proxy_version: "book_proxy_v1".to_owned(),
        max_holding_secs: settings.strategy.pulse_arb.session.max_holding_secs,
        opening_request_notional_usd: settings
            .strategy
            .pulse_arb
            .session
            .effective_opening_request_notional_usd()
            .0
            .normalize()
            .to_string(),
        min_opening_notional_usd: settings
            .strategy
            .pulse_arb
            .session
            .min_opening_notional_usd
            .0
            .normalize()
            .to_string(),
        min_expected_net_pnl_usd: settings
            .strategy
            .pulse_arb
            .session
            .effective_min_expected_net_pnl_usd()
            .0
            .normalize()
            .to_string(),
        require_nonzero_hedge: settings.strategy.pulse_arb.session.require_nonzero_hedge,
        min_open_fill_ratio: settings
            .strategy
            .pulse_arb
            .session
            .min_open_fill_ratio
            .map(|value| value.normalize().to_string()),
    }
}

fn build_pulse_inspect_view(env: &str, settings: &Settings) -> PulseInspectView {
    let mut routes = settings
        .strategy
        .pulse_arb
        .routing
        .iter()
        .filter(|(_, route)| route.enabled)
        .map(|(asset, route)| {
            let provider_kind = settings
                .strategy
                .pulse_arb
                .providers
                .get(&route.anchor_provider)
                .map(|provider| provider.kind.clone())
                .unwrap_or_else(|| "unknown".to_owned());
            PulseInspectRouteView {
                asset: asset.to_owned(),
                anchor_provider: route.anchor_provider.clone(),
                provider_kind,
                hedge_venue: route.hedge_venue.clone(),
            }
        })
        .collect::<Vec<_>>();
    routes.sort_by(|left, right| left.asset.cmp(&right.asset));

    let enabled_assets = routes
        .iter()
        .map(|route| route.asset.clone())
        .collect::<Vec<_>>();

    PulseInspectView {
        env: env.to_owned(),
        runtime_enabled: settings.strategy.pulse_arb.runtime.enabled,
        max_concurrent_sessions_per_asset: settings
            .strategy
            .pulse_arb
            .runtime
            .max_concurrent_sessions_per_asset,
        max_holding_secs: settings.strategy.pulse_arb.session.max_holding_secs,
        min_opening_notional_usd: settings
            .strategy
            .pulse_arb
            .session
            .min_opening_notional_usd
            .0
            .normalize()
            .to_string(),
        supported_modes: vec!["paper".to_owned(), "live-mock".to_owned()],
        enabled_assets,
        routes,
    }
}

fn decorate_pulse_monitor_state(
    mut state: MonitorState,
    settings: &Settings,
    now_ms: u64,
    uptime_secs: u64,
) -> MonitorState {
    state.timestamp_ms = now_ms;
    state.uptime_secs = uptime_secs;
    state.config = build_pulse_monitor_strategy_config(settings);
    if let Some(view) = state.pulse.as_ref() {
        state.connections = pulse_connections_from_view(view, settings, now_ms);
        if state.markets.is_empty() {
            state.markets = view
                .markets
                .iter()
                .map(|row| pulse_market_view_from_row(row, now_ms))
                .collect();
        }
    }
    state
}

fn build_pulse_monitor_strategy_config(settings: &Settings) -> MonitorStrategyConfig {
    let pulse = &settings.strategy.pulse_arb;
    let entry_score = pulse.entry.min_pulse_score_bps.to_f64().unwrap_or_default() / 100.0;
    let entry_edge = pulse
        .entry
        .min_net_session_edge_bps
        .to_f64()
        .unwrap_or_default()
        / 100.0;
    let position_notional_usd = pulse
        .session
        .effective_opening_request_notional_usd()
        .0
        .to_f64()
        .unwrap_or_default();
    let rolling_window = pulse.entry.pulse_window_ms / 1_000;
    let freshness = &settings.strategy.market_data;
    let mut per_asset = pulse
        .routing
        .iter()
        .filter(|(_, route)| route.enabled)
        .filter_map(|(asset, _)| {
            PulseAsset::from_routing_key(asset).map(|asset| MonitorAssetStrategyConfig {
                asset: asset.as_str().to_owned(),
                entry_z: entry_score,
                exit_z: entry_edge,
                position_notional_usd,
                rolling_window,
                band_policy: "pulse_score/net_edge".to_owned(),
                min_poly_price: 0.0,
                max_poly_price: 1.0,
                max_open_instant_loss_pct_of_budget: 0.0,
                delta_rebalance_threshold: pulse
                    .rehedge
                    .delta_drift_threshold
                    .to_f64()
                    .unwrap_or_default(),
                delta_rebalance_interval_secs: 0,
                enable_freshness_check: true,
                reject_on_disconnect: true,
            })
        })
        .collect::<Vec<_>>();
    per_asset.sort_by(|left, right| left.asset.cmp(&right.asset));

    MonitorStrategyConfig {
        entry_z: entry_score,
        exit_z: entry_edge,
        position_notional_usd,
        rolling_window,
        band_policy: "pulse_score/net_edge".to_owned(),
        market_data_mode: "ws".to_owned(),
        max_stale_ms: freshness.max_stale_ms,
        poly_open_max_quote_age_ms: freshness.resolved_poly_open_max_quote_age_ms(),
        cex_open_max_quote_age_ms: freshness.resolved_cex_open_max_quote_age_ms(),
        close_max_quote_age_ms: freshness.resolved_close_max_quote_age_ms(),
        max_cross_leg_skew_ms: freshness.resolved_max_cross_leg_skew_ms(),
        borderline_poly_quote_age_ms: freshness.resolved_borderline_poly_quote_age_ms(),
        min_poly_price: 0.0,
        max_poly_price: 1.0,
        poly_slippage_bps: settings.paper_slippage.poly_slippage_bps,
        cex_slippage_bps: settings.paper_slippage.cex_slippage_bps,
        max_total_exposure_usd: settings
            .risk
            .max_total_exposure_usd
            .0
            .to_f64()
            .unwrap_or_default(),
        max_single_position_usd: settings
            .risk
            .max_single_position_usd
            .0
            .to_f64()
            .unwrap_or_default(),
        max_daily_loss_usd: settings
            .risk
            .max_daily_loss_usd
            .0
            .to_f64()
            .unwrap_or_default(),
        max_open_orders: settings.risk.max_open_orders,
        rate_limit_orders_per_sec: settings.risk.rate_limit_orders_per_sec,
        min_liquidity: settings
            .paper_slippage
            .min_liquidity
            .to_f64()
            .unwrap_or_default(),
        allow_partial_fill: true,
        enable_freshness_check: true,
        reject_on_disconnect: true,
        per_asset,
    }
}

fn pulse_connections_from_view(
    view: &polyalpha_core::PulseMonitorView,
    settings: &Settings,
    now_ms: u64,
) -> HashMap<String, ConnectionInfo> {
    let mut poly_age_ms = None;
    let mut cex_age_ms = None;
    let mut anchor_age_ms = None;
    let mut anchor_latency_ms = None;

    for row in &view.asset_health {
        record_freshest_age(&mut poly_age_ms, row.poly_quote_age_ms);
        record_freshest_age(&mut cex_age_ms, row.cex_quote_age_ms);
        record_freshest_age(&mut anchor_age_ms, row.anchor_age_ms);
        record_freshest_age(&mut anchor_latency_ms, row.anchor_latency_delta_ms);
    }

    let anchor_stale_limit_ms = view
        .asset_health
        .iter()
        .filter_map(|row| row.provider_id.as_ref())
        .filter_map(|provider_id| settings.strategy.pulse_arb.providers.get(provider_id))
        .map(|provider| provider.max_anchor_age_ms)
        .min()
        .unwrap_or_else(|| settings.strategy.market_data.max_stale_ms.max(1));

    HashMap::from([
        (
            "polymarket_ws".to_owned(),
            pulse_connection_info(
                poly_age_ms,
                None,
                settings
                    .strategy
                    .market_data
                    .resolved_poly_open_max_quote_age_ms(),
                now_ms,
            ),
        ),
        (
            "binance_ws".to_owned(),
            pulse_connection_info(
                cex_age_ms,
                None,
                settings
                    .strategy
                    .market_data
                    .resolved_cex_open_max_quote_age_ms(),
                now_ms,
            ),
        ),
        (
            "deribit_ws".to_owned(),
            pulse_connection_info(
                anchor_age_ms,
                anchor_latency_ms,
                anchor_stale_limit_ms,
                now_ms,
            ),
        ),
    ])
}

fn pulse_connection_info(
    age_ms: Option<u64>,
    latency_ms: Option<u64>,
    stale_limit_ms: u64,
    now_ms: u64,
) -> ConnectionInfo {
    let status = match age_ms {
        Some(age_ms) if age_ms <= stale_limit_ms => ConnectionStatus::Connected,
        Some(_) => ConnectionStatus::Reconnecting,
        None => ConnectionStatus::Connecting,
    };

    ConnectionInfo {
        status,
        latency_ms,
        updated_at_ms: age_ms.map(|age_ms| now_ms.saturating_sub(age_ms)),
        transport_idle_ms: age_ms,
        reconnect_count: 0,
        disconnect_count: 0,
        last_disconnect_at_ms: None,
    }
}

fn record_freshest_age(slot: &mut Option<u64>, candidate: Option<u64>) {
    if let Some(candidate) = candidate {
        match slot {
            Some(current) if *current <= candidate => {}
            _ => *slot = Some(candidate),
        }
    }
}

fn pulse_market_view_from_row(
    row: &polyalpha_core::PulseMarketMonitorRow,
    now_ms: u64,
) -> MarketView {
    let poly_price = match row.claim_side.as_deref() {
        Some("YES") => row.poly_yes_price,
        Some("NO") => row.poly_no_price,
        _ => row.poly_yes_price.or(row.poly_no_price),
    };
    let cross_leg_skew_ms = row
        .poly_quote_age_ms
        .zip(row.cex_quote_age_ms)
        .map(|(poly_age_ms, cex_age_ms)| poly_age_ms.abs_diff(cex_age_ms));
    let evaluable_status = match row.status.as_str() {
        "ready" => EvaluableStatus::Evaluable,
        "waiting_books" => EvaluableStatus::NotEvaluableNoPoly,
        "degraded" if row.disable_reason.as_deref() == Some("poly_quote_stale") => {
            EvaluableStatus::PolyQuoteStale
        }
        "degraded" if row.disable_reason.as_deref() == Some("cex_quote_stale") => {
            EvaluableStatus::CexQuoteStale
        }
        "degraded" => EvaluableStatus::ConnectionImpaired,
        _ => EvaluableStatus::Unknown,
    };

    MarketView {
        symbol: row.symbol.clone(),
        z_score: row.pulse_score_bps.map(|value| value / 100.0),
        basis_pct: row.net_edge_bps.map(|value| value / 100.0),
        poly_price,
        poly_yes_price: row.poly_yes_price,
        poly_no_price: row.poly_no_price,
        cex_price: row.cex_price,
        fair_value: row.fair_prob_yes,
        has_position: row.open_sessions > 0,
        minutes_to_expiry: None,
        basis_history_len: 0,
        raw_sigma: None,
        effective_sigma: None,
        sigma_source: Some("pulse".to_owned()),
        returns_window_len: 0,
        active_token_side: row.claim_side.clone(),
        poly_updated_at_ms: row
            .poly_quote_age_ms
            .map(|age_ms| now_ms.saturating_sub(age_ms)),
        cex_updated_at_ms: row
            .cex_quote_age_ms
            .map(|age_ms| now_ms.saturating_sub(age_ms)),
        poly_quote_age_ms: row.poly_quote_age_ms,
        cex_quote_age_ms: row.cex_quote_age_ms,
        cross_leg_skew_ms,
        evaluable_status,
        async_classification: AsyncClassification::default(),
        market_tier: MarketTier::Observation,
        retention_reason: if row.open_sessions > 0 {
            MarketRetentionReason::HasPosition
        } else {
            MarketRetentionReason::None
        },
        last_focus_at_ms: None,
        last_tradeable_at_ms: None,
    }
}

fn apply_pulse_asset_filter(mut settings: Settings, assets: &[PulseAsset]) -> Settings {
    for (asset_key, route) in settings.strategy.pulse_arb.routing.iter_mut() {
        let enabled_for_selection = PulseAsset::from_routing_key(asset_key)
            .map(|asset| assets.contains(&asset))
            .unwrap_or(false);
        route.enabled = route.enabled && enabled_for_selection;
    }

    settings.markets.retain(|market| {
        PulseAsset::from_routing_key(&asset_key_from_cex_symbol(&market.cex_symbol))
            .is_some_and(|asset| assets.contains(&asset))
    });
    settings
}

struct DeribitRuntimeProvider {
    provider: Arc<DeribitAnchorProvider>,
    client: Arc<DeribitOptionsClient>,
}

#[derive(Clone, Debug, Default)]
struct LocalPulsePolyBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    last_trade_price: Option<Price>,
    sequence: u64,
}

impl LocalPulsePolyBook {
    fn from_update_with_previous(update: &PolyBookUpdate, previous_sequence: u64) -> Self {
        let mut book = Self::default();
        for level in &update.bids {
            book.bids.insert(level.price.0, level.shares.0);
        }
        for level in &update.asks {
            book.asks.insert(level.price.0, level.shares.0);
        }
        book.last_trade_price = update.last_trade_price;
        book.sequence = normalized_pulse_ws_poly_sequence(previous_sequence, update.sequence);
        book
    }

    fn apply_level(&mut self, side: PulsePolymarketBookSide, price: Decimal, size: Decimal) {
        let levels = match side {
            PulsePolymarketBookSide::Bid => &mut self.bids,
            PulsePolymarketBookSide::Ask => &mut self.asks,
        };
        if size <= Decimal::ZERO {
            levels.remove(&price);
        } else {
            levels.insert(price, size);
        }
    }

    fn next_sequence(&self) -> u64 {
        normalized_pulse_ws_poly_sequence(self.sequence, 0)
    }

    fn to_update(
        &self,
        asset_id: &str,
        exchange_timestamp_ms: u64,
        sequence: u64,
    ) -> PolyBookUpdate {
        PolyBookUpdate {
            asset_id: asset_id.to_owned(),
            bids: self
                .bids
                .iter()
                .rev()
                .map(|(price, size)| PolyBookLevel {
                    price: Price(*price),
                    shares: PolyShares(*size),
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(price, size)| PolyBookLevel {
                    price: Price(*price),
                    shares: PolyShares(*size),
                })
                .collect(),
            exchange_timestamp_ms,
            received_at_ms: current_time_ms(),
            sequence,
            last_trade_price: self.last_trade_price,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PulsePolymarketBookSide {
    Bid,
    Ask,
}

#[derive(Clone, Debug)]
struct PulsePolymarketPriceChange {
    asset_id: String,
    price: Decimal,
    size: Decimal,
    side: PulsePolymarketBookSide,
    timestamp_ms: u64,
}

fn normalized_pulse_ws_poly_sequence(previous_sequence: u64, incoming_sequence: u64) -> u64 {
    incoming_sequence
        .max(previous_sequence.saturating_add(1))
        .max(1)
}

async fn handle_pulse_polymarket_ws_text(
    manager: &DataManager,
    books: &Arc<Mutex<HashMap<String, LocalPulsePolyBook>>>,
    payload: &str,
) -> Result<()> {
    let value: Value = serde_json::from_str(payload)?;
    let messages = match value {
        Value::Array(items) => items,
        other => vec![other],
    };

    for message in messages {
        if message
            .get("event_type")
            .or_else(|| message.get("type"))
            .and_then(Value::as_str)
            .map(|kind| kind.eq_ignore_ascii_case("price_change"))
            .unwrap_or(false)
        {
            let changes = parse_pulse_polymarket_price_changes(&message)?;
            if !changes.is_empty() {
                let _ = apply_pulse_polymarket_price_changes(manager, books, &changes)?;
            }
            continue;
        }

        if let Some(last_trade_price) = pulse_parse_decimal_value(
            message
                .get("last_trade_price")
                .or_else(|| message.get("price")),
        ) {
            if let Some(asset_id) = message
                .get("asset_id")
                .or_else(|| message.get("market"))
                .and_then(Value::as_str)
            {
                if let Ok(mut guard) = books.lock() {
                    let book = guard.entry(asset_id.to_owned()).or_default();
                    book.last_trade_price = Some(Price(last_trade_price));
                }
            }
        }

        if message.get("book").is_some()
            || message.get("bids").is_some()
            || message.get("asks").is_some()
        {
            let mut update =
                PolymarketLiveDataSource::parse_ws_orderbook_payload(&message.to_string(), "")?;
            if let Ok(mut guard) = books.lock() {
                let previous_sequence = guard
                    .get(&update.asset_id)
                    .map(|book| book.sequence)
                    .unwrap_or(0);
                let book =
                    LocalPulsePolyBook::from_update_with_previous(&update, previous_sequence);
                update.sequence = book.sequence;
                guard.insert(update.asset_id.clone(), book);
            }
            let _ = manager.normalize_and_publish_poly_orderbook(update)?;
        }
    }

    Ok(())
}

fn apply_pulse_polymarket_price_changes(
    manager: &DataManager,
    books: &Arc<Mutex<HashMap<String, LocalPulsePolyBook>>>,
    changes: &[PulsePolymarketPriceChange],
) -> Result<usize> {
    let mut grouped: HashMap<String, (u64, LocalPulsePolyBook)> = HashMap::new();
    let mut guard = books
        .lock()
        .map_err(|_| anyhow!("pulse Polymarket 订单簿缓存锁异常"))?;

    for change in changes {
        let entry = grouped.entry(change.asset_id.clone()).or_insert_with(|| {
            let book = guard.get(&change.asset_id).cloned().unwrap_or_default();
            (change.timestamp_ms, book)
        });
        entry.0 = entry.0.max(change.timestamp_ms);
        entry.1.apply_level(change.side, change.price, change.size);
    }

    let emitted_updates = grouped.len();
    for (asset_id, (timestamp_ms, mut book)) in grouped {
        let sequence = book.next_sequence();
        book.sequence = sequence;
        guard.insert(asset_id.clone(), book.clone());
        let update = book.to_update(&asset_id, timestamp_ms, sequence);
        let _ = manager.normalize_and_publish_poly_orderbook(update)?;
    }

    Ok(emitted_updates)
}

fn parse_pulse_polymarket_price_changes(
    message: &Value,
) -> Result<Vec<PulsePolymarketPriceChange>> {
    let changes = message
        .get("changes")
        .or_else(|| message.get("price_changes"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let default_asset_id = message
        .get("asset_id")
        .or_else(|| message.get("market"))
        .and_then(Value::as_str)
        .map(|value| value.to_owned());
    let default_timestamp_ms = pulse_parse_timestamp_value(
        message
            .get("timestamp")
            .or_else(|| message.get("ts"))
            .or_else(|| message.get("created_at")),
    )
    .unwrap_or_else(current_time_ms);

    changes
        .into_iter()
        .map(|change| {
            let asset_id = change
                .get("asset_id")
                .or_else(|| change.get("market"))
                .and_then(Value::as_str)
                .map(|value| value.to_owned())
                .or_else(|| default_asset_id.clone())
                .ok_or_else(|| anyhow!("pulse Polymarket price_change 缺少 asset_id"))?;
            let price = pulse_parse_decimal_value(
                change
                    .get("price")
                    .or_else(|| change.get("px"))
                    .or_else(|| change.get("p")),
            )
            .ok_or_else(|| anyhow!("pulse Polymarket price_change 缺少 price"))?;
            let size = pulse_parse_decimal_value(
                change
                    .get("size")
                    .or_else(|| change.get("qty"))
                    .or_else(|| change.get("quantity"))
                    .or_else(|| change.get("s")),
            )
            .ok_or_else(|| anyhow!("pulse Polymarket price_change 缺少 size"))?;
            let side = parse_pulse_polymarket_side(
                change
                    .get("side")
                    .or_else(|| change.get("book_side"))
                    .and_then(Value::as_str),
            )
            .ok_or_else(|| anyhow!("pulse Polymarket price_change 缺少 side"))?;
            let timestamp_ms = pulse_parse_timestamp_value(
                change
                    .get("timestamp")
                    .or_else(|| change.get("ts"))
                    .or_else(|| change.get("created_at")),
            )
            .unwrap_or(default_timestamp_ms);

            Ok(PulsePolymarketPriceChange {
                asset_id,
                price,
                size,
                side,
                timestamp_ms,
            })
        })
        .collect()
}

fn parse_pulse_polymarket_side(value: Option<&str>) -> Option<PulsePolymarketBookSide> {
    match value?.to_ascii_uppercase().as_str() {
        "BUY" | "BID" => Some(PulsePolymarketBookSide::Bid),
        "SELL" | "ASK" => Some(PulsePolymarketBookSide::Ask),
        _ => None,
    }
}

fn pulse_parse_decimal_value(value: Option<&Value>) -> Option<Decimal> {
    match value? {
        Value::String(raw) => Decimal::from_str(raw).ok(),
        Value::Number(number) => Decimal::from_str(&number.to_string()).ok(),
        _ => None,
    }
}

fn pulse_parse_timestamp_value(value: Option<&Value>) -> Option<u64> {
    match value {
        Some(Value::Number(number)) => number.as_u64(),
        Some(Value::String(raw)) => raw.parse::<u64>().ok(),
        _ => None,
    }
}

async fn build_deribit_anchor_provider(
    settings: &Settings,
    assets: &[PulseAsset],
) -> Result<DeribitRuntimeProvider> {
    let mut client = DeribitOptionsClient::new(
        settings.deribit.rest_url.clone(),
        settings.deribit.ws_url.clone(),
        Default::default(),
    );
    client.connect().await.context("connect Deribit client")?;
    for asset in assets {
        let Some(deribit_asset) = asset.as_deribit_asset() else {
            continue;
        };
        client
            .prime_asset(deribit_asset)
            .await
            .with_context(|| format!("prime Deribit asset `{}`", asset.as_str()))?;
    }
    let client = Arc::new(client);
    let provider_config = settings
        .strategy
        .pulse_arb
        .providers
        .get("deribit_primary")
        .cloned()
        .ok_or_else(|| anyhow!("missing pulse provider `deribit_primary`"))?;
    let provider = Arc::new(DeribitAnchorProvider::with_client(
        "deribit_primary",
        provider_config,
        client.clone(),
    ));
    Ok(DeribitRuntimeProvider { provider, client })
}

fn spawn_pulse_monitor_server(
    settings: &Settings,
) -> (broadcast::Sender<MonitorState>, JoinHandle<()>) {
    let current_state = Arc::new(std::sync::RwLock::new(None));
    let socket_server = MonitorSocketServer::new_with_current_state(
        settings.general.monitor_socket_path.clone(),
        current_state,
    );
    let state_broadcaster = socket_server.state_broadcaster();
    let handle = tokio::spawn(async move {
        let handler = Box::new(|_command_id, _kind| {
            Err::<MonitorMessage, String>(
                "pulse runtime does not accept monitor commands".to_owned(),
            )
        });
        let _ = socket_server.run(handler).await;
    });
    (state_broadcaster, handle)
}

async fn spawn_pulse_market_data_tasks(
    manager: DataManager,
    settings: &Settings,
    markets: &[polyalpha_core::MarketConfig],
    depth: u16,
    deribit: DeribitRuntimeProvider,
) -> Result<Vec<JoinHandle<()>>> {
    let mut tasks = Vec::new();
    let proxy_settings = WsProxySettings::detect();
    tasks.push(spawn_polymarket_ws_task(
        manager.clone(),
        settings.polymarket.ws_url.clone(),
        markets,
        proxy_settings.clone(),
    ));
    tasks.push(spawn_binance_ws_task(
        manager.clone(),
        settings.binance.ws_url.clone(),
        markets,
        depth,
        proxy_settings.clone(),
    ));
    tasks.push(spawn_deribit_ws_task(
        deribit.client,
        settings.deribit.ws_url.clone(),
        markets,
        proxy_settings,
    ));
    Ok(tasks)
}

fn spawn_polymarket_ws_task(
    manager: DataManager,
    ws_url: String,
    markets: &[polyalpha_core::MarketConfig],
    proxy_settings: WsProxySettings,
) -> JoinHandle<()> {
    let asset_ids = markets
        .iter()
        .flat_map(|market| {
            [
                market.poly_ids.yes_token_id.clone(),
                market.poly_ids.no_token_id.clone(),
            ]
        })
        .collect::<Vec<_>>();
    tokio::spawn(async move {
        if asset_ids.is_empty() {
            return;
        }
        let books: Arc<Mutex<HashMap<String, LocalPulsePolyBook>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let proxy = match proxy_settings.resolve_for_url(&ws_url) {
            Ok(proxy) => proxy,
            Err(err) => {
                log_ws_warning(format!(
                    "pulse polymarket ws proxy resolution failed: {err}"
                ));
                return;
            }
        };
        loop {
            match run_ws_future_with_timeout(
                connect_websocket(&ws_url, proxy.as_ref()),
                Duration::from_secs(10),
                "pulse polymarket ws connect".to_owned(),
            )
            .await
            {
                Ok((stream, _)) => {
                    let (mut writer, mut reader) = stream.split();
                    let subscribe =
                        PolymarketLiveDataSource::build_orderbook_subscribe_message(&asset_ids);
                    if writer
                        .send(Message::Text(subscribe.to_string().into()))
                        .await
                        .is_err()
                    {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                    let mut heartbeat = interval(Duration::from_secs(10));
                    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);

                    loop {
                        tokio::select! {
                            _ = heartbeat.tick() => {
                                if writer.send(Message::Text("PING".to_owned().into())).await.is_err() {
                                    break;
                                }
                            }
                            message = reader.next() => {
                                match message {
                                    Some(Ok(Message::Text(text))) => {
                                        if text == "PONG" {
                                            continue;
                                        }
                                        if let Err(err) =
                                            handle_pulse_polymarket_ws_text(&manager, &books, &text).await
                                        {
                                            log_ws_warning(format!(
                                                "pulse polymarket ws payload ignored: {err}"
                                            ));
                                        }
                                    }
                                    Some(Ok(Message::Ping(payload))) => {
                                        if writer.send(Message::Pong(payload)).await.is_err() {
                                            break;
                                        }
                                    }
                                    Some(Ok(Message::Close(frame))) => {
                                        log_ws_warning(format!("pulse polymarket ws closed: {:?}", frame));
                                        break;
                                    }
                                    None => break,
                                    Some(Err(err)) => {
                                        log_ws_warning(format!("pulse polymarket ws read error: {err}"));
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                Err(err) => {
                    log_ws_warning(format!("pulse polymarket ws connect failed: {err}"));
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    })
}

fn spawn_binance_ws_task(
    manager: DataManager,
    ws_url: String,
    markets: &[polyalpha_core::MarketConfig],
    depth: u16,
    proxy_settings: WsProxySettings,
) -> JoinHandle<()> {
    let mut seen = HashSet::new();
    let venue_symbols = markets
        .iter()
        .map(|market| market.cex_symbol.clone())
        .filter(|symbol| seen.insert(symbol.clone()))
        .collect::<Vec<_>>();
    tokio::spawn(async move {
        if venue_symbols.is_empty() {
            return;
        }
        let stream_path = BinanceFuturesDataSource::build_combined_partial_depth_stream_path(
            &venue_symbols,
            depth,
        );
        let endpoint = format!("{}{}", ws_url.trim_end_matches('/'), stream_path);
        let proxy = match proxy_settings.resolve_for_url(&endpoint) {
            Ok(proxy) => proxy,
            Err(err) => {
                log_ws_warning(format!("pulse binance ws proxy resolution failed: {err}"));
                return;
            }
        };
        loop {
            match run_ws_future_with_timeout(
                connect_websocket(&endpoint, proxy.as_ref()),
                Duration::from_secs(10),
                "pulse binance ws connect".to_owned(),
            )
            .await
            {
                Ok((stream, _)) => {
                    let (_writer, mut reader) = stream.split();
                    while let Some(message) = reader.next().await {
                        match message {
                            Ok(Message::Text(text)) => {
                                if let Ok(Some(update)) =
                                    BinanceFuturesDataSource::parse_partial_depth_ws_payload(
                                        &text, "",
                                    )
                                {
                                    let _ = manager.normalize_and_publish_cex_orderbook(update);
                                }
                            }
                            Ok(Message::Close(frame)) => {
                                log_ws_warning(format!("pulse binance ws closed: {:?}", frame));
                                break;
                            }
                            Err(err) => {
                                log_ws_warning(format!("pulse binance ws read error: {err}"));
                                break;
                            }
                            _ => {}
                        }
                    }
                }
                Err(err) => {
                    log_ws_warning(format!("pulse binance ws connect failed: {err}"));
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    })
}

fn spawn_deribit_ws_task(
    client: Arc<DeribitOptionsClient>,
    ws_url: String,
    markets: &[polyalpha_core::MarketConfig],
    proxy_settings: WsProxySettings,
) -> JoinHandle<()> {
    let assets = markets
        .iter()
        .filter_map(|market| {
            PulseAsset::from_routing_key(&asset_key_from_cex_symbol(&market.cex_symbol))
        })
        .filter_map(|asset| asset.as_deribit_asset())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    tokio::spawn(async move {
        if assets.is_empty() {
            return;
        }
        let proxy = match proxy_settings.resolve_for_url(&ws_url) {
            Ok(proxy) => proxy,
            Err(err) => {
                log_ws_warning(format!("pulse deribit ws proxy resolution failed: {err}"));
                return;
            }
        };

        loop {
            let now_ms = current_time_ms();
            let mut channels = Vec::new();
            for asset in &assets {
                let _ = client.prime_asset(*asset).await;
                let Ok(index_price) = client.fetch_index_price(*asset).await else {
                    continue;
                };
                channels.extend(
                    client
                        .selected_instruments(*asset, index_price, now_ms)
                        .into_iter()
                        .map(|instrument| {
                            DeribitOptionsClient::ticker_channel(&instrument.instrument_name)
                        }),
                );
            }
            if channels.is_empty() {
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }

            match run_ws_future_with_timeout(
                connect_websocket(&ws_url, proxy.as_ref()),
                Duration::from_secs(10),
                "pulse deribit ws connect".to_owned(),
            )
            .await
            {
                Ok((stream, _)) => {
                    let (mut writer, mut reader) = stream.split();
                    let subscribe =
                        DeribitOptionsClient::build_subscribe_message(&channels, 1).to_string();
                    if writer.send(Message::Text(subscribe.into())).await.is_err() {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                    let reprune_after = Duration::from_secs(
                        client.discovery_filter().reprune_interval_secs.max(60),
                    );
                    let reprune_deadline = Instant::now() + reprune_after;

                    loop {
                        tokio::select! {
                            _ = tokio::time::sleep_until(reprune_deadline) => break,
                            message = reader.next() => {
                                match message {
                                    Some(Ok(Message::Text(text))) => {
                                        if let Ok(ticker) = DeribitTickerMessage::from_text(&text) {
                                            client.ingest_ticker(ticker);
                                        }
                                    }
                                    Some(Ok(Message::Close(frame))) => {
                                        log_ws_warning(format!("pulse deribit ws closed: {:?}", frame));
                                        break;
                                    }
                                    None => break,
                                    Some(Err(err)) => {
                                        log_ws_warning(format!("pulse deribit ws read error: {err}"));
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                Err(err) => {
                    log_ws_warning(format!("pulse deribit ws connect failed: {err}"));
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    })
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs, path::PathBuf, sync::Mutex};

    use polyalpha_audit::{AuditReader, AuditSessionStatus, PulseExecutionContext};
    use polyalpha_core::{
        create_channels, CexBaseQty, Exchange, InstrumentKind, MarketConfig, MarketDataEvent,
        OrderBookSnapshot, PolyShares, PolymarketIds, Price, PriceLevel, Symbol, VenueQuantity,
    };
    use polyalpha_pulse::{
        anchor::provider::{AnchorError, AnchorProvider},
        model::{AnchorQualityMetrics, AnchorSnapshot, LocalSurfacePoint},
        runtime::{PulseExecutionMode, PulseRuntimeBuilder},
    };
    use rust_decimal::Decimal;
    use serde_json::json;
    use tokio::sync::broadcast::error::TryRecvError;

    use super::*;

    fn pulse_settings_json() -> serde_json::Value {
        json!({
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
                        "btc": { "enabled": true, "anchor_provider": "deribit_primary", "hedge_venue": "binance_perp" },
                        "eth": { "enabled": true, "anchor_provider": "deribit_primary", "hedge_venue": "binance_perp" },
                        "sol": { "enabled": false, "anchor_provider": "deribit_primary", "hedge_venue": "binance_perp" }
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
        })
    }

    fn sample_pulse_market() -> MarketConfig {
        MarketConfig {
            symbol: Symbol::new("btc-pulse-test"),
            poly_ids: PolymarketIds {
                condition_id: "cond-1".to_owned(),
                yes_token_id: "yes".to_owned(),
                no_token_id: "no".to_owned(),
            },
            market_question: Some("Will BTC be above 100k?".to_owned()),
            market_rule: None,
            cex_symbol: "BTCUSDT".to_owned(),
            hedge_exchange: Exchange::Binance,
            strike_price: Some(Price(Decimal::new(100_000, 0))),
            settlement_timestamp: 1_800_000_000,
            min_tick_size: Price(Decimal::new(1, 2)),
            neg_risk: false,
            cex_price_tick: Decimal::ONE,
            cex_qty_step: Decimal::new(1, 3),
            cex_contract_multiplier: Decimal::ONE,
        }
    }

    #[derive(Clone)]
    struct StaticAnchorProvider {
        snapshot: AnchorSnapshot,
    }

    impl AnchorProvider for StaticAnchorProvider {
        fn provider_id(&self) -> &str {
            &self.snapshot.provider_id
        }

        fn snapshot_for_target(
            &self,
            _asset: PulseAsset,
            _target_event_expiry_ts_ms: Option<u64>,
        ) -> std::result::Result<Option<AnchorSnapshot>, AnchorError> {
            Ok(Some(self.snapshot.clone()))
        }
    }

    fn price_level(price: Decimal, quantity: VenueQuantity) -> PriceLevel {
        PriceLevel {
            price: Price(price),
            quantity,
        }
    }

    fn poly_book_update(
        symbol: &str,
        instrument: InstrumentKind,
        bid_price: Decimal,
        ask_price: Decimal,
        ts_ms: u64,
    ) -> MarketDataEvent {
        MarketDataEvent::OrderBookUpdate {
            snapshot: OrderBookSnapshot {
                exchange: Exchange::Polymarket,
                symbol: Symbol::new(symbol),
                instrument,
                bids: vec![price_level(
                    bid_price,
                    VenueQuantity::PolyShares(PolyShares(Decimal::new(10_000, 0))),
                )],
                asks: vec![price_level(
                    ask_price,
                    VenueQuantity::PolyShares(PolyShares(Decimal::new(10_000, 0))),
                )],
                exchange_timestamp_ms: ts_ms,
                received_at_ms: ts_ms,
                sequence: 1,
                last_trade_price: None,
            },
        }
    }

    fn cex_book_update(
        symbol: &str,
        bid_price: Decimal,
        ask_price: Decimal,
        ts_ms: u64,
    ) -> MarketDataEvent {
        MarketDataEvent::OrderBookUpdate {
            snapshot: OrderBookSnapshot {
                exchange: Exchange::Binance,
                symbol: Symbol::new(symbol),
                instrument: InstrumentKind::CexPerp,
                bids: vec![price_level(
                    bid_price,
                    VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(50, 3))),
                )],
                asks: vec![price_level(
                    ask_price,
                    VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(50, 3))),
                )],
                exchange_timestamp_ms: ts_ms,
                received_at_ms: ts_ms,
                sequence: 1,
                last_trade_price: None,
            },
        }
    }

    #[test]
    fn parse_pulse_assets_rejects_unknown_asset() {
        let err = parse_pulse_assets("btc,doge").expect_err("unknown asset should fail");
        assert!(err.to_string().contains("unsupported pulse asset `doge`"));
    }

    #[test]
    fn apply_pulse_asset_filter_only_keeps_selected_assets_enabled() {
        let settings: Settings =
            serde_json::from_value(pulse_settings_json()).expect("deserialize pulse settings");

        let settings = apply_pulse_asset_filter(settings, &[PulseAsset::Btc]);

        assert!(
            settings
                .strategy
                .pulse_arb
                .routing
                .get("btc")
                .expect("btc route")
                .enabled
        );
        assert!(
            !settings
                .strategy
                .pulse_arb
                .routing
                .get("eth")
                .expect("eth route")
                .enabled
        );
    }

    #[test]
    fn build_pulse_inspect_view_lists_only_enabled_routes() {
        let settings: Settings =
            serde_json::from_value(pulse_settings_json()).expect("deserialize pulse settings");

        let inspect = build_pulse_inspect_view("default", &settings);

        assert!(inspect.runtime_enabled);
        assert_eq!(
            inspect.enabled_assets,
            vec!["btc".to_owned(), "eth".to_owned()]
        );
        assert_eq!(inspect.routes.len(), 2);
        assert!(inspect
            .routes
            .iter()
            .all(|route| route.anchor_provider == "deribit_primary"));
    }

    #[tokio::test]
    async fn pulse_audit_runtime_start_writes_execution_context_sidecar() {
        let mut settings: Settings =
            serde_json::from_value(pulse_settings_json()).expect("deserialize pulse settings");
        let root = std::env::temp_dir().join(format!("polyalpha-pulse-context-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).expect("create temp root");
        settings.general.data_dir = root.to_string_lossy().into_owned();

        let now_ms = 1_717_171_717_000;
        let (_runtime_tx, runtime_rx, _market_tx, market_rx) = pulse_audit_channels();
        let audit = PulseAuditRuntime::start(
            &settings,
            "test",
            PulseExecutionMode::LiveMock,
            now_ms,
            runtime_rx,
            market_rx,
        )
        .expect("start audit runtime")
        .expect("audit runtime enabled");

        let raw = fs::read_to_string(audit.writer.paths().execution_context_path())
            .expect("read execution context");
        let context: PulseExecutionContext =
            serde_json::from_str(&raw).expect("decode execution context");

        assert_eq!(context.session_id, audit.summary.session_id);
        assert_eq!(context.mode, "live_mock");
        assert_eq!(context.hedge_venue_for_asset("btc"), Some("binance_perp"));
        assert_eq!(context.min_open_fill_ratio, None);
    }

    #[tokio::test]
    async fn pulse_audit_runtime_filters_high_frequency_raw_events() {
        let mut settings: Settings =
            serde_json::from_value(pulse_settings_json()).expect("deserialize pulse settings");
        let root = std::env::temp_dir().join(format!("polyalpha-pulse-raw-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).expect("create temp root");
        settings.general.data_dir = root.to_string_lossy().into_owned();
        settings.markets = vec![sample_pulse_market()];
        let registry = SymbolRegistry::new(settings.markets.clone());

        let now_ms = 1_717_171_717_000;
        let anchor_provider = Arc::new(StaticAnchorProvider {
            snapshot: AnchorSnapshot {
                asset: PulseAsset::Btc,
                provider_id: "deribit_primary".to_owned(),
                ts_ms: now_ms,
                index_price: Decimal::new(100_000, 0),
                expiry_ts_ms: settings.markets[0].settlement_timestamp * 1_000,
                atm_iv: 55.0,
                local_surface_points: vec![LocalSurfacePoint {
                    instrument_name: "BTC-26APR26-100000-C".to_owned(),
                    strike: Decimal::new(100_000, 0),
                    expiry_ts_ms: settings.markets[0].settlement_timestamp * 1_000,
                    bid_iv: Some(54.5),
                    ask_iv: Some(55.5),
                    mark_iv: 55.0,
                    delta: Some(0.5),
                    gamma: Some(0.0002),
                    best_bid: Some(Decimal::new(12, 2)),
                    best_ask: Some(Decimal::new(13, 2)),
                }],
                quality: AnchorQualityMetrics {
                    anchor_age_ms: 10,
                    max_quote_spread_bps: None,
                    has_strike_coverage: true,
                    has_liquidity: true,
                    expiry_mismatch_minutes: 0,
                    greeks_complete: true,
                },
            },
        });
        let (runtime_tx, runtime_rx, market_tape_tx, market_tape_rx) = pulse_audit_channels();
        let mut runtime = PulseRuntimeBuilder::new(settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(settings.markets.clone())
            .with_audit_emitter(build_runtime_audit_emitter(runtime_tx))
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build pulse runtime");
        let mut audit = PulseAuditRuntime::start(
            &settings,
            "test",
            PulseExecutionMode::Paper,
            now_ms,
            runtime_rx,
            market_tape_rx,
        )
        .expect("start audit runtime")
        .expect("audit runtime enabled");

        let yes_event = poly_book_update(
            "btc-pulse-test",
            InstrumentKind::PolyYes,
            Decimal::new(45, 2),
            Decimal::new(46, 2),
            now_ms,
        );
        forward_market_event_to_pulse_audit(&registry, &market_tape_tx, &yes_event)
            .await
            .expect("persist yes raw tape");
        runtime.observe_market_event(yes_event);

        let no_event = poly_book_update(
            "btc-pulse-test",
            InstrumentKind::PolyNo,
            Decimal::new(54, 2),
            Decimal::new(55, 2),
            now_ms,
        );
        forward_market_event_to_pulse_audit(&registry, &market_tape_tx, &no_event)
            .await
            .expect("persist no raw tape");
        runtime.observe_market_event(no_event);

        let cex_event = cex_book_update(
            "btc-pulse-test",
            Decimal::new(99_995, 0),
            Decimal::new(100_005, 0),
            now_ms,
        );
        forward_market_event_to_pulse_audit(&registry, &market_tape_tx, &cex_event)
            .await
            .expect("persist cex raw tape");
        runtime.observe_market_event(cex_event);
        runtime
            .run_tick(now_ms + 100)
            .await
            .expect("run pulse tick");

        let mut monitor_state = MonitorState::default();
        monitor_state.timestamp_ms = now_ms + 100;
        monitor_state.mode = TradingMode::Paper;
        audit
            .finalize(
                &runtime,
                &monitor_state,
                now_ms + 100,
                AuditSessionStatus::Completed,
            )
            .expect("finalize audit runtime");

        let events = AuditReader::load_events(&root, &audit.summary.session_id)
            .expect("load persisted pulse audit events");
        assert!(!events
            .iter()
            .any(|event| { matches!(event.payload, AuditEventPayload::PulseMarketTape(_)) }));
        assert!(!events
            .iter()
            .any(|event| { matches!(event.payload, AuditEventPayload::PulseSignalSnapshot(_)) }));
        assert!(!events
            .iter()
            .any(|event| { matches!(event.payload, AuditEventPayload::PulseAssetHealth(_)) }));

        let raw = fs::read_to_string(audit.writer.paths().execution_context_path())
            .expect("read execution context");
        let context: PulseExecutionContext =
            serde_json::from_str(&raw).expect("decode execution context");
        assert_eq!(context.session_id, audit.summary.session_id);
        assert_eq!(context.market_symbols, vec!["btc-pulse-test".to_owned()]);
    }

    #[tokio::test]
    async fn pulse_audit_runtime_sync_consumes_emitted_runtime_records() {
        let mut settings: Settings =
            serde_json::from_value(pulse_settings_json()).expect("deserialize pulse settings");
        let root = std::env::temp_dir().join(format!("polyalpha-pulse-drain-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).expect("create temp root");
        settings.general.data_dir = root.to_string_lossy().into_owned();
        settings.markets = vec![sample_pulse_market()];

        let now_ms = 1_717_171_717_000;
        let anchor_provider = Arc::new(StaticAnchorProvider {
            snapshot: AnchorSnapshot {
                asset: PulseAsset::Btc,
                provider_id: "deribit_primary".to_owned(),
                ts_ms: now_ms,
                index_price: Decimal::new(100_000, 0),
                expiry_ts_ms: settings.markets[0].settlement_timestamp * 1_000,
                atm_iv: 55.0,
                local_surface_points: vec![LocalSurfacePoint {
                    instrument_name: "BTC-26APR26-100000-C".to_owned(),
                    strike: Decimal::new(100_000, 0),
                    expiry_ts_ms: settings.markets[0].settlement_timestamp * 1_000,
                    bid_iv: Some(54.5),
                    ask_iv: Some(55.5),
                    mark_iv: 55.0,
                    delta: Some(0.5),
                    gamma: Some(0.0002),
                    best_bid: Some(Decimal::new(12, 2)),
                    best_ask: Some(Decimal::new(13, 2)),
                }],
                quality: AnchorQualityMetrics {
                    anchor_age_ms: 10,
                    max_quote_spread_bps: None,
                    has_strike_coverage: true,
                    has_liquidity: true,
                    expiry_mismatch_minutes: 0,
                    greeks_complete: true,
                },
            },
        });
        let (runtime_tx, runtime_rx, _market_tape_tx, market_tape_rx) = pulse_audit_channels();
        let mut runtime = PulseRuntimeBuilder::new(settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(settings.markets.clone())
            .with_audit_emitter(build_runtime_audit_emitter(runtime_tx))
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build pulse runtime");
        let mut audit = PulseAuditRuntime::start(
            &settings,
            "test",
            PulseExecutionMode::Paper,
            now_ms,
            runtime_rx,
            market_tape_rx,
        )
        .expect("start audit runtime")
        .expect("audit runtime enabled");

        runtime.observe_market_event(poly_book_update(
            "btc-pulse-test",
            InstrumentKind::PolyYes,
            Decimal::new(45, 2),
            Decimal::new(46, 2),
            now_ms,
        ));
        runtime.observe_market_event(poly_book_update(
            "btc-pulse-test",
            InstrumentKind::PolyNo,
            Decimal::new(54, 2),
            Decimal::new(55, 2),
            now_ms,
        ));
        runtime.observe_market_event(cex_book_update(
            "btc-pulse-test",
            Decimal::new(99_995, 0),
            Decimal::new(100_005, 0),
            now_ms,
        ));
        runtime
            .run_tick(now_ms + 100)
            .await
            .expect("run pulse tick");

        let runtime_state_changes = runtime.total_audit_record_count() as u64;
        assert_eq!(
            runtime_state_changes, 0,
            "service path should not emit high-frequency audit records during idle evaluation"
        );
        assert!(
            runtime.audit_records().is_empty(),
            "service path should not retain runtime audit records in memory"
        );

        let mut monitor_state = MonitorState::default();
        monitor_state.timestamp_ms = now_ms + 100;
        monitor_state.mode = TradingMode::Paper;
        audit
            .sync_runtime_state(&runtime, &monitor_state, now_ms + 100)
            .expect("sync audit runtime");

        let persisted_state_changes = audit.writer.event_count();
        assert_eq!(persisted_state_changes, 0);
        assert_eq!(
            audit.summary.counters.state_changes,
            persisted_state_changes
        );
    }

    #[test]
    fn pulse_monitor_state_uses_ws_mode_and_projects_connection_health() {
        let settings: Settings =
            serde_json::from_value(pulse_settings_json()).expect("deserialize pulse settings");
        let mut runtime_state = MonitorState::default();
        runtime_state.pulse = Some(polyalpha_core::PulseMonitorView {
            active_sessions: Vec::new(),
            asset_health: vec![
                polyalpha_core::PulseAssetHealthRow {
                    asset: "btc".to_owned(),
                    provider_id: Some("deribit_primary".to_owned()),
                    anchor_age_ms: Some(120),
                    anchor_latency_delta_ms: Some(180),
                    poly_quote_age_ms: Some(35),
                    cex_quote_age_ms: Some(22),
                    open_sessions: 0,
                    net_target_delta: Some(0.0),
                    actual_exchange_position: Some(0.0),
                    status: Some("enabled".to_owned()),
                    disable_reason: None,
                },
                polyalpha_core::PulseAssetHealthRow {
                    asset: "eth".to_owned(),
                    provider_id: Some("deribit_primary".to_owned()),
                    anchor_age_ms: Some(160),
                    anchor_latency_delta_ms: Some(210),
                    poly_quote_age_ms: Some(48),
                    cex_quote_age_ms: Some(19),
                    open_sessions: 0,
                    net_target_delta: Some(0.0),
                    actual_exchange_position: Some(0.0),
                    status: Some("enabled".to_owned()),
                    disable_reason: None,
                },
            ],
            markets: Vec::new(),
            selected_session: None,
        });

        let projected =
            decorate_pulse_monitor_state(runtime_state, &settings, 1_717_171_717_000, 42);

        assert_eq!(projected.config.market_data_mode, "ws");
        assert!(projected.connections.contains_key("polymarket_ws"));
        assert!(projected.connections.contains_key("binance_ws"));
        assert!(projected.connections.contains_key("deribit_ws"));
        assert_eq!(
            projected
                .connections
                .get("polymarket_ws")
                .expect("polymarket ws connection")
                .status,
            polyalpha_core::ConnectionStatus::Connected
        );
    }

    #[tokio::test]
    async fn pulse_polymarket_ws_price_change_keeps_monotonic_poly_book_sequence() {
        let market = sample_pulse_market();
        let registry = SymbolRegistry::new(vec![market.clone()]);
        let channels = create_channels(std::slice::from_ref(&market.symbol));
        let manager = build_data_manager(&registry, channels.market_data_tx.clone());
        let books = Arc::new(Mutex::new(HashMap::new()));
        let mut market_data_rx = channels.market_data_tx.subscribe();

        handle_pulse_polymarket_ws_text(
            &manager,
            &books,
            r#"{
                "asset_id": "yes",
                "timestamp": "1715000000000",
                "sequence": "7",
                "bids": [{"price":"0.49","size":"12"}],
                "asks": [{"price":"0.51","size":"13"}]
            }"#,
        )
        .await
        .expect("full snapshot should parse");
        handle_pulse_polymarket_ws_text(
            &manager,
            &books,
            r#"{
                "event_type": "price_change",
                "asset_id": "yes",
                "timestamp": "1715000000123",
                "changes": [{
                    "price": "0.52",
                    "size": "9",
                    "side": "ask"
                }]
            }"#,
        )
        .await
        .expect("price change should parse");

        let mut observed_sequences = Vec::new();
        loop {
            match market_data_rx.try_recv() {
                Ok(MarketDataEvent::OrderBookUpdate { snapshot })
                    if snapshot.exchange == Exchange::Polymarket
                        && snapshot.instrument == InstrumentKind::PolyYes =>
                {
                    observed_sequences.push(snapshot.sequence);
                }
                Ok(_) => continue,
                Err(TryRecvError::Empty) | Err(TryRecvError::Closed) => break,
                Err(TryRecvError::Lagged(_)) => continue,
            }
        }

        assert_eq!(observed_sequences.first().copied(), Some(7));
        assert_eq!(observed_sequences.get(1).copied(), Some(8));
    }

    fn break_audit_warehouse_path(data_dir: &str) {
        let warehouse_dir = PathBuf::from(data_dir).join("audit").join("warehouse");
        let _ = fs::remove_dir_all(&warehouse_dir);
        fs::write(&warehouse_dir, b"blocked").expect("replace warehouse dir with file");
    }

    #[tokio::test]
    async fn pulse_audit_runtime_finalize_marks_summary_completed() {
        let mut settings: Settings =
            serde_json::from_value(pulse_settings_json()).expect("deserialize pulse settings");
        let root =
            std::env::temp_dir().join(format!("polyalpha-pulse-finalize-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).expect("create temp root");
        settings.general.data_dir = root.to_string_lossy().into_owned();

        let now_ms = 1_717_171_717_000;
        let (_runtime_tx, runtime_rx, _market_tx, market_rx) = pulse_audit_channels();
        let mut audit = PulseAuditRuntime::start(
            &settings,
            "test",
            PulseExecutionMode::Paper,
            now_ms,
            runtime_rx,
            market_rx,
        )
        .expect("start audit runtime")
        .expect("audit runtime enabled");
        let runtime = PulseRuntimeBuilder::new(settings.clone())
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");
        let mut monitor_state = MonitorState::default();
        monitor_state.timestamp_ms = now_ms + 1_000;
        monitor_state.mode = TradingMode::Paper;

        audit
            .finalize(
                &runtime,
                &monitor_state,
                now_ms + 1_000,
                AuditSessionStatus::Completed,
            )
            .expect("finalize audit runtime");

        let summary = AuditReader::load_summary(&root, &audit.summary.session_id)
            .expect("load finalized summary");
        assert_eq!(summary.status, AuditSessionStatus::Completed);
        assert_eq!(summary.ended_at_ms, Some(now_ms + 1_000));
    }

    #[tokio::test]
    async fn pulse_audit_runtime_finalize_keeps_summary_when_warehouse_sync_fails() {
        let mut settings: Settings =
            serde_json::from_value(pulse_settings_json()).expect("deserialize pulse settings");
        let root =
            std::env::temp_dir().join(format!("polyalpha-pulse-finalize-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).expect("create temp root");
        settings.general.data_dir = root.to_string_lossy().into_owned();

        let now_ms = 1_717_171_717_000;
        let (_runtime_tx, runtime_rx, _market_tx, market_rx) = pulse_audit_channels();
        let mut audit = PulseAuditRuntime::start(
            &settings,
            "test",
            PulseExecutionMode::Paper,
            now_ms,
            runtime_rx,
            market_rx,
        )
        .expect("start audit runtime")
        .expect("audit runtime enabled");
        let runtime = PulseRuntimeBuilder::new(settings.clone())
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");
        let mut monitor_state = MonitorState::default();
        monitor_state.timestamp_ms = now_ms + 1_000;
        monitor_state.mode = TradingMode::Paper;

        break_audit_warehouse_path(&settings.general.data_dir);

        audit
            .finalize(
                &runtime,
                &monitor_state,
                now_ms + 1_000,
                AuditSessionStatus::Completed,
            )
            .expect("finalize should keep raw audit artifacts even if warehouse sync fails");

        let summary = AuditReader::load_summary(&root, &audit.summary.session_id)
            .expect("load finalized summary");
        assert_eq!(summary.status, AuditSessionStatus::Completed);
        assert_eq!(summary.ended_at_ms, Some(now_ms + 1_000));
    }
}
